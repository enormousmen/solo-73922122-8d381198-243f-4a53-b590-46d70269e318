"""游标：创建时固定当时已提交的行，之后按上传顺序 + 文件内行序分页。

- 快照只包含创建时 status=active 的文件；合并切换产生的新文件不会
  追加进旧游标，旧游标引用的输入文件转为 obsolete 但在游标读完/关闭前不回收。
- 保留重复行（不做任何去重）。
- 读取位置在每页返回时持久化，进程重启后未关闭游标可继续翻页。

并发模型（修复“慢读占全局锁”）：
- 全局锁 ``db.LOCK`` 只覆盖短时元数据事务（取快照/推进位置/关闭），
  慢 Parquet 读盘在全局锁之外执行，因此慢读期间上传、新游标创建、
  合并切换等操作都能立即获得锁并完成；
- 同一游标的并发翻页由每游标 FIFO 锁串行：请求按到达顺序依次进入，
  每个请求只读取“上一个请求推进后”属于自己的区间，不重读也不跳行；
- 读盘前把本页涉及的文件 pin 到 :data:`~app.concurrency.pin_registry`，
  回收方在全局锁内同时检查“无未关闭游标引用且无在读 pin”才删除文件；
- 读盘结束、重新拿到全局锁推进位置前再检查一次游标状态：若游标已被
  关闭/删除，则本页明确失败且不推进任何进度；读盘自身异常同样不推进。
"""
from __future__ import annotations

import uuid

from . import storage
from .concurrency import cursor_locks, pin_registry
from .db import LOCK, get_conn


class CursorError(RuntimeError):
    """游标状态错误（不存在/已关闭），API 层映射为 404。"""


class CursorReadError(RuntimeError):
    """读盘或文件完整性失败；API 层映射为 500，且绝不推进读取位置。"""


def create_cursor() -> dict:
    conn = get_conn()
    cursor_id = uuid.uuid4().hex
    with LOCK:
        files = conn.execute(
            """SELECT id, row_count, read_order, seq_order FROM files
               WHERE status='active' ORDER BY read_order, seq_order"""
        ).fetchall()
        conn.execute("INSERT INTO cursors(id) VALUES (?)", (cursor_id,))
        for pos, f in enumerate(files):
            conn.execute(
                "INSERT INTO cursor_files(cursor_id, file_id, position, row_count) "
                "VALUES (?, ?, ?, ?)",
                (cursor_id, f["id"], pos, f["row_count"]),
            )
            conn.execute(
                "INSERT INTO cursor_positions(cursor_id, file_id, rows_read) "
                "VALUES (?, ?, 0)",
                (cursor_id, f["id"]),
            )
        conn.commit()
    return {
        "cursor_id": cursor_id,
        "snapshot_files": len(files),
        "snapshot_rows": sum(f["row_count"] for f in files),
        "closed": False,
    }


def _get_cursor(conn, cursor_id: str):
    row = conn.execute(
        "SELECT * FROM cursors WHERE id=?", (cursor_id,)
    ).fetchone()
    if row is None:
        raise CursorError(f"游标 {cursor_id} 不存在")
    return row


def _load_snapshot(conn, cursor_id: str):
    return conn.execute(
        """SELECT cf.file_id, cf.position, cf.row_count, f.path,
                  COALESCE(cp.rows_read, 0) AS rows_read
           FROM cursor_files cf
           LEFT JOIN cursor_positions cp
             ON cp.cursor_id = cf.cursor_id AND cp.file_id = cf.file_id
           JOIN files f ON f.id = cf.file_id
           WHERE cf.cursor_id=?
           ORDER BY cf.position""",
        (cursor_id,),
    ).fetchall()


def _build_read_plan(snapshot, limit: int):
    """根据当前位置和 limit，计算本页要读的 (文件主键, 路径, offset, take)。"""
    plan = []
    remaining = limit
    for entry in snapshot:
        if remaining <= 0:
            break
        unread = entry["row_count"] - entry["rows_read"]
        if unread <= 0:
            continue
        take = min(remaining, unread)
        plan.append(
            (entry["file_id"], entry["path"], entry["rows_read"], take)
        )
        remaining -= take
    return plan


def next_page(cursor_id: str, limit: int) -> dict:
    conn = get_conn()
    # 同一游标：FIFO 串行，保证并发翻页严格按请求顺序各读一段
    with cursor_locks.get(cursor_id):
        # ---- 阶段 1：短全局锁事务，取状态/快照并 pin 本页文件 ----
        with LOCK:
            cur = _get_cursor(conn, cursor_id)
            if cur["closed"]:
                raise CursorError(f"游标 {cursor_id} 已关闭")
            snapshot = _load_snapshot(conn, cursor_id)
            plan = _build_read_plan(snapshot, limit)
            pinned = [file_pk for file_pk, _path, _off, _take in plan]
            pin_registry.pin_many(pinned)
            if not plan:
                # 快照已读完（例如 limit 落在边界后的重复请求）：直接返回空页
                total_rows = sum(e["row_count"] for e in snapshot)
                pin_registry.unpin_many(pinned)
                conn.execute(
                    "UPDATE cursors SET exhausted=1 WHERE id=?", (cursor_id,)
                )
                conn.commit()
                return {
                    "cursor_id": cursor_id,
                    "limit": limit,
                    "returned": 0,
                    "rows": [],
                    "exhausted": True,
                    "total_rows": total_rows,
                    "rows_read": total_rows,
                    "remaining": 0,
                }

        # ---- 阶段 2：全局锁之外慢读盘（期间其他操作可自由拿锁）----
        rows: list[dict] = []
        touched: list[tuple[int, int]] = []
        short_read = False
        read_failure: Exception | None = None
        try:
            for file_pk, path, rows_read, take in plan:
                try:
                    batch = storage.read_rows(path, rows_read, take)
                except Exception as exc:  # noqa: BLE001 - pyarrow 异常类型很多
                    read_failure = exc
                    break
                rows.extend(batch)
                touched.append((file_pk, rows_read + len(batch)))
                if len(batch) < take:
                    # 物理文件行数与元数据不符：停止继续读，绝不推进位置
                    short_read = True
                    break
        finally:
            # ---- 阶段 3：重新拿全局锁，校验状态后再决定推进/失败 ----
            with LOCK:
                pin_registry.unpin_many(pinned)
                if read_failure is not None:
                    # 读盘失败：明确报错，不推进任何进度
                    raise CursorReadError(
                        f"游标 {cursor_id} 读盘失败，本次不推进进度: {read_failure}"
                    ) from read_failure

                cur_now = conn.execute(
                    "SELECT closed FROM cursors WHERE id=?", (cursor_id,)
                ).fetchone()
                if cur_now is None or cur_now["closed"]:
                    # 读盘期间游标被关闭（或删除）：本页明确失败，不推进。
                    # 本请求刚释放 pin，可能正是最后一个读者，顺手补一次回收，
                    # 实现“读者不再引用后即可回收”。
                    if cur_now is not None:
                        from .merger import manager

                        manager.reclaim()
                    raise CursorError(
                        f"游标 {cursor_id} 已关闭，读盘期间未推进读取进度"
                    )

                if short_read:
                    raise CursorReadError(
                        "物理文件行数少于元数据记录，已保护性中断且不推进进度"
                    )

                # 正常路径：推进位置（touched 全部为完整读取）
                updated = dict(touched)
                read_total = sum(
                    updated.get(e["file_id"], e["rows_read"]) for e in snapshot
                )
                total_rows = sum(e["row_count"] for e in snapshot)
                exhausted = read_total >= total_rows

                for file_id, rows_read in touched:
                    conn.execute(
                        "UPDATE cursor_positions SET rows_read=? "
                        "WHERE cursor_id=? AND file_id=?",
                        (rows_read, cursor_id, file_id),
                    )
                if exhausted:
                    conn.execute(
                        "UPDATE cursors SET exhausted=1 WHERE id=?",
                        (cursor_id,),
                    )
                conn.commit()

                total_rows = sum(e["row_count"] for e in snapshot)
                # 精确统计持久化后的已读行数
                pos_rows = conn.execute(
                    "SELECT COALESCE(SUM(rows_read),0) AS n FROM cursor_positions "
                    "WHERE cursor_id=?",
                    (cursor_id,),
                ).fetchone()["n"]

        return {
            "cursor_id": cursor_id,
            "limit": limit,
            "returned": len(rows),
            "rows": rows,
            "exhausted": bool(exhausted),
            "total_rows": total_rows,
            "rows_read": pos_rows,
            "remaining": total_rows - pos_rows,
        }


def close_cursor(cursor_id: str) -> dict:
    conn = get_conn()
    with LOCK:
        cur = _get_cursor(conn, cursor_id)
        already = bool(cur["closed"])
        if not already:
            conn.execute(
                "UPDATE cursors SET closed=1 WHERE id=?", (cursor_id,)
            )
            conn.commit()
        # 关闭后立即尝试回收因此失去最后引用的 obsolete 文件；
        # 仍被其他慢读请求 pin 的文件会被回收方跳过，等其读完后补回收。
        from .merger import manager

        rec = manager.reclaim()
    # 元数据已提交且本调用不持有游标锁：若锁已空闲则回收，避免注册表无限增长
    cursor_locks.discard_if_idle(cursor_id)
    return {
        "cursor_id": cursor_id,
        "closed": True,
        "already_closed": already,
        "reclaimed": rec["reclaimed"],
    }


def list_cursors() -> dict:
    conn = get_conn()
    with LOCK:
        rows = conn.execute(
            """SELECT c.id, c.created_at, c.closed, c.exhausted,
                      (SELECT COALESCE(SUM(cf.row_count),0) FROM cursor_files cf
                       WHERE cf.cursor_id=c.id) AS total_rows,
                      (SELECT COALESCE(SUM(cp.rows_read),0)
                       FROM cursor_positions cp WHERE cp.cursor_id=c.id) AS rows_read
               FROM cursors c ORDER BY c.created_at"""
        ).fetchall()
    return {
        "cursors": [
            {
                "cursor_id": r["id"],
                "created_at": r["created_at"],
                "closed": bool(r["closed"]),
                "exhausted": bool(r["exhausted"]),
                "total_rows": r["total_rows"],
                "rows_read": r["rows_read"],
            }
            for r in rows
        ]
    }
