"""慢读交错场景测试（本次修复的核心回归）。

覆盖：
1. 一页慢读占住读盘时，上传与新游标创建不被阻塞；
2. 同一游标并发翻页按 FIFO 各读一段，所有响应合起来无重读、无跳行、行序不变；
3. 慢读期间关闭游标：在途页明确失败且不推进进度，文件在读者释放前不回收；
4. 慢读期间合并切换：在途页仍完整返回，旧文件等读者释放后才回收；
5. 读盘失败明确报错且不推进进度；pin 注册表成对计数正确。
"""
from __future__ import annotations

import io
import os
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import config, storage
from app.concurrency import FifoLock, pin_registry
from app.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_slow_read():
    storage.slow_read.set(0)
    yield
    storage.slow_read.set(0)


@pytest.fixture(autouse=True)
def _clean_lakehouse():
    """每个用例前清空元数据与磁盘文件，保证行数断言不受其他用例累积影响。"""
    from app.db import get_conn
    _close_all_cursors()
    conn = get_conn()
    for table in ("merge_files", "merges", "cursor_positions",
                  "cursor_files", "cursors", "files", "settings"):
        conn.execute(f"DELETE FROM {table}")
    conn.execute(
        "INSERT OR IGNORE INTO settings(key, value) VALUES ('file_seq', 0)"
    )
    conn.execute("UPDATE settings SET value=0 WHERE key='file_seq'")
    conn.commit()
    for name in os.listdir(config.FILES_DIR):
        os.remove(os.path.join(config.FILES_DIR, name))
    for name in os.listdir(config.TMP_DIR):
        os.remove(os.path.join(config.TMP_DIR, name))
    yield


def _parquet_bytes(rows: int, tag: int) -> bytes:
    base = tag * 100_000
    table = pa.table({
        "station_id": pa.array(
            [f"ST-{(i + tag) % 4 + 1:03d}" for i in range(rows)], pa.string()
        ),
        "observed_at": pa.array(
            [pa.scalar(base + i, type=pa.timestamp("us", tz="UTC")).as_py()
             for i in range(rows)],
            pa.timestamp("us", tz="UTC"),
        ),
        "temperature": pa.array(
            [20.0 + (i % 7) + tag for i in range(rows)], pa.float64()
        ),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _close_all_cursors():
    for c in client.get("/cursors").json()["cursors"]:
        if not c["closed"]:
            client.post(f"/cursors/{c['cursor_id']}/close")


def _drain(cid: str, limit: int = 1000):
    """耗尽一个游标（假定没有其他读者），返回 (全部行, 快照总行数)。"""
    rows = []
    total_rows = None
    while True:
        page = client.get(f"/cursors/{cid}/pages?limit={limit}").json()
        rows.extend(page["rows"])
        total_rows = page["total_rows"]
        if page["exhausted"]:
            return rows, total_rows


def _wait_pinned(timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pin_registry.snapshot():
            return
        time.sleep(0.02)
    raise AssertionError("等待慢读 pin 超时")


def test_slow_page_does_not_block_upload_or_cursor_creation():
    _close_all_cursors()
    data = _parquet_bytes(40, 11)
    assert client.post("/uploads", content=data).status_code == 200
    cid = client.post("/cursors").json()["cursor_id"]

    storage.slow_read.set(0.8)
    start = time.monotonic()
    holder = {}

    def slow_page():
        holder["resp"] = client.get(f"/cursors/{cid}/pages?limit=10")

    t = threading.Thread(target=slow_page)
    t.start()
    _wait_pinned()
    assert pin_registry.snapshot(), "慢读期间对应文件应处于 pinned 状态"

    # 慢读进行中：上传与建游标都必须快速完成（远小于慢读延迟）
    op_start = time.monotonic()
    up = client.post("/uploads", content=_parquet_bytes(20, 12))
    new_cur = client.post("/cursors")
    op_elapsed = time.monotonic() - op_start
    assert up.status_code == 200 and new_cur.status_code == 200
    assert op_elapsed < 0.5, f"慢读拖住了其他操作，耗时 {op_elapsed:.2f}s"

    t.join(timeout=5)
    total_elapsed = time.monotonic() - start
    assert total_elapsed >= 0.8
    assert holder["resp"].status_code == 200
    assert holder["resp"].json()["returned"] == 10
    assert not pin_registry.snapshot(), "请求结束后 pin 必须全部释放"
    _close_all_cursors()


def test_concurrent_pages_on_same_cursor_are_fifo_without_reread_or_skip():
    _close_all_cursors()
    n_files, per_file = 4, 30
    total = 0
    for tag in range(n_files):
        r = client.post("/uploads", content=_parquet_bytes(per_file, tag + 300))
        total += r.json()["row_count"]

    # 基准：独立游标顺序翻页得到的完整行序
    base_cid = client.post("/cursors").json()["cursor_id"]
    baseline, baseline_total = _drain(base_cid, limit=16)
    assert baseline_total == total

    # 多个线程共用同一个游标并发翻页：FIFO 串行后所有页拼起来
    # 必须恰好等于完整快照——不重读（无重复）、不跳行（无缺失）、顺序不变。
    # 各线程完成时刻不同，因此记录每页的起始 rows_read，按读取位置重建行序。
    cid = client.post("/cursors").json()["cursor_id"]
    storage.slow_read.set(0.05)  # 放大并发交错窗口，但保持测试快速
    pages: list[tuple[int, list[dict]]] = []
    errors: list[Exception] = []
    collect_lock = threading.Lock()
    stop = threading.Event()

    def worker(page_limit: int):
        try:
            local_pages: list[tuple[int, list[dict]]] = []
            while not stop.is_set():
                r = client.get(f"/cursors/{cid}/pages?limit={page_limit}")
                if r.status_code == 404:
                    return
                assert r.status_code == 200, r.text
                page = r.json()
                # rows_read 是“含本页后”的位置，本页起始位置需减去本页行数
                local_pages.append(
                    (page["rows_read"] - page["returned"], list(page["rows"]))
                )
                if page["exhausted"]:
                    stop.set()
                    break
            with collect_lock:
                pages.extend(local_pages)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(limit,))
        for limit in (7, 13, 29)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    # 页与页的区间必须首尾相接、无重叠无空洞（FIFO 推进的直接证据）
    pages.sort(key=lambda p: p[0])
    expected_start = 0
    collected: list[dict] = []
    for start, page_rows in pages:
        assert start == expected_start, (
            f"页区间断裂: 期望起始 {expected_start}，实际 {start}"
        )
        collected.extend(page_rows)
        expected_start = start + len(page_rows)
    assert expected_start == total
    assert len(collected) == total, "存在重读或跳行"
    assert collected == baseline, "按读取位置重建后的行序与基线不一致"
    # 游标位置应恰好推进到 total
    match = next(c for c in client.get("/cursors").json()["cursors"]
                 if c["cursor_id"] == cid)
    assert match["rows_read"] == total and match["exhausted"] is True
    _close_all_cursors()


def test_close_during_slow_read_fails_explicitly_and_keeps_file():
    from app.db import get_conn

    _close_all_cursors()
    client.post("/merges/reclaim")
    # 两份文件，合并后输入文件变 obsolete，新游标仍引用它们
    client.post("/uploads", content=_parquet_bytes(25, 400))
    client.post("/uploads", content=_parquet_bytes(25, 401))
    cid = client.post("/cursors").json()["cursor_id"]
    snapshot_rows = client.get("/cursors").json()
    snap = next(c for c in snapshot_rows["cursors"] if c["cursor_id"] == cid)
    assert snap["total_rows"] == 50

    client.post("/merges")
    for _ in range(100):
        m = client.get("/merges/status").json()["active_merge"]
        if m and m["state"] == "completed":
            break
        time.sleep(0.05)
    assert m["state"] == "completed"

    obsolete = [f for f in client.get("/files").json()["files"]
                if f["status"] == "obsolete"]
    assert obsolete, "合并切换后输入文件应为 obsolete"
    obsolete_ids = {f["file_id"] for f in obsolete}

    storage.slow_read.set(0.8)
    holder = {}

    def slow_page():
        holder["resp"] = client.get(f"/cursors/{cid}/pages?limit=10")

    t = threading.Thread(target=slow_page)
    t.start()
    _wait_pinned()

    # 读盘期间关闭并尝试回收：
    # - 正在被本页 pin 的文件必须跳过、保留在磁盘；
    # - 未被任何读者引用的另一个 obsolete 文件可以立即回收。
    client.post(f"/cursors/{cid}/close")
    reclaim = client.post("/merges/reclaim").json()
    assert set(reclaim["skipped_pinned"]) <= obsolete_ids
    assert reclaim["skipped_pinned"], "在读文件应被 pin 保护而跳过回收"
    protected_ids = set(reclaim["skipped_pinned"])
    assert not (set(reclaim["reclaimed"]) & protected_ids)
    listing_now = {f["file_id"]: f for f in client.get("/files").json()["files"]}
    # 被 pin 保护的文件仍在元数据与磁盘上
    for fid in protected_ids:
        assert fid in listing_now and listing_now[fid]["status"] == "obsolete"
        assert os.path.exists(
            os.path.join(config.FILES_DIR, listing_now[fid]["file_name"])
        )

    t.join(timeout=5)
    # 在途页明确失败（游标已关闭），且不推进进度
    assert holder["resp"].status_code == 404
    conn = get_conn()
    progress = conn.execute(
        "SELECT COALESCE(SUM(rows_read),0) AS n FROM cursor_positions "
        "WHERE cursor_id=?", (cid,)
    ).fetchone()["n"]
    assert progress == 0

    # 读者释放 pin 后，obsolete 文件最终必须被回收（可能由慢读线程在
    # 阶段 3 的补回收完成，也可能由下面的显式 reclaim 完成——只校验终态）
    for _ in range(50):
        client.post("/merges/reclaim")
        remaining = client.get("/files").json()["files"]
        if not any(f["status"] == "obsolete" for f in remaining):
            break
        time.sleep(0.05)
    remaining = client.get("/files").json()["files"]
    assert all(f["status"] == "active" for f in remaining)
    assert not {f["file_id"] for f in remaining} & obsolete_ids
    assert all(
        not os.path.exists(os.path.join(config.FILES_DIR, f["file_name"]))
        for f in obsolete
    )
    storage.slow_read.set(0)


def test_merge_switch_during_slow_read_returns_complete_page():
    _close_all_cursors()
    client.post("/merges/reclaim")
    client.post("/uploads", content=_parquet_bytes(30, 500))
    client.post("/uploads", content=_parquet_bytes(30, 501))
    cid = client.post("/cursors").json()["cursor_id"]
    snap = next(c for c in client.get("/cursors").json()["cursors"]
                if c["cursor_id"] == cid)
    total = snap["total_rows"]
    assert total == 60

    storage.slow_read.set(1.0)
    holder = {}

    def slow_page():
        holder["resp"] = client.get(f"/cursors/{cid}/pages?limit=12")

    t = threading.Thread(target=slow_page)
    t.start()
    _wait_pinned()

    # 慢读进行中发起合并并等待切换完成
    assert client.post("/merges").status_code == 202
    for _ in range(120):
        m = client.get("/merges/status").json()["active_merge"]
        if m and m["state"] == "completed":
            break
        time.sleep(0.05)
    assert m["state"] == "completed"

    t.join(timeout=5)
    resp = holder["resp"]
    # 在途页必须完整返回（合并切换不影响已 pin 的旧文件读取）
    assert resp.status_code == 200, resp.text
    first = resp.json()
    assert first["returned"] == 12

    # 游标继续读完全部行（含在途首页的 12 行），行序/行数与快照一致
    rest, total_rows = _drain(cid, limit=17)
    assert total_rows == total
    assert len(first["rows"]) + len(rest) == total
    # 首页 + 后续页拼接顺序正确：与一个独立新游标的完整结果一致
    ref_cid = client.post("/cursors").json()["cursor_id"]
    ref_rows, _ = _drain(ref_cid, limit=19)
    assert first["rows"] + rest == ref_rows

    # 游标未关闭：obsolete 旧文件不得回收
    rec = client.post("/merges/reclaim").json()
    assert rec["reclaimed"] == []
    assert any(f["status"] == "obsolete"
               for f in client.get("/files").json()["files"])
    _close_all_cursors()
    # 关闭后最终全部回收（具体由哪个调用触发不作要求，只校验终态）
    for _ in range(50):
        client.post("/merges/reclaim")
        leftover = [f for f in client.get("/files").json()["files"]
                    if f["status"] == "obsolete"]
        if not leftover:
            break
        time.sleep(0.05)
    assert not leftover
    storage.slow_read.set(0)


def test_read_failure_does_not_advance_progress(monkeypatch):
    _close_all_cursors()
    client.post("/uploads", content=_parquet_bytes(20, 600))
    cid = client.post("/cursors").json()["cursor_id"]
    snap = next(c for c in client.get("/cursors").json()["cursors"]
                if c["cursor_id"] == cid)
    total = snap["total_rows"]

    def boom(*_args, **_kwargs):
        raise OSError("模拟磁盘读失败")

    monkeypatch.setattr(storage, "read_rows", boom)
    r = client.get(f"/cursors/{cid}/pages?limit=5")
    assert r.status_code == 500
    from app.db import get_conn
    progress = get_conn().execute(
        "SELECT COALESCE(SUM(rows_read),0) AS n FROM cursor_positions "
        "WHERE cursor_id=?", (cid,)
    ).fetchone()["n"]
    assert progress == 0  # 失败不推进，客户端可安全重试
    # pin 必须已释放，不会泄漏
    assert not pin_registry.snapshot()

    monkeypatch.undo()
    rows, total_rows = _drain(cid, limit=8)
    assert total_rows == total and len(rows) == total
    _close_all_cursors()


def test_fifo_lock_is_strictly_ordered():
    lock = FifoLock()
    order: list[int] = []
    barrier = threading.Event()

    def worker(i: int):
        with lock:
            order.append(i)
            if i == 0:
                barrier.set()
                time.sleep(0.15)

    # 先让 0 号持锁
    t0 = threading.Thread(target=worker, args=(0,))
    t0.start()
    barrier.wait(timeout=2)
    rest = [threading.Thread(target=worker, args=(i,))
            for i in range(1, 6)]
    for t in rest:
        t.start()
        time.sleep(0.01)  # 固定入队顺序
    for t in rest:
        t.join(timeout=5)
    t0.join(timeout=5)
    assert order == [0, 1, 2, 3, 4, 5]


def test_pin_registry_counts_and_releases():
    assert pin_registry.snapshot() == {}
    pin_registry.pin_many([1, 1, 2])
    assert pin_registry.is_pinned(1) and pin_registry.is_pinned(2)
    assert pin_registry.snapshot() == {1: 2, 2: 1}
    pin_registry.unpin_many([1])
    assert pin_registry.is_pinned(1)  # 还有一个引用
    pin_registry.unpin_many([1, 2])
    assert pin_registry.snapshot() == {}


def test_page_after_exhaustion_returns_empty_without_pin_leak():
    _close_all_cursors()
    client.post("/uploads", content=_parquet_bytes(12, 800))
    cid = client.post("/cursors").json()["cursor_id"]
    rows, total_rows = _drain(cid, limit=5)
    assert total_rows == 12 and len(rows) == 12
    # 耗尽后再请求：空页、exhausted，且不残留 pin
    again = client.get(f"/cursors/{cid}/pages?limit=5").json()
    assert again["returned"] == 0 and again["exhausted"] is True
    assert again["rows_read"] == 12 and again["remaining"] == 0
    assert not pin_registry.snapshot()
    _close_all_cursors()


def test_cursor_lock_registry_discards_idle_lock_on_close():
    from app.concurrency import cursor_locks

    _close_all_cursors()
    client.post("/uploads", content=_parquet_bytes(10, 700))
    cid = client.post("/cursors").json()["cursor_id"]
    lock = cursor_locks.get(cid)
    assert cursor_locks.get(cid) is lock  # 同一游标复用同一把锁
    client.get(f"/cursors/{cid}/pages?limit=3")
    client.post(f"/cursors/{cid}/close")
    # 关闭后空闲锁应被回收；再访问会懒分配一把新锁
    assert cid not in cursor_locks._locks  # noqa: SLF001
    assert cursor_locks.get(cid) is not lock
    _close_all_cursors()
