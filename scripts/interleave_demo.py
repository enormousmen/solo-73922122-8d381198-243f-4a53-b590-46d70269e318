#!/usr/bin/env python3
"""可控慢读下的交错操作验证客户端（仅用标准库 + pyarrow 计算预期行数）。

通过 /debug/slow-read 给分页读盘注入人为延迟，确定性地制造“一页正在
慢读”的时间窗，并在该窗口内验证：

  1. 上传补传与新游标创建立即完成，不被慢读拖住；
  2. 同一游标并发翻页按 FIFO 各读一段，合起来无重读、无跳行、行序不变；
  3. 慢读期间合并切换：在途页完整返回，obsolete 旧文件在读者释放前不回收；
  4. 慢读期间关闭游标：在途页明确失败且不推进进度，读者释放后才回收。

用法（容器内）：
    python scripts/interleave_demo.py --data /data/synthetic/weather-*.parquet
不带 --data 时脚本自己生成合成 Parquet（需 pyarrow，镜像内已具备）。
"""
from __future__ import annotations

import argparse
import io
import json
import os
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
FILES_DIR = os.path.join(DATA_DIR, "files")
SLOW = float(os.environ.get("INTERLEAVE_SLOW_SECONDS", "0.8"))


def call(method: str, path: str, body: bytes | None = None,
         expect=(200, 201, 202)):
    req = urllib.request.Request(
        BASE + path, data=body, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload, status = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        payload, status = e.read(), e.code
    data = json.loads(payload) if payload else {}
    if status not in expect:
        raise AssertionError(f"{method} {path} -> {status}: {data}")
    return status, data


def upload(path: str) -> dict:
    with open(path, "rb") as f:
        req = urllib.request.Request(
            BASE + "/uploads", data=f.read(), method="POST",
            headers={"Content-Type": "application/vnd.apache.parquet"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())


def set_slow(seconds: float) -> None:
    call("POST", "/debug/slow-read",
         json.dumps({"seconds": seconds}).encode())


def wait_pinned(timeout: float = 5.0) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, info = call("GET", "/debug/slow-read")
        if info["pinned_files"]:
            return info["pinned_files"]
        time.sleep(0.02)
    raise AssertionError("等待慢读 pin 超时")


def wait_merge(state: str, timeout: float = 60.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        merge = call("GET", "/merges/status")[1]["active_merge"]
        if merge and merge["state"] == state:
            return merge
        time.sleep(0.1)
    raise AssertionError(f"等待合并状态 {state} 超时")


def drain(cid: str, limit: int) -> list[dict]:
    rows = []
    while True:
        page = call("GET", f"/cursors/{cid}/pages?limit={limit}")[1]
        rows.extend(page["rows"])
        if page["exhausted"]:
            assert page["remaining"] == 0
            return rows


def section(title: str) -> None:
    print("\n" + "=" * 72 + f"\n {title}\n" + "=" * 72)


def ensure_data(args) -> list[str]:
    if args.data:
        paths = list(args.data)
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq

        out = os.path.join(DATA_DIR, "interleave-synthetic")
        os.makedirs(out, exist_ok=True)
        paths = []
        for i in range(args.files):
            rows = args.rows
            table = pa.table({
                "station_id": pa.array(
                    [f"ST-{(i * 3 + j) % 4 + 1:03d}" for j in range(rows)],
                    pa.string()),
                "observed_at": pa.array(
                    [pa.scalar(i * rows + j,
                               type=pa.timestamp("us", tz="UTC")).as_py()
                     for j in range(rows)], pa.timestamp("us", tz="UTC")),
                "temperature": pa.array(
                    [18.0 + (j % 5) + i for j in range(rows)], pa.float64()),
            })
            p = os.path.join(out, f"weather-{i:03d}.parquet")
            pq.write_table(table, p, compression="snappy")
            paths.append(p)
    return paths


def close_all_cursors() -> None:
    for c in call("GET", "/cursors")[1]["cursors"]:
        if not c["closed"]:
            call("POST", f"/cursors/{c['cursor_id']}/close")
    call("POST", "/merges/reclaim")


def main() -> None:
    ap = argparse.ArgumentParser(description="慢读交错操作验证")
    ap.add_argument("--data", nargs="*", help="已生成的 Parquet 文件列表")
    ap.add_argument("--files", type=int, default=4)
    ap.add_argument("--rows", type=int, default=40)
    args = ap.parse_args()

    section("0. 健康检查与环境清理")
    print(json.dumps(call("GET", "/health")[1], ensure_ascii=False))
    close_all_cursors()
    set_slow(0)

    paths = ensure_data(args)
    section(f"1. 上传 {len(paths)} 份 Parquet 并建立基线游标")
    total = 0
    for p in paths:
        info = upload(p)
        total += info["row_count"]
        print(f"  {os.path.basename(p)} -> rows={info['row_count']}")
    base_cid = call("POST", "/cursors")[1]["cursor_id"]
    baseline = drain(base_cid, limit=16)
    assert len(baseline) == total
    print(f"  基线顺序读取 {total} 行完成")

    section("2. 慢读窗口内：补传上传与新游标创建不被阻塞")
    cid = call("POST", "/cursors")[1]["cursor_id"]
    set_slow(SLOW)
    holder: dict = {}

    def slow_page():
        holder["resp"] = call(
            "GET", f"/cursors/{cid}/pages?limit=10", expect=(200, 404, 500)
        )

    t = threading.Thread(target=slow_page)
    t.start()
    pins = wait_pinned()
    print(f"  慢读已持有 pin: {pins}")
    t0 = time.monotonic()
    up_info = upload(paths[0])
    new_cur = call("POST", "/cursors")[1]
    blocked_for = time.monotonic() - t0
    print(f"  慢读期间上传 rows={up_info['row_count']}、建新游标 "
          f"snapshot_rows={new_cur['snapshot_rows']}，耗时 {blocked_for:.3f}s")
    assert blocked_for < SLOW, "慢读拖住了上传/建游标！"
    t.join(timeout=30)
    assert holder["resp"][0] == 200 and holder["resp"][1]["returned"] == 10
    set_slow(0)
    print("  慢读页完整返回 10 行 ✔")
    # 补传文件进入后续新建游标的快照；用独立游标取一份扩展基线
    ext_cid = call("POST", "/cursors")[1]["cursor_id"]
    extended_baseline = drain(ext_cid, limit=31)
    extended_total = len(extended_baseline)

    section("3. 同一游标并发翻页：FIFO 串行，无重读/跳行，行序不变")
    # 该游标在第 2 节补传之后创建，快照包含补传文件，总量=extended_total
    cid2 = call("POST", "/cursors")[1]["cursor_id"]
    snap2 = next(c for c in call("GET", "/cursors")[1]["cursors"]
                 if c["cursor_id"] == cid2)
    assert snap2["total_rows"] == extended_total
    set_slow(0.05)
    pages: list[tuple[int, list[dict]]] = []
    errors: list[Exception] = []
    lock = threading.Lock()
    stop = threading.Event()

    def worker(limit: int):
        try:
            local_pages: list[tuple[int, list[dict]]] = []
            while not stop.is_set():
                page = call(
                    "GET", f"/cursors/{cid2}/pages?limit={limit}")[1]
                # rows_read 是含本页后的位置，本页起点需减去本页行数
                local_pages.append(
                    (page["rows_read"] - page["returned"], list(page["rows"]))
                )
                if page["exhausted"]:
                    stop.set()
                    break
            with lock:
                pages.extend(local_pages)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(lim,))
               for lim in (7, 13, 29)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=60)
    set_slow(0)
    assert not errors, errors
    # 按读取起点排序后，各页区间必须首尾相接：无重叠（不重读）、无空洞（不跳行）
    pages.sort(key=lambda item: item[0])
    expect_start = 0
    assembled: list[dict] = []
    for start, page_rows in pages:
        assert start == expect_start, f"页区间断裂: {start} != {expect_start}"
        assembled.extend(page_rows)
        expect_start = start + len(page_rows)
    assert expect_start == extended_total
    assert len(assembled) == extended_total, "存在重读或跳行"
    assert assembled == extended_baseline, "行序与基线不一致"
    print(f"  3 个并发读者共拼出 {len(pages)} 页、{extended_total} 行，"
          "区间首尾相接且与基线行序完全一致 ✔")

    section("4. 慢读期间合并切换：在途页完整，旧文件读完前不回收")
    cid3 = call("POST", "/cursors")[1]["cursor_id"]
    snap3 = next(c for c in call("GET", "/cursors")[1]["cursors"]
                 if c["cursor_id"] == cid3)
    total3 = snap3["total_rows"]  # 含第 2 节补传，快照值以服务端为准
    set_slow(SLOW)
    holder2: dict = {}

    def slow_page2():
        holder2["resp"] = call(
            "GET", f"/cursors/{cid3}/pages?limit=12", expect=(200, 404, 500)
        )

    t = threading.Thread(target=slow_page2)
    t.start()
    wait_pinned()
    call("POST", "/merges")
    merge = wait_merge("completed")
    print(f"  慢读期间合并完成切换: output={merge['output_file_id']}")
    t.join(timeout=30)
    status, page = holder2["resp"]
    assert status == 200 and page["returned"] == 12, page
    print("  在途页完整返回 12 行，未受切换影响 ✔")
    rec = call("POST", "/merges/reclaim")[1]
    assert rec["reclaimed"] == [], "游标仍打开时旧文件不应被回收"
    # 在途首页已读 12 行，drain 只返回剩余部分，拼接后必须等于完整快照
    rest3 = drain(cid3, limit=17)
    rows3 = list(page["rows"]) + rest3
    assert len(rows3) == total3
    # 前 total 行（最初文件段）行序必须与基线一致
    assert rows3[:total] == baseline
    print(f"  切换后旧游标仍按原行序读完 {total3} 行（前 {total} 行与基线一致） ✔")

    section("5. 慢读期间关闭游标：在途页明确失败、不推进，释放后再回收")
    cid4 = call("POST", "/cursors")[1]["cursor_id"]
    before = call("GET", "/cursors")[1]["cursors"]
    cid4_progress_before = next(
        c["rows_read"] for c in before if c["cursor_id"] == cid4)
    set_slow(SLOW)
    holder3: dict = {}

    def slow_page3():
        holder3["resp"] = call(
            "GET", f"/cursors/{cid4}/pages?limit=9", expect=(200, 404, 500)
        )

    t = threading.Thread(target=slow_page3)
    t.start()
    pinned = wait_pinned()
    closed = call("POST", f"/cursors/{cid4}/close")[1]
    print(f"  慢读期间关闭游标，关闭回收: {closed['reclaimed']}，"
          f"在读 pin: {pinned}（被 pin 的文件必须跳过）")
    t.join(timeout=30)
    status3, body3 = holder3["resp"]
    assert status3 == 404, body3
    print(f"  在途页明确失败 404: {body3.get('detail')}")
    after = call("GET", "/cursors")[1]["cursors"]
    cid4_progress_after = next(
        c["rows_read"] for c in after if c["cursor_id"] == cid4)
    assert cid4_progress_after == cid4_progress_before, "失败页推进了进度！"
    print("  读取进度未推进 ✔")
    # 其他游标（基线/扩展/并发/切换各节创建）仍可能引用 obsolete 文件；
    # 全部关闭后，读者不再引用的 obsolete 文件才应被回收
    close_all_cursors()
    for _ in range(50):
        call("POST", "/merges/reclaim")
        leftover = [f for f in call("GET", "/files")[1]["files"]
                    if f["status"] == "obsolete"]
        if not leftover:
            break
        time.sleep(0.05)
    assert not leftover, "读者释放后仍有 obsolete 文件未回收"
    print("  读者不再引用后，obsolete 文件已全部回收 ✔")
    set_slow(0)

    section("6. 收尾：磁盘与元数据一致，active 文件不被误删")
    close_all_cursors()
    listing = call("GET", "/files")[1]["files"]
    disk = set(os.listdir(FILES_DIR))
    assert disk == {f["file_name"] for f in listing}, (disk, listing)
    assert all(f["status"] == "active" for f in listing)
    print(f"  磁盘 {len(disk)} 个文件全部为 active，元数据一致 ✔")
    section("interleave_demo 全部断言通过 ✔")


if __name__ == "__main__":
    main()
