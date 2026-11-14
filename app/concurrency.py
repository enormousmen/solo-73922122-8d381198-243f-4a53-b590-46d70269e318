"""并发原语：游标级 FIFO 串行锁与在读文件的 pin 注册表。

设计背景：历史实现把慢 Parquet 读盘放在进程全局锁 ``db.LOCK`` 内，
一页读得慢就会拖住上传、新游标创建等所有元数据操作。改造后：

- 慢读盘在全局锁之外进行，全局锁只保护短时元数据事务；
- 同一游标的并发翻页经 :class:`FifoLock` 严格按到达顺序串行，
  每次请求只在自己持有的区间内读盘并推进位置，不重读、不跳行；
- 读盘期间通过 :class:`FilePinRegistry` pin 住涉及的物理文件，
  回收方（``merger.reclaim``）只删除“无未关闭游标引用且无在读 pin”
  的 obsolete 文件，保证正在读的页不会与文件删除竞争。
"""
from __future__ import annotations

import threading
from collections import deque
from collections.abc import Iterable


class FifoLock:
    """严格 FIFO 的互斥锁（标准 ``Lock`` 不保证等待者获取顺序）。

    同一游标的并发翻页请求按调用顺序依次进入临界区，从而每个请求
    看到的都是上一个请求推进后的读取位置，天然杜绝重读/跳行。
    """

    def __init__(self) -> None:
        self._gate = threading.Lock()
        self._queue: deque[threading.Event] = deque()

    def acquire(self) -> None:
        ticket = threading.Event()
        with self._gate:
            head = not self._queue
            self._queue.append(ticket)
        if not head:
            ticket.wait()

    def release(self) -> None:
        with self._gate:
            self._queue.popleft()
            if self._queue:
                self._queue[0].set()

    @property
    def is_idle(self) -> bool:
        """没有任何持有者或等待者（调用方通常在释放后检查）。"""
        with self._gate:
            return not self._queue

    def __enter__(self) -> "FifoLock":
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class FilePinRegistry:
    """记录当前正在被读取（pin）的文件主键，供回收方跳过。

    pin/unpin 只在调用方持有 ``db.LOCK`` 的短事务内成对出现，
    与 ``reclaim`` 的检查互斥；``is_pinned`` 同样在 ``db.LOCK`` 内调用。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pins: dict[int, int] = {}

    def pin_many(self, file_pks: Iterable[int]) -> None:
        with self._lock:
            for pk in file_pks:
                self._pins[pk] = self._pins.get(pk, 0) + 1

    def unpin_many(self, file_pks: Iterable[int]) -> None:
        with self._lock:
            for pk in file_pks:
                count = self._pins.get(pk, 0)
                if count <= 1:
                    self._pins.pop(pk, None)
                else:
                    self._pins[pk] = count - 1

    def is_pinned(self, file_pk: int) -> bool:
        with self._lock:
            return file_pk in self._pins

    def snapshot(self) -> dict[int, int]:
        with self._lock:
            return dict(self._pins)


class CursorLockRegistry:
    """按游标 id 懒分配并复用 :class:`FifoLock`。"""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, FifoLock] = {}

    def get(self, cursor_id: str) -> FifoLock:
        with self._guard:
            lock = self._locks.get(cursor_id)
            if lock is None:
                lock = FifoLock()
                self._locks[cursor_id] = lock
            return lock

    def discard_if_idle(self, cursor_id: str) -> None:
        """游标关闭后回收其锁对象；仍有等待者时保留，避免破坏 FIFO。"""
        with self._guard:
            lock = self._locks.get(cursor_id)
            if lock is not None and lock.is_idle:
                self._locks.pop(cursor_id, None)


# 进程级单例
pin_registry = FilePinRegistry()
cursor_locks = CursorLockRegistry()
