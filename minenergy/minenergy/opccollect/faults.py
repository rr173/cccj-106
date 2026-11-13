"""故障注入夹具：写盘异常、写阻塞（小队列溢出）与慢写观察。

这些类只服务于验收演示和测试，生产代码路径不引用它们。它们都包装真实的
:class:`HistoryStore`，因此落库格式、批量事务与正常路径完全一致——故障只
发生在"写"这个动作上，不伪造通知。
"""
from __future__ import annotations

import threading
from collections.abc import Sequence

from .errors import StorageWriteError
from .storage import HistoryStore, NotificationRow


class FailingHistoryStore(HistoryStore):
    """成功写入累计 ``fail_after`` 行后的下一次插入抛 :class:`StorageWriteError`。

    用来验证：写盘失败时采集段必须以 ``failed`` 结束、失败原因落库、此前
    已提交的历史仍然可读，绝不报告"完整成功"。
    """

    def __init__(self, path, fail_after: int):
        super().__init__(path)
        self.fail_after = fail_after
        self._written = 0
        self.failure_observed = False
        self.last_committed = 0

    def insert_notifications(self, rows: Sequence[NotificationRow]) -> int:
        if self._written >= self.fail_after:
            self.failure_observed = True
            raise StorageWriteError(
                f"injected disk write failure after {self._written} committed rows"
            )
        result = super().insert_notifications(rows)
        self._written += result
        self.last_committed = self._written
        return result


class BlockingHistoryStore(HistoryStore):
    """写入被门闩挡住的存储：把通知积压在有界队列里直到溢出。

    配合很小的 ``queue_capacity`` 与高速通知，能确定性地触发
    :class:`~minenergy.opccollect.errors.QueueOverflowError`，验证"队列满
    显式失败、不丢通知谎报成功"。

    门闩必须能在采集失败/进程退出时打开，否则被阻塞的工作线程会一直挂住
    解释器退出；:meth:`close` 同时置关闭标志，让积压写入显式失败而不是在
    段已标记 failed 之后又混进新行。
    """

    def __init__(self, path, *, block: bool = True):
        super().__init__(path)
        self._gate = threading.Event()
        self._closing = False
        if not block:
            self._gate.set()

    def insert_notifications(self, rows: Sequence[NotificationRow]) -> int:
        # 阻塞但不持有 sqlite 锁；门闩打开后按关闭标志决定成败
        self._gate.wait()
        if self._closing:
            raise StorageWriteError(
                "store closing after queue-overflow failure; buffered batch "
                "is deliberately not committed to the already-failed segment"
            )
        return super().insert_notifications(rows)

    def unblock(self) -> None:
        self._gate.set()

    def close(self) -> None:
        # 先放开门闩让工作线程解除阻塞，再关闭连接，保证进程能干净退出
        self._closing = True
        self._gate.set()
        super().close()

    @property
    def is_blocked(self) -> bool:
        return not self._gate.is_set()
