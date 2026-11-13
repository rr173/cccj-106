"""asyncua 采集器：真实 OPC UA 协议通信、退避重连、有界队列、显式失败。

设计要点（逐条对应需求）：

1. **永久身份**：每次（重）连接先读服务器 ``NamespaceArray``，用契约里的
   namespace URI 换当前真实下标，再构造 NodeId；重连后绝不沿用旧下标。
2. **协议真实**：使用 :class:`asyncua.Client` 建立真实 socket / 会话 /
   subscription / monitored items；禁止用直接调用回调代替协议。
3. **接收语义**：通知处理协程只做"取值+编号+入队"，
   * 接收顺序号按回调到达顺序分配（节点内 + 全局），不按源时间排序；
   * 源时间/服务器时间原样保留，缺失为 ``None``，不用接收时间回填；
   * 非 Good 质量值保留原质量码并置 ``usable=False``，不丢弃、不改写 0；
   * 同源时间不同值的通知各占一个编号，两条都留。
4. **有界管线**：通知队列容量与每批提交规模都有界；队列满即
   :class:`QueueOverflowError` 显式失败并记录未完整接收的事实，不丢通知后
   谎报成功。
5. **断线重连**：自定义有上限间隔的指数退避；每次重连重新解析 URI、重新
   订阅；空档写连接事件（只给已知边界，不编造精确断点）；本机空档不会被
   标成设备停电（设备侧只体现在通知的 StatusCode 上）。
6. **终止条件**：URI 消失或节点类型与契约不符 -> 本次采集明确失败，历史
   保留；正常停止先排空队列再结束采集段，最后关闭订阅、客户端与数据库。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from asyncua import Client, ua
from asyncua.ua import status_codes as ua_status_codes

from .contract import (
    CollectorConfig,
    NodeContract,
    resolve_namespace_index,
)
from .errors import (
    ContractMismatchError,
    OpcCollectError,
    QueueOverflowError,
    StorageWriteError,
    UnsupportedValueError,
)
from .storage import (
    HistoryStore,
    NotificationRow,
    SegmentHandle,
    datetime_to_us,
    encode_value,
    now_us,
)

logger = logging.getLogger("minenergy.opccollect")


# ---------------------------------------------------------------------------
# 运行设置与结果
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CollectorSettings:
    """运行期设置（与文件配置分开，便于测试注入）。"""

    queue_capacity: int = 10_000
    batch_max_rows: int = 500
    batch_max_delay_ms: float = 200.0
    reconnect_backoff_initial_ms: float = 200.0
    reconnect_backoff_max_ms: float = 5_000.0
    publishing_interval_ms: float = 100.0
    sampling_interval_ms: float = 50.0
    request_timeout_ms: float = 5_000.0
    # 连接活性看门狗：两次成功 check 的最大间隔
    liveness_poll_ms: float = 250.0
    # 停止时等待队列排空的上限
    drain_timeout_ms: float = 30_000.0

    @classmethod
    def from_config(cls, config: CollectorConfig) -> "CollectorSettings":
        return cls(
            queue_capacity=config.queue_capacity,
            batch_max_rows=config.batch_max_rows,
            batch_max_delay_ms=config.batch_max_delay_ms,
            reconnect_backoff_initial_ms=config.reconnect_backoff_initial_ms,
            reconnect_backoff_max_ms=config.reconnect_backoff_max_ms,
            publishing_interval_ms=config.publishing_interval_ms,
            sampling_interval_ms=config.sampling_interval_ms,
        )


@dataclass(slots=True)
class RunResult:
    segment_id: int
    end_kind: str
    end_reason: str | None
    received_count: int
    committed_count: int
    enqueued_count: int
    connection_count: int
    disconnect_count: int

    @property
    def complete(self) -> bool:
        return (
            self.end_kind == "completed"
            and self.received_count == self.committed_count
        )


# ---------------------------------------------------------------------------
# 内部载荷
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Envelope:
    """一次 datachange 通知在管线内部的不可变载荷。

    序号在回调到达的瞬间分配并固化，写协程只是消费者，保证到达顺序不被
    后续处理（含批处理）打乱。
    """

    alias: str
    data_value: ua.DataValue
    arrival_us: int
    node_seq: int
    arrival_seq: int


# ---------------------------------------------------------------------------
# 采集器
# ---------------------------------------------------------------------------


class Collector:
    """单次采集生命周期（一个进程对应一个 :class:`Collector`）。"""

    def __init__(
        self,
        config: CollectorConfig,
        store: HistoryStore,
        settings: CollectorSettings | None = None,
    ):
        self.config = config
        self.store = store
        self.settings = settings or CollectorSettings.from_config(config)
        self._contracts = config.contract_by_alias()
        self._stop_event = asyncio.Event()
        self._fatal: BaseException | None = None
        self._fatal_reason: str | None = None
        # 接收编号（到达顺序）
        self._arrival_seq = 0
        self._node_seq: dict[str, int] = {c.alias: 0 for c in config.nodes}
        self._received_count = 0
        self._enqueued_count = 0
        self._committed_count = 0
        self._connection_count = 0
        self._disconnect_count = 0
        self._segment: SegmentHandle | None = None
        # 每轮连接的解析结果（NodeId 绝不跨连接复用）
        self._bound: dict[str, ua.NodeId] = {}
        self._last_liveness_us: int | None = None
        self._queue: asyncio.Queue[_Envelope | None] | None = None
        self._writer_task: asyncio.Task | None = None

    # -- 公共 API ------------------------------------------------------------

    async def run(self, until: Callable[[], Awaitable[bool]] | None = None) -> RunResult:
        """运行采集，直到 ``until`` 协程返回真、收到停止信号或发生致命错误。

        ``until`` 为可选的停止判定（演示用）；生产环境由 :meth:`request_stop`
        触发正常停止。契约类致命错误（URI 消失、类型不符、队列满、写盘失败）
        在把采集段记为 ``failed`` 后继续抛给调用方，已落库历史保留。
        """
        self._segment = self.store.begin_segment(
            self.config.endpoint,
            json.dumps(self.config.to_json_dict(), ensure_ascii=False),
        )
        self.store.register_contracts(
            self.config.nodes, self._segment.segment_id
        )
        # 接收顺序号在整个历史库内单调续编：新段不重置，避免跨段出现
        # 重复的 (alias, node_seq) 语义；段边界本身由 segments 表表达。
        max_arrival, node_max = self.store.max_sequences()
        self._arrival_seq = max_arrival
        for alias, value in node_max.items():
            if alias in self._node_seq:
                self._node_seq[alias] = value
        self._queue = asyncio.Queue(maxsize=self.settings.queue_capacity)
        self._writer_task = asyncio.create_task(
            self._writer_loop(self._segment.segment_id),
            name="opccollect-writer",
        )
        try:
            await self._supervise(until)
        except BaseException as exc:  # noqa: BLE001 - 统一收敛到段结束
            if self._fatal is None:
                self._fatal = exc
                self._fatal_reason = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            result = await self._finalize()
        if self._fatal is not None:
            raise self._fatal
        return result

    def request_stop(self) -> None:
        """请求正常停止：停止接收新通知，排空已入队通知后结束。"""
        self._stop_event.set()

    # -- 连接监督 + 退避重连 -------------------------------------------------

    async def _supervise(
        self, until: Callable[[], Awaitable[bool]] | None
    ) -> None:
        backoff_ms = self.settings.reconnect_backoff_initial_ms
        max_ms = self.settings.reconnect_backoff_max_ms
        while not self._stop_event.is_set() and self._fatal is None:
            client: Client | None = None
            subscription = None
            attempt_connected = False
            failure_reason: str | None = None
            failure_detail: dict[str, Any] | None = None
            try:
                client = Client(
                    self.config.endpoint,
                    timeout=self.settings.request_timeout_ms / 1000.0,
                    auto_reconnect=False,  # 重连策略由本类显式控制
                )
                await client.connect()
                subscription, bound = await self._bind_and_subscribe(client)
                self._bound = bound
                backoff_ms = self.settings.reconnect_backoff_initial_ms
                attempt_connected = True
                self._connection_count += 1
                self.store.record_connection(
                    self._segment.segment_id,
                    "session_connected",
                    now_us(),
                    detail={
                        "namespace_resolution": {
                            c.alias: self._node_id_tail(nid)
                            for c, nid in (
                                (self._contracts[a], bound[a]) for a in bound
                            )
                        },
                    },
                )
                self._last_liveness_us = now_us()
                # 连接健康期：主动 check_connection 发现断线或 stop 被请求才离开
                await self._wait_while_healthy(client, until)
            except _ConnectionLost as exc:
                failure_reason = exc.reason
                failure_detail = exc.detail
            except OpcCollectError:
                # 契约类致命错误：URI 消失/类型不符/不支持的值形状
                raise
            except Exception as exc:  # noqa: BLE001 - 连接期任何异常都走重连
                failure_reason = f"{type(exc).__name__}: {exc}"
            finally:
                await self._teardown_subscription(subscription)
                await self._teardown_client(client)
                self._bound = {}

            if self._stop_event.is_set() or self._fatal is not None:
                return

            if attempt_connected:
                # 已建立会话后断开：记录空档，只给已知边界不编造精确瞬间
                self._disconnect_count += 1
                self.store.record_connection(
                    self._segment.segment_id,
                    "session_disconnected",
                    observed_us=now_us(),
                    last_observed_us=self._last_liveness_us,
                    reason=failure_reason or "connection lost",
                    detail=failure_detail,
                )
            else:
                # 连接尝试本身失败（对端未起/拒绝连接）：不是已建立会话的断开，
                # 单独记账，避免把重连尝试伪装成多次断线，也不标成设备停电。
                self.store.record_connection(
                    self._segment.segment_id,
                    "connect_attempt_failed",
                    observed_us=now_us(),
                    last_observed_us=self._last_liveness_us,
                    reason=failure_reason or "connect attempt failed",
                    detail=failure_detail,
                )
            logger.warning(
                "OPC UA connection issue (%s); backing off %.0f ms",
                failure_reason, backoff_ms,
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=backoff_ms / 1000.0
                )
                # stop 在退避期间到达
                return
            except asyncio.TimeoutError:
                pass
            backoff_ms = min(backoff_ms * 2, max_ms)

    async def _wait_while_healthy(
        self,
        client: Client,
        until: Callable[[], Awaitable[bool]] | None,
    ) -> None:
        """连接健康期轮询：主动 check_connection 发现断线，stop/until 退出。

        每次成功探测都刷新 ``_last_liveness_us``，它作为断开事件的已知上界。
        """
        while not self._stop_event.is_set() and self._fatal is None:
            await asyncio.sleep(self.settings.liveness_poll_ms / 1000.0)
            if client.uaclient.state.name != "CONNECTED":
                raise _ConnectionLost(
                    f"client state {client.uaclient.state.name}"
                )
            try:
                await client.check_connection()
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                raise _ConnectionLost(
                    f"check_connection failed: {type(exc).__name__}: {exc}"
                ) from exc
            self._last_liveness_us = now_us()
            if until is not None and await until():
                self._stop_event.set()
                return

    # -- URI 解析 + 订阅 -----------------------------------------------------

    async def _bind_and_subscribe(
        self, client: Client
    ) -> tuple[Any, dict[str, ua.NodeId]]:
        namespace_array = await client.get_namespace_array()
        bound: dict[str, ua.NodeId] = {}
        nodes: list[Any] = []
        for contract in self.config.nodes:
            ns_index = resolve_namespace_index(
                namespace_array, contract.namespace_uri
            )
            node_id = contract.to_node_id(ns_index)
            node = client.get_node(node_id)
            # 必须能读到节点；区分"语义上读不到节点（致命，不能接到别的
            # 节点）"与"传输层临时失败（退避重连）"。
            try:
                data_value = await node.read_data_value()
            except ua.UaStatusCodeError as exc:
                if _is_transient_status(exc.code):
                    raise _ConnectionLost(
                        f"read {node_id.to_string()} transient status "
                        f"{hex(exc.code)}: {type(exc).__name__}"
                    ) from exc
                raise ContractMismatchError(
                    f"node {node_id.to_string()} cannot be bound: status "
                    f"{hex(exc.code)} {type(exc).__name__}",
                    alias=contract.alias,
                ) from exc
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                raise _ConnectionLost(
                    f"read {node_id.to_string()} transport error: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            self._check_received_shape(contract, data_value)
            bound[contract.alias] = node_id
            nodes.append(node)

        handler = _NotificationHandler(self)
        subscription = await client.create_subscription(
            self.settings.publishing_interval_ms, handler
        )
        results = await subscription.subscribe_data_change(
            nodes,
            queuesize=10,
            sampling_interval=self.settings.sampling_interval_ms,
        )
        self._check_monitored_item_results(results)
        return subscription, bound

    def _check_monitored_item_results(self, results: Any) -> None:
        if not isinstance(results, list):
            results = [results]
        aliases = [c.alias for c in self.config.nodes]
        for alias, result in zip(aliases, results):
            if isinstance(result, ua.StatusCode) and result.value != ua.StatusCodes.Good:
                name = ua_status_codes.code_to_name_doc.get(result.value, (None,))[0]
                raise ContractMismatchError(
                    f"monitored item creation rejected with status "
                    f"{hex(result.value)} ({name})",
                    alias=alias,
                )

    def _check_received_shape(
        self, contract: NodeContract, data_value: ua.DataValue
    ) -> None:
        """对连接时读到的首个 DataValue 做契约形状校验。

        质量码非 Good 不致命（坏质量读数本身就是要留痕的对象）；只有值
        形状/类型与契约不符才结束采集。
        """
        variant = data_value.Value
        if variant is None:
            return
        try:
            contract.validate_variant(variant)
        except (UnsupportedValueError, ContractMismatchError):
            raise

    # -- 通知回调（asyncua 协议层调用） --------------------------------------

    async def on_datachange(self, node: Any, val: Any, data: Any) -> None:
        if self._fatal is not None or self._stop_event.is_set():
            return
        try:
            await self._handle_datachange(node, data)
        except OpcCollectError as exc:
            # 契约/队列类致命错误：显式失败，段在 _finalize 记 failed
            self._fail(exc)
        except Exception as exc:  # noqa: BLE001 - 任何意外都不能被静默吞掉
            # 不允许异常逃出回调进入 asyncua 内部循环导致无声丢通知
            self._fail(
                OpcCollectError(
                    f"unexpected notification handling error: "
                    f"{type(exc).__name__}: {exc}"
                )
            )

    async def _handle_datachange(self, node: Any, data: Any) -> None:
        data_value: ua.DataValue = data.monitored_item.Value
        node_id: ua.NodeId = node.nodeid
        alias = self._alias_for_node_id(node_id)
        if alias is None:
            # 不属于本次契约的通知：显式失败而不是悄悄忽略
            self._fail(
                ContractMismatchError(
                    f"notification from unbound node {node_id.to_string()}"
                )
            )
            return
        # 形状校验在入队前完成：不支持的形状直接致命，不猜字符串继续收
        contract = self._contracts[alias]
        contract.validate_variant(data_value.Value)

        self._arrival_seq += 1
        self._node_seq[alias] += 1
        self._received_count += 1
        node_seq = self._node_seq[alias]
        arrival_seq = self._arrival_seq
        envelope = _Envelope(
            alias=alias,
            data_value=data_value,
            arrival_us=now_us(),
            node_seq=node_seq,
            arrival_seq=arrival_seq,
        )
        try:
            self._queue.put_nowait(envelope)
            self._enqueued_count += 1
        except asyncio.QueueFull:
            self._fail(
                QueueOverflowError(
                    f"bounded notification queue ({self.settings.queue_capacity} "
                    "items) is full; received notification cannot be persisted, "
                    "incomplete reception must be reported",
                    alias=alias,
                )
            )

    async def on_status_change(self, status: ua.StatusChangeNotification) -> None:
        code = status.Status.value
        name = ua_status_codes.code_to_name_doc.get(code, (None,))[0]
        logger.warning("subscription status change: %s %s", hex(code), name)
        if code != ua.StatusCodes.Good:
            # 服务端把订阅置坏；交给看门狗/状态轮询收敛成断线重连
            self._last_liveness_us = now_us()

    def _alias_for_node_id(self, node_id: ua.NodeId) -> str | None:
        for alias, bound_id in self._bound.items():
            if (
                bound_id.NamespaceIndex == node_id.NamespaceIndex
                and bound_id.Identifier == node_id.Identifier
            ):
                return alias
        return None

    # -- 写协程：有界批处理 --------------------------------------------------

    async def _writer_loop(self, segment_id: int) -> None:
        queue = self._queue
        batch: list[_Envelope] = []
        deadline: float | None = None
        loop = asyncio.get_event_loop()
        try:
            while True:
                if not batch:
                    # 无批时无限等待新通知或停止哨兵
                    item = await queue.get()
                else:
                    remaining = max(
                        0.0, (deadline or 0.0) - loop.time()
                    )
                    try:
                        item = await asyncio.wait_for(
                            queue.get(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        await self._flush(segment_id, batch)
                        batch = []
                        deadline = None
                        continue
                if item is None:
                    if batch:
                        await self._flush(segment_id, batch)
                    return
                batch.append(item)
                if deadline is None:
                    deadline = (
                        loop.time()
                        + self.settings.batch_max_delay_ms / 1000.0
                    )
                if len(batch) >= self.settings.batch_max_rows:
                    await self._flush(segment_id, batch)
                    batch = []
                    deadline = None
        except asyncio.CancelledError:
            # 致命失败路径：剩余批次不再承诺落库，直接退出
            raise
        except StorageWriteError as exc:
            # 写盘失败是致命错误：记录后退出写协程，由 _finalize 收敛段状态，
            # 不再消费后续通知（完整性已无法保证，不能假装继续成功）。
            self._fail(exc)
            return

    async def _flush(self, segment_id: int, batch: list[_Envelope]) -> None:
        rows = [self._to_row(segment_id, env) for env in batch]
        # sqlite 调用放到线程，避免阻塞事件循环
        await asyncio.to_thread(self.store.insert_notifications, rows)
        self._committed_count += len(rows)

    def _to_row(self, segment_id: int, env: _Envelope) -> NotificationRow:
        contract = self._contracts[env.alias]
        dv = env.data_value
        variant = dv.Value
        status_code = int(dv.StatusCode.value)
        usable = status_code == ua.StatusCodes.Good
        status_name = ua_status_codes.code_to_name_doc.get(
            status_code, (None,)
        )[0]
        if variant is None or variant.VariantType == ua.VariantType.Null:
            value = None
            value_is_null = True
        else:
            value = variant.Value
            value_is_null = value is None
        column, raw = encode_value(contract, value)
        return NotificationRow(
            alias=env.alias,
            segment_id=segment_id,
            node_seq=env.node_seq,
            arrival_seq=env.arrival_seq,
            arrival_us=env.arrival_us,
            source_us=datetime_to_us(dv.SourceTimestamp),
            server_us=datetime_to_us(dv.ServerTimestamp),
            status_code=status_code,
            status_name=status_name,
            usable=usable,
            value_is_null=value_is_null,
            value_column=column,
            value_raw=raw,
        )

    # -- 失败收敛与结束 ------------------------------------------------------

    def _fail(self, exc: BaseException) -> None:
        if self._fatal is None:
            self._fatal = exc
            self._fatal_reason = f"{type(exc).__name__}: {exc}"
            logger.error("collector failing: %s", exc)

    async def _finalize(self) -> RunResult:
        assert self._segment is not None
        # 正常停止：发哨兵并等待写协程把队列排空
        if self._fatal is None:
            try:
                await asyncio.wait_for(
                    self._drain_queue(),
                    self.settings.drain_timeout_ms / 1000.0,
                )
            except asyncio.TimeoutError:
                self._fail(
                    StorageWriteError(
                        "timed out draining the notification queue on shutdown; "
                        "received notifications remain uncommitted"
                    )
                )
        # 无论成功失败都收敛写协程
        if self._writer_task is not None:
            if self._fatal is None:
                # 正常路径下 drain 已 join；此处兜底
                if not self._writer_task.done():
                    self._writer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._writer_task
            else:
                self._writer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._writer_task

        end_kind = "completed" if self._fatal is None else "failed"
        reason = None if self._fatal is None else self._fatal_reason
        self.store.end_segment(
            self._segment,
            kind=end_kind,
            reason=reason,
            received_count=self._received_count,
            committed_count=self._committed_count,
            enqueued_count=self._enqueued_count,
        )
        result = RunResult(
            segment_id=self._segment.segment_id,
            end_kind=end_kind,
            end_reason=reason,
            received_count=self._received_count,
            committed_count=self._committed_count,
            enqueued_count=self._enqueued_count,
            connection_count=self._connection_count,
            disconnect_count=self._disconnect_count,
        )
        return result

    async def _drain_queue(self) -> None:
        assert self._queue is not None
        await self._queue.put(None)
        # 写协程收到哨兵后自行退出
        if self._writer_task is not None:
            await self._writer_task
            self._writer_task = None

    # -- 清理 ----------------------------------------------------------------

    async def _teardown_subscription(self, subscription: Any) -> None:
        if subscription is None:
            return
        with contextlib.suppress(Exception):
            await subscription.delete()

    async def _teardown_client(self, client: Client | None) -> None:
        if client is None:
            return
        with contextlib.suppress(Exception):
            await client.disconnect()

    @staticmethod
    def _node_id_tail(node_id: ua.NodeId) -> dict[str, Any]:
        return {
            "namespace_index": node_id.NamespaceIndex,
            "identifier": node_id.Identifier,
            "identifier_type": node_id.NodeIdType.name,
        }


# ---------------------------------------------------------------------------
# 回调适配器
# ---------------------------------------------------------------------------


class _NotificationHandler:
    """asyncua 订阅回调；全部转发给 :class:`Collector`。"""

    def __init__(self, collector: Collector):
        self._collector = collector

    async def datachange_notification(
        self, node: Any, val: Any, data: Any
    ) -> None:
        await self._collector.on_datachange(node, val, data)

    async def status_change_notification(
        self, status: ua.StatusChangeNotification
    ) -> None:
        await self._collector.on_status_change(status)


class _ConnectionLost(Exception):
    """内部信号：当前连接已不可用，应记录空档并退避重连。"""

    def __init__(self, reason: str, detail: dict[str, Any] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


# 绑定阶段可退避重试的传输/会话类状态码；其余状态码（BadNodeIdUnknown、
# BadTypeMismatch 等）视为永久的契约不符，直接结束采集。
_TRANSIENT_STATUS_CODES = frozenset({
    ua.StatusCodes.BadConnectionClosed,
    ua.StatusCodes.BadServerNotConnected,
    ua.StatusCodes.BadCommunicationError,
    ua.StatusCodes.BadTimeout,
    ua.StatusCodes.BadNoCommunication,
    ua.StatusCodes.BadSessionIdInvalid,
    ua.StatusCodes.BadSessionClosed,
    ua.StatusCodes.BadSecureChannelIdInvalid,
    ua.StatusCodes.BadSecureChannelClosed,
    ua.StatusCodes.BadTcpServerTooBusy,
})


def _is_transient_status(code: int) -> bool:
    return int(code) in _TRANSIENT_STATUS_CODES
