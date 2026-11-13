"""故障场景：写盘异常、小队列溢出、有界缓冲与正常结束的接收/提交一致性。"""
from __future__ import annotations

import asyncio

import pytest

from minenergy.opccollect.collector import Collector
from minenergy.opccollect.errors import (
    QueueOverflowError,
    StorageWriteError,
    UnsupportedValueError,
)
from minenergy.opccollect.faults import (
    BlockingHistoryStore,
    FailingHistoryStore,
)
from minenergy.opccollect.storage import HistoryReader, HistoryStore
from minenergy.opccollect.testing import fast_settings, make_config


async def _collector_with(config, db_path, store, **settings):
    base = fast_settings(config)
    for key, value in settings.items():
        setattr(base, key, value)
    collector = Collector(config, store, base)
    task = asyncio.create_task(collector.run())
    return collector, task


async def test_disk_write_failure_marks_segment_failed_history_readable(
    fixture_server, db_path
):
    config = make_config(fixture_server.endpoint)
    server = fixture_server
    # 先允许初始批（10 个节点的初始值）成功落库，随后注入写盘失败
    store = FailingHistoryStore(db_path, fail_after=12)
    collector, task = await _collector_with(
        config, db_path, store, batch_max_rows=10
    )

    async def drive() -> None:
        await asyncio.sleep(0.4)
        for i in range(1, 12):
            await server.write_value("meter_total", 1000 + i)
            await asyncio.sleep(0.05)

    driver = asyncio.create_task(drive())
    with pytest.raises(StorageWriteError):
        await asyncio.wait_for(task, timeout=10.0)
    await driver
    store.close()

    assert store.failure_observed is True
    with HistoryReader(db_path) as reader:
        segments = reader.segments()
        last = segments[-1]
        assert last["end_kind"] == "failed"
        assert "disk write failure" in (last["end_reason"] or "")
        # 失败前已提交的历史仍然完整可读（至少初始批落过库）
        assert store.last_committed >= 10
        committed_total = sum(reader.count(c.alias) for c in reader.contracts())
        assert committed_total == store.last_committed
        # 失败段不会被伪装成完整成功
        assert last["received_count"] >= last["committed_count"]
        # 仍能按契约 Arrow 类型读回
        batches = list(reader.read_record_batches("meter_total"))
        assert batches  # 历史没有被损坏或清空


async def test_queue_overflow_fails_explicitly_without_silent_drop(
    fixture_server, db_path
):
    # 极小队列 + 写线程被门闩挡住：通知必然积压到溢出
    config = make_config(fixture_server.endpoint, queue_capacity=4)
    server = fixture_server
    store = BlockingHistoryStore(db_path, block=True)
    collector, task = await _collector_with(config, db_path, store)

    async def drive() -> None:
        await asyncio.sleep(0.4)
        for i in range(200):
            await server.write_value("meter_total", 5000 + i)
        # 给回调足够时间把队列塞满
        await asyncio.sleep(1.0)

    driver = asyncio.create_task(drive())
    with pytest.raises(QueueOverflowError):
        await asyncio.wait_for(task, timeout=12.0)
    await driver
    store.close()

    with HistoryReader(db_path) as reader:
        segment = reader.segments()[-1]
        assert segment["end_kind"] == "failed"
        assert "queue" in (segment["end_reason"] or "").lower()


async def test_unsupported_shape_arriving_over_wire_fails_segment(
    fixture_server, db_path
):
    """线上收到多维数组（契约只要一维）：明确失败，不猜成字符串继续收。"""
    from asyncua import ua

    config = make_config(fixture_server.endpoint)
    server = fixture_server
    store = HistoryStore(db_path)
    collector, task = await _collector_with(config, db_path, store)

    async def drive() -> None:
        await asyncio.sleep(0.4)
        node = server._node_handles["waveform"]
        # 直接在服务端写多维 Variant（绕过夹具的 JSON 单维约束）
        await node.write_value(
            ua.Variant(
                [[1, 2], [3, 4]],
                ua.VariantType.Int64,
                Dimensions=[2, 2],
            )
        )

    driver = asyncio.create_task(drive())
    with pytest.raises(UnsupportedValueError):
        await asyncio.wait_for(task, timeout=8.0)
    await driver
    store.close()

    with HistoryReader(db_path) as reader:
        assert reader.segments()[-1]["end_kind"] == "failed"
        # 一维历史仍可读
        assert reader.count("waveform") >= 1


async def test_normal_stop_drains_queue_and_counts_match(
    fixture_server, db_path
):
    """正常停止：已接收队列全部写完，received == committed == enqueued。"""
    config = make_config(
        fixture_server.endpoint,
        queue_capacity=64,
        batch_max_rows=8,
        batch_max_delay_ms=500.0,  # 故意攒批，验证停止时仍排空
    )
    server = fixture_server
    store = HistoryStore(db_path)
    collector, task = await _collector_with(config, db_path, store)

    total = 60

    async def drive() -> None:
        await asyncio.sleep(0.4)
        for i in range(total):
            await server.write_value("meter_total", 7000 + i)
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.6)
        collector.request_stop()

    await asyncio.gather(drive(), task)
    store.close()

    with HistoryReader(db_path) as reader:
        segment = reader.segments()[-1]
        assert segment["end_kind"] == "completed"
        assert segment["received_count"] == segment["committed_count"]
        assert segment["enqueued_count"] == segment["committed_count"]
        # meter_total：初始值 + total 次更新（无丢失）
        assert reader.count("meter_total") >= total


async def test_bounded_queue_does_not_grow_under_sustained_notifications(
    fixture_server, db_path
):
    """持续通知下缓冲有界：队列水位始终不超过容量，提交持续推进。"""
    capacity = 32
    config = make_config(
        fixture_server.endpoint,
        queue_capacity=capacity,
        batch_max_rows=4,
        batch_max_delay_ms=20.0,
    )
    server = fixture_server
    store = HistoryStore(db_path)
    collector, task = await _collector_with(config, db_path, store)

    observed_max = 0

    async def monitor() -> None:
        nonlocal observed_max
        while not task.done():
            q = collector._queue
            if q is not None:
                observed_max = max(observed_max, q.qsize())
            await asyncio.sleep(0.005)

    async def drive() -> None:
        await asyncio.sleep(0.4)
        for i in range(300):
            await server.write_value("voltage", float(i))
            await asyncio.sleep(0.008)
        await asyncio.sleep(0.4)
        collector.request_stop()

    mon = asyncio.create_task(monitor())
    await asyncio.gather(drive(), task)
    mon.cancel()
    store.close()

    assert observed_max <= capacity
    with HistoryReader(db_path) as reader:
        segment = reader.segments()[-1]
        assert segment["end_kind"] == "completed"
        assert segment["received_count"] == segment["committed_count"]
        assert reader.count("voltage") >= 300


async def test_contract_change_on_reopen_is_rejected(tmp_path):
    """新进程用改动过的契约（换类型）打开同一历史库：拒绝混写。"""
    from minenergy.opccollect.contract import NodeContract
    from minenergy.opccollect.errors import StorageWriteError as SWE
    from minenergy.opccollect.server_fixture import BUSINESS_URI

    db = tmp_path / "h.sqlite"
    store = HistoryStore(db)
    seg = store.begin_segment("opc.tcp://x", "{}")
    store.register_contracts(
        (NodeContract("m", BUSINESS_URI, 1, "Int64"),), seg.segment_id
    )
    store.end_segment(seg, "completed", None, 0, 0, 0)
    store.close()

    store2 = HistoryStore(db)
    seg2 = store2.begin_segment("opc.tcp://x", "{}")
    with pytest.raises(SWE):
        store2.register_contracts(
            (NodeContract("m", BUSINESS_URI, 1, "String"),),
            seg2.segment_id,
        )
    store2.close()
