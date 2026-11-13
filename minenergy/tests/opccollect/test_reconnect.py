"""断线重连与永久身份：真实 socket 断开、占位 namespace、URI 消失、类型漂移。"""
from __future__ import annotations

import asyncio

import pytest

from minenergy.opccollect.collector import Collector
from minenergy.opccollect.errors import ContractMismatchError, UriMissingError
from minenergy.opccollect.server_fixture import (
    BUSINESS_URI,
    FixtureServer,
)
from minenergy.opccollect.storage import HistoryReader, HistoryStore
from minenergy.opccollect.testing import fast_settings, free_port, make_config


async def _start_collector(config, db_path):
    store = HistoryStore(db_path)
    collector = Collector(config, store, fast_settings(config))
    task = asyncio.create_task(collector.run())
    return collector, store, task


async def _wait_connections(reader_like_store, expected: int, timeout=6.0):
    """轮询 store 里的连接事件数到达 expected。"""
    deadline = asyncio.get_event_loop().time() + timeout

    async def count() -> int:
        with HistoryReader(reader_like_store.path) as reader:
            return len(
                [e for e in reader.connection_events()
                 if e["kind"] == "session_connected"]
            )

    while asyncio.get_event_loop().time() < deadline:
        if await count() >= expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"only {await count()} session_connected events, expected {expected}"
    )


async def test_reconnect_with_placeholder_namespace_shift(
    db_path, tmp_path
):
    """同一端点重启，业务 URI 换到新下标：历史连续、不串占位节点、空档可见。"""
    port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{port}"
    config = make_config(endpoint)

    # --- 第一台服务：业务 namespace 在下标 2 ---
    server1 = FixtureServer(port=port, placeholder_count=0)
    await server1.start()
    try:
        ns1 = await server1.namespace_array()
        biz_idx_1 = ns1.index(BUSINESS_URI)
        assert biz_idx_1 == 2

        collector, store, task = await _start_collector(config, db_path)
        await _wait_connections(store, 1)

        async def drive_phase1() -> None:
            await asyncio.sleep(0.3)
            await server1.write_value("meter_total", 100)
            await server1.write_value("voltage", 10.0)

        await drive_phase1()
        await asyncio.sleep(0.4)
        with HistoryReader(db_path) as reader:
            assert reader.count("meter_total") >= 1
    finally:
        await server1.stop()

    # --- 真正断开：采集器应记录空档并退避重连 ---
    # 等它观察到断线
    await asyncio.sleep(1.0)
    with HistoryReader(db_path) as reader:
        disconnects = [
            e for e in reader.connection_events()
            if e["kind"] == "session_disconnected"
        ]
        assert disconnects, "断开必须留下连接事件"
        gap = disconnects[-1]
        # 已知边界：last_observed_us <= observed_us，不编造精确断点
        assert gap["last_observed_us"] is not None
        assert gap["last_observed_us"] <= gap["observed_us"]
        assert gap["reason"]

    # --- 同端点重启：先注册 2 个占位 namespace，业务 URI 下标变成 4 ---
    server2 = FixtureServer(port=port, placeholder_count=2)
    await server2.start()
    try:
        ns2 = await server2.namespace_array()
        biz_idx_2 = ns2.index(BUSINESS_URI)
        assert biz_idx_2 == 4
        assert biz_idx_2 != biz_idx_1

        await _wait_connections(store, 2, timeout=8.0)

        async def drive_phase2() -> None:
            await asyncio.sleep(0.3)
            await server2.write_value("meter_total", 200)

        await drive_phase2()
        await asyncio.sleep(0.5)

        collector.request_stop()
        await asyncio.wait_for(task, timeout=5.0)
    finally:
        await server2.stop()
    store.close()

    # --- 新进程只读核对：同一永久节点的历史连续，且没串到占位节点 ---
    with HistoryReader(db_path) as reader:
        rows = list(reader.iter_rows("meter_total"))
        values = [r["value"] for r in rows if r["value"] is not None]
        assert 100 in values and 200 in values
        # 节点编号在同一别名下连续（两段采集？不——同一采集进程内重连，
        # node_seq 不重置）
        seqs = [r["node_seq"] for r in rows]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))

        events = reader.connection_events()
        connected = [e for e in events if e["kind"] == "session_connected"]
        disconnected = [e for e in events if e["kind"] == "session_disconnected"]
        assert len(connected) == 2 and len(disconnected) >= 1

        # 两次连接解析到的真实下标不同，但 URI 身份相同
        resolutions = [
            e["detail"]["namespace_resolution"]["meter_total"]["namespace_index"]
            for e in connected
        ]
        assert resolutions == [biz_idx_1, biz_idx_2]
        contracts = reader.contracts()
        meter = next(c for c in contracts if c.alias == "meter_total")
        assert meter.namespace_uri == BUSINESS_URI

        segments = reader.segments()
        assert len(segments) == 1  # 同一采集进程，重连不产生新段
        assert segments[0]["end_kind"] == "completed"


async def test_uri_disappearance_ends_collection_keeps_history(
    db_path,
):
    """重启后业务 URI 消失：明确失败结束，旧历史保留，不接到别的节点。"""
    port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{port}"
    config = make_config(endpoint)

    server1 = FixtureServer(port=port)
    await server1.start()
    try:
        collector, store, task = await _start_collector(config, db_path)
        await _wait_connections(store, 1)
        await asyncio.sleep(0.3)
        await server1.write_value("meter_total", 321)
        await asyncio.sleep(0.4)
    finally:
        await server1.stop()

    # 重启但不注册业务 URI
    server2 = FixtureServer(port=port, include_business_uri=False)
    await server2.start()
    try:
        with pytest.raises(UriMissingError):
            await asyncio.wait_for(task, timeout=10.0)
    finally:
        await server2.stop()
    store.close()

    with HistoryReader(db_path) as reader:
        segments = reader.segments()
        assert segments[-1]["end_kind"] == "failed"
        assert "UriMissing" in (segments[-1]["end_reason"] or "")
        # 旧历史仍可读
        values = [
            r["value"] for r in reader.iter_rows("meter_total")
            if r["value"] is not None
        ]
        assert 321 in values
        # 契约仍然登记着永久 URI
        meter = next(c for c in reader.contracts() if c.alias == "meter_total")
        assert meter.namespace_uri == BUSINESS_URI


async def test_type_drift_fails_without_mixing_types(db_path):
    """同标识节点类型与契约不符：失败结束，不换类型混写，旧历史保留。"""
    port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{port}"
    config = make_config(endpoint)

    server1 = FixtureServer(port=port)
    await server1.start()
    try:
        collector, store, task = await _start_collector(config, db_path)
        await _wait_connections(store, 1)
        await asyncio.sleep(0.3)
        await server1.write_value("meter_total", 900)
        await asyncio.sleep(0.4)
    finally:
        await server1.stop()

    # 重启：meter_total 变成 String 节点（同一 i=1001 标识）
    server2 = FixtureServer(port=port, type_change=True)
    await server2.start()
    try:
        with pytest.raises(ContractMismatchError):
            await asyncio.wait_for(task, timeout=10.0)
    finally:
        await server2.stop()
    store.close()

    with HistoryReader(db_path) as reader:
        segments = reader.segments()
        assert segments[-1]["end_kind"] == "failed"
        # 历史里的 meter_total 仍然是 Int64 语义，没有字符串混写
        import pyarrow as pa

        table = pa.Table.from_batches(
            list(reader.read_record_batches("meter_total"))
        )
        assert table.schema.field("value").type == pa.int64()
        assert 900 in table.column("value").to_pylist()


async def test_restarting_collector_creates_new_segment_with_gap(db_path):
    """采集机退出再开：新段 + 可观察空档，不伪装成连续无中断。"""
    port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{port}"
    config = make_config(endpoint)

    server = FixtureServer(port=port)
    await server.start()
    try:
        # 第一段
        collector1, store1, task1 = await _start_collector(config, db_path)
        await _wait_connections(store1, 1)
        await asyncio.sleep(0.3)
        await server.write_value("meter_total", 1)
        await asyncio.sleep(0.4)
        collector1.request_stop()
        await asyncio.wait_for(task1, timeout=5.0)
        store1.close()

        await asyncio.sleep(0.4)  # 采集机空档

        # 第二段：新进程/新 Collector，同一 db
        collector2, store2, task2 = await _start_collector(config, db_path)
        await _wait_connections(store2, 1)
        await asyncio.sleep(0.3)
        await server.write_value("meter_total", 2)
        await asyncio.sleep(0.4)
        collector2.request_stop()
        await asyncio.wait_for(task2, timeout=5.0)
        store2.close()
    finally:
        await server.stop()

    with HistoryReader(db_path) as reader:
        segments = reader.segments()
        assert len(segments) == 2
        assert all(s["end_kind"] == "completed" for s in segments)
        # 两段之间有真实时间空档
        assert segments[1]["started_us"] > segments[0]["ended_us"]
        # 每段各自从 node_seq=1 重新编号（段内顺序，不冒充全局连续）
        seg1_rows = [
            r for r in reader.iter_rows("meter_total") if r["value"] in (1,)
        ]
        assert seg1_rows
        # 两段都登记了连接事件
        assert len(reader.connection_events(segments[0]["segment_id"])) >= 1
        assert len(reader.connection_events(segments[1]["segment_id"])) >= 1
