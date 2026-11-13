"""退避重连的有界性与重连行为细节。"""
from __future__ import annotations

import asyncio

import pytest

from minenergy.opccollect.collector import Collector
from minenergy.opccollect.server_fixture import FixtureServer
from minenergy.opccollect.storage import HistoryReader, HistoryStore
from minenergy.opccollect.testing import fast_settings, free_port, make_config


async def test_backoff_is_capped_and_repeated_attempts_recorded(db_path):
    """无服务期内持续重连：退避间隔有上限，尝试事件持续产生且原因已知。"""
    port = free_port()
    config = make_config(
        f"opc.tcp://127.0.0.1:{port}",
        reconnect_backoff_initial_ms=50,
        reconnect_backoff_max_ms=200,
    )
    settings = fast_settings(config)
    settings.liveness_poll_ms = 50
    store = HistoryStore(db_path)
    collector = Collector(config, store, settings)
    task = asyncio.create_task(collector.run())

    # 先让它在"服务不存在"的状态下尝试约 2 秒
    await asyncio.sleep(2.0)
    with HistoryReader(db_path) as reader:
        attempts = [
            e for e in reader.connection_events()
            if e["kind"] == "connect_attempt_failed"
        ]
        assert len(attempts) >= 3, "应当发生多次有界退避后的重试"
        # 每次尝试都带已知原因（连接被拒绝），没有编造覆盖
        assert all(e["reason"] for e in attempts)

    # 现在把服务起在同端口，验证有界退避后能接上（不会因退避发散而永久失联）
    server = FixtureServer(port=port)
    await server.start()
    try:
        deadline = asyncio.get_event_loop().time() + 8.0
        while asyncio.get_event_loop().time() < deadline:
            with HistoryReader(db_path) as reader:
                connected = [
                    e for e in reader.connection_events()
                    if e["kind"] == "session_connected"
                ]
            if connected:
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("有界退避后未能在服务恢复时重新连上")
        collector.request_stop()
        await asyncio.wait_for(task, timeout=5.0)
    finally:
        await server.stop()
    store.close()

    with HistoryReader(db_path) as reader:
        seg = reader.segments()[-1]
        assert seg["end_kind"] == "completed"
        # 尝试事件数远小于"无退避风暴"的量级（2 秒内若每次立即重试会有数百次）
        assert len(reader.connection_events()) < 60


async def test_reconnect_resolves_fresh_nodeid_every_time(db_path):
    """每次重连都重新解析 URI，构造新 NodeId，绝不复用旧下标对象。"""
    port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{port}"
    config = make_config(endpoint)
    store = HistoryStore(db_path)
    collector = Collector(config, store, fast_settings(config))
    task = asyncio.create_task(collector.run())

    seen_nodeids: list[object] = []

    async def cycle(placeholders: int) -> None:
        server = FixtureServer(port=port, placeholder_count=placeholders)
        await server.start()
        try:
            # 等连接
            deadline = asyncio.get_event_loop().time() + 8.0
            while asyncio.get_event_loop().time() < deadline:
                if len(collector._bound) == len(config.nodes):
                    break
                await asyncio.sleep(0.05)
            # 抓取本轮绑定的 NodeId 副本
            seen_nodeids.append({
                alias: (nid.NamespaceIndex, nid.Identifier)
                for alias, nid in collector._bound.items()
            })
            await server.write_value("meter_total", 1000 + placeholders)
            await asyncio.sleep(0.4)
        finally:
            await server.stop()
        await asyncio.sleep(1.2)  # 等断线被观察到

    try:
        await cycle(0)
        await cycle(1)
        await cycle(3)
        collector.request_stop()
        await asyncio.wait_for(task, timeout=6.0)
    finally:
        store.close()

    # 三轮的 namespace 下标依次变化，但永久标识符不变
    assert len(seen_nodeids) == 3
    indexes = [round_["meter_total"][0] for round_ in seen_nodeids]
    identifiers = [round_["meter_total"][1] for round_ in seen_nodeids]
    assert indexes == [2, 3, 5]
    assert identifiers == [1001, 1001, 1001]


async def test_no_exactly_once_claim_in_segment_metadata(db_path):
    """段记录如实反映接收/提交计数；不声称服务器到采集机恰好一次投递。"""
    port = free_port()
    server = FixtureServer(port=port)
    await server.start()
    try:
        config = make_config(server.endpoint)
        store = HistoryStore(db_path)
        collector = Collector(config, store, fast_settings(config))
        task = asyncio.create_task(collector.run())
        await asyncio.sleep(1.0)
        await server.write_value("meter_total", 7)
        await asyncio.sleep(0.5)
        collector.request_stop()
        result = await asyncio.wait_for(task, timeout=5.0)
        store.close()
    finally:
        await server.stop()

    assert result.complete is True
    with HistoryReader(db_path) as reader:
        seg = reader.segments()[-1]
        # 只承诺"已接收且已提交"的计数一致，不提供任何 exactly-once 字段
        assert "exactly_once" not in seg and "delivery_guarantee" not in seg
        assert seg["received_count"] == seg["committed_count"]
