"""核心接收语义：真实 OPC UA 协议、编号、质量、空值与 Arrow 类型。"""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import pyarrow as pa
import pytest
from asyncua import ua

from minenergy.opccollect.collector import Collector
from minenergy.opccollect.contract import resolve_namespace_index
from minenergy.opccollect.server_fixture import BUSINESS_URI
from minenergy.opccollect.storage import HistoryReader, HistoryStore
from minenergy.opccollect.testing import fast_settings, make_config


async def _run_collector_for(config, db_path, run_ms=1500, **settings):
    store = HistoryStore(db_path)
    base = fast_settings(config)
    for key, value in settings.items():
        setattr(base, key, value)
    collector = Collector(config, store, base)

    async def _stop_timer() -> None:
        await asyncio.sleep(run_ms / 1000.0)
        collector.request_stop()

    timer = asyncio.create_task(_stop_timer())
    try:
        result = await collector.run()
    finally:
        timer.cancel()
        store.close()
    return result


async def test_real_protocol_basic_types_and_arrival_order(
    fixture_server, db_path
):
    config = make_config(fixture_server.endpoint)
    server = fixture_server

    async def drive() -> None:
        # 等订阅建立（首次连接含初始值通知）
        await asyncio.sleep(0.4)
        await server.write_value("meter_total", 100)
        await server.write_value("voltage", 230.5)
        await server.write_value("running", False)
        await server.write_value("device_name", "PUMP-A")
        await server.write_value("raw_frame", base64.b64encode(b"\x00\xff\x10").decode())
        await server.write_value("waveform", [10, -20, 30])
        await server.write_value("doubles", [1.25, 2.5])
        await server.write_value("flags", [True, False, True])
        await server.write_value("tags", ["a", None, "c"])
        await server.write_value(
            "blobs", [base64.b64encode(b"x").decode(), None,
                      base64.b64encode(b"\x00").decode()]
        )

    driver = asyncio.create_task(drive())
    result = await _run_collector_for(config, db_path, run_ms=1600)
    await driver

    assert result.end_kind == "completed"
    assert result.received_count == result.committed_count
    assert result.connection_count == 1
    assert result.disconnect_count == 0

    with HistoryReader(db_path) as reader:
        # 每个节点都有初始值 + 一次更新
        assert reader.count("meter_total") >= 2
        meter = list(reader.iter_rows("meter_total"))
        assert meter[-1]["value"] == 100
        assert isinstance(meter[-1]["value"], int)
        assert meter[-1]["usable"] is True
        # 接收编号连续且按到达顺序（不按源时间排序）
        seqs = [r["node_seq"] for r in meter]
        assert seqs == list(range(1, len(seqs) + 1))
        assert [r["arrival_us"] for r in meter] == sorted(
            r["arrival_us"] for r in meter
        )

        # Arrow 类型由契约决定
        batches = list(reader.read_record_batches("meter_total"))
        table = pa.Table.from_batches(batches)
        assert table.schema.field("value").type == pa.int64()
        assert table.schema.metadata[b"opccollect.namespace_uri"] == BUSINESS_URI.encode()
        assert table.schema.metadata[b"opccollect.identifier"] == b"numeric:1001"

        vtable = pa.Table.from_batches(
            list(reader.read_record_batches("voltage"))
        )
        assert vtable.schema.field("value").type == pa.float64()
        assert vtable.column("value").to_pylist()[-1] == pytest.approx(230.5)

        btable = pa.Table.from_batches(
            list(reader.read_record_batches("raw_frame"))
        )
        assert btable.schema.field("value").type == pa.binary()
        assert btable.column("value").to_pylist()[-1] == b"\x00\xff\x10"

        wtable = pa.Table.from_batches(
            list(reader.read_record_batches("waveform"))
        )
        assert wtable.schema.field("value").type == pa.list_(pa.int64())
        assert wtable.column("value").to_pylist()[-1] == [10, -20, 30]

        # String 数组的 null 元素保留
        ttable = pa.Table.from_batches(list(reader.read_record_batches("tags")))
        assert ttable.schema.field("value").type == pa.list_(pa.string())
        assert ttable.column("value").to_pylist()[-1] == ["a", None, "c"]

        # ByteString 数组的字节内容与 null 元素
        bstable = pa.Table.from_batches(list(reader.read_record_batches("blobs")))
        assert bstable.schema.field("value").type == pa.list_(pa.binary())
        assert bstable.column("value").to_pylist()[-1] == [b"x", None, b"\x00"]


async def test_bad_quality_is_kept_not_dropped_or_zeroed(
    fixture_server, db_path
):
    """非 Good 读数保留原质量码并标为不可用，不丢弃、不改写 0。

    OPC UA Part 4 §7.7.1：Bad 状态码到达时服务器按规范把值置空（Null
    Variant）；Uncertain 不属于 Bad，值原样保留。两种形态都要留痕。
    """
    config = make_config(fixture_server.endpoint)
    server = fixture_server
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bad_ts = ts + timedelta(seconds=1)

    async def drive() -> None:
        await asyncio.sleep(0.4)
        # Uncertain：值保留，质量非 Good
        await server.write_data_value(
            "meter_total", 555,
            status_name="Uncertain", source_ts=ts,
        )
        await asyncio.sleep(0.15)
        # Bad：按 OPC UA 规范值被置空，但质量码原样到达
        await server.write_data_value(
            "meter_total", 777,
            status_name="BadSensorFailure", source_ts=bad_ts,
        )

    driver = asyncio.create_task(drive())
    result = await _run_collector_for(config, db_path, run_ms=1600)
    await driver

    assert result.end_kind == "completed"
    with HistoryReader(db_path) as reader:
        rows = list(reader.iter_rows("meter_total"))
        non_good = [r for r in rows if not r["usable"]]
        assert len(non_good) == 2

        uncertain = next(
            r for r in non_good
            if r["status_code"] == ua.StatusCodes.Uncertain
        )
        assert uncertain["value"] == 555  # Uncertain 原值保留，不改写 0
        assert uncertain["value_is_null"] is False
        assert uncertain["status_name"] == "Uncertain"

        bad = next(
            r for r in non_good
            if r["status_code"] == ua.StatusCodes.BadSensorFailure
        )
        # Bad 按 OPC UA 规范以 Null 形态到达：值为空但原质量码完整保留
        assert bad["value"] is None
        assert bad["value_is_null"] is True
        assert bad["status_name"] == "BadSensorFailure"
        assert bad["source_us"] == int(bad_ts.timestamp() * 1_000_000)

        # 非 Good 行没有被改写成 0
        assert all(r["value"] != 0 for r in non_good)
        # 坏质量读数与"连接断开没收到"可区分：这些行都有到达时间与质量码
        assert all(r["arrival_us"] > 0 for r in non_good)


async def test_missing_device_time_stays_null_not_backfilled(
    fixture_server, db_path
):
    config = make_config(fixture_server.endpoint)
    server = fixture_server

    async def drive() -> None:
        await asyncio.sleep(0.4)
        # 数值标量的设备无值：Null 型 Variant + BadNoData，且不给源时间
        await server.write_data_value(
            "meter_total", None, status_name="BadNoData"
        )
        # String 标量的类型化 null（Good 质量，协议允许）
        await server.write_value("device_name", None)

    driver = asyncio.create_task(drive())
    result = await _run_collector_for(config, db_path, run_ms=1500)
    await driver

    assert result.end_kind == "completed"
    with HistoryReader(db_path) as reader:
        null_rows = [
            r for r in reader.iter_rows("meter_total") if r["value_is_null"]
        ]
        assert null_rows, "expected at least one null meter value"
        row = null_rows[-1]
        assert row["value"] is None
        assert row["source_us"] is None  # 设备时间缺失保持空值
        assert row["server_us"] is None or isinstance(row["server_us"], int)
        assert row["arrival_us"] > 0  # 接收时间独立存在
        assert row["usable"] is False

        name_rows = list(reader.iter_rows("device_name"))
        null_names = [r for r in name_rows if r["value_is_null"]]
        assert null_names[-1]["value"] is None
        assert null_names[-1]["usable"] is True


async def test_same_source_time_different_values_both_kept(
    fixture_server, db_path
):
    config = make_config(fixture_server.endpoint)
    server = fixture_server
    ts = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    async def drive() -> None:
        await asyncio.sleep(0.4)
        await server.write_data_value(
            "meter_total", 111, status_name="Good", source_ts=ts
        )
        await asyncio.sleep(0.1)
        await server.write_data_value(
            "meter_total", 222, status_name="Good", source_ts=ts
        )

    driver = asyncio.create_task(drive())
    await _run_collector_for(config, db_path, run_ms=1600)
    await driver

    with HistoryReader(db_path) as reader:
        rows = list(reader.iter_rows("meter_total"))
        same_ts = [r for r in rows if r["source_us"] == int(ts.timestamp() * 1e6)]
        assert len(same_ts) == 2
        assert {r["value"] for r in same_ts} == {111, 222}
        # 两条到达编号不同且顺序保留
        assert same_ts[0]["node_seq"] < same_ts[1]["node_seq"]
        assert same_ts[0]["arrival_seq"] != same_ts[1]["arrival_seq"]


async def test_empty_arrays_distinct_from_null(fixture_server, db_path):
    config = make_config(fixture_server.endpoint)
    server = fixture_server

    async def drive() -> None:
        await asyncio.sleep(0.4)
        await server.write_value("waveform", [])
        await server.write_value("waveform", [1])
        await server.write_value("tags", [])

    driver = asyncio.create_task(drive())
    await _run_collector_for(config, db_path, run_ms=1500)
    await driver

    with HistoryReader(db_path) as reader:
        rows = list(reader.iter_rows("waveform"))
        # 初始空数组 + 再写空数组 + 非空
        empties = [r for r in rows if r["value"] == []]
        assert empties and all(r["value_is_null"] is False for r in empties)
        assert [1] in [r["value"] for r in rows]
        tag_rows = list(reader.iter_rows("tags"))
        assert any(r["value"] == [] and not r["value_is_null"] for r in tag_rows)


async def test_namespace_resolution_uses_uri_not_index(fixture_server):
    ns_array = await fixture_server.namespace_array()
    idx = resolve_namespace_index(ns_array, BUSINESS_URI)
    assert ns_array[idx] == BUSINESS_URI
    # 下标应与夹具注册结果一致（但调用方只依赖 URI）
    assert idx == fixture_server.business_namespace_index
