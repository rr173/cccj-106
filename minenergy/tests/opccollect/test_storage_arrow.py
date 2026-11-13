"""SQLite 存储与 Arrow 读出：精度、字节、空值边界、跨进程契约恢复。"""
from __future__ import annotations

import pyarrow as pa
import pytest

from minenergy.opccollect.contract import NodeContract
from minenergy.opccollect.storage import (
    HistoryReader,
    HistoryStore,
    NotificationRow,
    build_record_batch,
    encode_value,
    decode_value,
)
from minenergy.opccollect.server_fixture import BUSINESS_URI

URI = BUSINESS_URI


def _contract(name, vtype, ident=1):
    return NodeContract(name, URI, ident, vtype)


def _row(segment_id, contract, seq, *, value, status=0, usable=True,
         source_us=1000, null=False):
    column, raw = encode_value(contract, value)
    return NotificationRow(
        alias=contract.alias,
        segment_id=segment_id,
        node_seq=seq,
        arrival_seq=seq,
        arrival_us=2000 + seq,
        source_us=source_us,
        server_us=3000 + seq,
        status_code=status,
        status_name="Good" if status == 0 else "BadX",
        usable=usable,
        value_is_null=null,
        value_column=column,
        value_raw=raw,
    )


@pytest.fixture
def store(tmp_path):
    s = HistoryStore(tmp_path / "h.sqlite")
    seg = s.begin_segment("opc.tcp://x", "{}")
    yield s, seg.segment_id
    s.close()


def test_int64_precision_and_blob_bytes_roundtrip(store):
    s, sid = store
    big = 2**62 + 17
    neg = -(2**62) - 5
    c_int = _contract("i", "Int64")
    c_blob = _contract("b", "ByteString", ident=2)
    s.register_contracts((c_int, c_blob), sid)
    blob = bytes(range(256))
    s.insert_notifications([
        _row(sid, c_int, 1, value=big),
        _row(sid, c_int, 2, value=neg),
        _row(sid, c_blob, 1, value=blob),
        _row(sid, c_blob, 2, value=b""),
    ])
    with HistoryReader(s.path) as reader:
        ints = [r["value"] for r in reader.iter_rows("i")]
        assert ints == [big, neg]  # 整数精度不丢
        blobs = [r["value"] for r in reader.iter_rows("b")]
        assert blobs == [blob, b""]  # 全字节域内容保留


def test_null_value_columns_are_typed_arrow_nulls(store):
    s, sid = store
    contracts = (
        _contract("i", "Int64", ident=1),
        _contract("d", "Double", ident=2),
        _contract("s", "String", ident=3),
        _contract("bs", "ByteString", ident=4),
        _contract("ia", "Int64_array", ident=5),
    )
    s.register_contracts(contracts, sid)
    rows = [
        _row(sid, contracts[0], 1, value=None, null=True, source_us=None),
        _row(sid, contracts[1], 1, value=None, null=True, source_us=None),
        _row(sid, contracts[2], 1, value=None, null=True, source_us=None),
        _row(sid, contracts[3], 1, value=None, null=True, source_us=None),
        _row(sid, contracts[4], 1, value=None, null=True, source_us=None),
    ]
    s.insert_notifications(rows)
    with HistoryReader(s.path) as reader:
        for contract in contracts:
            batches = list(reader.read_record_batches(contract.alias))
            table = pa.Table.from_batches(batches)
            assert table.schema.field("value").type == contract.arrow_type
            assert table.column("value").to_pylist() == [None]
            row = next(reader.iter_rows(contract.alias))
            assert row["value"] is None and row["value_is_null"] is True
            assert row["source_us"] is None  # 设备时间缺失不回填


def test_empty_array_distinct_from_null_and_null_elements(store):
    s, sid = store
    c_tags = _contract("tags", "String_array", ident=6)
    c_blobs = _contract("blobs", "ByteString_array", ident=7)
    c_ints = _contract("ints", "Int64_array", ident=8)
    s.register_contracts((c_tags, c_blobs, c_ints), sid)
    s.insert_notifications([
        _row(sid, c_tags, 1, value=[]),
        _row(sid, c_tags, 2, value=["a", None, "c"]),
        _row(sid, c_tags, 3, value=None, null=True),
        _row(sid, c_blobs, 1, value=[b"x", None, b"\x00\xff"]),
        _row(sid, c_ints, 1, value=[]),
        _row(sid, c_ints, 2, value=[1, 2, 3]),
    ])
    with HistoryReader(s.path) as reader:
        tag_rows = list(reader.iter_rows("tags"))
        assert tag_rows[0]["value"] == [] and tag_rows[0]["value_is_null"] is False
        assert tag_rows[1]["value"] == ["a", None, "c"]
        assert tag_rows[2]["value"] is None and tag_rows[2]["value_is_null"] is True

        blob_rows = list(reader.iter_rows("blobs"))
        assert blob_rows[0]["value"] == [b"x", None, b"\x00\xff"]

        # Arrow 端到端类型与空值
        table = pa.Table.from_batches(list(reader.read_record_batches("tags")))
        assert table.schema.field("value").type == pa.list_(pa.string())
        assert table.column("value").to_pylist() == [
            [], ["a", None, "c"], None,
        ]


def test_seq_range_filter_and_batching(store):
    s, sid = store
    c = _contract("i", "Int64")
    s.register_contracts((c,), sid)
    s.insert_notifications([_row(sid, c, k, value=k) for k in range(1, 11)])
    with HistoryReader(s.path) as reader:
        assert reader.seq_bounds("i") == (1, 10)
        batches = list(reader.read_record_batches("i", 3, 7, batch_size=2))
        assert len(batches) == 3  # 5 行，批大小 2 -> 2+2+1
        values = pa.Table.from_batches(batches).column("node_seq").to_pylist()
        assert values == [3, 4, 5, 6, 7]


def test_contracts_recoverable_in_new_reader(store):
    s, sid = store
    c_num = _contract("num", "Double")
    c_str = _contract("str", "String_array", ident="TAGS")
    s.register_contracts((c_num, c_str), sid)
    s.insert_notifications([
        _row(sid, c_num, 1, value=1.5),
        _row(sid, c_str, 1, value=["x"]),
    ])
    # 模拟新进程：全新的只读连接对象
    with HistoryReader(s.path) as fresh:
        recovered = {c.alias: c for c in fresh.contracts()}
        assert recovered["num"].value_type == "Double"
        assert recovered["num"].identifier == 1
        assert recovered["str"].identifier == "TAGS"
        assert recovered["str"].namespace_uri == URI
        # 导出的 RecordBatch metadata 对应配置里的永久身份
        batch = next(fresh.read_record_batches("str"))
        meta = batch.schema.metadata
        assert meta[b"opccollect.namespace_uri"] == URI.encode()
        assert meta[b"opccollect.identifier"] == b"string:TAGS"


def test_segment_running_left_on_abnormal_disappearance(tmp_path):
    """未正常结束的段保持 running（进程被杀语义），不被伪造成 completed。"""
    path = tmp_path / "h.sqlite"
    s = HistoryStore(path)
    s.begin_segment("opc.tcp://x", "{}")
    s.close()  # 故意不调用 end_segment
    with HistoryReader(path) as reader:
        only = reader.segments()[0]
        assert only["end_kind"] == "running"
        assert only["ended_us"] is None


def test_encode_decode_byte_string_array_framing():
    c = _contract("blobs", "ByteString_array")
    values = [b"", b"abc", None, bytes(range(10))]
    column, raw = encode_value(c, values)
    assert column == "value_blob"
    assert decode_value(c, column, raw) == values
    # 空数组与 None 的编码不同
    _, empty_raw = encode_value(c, [])
    assert decode_value(c, column, empty_raw) == []
    _, none_raw = encode_value(c, None)
    assert none_raw is None


def test_build_batch_carries_metadata_and_column_order(store):
    s, sid = store
    c = _contract("i", "Int64")
    s.register_contracts((c,), sid)
    batch = build_record_batch(c, [
        {
            "alias": "i", "node_seq": 1, "arrival_seq": 1, "arrival_us": 2,
            "source_us": None, "server_us": 3, "status_code": 0,
            "status_name": "Good", "usable": True, "value_is_null": False,
            "value": 42,
        }
    ])
    assert batch.schema.names == [
        "alias", "node_seq", "arrival_seq", "arrival_us", "source_us",
        "server_us", "status_code", "status_name", "usable",
        "value_is_null", "value",
    ]
    assert batch.schema.metadata[b"opccollect.alias"] == b"i"
