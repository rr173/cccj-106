"""拒绝案例：坏清册、操作编号碰撞、同刻矛盾、非法演练对象等。

每类坏数据都必须带对象/记录定位信息整体拒绝，不允许省略坏记录继续给答案。
"""
from __future__ import annotations

import pytest

from minenergy.gridtrace.errors import (
    LedgerValidationError,
    ModelValidationError,
    QueryError,
)
from minenergy.gridtrace.loader import ledger_from_dict, model_from_dict
from minenergy.gridtrace.scenario import (
    BASE_TIME, T1, T2, T4, build_ledger_dict, build_model_dict,
)
from minenergy.gridtrace.snapshot import Replay
from minenergy.gridtrace.tracing import drill, source_history, trace_routes


def _problems(excinfo):
    rendered = "\n".join(p.render() for p in excinfo.value.problems)
    return rendered


# ---------------------------------------------------------------------------
# 坏清册
# ---------------------------------------------------------------------------


def test_duplicate_id_in_roster_rejected():
    data = build_model_dict()
    data["nodes"].append({"id": "N1", "name": "重复接点"})
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    text = _problems(exc)
    assert "nodes[7].id" in text
    assert "重号" in text


def test_dangling_wire_both_endpoints_reported():
    data = build_model_dict()
    data["lines"].append({"id": "LX", "kind": "wire",
                          "endpoint_a": "N4", "endpoint_b": "NOPE"})
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    text = _problems(exc)
    assert "lines[8].endpoint_b" in text
    assert "悬空接线" in text and "NOPE" in text


def test_bad_enum_and_wire_initial_state():
    data = build_model_dict()
    data["lines"][0]["initial_state"] = "half_closed"  # 错误枚举
    data["lines"][2]["initial_state"] = "closed"       # 常接导线不允许有初态
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    text = _problems(exc)
    assert "lines[0].initial_state" in text
    assert "lines[2].initial_state" in text
    assert "常接导线没有初态" in text


def test_switch_without_initial_state_rejected():
    data = build_model_dict()
    del data["lines"][0]["initial_state"]
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    assert "lines[0].initial_state" in _problems(exc)


def test_naive_timestamp_without_offset_rejected():
    data = build_model_dict()
    data["base_time"] = "2026-01-01T00:00:00"  # 没有偏移
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    text = _problems(exc)
    assert "$.base_time" in text
    assert "缺少 UTC 偏移" in text


def test_meter_and_source_on_unknown_node_rejected():
    data = build_model_dict()
    data["meters"].append({"id": "MX", "node_id": "GHOST"})
    data["sources"][0]["node_id"] = "GHOST2"
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    text = _problems(exc)
    assert "meters[5].node_id" in text and "GHOST" in text
    assert "sources[0].node_id" in text and "GHOST2" in text


def test_unknown_field_and_missing_top_level_reported():
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict({"base_time": BASE_TIME, "nodes": [], "lines": [],
                         "sources": [], "meters": [], "extra": 1})
    text = _problems(exc)
    assert "$.extra" in text and "未知字段" in text

    with pytest.raises(ModelValidationError) as exc:
        model_from_dict({"nodes": [], "lines": [], "sources": [], "meters": []})
    assert "$.base_time" in _problems(exc)


def test_multiple_problems_aggregated_not_short_circuited():
    """一次载入尽量报出全部坏记录，而不是遇错即停。"""
    data = build_model_dict()
    data["nodes"].append({"id": "N1", "name": "重号"})
    data["lines"].append({"id": "BAD", "kind": "valve",
                          "endpoint_a": "X", "endpoint_b": "Y"})
    data["sources"].append({"id": "SX", "node_id": "Z",
                            "initial_state": "maybe"})
    with pytest.raises(ModelValidationError) as exc:
        model_from_dict(data)
    assert len(exc.value.problems) >= 4  # 重号 + 错误枚举 + 两个悬空 + 源枚举...


# ---------------------------------------------------------------------------
# 操作簿：编号碰撞 / 同刻矛盾 / 越界 / 非法目标
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def model():
    return model_from_dict(build_model_dict())


def test_op_id_reuse_with_changed_content_rejected(model):
    data = build_ledger_dict()
    data["operations"].append({
        "at_time": T2, "op_id": "OP21", "target_kind": "switch",
        "target_id": "E2", "new_state": "closed",  # OP21 原内容是 open
    })
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    text = _problems(exc)
    assert "op_id=OP21" in text
    assert "操作编号被复用且内容已修改" in text


def test_identical_op_id_retransmit_ignored(model):
    """完全一致的操作编号重传可忽略，操作簿仍可正常载入与回放。"""
    data = build_ledger_dict()  # 场景里已含一条 OP21 完全一致重传
    ledger = ledger_from_dict(data, model)
    same_id = [op for op in ledger.operations if op.op_id == "OP21"]
    assert len(same_id) == 1
    replay = Replay(model, ledger)
    from minenergy.gridtrace.timeutil import parse_offset_time
    t1 = parse_offset_time(T1, location="t")
    assert replay.at(t1).line_conducts("E2") is False


def test_same_instant_contradiction_lists_both_ops(model):
    data = {"operations": [
        {"at_time": T1, "op_id": "A1", "target_kind": "switch",
         "target_id": "E1", "new_state": "open"},
        {"at_time": T1, "op_id": "A2", "target_kind": "switch",
         "target_id": "E1", "new_state": "closed"},
    ]}
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    text = _problems(exc)
    assert "相互矛盾" in text
    assert "A1" in text and "A2" in text


def test_same_instant_different_targets_are_fine(model):
    """同刻对不同目标的动作合法，且整体作用（不制造临时失电）。"""
    data = {"operations": [
        {"at_time": T1, "op_id": "B1", "target_kind": "switch",
         "target_id": "E1", "new_state": "open"},
        {"at_time": T1, "op_id": "B2", "target_kind": "source",
         "target_id": "SA", "new_state": "retired"},
    ]}
    ledger = ledger_from_dict(data, model)
    replay = Replay(model, ledger)
    from minenergy.gridtrace.timeutil import parse_offset_time
    snap = replay.at(parse_offset_time(T1, location="t"))
    assert snap.line_conducts("E1") is False
    assert snap.source_is_engaged("SA") is False


def test_operation_before_base_time_rejected(model):
    data = {"operations": [
        {"at_time": "2025-12-31T23:59:59Z", "op_id": "EARLY",
         "target_kind": "switch", "target_id": "E1", "new_state": "open"},
    ]}
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    text = _problems(exc)
    assert "at_time" in text and "早于基准时刻" in text


def test_operation_targeting_wire_rejected(model):
    data = {"operations": [
        {"at_time": T1, "op_id": "W1", "target_kind": "switch",
         "target_id": "E3", "new_state": "open"},  # E3 是常接导线
    ]}
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    assert "常接导线" in _problems(exc)


def test_operation_unknown_target_and_bad_enum(model):
    data = {"operations": [
        {"at_time": T1, "op_id": "U1", "target_kind": "switch",
         "target_id": "NO_SWITCH", "new_state": "open"},
        {"at_time": T2, "op_id": "U2", "target_kind": "source",
         "target_id": "SA", "new_state": "paused"},
        {"at_time": T4, "op_id": "U3", "target_kind": "breaker",
         "target_id": "E1", "new_state": "closed"},
    ]}
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    text = _problems(exc)
    assert "NO_SWITCH" in text and "paused" in text and "breaker" in text


def test_naive_operation_time_rejected(model):
    data = {"operations": [
        {"at_time": "2026-01-01T01:00:00", "op_id": "N1",
         "target_kind": "switch", "target_id": "E1", "new_state": "open"},
    ]}
    with pytest.raises(LedgerValidationError) as exc:
        ledger_from_dict(data, model)
    assert "缺少 UTC 偏移" in _problems(exc)


# ---------------------------------------------------------------------------
# 查询与演练参数
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def replay(model):
    return Replay(model, ledger_from_dict(build_ledger_dict(), model))


def test_unknown_meter_query_rejected(replay):
    with pytest.raises(QueryError, match="不在挂表清册"):
        source_history(replay, "NO_METER", T1, T2)
    with pytest.raises(QueryError, match="不在挂表清册"):
        trace_routes(replay, "NO_METER", T1)


def test_query_before_base_rejected(replay):
    with pytest.raises(QueryError, match="早于基准时刻"):
        source_history(replay, "M3", "2025-12-31T00:00:00Z", T1)
    with pytest.raises(QueryError, match="早于基准时刻"):
        trace_routes(replay, "M3", "2025-12-31T00:00:00Z")
    with pytest.raises(QueryError, match="早于基准时刻"):
        drill(replay, "2025-12-31T00:00:00Z")


def test_inverted_history_window_rejected(replay):
    with pytest.raises(QueryError, match="start < end"):
        source_history(replay, "M3", T2, T1)
    with pytest.raises(QueryError, match="start < end"):
        source_history(replay, "M3", T1, T1)


def test_illegal_drill_objects_rejected(replay):
    with pytest.raises(QueryError, match="演练拔除的连线不存在"):
        drill(replay, T4, remove_lines=("GHOST_LINE",))
    with pytest.raises(QueryError, match="演练退出的源不存在"):
        drill(replay, T4, remove_sources=("GHOST_SRC",))
    with pytest.raises(QueryError):
        trace_routes(replay, "M3", T4, remove_lines=("NOPE",))


def test_duplicate_removal_ids_are_idempotent(replay):
    """同一对象重复列出不报错，按一次拔除处理。"""
    result = drill(replay, T4, remove_lines=("P1", "P1"))
    m5 = next(r for r in result.rows if r.meter_id == "M5")
    assert m5.after_sources == ("SA", "SB")  # P2 仍在
    assert result.removed_lines == ("P1",)


def test_bad_time_argument_type(replay):
    with pytest.raises(QueryError):
        source_history(replay, "M3", 1.5, T2)  # float 不接受
    with pytest.raises(QueryError):
        source_history(replay, "M3", None, T2)
