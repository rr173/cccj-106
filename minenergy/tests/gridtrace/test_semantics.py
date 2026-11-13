"""时间语义边界：基准当刻参与回放、同刻整体作用、半开区间归属。"""
from __future__ import annotations

from minenergy.gridtrace.loader import ledger_from_dict, model_from_dict
from minenergy.gridtrace.scenario import build_model_dict
from minenergy.gridtrace.snapshot import Replay
from minenergy.gridtrace.timeutil import parse_offset_time
from minenergy.gridtrace.tracing import source_history, trace_routes


def _us(text: str) -> int:
    return parse_offset_time(text, location="test")


def test_operation_exactly_at_base_time_is_applied():
    """基准时刻就是尚未应用当刻操作的初态；当刻记录要参与回放。"""
    model = model_from_dict(build_model_dict())
    ledger = ledger_from_dict({"operations": [
        {"at_time": "2026-01-01T00:00:00Z", "op_id": "BASE1",
         "target_kind": "source", "target_id": "SA", "new_state": "retired"},
    ]}, model)
    replay = Replay(model, ledger)
    base = _us("2026-01-01T00:00:00Z")
    # 基准当刻观察：操作已整体应用，SA 已退出
    assert replay.at(base).source_is_engaged("SA") is False
    # 无操作簿时初态不变（对照）
    replay0 = Replay(model, ledger_from_dict({"operations": []}, model))
    assert replay0.at(base).source_is_engaged("SA") is True


def test_same_instant_actions_apply_atomically_no_temporary_outage():
    """同刻断开唯一旧路、合上新路：任何时刻都观察不到临时失电。

    若按行顺序先断后合，M6 在两条记录之间会瞬间失去 SA；整体作用语义下
    来源集合在动作前后恒为 {SA, SB}。
    """
    model = model_from_dict(build_model_dict())
    # 反向排列：先出现“合新路”，再出现“断旧路”，验证与行序无关
    ledger = ledger_from_dict({"operations": [
        {"at_time": "2026-01-01T05:00:00Z", "op_id": "CLOSE",
         "target_kind": "switch", "target_id": "E7", "new_state": "closed"},
        {"at_time": "2026-01-01T05:00:00Z", "op_id": "OPEN",
         "target_kind": "switch", "target_id": "E1", "new_state": "open"},
    ]}, model)
    replay = Replay(model, ledger)
    t_before, t_after = _us("2026-01-01T04:59:00Z"), _us("2026-01-01T05:00:00Z")
    before = {r.source_id for r in trace_routes(replay, "M6", t_before)}
    after = {r.source_id for r in trace_routes(replay, "M6", t_after)}
    assert before == after == {"SA", "SB"}
    # 历史上该时刻不产生失电段，整段连贯
    segments = source_history(
        replay, "M6", "2026-01-01T04:00:00Z", "2026-01-01T06:00:00Z")
    assert all(seg.powered for seg in segments)
    assert len(segments) == 1  # 来源集合未变 -> 归并


def test_half_open_window_boundary_membership():
    """左含右不含：start 当刻动作在内，end 当刻动作不属本窗口。"""
    model = model_from_dict(build_model_dict())
    ledger = ledger_from_dict({"operations": [
        {"at_time": "2026-01-01T02:00:00Z", "op_id": "A",
         "target_kind": "source", "target_id": "SA", "new_state": "retired"},
        {"at_time": "2026-01-01T03:00:00Z", "op_id": "B",
         "target_kind": "source", "target_id": "SB", "new_state": "retired"},
    ]}, model)
    replay = Replay(model, ledger)
    segments = source_history(
        replay, "M3", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z")
    # 窗口内只含 02:00 的退 SA；03:00 的退 SB 落在右边界之外
    assert len(segments) == 1
    assert set(segments[0].sources) == {"SB"}
    assert (segments[0].start_us, segments[0].end_us) == (
        _us("2026-01-01T02:00:00Z"), _us("2026-01-01T03:00:00Z"))


def test_routing_at_end_boundary_includes_that_instant_operation():
    """追线时刻与操作同刻：当刻记录参与回放（区别于历史的右不含切窗）。"""
    model = model_from_dict(build_model_dict())
    ledger = ledger_from_dict({"operations": [
        {"at_time": "2026-01-01T05:00:00Z", "op_id": "OPEN",
         "target_kind": "switch", "target_id": "E1", "new_state": "open"},
        {"at_time": "2026-01-01T05:00:00Z", "op_id": "CLOSE",
         "target_kind": "switch", "target_id": "E7", "new_state": "closed"},
    ]}, model)
    replay = Replay(model, ledger)
    routes = {r.source_id: r.edge_ids
              for r in trace_routes(replay, "M6", "2026-01-01T05:00:00Z")}
    assert routes["SA"] == ("E7",)
