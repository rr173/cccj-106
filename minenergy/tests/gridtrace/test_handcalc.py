"""四个值班工单的手算验收（值全部来自人工推演，不取自模块输出）。"""
from __future__ import annotations

import pytest

from minenergy.gridtrace.loader import ledger_from_dict, model_from_dict
from minenergy.gridtrace.scenario import (
    T1, T2, T3, T4, T6, T7, build_ledger_dict, build_model_dict,
)
from minenergy.gridtrace.snapshot import Replay
from minenergy.gridtrace.timeutil import parse_offset_time
from minenergy.gridtrace.tracing import (
    CATEGORY_DEAD,
    CATEGORY_DEGRADED,
    CATEGORY_UNCHANGED,
    drill,
    source_history,
    trace_routes,
)

T_MID = "2026-01-01T05:30:00Z"
T_HALF = "2026-01-01T04:30:00Z"


def _us(text: str) -> int:
    return parse_offset_time(text, location="test")


@pytest.fixture(scope="module")
def replay():
    model = model_from_dict(build_model_dict())
    ledger = ledger_from_dict(build_ledger_dict(), model)
    return Replay(model, ledger)


def _sets(segments):
    return [(s.start_us, s.end_us, frozenset(s.sources), s.powered) for s in segments]


def test_workorder1_history_dual_retire_all_dead_restore(replay):
    """工单一：两源并供 -> 退 SA -> 全断 -> 同刻双源恢复（M3）。"""
    segments = source_history(replay, "M3", T1, T7)
    t1, t2, t3, t4, t7 = (_us(x) for x in (T1, T2, T3, T4, T7))
    expected = [
        (t1, t2, frozenset({"SA", "SB"}), True),   # T1 只断 E2，环仍双供
        (t2, t3, frozenset({"SB"}), True),          # 退 SA，SB 单供
        (t3, t4, frozenset(), False),               # 再退 SB，全断（失电段保留）
        (t4, t7, frozenset({"SA", "SB"}), True),    # 同刻双源恢复+合 E2
    ]
    assert _sets(segments) == expected
    # 时段连贯且不重叠，恰好铺满 [T1, T7)
    assert segments[0].start_us == t1
    assert segments[-1].end_us == t7
    for prev, nxt in zip(segments, segments[1:]):
        assert prev.end_us == nxt.start_us


def test_workorder1_isolated_meter_is_dead_not_absent(replay):
    """孤立表 M9 整段失电，但历史里必须有行，不能被当成不存在。"""
    segments = source_history(replay, "M9", T1, T7)
    assert len(segments) == 1
    seg = segments[0]
    assert seg.powered is False
    assert seg.sources == ()
    assert (seg.start_us, seg.end_us) == (_us(T1), _us(T7))


def test_workorder1_dual_supply_before_retirement(replay):
    """基准到 T1 之间 M1（挂公共接点 N1）双路并供，列出两个源。"""
    segments = source_history(replay, "M1", "2026-01-01T00:00:00Z", T1)
    assert len(segments) == 1
    assert set(segments[0].sources) == {"SA", "SB"}


def test_workorder2_same_instant_switchover_keeps_power(replay):
    """工单二：同刻断 E1 合 E6，来源集合不变 -> 历史归并为一段。"""
    segments = source_history(replay, "M6", T4, T6)
    t4, t6 = _us(T4), _us(T6)
    assert len(segments) == 1  # 换路但电源没变，不额外切行
    assert set(segments[0].sources) == {"SA", "SB"}
    assert (segments[0].start_us, segments[0].end_us) == (t4, t6)


def test_workorder2_trace_inside_merged_segment_differs(replay):
    """归并段内部追线：T5 前后 M6->SA 的实际走线不同，须按瞬间还原。"""
    before = {r.source_id: r.edge_ids for r in trace_routes(replay, "M6", T4)}
    middle = {r.source_id: r.edge_ids for r in trace_routes(replay, "M6", T_MID)}
    # T4（E1 合、E6 分）：M6(N4)->E2->N2->E1->N1(SA)
    assert before["SA"] == ("E2", "E1")
    # T5 同刻断 E1 合 E7 后：N4->E7->N1(SA)，最短即 [E7]
    assert middle["SA"] == ("E7",)
    # 到 SB 的最短路径不受 E1/E7 影响，两个瞬间一致（也是“归并但内部可能改线”的佐证）
    assert before["SB"] == middle["SB"]
    # 节点名与节点号一并给出
    sa_route = next(r for r in trace_routes(replay, "M6", T_MID) if r.source_id == "SA")
    assert sa_route.node_ids == ("N4", "N1")
    assert sa_route.node_names == ("4号接点", "1号母线")


def test_workorder3_equal_length_lexicographic_choice(replay):
    """工单三：M5 到 SA/SB 各有等长平行路径，P1 字典序胜出。"""
    routes = {r.source_id: r for r in trace_routes(replay, "M5", T_HALF)}
    # M5(N6)->N4 有 P1/P2 两条等长边；P1<P2，整条边号序列 P1 开头者胜
    assert routes["SA"].edge_ids[0] == "P1"
    assert routes["SB"].edge_ids[0] == "P1"
    assert routes["SA"].edge_ids == ("P1", "E2", "E1")
    # M5->SB 两条等长（3 边）：[P1,E2,E3] 与 [P1,E5,E4]，第二位 E2<E5
    assert routes["SB"].edge_ids == ("P1", "E2", "E3")


@pytest.mark.parametrize("removed,expected_first_edge", [
    (("P1",), "P2"),   # 拔掉 P1，备用 P2 仍供电
    (("P2",), "P1"),   # 拔掉 P2，主用 P1 不变
])
def test_workorder3_parallel_backup_one_by_one(replay, removed, expected_first_edge):
    """逐根拔平行线：单根拔除时 M5 不失电，自动走剩下的一根。"""
    result = drill(replay, T_HALF, remove_lines=removed)
    row = next(r for r in result.rows if r.meter_id == "M5")
    assert row.category == CATEGORY_UNCHANGED  # 来源集合不变（走备用连接）
    routes = trace_routes(replay, "M5", T_HALF, remove_lines=removed)
    assert routes[0].edge_ids[0] == expected_first_edge


def test_workorder3_pull_both_parallel_dead_branch(replay):
    """两根平行线一起拔掉，N6 支路完全失电。"""
    result = drill(replay, T_HALF, remove_lines=("P1", "P2"))
    m5 = next(r for r in result.rows if r.meter_id == "M5")
    assert m5.category == CATEGORY_DEAD
    assert m5.before_sources == ("SA", "SB")
    assert m5.after_sources == ()
    # 支路上游表 M6 不受影响
    m6 = next(r for r in result.rows if r.meter_id == "M6")
    assert m6.category == CATEGORY_UNCHANGED


def test_workorder4_three_categories(replay):
    """工单四：退 SB 且断 E2，逐表核对三类影响。"""
    result = drill(replay, T4, remove_lines=("E2",), remove_sources=("SB",))
    by_meter = {row.meter_id: row for row in result.rows}

    # M9 本就失电：演练前后来源集合都是空 -> 毫无变化
    assert by_meter["M9"].category == CATEGORY_UNCHANGED
    assert by_meter["M9"].before_sources == ()
    assert by_meter["M9"].after_sources == ()

    # M1 在 N1（SA 侧）：SB 退出后只剩 SA -> 少了来源
    assert by_meter["M1"].category == CATEGORY_DEGRADED
    assert by_meter["M1"].before_sources == ("SA", "SB")
    assert by_meter["M1"].after_sources == ("SA",)

    # M5/M6 所在的 N4 侧被 E2 断开且 SB 退出，环上仍经 E5-E4-N3... 但 SB 已退，
    # N4 经 N5-N3-N2? N3-N2 是常接导线 E3，N2->N1 的 E1 仍合 -> SA 可达
    for meter_id in ("M5", "M6", "M3"):
        row = by_meter[meter_id]
        assert row.category == CATEGORY_DEGRADED
        assert row.after_sources == ("SA",)

    # 三类至少各出现一次（dead：构造一个全断演练补充核对）
    all_dead = drill(replay, T4, remove_sources=("SA", "SB"))
    assert all(row.category == CATEGORY_DEAD for row in all_dead.rows
               if row.meter_id != "M9")
    # M9 前后皆空，仍是 unchanged
    assert next(r for r in all_dead.rows if r.meter_id == "M9").category \
        == CATEGORY_UNCHANGED


def test_workorder4_drill_does_not_pollute_ledger(replay):
    """演练后重复正常追溯，必须得到与演练前完全相同的原答案。"""
    before = source_history(replay, "M3", T1, T7)
    drill(replay, T4, remove_lines=("E1", "E2", "E4", "P1", "P2"),
          remove_sources=("SA", "SB"))
    drill(replay, T2, remove_sources=("SB",))
    after = source_history(replay, "M3", T1, T7)
    assert _sets(after) == _sets(before)
    # 正式快照里 E2 在 T4 仍是闭合（演练没有改操作簿）
    assert replay.at(_us(T4)).line_conducts("E2") is True


def test_routes_sorted_by_source_and_cover_all_reachable(replay):
    routes = trace_routes(replay, "M3", T_HALF)
    assert [r.source_id for r in routes] == ["SA", "SB"]
    # M3(N5)->SA 最短：E3 是 N2-N3 常接线，走 N5-E4-N3-E3-N2-E1-N1
    sa = next(r for r in routes if r.source_id == "SA")
    assert sa.edge_ids == ("E4", "E3", "E1")
    sb = next(r for r in routes if r.source_id == "SB")
    assert sb.edge_ids == ("E4",)  # N5->E4->N3(SB)
