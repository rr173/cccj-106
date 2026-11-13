"""随机接线/操作组合上的独立对照验收。

对照程序 ``oracle.py`` 自行回放、自行 DFS 枚举全部简单路径，不调用模块
自身的图搜索或回放函数；本文件逐字段比对三种业务操作的答案。
"""
from __future__ import annotations

import inspect
import random
from pathlib import Path

import pytest

from minenergy.gridtrace import oracle
from minenergy.gridtrace.loader import ledger_from_dict, model_from_dict
from minenergy.gridtrace.randomgen import build_random_case
from minenergy.gridtrace.scenario import BASE_TIME
from minenergy.gridtrace.snapshot import Replay
from minenergy.gridtrace.timeutil import parse_offset_time
from minenergy.gridtrace.tracing import drill, source_history, trace_routes

BASE_US = parse_offset_time(BASE_TIME, location="base")
MINUTE_US = 60_000_000


def test_oracle_is_independent_of_module_search_and_replay():
    """对照程序源码不得 import 模块自身的图搜索/回放/追线实现。"""
    source = Path(inspect.getsourcefile(oracle)).read_text(encoding="utf-8")
    for forbidden in ("from minenergy", "import minenergy", "from .graph",
                      "from .snapshot", "from .tracing", "from .model"):
        assert forbidden not in source, f"oracle 出现禁用依赖：{forbidden}"


@pytest.mark.parametrize("seed", range(60))
def test_three_operations_match_oracle_on_random_cases(seed):
    rng = random.Random(20260113 + seed)
    model_data, ledger_data = build_random_case(rng)

    # 模块答案
    model = model_from_dict(model_data)
    ledger = ledger_from_dict(ledger_data, model)
    replay = Replay(model, ledger)

    meter_ids = list(model.meters)
    line_ids = list(model.lines)
    source_ids = list(model.sources)

    # ---- 1. 历史：随机 [start, end) 窗口，逐段对照 ----------------------
    for _ in range(3):
        meter_id = rng.choice(meter_ids)
        start_min = rng.randint(0, 40)
        end_min = rng.randint(start_min + 1, 61)
        start_us = BASE_US + start_min * MINUTE_US
        end_us = BASE_US + end_min * MINUTE_US
        got = source_history(replay, meter_id, start_us, end_us)
        want = oracle.oracle_history(
            model_data, ledger_data, meter_id, start_us, end_us)
        got_flat = [(s.start_us, s.end_us, frozenset(s.sources)) for s in got]
        assert got_flat == want, f"seed={seed} meter={meter_id} 历史不一致"
        # 连贯不重叠且铺满窗口
        assert got_flat[0][0] == start_us and got_flat[-1][1] == end_us
        for prev, nxt in zip(got_flat, got_flat[1:]):
            assert prev[1] == nxt[0]

    # ---- 2. 追线：随机时刻（含非操作整点）逐源路线对照 ------------------
    for _ in range(4):
        meter_id = rng.choice(meter_ids)
        at_us = BASE_US + rng.randint(0, 60) * MINUTE_US
        got_routes = {r.source_id: r.edge_ids
                      for r in trace_routes(replay, meter_id, at_us)}
        want_routes = oracle.oracle_routes(
            model_data, ledger_data, meter_id, at_us)
        assert set(got_routes) == set(want_routes), f"seed={seed} 可达源集合不一致"
        for sid, (want_edges, want_nodes) in want_routes.items():
            assert got_routes[sid] == want_edges, (
                f"seed={seed} meter={meter_id} source={sid} 路线不一致")
            # 模块给出的节点序列必须与边序列自洽（独立再核一遍邻接关系）
            route = next(r for r in trace_routes(replay, meter_id, at_us)
                         if r.source_id == sid)
            assert len(route.node_ids) == len(route.edge_ids) + 1
            for edge_id, a, b in zip(route.edge_ids, route.node_ids,
                                     route.node_ids[1:]):
                line = model.lines[edge_id]
                assert {line.endpoint_a, line.endpoint_b} == {a, b}
            assert route.node_ids[0] == model.meters[meter_id].node_id
            assert route.node_ids[-1] == model.sources[sid].node_id

    # ---- 3. 演练：随机拔线/退源，三分类与前后来源集合对照 ---------------
    for _ in range(4):
        at_us = BASE_US + rng.randint(0, 60) * MINUTE_US
        rm_lines = frozenset(rng.sample(line_ids, rng.randint(0, len(line_ids))))
        rm_sources = frozenset(rng.sample(source_ids, rng.randint(
            0, len(source_ids))))
        got = drill(replay, at_us,
                    remove_lines=tuple(rm_lines), remove_sources=tuple(rm_sources))
        want = oracle.oracle_drill(
            model_data, ledger_data, at_us, rm_lines, rm_sources)
        assert {row.meter_id for row in got.rows} == set(want)
        for row in got.rows:
            w_before, w_after, w_category = want[row.meter_id]
            assert frozenset(row.before_sources) == w_before
            assert frozenset(row.after_sources) == w_after
            assert row.category == w_category

        # 演练瞬间的追线同样要与带拔除条件的 oracle 路线一致
        if meter_ids:
            meter_id = rng.choice(meter_ids)
            got_routes = {r.source_id: r.edge_ids
                          for r in trace_routes(
                              replay, meter_id, at_us,
                              remove_lines=tuple(rm_lines),
                              remove_sources=tuple(rm_sources))}
            want_routes = oracle.oracle_routes(
                model_data, ledger_data, meter_id, at_us,
                rm_lines, rm_sources)
            assert set(got_routes) == set(want_routes)
            for sid, (want_edges, _nodes) in want_routes.items():
                assert got_routes[sid] == want_edges


def test_drill_never_mutates_replay_state():
    """随机演练后正式快照与历史答案保持不变（与无演练时逐字段相等）。"""
    rng = random.Random(424242)
    model_data, ledger_data = build_random_case(rng)
    model = model_from_dict(model_data)
    ledger = ledger_from_dict(ledger_data, model)
    replay = Replay(model, ledger)

    baseline = {
        mid: source_history(replay, mid, BASE_US, BASE_US + 61 * MINUTE_US)
        for mid in model.meters
    }
    for _ in range(10):
        drill(
            replay, BASE_US + rng.randint(0, 60) * MINUTE_US,
            remove_lines=tuple(rng.sample(list(model.lines),
                                          rng.randint(0, len(model.lines)))),
            remove_sources=tuple(rng.sample(list(model.sources),
                                            rng.randint(0, len(model.sources)))),
        )
    for mid, segments in baseline.items():
        again = source_history(replay, mid, BASE_US, BASE_US + 61 * MINUTE_US)
        assert [(s.start_us, s.end_us, s.sources, s.powered) for s in again] == \
               [(s.start_us, s.end_us, s.sources, s.powered) for s in segments]
