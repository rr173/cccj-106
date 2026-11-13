"""独立对照程序（oracle）。

随机接线与随机操作组合的验收要求一份**不调用模块自身图搜索或回放函数**
的独立答案。本模块刻意不 import 任何 ``minenergy.gridtrace`` 子模块：

* 输入直接吃原始 JSON 风格的 ``dict``（模型与操作簿各一份），自己解析；
* 自己按发生时刻回放开关/源状态（同刻整体应用）；
* 自己枚举全部**简单路径**做连通性与最优路径判定（小图可枚举）；
* 三类业务操作的答案全部由这些原语重新推出。

除标准库外无任何依赖。
"""
from __future__ import annotations

from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 时刻解析（自含，不复用 gridtrace.timeutil）
# ---------------------------------------------------------------------------


def parse_time(value: str) -> int:
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError("朴素时刻不接受")
    dt = dt.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch
    return delta.days * 86_400 * 1_000_000 + delta.seconds * 1_000_000 + delta.microseconds


# ---------------------------------------------------------------------------
# 自含图结构与回放
# ---------------------------------------------------------------------------


class OracleModel:
    def __init__(self, model_data: dict):
        self.base_us = parse_time(model_data["base_time"])
        self.node_names = {n["id"]: n["name"] for n in model_data["nodes"]}
        self.lines = {}
        for line in model_data["lines"]:
            self.lines[line["id"]] = (
                line["kind"], line["endpoint_a"], line["endpoint_b"],
                line.get("initial_state"),
            )
        self.source_node = {s["id"]: s["node_id"] for s in model_data["sources"]}
        self.source_initial = {
            s["id"]: s["initial_state"] == "engaged" for s in model_data["sources"]
        }
        self.meter_node = {m["id"]: m["node_id"] for m in model_data["meters"]}

        self._adj: dict[str, list[tuple[str, str]]] = {nid: [] for nid in self.node_names}
        for edge_id, (kind, a, b, _state) in self.lines.items():
            self._adj[a].append((edge_id, b))
            self._adj[b].append((edge_id, a))

    def neighbors(self, node_id: str, removed_lines: frozenset[str]):
        for edge_id, other in self._adj[node_id]:
            if edge_id not in removed_lines:
                yield edge_id, other


def replay_states(model: OracleModel, ledger_data: dict) -> dict[int, dict]:
    """按发生时刻回放，返回 instant -> {'switch': {id: closed_bool},
    'source': {id: engaged_bool}}（仅含发生动作的时刻，外加 base 键）。"""
    switch_state = {
        edge_id: state == "closed"
        for edge_id, (kind, _a, _b, state) in model.lines.items()
        if kind == "switch"
    }
    source_state = dict(model.source_initial)
    timeline: dict[int, dict] = {
        model.base_us: {"switch": dict(switch_state), "source": dict(source_state)}
    }
    groups: dict[int, list[dict]] = {}
    for rec in ledger_data["operations"]:
        groups.setdefault(parse_time(rec["at_time"]), []).append(rec)
    for at_us in sorted(groups):
        # 同刻动作整体作用：先在副本上全部改完，再登记快照
        next_switch = dict(switch_state)
        next_source = dict(source_state)
        for rec in groups[at_us]:
            if rec["target_kind"] == "switch":
                next_switch[rec["target_id"]] = rec["new_state"] == "closed"
            else:
                next_source[rec["target_id"]] = rec["new_state"] == "engaged"
        switch_state, source_state = next_switch, next_source
        timeline[at_us] = {"switch": dict(switch_state), "source": dict(source_state)}
    return timeline


def state_at(model: OracleModel, timeline: dict[int, dict], at_us: int) -> dict:
    chosen = timeline[model.base_us]
    for instant in sorted(timeline):
        if instant <= at_us:
            chosen = timeline[instant]
        else:
            break
    return chosen


def _edge_conducts(model: OracleModel, edge_id: str, states: dict) -> bool:
    kind = model.lines[edge_id][0]
    if kind == "wire":
        return True
    return states["switch"].get(edge_id, False)


# ---------------------------------------------------------------------------
# 简单路径枚举（小图可全枚举）——独立于模块自身的 BFS
# ---------------------------------------------------------------------------


def all_simple_paths(model: OracleModel, start: str, target: str, states: dict,
                     removed_lines: frozenset[str]
                     ) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """DFS 枚举 start -> target 的全部简单路径，返回 (边号序列, 节点序列)。"""
    results: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

    def dfs(node: str, visited: set[str], edges: list[str], nodes: list[str]) -> None:
        if node == target:
            results.append((tuple(edges), tuple(nodes)))
            return
        for edge_id, other in model.neighbors(node, removed_lines):
            if other in visited:
                continue
            if not _edge_conducts(model, edge_id, states):
                continue
            visited.add(other)
            dfs(other, visited, edges + [edge_id], nodes + [other])
            visited.remove(other)

    dfs(start, {start}, [], [start])
    return results


def reachable_sources(model: OracleModel, node_id: str, states: dict,
                      removed_lines: frozenset[str],
                      removed_sources: frozenset[str]) -> frozenset[str]:
    """自己的 DFS 连通分量 + 投入源判定。"""
    seen = {node_id}
    stack = [node_id]
    while stack:
        cur = stack.pop()
        for edge_id, other in model.neighbors(cur, removed_lines):
            if other in seen or not _edge_conducts(model, edge_id, states):
                continue
            seen.add(other)
            stack.append(other)
    return frozenset(
        sid for sid, snode in model.source_node.items()
        if snode in seen and states["source"].get(sid, False) and sid not in removed_sources
    )


def best_path(model: OracleModel, start: str, target: str, states: dict,
              removed_lines: frozenset[str]
              ) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """枚举全部简单路径后按 (边数, 边号序列字典序) 取最优。"""
    paths = all_simple_paths(model, start, target, states, removed_lines)
    if not paths:
        return None
    return min(paths, key=lambda path: (len(path[0]), path[0]))


# ---------------------------------------------------------------------------
# 三类业务操作的独立答案
# ---------------------------------------------------------------------------


def oracle_history(model_data: dict, ledger_data: dict, meter_id: str,
                   start_us: int, end_us: int) -> list[tuple[int, int, frozenset[str]]]:
    model = OracleModel(model_data)
    timeline = replay_states(model, ledger_data)
    node_id = model.meter_node[meter_id]
    boundaries = sorted({model.base_us, start_us,
                         *(t for t in timeline if start_us < t < end_us)})
    boundaries = [t for t in boundaries if t >= start_us]
    raw: list[tuple[int, frozenset[str]]] = []
    for boundary in boundaries:
        states = state_at(model, timeline, boundary)
        raw.append((boundary, reachable_sources(
            model, node_id, states, frozenset(), frozenset())))
    merged: list[list] = []
    for boundary, sources in raw:
        if merged and merged[-1][2] == sources:
            continue
        if merged:
            merged[-1][1] = boundary
        merged.append([boundary, boundary, sources])
    if merged:
        merged[-1][1] = end_us
    return [(lo, hi, src) for lo, hi, src in merged]


def oracle_routes(model_data: dict, ledger_data: dict, meter_id: str, at_us: int,
                  removed_lines=frozenset(), removed_sources=frozenset()
                  ) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    model = OracleModel(model_data)
    timeline = replay_states(model, ledger_data)
    states = state_at(model, timeline, at_us)
    start = model.meter_node[meter_id]
    answer: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    for sid, source_node in model.source_node.items():
        if sid in removed_sources or not states["source"].get(sid, False):
            continue
        path = best_path(model, start, source_node, states, frozenset(removed_lines))
        if path is not None:
            answer[sid] = path
    return answer


def oracle_drill(model_data: dict, ledger_data: dict, at_us: int,
                 removed_lines=frozenset(), removed_sources=frozenset()
                 ) -> dict[str, tuple[frozenset[str], frozenset[str], str]]:
    model = OracleModel(model_data)
    timeline = replay_states(model, ledger_data)
    states = state_at(model, timeline, at_us)
    answer = {}
    for meter_id, node_id in model.meter_node.items():
        before = reachable_sources(model, node_id, states, frozenset(), frozenset())
        after = reachable_sources(model, node_id, states,
                                  frozenset(removed_lines), frozenset(removed_sources))
        if after == before:
            category = "unchanged"
        elif not after:
            category = "dead"
        else:
            category = "degraded"
        answer[meter_id] = (before, after, category)
    return answer
