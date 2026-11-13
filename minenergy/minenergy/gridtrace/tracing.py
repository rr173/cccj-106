"""三类值班业务操作。

1. :func:`source_history` —— 指定表号在 ``[start, end)`` 内的供电来源演变，
   左含右不含；一个源都到不了标为失电；邻接两段来源集合一致则归并，
   “换路但电源没变”不会额外切行。
2. :func:`trace_routes` —— 点进某一瞬间逐源追线，每个可达源一条由节点名与
   边号组成的有序路线；最少边数优先，同长按边号序列字典序决胜。追线按被
   点中瞬间的快照还原，不复用归并段的首条路线。
3. :func:`drill` —— 临时拔线/退源，逐表分完全失电、仍有电但少来源、毫无
   变化三类，给出演练前后来源集合；只在派生快照上进行，不碰正式操作簿。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import QueryError
from .model import GridModel
from .snapshot import Replay, Snapshot
from .timeutil import format_offset_time, parse_offset_time

CATEGORY_DEAD = "dead"            # 完全失电
CATEGORY_DEGRADED = "degraded"    # 仍有电但少了来源
CATEGORY_UNCHANGED = "unchanged"  # 毫无变化


@dataclass(frozen=True)
class Segment:
    """历史中的一个连贯时段（左含右不含）。"""

    start_us: int
    end_us: int | None  # None 仅理论上可能；本实现查询都带显式止点
    sources: tuple[str, ...]
    powered: bool

    @property
    def source_set(self) -> frozenset[str]:
        return frozenset(self.sources)


@dataclass(frozen=True)
class Route:
    """单个可达源的一条有序路线。"""

    source_id: str
    source_node_id: str
    node_ids: tuple[str, ...]
    node_names: tuple[str, ...]
    edge_ids: tuple[str, ...]

    @property
    def edge_count(self) -> int:
        return len(self.edge_ids)


@dataclass(frozen=True)
class DrillRow:
    meter_id: str
    node_id: str
    before_sources: tuple[str, ...]
    after_sources: tuple[str, ...]
    category: str  # CATEGORY_*


@dataclass(frozen=True)
class DrillResult:
    at_us: int
    removed_lines: tuple[str, ...]
    removed_sources: tuple[str, ...]
    rows: tuple[DrillRow, ...]

    def by_category(self, category: str) -> tuple[DrillRow, ...]:
        return tuple(row for row in self.rows if row.category == category)


def _as_epoch(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise QueryError(f"{field} 必须是 UTC 微秒整数或带偏移时间字符串，收到 {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return parse_offset_time(value, location=field)
        except ValueError as exc:
            raise QueryError(str(exc)) from None
    raise QueryError(f"{field} 必须是 UTC 微秒整数或带偏移时间字符串，收到 {type(value).__name__}")


def _check_meter(model: GridModel, meter_id: str) -> str:
    if meter_id not in model.meters:
        raise QueryError(f"表号 {meter_id!r} 不在挂表清册中")
    return model.meters[meter_id].node_id


def _check_base(model: GridModel, epoch_us: int, what: str) -> None:
    if epoch_us < model.base_time_us:
        raise QueryError(
            f"{what}时刻 {format_offset_time(epoch_us)} 早于基准时刻 "
            f"{format_offset_time(model.base_time_us)}，基准之前不可查询"
        )


def source_history(replay: Replay, meter_id: str, start: int | str,
                   end: int | str) -> list[Segment]:
    """表计 ``meter_id`` 在 ``[start, end)`` 的供电来源演变。

    时段左含右不含、连贯不重叠；邻接两段来源集合（忽略顺序）一致即归并。
    孤立表/无源可达得到 ``powered=False`` 的失电段，行不会缺失。
    """
    model = replay.model
    start_us = _as_epoch(start, "start")
    end_us = _as_epoch(end, "end")
    _check_meter(model, meter_id)
    _check_base(model, start_us, "查询起点")
    if end_us < model.base_time_us:
        _check_base(model, end_us, "查询止点")
    if start_us >= end_us:
        raise QueryError(
            f"历史区间必须满足 start < end，收到 "
            f"[{format_offset_time(start_us)}, {format_offset_time(end_us)})"
        )

    # 落在 (start, end) 内的动作时刻才会切开区间；== start 的动作已包含在
    # replay.at(start) 的快照里，== end 的动作不属于本半开区间。
    boundaries = [start_us]
    boundaries += [t for t in replay.instants() if start_us < t < end_us]
    boundaries = sorted(set(boundaries))

    node_id = model.meters[meter_id].node_id
    raw: list[tuple[int, frozenset[str]]] = []
    for boundary in boundaries:
        snapshot = replay.at(boundary)
        sources = frozenset(snapshot.graph.reachable_sources(node_id, snapshot))
        raw.append((boundary, sources))

    # 归并：邻接两段来源集合一致（双路并供也按集合比较）即合并；
    # 换路但电源没变自然不会切出新行。
    merged: list[list] = []
    for boundary, sources in raw:
        if merged and merged[-1][2] == sources:
            continue
        if merged:
            merged[-1][1] = boundary
        merged.append([boundary, boundary, sources])
    if merged:
        merged[-1][1] = end_us

    return [
        Segment(
            start_us=lo,
            end_us=hi,
            sources=tuple(sorted(source_set)),
            powered=bool(source_set),
        )
        for lo, hi, source_set in merged
    ]


def _validate_removals(model: GridModel, remove_lines, remove_sources) -> tuple[
        frozenset[str], frozenset[str]]:
    bad_lines: list[str] = []
    lines: set[str] = set()
    for line_id in remove_lines or ():
        if not isinstance(line_id, str) or line_id not in model.lines:
            bad_lines.append(str(line_id))
        elif line_id in lines:
            continue
        else:
            lines.add(line_id)
    bad_sources: list[str] = []
    sources: set[str] = set()
    for source_id in remove_sources or ():
        if not isinstance(source_id, str) or source_id not in model.sources:
            bad_sources.append(str(source_id))
        elif source_id in sources:
            continue
        else:
            sources.add(source_id)
    problems = []
    if bad_lines:
        problems.append(f"演练拔除的连线不存在：{bad_lines}")
    if bad_sources:
        problems.append(f"演练退出的源不存在：{bad_sources}")
    if problems:
        raise QueryError("非法演练对象：" + "；".join(problems))
    return frozenset(lines), frozenset(sources)


def _drill_snapshot(replay: Replay, at_us: int,
                    remove_lines: frozenset[str],
                    remove_sources: frozenset[str]) -> tuple[Snapshot, Snapshot]:
    before = replay.at(at_us)
    after = before.with_overrides(
        at_us, removed_lines=remove_lines, removed_sources=remove_sources,
    )
    return before, after


def trace_routes(replay: Replay, meter_id: str, at: int | str, *,
                 remove_lines: tuple[str, ...] | frozenset[str] | None = None,
                 remove_sources: tuple[str, ...] | frozenset[str] | None = None
                 ) -> list[Route]:
    """点进某一瞬间，为每个可达源画出一条最优有序路线。

    可选的 ``remove_lines/remove_sources`` 只在本次追线内生效（供故障演练
    核对走线），不改变回放器与操作簿。
    """
    model = replay.model
    at_us = _as_epoch(at, "at")
    node_id = _check_meter(model, meter_id)
    _check_base(model, at_us, "追线")
    rm_lines, rm_sources = _validate_removals(
        model, remove_lines or (), remove_sources or ())

    snapshot = replay.at(at_us)
    if rm_lines or rm_sources:
        snapshot = snapshot.with_overrides(
            at_us, removed_lines=rm_lines, removed_sources=rm_sources)

    target_nodes = {
        source.node_id
        for sid, source in model.sources.items()
        if snapshot.source_is_engaged(sid)
    }
    found = snapshot.graph.lexicographic_shortest_paths(node_id, target_nodes, snapshot)

    routes: list[Route] = []
    for sid in sorted(model.sources):
        source = model.sources[sid]
        if not snapshot.source_is_engaged(sid):
            continue
        path = found.get(source.node_id)
        if path is None:
            continue  # 该瞬间不可达
        edge_ids, node_ids = path
        node_names = tuple(model.nodes[nid].name for nid in node_ids)
        routes.append(Route(
            source_id=sid,
            source_node_id=source.node_id,
            node_ids=tuple(node_ids),
            node_names=node_names,
            edge_ids=tuple(edge_ids),
        ))
    return routes


def drill(replay: Replay, at: int | str, *,
          remove_lines: tuple[str, ...] | frozenset[str] | None = None,
          remove_sources: tuple[str, ...] | frozenset[str] | None = None
          ) -> DrillResult:
    """故障演练：临时拔线/退源，逐表三分类并展示演练前后来源集合。

    演练在 :meth:`Snapshot.with_overrides` 派生出的临时快照上进行，正式
    操作簿与回放器不被修改，演练后重新查询历史仍得到原答案。
    """
    model = replay.model
    at_us = _as_epoch(at, "at")
    _check_base(model, at_us, "演练")
    rm_lines, rm_sources = _validate_removals(
        model, remove_lines or (), remove_sources or ())
    before, after = _drill_snapshot(replay, at_us, rm_lines, rm_sources)

    rows: list[DrillRow] = []
    for meter_id in sorted(model.meters):
        node_id = model.meters[meter_id].node_id
        before_set = frozenset(before.graph.reachable_sources(node_id, before))
        after_set = frozenset(after.graph.reachable_sources(node_id, after))
        if after_set == before_set:
            category = CATEGORY_UNCHANGED
        elif not after_set:
            category = CATEGORY_DEAD
        else:
            category = CATEGORY_DEGRADED
        rows.append(DrillRow(
            meter_id=meter_id,
            node_id=node_id,
            before_sources=tuple(sorted(before_set)),
            after_sources=tuple(sorted(after_set)),
            category=category,
        ))
    return DrillResult(
        at_us=at_us,
        removed_lines=tuple(sorted(rm_lines)),
        removed_sources=tuple(sorted(rm_sources)),
        rows=tuple(rows),
    )
