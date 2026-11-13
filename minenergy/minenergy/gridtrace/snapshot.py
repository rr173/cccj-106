"""接线快照与操作簿回放。

:class:`Snapshot` 是某一瞬间的接线状态（哪些开关闭合、哪些源投入），所有
图搜索都基于快照，不直接读模型初态。:class:`Replay` 负责按**发生时刻**
回放操作簿：同一瞬间的全部动作先整体作用到接线上，然后才允许观察供电，
因此文件行序不会制造临时失电。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import QueryError
from .model import GridModel, Ledger, Operation, TARGET_SOURCE, TARGET_SWITCH
from .graph import Graph


@dataclass(frozen=True)
class Snapshot:
    """某一瞬间的不可变接线状态。"""

    epoch_us: int
    switch_closed: dict[str, bool]   # 仅开关
    source_engaged: dict[str, bool]  # 全部源
    graph: Graph

    def line_conducts(self, line_id: str) -> bool:
        line = self.graph.model.lines[line_id]
        if not line.is_switch:
            return True
        return self.switch_closed[line_id]

    def source_is_engaged(self, source_id: str) -> bool:
        return self.source_engaged[source_id]

    def with_overrides(
        self,
        epoch_us: int,
        *,
        removed_lines: frozenset[str] | None = None,
        removed_sources: frozenset[str] | None = None,
        switch_closed: dict[str, bool] | None = None,
        source_engaged: dict[str, bool] | None = None,
    ) -> "Snapshot":
        """派生出只在故障演练中使用的临时快照（不改任何正式状态）。"""
        new_switch = dict(switch_closed if switch_closed is not None
                          else self.switch_closed)
        new_source = dict(source_engaged if source_engaged is not None
                          else self.source_engaged)
        if removed_lines:
            for line_id in removed_lines:
                line = self.graph.model.lines[line_id]
                if line.is_switch:
                    new_switch[line_id] = False
        if removed_sources:
            for source_id in removed_sources:
                new_source[source_id] = False
        graph = self.graph.with_removed_lines(removed_lines or frozenset())
        return Snapshot(epoch_us, new_switch, new_source, graph)


class Replay:
    """操作簿回放器；同一个回放器可服务任意多次查询，本身无状态变更。"""

    def __init__(self, model: GridModel, ledger: Ledger):
        self.model = model
        self.ledger = ledger
        self._base_snapshot = self._build_base_snapshot()
        # 按时刻缓存“该时刻动作整体应用之后”的快照
        self._instant_snapshots: dict[int, Snapshot] = {}
        self._build_instant_snapshots()

    def _build_base_snapshot(self) -> Snapshot:
        switch_closed = {
            line_id: bool(line.initial_closed)
            for line_id, line in self.model.lines.items()
            if line.is_switch
        }
        source_engaged = {
            sid: source.initial_engaged
            for sid, source in self.model.sources.items()
        }
        graph = Graph(self.model)
        return Snapshot(self.model.base_time_us, switch_closed, source_engaged, graph)

    def _build_instant_snapshots(self) -> None:
        # 按时刻分组，保证同刻动作整体作用
        groups: dict[int, list[Operation]] = {}
        for op in self.ledger.operations:
            groups.setdefault(op.at_us, []).append(op)

        current = self._base_snapshot
        for at_us in sorted(groups):
            switch_closed = dict(current.switch_closed)
            source_engaged = dict(current.source_engaged)
            for op in groups[at_us]:
                if op.target_kind == TARGET_SWITCH:
                    switch_closed[op.target_id] = op.new_state
                elif op.target_kind == TARGET_SOURCE:
                    source_engaged[op.target_id] = op.new_state
            current = Snapshot(at_us, switch_closed, source_engaged, current.graph)
            self._instant_snapshots[at_us] = current

    @property
    def base_snapshot(self) -> Snapshot:
        return self._base_snapshot

    def instants(self) -> list[int]:
        return sorted(self._instant_snapshots)

    def at(self, epoch_us: int) -> Snapshot:
        """返回查询时刻的接线快照。

        语义：基准时刻是尚未应用当刻操作的初态；当刻记录参与回放。因此
        ``t`` 时刻观察到的是所有 ``at <= t`` 的操作整体应用后的状态。
        查询早于基准时刻直接报错。
        """
        if epoch_us < self.model.base_time_us:
            from .timeutil import format_offset_time
            raise QueryError(
                f"查询时刻 {format_offset_time(epoch_us)} 早于基准时刻 "
                f"{format_offset_time(self.model.base_time_us)}，基准之前不可查询"
            )
        chosen = self._base_snapshot
        for at_us in sorted(self._instant_snapshots):
            if at_us <= epoch_us:
                chosen = self._instant_snapshots[at_us]
            else:
                break
        return chosen
