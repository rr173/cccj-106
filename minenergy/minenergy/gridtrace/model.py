"""接线模型与操作簿的数据结构。

接线按**无向多重图**理解：

* 接点（node）是顶点；同一接点可挂多只表、多个源；
* 连线（line）是无向边，分 ``wire``（常接导线）与 ``switch``（开关）两类；
  两点之间允许平行多根线，也允许闭环与自环；
* 开关有初态（``closed`` 导通 / ``open`` 断开），源有投入状态
  （``engaged`` 投入 / ``retired`` 退出）；
* 常接导线没有初态字段，正式操作簿不能操纵它，只有故障演练能临时拔掉。

所有 ID 仅要求在**各自清册内**不可重号（不同清册之间允许同名，用
``target_kind`` 消歧）。
"""
from __future__ import annotations

from dataclasses import dataclass, field

WIRE = "wire"
SWITCH = "switch"
LINE_KINDS = (WIRE, SWITCH)

CLOSED = "closed"
OPEN = "open"
SWITCH_STATES = (CLOSED, OPEN)

ENGAGED = "engaged"
RETIRED = "retired"
SOURCE_STATES = (ENGAGED, RETIRED)

TARGET_SWITCH = "switch"
TARGET_SOURCE = "source"
TARGET_KINDS = (TARGET_SWITCH, TARGET_SOURCE)


@dataclass(frozen=True)
class Node:
    id: str
    name: str


@dataclass(frozen=True)
class Line:
    id: str
    kind: str  # WIRE | SWITCH
    endpoint_a: str
    endpoint_b: str
    initial_closed: bool | None  # 仅开关有；常接导线恒为 None（视为导通）

    @property
    def is_switch(self) -> bool:
        return self.kind == SWITCH

    def other_end(self, node_id: str) -> str:
        if node_id == self.endpoint_a:
            return self.endpoint_b
        return self.endpoint_a


@dataclass(frozen=True)
class Source:
    id: str
    node_id: str
    initial_engaged: bool


@dataclass(frozen=True)
class Meter:
    id: str
    node_id: str


@dataclass(frozen=True)
class Operation:
    """操作簿中的一条倒闸记录。

    ``record_index`` 是文件内原始行序（0 起），即使文件顺序打乱、语义按
    发生时刻回放，报错仍能指回原记录。
    """

    at_us: int
    op_id: str
    target_kind: str  # TARGET_SWITCH | TARGET_SOURCE
    target_id: str
    new_state: bool
    record_index: int


@dataclass
class GridModel:
    base_time_us: int
    nodes: dict[str, Node]
    lines: dict[str, Line]
    sources: dict[str, Source]
    meters: dict[str, Meter]

    def line_endpoints(self, line_id: str) -> tuple[str, str]:
        line = self.lines[line_id]
        return line.endpoint_a, line.endpoint_b


@dataclass
class Ledger:
    """校验并排序后的操作簿（按发生时刻回放，与文件顺序无关）。"""

    operations: list[Operation] = field(default_factory=list)

    def instants(self) -> list[int]:
        seen: list[int] = []
        last = None
        for op in self.operations:
            if op.at_us != last:
                seen.append(op.at_us)
                last = op.at_us
        return seen
