"""配电网追溯模块（gridtrace）。

面向值班员的三类业务操作，答案全部来自接线模型与倒闸操作回放：

* :func:`source_history` —— 历史供电来源演变（左含右不含时段、自动归并）；
* :func:`trace_routes`   —— 某一瞬间逐源追线（最少边数、边号字典序决胜）；
* :func:`drill`          —— 临时拔线/退源故障演练（三分类，不污染正式操作簿）。

分析结果通过 :mod:`pyarrow` 接出，可写 Parquet。模块只依赖标准库与 PyArrow，
不触碰读数加工（minenergy 主管道）与录波解码（minenergy.comtrade）。
"""
from __future__ import annotations

from .errors import (
    GridtraceError,
    LedgerValidationError,
    ModelValidationError,
    QueryError,
)
from .model import GridModel, Ledger, Line, Meter, Node, Operation, Source
from .tracing import (
    DrillResult,
    DrillRow,
    Route,
    Segment,
    drill,
    source_history,
    trace_routes,
)
from .loader import load_ledger, load_model
from .arrow import drill_to_arrow, history_to_arrow, routes_to_arrow, write_parquet
from .snapshot import Replay

__all__ = [
    "GridtraceError",
    "ModelValidationError",
    "LedgerValidationError",
    "QueryError",
    "GridModel",
    "Ledger",
    "Line",
    "Meter",
    "Node",
    "Operation",
    "Source",
    "Replay",
    "Segment",
    "Route",
    "DrillRow",
    "DrillResult",
    "source_history",
    "trace_routes",
    "drill",
    "load_model",
    "load_ledger",
    "history_to_arrow",
    "routes_to_arrow",
    "drill_to_arrow",
    "write_parquet",
]
