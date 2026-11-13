"""错误类型。

所有“数据不可信、必须拒绝整批输入”的情形都携带**对象或记录定位信息**
（清册名、下标、操作编号、JSON 指针等），错误聚合后一次性抛出，
不允许省略坏记录继续给答案。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Problem:
    """单条校验问题。

    ``location`` 形如 ``nodes[3].id``、``operations[5] (op_id=OP12)``、
    ``lines/L7``，保证值班员能直接定位坏记录。
    """

    location: str
    message: str

    def render(self) -> str:
        return f"{self.location}: {self.message}"


class GridtraceError(Exception):
    """本模块所有异常的基类。"""

    code: str = "GridtraceError"

    def __init__(self, message: str, *, problems: list[Problem] | None = None):
        self.message = message
        self.problems: list[Problem] = list(problems or [])
        if self.problems:
            rendered = "\n  - " + "\n  - ".join(p.render() for p in self.problems)
            super().__init__(f"{self.code}: {message}{rendered}")
        else:
            super().__init__(f"{self.code}: {message}")


class ModelValidationError(GridtraceError):
    """基准时刻/接点/连线/源/挂表清册本身不合法。"""

    code = "ModelValidationError"


class LedgerValidationError(GridtraceError):
    """操作簿不合法（坏记录、同刻矛盾、编号复用、时间越界等）。"""

    code = "LedgerValidationError"


class QueryError(GridtraceError):
    """业务查询参数不合法（未知表号、查询早于基准、非法演练对象等）。"""

    code = "QueryError"


@dataclass
class ProblemCollector:
    """收集校验问题的小工具；累积后统一抛出，避免遇错即停漏掉后续坏记录。"""

    prefix: str = ""
    problems: list[Problem] = field(default_factory=list)

    def sub(self, prefix: str) -> "ProblemCollector":
        base = f"{self.prefix}.{prefix}" if self.prefix else prefix
        return ProblemCollector(prefix=base, problems=self.problems)

    def add(self, location: str, message: str) -> None:
        full = f"{self.prefix}.{location}" if self.prefix else location
        self.problems.append(Problem(full, message))

    def check(self, condition: bool, location: str, message: str) -> bool:
        if not condition:
            self.add(location, message)
        return condition

    def raise_if_any(self, exc_type: type[GridtraceError], summary: str) -> None:
        if self.problems:
            raise exc_type(summary, problems=self.problems)
