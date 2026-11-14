"""Job-definition loading, validation and per-date expansion.

A definition is a JSON document::

    {
      "name": "night-shift",
      "limits": {
        "max_parallel": 2,
        "default_timeout_seconds": 600,
        "default_max_attempts": 2,
        "retry_delay_seconds": 0.5,
        "term_grace_seconds": 3.0
      },
      "nodes": [
        {
          "name": "samples",
          "args": ["python3", "-m", "minenergy.generate", "--out", "{run_dir}/in/{date}", "..."],
          "depends_on": [],
          "cwd": "{run_dir}",            # optional, default: the run directory
          "timeout_seconds": 120,        # optional, default from limits
          "max_attempts": 2              # optional, default from limits
        }
      ]
    }

The argument array is handed to the child process verbatim (``execvp`` style,
no shell).  Two placeholders are substituted per instance: ``{date}`` (the
YYYY-MM-DD data date) and ``{run_dir}`` (this run's directory).  Any other
placeholder — or an unbalanced brace — rejects the whole definition.

Validation is total: every problem is collected and reported at once, and an
invalid definition never creates a run (no half-started graph).  Dependencies
only connect nodes of the *same* date; different dates form independent
instances of the same graph.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .errors import DefinitionError

# Placeholders known to the orchestrator.  Anything else inside braces is an
# error, so a typo can never silently leak into a child process's argv.
PLACEHOLDER_DATE = "date"
PLACEHOLDER_RUN_DIR = "run_dir"
KNOWN_PLACEHOLDERS = {PLACEHOLDER_DATE, PLACEHOLDER_RUN_DIR}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NODE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_PLACEHOLDER_RE = re.compile(r"\{([^{}]*)\}")

DEFAULT_MAX_PARALLEL = 2
DEFAULT_TIMEOUT_SECONDS = 600.0
DEFAULT_MAX_ATTEMPTS = 2
DEFAULT_RETRY_DELAY_SECONDS = 0.5
DEFAULT_TERM_GRACE_SECONDS = 3.0


@dataclass(frozen=True)
class NodeDef:
    """One node of the definition graph (limits already resolved)."""

    name: str
    args: tuple[str, ...]
    depends_on: tuple[str, ...] = ()
    cwd: str | None = None  # None -> run directory
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_attempts: int = DEFAULT_MAX_ATTEMPTS


@dataclass(frozen=True)
class Limits:
    max_parallel: int = DEFAULT_MAX_PARALLEL
    default_timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    default_max_attempts: int = DEFAULT_MAX_ATTEMPTS
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS
    term_grace_seconds: float = DEFAULT_TERM_GRACE_SECONDS


@dataclass(frozen=True)
class Definition:
    """A fully validated job definition; immutable once loaded."""

    name: str
    nodes: tuple[NodeDef, ...]
    limits: Limits
    canonical_json: str  # canonical serialisation bound to the run
    sha256: str
    topo_order: tuple[str, ...] = field(default=())

    def node_map(self) -> dict[str, NodeDef]:
        return {n.name: n for n in self.nodes}


@dataclass(frozen=True)
class JobInstance:
    """One logical job: a node bound to one data date."""

    date: str
    node: str
    args: tuple[str, ...]  # placeholders already substituted
    cwd: str
    timeout_seconds: float
    max_attempts: int
    depends_on: tuple[str, ...]  # node names, same date


def _find_placeholder_problems(text: str, where: str, problems: list[str]) -> None:
    """Reject unknown placeholders and unbalanced braces in ``text``."""
    consumed = [False] * len(text)
    for match in _PLACEHOLDER_RE.finditer(text):
        for i in range(match.start(), match.end()):
            consumed[i] = True
        name = match.group(1)
        if name not in KNOWN_PLACEHOLDERS:
            problems.append(
                f"{where}: unknown placeholder '{{{name}}}'"
                f" (known: {sorted(KNOWN_PLACEHOLDERS)})"
            )
    for i, ch in enumerate(text):
        if ch in "{}" and not consumed[i]:
            problems.append(f"{where}: unbalanced '{ch}'")


def substitute(text: str, date: str, run_dir: str) -> str:
    """Replace known placeholders; validation already rejected the rest."""
    return (
        text.replace("{" + PLACEHOLDER_DATE + "}", date)
        .replace("{" + PLACEHOLDER_RUN_DIR + "}", run_dir)
    )


def validate_dates(dates: list[str]) -> list[str]:
    """Validate the YYYY-MM-DD data-date list; duplicates are an error."""
    problems: list[str] = []
    seen: set[str] = set()
    out: list[str] = []
    for raw in dates:
        d = str(raw)
        if not _DATE_RE.match(d):
            problems.append(f"date {d!r}: not YYYY-MM-DD")
            continue
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            problems.append(f"date {d!r}: not a real calendar date")
            continue
        if d in seen:
            problems.append(f"date {d!r}: duplicated date")
            continue
        seen.add(d)
        out.append(d)
    if not out and not problems:
        problems.append("at least one data date is required")
    if problems:
        raise DefinitionError(problems)
    return out


def _topo_order(names: list[str], deps: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """Kahn's algorithm; returns (order, cycle_nodes)."""
    indegree = {n: 0 for n in names}
    for n in names:
        for _ in deps[n]:
            indegree[n] += 1
    ready = sorted(n for n in names if indegree[n] == 0)
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for m in names:
            if node in deps[m]:
                indegree[m] -= 1
                if indegree[m] == 0:
                    ready.append(m)
                    ready.sort()
    remaining = sorted(n for n in names if indegree[n] > 0)
    return order, remaining


def definition_from_dict(data: dict) -> Definition:
    """Validate a parsed definition document; raise DefinitionError listing
    every problem found."""
    problems: list[str] = []
    if not isinstance(data, dict):
        raise DefinitionError(["definition root must be a JSON object"])

    name = data.get("name", "run")
    if not isinstance(name, str) or not name.strip():
        problems.append("name: must be a non-empty string")
        name = "run"

    raw_limits = data.get("limits", {}) or {}
    if not isinstance(raw_limits, dict):
        problems.append("limits: must be an object")
        raw_limits = {}

    def _limit(key: str, default: float, lo: float, integer: bool = False) -> float:
        value = raw_limits.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            problems.append(f"limits.{key}: must be a number")
            return default
        if integer and int(value) != value:
            problems.append(f"limits.{key}: must be an integer")
            return default
        if value < lo:
            problems.append(f"limits.{key}: must be >= {lo}")
            return default
        return int(value) if integer else float(value)

    limits = Limits(
        max_parallel=_limit("max_parallel", DEFAULT_MAX_PARALLEL, 1, integer=True),
        default_timeout_seconds=_limit("default_timeout_seconds", DEFAULT_TIMEOUT_SECONDS, 0.001),
        default_max_attempts=_limit("default_max_attempts", DEFAULT_MAX_ATTEMPTS, 1, integer=True),
        retry_delay_seconds=_limit("retry_delay_seconds", DEFAULT_RETRY_DELAY_SECONDS, 0.0),
        term_grace_seconds=_limit("term_grace_seconds", DEFAULT_TERM_GRACE_SECONDS, 0.1),
    )

    raw_nodes = data.get("nodes")
    nodes: list[NodeDef] = []
    if not isinstance(raw_nodes, list) or not raw_nodes:
        problems.append("nodes: must be a non-empty array")
        raw_nodes = []

    seen_names: set[str] = set()
    deps: dict[str, list[str]] = {}
    for index, raw in enumerate(raw_nodes):
        where = f"nodes[{index}]"
        if not isinstance(raw, dict):
            problems.append(f"{where}: must be an object")
            continue
        node_name = raw.get("name")
        if not isinstance(node_name, str) or not _NODE_NAME_RE.match(node_name or ""):
            problems.append(
                f"{where}.name: must match {_NODE_NAME_RE.pattern} (got {node_name!r})"
            )
            continue
        where = f"node {node_name!r}"
        if node_name in seen_names:
            problems.append(f"{where}: duplicate node name")
            continue
        seen_names.add(node_name)

        args = raw.get("args")
        if (
            not isinstance(args, list)
            or not args
            or any(not isinstance(a, str) or a == "" for a in args)
        ):
            problems.append(f"{where}.args: must be a non-empty array of strings")
            args = []
        for i, a in enumerate(args):
            _find_placeholder_problems(a, f"{where}.args[{i}]", problems)

        raw_deps = raw.get("depends_on", [])
        if not isinstance(raw_deps, list) or any(not isinstance(d, str) for d in raw_deps):
            problems.append(f"{where}.depends_on: must be an array of node names")
            raw_deps = []
        if len(set(raw_deps)) != len(raw_deps):
            problems.append(f"{where}.depends_on: duplicate entries")
        if node_name in raw_deps:
            problems.append(f"{where}: depends on itself")
        deps[node_name] = list(dict.fromkeys(d for d in raw_deps if isinstance(d, str)))

        cwd = raw.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            problems.append(f"{where}.cwd: must be a string")
            cwd = None
        if isinstance(cwd, str):
            _find_placeholder_problems(cwd, f"{where}.cwd", problems)

        timeout = raw.get("timeout_seconds", limits.default_timeout_seconds)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            problems.append(f"{where}.timeout_seconds: must be a number > 0")
            timeout = limits.default_timeout_seconds
        max_attempts = raw.get("max_attempts", limits.default_max_attempts)
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or max_attempts < 1
        ):
            problems.append(f"{where}.max_attempts: must be an integer >= 1")
            max_attempts = limits.default_max_attempts

        nodes.append(
            NodeDef(
                name=node_name,
                args=tuple(args),
                depends_on=tuple(deps[node_name]),
                cwd=cwd,
                timeout_seconds=float(timeout),
                max_attempts=int(max_attempts),
            )
        )

    # Dangling dependencies (references to nodes that do not exist).
    for node in nodes:
        for dep in deps.get(node.name, []):
            if dep not in seen_names:
                problems.append(f"node {node.name!r}: dangling dependency {dep!r}")

    topo: tuple[str, ...] = ()
    if not problems:
        order, cycle = _topo_order([n.name for n in nodes], deps)
        if cycle:
            problems.append(f"dependency cycle involving: {', '.join(cycle)}")
        else:
            topo = tuple(order)

    if problems:
        raise DefinitionError(problems)

    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return Definition(
        name=name.strip(),
        nodes=tuple(nodes),
        limits=limits,
        canonical_json=canonical,
        sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        topo_order=topo,
    )


def load_definition(path: str | Path) -> Definition:
    """Load and validate a definition JSON file."""
    p = Path(path)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DefinitionError([f"{p}: not valid JSON: {exc}"]) from exc
    except OSError as exc:
        raise DefinitionError([f"{p}: cannot read: {exc}"]) from exc
    return definition_from_dict(data)


def expand(definition: Definition, dates: list[str], run_dir: str | Path) -> list[JobInstance]:
    """Instantiate the graph once per date; dependencies stay within a date."""
    run_dir_str = str(Path(run_dir).resolve())
    instances: list[JobInstance] = []
    for date in dates:
        for node in definition.nodes:
            args = tuple(substitute(a, date, run_dir_str) for a in node.args)
            cwd = (
                substitute(node.cwd, date, run_dir_str)
                if node.cwd is not None
                else run_dir_str
            )
            instances.append(
                JobInstance(
                    date=date,
                    node=node.name,
                    args=args,
                    cwd=cwd,
                    timeout_seconds=node.timeout_seconds,
                    max_attempts=node.max_attempts,
                    depends_on=node.depends_on,
                )
            )
    return instances


def plan_lines(definition: Definition, dates: list[str], run_dir: str | Path) -> list[str]:
    """Human-readable listing of the instances and dependencies to be executed."""
    instances = expand(definition, dates, run_dir)
    lines = [
        f"definition {definition.name!r} sha256={definition.sha256[:16]}…",
        f"dates: {', '.join(dates)}",
        f"limits: max_parallel={definition.limits.max_parallel}"
        f" default_timeout={definition.limits.default_timeout_seconds}s"
        f" default_max_attempts={definition.limits.default_max_attempts}"
        f" retry_delay={definition.limits.retry_delay_seconds}s",
        f"instances ({len(instances)}):",
    ]
    by_date: dict[str, list[JobInstance]] = {}
    for inst in instances:
        by_date.setdefault(inst.date, []).append(inst)
    for date in dates:
        lines.append(f"  [{date}]")
        order = {name: i for i, name in enumerate(definition.topo_order)}
        for inst in sorted(by_date[date], key=lambda i: order.get(i.node, 0)):
            deps = ",".join(inst.depends_on) if inst.depends_on else "-"
            lines.append(
                f"    {inst.node}: deps=[{deps}] timeout={inst.timeout_seconds}s"
                f" attempts<={inst.max_attempts} cwd={inst.cwd}"
            )
            lines.append(f"      argv: {' '.join(inst.args)}")
    return lines
