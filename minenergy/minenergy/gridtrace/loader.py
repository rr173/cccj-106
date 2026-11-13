"""本地 JSON 载入与全量校验。

输入是本地 JSON 文件（不需要网络采集）。校验遵循“不省略坏记录”原则：
一次载入会尽量找出全部问题并聚合抛出，每条问题都带对象或记录定位
（清册下标 + ID、操作编号、JSON 字段路径）。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import (
    GridtraceError,
    LedgerValidationError,
    ModelValidationError,
    ProblemCollector,
)
from .model import (
    CLOSED,
    ENGAGED,
    GridModel,
    Ledger,
    Line,
    Meter,
    Node,
    Operation,
    SOURCE_STATES,
    SWITCH,
    SWITCH_STATES,
    TARGET_SOURCE,
    TARGET_SWITCH,
    WIRE,
    LINE_KINDS,
    Source,
    TARGET_KINDS,
)
from .timeutil import parse_offset_time

MODEL_TOP_KEYS = ("base_time", "nodes", "lines", "sources", "meters")
LEDGER_TOP_KEYS = ("operations",)


def _read_json(path: str | Path, exc_type: type[GridtraceError]) -> Any:
    p = Path(path)
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise exc_type(f"无法读取 JSON 文件 {p}: {exc}") from None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise exc_type(
            f"JSON 语法错误：{p} 第 {exc.lineno} 行第 {exc.colno} 列：{exc.msg}"
        ) from None


def _require_dict(data: Any, exc_type: type[GridtraceError], what: str) -> dict:
    if not isinstance(data, dict):
        raise exc_type(f"{what} 顶层必须是 JSON 对象，收到 {type(data).__name__}")
    return data


def _get_str(obj: dict, key: str, loc: str, pc: ProblemCollector) -> str | None:
    if key not in obj:
        pc.add(f"{loc}.{key}", "缺少必填字段")
        return None
    value = obj[key]
    if not isinstance(value, str) or not value.strip():
        pc.add(f"{loc}.{key}", f"必须是非空字符串，收到 {value!r}")
        return None
    return value


def _check_unknown_keys(obj: dict, allowed: tuple[str, ...], loc: str,
                        pc: ProblemCollector) -> None:
    for key in obj:
        if key not in allowed:
            pc.add(f"{loc}.{key}", f"未知字段（允许字段：{', '.join(allowed)}）")


def _check_unique_ids(entries: list, roster: str, pc: ProblemCollector) -> set[str]:
    seen: dict[str, int] = {}
    unique: set[str] = set()
    for idx, entry in enumerate(entries):
        eid = entry.get("id") if isinstance(entry, dict) else None
        if not isinstance(eid, str) or not eid.strip():
            continue  # 缺 ID 的问题已在字段检查处登记
        loc = f"{roster}[{idx}].id"
        if eid in seen:
            pc.add(loc, f"ID {eid!r} 与 {roster}[{seen[eid]}] 重号（清册内不可重号）")
        else:
            seen[eid] = idx
            unique.add(eid)
    return unique


def model_from_dict(data: dict) -> GridModel:
    pc = ProblemCollector()

    _check_unknown_keys(data, MODEL_TOP_KEYS, "$", pc)
    for key in MODEL_TOP_KEYS:
        if key not in data:
            pc.add(f"$.{key}", "缺少顶层字段")

    base_time_us: int | None = None
    if "base_time" in data:
        try:
            base_time_us = parse_offset_time(data["base_time"], location="$.base_time")
        except ValueError as exc:
            pc.add("$.base_time", str(exc))

    rosters: dict[str, list] = {}
    for roster in ("nodes", "lines", "sources", "meters"):
        value = data.get(roster)
        if value is None:
            rosters[roster] = []
            continue
        if not isinstance(value, list):
            pc.add(f"$.{roster}", f"必须是数组，收到 {type(value).__name__}")
            rosters[roster] = []
        else:
            rosters[roster] = value
            for idx, entry in enumerate(value):
                if not isinstance(entry, dict):
                    pc.add(f"{roster}[{idx}]", f"清册条目必须是对象，收到 {type(entry).__name__}")

    node_ids = _check_unique_ids(rosters["nodes"], "nodes", pc)
    line_ids = _check_unique_ids(rosters["lines"], "lines", pc)
    source_ids = _check_unique_ids(rosters["sources"], "sources", pc)
    meter_ids = _check_unique_ids(rosters["meters"], "meters", pc)

    nodes: dict[str, Node] = {}
    for idx, entry in enumerate(rosters["nodes"]):
        if not isinstance(entry, dict):
            continue
        loc = f"nodes[{idx}]"
        _check_unknown_keys(entry, ("id", "name"), loc, pc)
        nid = _get_str(entry, "id", loc, pc)
        name = _get_str(entry, "name", loc, pc)
        if nid and nid not in nodes:
            nodes[nid] = Node(nid, name or nid)

    lines: dict[str, Line] = {}
    for idx, entry in enumerate(rosters["lines"]):
        if not isinstance(entry, dict):
            continue
        loc = f"lines[{idx}]"
        _check_unknown_keys(entry, ("id", "kind", "endpoint_a", "endpoint_b",
                                    "initial_state"), loc, pc)
        lid = _get_str(entry, "id", loc, pc)
        kind = entry.get("kind")
        if kind not in LINE_KINDS:
            pc.add(f"{loc}.kind", f"连线类别必须是 {LINE_KINDS} 之一，收到 {kind!r}")
        for endpoint_key in ("endpoint_a", "endpoint_b"):
            endpoint = entry.get(endpoint_key)
            if not isinstance(endpoint, str) or not endpoint.strip():
                pc.add(f"{loc}.{endpoint_key}",
                       f"必须是非空接点 ID 字符串，收到 {endpoint!r}")
            elif endpoint not in node_ids:
                pc.add(f"{loc}.{endpoint_key}",
                       f"悬空接线：接点 {endpoint!r} 不在接点清册中")
        state_value = entry.get("initial_state", _MISSING)
        if kind == SWITCH:
            if state_value is _MISSING:
                pc.add(f"{loc}.initial_state", "开关必须给出初态 closed/open")
            elif state_value not in SWITCH_STATES:
                pc.add(f"{loc}.initial_state",
                       f"开关初态枚举错误，必须是 {SWITCH_STATES} 之一，收到 {state_value!r}")
        elif kind == WIRE:
            if state_value is not _MISSING:
                pc.add(f"{loc}.initial_state",
                       "常接导线没有初态字段（只有开关允许 initial_state）")
        if lid and lid in line_ids and lid not in lines and kind in LINE_KINDS:
            ep_a = entry.get("endpoint_a")
            ep_b = entry.get("endpoint_b")
            initial_closed = None
            if kind == SWITCH and state_value in SWITCH_STATES:
                initial_closed = state_value == CLOSED
            if isinstance(ep_a, str) and isinstance(ep_b, str):
                lines[lid] = Line(lid, kind, ep_a, ep_b, initial_closed)

    sources: dict[str, Source] = {}
    for idx, entry in enumerate(rosters["sources"]):
        if not isinstance(entry, dict):
            continue
        loc = f"sources[{idx}]"
        _check_unknown_keys(entry, ("id", "node_id", "initial_state"), loc, pc)
        sid = _get_str(entry, "id", loc, pc)
        node_id = entry.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            pc.add(f"{loc}.node_id", f"必须是非空接点 ID 字符串，收到 {node_id!r}")
        elif node_id not in node_ids:
            pc.add(f"{loc}.node_id", f"源挂接的接点 {node_id!r} 不在接点清册中")
        state_value = entry.get("initial_state")
        if state_value not in SOURCE_STATES:
            pc.add(f"{loc}.initial_state",
                   f"源投入状态枚举错误，必须是 {SOURCE_STATES} 之一，收到 {state_value!r}")
        if sid and sid in source_ids and sid not in sources:
            if isinstance(node_id, str) and state_value in SOURCE_STATES:
                sources[sid] = Source(sid, node_id, state_value == ENGAGED)

    meters: dict[str, Meter] = {}
    for idx, entry in enumerate(rosters["meters"]):
        if not isinstance(entry, dict):
            continue
        loc = f"meters[{idx}]"
        _check_unknown_keys(entry, ("id", "node_id"), loc, pc)
        mid = _get_str(entry, "id", loc, pc)
        node_id = entry.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            pc.add(f"{loc}.node_id", f"必须是非空接点 ID 字符串，收到 {node_id!r}")
        elif node_id not in node_ids:
            pc.add(f"{loc}.node_id", f"表挂接的接点 {node_id!r} 不在接点清册中")
        if mid and mid in meter_ids and mid not in meters and isinstance(node_id, str):
            meters[mid] = Meter(mid, node_id)

    pc.raise_if_any(ModelValidationError, "接线清册校验失败，拒绝载入")
    assert base_time_us is not None
    return GridModel(base_time_us, nodes, lines, sources, meters)


_MISSING = object()


def ledger_from_dict(data: dict, model: GridModel) -> Ledger:
    pc = ProblemCollector()
    data = _require_dict(data, LedgerValidationError, "操作簿")
    _check_unknown_keys(data, LEDGER_TOP_KEYS, "$", pc)
    records = data.get("operations")
    if records is None:
        pc.add("$.operations", "缺少顶层字段")
        raise LedgerValidationError("操作簿校验失败，拒绝载入", problems=pc.problems)
    if not isinstance(records, list):
        pc.add("$.operations", f"必须是数组，收到 {type(records).__name__}")
        raise LedgerValidationError("操作簿校验失败，拒绝载入", problems=pc.problems)

    parsed: list[Operation] = []
    # op_id -> (首条记录下标, 内容指纹)，用于重传忽略与复用拒绝
    op_index: dict[str, tuple[int, tuple]] = {}
    for idx, entry in enumerate(records):
        loc = f"operations[{idx}]"
        if not isinstance(entry, dict):
            pc.add(loc, f"操作记录必须是对象，收到 {type(entry).__name__}")
            continue
        _check_unknown_keys(
            entry, ("at_time", "op_id", "target_kind", "target_id", "new_state"),
            loc, pc,
        )
        op_id = _get_str(entry, "op_id", loc, pc)
        at_us = None
        if "at_time" in entry:
            try:
                at_us = parse_offset_time(entry["at_time"], location=f"{loc}.at_time")
            except ValueError as exc:
                pc.add(f"{loc}.at_time", str(exc))
        else:
            pc.add(f"{loc}.at_time", "缺少必填字段")

        target_kind = entry.get("target_kind")
        if target_kind not in TARGET_KINDS:
            pc.add(f"{loc}.target_kind",
                   f"目标类别枚举错误，必须是 {TARGET_KINDS} 之一，收到 {target_kind!r}")
        target_id = entry.get("target_id")
        if not isinstance(target_id, str) or not target_id.strip():
            pc.add(f"{loc}.target_id", f"必须是非空目标 ID 字符串，收到 {target_id!r}")
        elif target_kind == TARGET_SWITCH and target_id not in model.lines:
            pc.add(f"{loc}.target_id",
                   f"开关 {target_id!r} 不在连线清册中（或不是开关）")
        elif target_kind == TARGET_SOURCE and target_id not in model.sources:
            pc.add(f"{loc}.target_id", f"源 {target_id!r} 不在源清册中")
        elif target_kind == TARGET_SWITCH and model.lines[target_id].kind != SWITCH:
            pc.add(f"{loc}.target_id",
                   f"目标 {target_id!r} 是常接导线，操作簿只能操纵开关和源")

        new_state = entry.get("new_state")
        allowed_states = SWITCH_STATES if target_kind == TARGET_SWITCH else (
            SOURCE_STATES if target_kind == TARGET_SOURCE else ())
        if new_state not in allowed_states:
            pc.add(f"{loc}.new_state",
                   f"目标状态枚举错误，{target_kind} 只接受 {allowed_states}，收到 {new_state!r}")

        if at_us is not None and at_us < model.base_time_us:
            pc.add(f"{loc}.at_time",
                   f"操作时刻早于基准时刻（记录 {op_id or '?'}），基准之前的记录不接受")

        fingerprint = (at_us, target_kind, target_id, new_state)
        if op_id is not None:
            if op_id in op_index:
                first_idx, first_fp = op_index[op_id]
                if first_fp == fingerprint:
                    # 完全一致的操作编号重传：忽略，不参与回放
                    continue
                pc.add(
                    f"{loc} (op_id={op_id})",
                    f"操作编号被复用且内容已修改；首次出现于 operations[{first_idx}]，"
                    f"原指纹 {first_fp}，新指纹 {fingerprint}",
                )
                continue
            op_index[op_id] = (idx, fingerprint)

        if at_us is not None and target_kind in TARGET_KINDS and \
                isinstance(target_id, str) and new_state in allowed_states:
            parsed.append(Operation(
                at_us=at_us,
                op_id=op_id or f"<record-{idx}>",
                target_kind=target_kind,
                target_id=target_id,
                new_state=(new_state == CLOSED if target_kind == TARGET_SWITCH
                           else new_state == ENGAGED),
                record_index=idx,
            ))

    # 同一瞬间同一目标被要求两种状态 -> 列明相互矛盾的操作并停止
    instant_targets: dict[tuple[int, str, str], list[Operation]] = {}
    for op in parsed:
        instant_targets.setdefault((op.at_us, op.target_kind, op.target_id), []).append(op)
    for (at_us, kind, tid), ops in sorted(instant_targets.items(),
                                          key=lambda item: item[0][0]):
        states = {op.new_state for op in ops}
        if len(states) > 1:
            ids = ", ".join(f"{op.op_id}(records[{op.record_index}])" for op in ops)
            from .timeutil import format_offset_time
            pc.add(
                f"instant={format_offset_time(at_us)} target={kind}/{tid}",
                f"同一瞬间对同一目标给出相互矛盾的状态，冲突操作：{ids}",
            )

    pc.raise_if_any(LedgerValidationError, "操作簿校验失败，拒绝载入")
    parsed.sort(key=lambda op: (op.at_us, op.record_index))
    return Ledger(operations=parsed)


def load_model(path: str | Path) -> GridModel:
    """从本地 JSON 文件载入并校验接线模型。"""
    data = _require_dict(_read_json(path, ModelValidationError),
                         ModelValidationError, "接线模型")
    return model_from_dict(data)


def load_ledger(path: str | Path, model: GridModel) -> Ledger:
    """从本地 JSON 文件载入并校验操作簿（语义按发生时刻回放）。"""
    data = _read_json(path, LedgerValidationError)
    return ledger_from_dict(data, model)


def model_from_json_text(text: str) -> GridModel:
    """便于测试/内存数据：直接从 JSON 文本构建模型。"""
    return model_from_dict(_require_dict(json.loads(text), ModelValidationError, "接线模型"))


def ledger_from_json_text(text: str, model: GridModel) -> Ledger:
    return ledger_from_dict(_require_dict(json.loads(text), LedgerValidationError, "操作簿"),
                            model)
