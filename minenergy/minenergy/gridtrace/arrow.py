"""分析结果通过 PyArrow 接出并可存 Parquet。

三张结果表各有显式 Arrow schema（不依赖隐式推断），来源集合用
``list<string>`` 承载，时刻同时给出 UTC 微秒整数与 ``Z`` 形式字符串，
便于跨语言核对。
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .timeutil import format_offset_time
from .tracing import DrillResult, Route, Segment

HISTORY_SCHEMA = pa.schema([
    pa.field("meter_id", pa.string()),
    pa.field("segment_start_us", pa.int64()),
    pa.field("segment_end_us", pa.int64()),
    pa.field("segment_start", pa.string()),
    pa.field("segment_end", pa.string()),
    pa.field("powered", pa.bool_()),
    pa.field("source_ids", pa.list_(pa.string())),
])

ROUTE_SCHEMA = pa.schema([
    pa.field("meter_id", pa.string()),
    pa.field("at_us", pa.int64()),
    pa.field("at_time", pa.string()),
    pa.field("source_id", pa.string()),
    pa.field("source_node_id", pa.string()),
    pa.field("edge_count", pa.int32()),
    pa.field("node_ids", pa.list_(pa.string())),
    pa.field("node_names", pa.list_(pa.string())),
    pa.field("edge_ids", pa.list_(pa.string())),
])

DRILL_SCHEMA = pa.schema([
    pa.field("at_us", pa.int64()),
    pa.field("at_time", pa.string()),
    pa.field("meter_id", pa.string()),
    pa.field("node_id", pa.string()),
    pa.field("category", pa.string()),
    pa.field("before_sources", pa.list_(pa.string())),
    pa.field("after_sources", pa.list_(pa.string())),
    pa.field("removed_lines", pa.list_(pa.string())),
    pa.field("removed_sources", pa.list_(pa.string())),
])

DRILL_META_SCHEMA = pa.schema([
    pa.field("at_us", pa.int64()),
    pa.field("at_time", pa.string()),
    pa.field("removed_lines", pa.list_(pa.string())),
    pa.field("removed_sources", pa.list_(pa.string())),
])


def history_to_arrow(meter_id: str, segments: list[Segment]) -> pa.Table:
    return pa.table({
        "meter_id": [meter_id] * len(segments),
        "segment_start_us": [seg.start_us for seg in segments],
        "segment_end_us": [seg.end_us for seg in segments],
        "segment_start": [format_offset_time(seg.start_us) for seg in segments],
        "segment_end": [format_offset_time(seg.end_us) for seg in segments],
        "powered": [seg.powered for seg in segments],
        "source_ids": [list(seg.sources) for seg in segments],
    }, schema=HISTORY_SCHEMA)


def routes_to_arrow(meter_id: str, at_us: int, routes: list[Route]) -> pa.Table:
    n = len(routes)
    at_text = format_offset_time(at_us)
    return pa.table({
        "meter_id": [meter_id] * n,
        "at_us": [at_us] * n,
        "at_time": [at_text] * n,
        "source_id": [r.source_id for r in routes],
        "source_node_id": [r.source_node_id for r in routes],
        "edge_count": [r.edge_count for r in routes],
        "node_ids": [list(r.node_ids) for r in routes],
        "node_names": [list(r.node_names) for r in routes],
        "edge_ids": [list(r.edge_ids) for r in routes],
    }, schema=ROUTE_SCHEMA)


def drill_to_arrow(result: DrillResult) -> pa.Table:
    at_text = format_offset_time(result.at_us)
    n = len(result.rows)
    return pa.table({
        "at_us": [result.at_us] * n,
        "at_time": [at_text] * n,
        "meter_id": [row.meter_id for row in result.rows],
        "node_id": [row.node_id for row in result.rows],
        "category": [row.category for row in result.rows],
        "before_sources": [list(row.before_sources) for row in result.rows],
        "after_sources": [list(row.after_sources) for row in result.rows],
        "removed_lines": [list(result.removed_lines) for _ in range(n)],
        "removed_sources": [list(result.removed_sources) for _ in range(n)],
    }, schema=DRILL_SCHEMA)


def write_parquet(table: pa.Table, path: str | Path) -> Path:
    """原子写单个 Parquet 文件（先写临时文件再 ``os.replace``）。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(target)
    return target
