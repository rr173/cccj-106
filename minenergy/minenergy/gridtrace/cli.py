"""终端演示入口（三种业务操作均可直接从 Python 调用，CLI 仅便于核对）。

示例::

    python -m minenergy.gridtrace.cli history \
        --model grid_model.json --ledger operations.json --meter M3 \
        --start 2026-01-01T00:00:00Z --end 2026-01-01T07:00:00Z

    python -m minenergy.gridtrace.cli trace ... --at 2026-01-01T05:30:00Z
    python -m minenergy.gridtrace.cli drill ... --at ... --remove-lines P1 P2
    python -m minenergy.gridtrace.cli make-case --out /tmp/gridtrace-case
    python -m minenergy.gridtrace.cli demo --work /tmp/gridtrace-demo
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from .arrow import drill_to_arrow, history_to_arrow, routes_to_arrow, write_parquet
from .errors import GridtraceError
from .loader import load_ledger, load_model
from .scenario import (
    T1, T4, T6, T7, write_case_files,
)
from .snapshot import Replay
from .timeutil import format_offset_time
from .tracing import (
    CATEGORY_DEAD,
    CATEGORY_DEGRADED,
    CATEGORY_UNCHANGED,
    drill,
    source_history,
    trace_routes,
)

CATEGORY_LABELS = {
    CATEGORY_DEAD: "完全失电",
    CATEGORY_DEGRADED: "仍有电但少了来源",
    CATEGORY_UNCHANGED: "毫无变化",
}


def _open_replay(args) -> Replay:
    model = load_model(args.model)
    ledger = load_ledger(args.ledger, model) if args.ledger else None
    return Replay(model, ledger)


def _print_history(meter_id: str, segments) -> None:
    print(f"表 {meter_id} 的供电来源演变（左含右不含；失电段显式保留）：")
    for seg in segments:
        sources = ", ".join(seg.sources) if seg.sources else "（失电）"
        tag = "双路并供" if len(seg.sources) > 1 else (
            "失电" if not seg.powered else "单源")
        print(f"  [{format_offset_time(seg.start_us)}, {format_offset_time(seg.end_us)}) "
              f"{tag:4s} 来源=[{sources}]")


def _print_routes(meter_id: str, at_us: int, routes) -> None:
    print(f"表 {meter_id} 在 {format_offset_time(at_us)} 的逐源追线：")
    if not routes:
        print("  该瞬间没有任何可达源（失电）")
        return
    for route in routes:
        chain = " -> ".join(
            f"{route.node_names[i]}({route.node_ids[i]})"
            + (f" --[{route.edge_ids[i]}]--" if i < len(route.edge_ids) else "")
            for i in range(len(route.node_ids))
        )
        print(f"  源 {route.source_id}（{route.edge_count} 边）: {chain}")
        print(f"    边号序列={list(route.edge_ids)}")


def _print_drill(result) -> None:
    print(f"故障演练 @ {format_offset_time(result.at_us)} "
          f"拔线={list(result.removed_lines)} 退源={list(result.removed_sources)}")
    for category in (CATEGORY_DEAD, CATEGORY_DEGRADED, CATEGORY_UNCHANGED):
        rows = result.by_category(category)
        print(f"  [{CATEGORY_LABELS[category]}] {len(rows)} 只")
        for row in rows:
            print(f"    {row.meter_id}@{row.node_id}: "
                  f"{list(row.before_sources)} -> {list(row.after_sources)}")


def cmd_history(args) -> int:
    replay = _open_replay(args)
    segments = source_history(replay, args.meter, args.start, args.end)
    if args.json:
        payload = [
            {"start": format_offset_time(s.start_us), "end": format_offset_time(s.end_us),
             "powered": s.powered, "sources": list(s.sources)}
            for s in segments
        ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_history(args.meter, segments)
    if args.parquet:
        write_parquet(history_to_arrow(args.meter, segments), args.parquet)
        print(f"已写 Parquet: {args.parquet}")
    return 0


def cmd_trace(args) -> int:
    replay = _open_replay(args)
    routes = trace_routes(replay, args.meter, args.at,
                          remove_lines=args.remove_lines or (),
                          remove_sources=args.remove_sources or ())
    at_us = replay.at(args.at if isinstance(args.at, int)
                      else _parse_cli_time(args.at)).epoch_us
    if args.json:
        payload = [
            {"source": r.source_id, "edge_count": r.edge_count,
             "nodes": list(r.node_ids), "node_names": list(r.node_names),
             "edges": list(r.edge_ids)}
            for r in routes
        ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_routes(args.meter, at_us, routes)
    if args.parquet:
        write_parquet(routes_to_arrow(args.meter, at_us, routes), args.parquet)
        print(f"已写 Parquet: {args.parquet}")
    return 0


def _parse_cli_time(value: str) -> int:
    from .timeutil import parse_offset_time
    return parse_offset_time(value, location="CLI 时间参数")


def cmd_drill(args) -> int:
    replay = _open_replay(args)
    result = drill(replay, args.at,
                   remove_lines=args.remove_lines or (),
                   remove_sources=args.remove_sources or ())
    if args.json:
        payload = {
            "at": format_offset_time(result.at_us),
            "removed_lines": list(result.removed_lines),
            "removed_sources": list(result.removed_sources),
            "rows": [
                {"meter": r.meter_id, "category": r.category,
                 "before": list(r.before_sources), "after": list(r.after_sources)}
                for r in result.rows
            ],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_drill(result)
    if args.parquet:
        write_parquet(drill_to_arrow(result), args.parquet)
        print(f"已写 Parquet: {args.parquet}")
    return 0


def cmd_make_case(args) -> int:
    paths = write_case_files(args.out)
    print(json.dumps({k: str(v) for k, v in paths.items()},
                     ensure_ascii=False, indent=2))
    return 0


def cmd_demo(args) -> int:
    """在全新临时（或指定）目录重建案例并跑完四个工单的核对演示。"""
    work = Path(args.work) if args.work else Path(tempfile.mkdtemp(prefix="gridtrace-demo-"))
    paths = write_case_files(work / "case")
    model = load_model(paths["model"])
    ledger = load_ledger(paths["ledger"], model)
    replay = Replay(model, ledger)

    print("== 工单一：两源并供 -> 退一源 -> 全断 -> 送电恢复（M3）")
    segments = source_history(replay, "M3", T1, T7)
    _print_history("M3", segments)

    print("\n== 工单二：同刻断旧路合新路，归并段内部追线（M6）")
    seg2 = source_history(replay, "M6", T4, T6)
    _print_history("M6", seg2)
    routes_before = trace_routes(replay, "M6", T4)
    routes_mid = trace_routes(replay, "M6", "2026-01-01T05:30:00Z")
    _print_routes("M6", _parse_cli_time(T4), routes_before)
    _print_routes("M6", _parse_cli_time("2026-01-01T05:30:00Z"), routes_mid)

    print("\n== 工单三：等长路线字典序决胜，逐根拔平行线（M5 @ T4）")
    _print_routes("M5", _parse_cli_time(T4), trace_routes(replay, "M5", T4))
    for removed in (("P1",), ("P2",), ("P1", "P2")):
        _print_drill(drill(replay, T4, remove_lines=removed))

    print("\n== 工单四：退源 + 断支路三分类，再验证正式操作簿未被污染")
    _print_drill(drill(replay, T4, remove_lines=("E2",), remove_sources=("SB",)))
    again = source_history(replay, "M3", T1, T7)
    assert again == segments, "演练污染了正式操作簿！"
    print("  演练后重查 M3 历史与演练前完全一致：正式操作簿无污染")

    if args.parquet_dir:
        pdir = Path(args.parquet_dir)
        write_parquet(history_to_arrow("M3", segments), pdir / "history_m3.parquet")
        write_parquet(routes_to_arrow("M5", _parse_cli_time(T4),
                                      trace_routes(replay, "M5", T4)),
                      pdir / "routes_m5.parquet")
        write_parquet(drill_to_arrow(drill(
            replay, T4, remove_lines=("E2",), remove_sources=("SB",))),
            pdir / "drill.parquet")
        print(f"\nParquet 已写出: {pdir}")
    print(f"\n案例目录: {work}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy-gridtrace",
                                     description="配电网追溯：历史/追线/演练")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p, *, need_ledger=True):
        p.add_argument("--model", required=True, help="接线模型 JSON")
        if need_ledger:
            p.add_argument("--ledger", required=True, help="操作簿 JSON")
        p.add_argument("--parquet", help="结果另存为 Parquet 的路径")
        p.add_argument("--json", action="store_true", help="机器可读 JSON 输出")

    p_hist = sub.add_parser("history", help="查历史供电来源演变")
    add_common(p_hist)
    p_hist.add_argument("--meter", required=True)
    p_hist.add_argument("--start", required=True)
    p_hist.add_argument("--end", required=True)
    p_hist.set_defaults(func=cmd_history)

    p_trace = sub.add_parser("trace", help="点进瞬间逐源追线")
    add_common(p_trace)
    p_trace.add_argument("--meter", required=True)
    p_trace.add_argument("--at", required=True)
    p_trace.add_argument("--remove-lines", nargs="*", default=[])
    p_trace.add_argument("--remove-sources", nargs="*", default=[])
    p_trace.set_defaults(func=cmd_trace)

    p_drill = sub.add_parser("drill", help="故障演练（临时拔线/退源）")
    add_common(p_drill)
    p_drill.add_argument("--at", required=True)
    p_drill.add_argument("--remove-lines", nargs="*", default=[])
    p_drill.add_argument("--remove-sources", nargs="*", default=[])
    p_drill.set_defaults(func=cmd_drill)

    p_case = sub.add_parser("make-case", help="在指定目录生成值班案例 JSON")
    p_case.add_argument("--out", required=True)
    p_case.set_defaults(func=cmd_make_case)

    p_demo = sub.add_parser("demo", help="一条命令重建案例并跑完四个工单")
    p_demo.add_argument("--work", help="工作目录（默认新建临时目录）")
    p_demo.add_argument("--parquet-dir", help="把三类结果写为 Parquet 的目录")
    p_demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except GridtraceError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
