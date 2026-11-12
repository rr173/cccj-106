"""End-to-end differential runner.

``compare`` replays a batch directory through the incremental path, then runs a
full recompute of the same raw log in a separate store and diffs both minute
facts, both interval tables, and both against the pure-Python reference.
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
import time
from pathlib import Path

import polars as pl

from .engine import full_run, incremental_run
from .storage import Store
from .verify import (
    check_conservation,
    check_interval_conservation,
    check_invariants,
    compare_to_reference,
)

MINUTE_COMPARE_COLUMNS = [
    "meter_id",
    "minute_epoch_us",
    "energy_wh",
    "valid_us",
    "invalid_us",
    "coverage_status",
    "quality_reasons",
    "local_minute_label",
    "utc_offset_seconds",
    "is_zero_usage",
]


def _diff_frames(left: pl.DataFrame, right: pl.DataFrame, columns, label: str, tol=1e-9):
    problems = []
    key = "minute_epoch_us" if "minute_epoch_us" in columns else "start_us"
    keys = ["meter_id", key]
    marker = "__right__"
    left = left.select(columns).with_columns(pl.lit(True).alias(marker))
    right = right.select(columns).with_columns(pl.lit(True).alias(marker))
    j = left.join(right, on=keys, how="full", suffix="_other", coalesce=True)
    missing = j.filter(pl.col(marker).is_null())
    if missing.height:
        problems.append(f"{label}: {missing.height} rows only in incremental")
    extra = j.filter(pl.col(marker + "_other").is_null())
    if extra.height:
        problems.append(f"{label}: {extra.height} rows only in full recompute")
    joined = j.filter(
        pl.col(marker).is_not_null() & pl.col(marker + "_other").is_not_null()
    )
    for col in columns:
        if col in ("meter_id", key):
            continue
        other = col + "_other"
        if other not in joined.columns:
            continue
        dtype = joined.schema[col]
        if dtype == pl.Float64:
            bad = joined.filter((pl.col(col) - pl.col(other)).abs() > tol)
        elif dtype == pl.List(pl.String):
            bad = joined.filter(pl.col(col).ne_missing(pl.col(other)))
        else:
            bad = joined.filter(pl.col(col).ne_missing(pl.col(other)))
        if bad.height:
            problems.append(
                f"{label}: {bad.height} rows differ in {col}; "
                f"first={bad.select('meter_id', key, col, other).head(1).to_dicts()}"
            )
    return problems


def replay_batches(
    store: Path,
    settings: Path,
    batches: list[Path],
    configs: Path | None = None,
    out: Path | None = None,
) -> list[dict]:
    """First batch through ``full``, remaining batches through ``incremental``."""
    if store.exists():
        shutil.rmtree(store)
    s = Store(store)
    summaries = []
    config_frame = pl.read_parquet(configs) if configs else None
    for i, batch in enumerate(batches):
        raw = pl.read_parquet(batch)
        t0 = time.perf_counter()
        if i == 0:
            s.append_raw(raw, config_frame)
            res = full_run(store, settings, run_id=f"replay-{i:02d}")
        else:
            res = incremental_run(
                store, settings, run_id=f"replay-{i:02d}", new_samples=raw
            )
        meta = s.read_run_meta(res.run_id)
        meta["batch"] = str(batch)
        meta["wall_seconds"] = round(time.perf_counter() - t0, 3)
        summaries.append(meta)
        if out:
            out.mkdir(parents=True, exist_ok=True)
            res.affected_intervals.write_parquet(
                out / f"batch_{i:02d}_affected_intervals.parquet"
            )
            res.affected_minutes.write_parquet(
                out / f"batch_{i:02d}_affected_minutes.parquet"
            )
    return summaries


def cmd_compare(args) -> int:
    batches = [Path(p) for p in args.batches]
    for b in batches:
        if not b.exists():
            print(f"missing batch {b}", file=sys.stderr)
            return 2
    settings = Path(args.settings)
    inc_store = Path(args.store) / "incremental"
    full_store = Path(args.store) / "full"
    summaries = replay_batches(
        inc_store, settings, batches, configs=args.configs, out=args.report_dir and Path(args.report_dir)
    )
    # full recompute of exactly the same raw log
    if full_store.exists():
        shutil.rmtree(full_store)
    src = Store(inc_store)
    dst = Store(full_store)
    dst.append_raw(src.read_raw_samples(), src.read_raw_configs())
    full_run(full_store, settings, run_id="full-reference")

    inc_facts = Store(inc_store).read_facts()
    full_facts = Store(full_store).read_facts()
    inc_intervals = Store(inc_store).read_intervals()
    full_intervals = Store(full_store).read_intervals()

    problems = []
    problems += check_invariants(inc_facts)
    problems += check_conservation(inc_facts, inc_intervals)
    problems += check_interval_conservation(inc_intervals)
    problems += _diff_frames(
        inc_facts,
        full_facts,
        MINUTE_COMPARE_COLUMNS,
        "minute facts incremental vs full",
        tol=args.tolerance,
    )
    problems += _diff_frames(
        inc_intervals,
        full_intervals,
        [
            "meter_id",
            "start_us",
            "end_us",
            "is_valid",
            "reason_codes",
            "segments",
            "expected_energy_wh",
            "content_hash",
        ],
        "intervals incremental vs full",
        tol=args.tolerance,
    )
    ref_problems = _reference_check(inc_store, settings)
    problems += ref_problems
    print(json.dumps(summaries, indent=2, default=str))
    if problems:
        print(f"\nCOMPARE FAILED ({len(problems)} problems)")
        for p in problems[:40]:
            print(" -", p)
        return 1
    print(
        f"\nCOMPARE OK: {sum(len(m.get('affected_meters', []) or []) for m in summaries)} "
        f"meter-updates across {len(batches)} batches; "
        f"incremental minute rows={inc_facts.height} match full recompute "
        f"({full_facts.height}) and the reference; energy={inc_facts['energy_wh'].sum():.6f} Wh"
    )
    return 0


def _reference_check(store: Path, settings: Path):
    from .cli import cmd_verify  # reuse the exact same checks
    import io
    from contextlib import redirect_stdout

    class _Args:
        pass

    args = _Args()
    args.store = store
    args.settings = settings
    args.tolerance = 1e-6
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = cmd_verify(args)
    text = buf.getvalue().strip()
    if rc:
        return text.splitlines()[1:]
    return []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy.compare")
    parser.add_argument("--store", required=True, help="directory holding both stores")
    parser.add_argument("--settings", required=True)
    parser.add_argument("--configs", default=None)
    parser.add_argument("--batches", nargs="+", required=True)
    parser.add_argument("--report-dir", default=None)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.set_defaults(func=cmd_compare)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
