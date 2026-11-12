"""Command-line interface.

``minenergy full``        full recompute of the whole store
``minenergy incremental`` append a batch and recompute only affected neighbourhoods
``minenergy verify``      compare the store with an independent full reference
``minenergy show``        print canonical / interval / minute tables
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import polars as pl

from .canonical import RawValidationError, canonicalize_configs, canonicalize_samples
from .engine import full_run, incremental_run
from .schema import (
    AFFECTED_INTERVAL_DTYPES,
    AFFECTED_MINUTE_DTYPES,
    CANONICAL_CONFIG_DTYPES,
    CANONICAL_SAMPLE_DTYPES,
    CONFLICT_CONFIG_DTYPES,
    CONFLICT_SAMPLE_DTYPES,
    INTERVAL_DTYPES,
    MINUTE_DTYPES,
)
from .settings import load_settings
from .storage import Store
from .verify import (
    check_conservation,
    check_interval_conservation,
    check_invariants,
    compare_to_reference,
    raw_configs_frame,
    raw_samples_frame,
)

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(12)
pl.Config.set_tbl_width_chars(160)
pl.Config.set_fmt_str_lengths(60)
pl.Config.set_tbl_hide_dataframe_shape(False)


def _sample_frame_from_any(df: pl.DataFrame) -> pl.DataFrame:
    return raw_samples_frame(df)


def _config_frame_from_any(df: pl.DataFrame) -> pl.DataFrame:
    return raw_configs_frame(df)


def _read_many(paths) -> pl.DataFrame:
    if isinstance(paths, (str, Path)):
        paths = [paths]
    files = [Path(p) for p in paths]
    frames = [_read_any(p) for p in files]
    return pl.concat(frames, how="vertical_relaxed") if len(frames) > 1 else frames[0]


def _read_any(path: str) -> pl.DataFrame:
    p = Path(path)
    if p.is_dir():
        files = sorted(p.rglob("*.parquet"))
        if not files:
            raise SystemExit(f"no parquet files under {p}")
        return pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")
    return pl.read_parquet(p)


def cmd_full(args) -> int:
    store = Path(args.store)
    if args.samples or args.configs:
        # seed the raw log before the full recompute
        if args.samples:
            Store(store).append_raw(
                samples=_sample_frame_from_any(_read_many(args.samples)), configs=None
            )
        if args.configs:
            Store(store).append_raw(
                samples=None, configs=_config_frame_from_any(_read_many(args.configs))
            )
    result = full_run(store, args.settings, run_id=args.run_id)
    print(result.summary())
    facts = Store(store).read_facts()
    if facts.height:
        print(
            "minute facts:"
            f" rows={facts.height}"
            f" meters={facts['meter_id'].n_unique()}"
            f" energy_wh={facts['energy_wh'].sum():.6f}"
        )
    return 0


def cmd_incremental(args) -> int:
    store = Path(args.store)
    sample_paths = args.samples or []
    config_paths = args.configs or []
    new_samples = (
        _sample_frame_from_any(_read_many(sample_paths))
        if sample_paths
        else None
    )
    new_configs = (
        _config_frame_from_any(_read_many(config_paths))
        if config_paths
        else None
    )
    result = incremental_run(
        store,
        args.settings,
        run_id=args.run_id,
        new_samples=new_samples,
        new_configs=new_configs,
    )
    print(result.summary())
    if result.affected_intervals.height:
        print("\naffected sampling intervals:")
        print(
            result.affected_intervals.select(
                "change_kind",
                "meter_id",
                "start_us",
                "end_us",
                "is_valid",
                "reason_codes",
                "expected_energy_wh",
                "previous_expected_energy_wh",
                "changed_fields",
            )
        )
    if result.affected_minutes.height:
        print("\naffected minute partitions (first rows):")
        print(result.affected_minutes.head(15))
    if args.export_report:
        out = Path(args.export_report)
        out.mkdir(parents=True, exist_ok=True)
        result.affected_intervals.write_parquet(out / "affected_intervals.parquet")
        result.affected_minutes.write_parquet(out / "affected_minutes.parquet")
        print(f"\nreports written to {out}")
    return 0


def cmd_show(args) -> int:
    store = Store(args.store)
    if args.table == "samples":
        print(store.read_canonical_samples())
    elif args.table == "configs":
        print(store.read_canonical_configs())
    elif args.table == "conflicts":
        print("sample conflicts:")
        print(store.read_sample_conflicts())
        print("config conflicts:")
        print(store.read_config_conflicts())
    elif args.table == "intervals":
        print(store.read_intervals())
    elif args.table == "facts":
        facts = store.read_facts()
        if args.meter:
            facts = facts.filter(pl.col("meter_id") == args.meter)
        if args.date:
            facts = facts.filter(pl.col("local_date") == args.date)
        print(facts)
    elif args.table == "runs":
        for run_id in store.list_runs():
            meta = store.read_run_meta(run_id)
            print(json.dumps(meta, default=str))
    elif args.table == "affected":
        run_id = args.run_id or (store.list_runs()[-1] if store.list_runs() else None)
        if run_id is None:
            raise SystemExit("no runs recorded")
        print(f"== {run_id} affected intervals")
        print(pl.read_parquet(store.root / "affected" / run_id / "intervals.parquet"))
        print(f"== {run_id} affected minutes")
        print(pl.read_parquet(store.root / "affected" / run_id / "minutes.parquet"))
    else:
        raise SystemExit(f"unknown table {args.table}")
    return 0


def cmd_verify(args) -> int:
    """Independent check of the stored minute facts and interval conservation.

    Recomputes everything from the raw parquet logs with the pure-Python
    reference implementation, compares minute facts field by field and asserts
    the invariants (<= 60s coverage, no materialised empty minutes, global
    energy conservation between intervals and minute facts).
    """
    store = Store(args.store)
    raw_samples = store.read_raw_samples()
    raw_configs = store.read_raw_configs()
    settings = load_settings(args.settings)
    scenario_rows = raw_samples.to_dicts()
    cfg_rows = raw_configs.to_dicts()
    meters = sorted(set(raw_samples["meter_id"].to_list()) | set(raw_configs["meter_id"].to_list()))
    from .settings import meter_settings

    meter_cfg = {
        m: {
            "timezone": meter_settings(settings, m).timezone,
            "gap_threshold_seconds": meter_settings(settings, m).gap_threshold_seconds,
        }
        for m in meters
    }
    facts = store.read_facts().sort("meter_id", "minute_epoch_us")
    intervals = store.read_intervals()
    problems = []
    problems += check_invariants(facts)
    problems += check_conservation(facts, intervals)
    problems += check_interval_conservation(intervals)
    scenario = {"meters": meter_cfg, "samples": scenario_rows, "configs": cfg_rows}
    problems += compare_to_reference(facts, scenario, tol=args.tolerance)
    if problems:
        print(f"VERIFY FAILED ({len(problems)} problems)")
        for p in problems[:30]:
            print(" -", p)
        return 1
    print(
        f"VERIFY OK: {facts.height} minute rows, {intervals.height} intervals, "
        f"energy={facts['energy_wh'].sum():.6f} Wh match the reference"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy")
    sub = parser.add_subparsers(dest="command", required=True)

    p_full = sub.add_parser("full", help="full recompute")
    p_full.add_argument("--store", required=True)
    p_full.add_argument("--settings", required=True)
    p_full.add_argument("--run-id", default=None)
    p_full.add_argument("--samples", nargs="*", default=None, help="parquet of raw samples to ingest first")
    p_full.add_argument("--configs", nargs="*", default=None, help="parquet of raw configs to ingest first")
    p_full.set_defaults(func=cmd_full)

    p_inc = sub.add_parser("incremental", help="latency-tolerant incremental update")
    p_inc.add_argument("--store", required=True)
    p_inc.add_argument("--settings", required=True)
    p_inc.add_argument("--samples", nargs="*", default=None, help="parquet batch(es) of new raw samples")
    p_inc.add_argument("--configs", nargs="*", default=None, help="parquet batch(es) of new raw configs")
    p_inc.add_argument("--run-id", default=None)
    p_inc.add_argument("--export-report", default=None)
    p_inc.set_defaults(func=cmd_incremental)

    p_show = sub.add_parser("show", help="inspect stored tables")
    p_show.add_argument("--store", required=True)
    p_show.add_argument(
        "--table",
        required=True,
        choices=["samples", "configs", "conflicts", "intervals", "facts", "runs", "affected"],
    )
    p_show.add_argument("--meter", default=None)
    p_show.add_argument("--date", default=None)
    p_show.add_argument("--run-id", default=None)
    p_show.set_defaults(func=cmd_show)

    p_verify = sub.add_parser("verify", help="compare store with independent reference")
    p_verify.add_argument("--store", required=True)
    p_verify.add_argument("--settings", required=True)
    p_verify.add_argument("--tolerance", type=float, default=1e-6)
    p_verify.set_defaults(func=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except RawValidationError as exc:
        print(f"RAW INPUT ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
