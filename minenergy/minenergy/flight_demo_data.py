"""Build a store big enough to cross several partitions, row groups and batches.

    python -m minenergy.flight_demo_data --out /tmp/minenergy-flight-demo

Writes ``<out>/seed/{settings.json,batches/,configs.parquet}`` (the raw input,
reusable by the existing CLI) and ``<out>/store`` (the committed fact store the
Flight service serves).  Six meters in different time zones over three weeks:
more than a hundred minute partitions, each split into several parquet row
groups by the fact writer.  A handful of late revisions exercise the store's
normal incremental path, so this is an ordinary minenergy store, not a special
case.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
from pathlib import Path

import polars as pl

from .ingest import ingest_directory
from .schema import (
    RAW_CONFIG_COLUMNS,
    RAW_CONFIG_DTYPES,
    RAW_SAMPLE_COLUMNS,
    RAW_SAMPLE_DTYPES,
)
from .storage import Store

BASE = int(dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000
UTC = dt.timezone.utc

METERS = [
    # meter, IANA zone, sampling interval (seconds)
    ("M0", "UTC", 300),
    ("M1", "America/New_York", 600),
    ("M2", "Europe/Berlin", 900),
    ("M3", "Asia/Kolkata", 1800),
    ("M4", "UTC", 120),
    ("M5", "America/New_York", 3600),
]


def _iso(us: int) -> str:
    return dt.datetime.fromtimestamp(us / 1e6, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_frames(days: int = 21, meters=None):
    meters = meters or METERS
    samples: list[dict] = []
    configs: list[dict] = []
    for meter, tz, gap in meters:
        configs.append(
            {
                "meter_id": meter,
                "effective_from": _iso(BASE - 86400 * 1_000_000),
                "multiplier": 1.0,
                "config_seq": 1,
            }
        )
        t = BASE
        counter = 0.0
        for _ in range(days * 24 * 3600 // gap + 5):
            t += gap * 1_000_000
            counter += 5.0
            samples.append(
                {
                    "meter_id": meter,
                    "event_time": _iso(t),
                    "ingest_seq": 1,
                    "counter_wh": round(counter, 6),
                    "boot_id": "B0",
                }
            )
        # a late correction in the middle of the span (exercises incremental)
        samples.append(
            {
                "meter_id": meter,
                "event_time": _iso(BASE + 10 * 86_400_000_000 + 3_600_000_000),
                "ingest_seq": 3,
                "counter_wh": counter + 50.0,
                "boot_id": "B0",
            }
        )
    settings = {
        "defaults": {"timezone": "UTC", "gap_threshold_seconds": 3600},
        "meters": {
            m: {"timezone": tz, "gap_threshold_seconds": gap}
            for m, tz, gap in meters
        },
    }
    samples_df = pl.DataFrame(samples, schema=RAW_SAMPLE_DTYPES, orient="row").select(
        RAW_SAMPLE_COLUMNS
    )
    configs_df = pl.DataFrame(configs, schema=RAW_CONFIG_DTYPES, orient="row").select(
        RAW_CONFIG_COLUMNS
    )
    return settings, samples_df, configs_df


def build(out: Path, days: int = 21, force: bool = False) -> dict:
    out = Path(out)
    store = out / "store"
    if store.exists():
        if not force:
            raise SystemExit(f"{store} exists; pass --force to rebuild")
        shutil.rmtree(store)
    seed = out / "seed"
    seed.mkdir(parents=True, exist_ok=True)
    if (seed / "settings.json").exists() and (seed / "batches" / "batch_00.parquet").exists():
        settings_path = seed / "settings.json"
        configs_path = seed / "configs.parquet"
    else:
        settings, samples, configs = build_frames(days=days)
        settings_path = seed / "settings.json"
        settings_path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
        (seed / "batches").mkdir(exist_ok=True)
        samples.write_parquet(seed / "batches" / "batch_00.parquet")
        configs_path = seed / "configs.parquet"
        configs.write_parquet(configs_path)
    ingest_directory(
        store, settings_path, seed / "batches", configs_path
    )
    s = Store(store)
    facts = s.read_facts()
    partitions = s.existing_fact_partitions()
    import pyarrow.parquet as pq

    row_groups = 0
    for path in (store / "facts").rglob("data.parquet"):
        row_groups += pq.ParquetFile(path).metadata.num_row_groups
    summary = {
        "out": str(out),
        "store": str(store),
        "settings": str(settings_path),
        "row_groups": row_groups,
        "files": len(partitions),
        "meters": sorted(facts["meter_id"].unique().to_list()),
        "fact_rows": facts.height,
        "minute_start": int(facts["minute_epoch_us"].min()),
        "minute_end": int(facts["minute_epoch_us"].max()),
        "first_utc": _iso(int(facts["minute_epoch_us"].min())),
        "last_utc": _iso(int(facts["minute_epoch_us"].max())),
        "energy_wh": float(facts["energy_wh"].sum()),
    }
    (out / "dataset.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="minenergy.flight_demo_data")
    parser.add_argument("--out", required=True, help="demo work directory")
    parser.add_argument("--days", type=int, default=21, help="days of samples per meter")
    parser.add_argument("--force", action="store_true", help="rebuild the store if present")
    args = parser.parse_args(argv)
    summary = build(Path(args.out), days=args.days, force=args.force)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
