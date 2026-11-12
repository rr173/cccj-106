"""Batched ingestion CLI: feed one directory of batch files into a store."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import polars as pl

from .engine import full_run, incremental_run
from .storage import Store


def ingest_directory(store: Path, settings: Path, batch_dir: Path, configs: Path | None):
    """First batch seeds the store via ``full``; the rest go incremental."""
    batches = sorted(Path(batch_dir).glob("*.parquet"))
    if not batches:
        raise SystemExit(f"no parquet batches in {batch_dir}")
    s = Store(store)
    config_frame = pl.read_parquet(configs) if configs else None
    timings = []
    for i, batch in enumerate(batches):
        t0 = time.perf_counter()
        raw = pl.read_parquet(batch)
        if i == 0:
            s.append_raw(raw, config_frame)
            result = full_run(store, settings, run_id=f"batch-{i:02d}")
        else:
            result = incremental_run(
                store, settings, run_id=f"batch-{i:02d}", new_samples=raw
            )
        entry = {
            "batch": batch.name,
            "run_id": result.run_id,
            "rows_in_batch": raw.height,
            "affected_meters": result.affected_meters,
            "affected_intervals": result.affected_intervals.height,
            "affected_minutes": result.affected_minutes.height,
            "partitions_written": len(result.partitions_written),
            "partitions_removed": len(result.partitions_removed),
            "wall_seconds": round(time.perf_counter() - t0, 4),
        }
        timings.append(entry)
        print(json.dumps(entry))
    return timings


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="minenergy.ingest")
    parser.add_argument("--store", required=True)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--batch-dir", required=True)
    parser.add_argument("--configs", default=None)
    parser.add_argument("--fresh", action="store_true", help="delete the store first")
    args = parser.parse_args(argv)
    store = Path(args.store)
    if args.fresh and store.exists():
        shutil.rmtree(store)
    ingest_directory(store, Path(args.settings), Path(args.batch_dir), args.configs and Path(args.configs))
    facts = Store(store).read_facts()
    print(
        f"store has {facts.height} minute rows, "
        f"{facts['meter_id'].n_unique()} meters, energy={facts['energy_wh'].sum():.6f} Wh"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
