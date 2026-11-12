"""Scenario generator: raw samples and multiplier configs as parquet.

Produces deliberately unordered, revised, conflicting and gapy input so the
pipeline's deduplication, revision and invalidation rules are exercised.

Usage::

    python -m minenergy.generate --out DIR --scenario all
    python -m minenergy.generate --out DIR --scenario randomized --seed 7 --batches 4
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
from pathlib import Path

import polars as pl

from .schema import RAW_CONFIG_COLUMNS, RAW_CONFIG_DTYPES, RAW_SAMPLE_COLUMNS, RAW_SAMPLE_DTYPES
from .verify import (
    BASE,
    SECONDS,
    fixtures,
    iso,
    raw_configs_frame,
    raw_samples_frame,
    random_scenario,
    sample_row,
    config_row,
    settings_file,
)


def write_scenario(out: Path, scenario: dict, batches: int = 1, seed: int = 0) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    settings_path = out / "settings.json"
    settings_file(settings_path, scenario["meters"])

    rng = random.Random(seed)
    samples = list(scenario["samples"])
    shuffled = sorted(samples, key=lambda _: rng.random()) if batches > 1 else samples
    parts: list[list[dict]] = [[] for _ in range(batches)]
    for i, row in enumerate(shuffled):
        parts[i % batches].append(row)

    for i, part in enumerate(parts):
        path = out / "batches" / f"batch_{i:02d}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        raw_samples_frame(part).write_parquet(path)
    raw_configs_frame(scenario["configs"]).write_parquet(out / "configs_all.parquet")
    return {
        "dir": str(out),
        "settings": str(settings_path),
        "batches": [str(out / "batches" / f"batch_{i:02d}.parquet") for i in range(len(parts))],
        "configs": str(out / "configs_all.parquet"),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="minenergy.generate")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument(
        "--scenario",
        default="randomized",
        help="'all' for the named fixtures, otherwise a random scenario",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--meters", type=int, default=3)
    parser.add_argument("--points", type=int, default=40)
    args = parser.parse_args(argv)

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    if args.scenario == "all":
        payload = {}
        for name, scenario in fixtures().items():
            meta = write_scenario(out / name, scenario, batches=1, seed=args.seed)
            payload[name] = meta
    else:
        scenario = random_scenario(args.seed, n_meters=args.meters, n_points=args.points)
        payload = write_scenario(out, scenario, batches=args.batches, seed=args.seed)
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
