"""Parquet layout for raw inputs, canonical revisions and minute facts.

Layout under the store root::

    raw/samples.parquet
    raw/configs.parquet
    canonical/samples.parquet
    canonical/configs.parquet
    canonical/sample_conflicts.parquet
    canonical/config_conflicts.parquet
    intervals/meter_id=<id>/data.parquet
    facts/meter_id=<id>/timezone=<tz>/local_date=<date>/data.parquet
    affected/<run_id>/intervals.parquet
    affected/<run_id>/minutes.parquet
    affected/<run_id>/run.json
    state/settings.json

Every write is atomic (temporary file + ``os.replace``) so an interrupted run
never leaves a half-written partition.
"""
from __future__ import annotations

import json
import os
import tempfile
from urllib.parse import quote, unquote
from datetime import date
from pathlib import Path

import polars as pl

from .schema import (
    AFFECTED_INTERVAL_DTYPES,
    AFFECTED_MINUTE_DTYPES,
    CANONICAL_CONFIG_DTYPES,
    CANONICAL_SAMPLE_DTYPES,
    CONFLICT_CONFIG_DTYPES,
    CONFLICT_SAMPLE_DTYPES,
    INTERVAL_DTYPES,
    MINUTE_DTYPES,
    RAW_CONFIG_DTYPES,
    RAW_SAMPLE_DTYPES,
)


def _write_atomic(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".parquet", dir=path.parent)
    os.close(fd)
    try:
        df.write_parquet(tmp, compression="zstd", statistics=True)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def read_parquet_tree(root: Path, dtypes: dict[str, pl.DataType]) -> pl.DataFrame:
    """Concatenate every parquet under ``root``; empty/missing tree is empty."""
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return pl.DataFrame(schema=dtypes)
    parts = [pl.read_parquet(f) for f in files]
    merged = pl.concat(parts, how="vertical_relaxed")
    present = [c for c in dtypes if c in merged.columns]
    return merged.select(present).cast({k: dtypes[k] for k in present})


class Store:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ---- raw -------------------------------------------------------------
    @property
    def raw_samples_path(self) -> Path:
        return self.root / "raw" / "samples.parquet"

    @property
    def raw_configs_path(self) -> Path:
        return self.root / "raw" / "configs.parquet"

    def append_raw(
        self, samples: pl.DataFrame | None = None, configs: pl.DataFrame | None = None
    ) -> None:
        if samples is not None and samples.height:
            existing = self.read_raw_samples()
            merged = pl.concat(
                [existing, samples.select(existing.columns if existing.height else samples.columns)],
                how="vertical_relaxed",
            )
            _write_atomic(merged.cast(RAW_SAMPLE_DTYPES), self.raw_samples_path)
        if configs is not None and configs.height:
            existing = self.read_raw_configs()
            merged = pl.concat(
                [existing, configs.select(existing.columns if existing.height else configs.columns)],
                how="vertical_relaxed",
            )
            _write_atomic(merged.cast(RAW_CONFIG_DTYPES), self.raw_configs_path)

    def read_raw_samples(self) -> pl.DataFrame:
        if not self.raw_samples_path.exists():
            return pl.DataFrame(schema=RAW_SAMPLE_DTYPES)
        return pl.read_parquet(self.raw_samples_path).cast(RAW_SAMPLE_DTYPES)

    def read_raw_configs(self) -> pl.DataFrame:
        if not self.raw_configs_path.exists():
            return pl.DataFrame(schema=RAW_CONFIG_DTYPES)
        return pl.read_parquet(self.raw_configs_path).cast(RAW_CONFIG_DTYPES)

    def clear_raw(self) -> None:
        for p in (self.raw_samples_path, self.raw_configs_path):
            p.unlink(missing_ok=True)

    # ---- canonical -------------------------------------------------------
    def write_canonical(
        self,
        samples: pl.DataFrame,
        configs: pl.DataFrame,
        sample_conflicts: pl.DataFrame,
        config_conflicts: pl.DataFrame,
    ) -> None:
        _write_atomic(samples.cast(CANONICAL_SAMPLE_DTYPES), self.root / "canonical" / "samples.parquet")
        _write_atomic(configs.cast(CANONICAL_CONFIG_DTYPES), self.root / "canonical" / "configs.parquet")
        _write_atomic(
            sample_conflicts.cast(CONFLICT_SAMPLE_DTYPES),
            self.root / "canonical" / "sample_conflicts.parquet",
        )
        _write_atomic(
            config_conflicts.cast(CONFLICT_CONFIG_DTYPES),
            self.root / "canonical" / "config_conflicts.parquet",
        )

    def read_canonical_samples(self) -> pl.DataFrame:
        p = self.root / "canonical" / "samples.parquet"
        if not p.exists():
            return pl.DataFrame(schema=CANONICAL_SAMPLE_DTYPES)
        return pl.read_parquet(p).cast(CANONICAL_SAMPLE_DTYPES)

    def read_canonical_configs(self) -> pl.DataFrame:
        p = self.root / "canonical" / "configs.parquet"
        if not p.exists():
            return pl.DataFrame(schema=CANONICAL_CONFIG_DTYPES)
        return pl.read_parquet(p).cast(CANONICAL_CONFIG_DTYPES)

    def read_sample_conflicts(self) -> pl.DataFrame:
        p = self.root / "canonical" / "sample_conflicts.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema=CONFLICT_SAMPLE_DTYPES)

    def read_config_conflicts(self) -> pl.DataFrame:
        p = self.root / "canonical" / "config_conflicts.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema=CONFLICT_CONFIG_DTYPES)

    # ---- intervals -------------------------------------------------------
    def read_intervals(self) -> pl.DataFrame:
        return read_parquet_tree(self.root / "intervals", INTERVAL_DTYPES)

    def write_intervals_for_meter(self, meter_id: str, intervals: pl.DataFrame) -> Path:
        path = self.root / "intervals" / f"meter_id={meter_id}" / "data.parquet"
        _write_atomic(intervals.cast(INTERVAL_DTYPES), path)
        return path

    def replace_all_intervals(self, intervals: pl.DataFrame) -> None:
        base = self.root / "intervals"
        if base.exists():
            for f in base.rglob("*.parquet"):
                f.unlink()
        for meter_id in sorted(set(intervals["meter_id"].to_list())) if intervals.height else []:
            self.write_intervals_for_meter(
                meter_id, intervals.filter(pl.col("meter_id") == meter_id)
            )

    # ---- minute facts ----------------------------------------------------
    @staticmethod
    def _partition_path(root: Path, meter_id: str, timezone: str, local_date: date) -> Path:
        return (
            root
            / f"meter_id={meter_id}"
            / f"timezone={quote(timezone, safe='')}"
            / f"local_date={local_date.isoformat()}"
            / "data.parquet"
        )

    def existing_fact_partitions(self) -> dict[tuple[str, str], Path]:
        base = self.root / "facts"
        out: dict[tuple[str, str], Path] = {}
        if not base.exists():
            return out
        for path in base.rglob("data.parquet"):
            parts = dict(p.split("=", 1) for p in path.parent.parts[-3:])
            out[(unquote(parts["meter_id"]), parts["local_date"])] = path
        return out

    def read_facts(self) -> pl.DataFrame:
        return read_parquet_tree(self.root / "facts", MINUTE_DTYPES)

    def write_fact_partitions(self, facts: pl.DataFrame) -> list[Path]:
        written = []
        if facts.height == 0:
            return written
        facts = facts.cast(MINUTE_DTYPES)
        for keys, part in facts.partition_by(
            ["meter_id", "timezone", "local_date"],
            as_dict=True,
            maintain_order=True,
        ).items():
            meter_id, timezone, local_date = keys
            path = self._partition_path(self.root / "facts", meter_id, timezone, local_date)
            _write_atomic(part, path)
            written.append(path)
        return written

    def replace_all_fact_partitions(self, facts: pl.DataFrame) -> list[Path]:
        base = self.root / "facts"
        if base.exists():
            for f in base.rglob("*.parquet"):
                f.unlink()
        return self.write_fact_partitions(facts)

    def delete_fact_partition(self, meter_id: str, local_date_iso: str) -> str | None:
        partitions = self.existing_fact_partitions()
        path = partitions.get((meter_id, local_date_iso))
        if path is None:
            return None
        path.unlink(missing_ok=True)
        for parent in (path.parent, path.parent.parent, path.parent.parent.parent):
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        return str(path)

    # ---- reports and state ----------------------------------------------
    def write_affected(self, run_id: str, intervals: pl.DataFrame, minutes: pl.DataFrame) -> None:
        base = self.root / "affected" / run_id
        _write_atomic(intervals.cast(AFFECTED_INTERVAL_DTYPES), base / "intervals.parquet")
        _write_atomic(minutes.cast(AFFECTED_MINUTE_DTYPES), base / "minutes.parquet")

    def write_run_meta(self, run_id: str, meta: dict) -> None:
        base = self.root / "affected" / run_id
        base.mkdir(parents=True, exist_ok=True)
        (base / "run.json").write_text(
            json.dumps(meta, indent=2, default=str) + "\n", encoding="utf-8"
        )

    def read_run_meta(self, run_id: str) -> dict:
        p = self.root / "affected" / run_id / "run.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    def list_runs(self) -> list[str]:
        base = self.root / "affected"
        if not base.exists():
            return []
        return sorted(p.name for p in base.iterdir() if p.is_dir())

    def _state_path(self) -> Path:
        return self.root / "state" / "settings.json"

    def write_settings_fingerprint(self, fingerprints: dict[str, tuple[str, int]]) -> None:
        p = self._state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {m: {"timezone": tz, "gap_threshold_seconds": g} for m, (tz, g) in fingerprints.items()}
        p.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def read_settings_fingerprint(self) -> dict[str, dict]:
        p = self._state_path()
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
