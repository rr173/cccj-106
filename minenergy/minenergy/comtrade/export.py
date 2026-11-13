"""Batched Parquet export with staging and provenance metadata.

Import safety is a two-step process:

1. :meth:`Recording.validate` streams the whole DAT in bounded memory first, so
   nothing is written while frame/timestamp errors are still possible;
2. all Parquet files go into a private staging directory next to the target and
   are only published once every file is complete.  A ``manifest.json`` written
   last is the single completion marker; any exception removes the staging
   directory, so the target can never present half an import as success.

Every Parquet file carries schema metadata tracing the original files (with
SHA-256 hashes), the time interpretation (zone, timestamp mode, timemult) and
the physical conversion actually applied (side, a/b coefficients, ratios and
P/S markers).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .recording import ANALOG_SCHEMA, DIGITAL_SCHEMA, Recording

MANIFEST_NAME = "manifest.json"
ANALOG_FILE = "analog.parquet"
DIGITAL_FILE = "digital.parquet"


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def provenance_metadata(recording: Recording, *, side: str,
                        window: tuple[int, int],
                        analog_channels: list, digital_channels: list) -> dict:
    cfg = recording.config
    return {
        "format": "IEEE C37.111-1999 COMTRADE",
        "cfg_file": str(cfg.cfg_path),
        "dat_file": str(cfg.dat_path),
        "cfg_sha256": sha256_file(cfg.cfg_path),
        "dat_sha256": sha256_file(cfg.dat_path),
        "station_name": cfg.station_name,
        "recorder_id": cfg.recorder_id,
        "dat_encoding": cfg.file_type,
        "timezone": recording.timezone,
        "timestamp_mode": recording.timestamp_mode or "unknown",
        "timemult": str(recording.clock.timemult),
        "start_epoch_us": str(recording.clock.start_epoch_us),
        "trigger_epoch_us": str(recording.clock.trigger_epoch_us),
        "trigger_offset_us": str(recording.clock.trigger_offset_us),
        "window_relative_trigger_us": [str(window[0]), str(window[1])],
        "value_side": side,
        "physical_conversion": (
            "marked-side engineering value = a*raw + b (P marker => primary, "
            "S marker => secondary); the opposite side is derived with "
            "primary_ratio/secondary_ratio in the direction the marker dictates"
        ),
        "analog_channels": [
            {
                "index": c.index, "name": c.name, "unit": c.unit,
                "phase": c.phase, "a": c.a, "b": c.b, "skew_us": c.skew_us,
                "primary_ratio": c.primary_ratio,
                "secondary_ratio": c.secondary_ratio,
                "ps_marker": c.ps_marker,
            }
            for c in analog_channels
        ],
        "digital_channels": [
            {"index": c.index, "name": c.name, "phase": c.phase,
             "normal_state": c.normal_state}
            for c in digital_channels
        ],
        "sample_rates": [
            {"rate_hz": str(seg.rate_hz), "end_sample": seg.end_sample,
             "start_offset_us": seg.start_offset_us}
            for seg in recording.clock.segments
        ],
        "producer": "minenergy.comtrade",
    }


def _schema_with_metadata(base: pa.Schema, metadata: dict) -> pa.Schema:
    encoded = {k: json.dumps(v, ensure_ascii=False).encode("utf-8")
               for k, v in metadata.items()}
    encoded["comtrade.provenance"] = json.dumps(
        metadata, ensure_ascii=False, sort_keys=True,
    ).encode("utf-8")
    return base.with_metadata(encoded)


def export_parquet(recording: Recording, out_dir: str | Path, *,
                   start_rel_us: int, end_rel_us: int,
                   analog_channels=None, digital_channels=None,
                   side: str = "secondary", batch_size: int = 8192,
                   batch_frames: int = 4096, overwrite: bool = False) -> dict:
    """Validate first, then stream batches into Parquet via a staging dir."""
    target = Path(out_dir)
    if target.exists() and not target.is_dir():
        raise NotADirectoryError(f"export target exists and is not a directory: {target}")
    if target.exists() and any(target.iterdir()) and not overwrite:
        raise FileExistsError(
            f"export directory {target} is not empty; pass overwrite=True to "
            "replace its contents",
        )

    # Full bounded-memory check before a single byte of output is written.
    recording.validate(batch_frames=batch_frames)

    analog_chosen, _ = recording._resolve_channels(analog_channels, "analog")
    digital_chosen, _ = recording._resolve_channels(digital_channels, "digital")
    window = (int(start_rel_us), int(end_rel_us))
    metadata = provenance_metadata(
        recording, side=side, window=window,
        analog_channels=analog_chosen, digital_channels=digital_chosen,
    )

    parent = target.parent if target.parent != Path("") else Path(".")
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".tmp-comtrade-{uuid.uuid4().hex}-",
                                    dir=parent))
    counts = {"analog_rows": 0, "digital_rows": 0, "files": []}
    try:
        if recording.config.n_analog:
            schema = _schema_with_metadata(ANALOG_SCHEMA, metadata)
            staged_file = staging / ANALOG_FILE
            with pq.ParquetWriter(staged_file, schema, compression="zstd") as writer:
                for batch in recording.iter_analog_window(
                    window[0], window[1], channels=analog_channels, side=side,
                    batch_size=batch_size, batch_frames=batch_frames,
                ):
                    writer.write_batch(batch)
                    counts["analog_rows"] += batch.num_rows
            counts["files"].append(ANALOG_FILE)

        if recording.config.n_digital:
            schema = _schema_with_metadata(DIGITAL_SCHEMA, metadata)
            staged_file = staging / DIGITAL_FILE
            with pq.ParquetWriter(staged_file, schema, compression="zstd") as writer:
                for batch in recording.iter_digital_window(
                    window[0], window[1], channels=digital_channels,
                    batch_size=batch_size, batch_frames=batch_frames,
                ):
                    writer.write_batch(batch)
                    counts["digital_rows"] += batch.num_rows
            counts["files"].append(DIGITAL_FILE)

        target.mkdir(parents=True, exist_ok=True)
        if overwrite:
            for name in (ANALOG_FILE, DIGITAL_FILE, MANIFEST_NAME):
                stale = target / name
                if stale.exists():
                    stale.unlink()
        for name in counts["files"]:
            os.replace(staging / name, target / name)

        manifest = {
            **metadata,
            "analog_rows": counts["analog_rows"],
            "digital_rows": counts["digital_rows"],
            "parquet_files": counts["files"],
            "batch_size": batch_size,
        }
        manifest_tmp = staging / MANIFEST_NAME
        manifest_tmp.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        # The manifest is the completion marker: only its atomic replace makes
        # the directory a finished import.
        os.replace(manifest_tmp, target / MANIFEST_NAME)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    else:
        shutil.rmtree(staging, ignore_errors=True)

    counts["out_dir"] = str(target)
    counts["manifest"] = str(target / MANIFEST_NAME)
    return counts
