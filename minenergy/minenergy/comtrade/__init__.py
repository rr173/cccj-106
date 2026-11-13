"""COMTRADE (IEEE C37.111-1999) waveform ingestion.

This package turns paired ``.cfg``/``.dat`` fault recordings into Arrow data
for downstream algorithms.  It deliberately does **not** compute another
minute-level metric: it preserves the pre/post-trip analog and digital samples
with their physical units, per-channel skews and normal-state definitions.

Public entry points::

    Recording.open(cfg_path, timezone)      # parse CFG, bind an IANA zone
    recording.catalog()                     # channel directory
    recording.validate()                    # bounded-memory import check
    recording.iter_analog_window(...)       # streaming Arrow RecordBatches
    recording.iter_digital_window(...)
    export_parquet(recording, out_dir, ...) # staged, atomic Parquet export
"""
from __future__ import annotations

from .config import (
    AnalogChannel,
    ComtradeConfig,
    DigitalChannel,
    SampleRate,
    parse_cfg,
)
from .errors import (
    CfgParseError,
    ChannelError,
    ClockZoneError,
    ComtradeError,
    FrameLayoutError,
    FrameSequenceError,
    TimeError,
)
from .export import export_parquet, provenance_metadata, sha256_file
from .recording import (
    ANALOG_SCHEMA,
    DIGITAL_SCHEMA,
    ChannelRef,
    Recording,
    convert_side,
    restore_secondary,
    unpack_digital_words,
)

__all__ = [
    "ANALOG_SCHEMA",
    "DIGITAL_SCHEMA",
    "AnalogChannel",
    "CfgParseError",
    "ChannelError",
    "ChannelRef",
    "ClockZoneError",
    "ComtradeConfig",
    "ComtradeError",
    "DigitalChannel",
    "FrameLayoutError",
    "FrameSequenceError",
    "Recording",
    "SampleRate",
    "TimeError",
    "convert_side",
    "export_parquet",
    "parse_cfg",
    "provenance_metadata",
    "restore_secondary",
    "sha256_file",
    "unpack_digital_words",
]
