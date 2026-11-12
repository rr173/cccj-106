"""Column schemas, dtypes and shared constants.

All timestamps are integer microseconds since the Unix epoch (UTC).  Energy is
float64 watt-hours.  Coverage is tracked in integer microseconds so the
"no more than 60s covered per minute" invariant can be checked exactly.
"""
from __future__ import annotations

import polars as pl

MINUTE_US = 60_000_000
SECOND_US = 1_000_000

UTC_DTYPE = pl.Datetime("us", "UTC")

RAW_SAMPLE_COLUMNS = ["meter_id", "event_time", "ingest_seq", "counter_wh", "boot_id"]
RAW_CONFIG_COLUMNS = ["meter_id", "effective_from", "multiplier", "config_seq"]

RAW_SAMPLE_DTYPES = {
    "meter_id": pl.String,
    "event_time": pl.String,
    "ingest_seq": pl.Int64,
    "counter_wh": pl.Float64,
    "boot_id": pl.String,
}
RAW_CONFIG_DTYPES = {
    "meter_id": pl.String,
    "effective_from": pl.String,
    "multiplier": pl.Float64,
    "config_seq": pl.Int64,
}

PAYLOAD_DTYPE = pl.Struct({"counter_wh": pl.Float64, "boot_id": pl.String})
MULTIPLIER_PAYLOAD_DTYPE = pl.Struct(
    {"config_seq": pl.Int64, "multiplier": pl.Float64}
)
SEGMENT_DTYPE = pl.Struct(
    {
        "start_us": pl.Int64,
        "end_us": pl.Int64,
        "multiplier": pl.Float64,
        "config_conflicted": pl.Boolean,
        "defaulted": pl.Boolean,
    }
)
SEGMENT_LIST_DTYPE = pl.List(SEGMENT_DTYPE)
REASON_DTYPE = pl.Struct(
    {"reason": pl.String, "invalid_us": pl.Int64, "invalid_seconds": pl.Float64}
)
REASON_LIST_DTYPE = pl.List(REASON_DTYPE)

CANONICAL_SAMPLE_DTYPES = {
    "meter_id": pl.String,
    "event_time_us": pl.Int64,
    "event_time_utc": UTC_DTYPE,
    "selected_ingest_seq": pl.Int64,
    "counter_wh": pl.Float64,
    "boot_id": pl.String,
    "revision_count": pl.UInt32,
    "all_ingest_seqs": pl.List(pl.Int64),
    "conflicted": pl.Boolean,
    "conflicting_seqs": pl.List(pl.Int64),
}
CANONICAL_SAMPLE_COLUMNS = list(CANONICAL_SAMPLE_DTYPES)

CANONICAL_CONFIG_DTYPES = {
    "meter_id": pl.String,
    "effective_us": pl.Int64,
    "effective_utc": UTC_DTYPE,
    "selected_config_seq": pl.Int64,
    "multiplier": pl.Float64,
    "revision_count": pl.UInt32,
    "all_config_seqs": pl.List(pl.Int64),
    "conflicted": pl.Boolean,
    "conflicting_seqs": pl.List(pl.Int64),
}
CANONICAL_CONFIG_COLUMNS = list(CANONICAL_CONFIG_DTYPES)

INTERVAL_DTYPES = {
    "meter_id": pl.String,
    "start_us": pl.Int64,
    "end_us": pl.Int64,
    "start_utc": UTC_DTYPE,
    "end_utc": UTC_DTYPE,
    "total_us": pl.Int64,
    "duration_seconds": pl.Float64,
    "prev_counter_wh": pl.Float64,
    "next_counter_wh": pl.Float64,
    "delta_wh": pl.Float64,
    "prev_boot_id": pl.String,
    "next_boot_id": pl.String,
    "prev_ingest_seq": pl.Int64,
    "next_ingest_seq": pl.Int64,
    "prev_revision_count": pl.UInt32,
    "next_revision_count": pl.UInt32,
    "is_valid": pl.Boolean,
    "reason_codes": pl.List(pl.String),
    "segments": SEGMENT_LIST_DTYPE,
    "multiplier_defaulted": pl.Boolean,
    "weighted_multiplier": pl.Float64,
    "expected_energy_wh": pl.Float64,
    "first_minute_us": pl.Int64,
    "last_minute_us": pl.Int64,
    "content_hash": pl.String,
}
INTERVAL_COLUMNS = list(INTERVAL_DTYPES)

MINUTE_DTYPES = {
    "meter_id": pl.String,
    "timezone": pl.String,
    "local_date": pl.Date,
    "minute_epoch_us": pl.Int64,
    "minute_start_utc": UTC_DTYPE,
    "local_minute": pl.String,
    "local_minute_label": pl.String,
    "utc_offset_seconds": pl.Int64,
    "energy_wh": pl.Float64,
    "valid_us": pl.Int64,
    "invalid_us": pl.Int64,
    "valid_seconds": pl.Float64,
    "invalid_seconds": pl.Float64,
    "coverage_us": pl.Int64,
    "coverage_status": pl.String,
    "quality_reasons": pl.List(pl.String),
    "invalid_by_reason": REASON_LIST_DTYPE,
    "has_quality_issue": pl.Boolean,
    "is_zero_usage": pl.Boolean,
    "interval_count": pl.UInt32,
}
MINUTE_COLUMNS = list(MINUTE_DTYPES)

AFFECTED_INTERVAL_DTYPES = {
    "run_id": pl.String,
    "change_kind": pl.String,
    "meter_id": pl.String,
    "start_us": pl.Int64,
    "end_us": pl.Int64,
    "start_utc": UTC_DTYPE,
    "end_utc": UTC_DTYPE,
    "is_valid": pl.Boolean,
    "reason_codes": pl.List(pl.String),
    "prev_counter_wh": pl.Float64,
    "next_counter_wh": pl.Float64,
    "delta_wh": pl.Float64,
    "expected_energy_wh": pl.Float64,
    "previous_expected_energy_wh": pl.Float64,
    "previous_content_hash": pl.String,
    "content_hash": pl.String,
    "changed_fields": pl.List(pl.String),
    "first_minute_us": pl.Int64,
    "last_minute_us": pl.Int64,
    "minutes_touched": pl.UInt32,
}
AFFECTED_INTERVAL_COLUMNS = list(AFFECTED_INTERVAL_DTYPES)

AFFECTED_MINUTE_DTYPES = {
    "run_id": pl.String,
    "meter_id": pl.String,
    "minute_epoch_us": pl.Int64,
    "first_causing_start_us": pl.Int64,
    "last_causing_end_us": pl.Int64,
    "causing_intervals": pl.UInt32,
}
AFFECTED_MINUTE_COLUMNS = list(AFFECTED_MINUTE_DTYPES)

CONFLICT_SAMPLE_DTYPES = {
    "meter_id": pl.String,
    "event_time_us": pl.Int64,
    "event_time_utc": UTC_DTYPE,
    "ingest_seq": pl.Int64,
    "is_selected": pl.Boolean,
    "payload_count": pl.Int64,
    "payloads": pl.List(PAYLOAD_DTYPE),
}
CONFLICT_SAMPLE_COLUMNS = list(CONFLICT_SAMPLE_DTYPES)

CONFLICT_CONFIG_DTYPES = {
    "meter_id": pl.String,
    "effective_us": pl.Int64,
    "effective_utc": UTC_DTYPE,
    "config_seq": pl.Int64,
    "value_count": pl.Int64,
    "multipliers": pl.List(pl.Float64),
}
CONFLICT_CONFIG_COLUMNS = list(CONFLICT_CONFIG_DTYPES)

REASON_BOOT_ID_CHANGE = "boot_id_change"
REASON_COUNTER_RESET = "counter_reset"
REASON_GAP_EXCEEDED = "gap_exceeded"
REASON_INGEST_CONFLICT = "ingest_conflict"
REASON_CONFIG_CONFLICT = "config_conflict"
REASON_MULTIPLIER_DEFAULTED = "multiplier_defaulted"

# Reasons that make an interval's energy untrustworthy: the whole span is
# reported as invalid coverage and no usage is interpolated across it.
INVALIDATING_REASON_CODES = {
    REASON_BOOT_ID_CHANGE,
    REASON_COUNTER_RESET,
    REASON_GAP_EXCEEDED,
    REASON_INGEST_CONFLICT,
    REASON_CONFIG_CONFLICT,
}

# Reasons that only lower confidence but leave the interval integrable.
QUALITY_ONLY_REASON_CODES = {REASON_MULTIPLIER_DEFAULTED}

VALID_REASON_CODES = INVALIDATING_REASON_CODES | QUALITY_ONLY_REASON_CODES

FACT_SCHEMA_VERSION = 1
