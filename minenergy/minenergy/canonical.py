"""Parsing and canonicalization of raw samples and multiplier configs.

Rules
-----
* ``event_time`` / ``effective_from`` must be ISO-8601 with an explicit UTC
  offset; naive timestamps are rejected.
* Within one (meter_id, event time) the row with the largest ``ingest_seq``
  wins.  Rows sharing that maximum sequence but differing in payload are a
  hard conflict: the sample is flagged and excluded from integration.
* The same rule applies to (meter_id, effective_from) and ``config_seq``.
"""
from __future__ import annotations

import polars as pl

from .schema import (
    CANONICAL_CONFIG_COLUMNS,
    CANONICAL_CONFIG_DTYPES,
    CANONICAL_SAMPLE_COLUMNS,
    CANONICAL_SAMPLE_DTYPES,
    CONFLICT_CONFIG_COLUMNS,
    CONFLICT_CONFIG_DTYPES,
    CONFLICT_SAMPLE_COLUMNS,
    CONFLICT_SAMPLE_DTYPES,
    PAYLOAD_DTYPE,
    RAW_CONFIG_COLUMNS,
    RAW_CONFIG_DTYPES,
    RAW_SAMPLE_COLUMNS,
    RAW_SAMPLE_DTYPES,
    UTC_DTYPE,
)
from .settings import has_explicit_offset


class RawValidationError(ValueError):
    """Raw rows cannot be interpreted safely."""


def empty_raw_samples() -> pl.DataFrame:
    return pl.DataFrame(schema=RAW_SAMPLE_DTYPES)


def empty_raw_configs() -> pl.DataFrame:
    return pl.DataFrame(schema=RAW_CONFIG_DTYPES)


def _require_columns(df: pl.DataFrame, required: list[str], kind: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RawValidationError(f"{kind} input missing columns: {missing}")


def _parse_instant(df: pl.DataFrame, column: str, kind: str) -> pl.DataFrame:
    """Validate and convert an ISO-8601-with-offset column to UTC microseconds."""
    work = df.with_columns(
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .alias(column)
    )

    blank = work.filter(pl.col(column).is_null() | (pl.col(column) == ""))
    if blank.height:
        raise RawValidationError(
            f"{kind} input has blank {column} values; meter_id="
            f"{blank['meter_id'].unique().to_list()}"
        )

    needs_offset = work.filter(
        ~pl.col(column).map_elements(has_explicit_offset, return_dtype=pl.Boolean)
    )
    if needs_offset.height:
        sample = needs_offset.head(5).select("meter_id", column).to_dicts()
        raise RawValidationError(
            f"{kind} input has {column} values without an explicit UTC offset; "
            f"e.g. {sample}"
        )

    parsed = work.with_columns(
        pl.col(column).str.to_datetime(time_zone="UTC", strict=False).alias("_instant")
    )
    unparseable = parsed.filter(pl.col("_instant").is_null())
    if unparseable.height:
        sample = unparseable.head(5).select("meter_id", column).to_dicts()
        raise RawValidationError(
            f"{kind} input has unparseable {column} values; e.g. {sample}"
        )
    return (
        parsed.drop(column)
        .with_columns(
            pl.col("_instant").dt.timestamp("us").alias(column + "_us"),
            pl.col("_instant").cast(UTC_DTYPE).alias(column + "_utc"),
        )
        .drop("_instant")
    )


def _strict_cast(df: pl.DataFrame, dtypes: dict[str, pl.DataType], kind: str) -> pl.DataFrame:
    exprs = []
    for name, dtype in dtypes.items():
        if name in ("event_time", "effective_from"):
            exprs.append(pl.col(name))
            continue
        exprs.append(pl.col(name).cast(dtype, strict=False).alias(name))
    out = df.select(exprs)
    for name in dtypes:
        if name in ("event_time", "effective_from"):
            continue
        if out[name].null_count():
            raise RawValidationError(f"{kind} input has null/invalid {name} values")
    return out


def canonicalize_samples(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (canonical samples, conflict report)."""
    if raw.height == 0:
        return (
            pl.DataFrame(schema=CANONICAL_SAMPLE_DTYPES),
            pl.DataFrame(schema=CONFLICT_SAMPLE_DTYPES),
        )
    _require_columns(raw, RAW_SAMPLE_COLUMNS, "sample")
    df = _strict_cast(raw, RAW_SAMPLE_DTYPES, "sample")
    df = _parse_instant(df, "event_time", "sample")

    df = df.with_columns(
        pl.struct(
            pl.col("counter_wh").alias("counter_wh"),
            pl.col("boot_id").alias("boot_id"),
        ).alias("_payload")
    )

    # per (meter, instant): chosen sequence, all sequences, distinct payloads
    grouped = df.group_by("meter_id", "event_time_us").agg(
        pl.col("event_time_utc").first(),
        pl.col("ingest_seq").max().alias("selected_ingest_seq"),
        pl.col("ingest_seq").unique().sort().alias("all_ingest_seqs"),
        pl.col("_payload").unique().sort().alias("_payloads"),
    )
    grouped = grouped.with_columns(
        pl.col("all_ingest_seqs").list.len().cast(pl.UInt32).alias("revision_count"),
        pl.col("_payloads").list.len().__gt__(1).alias("_payload_conflict"),
    )

    # sequences whose version has more than one distinct payload
    seq_payloads = df.group_by("meter_id", "event_time_us", "ingest_seq").agg(
        pl.col("_payload").unique().sort().alias("_seq_payloads")
    )
    conflicts = (
        seq_payloads.filter(pl.col("_seq_payloads").list.len() > 1)
        .join(
            grouped.select("meter_id", "event_time_us", "selected_ingest_seq"),
            on=["meter_id", "event_time_us"],
        )
        .select(
            "meter_id",
            "event_time_us",
            pl.col("ingest_seq"),
            pl.col("_seq_payloads").alias("payloads"),
            pl.col("_seq_payloads").list.len().cast(pl.Int64).alias("payload_count"),
            (pl.col("ingest_seq") == pl.col("selected_ingest_seq")).alias("is_selected"),
        )
        .with_columns(pl.col("event_time_us").cast(UTC_DTYPE).alias("event_time_utc"))
        .select(CONFLICT_SAMPLE_COLUMNS)
        .sort("meter_id", "event_time_us", "ingest_seq")
    )
    # A conflict only makes the retained sample untrustworthy when it occurs on
    # the winning (largest) sequence; conflicts on superseded revisions are
    # still reported but do not invalidate the selected payload.
    seq_conflicting = (
        conflicts.join(
            grouped.select("meter_id", "event_time_us", "selected_ingest_seq"),
            on=["meter_id", "event_time_us"],
        )
        .with_columns(
            (pl.col("ingest_seq") == pl.col("selected_ingest_seq")).alias("is_selected")
        )
        .group_by("meter_id", "event_time_us")
        .agg(
            pl.col("ingest_seq").sort().alias("conflicting_seqs"),
            pl.col("ingest_seq").filter(pl.col("is_selected")).alias("_selected_conflicts"),
        )
        .with_columns(pl.col("_selected_conflicts").list.len().gt(0).alias("conflicted"))
        .drop("_selected_conflicts")
    )

    winners = (
        df.join(
            grouped.select("meter_id", "event_time_us", "selected_ingest_seq"),
            on=["meter_id", "event_time_us"],
        )
        .filter(pl.col("ingest_seq") == pl.col("selected_ingest_seq"))
        # deterministic pick when the winning sequence itself is conflicted
        .sort("meter_id", "event_time_us", "counter_wh", "boot_id")
        .unique(subset=["meter_id", "event_time_us"], keep="first", maintain_order=True)
        .select(
            "meter_id",
            "event_time_us",
            "event_time_utc",
            "selected_ingest_seq",
            "counter_wh",
            "boot_id",
        )
    )

    canon = (
        winners.join(grouped, on=["meter_id", "event_time_us"], how="left")
        .join(seq_conflicting, on=["meter_id", "event_time_us"], how="left")
        .with_columns(
            pl.col("conflicted").fill_null(False),
            pl.col("conflicting_seqs").fill_null([]),
        )
        .select(CANONICAL_SAMPLE_COLUMNS)
        .cast(CANONICAL_SAMPLE_DTYPES)
        .sort("meter_id", "event_time_us")
    )
    return canon, conflicts


def canonicalize_configs(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (canonical config grid, conflict report)."""
    if raw.height == 0:
        return (
            pl.DataFrame(schema=CANONICAL_CONFIG_DTYPES),
            pl.DataFrame(schema=CONFLICT_CONFIG_DTYPES),
        )
    _require_columns(raw, RAW_CONFIG_COLUMNS, "config")
    df = _strict_cast(raw, RAW_CONFIG_DTYPES, "config")
    df = _parse_instant(df, "effective_from", "config").rename(
        {"effective_from_us": "effective_us", "effective_from_utc": "effective_utc"}
    )

    # A null unparseable effective_from already raised; multipliers are finite.
    bad_mult = df.filter(
        ~pl.col("multiplier").is_finite() | (pl.col("multiplier") < 0)
    )
    if bad_mult.height:
        sample = bad_mult.head(5).to_dicts()
        raise RawValidationError(
            f"config input has negative/non-finite multiplier; e.g. {sample}"
        )

    conflicts = (
        df.group_by("meter_id", "effective_us", "config_seq")
        .agg(pl.col("multiplier").unique().sort().alias("multipliers"))
        .filter(pl.col("multipliers").list.len() > 1)
        .with_columns(
            pl.col("effective_us").cast(UTC_DTYPE).alias("effective_utc"),
            pl.col("multipliers").list.len().cast(pl.Int64).alias("value_count"),
        )
        .select(CONFLICT_CONFIG_COLUMNS)
        .sort("meter_id", "effective_us", "config_seq")
    )
    grouped = df.group_by("meter_id", "effective_us").agg(
        pl.col("effective_utc").first(),
        pl.col("config_seq").max().alias("selected_config_seq"),
        pl.col("config_seq").unique().sort().alias("all_config_seqs"),
    ).with_columns(
        pl.col("all_config_seqs").list.len().cast(pl.UInt32).alias("revision_count"),
    )
    seq_conflicting = (
        conflicts.join(
            grouped.select("meter_id", "effective_us", "selected_config_seq"),
            on=["meter_id", "effective_us"],
        )
        .with_columns(
            (pl.col("config_seq") == pl.col("selected_config_seq")).alias("is_selected")
        )
        .group_by("meter_id", "effective_us")
        .agg(
            pl.col("config_seq").sort().alias("conflicting_seqs"),
            pl.col("config_seq").filter(pl.col("is_selected")).alias("_selected_conflicts"),
        )
        .with_columns(
            pl.col("_selected_conflicts").list.len().gt(0).alias("conflicted")
        )
        .drop("_selected_conflicts")
    )

    winners = (
        df.join(
            grouped.select("meter_id", "effective_us", "selected_config_seq"),
            on=["meter_id", "effective_us"],
        )
        .filter(pl.col("config_seq") == pl.col("selected_config_seq"))
        .sort("meter_id", "effective_us", "multiplier")
        .unique(subset=["meter_id", "effective_us"], keep="first", maintain_order=True)
        .select(
            "meter_id",
            "effective_us",
            "effective_utc",
            "selected_config_seq",
            "multiplier",
        )
    )

    canon = (
        winners.join(grouped, on=["meter_id", "effective_us"], how="left")
        .join(seq_conflicting, on=["meter_id", "effective_us"], how="left")
        .with_columns(
            pl.col("conflicted").fill_null(False),
            pl.col("conflicting_seqs").fill_null([]),
        )
        .select(CANONICAL_CONFIG_COLUMNS)
        .cast(CANONICAL_CONFIG_DTYPES)
        .sort("meter_id", "effective_us")
    )
    return canon, conflicts
