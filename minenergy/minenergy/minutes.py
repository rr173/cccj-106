"""Aggregate interval coverage into per-minute energy facts.

For every valid interval the counter delta is spread uniformly over elapsed
seconds.  Multiplier changes split an interval into segments, each carrying its
own share.  Segments and invalid spans are split at UTC minute boundaries, so a
minute receives at most 60 seconds of coverage per meter (intervals of one
meter never overlap).

Invalid intervals contribute invalid seconds tagged with reasons and never
contribute energy.  A minute with valid coverage but no energy is a genuine
zero-usage reading; a minute with only invalid coverage is distinct from that;
a minute with no interval coverage at all is absent from the fact table (it is
never materialised as a zero).

``invalid_by_reason`` attributes each invalid second to every reason that the
covering span carries, so its entries can overlap when a single span has
several reasons.  ``invalid_us`` is the union of invalid coverage and never
double counts.
"""
from __future__ import annotations

import polars as pl

from .schema import (
    MINUTE_DTYPES,
    MINUTE_US,
    REASON_LIST_DTYPE,
    SECOND_US,
    UTC_DTYPE,
)
from .settings import MeterSettings

_VALID_PIECE_SCHEMA = {
    "meter_id": pl.String,
    "minute_epoch_us": pl.Int64,
    "interval_start_us": pl.Int64,
    "valid_us": pl.Int64,
    "energy_wh": pl.Float64,
    "reason": pl.String,
}
_INVALID_PIECE_SCHEMA = {
    "meter_id": pl.String,
    "minute_epoch_us": pl.Int64,
    "interval_start_us": pl.Int64,
    "invalid_us": pl.Int64,
    "reason": pl.String,
}


def empty_minutes() -> pl.DataFrame:
    return pl.DataFrame(schema=MINUTE_DTYPES)


def _split_at_minutes(df: pl.DataFrame, span_start: str, span_end: str) -> pl.DataFrame:
    """Explode [span_start, span_end) spans into per-minute pieces."""
    df = df.with_columns(
        (
            (pl.col(span_end) - 1) // MINUTE_US - pl.col(span_start) // MINUTE_US + 1
        ).alias("_minute_count")
    )
    df = df.with_columns(pl.int_ranges(0, pl.col("_minute_count")).alias("_idx"))
    df = df.explode("_idx").with_columns(
        (
            pl.col(span_start) // MINUTE_US * MINUTE_US + pl.col("_idx") * MINUTE_US
        ).alias("minute_epoch_us")
    )
    df = df.with_columns(
        pl.max_horizontal(pl.col(span_start), pl.col("minute_epoch_us")).alias(
            "_piece_start"
        ),
        pl.min_horizontal(pl.col(span_end), pl.col("minute_epoch_us") + MINUTE_US).alias(
            "_piece_end"
        ),
    )
    return df.with_columns(
        (pl.col("_piece_end") - pl.col("_piece_start")).alias("_piece_us")
    )


def _valid_pieces(df: pl.DataFrame) -> pl.DataFrame:
    valid = df.filter(pl.col("is_valid")).select(
        pl.all().exclude("segments"),
        pl.col("segments").list.eval(
            pl.element().struct.rename_fields(["seg_start", "seg_end", "seg_multiplier", "config_conflicted", "defaulted"])
        ).alias("_segments"),
    )
    valid = valid.explode("_segments").unnest("_segments")
    valid = _split_at_minutes(valid, "seg_start", "seg_end")
    valid = valid.with_columns(
        (
            pl.col("delta_wh")
            * pl.col("seg_multiplier")
            * pl.col("_piece_us")
            / pl.col("total_us")
        ).alias("energy_wh")
    )
    valid = valid.explode("reason_codes", empty_as_null=True).rename(
        {"reason_codes": "reason"}
    )
    return valid.select(
        "meter_id",
        "minute_epoch_us",
        pl.col("seg_start").alias("interval_start_us"),
        pl.col("_piece_us").alias("valid_us"),
        "energy_wh",
        "reason",
    ).cast(_VALID_PIECE_SCHEMA)


def _invalid_pieces(df: pl.DataFrame) -> pl.DataFrame:
    invalid = df.filter(~pl.col("is_valid")).with_columns(
        pl.col("start_us").alias("_span_start"), pl.col("end_us").alias("_span_end")
    )
    invalid = _split_at_minutes(invalid, "_span_start", "_span_end")
    invalid = invalid.explode("reason_codes", empty_as_null=True).rename(
        {"reason_codes": "reason"}
    )
    return invalid.select(
        "meter_id",
        "minute_epoch_us",
        pl.col("start_us").alias("interval_start_us"),
        pl.col("_piece_us").alias("invalid_us"),
        "reason",
    ).cast(_INVALID_PIECE_SCHEMA)


def build_minutes(
    intervals: pl.DataFrame,
    meter_settings: dict[str, MeterSettings],
    meter_ids: list[str] | None = None,
    dates: list | None = None,
    minute_start_us: int | None = None,
    minute_end_us: int | None = None,
) -> pl.DataFrame:
    if intervals.height == 0:
        return empty_minutes()

    df = intervals
    if meter_ids is not None:
        df = df.filter(pl.col("meter_id").is_in(list(meter_ids)))
    if minute_start_us is not None:
        df = df.filter(pl.col("end_us") > minute_start_us)
    if minute_end_us is not None:
        df = df.filter(pl.col("start_us") < minute_end_us)
    if df.height == 0:
        return empty_minutes()

    tz_map = pl.DataFrame(
        [
            {"meter_id": m, "timezone": ms.timezone}
            for m, ms in sorted(meter_settings.items())
        ],
        schema={"meter_id": pl.String, "timezone": pl.String},
    )

    valid_pieces = _valid_pieces(df)
    invalid_pieces = _invalid_pieces(df)

    # --- valid side ---------------------------------------------------------
    valid_agg = valid_pieces.group_by("meter_id", "minute_epoch_us").agg(
        pl.col("energy_wh").sum(),
        pl.col("valid_us").sum(),
        pl.col("reason").drop_nulls().unique().sort().alias("_valid_reasons"),
        pl.col("interval_start_us").n_unique().cast(pl.UInt32).alias("_valid_intervals"),
    )

    # --- invalid side -------------------------------------------------------
    if invalid_pieces.height:
        # union coverage must not double count spans that carry several reasons
        invalid_union = invalid_pieces.group_by(
            "meter_id", "minute_epoch_us", "interval_start_us"
        ).agg(pl.col("invalid_us").first())
        invalid_union = invalid_union.group_by("meter_id", "minute_epoch_us").agg(
            pl.col("invalid_us").sum(),
            pl.col("interval_start_us").n_unique().cast(pl.UInt32).alias("_invalid_intervals"),
        )
        invalid_reasons = (
            invalid_pieces.group_by("meter_id", "minute_epoch_us", "reason")
            .agg(pl.col("invalid_us").max().alias("reason_us"))
            .group_by("meter_id", "minute_epoch_us")
            .agg(
                pl.col("reason").drop_nulls().unique().sort().alias("_invalid_reasons"),
                pl.struct(
                    pl.col("reason"),
                    pl.col("reason_us"),
                    (pl.col("reason_us") / SECOND_US).alias("invalid_seconds"),
                )
                .sort_by(pl.col("reason"))
                .implode()
                .alias("invalid_by_reason"),
            )
        )
        invalid_agg = invalid_union.join(
            invalid_reasons, on=["meter_id", "minute_epoch_us"], how="left"
        )
    else:
        invalid_agg = pl.DataFrame(
            {
                "meter_id": [],
                "minute_epoch_us": [],
                "invalid_us": [],
                "_invalid_intervals": [],
                "_invalid_reasons": [],
                "invalid_by_reason": [],
            },
            schema={
                "meter_id": pl.String,
                "minute_epoch_us": pl.Int64,
                "invalid_us": pl.Int64,
                "_invalid_intervals": pl.UInt32,
                "_invalid_reasons": pl.List(pl.String),
                "invalid_by_reason": REASON_LIST_DTYPE,
            },
        )

    facts = valid_agg.join(
        invalid_agg, on=["meter_id", "minute_epoch_us"], how="full", coalesce=True
    )
    facts = facts.with_columns(
        pl.col("energy_wh").fill_null(0.0),
        pl.col("valid_us").fill_null(0),
        pl.col("invalid_us").fill_null(0),
        pl.col("_valid_reasons").fill_null([]),
        pl.col("_invalid_reasons").fill_null([]),
        pl.col("invalid_by_reason").fill_null([]),
        pl.col("_valid_intervals").fill_null(0),
        pl.col("_invalid_intervals").fill_null(0),
    )
    facts = facts.with_columns(
        (pl.col("valid_us") + pl.col("invalid_us")).alias("coverage_us"),
        pl.col("valid_us").gt(0).alias("_has_valid"),
    )
    facts = facts.with_columns(
        pl.col("_valid_reasons")
        .list.set_union(pl.col("_invalid_reasons"))
        .list.sort()
        .alias("quality_reasons"),
        (pl.col("valid_us") / SECOND_US).alias("valid_seconds"),
        (pl.col("invalid_us") / SECOND_US).alias("invalid_seconds"),
    )
    facts = facts.with_columns(
        pl.when(pl.col("valid_us") == 0)
        .then(pl.lit("invalid_only"))
        .when(pl.col("invalid_us") == 0)
        .then(
            pl.when(pl.col("coverage_us") == MINUTE_US)
            .then(pl.lit("full_valid"))
            .otherwise(pl.lit("partial_valid"))
        )
        .otherwise(pl.lit("mixed"))
        .alias("coverage_status"),
        (pl.col("valid_us").gt(0) & (pl.col("energy_wh") == 0.0)).alias("is_zero_usage"),
        pl.col("quality_reasons").list.len().gt(0).alias("has_quality_issue"),
        (pl.col("_valid_intervals") + pl.col("_invalid_intervals")).alias("interval_count"),
        pl.col("minute_epoch_us").cast(UTC_DTYPE).alias("minute_start_utc"),
    )

    facts = facts.join(tz_map, on="meter_id", how="left")
    # Per-meter IANA zone: convert each group with its own zone so the two
    # repeated wall-clock minutes of a DST fall-back stay distinguishable.
    frames = []
    if facts.height:
        for tz in sorted(facts["timezone"].unique().to_list()):
            part = facts.filter(pl.col("timezone") == tz)
            local = part["minute_start_utc"].dt.convert_time_zone(tz)
            part = part.with_columns(
                local.dt.date().alias("local_date"),
                local.dt.strftime("%Y-%m-%dT%H:%M").alias("local_minute"),
                local.dt.strftime("%Y-%m-%dT%H:%M%z").alias("local_minute_label"),
                (
                    (
                        local.dt.replace_time_zone(None).dt.epoch("us")
                        - part["minute_epoch_us"]
                    )
                    / SECOND_US
                )
                .cast(pl.Int64)
                .alias("utc_offset_seconds"),
            )
            frames.append(part)
    if frames:
        facts = pl.concat(frames, how="vertical_relaxed")
    else:
        return empty_minutes()

    facts = facts.select(
        "meter_id",
        "timezone",
        "local_date",
        "minute_epoch_us",
        "minute_start_utc",
        "local_minute",
        "local_minute_label",
        "utc_offset_seconds",
        "energy_wh",
        "valid_us",
        "invalid_us",
        "valid_seconds",
        "invalid_seconds",
        "coverage_us",
        "coverage_status",
        "quality_reasons",
        "invalid_by_reason",
        "has_quality_issue",
        "is_zero_usage",
        "interval_count",
    ).cast(MINUTE_DTYPES).sort("meter_id", "minute_epoch_us")

    if dates is not None:
        facts = facts.filter(pl.col("local_date").is_in(list(dates)))
    return facts
