"""Derive integrable intervals from canonical samples and multiplier config.

An *interval* is the half-open span ``[prev.event_time, sample.event_time)``
between two consecutive canonical samples of one meter.  Its counter delta is
spread uniformly over the elapsed seconds.

An interval contributes energy only if all of the following hold:
  * the elapsed gap is at most the meter's configured threshold,
  * both endpoints share the same ``boot_id``,
  * the counter does not decrease, and
  * neither endpoint carries a same-sequence ingest conflict, and no
    conflicting config revision is used anywhere inside the interval.

Every other interval is reported as invalid coverage over its whole span with
one or more reason codes; no usage is interpolated across it.

Multiplier changes falling strictly inside an interval split it into
segments, so the multiplier in force at every instant is used.  When no config
is in effect at an interval's start, the multiplier defaults to 1.0 and the
interval gets the non-invalidating ``multiplier_defaulted`` quality reason.
"""
from __future__ import annotations

import hashlib

import polars as pl

from .canonical import RawValidationError
from .schema import (
    INTERVAL_DTYPES,
    MINUTE_US,
    REASON_BOOT_ID_CHANGE,
    REASON_CONFIG_CONFLICT,
    REASON_COUNTER_RESET,
    REASON_GAP_EXCEEDED,
    REASON_INGEST_CONFLICT,
    REASON_MULTIPLIER_DEFAULTED,
    SECOND_US,
)
from .settings import MeterSettings


def empty_intervals() -> pl.DataFrame:
    return pl.DataFrame(schema=INTERVAL_DTYPES)


def derive_intervals(
    canon_samples: pl.DataFrame,
    canon_config: pl.DataFrame,
    meter_settings: dict[str, MeterSettings],
) -> pl.DataFrame:
    if canon_samples.height == 0:
        return empty_intervals()

    pairs = (
        canon_samples.sort("meter_id", "event_time_us")
        .with_columns(
            pl.col("counter_wh").shift(1).over("meter_id").alias("prev_counter_wh"),
            pl.col("boot_id").shift(1).over("meter_id").alias("prev_boot_id"),
            pl.col("event_time_us").shift(1).over("meter_id").alias("start_us"),
            pl.col("event_time_utc").shift(1).over("meter_id").alias("start_utc"),
            pl.col("selected_ingest_seq")
            .shift(1)
            .over("meter_id")
            .alias("prev_ingest_seq"),
            pl.col("revision_count")
            .shift(1)
            .over("meter_id")
            .alias("prev_revision_count"),
            pl.col("conflicted").shift(1).over("meter_id").alias("prev_conflicted"),
        )
        .filter(pl.col("start_us").is_not_null())
        .rename(
            {
                "event_time_us": "end_us",
                "event_time_utc": "end_utc",
                "counter_wh": "next_counter_wh",
                "boot_id": "next_boot_id",
                "selected_ingest_seq": "next_ingest_seq",
                "revision_count": "next_revision_count",
                "conflicted": "next_conflicted",
            }
        )
        .sort("meter_id", "start_us")
    )

    thresholds = pl.DataFrame(
        [
            {"meter_id": m, "gap_threshold_us": ms.gap_threshold_us}
            for m, ms in meter_settings.items()
        ],
        schema={"meter_id": pl.String, "gap_threshold_us": pl.Int64},
    )
    pairs = pairs.join(thresholds, on="meter_id", how="left")
    unknown = pairs.filter(pl.col("gap_threshold_us").is_null())
    if unknown.height:
        raise RawValidationError(
            "no settings for meters " + str(unknown["meter_id"].unique().to_list())
        )

    pairs = pairs.with_columns(
        (pl.col("end_us") - pl.col("start_us")).alias("total_us"),
        (pl.col("next_counter_wh") - pl.col("prev_counter_wh")).alias("delta_wh"),
    )

    # ---- coverage validity caused by the samples alone ---------------------
    pairs = pairs.with_columns(
        pl.when(pl.col("prev_boot_id") != pl.col("next_boot_id"))
        .then(pl.lit([REASON_BOOT_ID_CHANGE]))
        .otherwise(pl.lit([]))
        .alias("_r_boot"),
        pl.when(pl.col("delta_wh") < 0)
        .then(pl.lit([REASON_COUNTER_RESET]))
        .otherwise(pl.lit([]))
        .alias("_r_reset"),
        pl.when(pl.col("total_us") > pl.col("gap_threshold_us"))
        .then(pl.lit([REASON_GAP_EXCEEDED]))
        .otherwise(pl.lit([]))
        .alias("_r_gap"),
        pl.when(pl.col("prev_conflicted") | pl.col("next_conflicted"))
        .then(pl.lit([REASON_INGEST_CONFLICT]))
        .otherwise(pl.lit([]))
        .alias("_r_ingest"),
    ).with_columns(
        pl.concat_list("_r_boot", "_r_reset", "_r_gap", "_r_ingest").alias(
            "_reason_sample"
        )
    )

    # ---- multiplier segmentation -------------------------------------------
    cfg = (
        canon_config.select(
            "meter_id",
            "effective_us",
            "multiplier",
            pl.col("conflicted").alias("config_conflicted"),
        )
        .sort("meter_id", "effective_us")
    )

    # config revision in force at each interval start
    base = pairs.select("meter_id", "start_us", "end_us").join(
        cfg, on="meter_id", how="inner"
    ).filter(pl.col("effective_us") <= pl.col("start_us"))
    base = (
        base.sort("meter_id", "start_us", "effective_us")
        .group_by("meter_id", "start_us")
        .agg(
            pl.col("multiplier").last().alias("_base_multiplier"),
            pl.col("config_conflicted").last().alias("_base_config_conflicted"),
        )
    )
    base = pairs.select("meter_id", "start_us", "end_us").join(
        base, on=["meter_id", "start_us"], how="left"
    )
    base = base.with_columns(
        pl.col("_base_multiplier").is_null().alias("_needs_default_at_start"),
        pl.col("_base_multiplier").fill_null(1.0).alias("_base_multiplier"),
        pl.col("_base_config_conflicted").fill_null(False),
    )

    # config revisions strictly inside each interval
    boundaries = (
        pairs.select("meter_id", "start_us", "end_us")
        .join(cfg, on="meter_id", how="inner")
        .filter(
            (pl.col("effective_us") > pl.col("start_us"))
            & (pl.col("effective_us") < pl.col("end_us"))
        )
        .select(
            "meter_id",
            "start_us",
            "end_us",
            pl.col("effective_us").alias("seg_start"),
            pl.col("multiplier").cast(pl.Float64),
            pl.col("config_conflicted").cast(pl.Int64).alias("_cfl_i"),
            pl.lit(0, dtype=pl.Int64).alias("_def_i"),
            pl.lit(1, dtype=pl.Int64).alias("_kind"),
        )
    )
    base_points = base.select(
        "meter_id",
        "start_us",
        "end_us",
        pl.col("start_us").alias("seg_start"),
        pl.col("_base_multiplier").alias("multiplier"),
        pl.col("_base_config_conflicted").cast(pl.Int64).alias("_cfl_i"),
        pl.col("_needs_default_at_start").cast(pl.Int64).alias("_def_i"),
        pl.lit(0, dtype=pl.Int64).alias("_kind"),
    )
    end_points = pairs.select(
        "meter_id",
        "start_us",
        "end_us",
        pl.col("end_us").alias("seg_start"),
        pl.lit(1.0, dtype=pl.Float64).alias("multiplier"),
        pl.lit(0, dtype=pl.Int64).alias("_cfl_i"),
        pl.lit(0, dtype=pl.Int64).alias("_def_i"),
        pl.lit(2, dtype=pl.Int64).alias("_kind"),
    )
    points = pl.concat([base_points, boundaries, end_points]).sort(
        "meter_id", "start_us", "seg_start", "_kind"
    )
    points = points.with_columns(
        pl.col("seg_start")
        .shift(-1)
        .over(["meter_id", "start_us"])
        .alias("seg_end")
    ).filter(pl.col("seg_end").is_not_null())

    points = points.with_columns(
        pl.struct(
            pl.col("seg_start").alias("start_us"),
            pl.col("seg_end").alias("end_us"),
            pl.col("multiplier"),
            pl.col("_cfl_i").cast(pl.Boolean).alias("config_conflicted"),
            pl.col("_def_i").cast(pl.Boolean).alias("defaulted"),
        ).alias("_segment")
    )
    segment_lists = points.group_by("meter_id", "start_us").agg(
        pl.col("_segment")
        .sort_by(pl.col("_segment").struct.field("start_us"))
        .alias("segments")
    )
    pairs = pairs.join(segment_lists, on=["meter_id", "start_us"], how="left")
    # meters with no config at all get one default segment spanning the whole
    # interval; this keeps the "no configuration => multiplier 1.0" rule.
    pairs = pairs.with_columns(
        pl.when(pl.col("segments").is_null())
        .then(
            pl.concat_list(
                pl.struct(
                    pl.col("start_us").alias("start_us"),
                    pl.col("end_us").alias("end_us"),
                    pl.lit(1.0, dtype=pl.Float64).alias("multiplier"),
                    pl.lit(False).alias("config_conflicted"),
                    pl.lit(True).alias("defaulted"),
                )
            )
        )
        .otherwise(pl.col("segments"))
        .alias("segments")
    )

    pairs = pairs.with_columns(
        pl.col("segments")
        .list.eval(
            pl.element().struct.field("multiplier")
            * (
                pl.element().struct.field("end_us")
                - pl.element().struct.field("start_us")
            )
        )
        .list.sum()
        .alias("_weighted_multiplier_us"),
        pl.col("segments")
        .list.eval(pl.when(pl.element().struct.field("defaulted")).then(1).otherwise(0))
        .list.sum()
        .fill_null(0)
        .alias("_defaulted_segments"),
        pl.col("segments")
        .list.eval(
            pl.when(pl.element().struct.field("config_conflicted")).then(1).otherwise(0)
        )
        .list.sum()
        .fill_null(0)
        .alias("_conflicted_segments"),
    )

    pairs = pairs.with_columns(
        pl.when(pl.col("_conflicted_segments") > 0)
        .then(pl.lit([REASON_CONFIG_CONFLICT]))
        .otherwise(pl.lit([]))
        .alias("_r_cfg"),
        pl.col("_defaulted_segments").gt(0).alias("multiplier_defaulted"),
    )
    pairs = pairs.with_columns(
        pl.concat_list("_reason_sample", "_r_cfg").alias("_reasons"),
    )
    pairs = pairs.with_columns(
        pl.col("_reasons").list.len().eq(0).alias("is_valid"),
        (pl.col("_weighted_multiplier_us") / pl.col("total_us")).alias(
            "weighted_multiplier"
        ),
        (
            pl.col("delta_wh")
            * pl.col("_weighted_multiplier_us")
            / pl.col("total_us")
        ).alias("_valid_energy"),
    )
    pairs = pairs.with_columns(
        pl.when(pl.col("is_valid"))
        .then(pl.col("_valid_energy"))
        .otherwise(pl.lit(0.0))
        .alias("expected_energy_wh"),
        pl.when(pl.col("multiplier_defaulted"))
        .then(pl.concat_list("_reasons", pl.lit([REASON_MULTIPLIER_DEFAULTED])))
        .otherwise(pl.col("_reasons"))
        .alias("reason_codes"),
    )

    _assert_segments_tile(pairs)

    return pairs.select(
        "meter_id",
        "start_us",
        "end_us",
        "start_utc",
        "end_utc",
        "total_us",
        (pl.col("total_us") / SECOND_US).alias("duration_seconds"),
        "prev_counter_wh",
        "next_counter_wh",
        "delta_wh",
        "prev_boot_id",
        "next_boot_id",
        "prev_ingest_seq",
        "next_ingest_seq",
        "prev_revision_count",
        "next_revision_count",
        "is_valid",
        "reason_codes",
        "segments",
        "multiplier_defaulted",
        "weighted_multiplier",
        "expected_energy_wh",
        ((pl.col("start_us") // MINUTE_US) * MINUTE_US).alias("first_minute_us"),
        ((pl.col("end_us") - 1) // MINUTE_US * MINUTE_US).alias("last_minute_us"),
        _content_hash_expr(),
    ).cast(INTERVAL_DTYPES).sort("meter_id", "start_us")


def _assert_segments_tile(pairs: pl.DataFrame) -> None:
    bad = pairs.filter(
        (pl.col("segments").list.len() == 0)
        | (pl.col("segments").list.first().struct.field("start_us") != pl.col("start_us"))
        | (pl.col("segments").list.last().struct.field("end_us") != pl.col("end_us"))
    )
    if bad.height:
        raise AssertionError(
            f"{bad.height} interval(s) have segments that do not tile the span; "
            f"first: {bad.head(1).select('meter_id', 'start_us', 'end_us').to_dicts()}"
        )


def _content_hash_expr() -> pl.Expr:
    """Stable 16-hex-digit fingerprint of everything that affects minute facts."""
    payload = pl.struct(
        "meter_id",
        "start_us",
        "end_us",
        "prev_counter_wh",
        "next_counter_wh",
        "prev_boot_id",
        "next_boot_id",
        "is_valid",
        "segments",
        "expected_energy_wh",
    ).struct.json_encode()
    return payload.map_elements(
        lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest()[:16] if s else None,
        return_dtype=pl.String,
        skip_nulls=False,
    ).alias("content_hash")
