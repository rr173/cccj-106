"""Full and incremental processing entry points.

The store on disk is the source of truth.  Raw samples/configs are appended;
canonical revisions are always recomputed from the complete raw log, so a
process restart recognises late samples and multiplier corrections.  Intervals
are persisted per meter and minute facts per (meter, timezone, local date).

An incremental run compares the freshly canonicalised revision set with the one
stored on disk, derives the affected sampling neighbourhoods, recomputes only
those intervals from a scoped slice of canonical samples, splices them into the
stored interval table and rewrites only the minute partitions that the affected
spans touch.  The list of affected intervals and minutes is persisted under
``affected/<run_id>/``.
"""
from __future__ import annotations

import bisect
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone as dt_timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from .canonical import canonicalize_configs, canonicalize_samples
from .intervals import derive_intervals
from .minutes import build_minutes
from .schema import (
    AFFECTED_INTERVAL_COLUMNS,
    AFFECTED_INTERVAL_DTYPES,
    AFFECTED_MINUTE_COLUMNS,
    AFFECTED_MINUTE_DTYPES,
    CANONICAL_CONFIG_DTYPES,
    CANONICAL_SAMPLE_DTYPES,
    INTERVAL_DTYPES,
    MINUTE_US,
)
from .settings import (
    MeterSettings,
    load_settings,
    meter_settings,
    settings_fingerprints,
)
from .storage import Store

SAMPLE_COMPARE_FIELDS = [
    "counter_wh",
    "boot_id",
    "selected_ingest_seq",
    "revision_count",
    "conflicted",
]
CONFIG_COMPARE_FIELDS = ["multiplier", "selected_config_seq", "conflicted"]

INTERVAL_COMPARE_FIELDS = [
    "end_us",
    "prev_counter_wh",
    "next_counter_wh",
    "prev_boot_id",
    "next_boot_id",
    "is_valid",
    "reason_codes",
    "segments",
    "expected_energy_wh",
]


@dataclass
class RunResult:
    run_id: str
    mode: str
    affected_intervals: pl.DataFrame
    affected_minutes: pl.DataFrame
    affected_meters: list[str]
    partitions_written: list[str] = field(default_factory=list)
    partitions_removed: list[str] = field(default_factory=list)
    intervals_recomputed: int = 0
    intervals_unchanged: int = 0
    elapsed_seconds: float = 0.0

    def summary(self) -> str:
        return (
            f"{self.mode} run {self.run_id}: meters={len(self.affected_meters)} "
            f"affected_intervals={self.affected_intervals.height} "
            f"(unchanged_but_recomputed={self.intervals_unchanged}) "
            f"affected_minutes={self.affected_minutes.height} "
            f"partitions_written={len(self.partitions_written)} "
            f"partitions_removed={len(self.partitions_removed)} "
            f"in {self.elapsed_seconds:.3f}s"
        )


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def _signature(df: pl.DataFrame, keys: list[str], fields: list[str]) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(
            {**{k: [] for k in keys}, "_sig": []},
            schema={**{k: df.schema.get(k, pl.String) for k in keys}, "_sig": pl.String},
        )
    payload = pl.struct([pl.col(f) for f in fields]).struct.json_encode()
    return df.select(keys + [payload.alias("_sig")])


def changed_instants(
    old: pl.DataFrame, new: pl.DataFrame, fields: list[str]
) -> dict[str, list[int]]:
    """Instants that were added, removed or whose payload changed."""
    keys = ["meter_id", "event_time_us"]
    old_sig = _signature(old, keys, fields)
    new_sig = _signature(new, keys, fields)
    joined = old_sig.join(
        new_sig, on=keys, how="full", coalesce=True, suffix="_new"
    )
    if joined.height == 0:
        return {}
    changed = joined.filter(pl.col("_sig").ne_missing(pl.col("_sig_new")))
    out: dict[str, list[int]] = {}
    for meter, group in changed.partition_by(
        ["meter_id"], include_key=True, as_dict=True
    ).items():
        out[meter[0]] = sorted(set(group["event_time_us"].to_list()))
    return out


def changed_config_instants(
    old: pl.DataFrame, new: pl.DataFrame
) -> dict[str, list[int]]:
    keys = ["meter_id", "effective_us"]
    old_sig = _signature(old, keys, CONFIG_COMPARE_FIELDS)
    new_sig = _signature(new, keys, CONFIG_COMPARE_FIELDS)
    joined = old_sig.join(new_sig, on=keys, how="full", coalesce=True, suffix="_new")
    if joined.height == 0:
        return {}
    changed = joined.filter(pl.col("_sig").ne_missing(pl.col("_sig_new")))
    out: dict[str, list[int]] = {}
    for meter, group in changed.partition_by(
        ["meter_id"], include_key=True, as_dict=True
    ).items():
        out[meter[0]] = sorted(set(group["effective_us"].to_list()))
    return out


def _merge_spans(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for s, e in sorted(spans):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def sample_windows(
    union_instants: list[int], changed: list[int]
) -> list[tuple[int, int]]:
    """Neighbourhood spans [prev, next] around each changed sample instant.

    ``prev`` is the closest surviving instant at or before the change and
    ``next`` the closest instant at or after it (the change itself when it is
    an endpoint).  Every old or new interval touching the change lies inside.
    """
    spans = []
    for t in changed:
        i = bisect.bisect_left(union_instants, t)
        pred = union_instants[i - 1] if i > 0 else t
        nxt = union_instants[i + 1] if i + 1 < len(union_instants) else t
        spans.append((pred, nxt))
    return _merge_spans(spans)


def config_windows(
    union_instants: list[int],
    changed_cfg: list[int],
    config_instants: list[int],
) -> list[tuple[int, int]]:
    """Spans whose intervals may change because of a config revision change.

    A config at ``t`` changes (a) the segment layout of the interval whose
    span contains ``t`` and (b) the base multiplier of every interval starting
    in ``[t, next_config)``.  Both are covered by the sample span from the
    last sample at or before ``t`` to the first sample at or after the next
    config instant.
    """
    spans = []
    for t in changed_cfg:
        starts = [c for c in config_instants if c > t]
        horizon = starts[0] if starts else None
        i = bisect.bisect_left(union_instants, t)
        lo = union_instants[i - 1] if i > 0 else (union_instants[0] if union_instants else t)
        if horizon is None:
            hi = union_instants[-1] if union_instants else t
        else:
            j = bisect.bisect_left(union_instants, horizon)
            hi = union_instants[j] if j < len(union_instants) else union_instants[-1]
        if lo < hi:
            spans.append((lo, hi))
    return _merge_spans(spans)


def interval_signatures(df: pl.DataFrame, fields: list[str]) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={"meter_id": pl.String, "start_us": pl.Int64, "_sig": pl.String})
    payload = pl.struct([pl.col(f) for f in fields]).struct.json_encode()
    return df.select("meter_id", "start_us", payload.alias("_sig"))


def local_day_utc_bounds(local_date: date, tz: str) -> tuple[int, int]:
    zone = ZoneInfo(tz)
    start = datetime.combine(local_date, dtime(0, 0), tzinfo=zone)
    end = datetime.combine(local_date + timedelta(days=1), dtime(0, 0), tzinfo=zone)
    return int(start.timestamp() * 1_000_000), int(end.timestamp() * 1_000_000)


def touched_partitions(
    intervals: pl.DataFrame, meter_map: dict[str, MeterSettings]
) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    if intervals.height == 0:
        return out
    rows = intervals.select("meter_id", "start_us", "end_us").to_dicts()
    for row in rows:
        zone = ZoneInfo(meter_map[row["meter_id"]].timezone)
        day = datetime.fromtimestamp(row["start_us"] / 1e6, tz=dt_timezone.utc).astimezone(zone).date()
        last = datetime.fromtimestamp((row["end_us"] - 1) / 1e6, tz=dt_timezone.utc).astimezone(zone).date()
        while day <= last:
            out.add((row["meter_id"], day.isoformat()))
            day += timedelta(days=1)
    return out


def minute_report(
    run_id: str, old_intervals: pl.DataFrame, new_intervals: pl.DataFrame
) -> pl.DataFrame:
    """Every UTC minute touched by an old or a new affected interval."""
    frames = []
    for frame, role in ((old_intervals, "old"), (new_intervals, "new")):
        if frame.height == 0:
            continue
        expanded = frame.select("meter_id", "start_us", "end_us").with_columns(
            (
                (pl.col("end_us") - 1) // MINUTE_US - pl.col("start_us") // MINUTE_US + 1
            ).alias("n")
        )
        expanded = (
            expanded.with_columns(pl.int_ranges(0, pl.col("n")).alias("i"))
            .explode("i")
            .with_columns(
                (pl.col("start_us") // MINUTE_US * MINUTE_US + pl.col("i") * MINUTE_US).alias(
                    "minute_epoch_us"
                )
            )
            .filter((pl.col("minute_epoch_us") >= pl.col("start_us")) & (pl.col("minute_epoch_us") < pl.col("end_us")))
        )
        frames.append(expanded.select("meter_id", "minute_epoch_us", "start_us", "end_us"))
    if not frames:
        return pl.DataFrame(schema=AFFECTED_MINUTE_DTYPES)
    union = pl.concat(frames).group_by("meter_id", "minute_epoch_us").agg(
        pl.col("start_us").min().alias("first_causing_start_us"),
        pl.col("end_us").max().alias("last_causing_end_us"),
        pl.len().cast(pl.UInt32).alias("causing_intervals"),
    )
    return (
        union.with_columns(pl.lit(run_id).alias("run_id"))
        .select(AFFECTED_MINUTE_COLUMNS)
        .cast(AFFECTED_MINUTE_DTYPES)
        .sort("meter_id", "minute_epoch_us")
    )


def interval_report(
    run_id: str, old_intervals: pl.DataFrame, new_intervals: pl.DataFrame
) -> pl.DataFrame:
    """Added / removed / revised intervals among the recomputed neighbourhoods.

    A row is reported for every recomputed interval whose stored form was
    absent, disappeared, or changed in any field that affects the minute facts.
    ``changed_fields`` lists the differing fields of revised intervals.
    """
    if old_intervals.height == 0 and new_intervals.height == 0:
        return pl.DataFrame(schema=AFFECTED_INTERVAL_DTYPES)

    def keyed(df: pl.DataFrame) -> dict[tuple[str, int], dict]:
        if df.height == 0:
            return {}
        wanted = sorted(
            set(
                [
                    "meter_id",
                    "start_us",
                    "start_utc",
                    "expected_energy_wh",
                    "content_hash",
                    "first_minute_us",
                    "last_minute_us",
                    "reason_codes",
                ]
                + INTERVAL_COMPARE_FIELDS
            )
        )
        wanted = [c for c in wanted if c in df.columns]
        return {
            (r["meter_id"], r["start_us"]): r
            for r in df.select(wanted)
            .unique(subset=["meter_id", "start_us"])
            .to_dicts()
        }

    old_by_key = keyed(old_intervals)
    new_by_key = keyed(new_intervals)
    rows = []
    for key in sorted(set(old_by_key) | set(new_by_key)):
        old = old_by_key.get(key)
        new = new_by_key.get(key)
        if old is None:
            kind, cf = "added", []
        elif new is None:
            kind, cf = "removed", []
        else:
            cf = [
                f
                for f in INTERVAL_COMPARE_FIELDS
                if not _values_equal(old.get(f), new.get(f))
            ]
            if not cf:
                continue
            kind = "revised"
        src = new if new is not None else old
        minutes = 0
        if src.get("start_us") is not None and src.get("end_us") is not None:
            minutes = (src["end_us"] - 1) // MINUTE_US - src["start_us"] // MINUTE_US + 1
        rows.append(
            {
                "run_id": run_id,
                "change_kind": kind,
                "meter_id": key[0],
                "start_us": key[1],
                "end_us": src.get("end_us"),
                "start_utc": src.get("start_utc"),
                "end_utc": src.get("end_utc"),
                "is_valid": src.get("is_valid"),
                "reason_codes": src.get("reason_codes"),
                "prev_counter_wh": src.get("prev_counter_wh"),
                "next_counter_wh": src.get("next_counter_wh"),
                "delta_wh": src.get("delta_wh"),
                "expected_energy_wh": src.get("expected_energy_wh"),
                "previous_expected_energy_wh": old.get("expected_energy_wh") if old else None,
                "previous_content_hash": old.get("content_hash") if old else None,
                "content_hash": src.get("content_hash"),
                "changed_fields": cf,
                "first_minute_us": src.get("first_minute_us"),
                "last_minute_us": src.get("last_minute_us"),
                "minutes_touched": int(minutes),
            }
        )
    if not rows:
        return pl.DataFrame(schema=AFFECTED_INTERVAL_DTYPES)
    frame = pl.DataFrame(rows, schema=AFFECTED_INTERVAL_DTYPES)
    return frame.sort("meter_id", "start_us")


def _values_equal(a, b) -> bool:
    if a is None and b is None:
        return True
    if isinstance(a, list) and isinstance(b, list):
        return a == b
    return bool(a == b)


# ---------------------------------------------------------------------------
# public entry points
# ---------------------------------------------------------------------------
def full_run(
    store_root: str | Path, settings_path: str | Path, run_id: str | None = None
) -> RunResult:
    started = time.perf_counter()
    run_id = run_id or _new_run_id("full")
    store = Store(store_root)
    settings = load_settings(settings_path)

    raw_samples = store.read_raw_samples()
    raw_configs = store.read_raw_configs()
    canon, sample_conflicts = canonicalize_samples(raw_samples)
    canon_cfg, config_conflicts = canonicalize_configs(raw_configs)
    store.write_canonical(canon, canon_cfg, sample_conflicts, config_conflicts)

    meter_map = _meter_map(canon, canon_cfg, settings)
    intervals = derive_intervals(canon, canon_cfg, meter_map)
    store.replace_all_intervals(intervals)

    minutes = build_minutes(intervals, meter_map)
    written = store.replace_all_fact_partitions(minutes)
    store.write_settings_fingerprint(settings_fingerprints(settings, sorted(meter_map)))

    empty_i = pl.DataFrame(schema=AFFECTED_INTERVAL_DTYPES)
    empty_m = pl.DataFrame(schema=AFFECTED_MINUTE_DTYPES)
    store.write_affected(run_id, empty_i, empty_m)
    elapsed = round(time.perf_counter() - started, 3)
    store.write_run_meta(
        run_id,
        {
            "run_id": run_id,
            "mode": "full",
            "raw_samples": raw_samples.height,
            "raw_configs": raw_configs.height,
            "canonical_samples": canon.height,
            "canonical_configs": canon_cfg.height,
            "conflicts": sample_conflicts.height + config_conflicts.height,
            "intervals": intervals.height,
            "minute_rows": minutes.height,
            "partitions_written": len(written),
            "elapsed_seconds": elapsed,
        },
    )
    return RunResult(
        run_id=run_id,
        mode="full",
        affected_intervals=empty_i,
        affected_minutes=empty_m,
        affected_meters=sorted(meter_map),
        partitions_written=[str(p) for p in written],
        elapsed_seconds=elapsed,
    )


def incremental_run(
    store_root: str | Path,
    settings_path: str | Path,
    run_id: str | None = None,
    new_samples: pl.DataFrame | None = None,
    new_configs: pl.DataFrame | None = None,
) -> RunResult:
    started = time.perf_counter()
    run_id = run_id or _new_run_id("inc")
    store = Store(store_root)
    settings = load_settings(settings_path)

    if (new_samples is not None and new_samples.height) or (
        new_configs is not None and new_configs.height
    ):
        store.append_raw(new_samples, new_configs)

    old_canon = store.read_canonical_samples()
    old_cfg = store.read_canonical_configs()
    old_intervals = store.read_intervals()

    canon, sample_conflicts = canonicalize_samples(store.read_raw_samples())
    canon_cfg, config_conflicts = canonicalize_configs(store.read_raw_configs())
    store.write_canonical(canon, canon_cfg, sample_conflicts, config_conflicts)

    meter_map = _meter_map(canon, canon_cfg, settings)

    cfg_changed = changed_config_instants(old_cfg, canon_cfg)
    sample_changed = changed_instants(old_canon, canon, SAMPLE_COMPARE_FIELDS)

    previous_fp = store.read_settings_fingerprint()
    current_fp = settings_fingerprints(settings, sorted(meter_map))
    settings_changed = sorted(
        m
        for m in meter_map
        if m in previous_fp
        and (
            previous_fp[m].get("timezone") != current_fp[m][0]
            or previous_fp[m].get("gap_threshold_seconds") != current_fp[m][1]
        )
    )

    affected_meters = sorted(set(cfg_changed) | set(sample_changed) | set(settings_changed))
    if not affected_meters:
        store.write_settings_fingerprint(current_fp)
        empty_i = pl.DataFrame(schema=AFFECTED_INTERVAL_DTYPES)
        empty_m = pl.DataFrame(schema=AFFECTED_MINUTE_DTYPES)
        store.write_affected(run_id, empty_i, empty_m)
        elapsed = round(time.perf_counter() - started, 3)
        store.write_run_meta(
            run_id,
            {
                "run_id": run_id,
                "mode": "incremental",
                "no_op": True,
                "affected_meters": [],
                "elapsed_seconds": elapsed,
            },
        )
        return RunResult(run_id, "incremental", empty_i, empty_m, [], elapsed_seconds=elapsed)

    union_instants: dict[str, list[int]] = {}
    union_frame = pl.concat(
        [
            old_canon.select("meter_id", "event_time_us"),
            canon.select("meter_id", "event_time_us"),
        ]
    ).unique()
    for meter, group in union_frame.partition_by(
        ["meter_id"], include_key=True, as_dict=True
    ).items():
        union_instants[meter[0]] = sorted(group["event_time_us"].to_list())

    config_instants: dict[str, list[int]] = {}
    cfg_union = pl.concat(
        [
            old_cfg.select("meter_id", "effective_us"),
            canon_cfg.select("meter_id", "effective_us"),
        ]
    ).unique()
    for meter, group in cfg_union.partition_by(
        ["meter_id"], include_key=True, as_dict=True
    ).items():
        config_instants[meter[0]] = sorted(group["effective_us"].to_list())

    windows: dict[str, list[tuple[int, int]]] = {}
    for meter in affected_meters:
        instants = union_instants.get(meter, [])
        spans: list[tuple[int, int]] = []
        if meter in sample_changed:
            spans += sample_windows(instants, sample_changed[meter])
        if meter in cfg_changed:
            spans += config_windows(
                instants, cfg_changed[meter], config_instants.get(meter, [])
            )
        if meter in settings_changed and instants:
            spans.append((instants[0], instants[-1]))
        windows[meter] = _merge_spans(spans)

    new_scoped = _recompute_scoped(canon, canon_cfg, meter_map, windows)

    # ---- splice intervals --------------------------------------------------
    # Only intervals overlapping an affected window can change.  Everything
    # else of the same meter is carried over byte-for-byte.
    window_frame = pl.DataFrame(
        [
            {"meter_id": m, "_w0": s, "_w1": e}
            for m, spans in windows.items()
            for s, e in spans
        ],
        schema={"meter_id": pl.String, "_w0": pl.Int64, "_w1": pl.Int64},
    )
    if window_frame.height:
        overlapping = (
            old_intervals.join(window_frame, on="meter_id", how="inner")
            .filter((pl.col("start_us") < pl.col("_w1")) & (pl.col("end_us") > pl.col("_w0")))
            .select("meter_id", "start_us")
            .unique()
            .with_columns(pl.lit(True).alias("_hit"))
        )
        old_affected = old_intervals.join(
            overlapping, on=["meter_id", "start_us"], how="left"
        ).filter(pl.col("_hit").is_not_null()).drop("_hit")
    else:
        old_affected = pl.DataFrame(schema=INTERVAL_DTYPES)
    merged_affected = new_scoped
    # rows overlapping a window are superseded *in their entirety* -- even when
    # the recomputation no longer produces a replacement for them (e.g. the
    # only remaining sample of a window is deleted).  Everything else is
    # carried over untouched.
    replaced_keys = old_affected.select("meter_id", "start_us").unique().with_columns(
        pl.lit(True).alias("_replaced")
    )
    carried = old_intervals.join(
        replaced_keys, on=["meter_id", "start_us"], how="left"
    ).filter(pl.col("_replaced").is_null()).drop("_replaced")
    intervals_all = (
        pl.concat([carried, merged_affected], how="vertical_relaxed")
        .cast(INTERVAL_DTYPES)
        .sort("meter_id", "start_us")
    )
    for meter in affected_meters:
        store.write_intervals_for_meter(
            meter, intervals_all.filter(pl.col("meter_id") == meter)
        )

    # ---- minute partitions --------------------------------------------------
    partitions = touched_partitions(old_affected, meter_map) | touched_partitions(
        new_scoped, meter_map
    )
    existing = store.existing_fact_partitions()
    written_paths: list[str] = []
    removed_paths: list[str] = []
    # A timezone setting change moves minutes to different local dates; stale
    # partitions under the previous timezone/date must not survive.
    for meter in settings_changed:
        wanted = {d for (m, d) in partitions if m == meter}
        stale = {
            (m, d) for (m, d) in existing if m == meter and d not in wanted
        }
        for m, d in sorted(stale):
            removed = store.delete_fact_partition(m, d)
            if removed:
                removed_paths.append(removed)
    for meter, date_iso in sorted(partitions):
        ms = meter_map[meter]
        local_date = date.fromisoformat(date_iso)
        day_start, day_end = local_day_utc_bounds(local_date, ms.timezone)
        slices = intervals_all.filter(
            (pl.col("meter_id") == meter)
            & (pl.col("start_us") < day_end)
            & (pl.col("end_us") > day_start)
        )
        part = build_minutes(slices, {meter: ms}, meter_ids=[meter], dates=[local_date])
        if part.height == 0:
            removed = store.delete_fact_partition(meter, date_iso)
            if removed:
                removed_paths.append(removed)
            continue
        written = store.write_fact_partitions(part)
        written_paths.extend(str(p) for p in written)

    report_minutes = minute_report(run_id, old_affected, new_scoped)
    report = interval_report(run_id, old_affected, new_scoped)
    if report.height:
        counts = report_minutes.group_by("meter_id").agg(
            pl.col("minute_epoch_us").n_unique().cast(pl.UInt32).alias("_mt")
        )
        report = report.join(counts, on="meter_id", how="left").with_columns(
            pl.col("_mt").fill_null(0)
        ).drop("minutes_touched").with_columns(pl.col("_mt").alias("minutes_touched")).drop("_mt")

    recomputed = new_scoped.height
    unchanged = max(0, recomputed - report.height)
    store.write_settings_fingerprint(current_fp)
    store.write_affected(run_id, report, report_minutes)
    elapsed = round(time.perf_counter() - started, 3)
    store.write_run_meta(
        run_id,
        {
            "run_id": run_id,
            "mode": "incremental",
            "affected_meters": affected_meters,
            "changed_sample_instants": {m: len(v) for m, v in sample_changed.items()},
            "changed_config_instants": {m: len(v) for m, v in cfg_changed.items()},
            "settings_changed_meters": settings_changed,
            "windows": {m: windows[m] for m in affected_meters},
            "intervals_recomputed": recomputed,
            "affected_intervals": report.height,
            "affected_minutes": report_minutes.height,
            "partitions_written": len(written_paths),
            "partitions_removed": len(removed_paths),
            "elapsed_seconds": elapsed,
        },
    )
    return RunResult(
        run_id=run_id,
        mode="incremental",
        affected_intervals=report,
        affected_minutes=report_minutes,
        affected_meters=affected_meters,
        partitions_written=written_paths,
        partitions_removed=removed_paths,
        intervals_recomputed=recomputed,
        intervals_unchanged=unchanged,
        elapsed_seconds=elapsed,
    )


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------
def _meter_map(
    canon: pl.DataFrame, canon_cfg: pl.DataFrame, settings: dict
) -> dict[str, MeterSettings]:
    meter_ids = set(canon["meter_id"].to_list()) | set(canon_cfg["meter_id"].to_list())
    meter_ids |= set(settings.get("meters", {}))
    return {m: meter_settings(settings, m) for m in sorted(meter_ids)}


def _recompute_scoped(
    canon: pl.DataFrame,
    canon_cfg: pl.DataFrame,
    meter_map: dict[str, MeterSettings],
    windows: dict[str, list[tuple[int, int]]],
) -> pl.DataFrame:
    parts = []
    for meter, spans in windows.items():
        if not spans:
            continue
        meter_samples = canon.filter(pl.col("meter_id") == meter).sort("event_time_us")
        meter_cfg = canon_cfg.filter(pl.col("meter_id") == meter)
        for start, end in spans:
            scoped = meter_samples.filter(
                (pl.col("event_time_us") >= start) & (pl.col("event_time_us") <= end)
            )
            if scoped.height < 2:
                continue
            derived = derive_intervals(scoped, meter_cfg, meter_map)
            derived = derived.filter(
                (pl.col("start_us") >= start) & (pl.col("end_us") <= end)
            )
            if derived.height:
                parts.append(derived)
    if not parts:
        return pl.DataFrame(schema=INTERVAL_DTYPES)
    combined = pl.concat(parts, how="vertical_relaxed").cast(INTERVAL_DTYPES)
    # overlapping scoped derivations are possible only for merged windows of the
    # same meter, which are disjoint; dedupe defensively by interval key.
    return combined.unique(subset=["meter_id", "start_us"], keep="first", maintain_order=True).sort(
        "meter_id", "start_us"
    )


def _splice(
    old_affected: pl.DataFrame, new_scoped: pl.DataFrame
) -> pl.DataFrame:
    """Drop every stored row overlapping a recomputed span; insert the new rows.

    ``old_affected`` must already be restricted to rows overlapping an
    affected window, so anything dropped here is genuinely superseded.
    """
    if old_affected.height == 0:
        return new_scoped.sort("meter_id", "start_us")
    if new_scoped.height == 0:
        return pl.DataFrame(schema=INTERVAL_DTYPES)
    spans = new_scoped.select(
        "meter_id",
        pl.col("start_us").alias("_w0"),
        pl.col("end_us").alias("_w1"),
    )
    joined = old_affected.join(spans, on="meter_id", how="inner")
    drop = (
        joined.filter((pl.col("start_us") < pl.col("_w1")) & (pl.col("end_us") > pl.col("_w0")))
        .select("meter_id", "start_us")
        .unique()
        .with_columns(pl.lit(True).alias("_drop"))
    )
    keep = old_affected.join(drop, on=["meter_id", "start_us"], how="left").filter(
        pl.col("_drop").is_null()
    ).drop("_drop")
    return pl.concat([keep, new_scoped], how="vertical_relaxed").cast(
        INTERVAL_DTYPES
    ).sort("meter_id", "start_us")


def _new_run_id(mode: str) -> str:
    return f"{mode}-{datetime.now(dt_timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
