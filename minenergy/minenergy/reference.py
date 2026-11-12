"""Independent, deliberately naive reference implementation.

Nothing here shares code with the Polars pipeline (except the raw-row
validation semantics documented in the README).  It uses plain Python
dictionaries, :mod:`fractions`, and :mod:`zoneinfo`:

* canonical samples are resolved by simple sorting;
* every pair of consecutive samples yields at most one interval;
* multiplier segments are built by walking the effective configs;
* each minute fact is accumulated in a per-minute dict with exact
  :class:`fractions.Fraction` energy;
* local calendar fields are computed by converting each UTC minute to the
  meter's IANA zone with :mod:`zoneinfo`.

The reference is intentionally quadratic-ish so it is obviously correct on the
small fixtures and randomized scenarios; it is not a production path.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from fractions import Fraction
from zoneinfo import ZoneInfo

import polars as pl

MINUTE_US = 60_000_000
SECOND_US = 1_000_000

INVALIDATING = {
    "boot_id_change",
    "counter_reset",
    "gap_exceeded",
    "ingest_conflict",
    "config_conflict",
}


def parse_utc_us(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"missing UTC offset: {value!r}")
    return int(dt.timestamp() * 1_000_000)


def canonical_samples(raw: list[dict]) -> list[dict]:
    """One dict per (meter, instant) with max ingest_seq; conflicts flagged."""
    buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in raw:
        buckets[(row["meter_id"], parse_utc_us(row["event_time"]))].append(row)

    canon = []
    for (meter, instant), rows in sorted(buckets.items()):
        max_seq = max(r["ingest_seq"] for r in rows)
        top = [r for r in rows if r["ingest_seq"] == max_seq]
        # deterministic pick among the winning sequence's payloads
        winner = min(top, key=lambda r: (r["counter_wh"], r["boot_id"]))
        seq_payloads = defaultdict(set)
        for r in rows:
            seq_payloads[r["ingest_seq"]].add((r["counter_wh"], r["boot_id"]))
        conflicting = sorted(
            seq for seq, payloads in seq_payloads.items() if len(payloads) > 1
        )
        canon.append(
            {
                "meter_id": meter,
                "event_time_us": instant,
                "counter_wh": winner["counter_wh"],
                "boot_id": winner["boot_id"],
                "selected_ingest_seq": max_seq,
                "revision_count": len({r["ingest_seq"] for r in rows}),
                "conflicted": bool(conflicting),
            }
        )
    return canon


def canonical_configs(raw: list[dict]) -> list[dict]:
    buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in raw:
        buckets[(row["meter_id"], parse_utc_us(row["effective_from"]))].append(row)
    out = []
    for (meter, instant), rows in sorted(buckets.items()):
        max_seq = max(r["config_seq"] for r in rows)
        winner = min(
            (r for r in rows if r["config_seq"] == max_seq),
            key=lambda r: r["multiplier"],
        )
        seq_vals = defaultdict(set)
        for r in rows:
            seq_vals[r["config_seq"]].add(r["multiplier"])
        conflicting = any(len(v) > 1 for v in seq_vals.values())
        out.append(
            {
                "meter_id": meter,
                "effective_us": instant,
                "multiplier": winner["multiplier"],
                "selected_config_seq": max_seq,
                "conflicted": conflicting,
            }
        )
    return out


def derive_intervals(
    samples: list[dict], configs: list[dict], meter_cfg: dict[str, dict]
) -> list[dict]:
    by_meter = defaultdict(list)
    for s in samples:
        by_meter[s["meter_id"]].append(s)
    cfg_by_meter = defaultdict(list)
    for c in configs:
        cfg_by_meter[c["meter_id"]].append(c)
    conflict_meters = {
        c["meter_id"] for c in configs if c["conflicted"]
    }

    intervals = []
    for meter, rows in by_meter.items():
        rows.sort(key=lambda r: r["event_time_us"])
        settings = meter_cfg[meter]
        gap_limit = int(settings["gap_threshold_seconds"]) * SECOND_US
        cfgs = sorted(cfg_by_meter.get(meter, []), key=lambda c: c["effective_us"])
        for prev, nxt in zip(rows, rows[1:]):
            start, end = prev["event_time_us"], nxt["event_time_us"]
            total = end - start
            delta = nxt["counter_wh"] - prev["counter_wh"]
            reasons = []
            if prev["boot_id"] != nxt["boot_id"]:
                reasons.append("boot_id_change")
            if delta < 0:
                reasons.append("counter_reset")
            if total > gap_limit:
                reasons.append("gap_exceeded")
            if prev["conflicted"] or nxt["conflicted"]:
                reasons.append("ingest_conflict")

            # multiplier segments
            base = None
            for c in cfgs:
                if c["effective_us"] <= start:
                    base = c
            defaulted = base is None
            if base is None:
                base_mult, base_conf = Fraction(1), False
            else:
                base_mult, base_conf = Fraction(base["multiplier"]).limit_denominator(10**9), base["conflicted"]
            points = [(start, base_mult, base_conf, defaulted)]
            for c in cfgs:
                if start < c["effective_us"] < end:
                    points.append(
                        (
                            c["effective_us"],
                            Fraction(c["multiplier"]).limit_denominator(10**9),
                            c["conflicted"],
                            False,
                        )
                    )
            points.append((end, Fraction(1), False, False))
            segments = []
            conflicted_any = False
            defaulted_any = False
            for (p0, m0, c0, d0), (p1, _, _, _) in zip(points, points[1:]):
                segments.append(
                    {
                        "start_us": p0,
                        "end_us": p1,
                        "multiplier": m0,
                        "config_conflicted": c0,
                        "defaulted": d0,
                    }
                )
                conflicted_any |= c0
                defaulted_any |= d0
            if conflicted_any and "config_conflict" not in reasons:
                reasons.append("config_conflict")
            if defaulted_any:
                reasons.append("multiplier_defaulted")

            valid = not any(r in INVALIDATING for r in reasons)
            weighted = sum(
                seg["multiplier"] * (seg["end_us"] - seg["start_us"]) for seg in segments
            )
            weighted /= Fraction(total)
            energy = Fraction(delta) * weighted if valid else Fraction(0)

            intervals.append(
                {
                    "meter_id": meter,
                    "start_us": start,
                    "end_us": end,
                    "total_us": total,
                    "prev_counter_wh": prev["counter_wh"],
                    "next_counter_wh": nxt["counter_wh"],
                    "delta_wh": delta,
                    "is_valid": valid,
                    "reason_codes": sorted(reasons),
                    "segments": segments,
                    "multiplier_defaulted": defaulted_any,
                    "weighted_multiplier": float(weighted),
                    "expected_energy_wh": float(energy),
                    "energy_exact": energy,
                }
            )
    intervals.sort(key=lambda i: (i["meter_id"], i["start_us"]))
    return intervals


def build_minutes(
    intervals: list[dict], meter_cfg: dict[str, dict]
) -> dict[tuple[str, int], dict]:
    facts: dict[tuple[str, int], dict] = {}

    def bucket(meter, minute):
        key = (meter, minute)
        if key not in facts:
            facts[key] = {
                "meter_id": meter,
                "minute_epoch_us": minute,
                "energy_exact": Fraction(0),
                "valid_us": 0,
                "invalid_us": 0,
                "reasons": set(),
                "invalid_reasons": [],
                "valid_intervals": set(),
                "invalid_intervals": set(),
            }
        return facts[key]

    for iv in intervals:
        if iv["is_valid"]:
            for seg in iv["segments"]:
                s, e = seg["start_us"], seg["end_us"]
                m0 = s // MINUTE_US * MINUTE_US
                while m0 < e:
                    m1 = m0 + MINUTE_US
                    lo, hi = max(s, m0), min(e, m1)
                    if hi > lo:
                        f = bucket(iv["meter_id"], m0)
                        exact = (
                            Fraction(iv["delta_wh"])
                            * seg["multiplier"]
                            * (hi - lo)
                            / iv["total_us"]
                        )
                        f["energy_exact"] += exact
                        f["valid_us"] += hi - lo
                        f["valid_intervals"].add(iv["start_us"])
                        f["reasons"].update(iv["reason_codes"])
                    m0 = m1
        else:
            s, e = iv["start_us"], iv["end_us"]
            m0 = s // MINUTE_US * MINUTE_US
            while m0 < e:
                m1 = m0 + MINUTE_US
                lo, hi = max(s, m0), min(e, m1)
                if hi > lo:
                    f = bucket(iv["meter_id"], m0)
                    f["invalid_us"] += hi - lo
                    f["invalid_intervals"].add(iv["start_us"])
                    f["invalid_reasons"].append((hi - lo, set(iv["reason_codes"])))
                m0 = m1

    for f in facts.values():
        reasons = sorted(f["reasons"])
        per_reason = defaultdict(int)
        for dur, rset in f["invalid_reasons"]:
            for r in rset:
                per_reason[r] = max(per_reason[r], dur)
        f["quality_reasons"] = sorted(set(reasons) | set(per_reason))
        f["invalid_by_reason"] = per_reason
        f["interval_count"] = len(f["valid_intervals"] | f["invalid_intervals"])
        f["valid_intervals"] = len(f["valid_intervals"])
        f["invalid_intervals"] = len(f["invalid_intervals"])
    return facts


def minute_rows(
    facts: dict[tuple[str, int], dict], meter_cfg: dict[str, dict]
) -> list[dict]:
    out = []
    for (meter, minute), f in sorted(facts.items()):
        tz = ZoneInfo(meter_cfg[meter]["timezone"])
        local = datetime.fromtimestamp(minute / 1e6, tz=timezone.utc).astimezone(tz)
        offset = int(local.utcoffset().total_seconds())
        half_open_end = datetime.fromtimestamp(
            (minute + MINUTE_US) / 1e6, tz=timezone.utc
        ).astimezone(tz)
        label = local.strftime("%Y-%m-%dT%H:%M%z")
        coverage = f["valid_us"] + f["invalid_us"]
        if f["valid_us"] == 0:
            status = "invalid_only"
        elif f["invalid_us"] == 0:
            status = "full_valid" if coverage == MINUTE_US else "partial_valid"
        else:
            status = "mixed"
        out.append(
            {
                "meter_id": meter,
                "minute_epoch_us": minute,
                "timezone": meter_cfg[meter]["timezone"],
                "local_date": local.date().isoformat(),
                "local_minute": local.strftime("%Y-%m-%dT%H:%M"),
                "local_minute_label": label,
                "utc_offset_seconds": offset,
                "second_local": half_open_end.strftime("%Y-%m-%dT%H:%M%z"),
                "energy_wh": float(f["energy_exact"]),
                "energy_exact": f["energy_exact"],
                "valid_us": f["valid_us"],
                "invalid_us": f["invalid_us"],
                "coverage_us": coverage,
                "coverage_status": status,
                "quality_reasons": f["quality_reasons"],
                "invalid_by_reason": sorted(
                    (r, us) for r, us in f["invalid_by_reason"].items()
                ),
                "is_zero_usage": f["valid_us"] > 0 and f["energy_exact"] == 0,
                "has_quality_issue": bool(f["quality_reasons"]),
                "interval_count": f["interval_count"],
            }
        )
    return out


def reference_run(
    raw_samples: list[dict], raw_configs: list[dict], meter_cfg: dict[str, dict]
) -> dict:
    samples = canonical_samples(raw_samples)
    configs = canonical_configs(raw_configs)
    intervals = derive_intervals(samples, configs, meter_cfg)
    facts = build_minutes(intervals, meter_cfg)
    rows = minute_rows(facts, meter_cfg)
    return {"samples": samples, "configs": configs, "intervals": intervals, "minutes": rows}
