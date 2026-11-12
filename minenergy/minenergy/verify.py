"""Hand-written fixtures, randomized differential runs and invariant checks."""
from __future__ import annotations

import json
import random
import shutil
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from .canonical import canonicalize_configs, canonicalize_samples
from .engine import full_run, incremental_run
from .intervals import derive_intervals
from .minutes import build_minutes
from .reference import reference_run
from .schema import (
    MINUTE_US,
    RAW_CONFIG_COLUMNS,
    RAW_CONFIG_DTYPES,
    RAW_SAMPLE_COLUMNS,
    RAW_SAMPLE_DTYPES,
    SECOND_US,
)
from .settings import MeterSettings, meter_settings
from .storage import Store

UTC = timezone.utc


# ---------------------------------------------------------------------------
# raw-row helpers
# ---------------------------------------------------------------------------
def iso(us: int) -> str:
    return datetime.fromtimestamp(us / 1e6, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"


def sample_row(meter, us, seq, counter, boot):
    return {
        "meter_id": meter,
        "event_time": iso(us),
        "ingest_seq": seq,
        "counter_wh": counter,
        "boot_id": boot,
    }


def config_row(meter, us, seq, multiplier):
    return {
        "meter_id": meter,
        "effective_from": iso(us),
        "multiplier": multiplier,
        "config_seq": seq,
    }


def raw_samples_frame(rows) -> pl.DataFrame:
    if isinstance(rows, pl.DataFrame):
        return rows.select(RAW_SAMPLE_COLUMNS).cast(RAW_SAMPLE_DTYPES)
    if not rows:
        return pl.DataFrame(schema=RAW_SAMPLE_DTYPES)
    return pl.DataFrame(rows, schema=RAW_SAMPLE_DTYPES, orient="row").select(
        RAW_SAMPLE_COLUMNS
    )


def raw_configs_frame(rows) -> pl.DataFrame:
    if isinstance(rows, pl.DataFrame):
        return rows.select(RAW_CONFIG_COLUMNS).cast(RAW_CONFIG_DTYPES)
    if not rows:
        return pl.DataFrame(schema=RAW_CONFIG_DTYPES)
    return pl.DataFrame(rows, schema=RAW_CONFIG_DTYPES, orient="row").select(
        RAW_CONFIG_COLUMNS
    )


def settings_file(path: Path, meters: dict[str, dict], default_gap=3600) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "defaults": {"timezone": "UTC", "gap_threshold_seconds": default_gap},
                "meters": meters,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# fixture scenarios
# ---------------------------------------------------------------------------
BASE = 1_735_689_600_000_000  # 2025-01-01T00:00:00Z, a minute boundary
SECONDS = SECOND_US


def fixtures() -> dict[str, dict]:
    """Small scenarios with hand-checkable expectations."""
    out = {}

    # 1. spread across minutes, exact conservation
    out["cross_minute"] = {
        "meters": {"M1": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
        "samples": [
            sample_row("M1", BASE, 1, 0.0, "B1"),
            sample_row("M1", BASE + 150 * SECOND_US, 2, 5.0, "B1"),
            sample_row("M1", BASE + 150 * SECOND_US, 1, 5.0, "B1"),
        ],
        "configs": [config_row("M1", BASE - 86400 * SECOND_US, 1, 1.0)],
        "expect": [
            # [0,150s): 5 Wh over 150s -> 1/3 Wh each full minute, 30s at end
            (BASE, 2.0, 60, 0),
            (BASE + 60 * SECOND_US, 2.0, 60, 0),
            (BASE + 120 * SECOND_US, 1.0, 30, 0),
        ],
        "energy_total": 5.0,
    }

    # 2. meter reset -> interval invalid, no guessed usage
    out["reset"] = {
        "meters": {"M2": {"timezone": "UTC", "gap_threshold_seconds": 600}},
        "samples": [
            sample_row("M2", BASE, 1, 100.0, "B1"),
            sample_row("M2", BASE + 120 * SECOND_US, 2, 130.0, "B1"),
            sample_row("M2", BASE + 240 * SECOND_US, 3, 0.0, "B1"),  # reset
            sample_row("M2", BASE + 360 * SECOND_US, 4, 30.0, "B1"),
        ],
        "configs": [config_row("M2", BASE - 86400 * SECOND_US, 1, 1.0)],
        "expect": [
            (BASE, 60, 120, 0),  # 0..120 valid: 2 min fully valid, 30 Wh
            (BASE + 120 * SECOND_US, 60, 0, 120),  # reset span invalid
            (BASE + 240 * SECOND_US, 0, 120, 0),
        ],
        # both valid spans: 120s at 0.25 Wh/s = 30, plus 120s at 0.25 Wh/s = 30
        "energy_total": 60.0,
    }

    # 3. long gap: no interpolation, invalid coverage for whole hole
    out["long_gap"] = {
        "meters": {"M3": {"timezone": "UTC", "gap_threshold_seconds": 300}},
        "samples": [
            sample_row("M3", BASE, 1, 10.0, "B1"),
            sample_row("M3", BASE + 60 * SECOND_US, 2, 20.0, "B1"),
            sample_row("M3", BASE + 600 * SECOND_US, 3, 25.0, "B1"),
        ],
        "configs": [config_row("M3", BASE - 86400 * SECOND_US, 1, 1.0)],
        "expect": [
            (BASE, 60, 60, 0),
            (BASE + 60 * SECOND_US, 0, 0, 540),  # 60..600 invalid
        ],
        "energy_total": 10.0,
    }

    # 4. same-instant revision: only latest ingest_seq counts
    out["same_instant_revision"] = {
        "meters": {"M4": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
        "samples": [
            sample_row("M4", BASE, 1, 0.0, "B1"),
            sample_row("M4", BASE + 60 * SECOND_US, 1, 10.0, "B1"),
            sample_row("M4", BASE + 60 * SECOND_US, 2, 14.0, "B1"),  # revision wins
            sample_row("M4", BASE + 120 * SECOND_US, 1, 14.0, "B1"),
        ],
        "configs": [config_row("M4", BASE - 86400 * SECOND_US, 1, 1.0)],
        "expect": [(BASE, 60, 60, 0), (BASE + 60 * SECOND_US, 60, 60, 0)],
        "energy_total": 14.0,
    }

    # 5. multiplier changes mid-interval
    out["mid_interval_multiplier"] = {
        "meters": {"M5": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
        "samples": [
            sample_row("M5", BASE, 1, 0.0, "B1"),
            sample_row("M5", BASE + 180 * SECOND_US, 2, 30.0, "B1"),
        ],
        "configs": [
            config_row("M5", BASE - 86400 * SECOND_US, 1, 1.0),
            config_row("M5", BASE + 60 * SECOND_US, 2, 2.0),
        ],
        # segment 0..60s multiplier 1 -> 10 Wh; 60..180s multiplier 2 -> 20 Wh * 2 = 40
        "expect": [
            (BASE, 10.0, 60, 0),
            (BASE + 60 * SECOND_US, 10.0 + 10.0, 60, 0),
            (BASE + 120 * SECOND_US, 10.0, 60, 0),
        ],
        "energy_total": 50.0,
    }

    # 6. DST fall-back: the two local 01:30 minutes are distinct
    dst_start = 1_762_059_600_000_000  # 2025-11-02T05:00:00Z == 01:00 EDT
    out["dst_fallback"] = {
        "meters": {"M6": {"timezone": "America/New_York", "gap_threshold_seconds": 7200}},
        "samples": [
            sample_row("M6", dst_start, 1, 0.0, "B1"),
            sample_row("M6", dst_start + 3600 * SECOND_US, 2, 60.0, "B1"),
            sample_row("M6", dst_start + 7200 * SECOND_US, 3, 120.0, "B1"),
        ],
        "configs": [config_row("M6", dst_start - 86400 * SECOND_US, 1, 1.0)],
        "expect": [],
        "energy_total": 120.0,
        "check_labels": True,
    }

    return out


def run_fixture(workdir: Path, scenario: dict, name: str) -> dict:
    store = workdir / "store"
    if store.exists():
        shutil.rmtree(store)
    settings = settings_file(workdir / "settings.json", scenario["meters"])
    Store(store).append_raw(
        raw_samples_frame(scenario["samples"]), raw_configs_frame(scenario["configs"])
    )
    full_run(store, settings, run_id=f"{name}-full")
    facts = Store(store).read_facts().sort("meter_id", "minute_epoch_us")
    return {"facts": facts, "store": store, "settings": settings}


def energy_by_minute(facts: pl.DataFrame, meter: str) -> dict[int, float]:
    rows = facts.filter(pl.col("meter_id") == meter)
    return {
        r["minute_epoch_us"]: r["energy_wh"] for r in rows.to_dicts()
    }


# ---------------------------------------------------------------------------
# randomized differential testing
# ---------------------------------------------------------------------------
def random_scenario(seed: int, n_meters=3, n_points=28, with_configs=True) -> dict:
    rng = random.Random(seed)
    meters = {}
    samples = []
    configs = []
    tz_options = ["UTC", "America/New_York", "Europe/Berlin", "Asia/Kolkata"]
    start = BASE + rng.randrange(0, 1440) * MINUTE_US
    for m in range(n_meters):
        meter = f"M{m}"
        tz = rng.choice(tz_options)
        gap = rng.choice([300, 600, 900, 1800, 3600])
        meters[meter] = {"timezone": tz, "gap_threshold_seconds": gap}
        t = start + rng.randrange(-120, 120) * SECOND_US
        boot = f"B{rng.randrange(0, 3)}"
        counter = float(rng.randrange(0, 500))
        # config grid
        cfg_t = start - 86400 * SECOND_US
        seq = 1
        configs.append(config_row(meter, cfg_t, seq, 1.0))
        seq += 1
        cfg_times = sorted(
            start + rng.randrange(0, n_points * 4) * 60 * SECOND_US
            for _ in range(rng.randrange(0, 4))
        )
        for ct in cfg_times:
            configs.append(config_row(meter, ct, seq, rng.choice([1.0, 1.5, 2.0, 3.0])))
            seq += 1
        for i in range(n_points):
            # mostly small gaps, sometimes huge ones
            if rng.random() < 0.08:
                step = rng.randrange(gap + 60, gap * 6) * SECOND_US
            else:
                step = rng.randrange(5, max(6, min(gap, 900)) + 1) * SECOND_US
            t += step
            if rng.random() < 0.10:
                boot = f"B{rng.randrange(0, 3)}"
                counter = float(rng.randrange(0, 500))
            if rng.random() < 0.08 and counter > 10:
                counter = float(rng.randrange(0, int(counter)))  # rollback
            else:
                counter += rng.choice([0, 0, 1, 3, 7, 7.5, 19.25])
            seq = rng.randrange(1, 6)
            samples.append(sample_row(meter, t, seq, round(counter, 6), boot))
            # late revision of the previous instant sometimes
            if rng.random() < 0.06 and len(samples) >= 2:
                last = samples[-2]
                rev = dict(last)
                rev["ingest_seq"] = last["ingest_seq"] + rng.randrange(1, 3)
                rev["counter_wh"] = round(last["counter_wh"] + rng.choice([0, 0.5, 2]), 6)
                samples.append(rev)
    return {"meters": meters, "samples": samples, "configs": configs}


def _rows_from_frame(df: pl.DataFrame, cols: list[str]) -> list[dict]:
    return df.select(cols).to_dicts()


def compare_to_reference(
    facts: pl.DataFrame,
    scenario: dict,
    tol=1e-6,
) -> list[str]:
    """Return a list of mismatch descriptions (empty means equal)."""
    problems = []
    cfg_map = {
        m: {"timezone": s["timezone"], "gap_threshold_seconds": s["gap_threshold_seconds"]}
        for m, s in scenario["meters"].items()
    }
    ref = reference_run(scenario["samples"], scenario["configs"], cfg_map)
    ref_minutes = ref["minutes"]

    got = {
        (r["meter_id"], r["minute_epoch_us"]): r
        for r in facts.select(
            "meter_id",
            "minute_epoch_us",
            "energy_wh",
            "valid_us",
            "invalid_us",
            "coverage_status",
            "local_minute_label",
            "quality_reasons",
            "is_zero_usage",
        ).to_dicts()
    }
    expected_keys = {(r["meter_id"], r["minute_epoch_us"]) for r in ref_minutes}
    if set(got) != expected_keys:
        missing = sorted(expected_keys - set(got))[:5]
        extra = sorted(set(got) - expected_keys)[:5]
        problems.append(f"minute key mismatch: missing={missing} extra={extra}")
    for r in ref_minutes:
        key = (r["meter_id"], r["minute_epoch_us"])
        g = got.get(key)
        if g is None:
            continue
        if abs(g["energy_wh"] - r["energy_wh"]) > tol:
            problems.append(
                f"energy mismatch {key}: got {g['energy_wh']} want {r['energy_wh']}"
            )
        for field in ("valid_us", "invalid_us"):
            if g[field] != r[field]:
                problems.append(f"{field} mismatch {key}: got {g[field]} want {r[field]}")
        if g["coverage_status"] != r["coverage_status"]:
            problems.append(
                f"status mismatch {key}: got {g['coverage_status']} want {r['coverage_status']}"
            )
        if g["local_minute_label"] != r["local_minute_label"]:
            problems.append(
                f"label mismatch {key}: got {g['local_minute_label']} want {r['local_minute_label']}"
            )
        if sorted(g["quality_reasons"] or []) != r["quality_reasons"]:
            problems.append(
                f"reasons mismatch {key}: got {g['quality_reasons']} want {r['quality_reasons']}"
            )
        if g["is_zero_usage"] != r["is_zero_usage"]:
            problems.append(f"zero flag mismatch {key}")
    return problems


def check_invariants(facts: pl.DataFrame) -> list[str]:
    problems = []
    if facts.height == 0:
        return problems
    bad = facts.filter(pl.col("coverage_us") > MINUTE_US)
    if bad.height:
        problems.append(f"{bad.height} minutes cover more than 60s")
    bad = facts.filter(
        pl.col("coverage_us") != pl.col("valid_us") + pl.col("invalid_us")
    )
    if bad.height:
        problems.append(f"{bad.height} minutes with inconsistent coverage arithmetic")
    bad = facts.filter(pl.col("energy_wh") < 0)
    if bad.height:
        problems.append(f"{bad.height} minutes with negative energy")
    # every minute must have coverage > 0 (no zero-filled placeholders)
    bad = facts.filter(pl.col("coverage_us") <= 0)
    if bad.height:
        problems.append(f"{bad.height} minutes listed with no coverage")
    # zero-usage requires valid coverage
    bad = facts.filter(pl.col("is_zero_usage") & (pl.col("valid_us") == 0))
    if bad.height:
        problems.append(f"{bad.height} zero-usage minutes without valid coverage")
    # reasons are known
    known = {
        "boot_id_change",
        "counter_reset",
        "gap_exceeded",
        "ingest_conflict",
        "config_conflict",
        "multiplier_defaulted",
    }
    unknown = set()
    for lst in facts["quality_reasons"].to_list():
        unknown.update(set(lst or []) - known)
    if unknown:
        problems.append(f"unknown quality reasons: {unknown}")
    return problems


def check_interval_conservation(intervals: pl.DataFrame) -> list[str]:
    """Each valid interval's minute-by-minute pieces must sum to its energy.

    This is the per-interval conservation check: re-split every integrable
    interval at UTC minute boundaries using the same segmentation, and compare
    the accumulated energy against ``expected_energy_wh``.
    """
    problems = []
    if intervals.height == 0:
        return problems
    for row in intervals.filter(pl.col("is_valid")).iter_rows(named=True):
        pieces = 0.0
        covered = 0
        for seg in row["segments"]:
            s, e = seg["start_us"], seg["end_us"]
            minute = s // MINUTE_US * MINUTE_US
            while minute < e:
                lo, hi = max(s, minute), min(e, minute + MINUTE_US)
                if hi > lo:
                    pieces += (
                        row["delta_wh"]
                        * seg["multiplier"]
                        * (hi - lo)
                        / row["total_us"]
                    )
                    covered += hi - lo
                minute += MINUTE_US
        if abs(pieces - row["expected_energy_wh"]) > 1e-9 * max(
            1.0, abs(row["expected_energy_wh"])
        ):
            problems.append(
                f"interval {row['meter_id']}@{row['start_us']} breaks conservation: "
                f"pieces={pieces} expected={row['expected_energy_wh']}"
            )
        if covered != row["total_us"]:
            problems.append(
                f"interval {row['meter_id']}@{row['start_us']} pieces cover "
                f"{covered}us of {row['total_us']}us"
            )
    return problems


def check_conservation(facts: pl.DataFrame, intervals: pl.DataFrame) -> list[str]:
    """Energy and coverage must reconcile exactly between intervals and minutes.

    * global minute energy equals the sum of valid interval energy;
    * every valid interval's expected energy equals its counter delta times its
      duration-weighted multiplier;
    * an invalid interval contributes zero energy and covers ``total_us`` of
      invalid seconds somewhere in the fact table.
    """
    problems = []
    total_energy = facts["energy_wh"].sum() if facts.height else 0.0
    expected = intervals.filter(pl.col("is_valid"))["expected_energy_wh"].sum()
    if abs(total_energy - expected) > 1e-5:
        problems.append(f"global conservation: facts={total_energy} intervals={expected}")
    if intervals.height:
        valid = intervals.filter(pl.col("is_valid"))
        bad = valid.filter(
            (
                pl.col("expected_energy_wh")
                - pl.col("delta_wh") * pl.col("weighted_multiplier")
            ).abs()
            > 1e-9 * pl.max_horizontal(pl.col("delta_wh").abs(), pl.lit(1.0))
        )
        if bad.height:
            problems.append(f"{bad.height} valid intervals break delta*multiplier")
        invalid_counts = intervals.filter(~pl.col("is_valid"))
        if invalid_counts.height:
            covered = facts["invalid_us"].sum() if facts.height else 0
            want = invalid_counts["total_us"].sum()
            if covered != want:
                problems.append(
                    f"invalid coverage: minutes={covered}us intervals={want}us"
                )
    return problems


def random_incremental_rounds(
    workdir: Path,
    rounds: int = 4,
    base_rounds: int = 3,
    seed: int = 7,
    n_meters: int = 3,
    n_points: int = 30,
) -> dict:
    """Compare incremental disk state with a full recompute and reference.

    Batches deliberately shuffle event order and deliver late revisions.
    """
    rng = random.Random(seed)
    store = workdir / "store"
    if store.exists():
        shutil.rmtree(store)
    scenario = random_scenario(seed, n_meters=n_meters, n_points=n_points)
    settings = settings_file(workdir / "incremental_settings.json", scenario["meters"])
    all_samples = list(scenario["samples"])
    all_configs = list(scenario["configs"])

    # split into ingestion batches
    shuffled = list(zip(all_samples, [rng.random() for _ in range(len(all_samples))]))
    batches = [[] for _ in range(rounds)]
    idx = 0
    for row, _ in shuffled:
        batches[idx % rounds].append(row)
        idx += 1

    s = Store(store)
    s.append_raw(raw_samples_frame(batches[0]), raw_configs_frame(all_configs))
    full_run(store, settings, run_id="inc-batch0-full")

    for bi in range(1, rounds):
        incremental_run(
            store,
            settings,
            run_id=f"inc-batch{bi}",
            new_samples=raw_samples_frame(batches[bi]),
        )

    facts = Store(store).read_facts()
    intervals = Store(store).read_intervals()
    problems = check_invariants(facts)

    full_store = workdir / "store_full_reference"
    if full_store.exists():
        shutil.rmtree(full_store)
    sf = Store(full_store)
    sf.append_raw(raw_samples_frame(all_samples), raw_configs_frame(all_configs))
    full_run(full_store, settings, run_id="verify-full")
    full_facts = sf.read_facts().sort("meter_id", "minute_epoch_us")
    inc_facts = facts.sort("meter_id", "minute_epoch_us")

    if inc_facts.height != full_facts.height:
        problems.append(
            f"incremental facts rows {inc_facts.height} != full {full_facts.height}"
        )
    else:
        joined = inc_facts.select(
            "meter_id", "minute_epoch_us", "energy_wh", "valid_us", "invalid_us"
        ).join(
            full_facts.select(
                "meter_id", "minute_epoch_us", "energy_wh", "valid_us", "invalid_us"
            ),
            on=["meter_id", "minute_epoch_us"],
            how="inner",
            suffix="_full",
        )
        if joined.height != inc_facts.height:
            problems.append("incremental and full minute keys differ")
        bad = joined.filter(
            (pl.col("energy_wh") - pl.col("energy_wh_full")).abs() > 1e-9
        )
        if bad.height:
            problems.append(f"{bad.height} minute energy rows differ from full run")

    problems += check_conservation(facts, intervals)
    problems += compare_to_reference(facts, scenario)
    return pass_report(
        problems,
        {
            "facts": facts,
            "intervals": intervals,
            "scenario": scenario,
            "reports": "affected/",
        },
        "random incremental",
    )


def pass_report(problems, payload, label):
    return {"label": label, "problems": problems, "ok": not problems, **payload}
