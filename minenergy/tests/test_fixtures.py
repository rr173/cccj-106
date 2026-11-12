"""Deterministic fixture tests with hand-derived expectations."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from minenergy.canonical import canonicalize_configs, canonicalize_samples, RawValidationError
from minenergy.schema import SECOND_US, MINUTE_US
from minenergy.storage import Store
from minenergy.verify import (
    BASE,
    check_conservation,
    check_invariants,
    compare_to_reference,
    config_row,
    fixtures,
    iso,
    raw_configs_frame,
    raw_samples_frame,
    run_fixture,
    sample_row,
    settings_file,
)


@pytest.fixture(scope="module")
def workdir():
    path = Path(tempfile.mkdtemp(prefix="minenergy-fixtures-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.mark.parametrize("name", list(fixtures()))
def test_fixture_matches_reference(workdir, name):
    scenario = fixtures()[name]
    result = run_fixture(workdir / name, scenario, name)
    facts = result["facts"]
    intervals = Store(result["store"]).read_intervals()
    problems = compare_to_reference(facts, scenario)
    problems += check_invariants(facts)
    problems += check_conservation(facts, intervals)
    assert not problems, problems


@pytest.mark.parametrize("name", list(fixtures()))
def test_fixture_energy_total(workdir, name):
    scenario = fixtures()[name]
    result = run_fixture(workdir / f"{name}_total", scenario, name)
    total = result["facts"]["energy_wh"].sum()
    assert total == pytest.approx(scenario["energy_total"], abs=1e-9)


def test_cross_minute_exact_values(workdir):
    scenario = fixtures()["cross_minute"]
    facts = run_fixture(workdir / "cross_minute_exact", scenario, "cross_minute")["facts"]
    rows = facts.filter(pl.col("meter_id") == "M1").sort("minute_epoch_us").to_dicts()
    assert [round(r["energy_wh"], 12) for r in rows] == [2.0, 2.0, 1.0]
    assert [r["valid_us"] for r in rows] == [60 * SECOND_US, 60 * SECOND_US, 30 * SECOND_US]
    assert all(r["invalid_us"] == 0 for r in rows)
    assert [r["coverage_status"] for r in rows] == [
        "full_valid",
        "full_valid",
        "partial_valid",
    ]


def test_reset_and_no_guessing(workdir):
    scenario = fixtures()["reset"]
    facts = run_fixture(workdir / "reset_exact", scenario, "reset")["facts"]
    rows = facts.filter(pl.col("meter_id") == "M2").sort("minute_epoch_us").to_dicts()
    reset_rows = [r for r in rows if r["coverage_status"] == "invalid_only"]
    assert reset_rows, "reset interval must appear as invalid coverage"
    for r in reset_rows:
        assert r["valid_us"] == 0
        assert r["invalid_us"] == 60 * SECOND_US
        assert r["energy_wh"] == 0.0
        assert "counter_reset" in r["quality_reasons"]
        assert not r["is_zero_usage"]
    total_valid = sum(r["valid_us"] for r in rows)
    assert total_valid == 240 * SECOND_US


def test_long_gap_marks_whole_hole_invalid(workdir):
    scenario = fixtures()["long_gap"]
    facts = run_fixture(workdir / "long_gap_exact", scenario, "long_gap")["facts"]
    rows = facts.filter(pl.col("meter_id") == "M3").sort("minute_epoch_us").to_dicts()
    gap_rows = rows[1:]
    assert gap_rows and all("gap_exceeded" in r["quality_reasons"] for r in gap_rows)
    assert all(r["energy_wh"] == 0 for r in gap_rows)
    # the whole 60s..600s hole is covered as invalid, not dropped
    assert rows[0]["minute_epoch_us"] + MINUTE_US == gap_rows[0]["minute_epoch_us"]
    assert sum(r["invalid_us"] for r in gap_rows) == 540 * SECOND_US


def test_same_instant_revision_keeps_latest(workdir):
    scenario = fixtures()["same_instant_revision"]
    store = run_fixture(workdir / "revision_exact", scenario, "revision")["store"]
    canon = Store(store).read_canonical_samples()
    assert canon.height == 3
    row = canon.filter(pl.col("event_time_us") == BASE + 60 * SECOND_US).row(0, named=True)
    assert row["counter_wh"] == 14.0
    assert row["selected_ingest_seq"] == 2
    assert row["revision_count"] == 2
    facts = Store(store).read_facts()
    assert facts["energy_wh"].sum() == pytest.approx(14.0)


def test_mid_interval_multiplier_split(workdir):
    scenario = fixtures()["mid_interval_multiplier"]
    store = run_fixture(workdir / "mult_exact", scenario, "mult")["store"]
    intervals = Store(store).read_intervals()
    assert intervals.height == 1
    segment = intervals.row(0, named=True)
    assert len(segment["segments"]) == 2, "config change must split the interval"
    total = intervals["expected_energy_wh"].sum()
    # 30 Wh split 60/180 s then 120/180 s, multiplier 1 then 2
    assert total == pytest.approx(50.0)


def test_dst_fallback_distinct_labels(workdir):
    scenario = fixtures()["dst_fallback"]
    facts = run_fixture(workdir / "dst_exact", scenario, "dst")["facts"]
    rows = facts.filter(pl.col("meter_id") == "M6").sort("minute_epoch_us").to_dicts()
    labels = [(r["local_minute_label"], r["utc_offset_seconds"]) for r in rows]
    # both 01:30 wall-clock minutes are present with different offsets
    assert ("2025-11-02T01:30-0400", -14400) in labels
    assert ("2025-11-02T01:30-0500", -18000) in labels
    assert len({r["local_minute"] for r in rows}) < len(rows), (
        "local_minute repeats during fall-back; labels must not"
    )
    assert rows[0]["local_minute_label"].endswith("-0400")
    assert rows[-1]["local_minute_label"].endswith("-0500")


def test_duplicate_identical_rows_are_one_sample():
    rows = [
        sample_row("M", BASE, 1, 0.0, "B"),
        sample_row("M", BASE, 1, 0.0, "B"),
        sample_row("M", BASE + 60 * SECOND_US, 1, 5.0, "B"),
        sample_row("M", BASE + 60 * SECOND_US, 1, 5.0, "B"),
    ]
    canon, conflicts = canonicalize_samples(raw_samples_frame(rows))
    assert canon.height == 2
    assert conflicts.height == 0
    assert canon["conflicted"].to_list() == [False, False]


def test_intra_sequence_conflict_is_flagged():
    rows = [
        sample_row("M", BASE, 1, 0.0, "B"),
        sample_row("M", BASE, 1, 99.0, "B"),  # same seq, different payload
        sample_row("M", BASE + 60 * SECOND_US, 2, 10.0, "B"),
    ]
    canon, conflicts = canonicalize_samples(raw_samples_frame(rows))
    assert conflicts.height == 1
    assert conflicts.row(0, named=True)["payload_count"] == 2
    flagged = canon.filter(pl.col("event_time_us") == BASE).row(0, named=True)
    assert flagged["conflicted"]
    assert flagged["conflicting_seqs"] == [1]


def test_naive_timestamp_rejected():
    bad = pl.DataFrame(
        [
            {
                "meter_id": "M",
                "event_time": "2025-01-01T00:00:00",
                "ingest_seq": 1,
                "counter_wh": 0.0,
                "boot_id": "B",
            }
        ]
    )
    with pytest.raises(RawValidationError, match="explicit UTC offset"):
        canonicalize_samples(bad)


def test_config_conflict_reported():
    cfgs = [
        config_row("M", BASE, 1, 1.0),
        config_row("M", BASE, 1, 2.0),
    ]
    canon, conflicts = canonicalize_configs(raw_configs_frame(cfgs))
    assert conflicts.height == 1
    assert canon.row(0, named=True)["conflicted"]
