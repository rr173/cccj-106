"""Tests for the incremental engine and randomized differential properties."""
from __future__ import annotations

import json
import random
import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from minenergy.engine import full_run, incremental_run
from minenergy.schema import MINUTE_US, SECOND_US
from minenergy.storage import Store
from minenergy.verify import (
    BASE,
    check_conservation,
    check_invariants,
    config_row,
    iso,
    raw_configs_frame,
    raw_samples_frame,
    random_incremental_rounds,
    random_scenario,
    sample_row,
    settings_file,
)


@pytest.fixture()
def tmpdir_path():
    path = Path(tempfile.mkdtemp(prefix="minenergy-inc-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _facts(store: Store) -> pl.DataFrame:
    return store.read_facts().sort("meter_id", "minute_epoch_us")


def test_late_sample_recomputes_only_neighbourhood(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 600}})
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 300 * SECOND_US, 2, 30.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 600 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    baseline = _facts(s)

    # late sample inside the existing interval splits it in two
    result = incremental_run(
        store,
        settings,
        run_id="late",
        new_samples=raw_samples_frame([sample_row("M", BASE + 200 * SECOND_US, 3, 20.0, "B")]),
    )
    assert result.affected_meters == ["M"]
    assert result.affected_intervals.height == 2
    meta = s.read_run_meta("late")
    assert meta["windows"]["M"] == [[BASE, BASE + 300 * SECOND_US]]
    # exactly one minute partition rewritten
    assert len(result.partitions_written) == 1
    facts = _facts(s)
    assert facts.height == 5  # 0..300s touches exactly five UTC minutes
    old_energy = baseline["energy_wh"].sum()
    assert facts["energy_wh"].sum() == pytest.approx(old_energy, abs=1e-9)
    assert check_invariants(facts) == []


def test_late_revision_replaces_minute_partition(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 3600}})
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 120 * SECOND_US, 1, 10.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    result = incremental_run(
        store,
        settings,
        run_id="revision",
        new_samples=raw_samples_frame([sample_row("M", BASE + 120 * SECOND_US, 2, 24.0, "B")]),
    )
    report = result.affected_intervals
    assert report.height == 1
    assert report.row(0, named=True)["change_kind"] == "revised"
    assert "next_counter_wh" in report.row(0, named=True)["changed_fields"]
    assert report.row(0, named=True)["previous_expected_energy_wh"] == pytest.approx(10.0)
    assert report.row(0, named=True)["expected_energy_wh"] == pytest.approx(24.0)
    facts = _facts(s)
    assert facts["energy_wh"].sum() == pytest.approx(24.0)


def test_same_ingest_conflict_marks_interval_invalid(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 600}})
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE, 1, 50.0, "B"),
                sample_row("M", BASE + 120 * SECOND_US, 2, 10.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    facts = _facts(s)
    # conflicted sample -> un-integrable neighbourhoods, no guessed usage
    assert facts["energy_wh"].sum() == 0.0 or "ingest_conflict" in (
        facts["quality_reasons"].to_list()
    )
    assert s.read_sample_conflicts().height == 1


def test_config_correction_mid_interval(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 3600}})
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 180 * SECOND_US, 2, 30.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    assert _facts(s)["energy_wh"].sum() == pytest.approx(30.0)
    result = incremental_run(
        store,
        settings,
        run_id="config",
        new_configs=raw_configs_frame([config_row("M", BASE + 60 * SECOND_US, 2, 2.0)]),
    )
    assert result.affected_intervals.height == 1
    assert result.affected_intervals.row(0, named=True)["change_kind"] == "revised"
    assert result.affected_intervals.row(0, named=True)["expected_energy_wh"] == pytest.approx(50.0)
    facts = _facts(s)
    assert facts["energy_wh"].sum() == pytest.approx(50.0)
    meta = s.read_run_meta("config")
    assert meta["changed_config_instants"] == {"M": 1}


def test_noop_incremental_run(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 600}})
    Store(store).append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 60 * SECOND_US, 2, 1.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    result = incremental_run(store, settings, run_id="noop")
    assert result.affected_intervals.height == 0
    assert result.affected_minutes.height == 0
    assert result.partitions_written == []
    assert Store(store).read_run_meta("noop")["no_op"] is True


def test_settings_change_triggers_full_meter_recompute(tmpdir_path):
    store = tmpdir_path / "store"
    settings = settings_file(tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 600}})
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 600 * SECOND_US, 2, 60.0, "B"),
                sample_row("M", BASE + 1200 * SECOND_US, 3, 120.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    # 600s gaps are exactly at the limit (<=), so both spans integrate
    assert _facts(s)["energy_wh"].sum() == pytest.approx(120.0)
    # tighten the threshold so the 600s spans become gaps
    settings_tight = settings_file(tmpdir_path / "s_tight.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 300}})
    result = incremental_run(store, settings_tight, run_id="tight")
    assert result.affected_meters == ["M"]
    facts = _facts(s)
    assert facts["energy_wh"].sum() == 0.0
    assert facts["invalid_us"].sum() == 1200 * SECOND_US


def test_incremental_matches_full_on_random_seeds(tmpdir_path):
    for seed in (1, 7, 11, 42):
        result = random_incremental_rounds(
            tmpdir_path / f"seed{seed}", seed=seed, rounds=4, n_meters=3, n_points=30
        )
        assert result["ok"], result["problems"]


def test_increment_restart_recognises_revisions(tmpdir_path):
    """Run -> restart (new objects) -> verify revisions still detected."""
    scenario = random_scenario(99, n_meters=2, n_points=30)
    settings = settings_file(tmpdir_path / "s.json", scenario["meters"])
    store = tmpdir_path / "store"
    rng = random.Random(99)
    rows = sorted(scenario["samples"], key=lambda _: rng.random())
    batches = [rows[i::3] for i in range(3)]
    s = Store(store)
    res = incremental_run(store, settings, run_id="b0", new_samples=raw_samples_frame(batches[0]))
    res = incremental_run(store, settings, run_id="b1", new_samples=raw_samples_frame(batches[1]))
    assert res.affected_meters, "second batch should change canonical revisions"
    # re-open the store from scratch (simulates a new process)
    s2 = Store(store)
    res = incremental_run(store, settings, run_id="b2", new_samples=raw_samples_frame(batches[2]))
    facts = s2.read_facts().sort("meter_id", "minute_epoch_us")
    problems = check_invariants(facts) + check_conservation(facts, s2.read_intervals())
    assert not problems
    # a repeated duplicate ingestion is a no-op on the canonical revision set
    res_dup = incremental_run(
        store, settings, run_id="dup", new_samples=raw_samples_frame(batches[2])
    )
    assert res_dup.affected_intervals.height == 0


def test_only_affected_partitions_rewritten_across_days(tmpdir_path):
    """Two local days exist; a late sample on day 2 rewrites only day 2."""
    store = tmpdir_path / "store"
    settings = settings_file(
        tmpdir_path / "s.json",
        {"M": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
    )
    day1 = BASE + 12 * 3600 * SECOND_US  # 2025-01-01T12:00Z
    day2 = BASE + 36 * 3600 * SECOND_US  # 2025-01-02T12:00Z

    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", day1, 1, 0.0, "B"),
                sample_row("M", day1 + 600 * SECOND_US, 2, 10.0, "B"),
                sample_row("M", day2, 3, 20.0, "B"),
                sample_row("M", day2 + 600 * SECOND_US, 4, 40.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 600 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    partitions = s.existing_fact_partitions()
    assert ("M", "2025-01-01") in partitions and ("M", "2025-01-02") in partitions
    day1_bytes = partitions[("M", "2025-01-01")].read_bytes()

    result = incremental_run(
        store,
        settings,
        run_id="late-day2",
        new_samples=raw_samples_frame([sample_row("M", day2 + 300 * SECOND_US, 5, 30.0, "B")]),
    )
    written = {Path(p) for p in result.partitions_written}
    assert len(written) == 1 and "2025-01-02" in str(next(iter(written)))
    # day 1 partition is byte-identical: untouched partitions are not rewritten
    assert s.existing_fact_partitions()[("M", "2025-01-01")].read_bytes() == day1_bytes
    # Day 1 contributes 10 Wh; the 600 s day-2 span carries a 20 Wh delta and
    # the late sample splits it into 10 Wh + 10 Wh, so the total stays 30 Wh.
    facts = _facts(s)
    assert facts["energy_wh"].sum() == pytest.approx(30.0)


def test_unaffected_intervals_preserved(tmpdir_path):
    """Intervals outside the affected neighbourhood keep their stored content."""
    scenario = random_scenario(303, n_meters=1, n_points=40)
    settings = settings_file(tmpdir_path / "s.json", scenario["meters"])
    store = tmpdir_path / "store"
    rng = random.Random(303)
    rows = sorted(scenario["samples"], key=lambda _: rng.random())
    first, rest = rows[: len(rows) // 2], rows[len(rows) // 2:]
    s = Store(store)
    s.append_raw(raw_samples_frame(first), raw_configs_frame(scenario["configs"]))
    full_run(store, settings, run_id="seed")
    before = {r["start_us"]: r["content_hash"] for r in s.read_intervals().to_dicts()}
    result = incremental_run(
        store, settings, run_id="later", new_samples=raw_samples_frame(rest)
    )
    after = {r["start_us"]: r["content_hash"] for r in s.read_intervals().to_dicts()}
    changed = {k for k in before if k in after and before[k] != after[k]}
    # every stored interval hash outside the reported changes is unchanged
    assert all(after[k] == before[k] for k in before if k in after and k not in changed)
    assert changed, "the late batch must change at least one interval"


def test_float_rounding_does_not_cause_false_changes(tmpdir_path):
    """ULP noise between a recomputed interval and its stored twin is not a change.

    Recomputing only a window must not report intervals whose values differ
    only in the last float bit; the interval report keys on exact field
    equality, so repeated recomputation of an unchanged neighbourhood is a no-op.
    """
    scenario = random_scenario(77, n_meters=1, n_points=40)
    settings = settings_file(tmpdir_path / "s.json", scenario["meters"])
    store = tmpdir_path / "store"
    s = Store(store)
    s.append_raw(raw_samples_frame(scenario["samples"]), raw_configs_frame(scenario["configs"]))
    full_run(store, settings, run_id="seed")
    before = {r["start_us"]: r["content_hash"] for r in s.read_intervals().to_dicts()}
    # force a recompute of an existing sample's exact payload (no visible change)
    existing = s.read_canonical_samples().head(1).row(0, named=True)
    row = {
        "meter_id": existing["meter_id"],
        "event_time": iso(existing["event_time_us"]),
        "ingest_seq": existing["selected_ingest_seq"],
        "counter_wh": existing["counter_wh"],
        "boot_id": existing["boot_id"],
    }
    result = incremental_run(
        store, settings, run_id="dup-payload", new_samples=raw_samples_frame([row])
    )
    after = {r["start_us"]: r["content_hash"] for r in s.read_intervals().to_dicts()}
    assert after == before
    assert result.affected_intervals.height == 0


def test_exact_reference_matches_incremental_facts(tmpdir_path):
    """Minute energy equals the exact-Fraction reference within 1e-9."""
    from minenergy.verify import compare_to_reference

    scenario = random_scenario(88, n_meters=2, n_points=45)
    settings = settings_file(tmpdir_path / "s.json", scenario["meters"])
    store = tmpdir_path / "store"
    s = Store(store)
    s.append_raw(raw_samples_frame(scenario["samples"]), raw_configs_frame(scenario["configs"]))
    full_run(store, settings, run_id="seed")
    problems = compare_to_reference(s.read_facts(), scenario, tol=1e-9)
    assert not problems, problems


def test_restart_in_a_new_process_recognises_revisions(tmpdir_path):
    """A separate interpreter process must pick up revisions from the parquet log."""
    import subprocess
    import sys
    import textwrap

    scenario = random_scenario(123, n_meters=1, n_points=20)
    settings = settings_file(tmpdir_path / "s.json", scenario["meters"])
    store = tmpdir_path / "store"
    boot = tmpdir_path / "boot.json"
    boot.write_text(
        json.dumps({"meters": scenario["meters"], "samples": scenario["samples"],
                    "configs": scenario["configs"]})
    )
    script = textwrap.dedent(
        f"""
        import json, sys
        import polars as pl
        from minenergy.verify import raw_samples_frame, raw_configs_frame
        from minenergy.engine import full_run, incremental_run
        d = json.loads(open({str(boot)!r}).read())
        mode = sys.argv[1]
        store = {str(store)!r}
        if mode == "seed":
            from minenergy.storage import Store
            Store(store).append_raw(raw_samples_frame(d["samples"]), raw_configs_frame(d["configs"]))
            full_run(store, {str(settings)!r}, run_id="seed")
        elif mode == "revise":
            s = d["samples"][0]
            rev = dict(s); rev["ingest_seq"] = s["ingest_seq"] + 7; rev["counter_wh"] = s["counter_wh"] + 3.5
            incremental_run(store, {str(settings)!r}, run_id="revise",
                            new_samples=raw_samples_frame([rev]))
            r = open({str(store)!r} + "/affected/revise/run.json").read()
            print(r)
        elif mode == "replay":
            incremental_run(store, {str(settings)!r}, run_id="replay")
            print(open({str(store)!r} + "/affected/replay/run.json").read())
        """
    )
    script_path = tmpdir_path / "worker.py"
    script_path.write_text(script)

    def run(mode):
        return subprocess.run(
            [sys.executable, str(script_path), mode],
            capture_output=True,
            text=True,
            env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
        )

    # seed in process 1
    res = run("seed")
    assert res.returncode == 0, res.stderr
    original = Store(store).read_canonical_samples().row(0, named=True)
    # revise in process 2 (fresh interpreter)
    res = run("revise")
    assert res.returncode == 0, res.stderr
    meta = json.loads(res.stdout)
    assert meta["affected_meters"] == [original["meter_id"]]
    revised = Store(store).read_canonical_samples().filter(
        pl.col("event_time_us") == original["event_time_us"]
    ).row(0, named=True)
    assert revised["counter_wh"] == original["counter_wh"] + 3.5
    # a third process must see *no* further change (revision already applied)
    res = run("replay")
    assert res.returncode == 0, res.stderr
    meta = json.loads(res.stdout)
    assert meta["no_op"] is True


def test_removing_the_last_sample_deletes_partition(tmpdir_path):
    """When the affected neighbourhood shrinks to a single sample no minute
    facts remain; the now-empty partition is removed rather than zero-filled."""
    from minenergy.storage import _write_atomic

    store = tmpdir_path / "store"
    settings = settings_file(
        tmpdir_path / "s.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 3600}}
    )
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 60 * SECOND_US, 2, 10.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="seed")
    assert ("M", "2025-01-01") in s.existing_fact_partitions()
    # drop one sample from the raw log (as raw compaction would)
    _write_atomic(
        s.read_raw_samples().filter(pl.col("ingest_seq") != 2), s.raw_samples_path
    )
    result = incremental_run(store, settings, run_id="remove")
    assert result.affected_intervals.height == 1
    assert result.affected_intervals.row(0, named=True)["change_kind"] == "removed"
    assert len(result.partitions_removed) == 1
    assert s.read_facts().height == 0
    assert s.existing_fact_partitions() == {}


def test_timezone_setting_change_relocates_partitions(tmpdir_path):
    """Changing a meter's IANA zone rewrites facts under the new local date and
    removes the stale partition rather than leaving duplicate coverage."""
    store = tmpdir_path / "store"
    s = Store(store)
    s.append_raw(
        raw_samples_frame(
            [
                sample_row("M", BASE, 1, 0.0, "B"),
                sample_row("M", BASE + 3600 * SECOND_US, 2, 60.0, "B"),
            ]
        ),
        raw_configs_frame([config_row("M", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    utc_settings = settings_file(
        tmpdir_path / "utc.json", {"M": {"timezone": "UTC", "gap_threshold_seconds": 7200}}
    )
    full_run(store, utc_settings, run_id="seed")
    assert ("M", "2025-01-01") in s.existing_fact_partitions()

    ny_settings = settings_file(
        tmpdir_path / "ny.json",
        {"M": {"timezone": "America/New_York", "gap_threshold_seconds": 7200}},
    )
    result = incremental_run(store, ny_settings, run_id="tz")
    assert result.affected_meters == ["M"]
    assert sorted(s.existing_fact_partitions().keys()) == [("M", "2024-12-31")]
    assert s.read_facts()["timezone"].unique().to_list() == ["America/New_York"]
    assert result.affected_minutes.height == 60
