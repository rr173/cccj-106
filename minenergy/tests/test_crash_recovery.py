"""Crash-injection tests for the cross-file commit protocol.

Each test seeds a store with two meters whose intervals cross local-day
boundaries (so one job touches several interval files, several fact partitions
and the change report), runs the revision in a *real* subprocess that calls
``os._exit(77)`` at a named point, then re-runs the original command in a fresh
process and checks that disk converges to the clean-run result:

* canonical/raw files belong to one revision set;
* interval rows and minute facts are internally consistent;
* the affected report describes exactly the right change (not a no-op when the
  job actually had work);
* no half-new/half-old file set survives, and files of meters not in the job
  are byte-identical;
* repeating the command after a successful commit is a no-op;
* recovery interrupted mid-way can itself be resumed.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests" / "crash_worker.py"

CRASH_POINTS = [
    "raw-after-append",
    "after-canonical",
    "before-first-intervals",
    "after-first-intervals",
    "before-first-facts",
    "after-first-facts",
    "before-affected",
    "after-affected",
    "before-run-meta",
    "after-run-meta",
    "before-prepare",
    "after-prepare",
    "publish-partial:1",
    "publish-partial:2",
    "publish-partial:3",
    "after-publish-before-marker",
    "after-commit",
]


def _env() -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT) + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return env


def run_worker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(WORKER), *args],
        capture_output=True,
        text=True,
        env=_env(),
        timeout=timeout,
    )


def make_store(work: Path) -> dict:
    store = work / "store"
    frames = work / "frames"
    res = run_worker("seed", "--store", str(store), "--batch-dir", str(frames))
    assert res.returncode == 0, res.stderr
    return {
        "store": store,
        "settings": frames / "settings.json",
        "revision": frames / "revision.parquet",
        "seed": frames / "seed.parquet",
        "configs": frames / "configs.parquet",
    }


def job(store: Path, settings: Path, batch: Path, run_id: str, *extra: str) -> subprocess.CompletedProcess:
    return run_worker(
        "job",
        "--store",
        str(store),
        "--settings",
        str(settings),
        "--batch",
        str(batch),
        "--run-id",
        run_id,
        *extra,
    )


def query(store: Path) -> dict:
    res = run_worker("query", "--store", str(store))
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)


def run_id_from(point: str) -> str:
    return "crash-" + point.replace(":", "-")


def snapshot_files(store: Path, skip: tuple[str, ...]) -> dict[str, bytes]:
    out = {}
    for p in store.rglob("*"):
        if not p.is_file() or "txn" in p.parts:
            continue
        rel = p.relative_to(store).as_posix()
        if any(rel.startswith(s) for s in skip):
            continue
        out[rel] = p.read_bytes()
    return out


def assert_converged(
    state: dict,
    clean: dict,
    point: str,
    recovered_again: bool = False,
) -> None:
    """The recovered disk state must equal the clean run, field by field."""
    assert state["invariants"] == [], state["invariants"]
    assert state["txn_dirs"] == [], f"leftover transaction dirs: {state['txn_dirs']}"
    assert state["raw_samples"] == clean["raw_samples"], "raw log must be replayed exactly once"
    assert state["canonical_samples"] == clean["canonical_samples"]
    assert state["canonical_counters"] == clean["canonical_counters"]
    assert state["facts_rows"] == clean["facts_rows"]
    assert state["interval_rows"] == clean["interval_rows"]
    assert state["partitions"] == clean["partitions"]
    assert state["energy_by_meter"] == pytest.approx(
        clean["energy_by_meter"], abs=1e-6
    ), point
    for part, energy in clean["facts_by_partition"].items():
        assert state["facts_by_partition"][part] == pytest.approx(energy, abs=1e-6), (
            f"{point}: partition {part} did not converge"
        )
    run_id = run_id_from(point)
    meta = state["runs"].get(run_id)
    assert meta is not None, f"job {run_id} has no committed run.json"
    if meta.get("no_op"):
        # A crash *after* prepare was recovered by begin() without rerunning the
        # analysis.  The retry itself is a genuine no-op, but it must reuse the
        # recovered job's completed report instead of overwriting it with an
        # empty one (the report files below are the authoritative check).
        assert point in (
            "after-prepare", "publish-partial:1", "publish-partial:2",
            "publish-partial:3", "after-publish-before-marker",
        )
        assert meta.get("reused_commit") is True
    if not meta.get("no_op"):
        assert meta.get("affected_meters") == clean["runs"]["clean"]["affected_meters"]
        assert meta.get("affected_intervals") == clean["runs"]["clean"]["affected_intervals"]
        assert meta.get("changed_sample_instants") == clean["runs"]["clean"]["changed_sample_instants"]
    # The change description is not just metadata: its parquet files must hold
    # the full change set, never a truncated half-report and never an empty
    # report for a job that really revised data.
    run_affected = state["affected"][run_id]
    assert run_affected["has_run_json"] is True
    assert run_affected["intervals_rows"] == clean["runs"]["clean"]["affected_intervals"]
    assert run_affected["minutes_rows"] == clean["runs"]["clean"]["affected_minutes"]


@pytest.fixture()
def clean_work():
    work = Path(tempfile.mkdtemp(prefix="minenergy-crash-clean-"))
    try:
        spec = make_store(work)
        assert job(spec["store"], spec["settings"], spec["revision"], "clean").returncode == 0
        clean = query(spec["store"])
        # A committed job re-run with the same batch is a no-op and adds nothing.
        res = job(spec["store"], spec["settings"], spec["revision"], "repeat")
        assert res.returncode == 0, res.stderr
        repeat = query(spec["store"])
        assert repeat["raw_samples"] == clean["raw_samples"]
        assert repeat["runs"]["repeat"]["no_op"] is True
        assert repeat["canonical_counters"] == clean["canonical_counters"]
        # The seeded "clean" run plus the repeat no-op leave the store unchanged.
        baseline_files = snapshot_files(spec["store"], skip=("affected/",))
        yield {"spec": spec, "clean": clean, "files": baseline_files}
    finally:
        shutil.rmtree(work, ignore_errors=True)


@pytest.mark.parametrize("point", CRASH_POINTS)
def test_crash_then_rerun_converges(clean_work, point):
    work = Path(tempfile.mkdtemp(prefix="minenergy-crash-"))
    try:
        spec = make_store(work)
        clean = clean_work["clean"]
        before = snapshot_files(spec["store"], skip=("affected/",))
        crash_at, _, after = point.partition(":")
        run_id = run_id_from(point)
        crashed = job(
            spec["store"],
            spec["settings"],
            spec["revision"],
            run_id,
            "--crash-at",
            crash_at,
            "--after",
            after or "1",
        )
        assert crashed.returncode == 77, (
            f"{point}: child did not crash as requested: {crashed.returncode} {crashed.stderr[-2000:]}"
        )
        # Re-run exactly the original command in a fresh process.
        resumed = job(spec["store"], spec["settings"], spec["revision"], run_id)
        assert resumed.returncode == 0, f"{point}: {resumed.stderr[-2000:]}"
        state = query(spec["store"])
        assert_converged(state, clean, point)

        after_files = snapshot_files(spec["store"], skip=("affected/",))
        # Files of a meter not in the job keep their exact bytes.
        m3 = {k: v for k, v in after_files.items() if "meter_id=M3" in k}
        m3_before = {k: v for k, v in before.items() if "meter_id=M3" in k}
        assert m3 == m3_before, f"{point}: untouched meter M3 changed"

        # One more invocation after the successful commit must be a no-op.
        again = job(spec["store"], spec["settings"], spec["revision"], "again")
        assert again.returncode == 0, again.stderr
        assert query(spec["store"])["runs"]["again"]["no_op"] is True
    finally:
        shutil.rmtree(work, ignore_errors=True)


@pytest.mark.parametrize("point", ["publish-partial:1", "publish-partial:2", "publish-partial:3", "after-publish-before-marker"])
def test_recovery_itself_can_be_interrupted(clean_work, point):
    work = Path(tempfile.mkdtemp(prefix="minenergy-crash2-"))
    try:
        spec = make_store(work)
        clean = clean_work["clean"]
        crash_at, _, after = point.partition(":")
        run_id = run_id_from(point)
        crashed = job(
            spec["store"], spec["settings"], spec["revision"], run_id,
            "--crash-at", crash_at, "--after", after or "1",
        )
        assert crashed.returncode == 77, crashed.stderr[-2000:]
        # Recovery is interrupted too, then resumed by another process.
        rec1 = run_worker(
            "recover", "--store", str(spec["store"]), "--crash-at", "recovery-partial", "--after", "1"
        )
        assert rec1.returncode == 77, rec1.stderr[-2000:]
        rec2 = run_worker("recover", "--store", str(spec["store"]))
        assert rec2.returncode == 0, rec2.stderr[-2000:]
        state = query(spec["store"])
        assert_converged(state, clean, point, recovered_again=True)
        assert job(spec["store"], spec["settings"], spec["revision"], "again").returncode == 0
        assert query(spec["store"])["runs"]["again"]["no_op"] is True
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_crash_without_rerun_never_publishes_partial_canonical(clean_work):
    """A crash before prepare leaves disk exactly at the committed baseline."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-crash3-"))
    try:
        spec = make_store(work)
        before = snapshot_files(spec["store"], skip=("affected/", "raw/"))
        res = job(
            spec["store"], spec["settings"], spec["revision"], "early",
            "--crash-at", "after-canonical",
        )
        assert res.returncode == 77
        state_before_recovery = query(spec["store"])
        # The interrupted process must not have changed any committed file; only
        # its private staging directory is visible.
        after = snapshot_files(spec["store"], skip=("affected/", "raw/"))
        assert after == before
        assert state_before_recovery["txn_dirs"], "staging dir should remain until recovery"
        # Recovery discards it and the store is still the seeded baseline.
        assert run_worker("recover", "--store", str(spec["store"])).returncode == 0
        recovered = query(spec["store"])
        assert recovered["txn_dirs"] == []
        assert recovered["canonical_counters"] == state_before_recovery["canonical_counters"]
        assert recovered["energy_by_meter"] == pytest.approx(
            {"M1": 180.0, "M2": 90.0, "M3": 9.0}, abs=1e-6
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_legacy_store_without_txn_directory_is_usable(tmp_path):
    """Stores written before the protocol (no state/txn) migrate implicitly."""
    tmpdir_path = Path(tmp_path)
    sys.path.insert(0, str(ROOT))
    from minenergy.engine import incremental_run, full_run
    from minenergy.verify import (
        BASE, SECOND_US, config_row, raw_configs_frame, raw_samples_frame,
        sample_row, settings_file,
    )
    from minenergy.storage import Store

    store = tmpdir_path / "legacy"
    settings = settings_file(
        tmpdir_path / "s.json", {"L": {"timezone": "UTC", "gap_threshold_seconds": 600}}
    )
    Store(store).append_raw(
        raw_samples_frame([
            sample_row("L", BASE, 1, 0.0, "B"),
            sample_row("L", BASE + 600 * SECOND_US, 2, 10.0, "B"),
        ]),
        raw_configs_frame([config_row("L", BASE - 60 * SECOND_US, 1, 1.0)]),
    )
    full_run(store, settings, run_id="legacy-seed")
    # Simulate a directory produced by the previous release: no protocol state
    # at all, just the committed tables.
    shutil.rmtree(store / "state" / "txn", ignore_errors=True)
    assert not (store / "state" / "txn").exists()
    res = incremental_run(
        store,
        settings,
        run_id="legacy-inc",
        new_samples=raw_samples_frame([sample_row("L", BASE + 600 * SECOND_US, 9, 30.0, "B")]),
    )
    facts = Store(store).read_facts()
    assert facts["energy_wh"].sum() == pytest.approx(30.0)
    assert res.affected_intervals.height == 1
    assert (store / "state" / "txn").exists()  # protocol state lives here now


def _frames_in_process(tmp_path):
    """Build the two-meter/cross-day frames without spawning a worker."""
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "tests"))
    import crash_worker

    batch_dir = Path(tmp_path) / "frames"
    return crash_worker.scenario_frames(batch_dir)


def test_live_job_staging_is_not_discarded_by_concurrent_recover(tmp_path):
    """A recover() in another store object must not delete a live job's staging.

    The liveness lock guards un-prepared transaction directories, so an
    independent process/thread that calls recover while a job is still
    analysing cannot corrupt it.
    """
    sys.path.insert(0, str(ROOT))
    from minenergy.storage import Store

    frames = _frames_in_process(tmp_path)
    store = Path(tmp_path) / "store"
    Store(store).append_raw(
        samples=pl.read_parquet(frames["seed"]),
        configs=pl.read_parquet(frames["configs"]),
    )
    from minenergy.engine import full_run

    full_run(store, frames["settings"], run_id="seed")

    s = Store(store)
    txn = s.begin()
    assert txn.dir.exists()
    # A recovery through a different Store object must leave the live job alone.
    other = Store(store)
    other.recover()
    assert txn.dir.exists(), "live staging area must survive recovery"
    assert (txn.dir / "manifest.json").exists() is False
    txn.abort()
    assert not txn.dir.exists()


def test_full_run_crash_then_rerun_is_atomic(tmp_path):
    """``full`` (including its raw seed) is one transaction: re-running it after
    a crash publishes the whole recompute exactly once."""
    tmp = Path(tmp_path)
    from minenergy.engine import full_run
    from minenergy.storage import Store, Transaction
    from minenergy.verify import BASE, SECOND_US, config_row, raw_configs_frame, raw_samples_frame, sample_row, settings_file

    settings = settings_file(
        tmp / "s.json", {"F": {"timezone": "UTC", "gap_threshold_seconds": 600}}
    )
    seed = raw_samples_frame([
        sample_row("F", BASE, 1, 0.0, "B"),
        sample_row("F", BASE + 600 * SECOND_US, 2, 10.0, "B"),
    ])
    configs = raw_configs_frame([config_row("F", BASE - 60 * SECOND_US, 1, 1.0)])
    store = tmp / "store"

    original = Transaction.prepare

    def crashing_prepare(self, meta=None):
        raise KeyboardInterrupt("power cut before the completion boundary")

    Transaction.prepare = crashing_prepare
    try:
        with pytest.raises(KeyboardInterrupt):
            full_run(store, settings, run_id="full", new_samples=seed, new_configs=configs)
    finally:
        Transaction.prepare = original
    # Nothing outside the staging area was published; the staging dir survives
    # for the next entry point to discard.
    assert Store(store).read_raw_samples().height == 0
    assert Store(store).read_facts().height == 0

    res = full_run(store, settings, run_id="full", new_samples=seed, new_configs=configs)
    facts = Store(store).read_facts()
    assert facts["energy_wh"].sum() == pytest.approx(10.0)
    assert Store(store).read_raw_samples().height == 2, "raw seeded once, not twice"
    assert res.run_id == "full"
