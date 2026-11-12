"""Subprocess harness for crash-recovery tests.

Run modes:

    crash_worker.py seed    --store DIR --settings FILE --batch-dir DIR
    crash_worker.py job     --store DIR --settings FILE --batch FILE --run-id ID
                            [--crash-at CODE] [--after N]
    crash_worker.py recover --store DIR [--crash-at recovery-partial] [--after N]
    crash_worker.py query   --store DIR

``--crash-at`` installs a monkeypatch that calls ``os._exit(77)`` at a named
point, so the process dies exactly where a power cut / kill would.  The codes
cover every phase of the job:

    raw-after-append        raw append staged, nothing else
    after-canonical         canonical revisions staged
    after-first-intervals   only the first meter's interval file staged
    after-first-facts       only the first minute partition staged
    before-affected         change report not yet produced
    after-affected          change report staged, run.json missing
    before-run-meta / after-run-meta
    before-prepare / after-prepare
    publish-partial         N files moved into place, rest still staged
    after-publish-before-marker   all files published, no commit marker yet
    after-commit            commit complete (marker written and dir removed)
    recovery-partial        recovery replays N files then dies again
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from minenergy.engine import full_run, incremental_run  # noqa: E402
from minenergy.storage import Store, Transaction  # noqa: E402


def _die() -> "None":
    os._exit(77)


def install_crash(code: str, after: int) -> None:
    """Replace the named step with one that kills the process at that point."""
    if code in ("", "none"):
        return

    if code == "raw-after-append":
        original = Store.append_raw

        def patched(self, samples=None, configs=None):
            if (samples is not None and samples.height) or (
                configs is not None and configs.height
            ):
                original(self, samples, configs)
                _die()
            return original(self, samples, configs)

        Store.append_raw = patched
        return

    if code == "after-canonical":
        original = Store.write_canonical

        def patched(self, *args, **kwargs):
            original(self, *args, **kwargs)
            _die()

        Store.write_canonical = patched
        return

    if code == "before-first-intervals":
        original = Store.write_intervals_for_meter

        def patched(self, meter_id, intervals):
            _die()
            return original(self, meter_id, intervals)

        Store.write_intervals_for_meter = patched
        return

    if code == "after-first-intervals":
        original = Store.write_intervals_for_meter
        state = {"calls": 0}

        def patched(self, meter_id, intervals):
            result = original(self, meter_id, intervals)
            state["calls"] += 1
            if state["calls"] >= 1:
                _die()
            return result

        Store.write_intervals_for_meter = patched
        return

    if code == "before-first-facts":
        original = Store.write_fact_partitions

        def patched(self, facts):
            if facts.height:
                _die()
            return original(self, facts)

        Store.write_fact_partitions = patched
        return

    if code == "after-first-facts":
        original = Store.write_fact_partitions
        state = {"calls": 0}

        def patched(self, facts):
            result = original(self, facts)
            state["calls"] += 1
            if state["calls"] >= 1:
                _die()
            return result

        Store.write_fact_partitions = patched
        return

    if code in ("before-affected", "after-affected"):
        original = Store.write_affected

        def patched(self, run_id, intervals, minutes):
            if code == "before-affected":
                _die()
            original(self, run_id, intervals, minutes)
            _die()

        Store.write_affected = patched
        return

    if code in ("before-run-meta", "after-run-meta"):
        original = Store.write_run_meta

        def patched(self, run_id, meta):
            if code == "before-run-meta":
                _die()
            original(self, run_id, meta)
            _die()

        Store.write_run_meta = patched
        return

    if code in ("before-prepare", "after-prepare"):
        original = Transaction.prepare

        def patched(self, meta=None):
            if code == "before-prepare":
                _die()
            original(self, meta)
            _die()

        Transaction.prepare = patched
        return

    if code == "after-commit":
        original = Transaction.commit

        def patched(self):
            original(self)
            _die()

        Transaction.commit = patched
        return

    if code == "publish-partial":
        _install_replace_counter(after)
        original_apply = Transaction.apply_manifest
        state = {"counting": False}

        def patched(self):
            # Count only the moves performed by the publish phase, not the
            # staging writes that preceded it.
            previous, _REPLACE_STATE["armed"] = _REPLACE_STATE.get("armed", False), True
            try:
                original_apply(self)
            finally:
                _REPLACE_STATE["armed"] = previous

        Transaction.apply_manifest = patched
        return

    if code == "after-publish-before-marker":
        import minenergy.storage as storage

        original_text = storage._write_atomic_text

        def patched_text(text, path):
            if Path(path).name == Transaction.COMMITTED:
                _die()
            original_text(text, path)

        storage._write_atomic_text = patched_text
        return

    if code == "recovery-partial":
        _install_replace_counter(after)
        _REPLACE_STATE["armed"] = True
        return

    raise SystemExit(f"unknown crash code {code}")


_REPLACE_STATE: dict = {}


def _install_replace_counter(after: int) -> None:
    real_replace = os.replace
    state = {"n": 0}

    def counting_replace(src, dst, *args, **kwargs):
        real_replace(src, dst, *args, **kwargs)
        if _REPLACE_STATE.get("armed") or _REPLACE_STATE.get("counting"):
            state["n"] += 1
            if state["n"] >= after:
                _die()

    os.replace = counting_replace
    _REPLACE_STATE["counter"] = state


def scenario_frames(batch_dir: Path):
    """Write the crash scenario's seed/config/revision parquet inputs.

    Returns the paths.  The scenario has two active meters whose revision
    intervals cross local-day boundaries, so a single job touches several
    interval files and several minute partitions, plus a third meter that the
    job never touches.
    """
    import minenergy.verify as verify
    from minenergy.schema import SECOND_US

    base = verify.BASE
    day1 = base
    day2 = base + 24 * 3600 * SECOND_US
    day3 = base + 48 * 3600 * SECOND_US

    seed_samples = [
        # M1 UTC, cross-midnight interval rewrites local dates Jan 1 and Jan 2
        verify.sample_row("M1", day1 + 0 * 3600 * SECOND_US, 1, 0.0, "B1"),
        verify.sample_row("M1", day1 + 23 * 3600 * SECOND_US, 2, 100.0, "B1"),
        verify.sample_row("M1", day2 + 1 * 3600 * SECOND_US, 3, 120.0, "B1"),
        verify.sample_row("M1", day2 + 12 * 3600 * SECOND_US, 4, 150.0, "B1"),
        verify.sample_row("M1", day3 + 0 * 3600 * SECOND_US, 5, 180.0, "B1"),
        # M2 America/New_York: UTC instants map to local Jan 1 / Jan 2
        verify.sample_row("M2", day1 + 5 * 3600 * SECOND_US, 1, 0.0, "B2"),
        verify.sample_row("M2", day2 + 2 * 3600 * SECOND_US, 2, 50.0, "B2"),
        verify.sample_row("M2", day2 + 6 * 3600 * SECOND_US, 3, 60.0, "B2"),
        verify.sample_row("M2", day3 + 2 * 3600 * SECOND_US, 4, 80.0, "B2"),
        verify.sample_row("M2", day3 + 6 * 3600 * SECOND_US, 5, 90.0, "B2"),
        # M3 is never touched by the revision job
        verify.sample_row("M3", day1 + 0 * 3600 * SECOND_US, 1, 0.0, "B3"),
        verify.sample_row("M3", day1 + 1 * 3600 * SECOND_US, 2, 5.0, "B3"),
        verify.sample_row("M3", day1 + 2 * 3600 * SECOND_US, 3, 9.0, "B3"),
    ]
    seed_configs = [
        verify.config_row("M1", day1 - 86400 * SECOND_US, 1, 1.0),
        verify.config_row("M2", day1 - 86400 * SECOND_US, 1, 1.0),
        verify.config_row("M3", day1 - 86400 * SECOND_US, 1, 1.0),
    ]
    # revisions: both meters, each touching two local-day partitions
    revision_samples = [
        verify.sample_row("M1", day2 + 1 * 3600 * SECOND_US, 9, 140.0, "B1"),
        verify.sample_row("M2", day2 + 6 * 3600 * SECOND_US, 9, 75.0, "B2"),
    ]
    settings = {
        "M1": {"timezone": "UTC", "gap_threshold_seconds": 100000},
        "M2": {"timezone": "America/New_York", "gap_threshold_seconds": 100000},
        "M3": {"timezone": "UTC", "gap_threshold_seconds": 7200},
    }

    batch_dir.mkdir(parents=True, exist_ok=True)
    verify.settings_file(batch_dir / "settings.json", settings)
    verify.raw_samples_frame(seed_samples).write_parquet(batch_dir / "seed.parquet")
    verify.raw_configs_frame(seed_configs).write_parquet(batch_dir / "configs.parquet")
    verify.raw_samples_frame(revision_samples).write_parquet(batch_dir / "revision.parquet")
    return {
        "settings": batch_dir / "settings.json",
        "seed": batch_dir / "seed.parquet",
        "configs": batch_dir / "configs.parquet",
        "revision": batch_dir / "revision.parquet",
    }


def cmd_seed(args) -> int:
    batch_dir = Path(args.batch_dir)
    frames = scenario_frames(batch_dir)
    Store(args.store).append_raw(
        samples=pl.read_parquet(frames["seed"]),
        configs=pl.read_parquet(frames["configs"]),
    )
    full_run(args.store, frames["settings"], run_id="seed")
    return 0


def cmd_job(args) -> int:
    install_crash(args.crash_at, args.after)
    incremental_run(
        args.store,
        args.settings,
        run_id=args.run_id,
        new_samples=pl.read_parquet(args.batch) if args.batch else None,
    )
    return 0


def cmd_recover(args) -> int:
    install_crash(args.crash_at, args.after)
    Store(args.store).recover()
    return 0


def cmd_query(args) -> int:
    import minenergy.verify as verify

    store = Store(args.store)
    facts = store.read_facts().sort("meter_id", "minute_epoch_us")
    intervals = store.read_intervals().sort("meter_id", "start_us")
    canon = store.read_canonical_samples().sort("meter_id", "event_time_us")
    partitions = store.existing_fact_partitions()
    runs = {}
    affected = {}
    for run_id in store.list_runs():
        runs[run_id] = store.read_run_meta(run_id)
        base = store.root / "affected" / run_id
        ipath, mpath = base / "intervals.parquet", base / "minutes.parquet"
        affected[run_id] = {
            "intervals_rows": pl.read_parquet(ipath).height if ipath.exists() else 0,
            "minutes_rows": pl.read_parquet(mpath).height if mpath.exists() else 0,
            "has_run_json": (base / "run.json").exists(),
        }
    payload = {
        "facts_rows": facts.height,
        "energy_total": float(facts["energy_wh"].sum()) if facts.height else 0.0,
        "energy_by_meter": {
            r["meter_id"]: float(r["energy_wh"])
            for r in facts.group_by("meter_id").agg(pl.col("energy_wh").sum()).to_dicts()
        },
        "facts_by_partition": {
            f"{m}:{d}": float(facts.filter((pl.col("meter_id") == m) & (pl.col("local_date").cast(pl.String) == d))["energy_wh"].sum())
            for (m, d) in sorted(partitions)
        },
        "interval_rows": intervals.height,
        "intervals_by_meter": {
            r["meter_id"]: r["len"]
            for r in intervals.group_by("meter_id").len().to_dicts()
        },
        "raw_samples": store.read_raw_samples().height,
        "canonical_samples": canon.height,
        "canonical_counters": {
            f"{r['meter_id']}@{r['event_time_us']}": r["counter_wh"]
            for r in canon.to_dicts()
        },
        "runs": runs,
        "affected": affected,
        "txn_dirs": sorted(p.name for p in (store.root / "state" / "txn").glob("*"))
        if (store.root / "state" / "txn").exists()
        else [],
        "partitions": sorted(f"{m}:{d}" for (m, d) in partitions),
    }
    problems = verify.check_invariants(facts) + verify.check_conservation(facts, intervals)
    payload["invariants"] = problems
    print(json.dumps(payload, indent=2, default=str))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["seed", "job", "recover", "query"])
    parser.add_argument("--store", required=True)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--batch", default=None)
    parser.add_argument("--batch-dir", default=None)
    parser.add_argument("--run-id", default="revise")
    parser.add_argument("--crash-at", default="none")
    parser.add_argument("--after", type=int, default=1)
    args = parser.parse_args(argv)
    handler = {
        "seed": cmd_seed,
        "job": cmd_job,
        "recover": cmd_recover,
        "query": cmd_query,
    }[args.mode]
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
