"""Subprocess harness for the Flight *request-admission* regression tests.

Every mode here runs in its own process and exits with a documented status, so
the tests can assert real process boundaries (a writer that dies holding a
transaction, a client that talks over a real socket) rather than in-process
mocks::

    admission_worker.py seed      --store DIR --settings FILE
    admission_worker.py revise    --store DIR --settings FILE --batch FILE
                                 --run-id ID [--exit CODE]
    admission_worker.py stage-new --store DIR --settings FILE --batch FILE
                                 --run-id ID --meter ID [--exit CODE]
    admission_worker.py client    --uri grpc://... --query-json FILE
                                 --out FILE [--pause-file F --signal-file F]
    admission_worker.py recover   --store DIR [--exit CODE]
    admission_worker.py txn-state --store DIR

Exit status contract:
  * 0  -> the mode finished cleanly;
  * 77 -> the process killed itself on purpose (``os._exit(77)``) at the named
          point, leaving a real transaction directory for a reader to take
          over; nothing here fakes a recovery scan -- the kill happens inside
          the real Transaction.prepare/commit file-move path.

The client mode consumes a DoGet stream batch by batch over the socket and
records, per batch, every (meter_id, local_date, energy_wh) value it saw, so a
two-revision mixture is detectable from the batch sequence itself.  An
optional pause is honoured *between batches* (the server-side scanner is
already holding its read permit at that point), coordinated through plain
files so no test-only RPC is added to the service.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from minenergy.engine import full_run, incremental_run  # noqa: E402
from minenergy.storage import Store, Transaction  # noqa: E402
from minenergy.verify import (  # noqa: E402
    BASE,
    config_row,
    raw_configs_frame,
    raw_samples_frame,
    sample_row,
    settings_file,
)
from minenergy.schema import SECOND_US  # noqa: E402

CRASH_EXIT = 77


def _wait_for(path: Path, timeout: float = 120.0) -> None:
    """Block until a file appears (cross-process barrier), with a hard deadline."""
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise SystemExit(f"timeout waiting for barrier file {path}")
        time.sleep(0.01)


# ---------------------------------------------------------------------------
# store construction
# ---------------------------------------------------------------------------
def build_two_day_store(store: Path, settings_path: Path) -> None:
    """One meter with one fact partition per local date, energy_wh == 10 each.

    Two independent clean intervals (2 samples each), one on 2025-01-01 and
    one on 2025-01-02, each integrating to 10 Wh spread over its minutes.
    """
    settings_file(
        settings_path,
        {"M0": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
    )
    day1, day2 = BASE, BASE + 24 * 3600 * SECOND_US
    samples = [
        sample_row("M0", day1, 1, 0.0, "B"),
        sample_row("M0", day1 + 3600 * SECOND_US, 2, 10.0, "B"),
        sample_row("M0", day2, 1, 0.0, "B"),
        sample_row("M0", day2 + 3600 * SECOND_US, 2, 10.0, "B"),
    ]
    configs = [config_row("M0", day1 - 86_400 * SECOND_US, 1, 1.0)]
    Store(store).append_raw(raw_samples_frame(samples), raw_configs_frame(configs))
    full_run(store, settings_path, run_id="seed")


def revision_frame(path: Path) -> Path:
    """Two higher-seq samples revising both days' counters to 20 Wh."""
    day1, day2 = BASE, BASE + 24 * 3600 * SECOND_US
    frame = raw_samples_frame(
        [
            sample_row("M0", day1 + 3600 * SECOND_US, 9, 20.0, "B"),
            sample_row("M0", day2 + 3600 * SECOND_US, 9, 20.0, "B"),
        ]
    )
    frame.write_parquet(path)
    return path


def new_meter_frame(path: Path, meter: str) -> Path:
    """A second meter's data, staged as a valid transaction but never published."""
    day1 = BASE
    frame = raw_samples_frame(
        [
            sample_row(meter, day1 + 2 * 3600 * SECOND_US, 1, 0.0, "C"),
            sample_row(meter, day1 + 3 * 3600 * SECOND_US, 2, 30.0, "C"),
        ]
    )
    frame.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# modes
# ---------------------------------------------------------------------------
def cmd_seed(args) -> int:
    build_two_day_store(Path(args.store), Path(args.settings))
    return 0


def _kill_after_first_publish_move(exit_code: int, facts_only: bool = False) -> None:
    """Die inside commit(), right after the first real staged->target move.

    The patch wraps the storage-level ``os.replace`` *only while the manifest
    is being applied*, so staging writes before prepare are unaffected and the
    kill leaves a genuine prepared-but-unmarked, partially published txn.
    With ``facts_only`` the kill is counted only once a replacement lands in
    the committed ``facts/`` tree (failure A: die after the first *fact*
    partition is replaced, while the other date still holds the old revision).
    """
    import minenergy.storage as storage

    real_replace = os.replace
    state = {"armed": False, "n": 0}

    def counting_replace(src, dst, *a, **kw):
        real_replace(src, dst, *a, **kw)
        if not state["armed"]:
            return
        if facts_only and f"{os.sep}facts{os.sep}" not in str(dst):
            return
        state["n"] += 1
        if state["n"] >= 1:
            os._exit(exit_code)

    os.replace = counting_replace
    real_apply = Transaction.apply_manifest

    def patched_apply(self):
        state["armed"] = True
        try:
            real_apply(self)
        finally:
            state["armed"] = False

    Transaction.apply_manifest = patched_apply
    storage.os = os  # keep the module's name lookup consistent under the patch


def cmd_revise(args) -> int:
    if args.exit is not None:
        _kill_after_first_publish_move(args.exit, facts_only=True)
    incremental_run(
        args.store,
        args.settings,
        run_id=args.run_id,
        new_samples=pl.read_parquet(args.batch),
    )
    return 0


def cmd_publish_park(args) -> None:
    """Prepare a real revision, then stay alive inside commit() on a barrier.

    The owner is genuinely alive (liveness lock held, manifest on disk, no
    COMMITTED marker) until the orchestrator releases the barrier -- a normal
    commit then runs -- or kills the process, leaving a prepared transaction
    whose owner just died.  Both branches are exercised by the regression.
    """
    real_commit = Transaction.commit

    def park_then_commit(self):
        parked = Path(args.parked_file)
        parked.parent.mkdir(parents=True, exist_ok=True)
        parked.write_text("1\n", encoding="utf-8")
        _wait_for(Path(args.release_file), timeout=180.0)
        return real_commit(self)

    Transaction.commit = park_then_commit
    incremental_run(
        args.store,
        args.settings,
        run_id=args.run_id,
        new_samples=pl.read_parquet(args.batch),
    )


def cmd_seed_bulk(args) -> int:
    """One meter with one fact per minute for ``--rows`` minutes (long stream)."""
    settings_file(
        Path(args.settings),
        {"B0": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
    )
    base = BASE
    samples = [
        sample_row("B0", base + i * 60 * SECOND_US, 1, float(i), "B")
        for i in range(args.rows + 1)
    ]
    configs = [config_row("B0", base - 86_400 * SECOND_US, 1, 1.0)]
    Store(args.store).append_raw(raw_samples_frame(samples), raw_configs_frame(configs))
    full_run(args.store, args.settings, run_id="bulk-seed")
    return 0


def cmd_stage_new(args) -> int:
    """Run the real job up to a prepared manifest, then terminate the owner.

    A valid, fully staged transaction for a brand-new meter is left on disk:
    its fact partition exists only under ``state/txn/<id>/staged/`` and no
    ``meter_id=`` partition has appeared in the committed tree yet.  Killing
    the owner releases its per-txn liveness lock exactly as a dead owner would
    leave things.
    """
    real_commit = Transaction.commit

    def die_after_prepare(self):
        # prepare() has atomically written the manifest; commit would be the
        # next call.  Kill before any staged file moves, so the target facts
        # are not yet in their formal location.
        os._exit(args.exit if args.exit is not None else CRASH_EXIT)

    Transaction.commit = die_after_prepare
    try:
        incremental_run(
            args.store,
            args.settings,
            run_id=args.run_id,
            new_samples=pl.read_parquet(args.batch),
        )
    finally:
        Transaction.commit = real_commit
    raise SystemExit("stage-new: commit patch did not fire")


def cmd_recover(args) -> int:
    if args.exit is not None:
        # Die after the first manifest replay move, i.e. the takeover itself
        # is interrupted; the unchanged recovery criteria must let a later
        # process resume it.
        import minenergy.storage as storage

        real_replace = os.replace
        state = {"n": 0}

        def counting_replace(src, dst, *a, **kw):
            real_replace(src, dst, *a, **kw)
            state["n"] += 1
            if state["n"] >= 1:
                os._exit(args.exit)

        os.replace = counting_replace
        storage.os = os
    handled = Store(args.store).recover()
    print(json.dumps({"handled": handled}))
    return 0


def cmd_txn_state(args) -> int:
    root = Path(args.store)
    base = root / "state" / "txn"
    out = []
    if base.exists():
        for d in sorted(base.iterdir()):
            if not d.is_dir():
                continue
            out.append(
                {
                    "txid": d.name,
                    "manifest": (d / "manifest.json").exists(),
                    "committed": (d / Transaction.COMMITTED).exists(),
                    "lock": (d / ".lock").exists(),
                }
            )
    print(json.dumps({"txn": out}, indent=2))
    return 0


def cmd_client(args) -> None:
    import pyarrow.flight as fl

    query = json.loads(Path(args.query_json).read_text())
    client = fl.FlightClient(args.uri)
    try:
        info = client.get_flight_info(
            fl.FlightDescriptor.for_command(json.dumps(query).encode())
        )
        reader = client.do_get(info.endpoints[0].ticket)
        batches: list[dict] = []
        all_rows: list[dict] = []
        # Read batch-by-batch from the socket; app_metadata per batch carries
        # the server's batch_index, so the consumer can prove which revision
        # each batch came from.
        for chunk in reader:
            if chunk.data is None:
                continue
            rows = chunk.data.to_pylist()
            meta = json.loads(chunk.app_metadata.to_pybytes().decode()) if chunk.app_metadata else {}
            batches.append(
                {
                    "batch_index": meta.get("batch_index"),
                    "rows": len(rows),
                    "energy_wh": [r.get("energy_wh") for r in rows],
                    "meters": sorted({r.get("meter_id") for r in rows}),
                    "dates": sorted({str(r.get("local_date")) for r in rows}),
                }
            )
            all_rows.extend(rows)
            if args.pause_file and Path(args.pause_file).exists():
                # The scanner is parked with its read permit held.  Signal the
                # orchestrator and wait to be released; the barrier files are
                # the only synchronisation channel.
                signal = Path(args.signal_file)
                signal.parent.mkdir(parents=True, exist_ok=True)
                signal.write_text("parked\n")
                _wait_for(Path(args.pause_file).with_suffix(".release"))
        summary = {
            "status": "ok",
            "schema": [f.name for f in reader.schema],
            "batches": batches,
            "rows": len(all_rows),
            "energy_total": sum(r.get("energy_wh", 0.0) for r in all_rows),
        }
        # The (meter, local date) breakdown only exists when the caller asked
        # for those columns; wide projections in the concurrency tests omit
        # them, so the summary must not assume they are present.
        if all_rows and "meter_id" in all_rows[0] and "local_date" in all_rows[0]:
            summary["energy_by_meter_date"] = {
                f"{r['meter_id']}:{r['local_date']}": round(
                    sum(
                        x["energy_wh"]
                        for x in all_rows
                        if x["meter_id"] == r["meter_id"]
                        and str(x["local_date"]) == str(r["local_date"])
                    ),
                    9,
                )
                for r in all_rows
            }
        Path(args.out).write_text(json.dumps(summary, indent=2, default=str))
    except Exception as exc:  # RPC-level failure: record it for the orchestrator
        Path(args.out).write_text(
            json.dumps(
                {
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
            )
        )
        raise
    finally:
        client.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("seed")
    p.add_argument("--store", required=True)
    p.add_argument("--settings", required=True)
    p.set_defaults(func=cmd_seed)

    p = sub.add_parser("revise")
    p.add_argument("--store", required=True)
    p.add_argument("--settings", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--run-id", default="revise")
    p.add_argument("--exit", type=int, default=None)
    p.set_defaults(func=cmd_revise)

    p = sub.add_parser("stage-new")
    p.add_argument("--store", required=True)
    p.add_argument("--settings", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--meter", required=True)
    p.add_argument("--run-id", default="stage-new")
    p.add_argument("--exit", type=int, default=None)
    p.set_defaults(func=cmd_stage_new)

    p = sub.add_parser("publish-park")
    p.add_argument("--store", required=True)
    p.add_argument("--settings", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--run-id", default="publish-park")
    p.add_argument("--parked-file", required=True)
    p.add_argument("--release-file", required=True)
    p.set_defaults(func=cmd_publish_park)

    p = sub.add_parser("seed-bulk")
    p.add_argument("--store", required=True)
    p.add_argument("--settings", required=True)
    p.add_argument("--rows", type=int, default=20_000)
    p.set_defaults(func=cmd_seed_bulk)

    p = sub.add_parser("recover")
    p.add_argument("--store", required=True)
    p.add_argument("--exit", type=int, default=None)
    p.set_defaults(func=cmd_recover)

    p = sub.add_parser("txn-state")
    p.add_argument("--store", required=True)
    p.set_defaults(func=cmd_txn_state)

    p = sub.add_parser("client")
    p.add_argument("--uri", required=True)
    p.add_argument("--query-json", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--pause-file", default=None)
    p.add_argument("--signal-file", default=None)
    p.set_defaults(func=cmd_client)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
