"""Request-admission failure regressions for the Flight fact service.

The two bug tickets both live on the *request admission* call chain -- the
work between "a request arrives" and "a read permit protects the transfer":

* **A (mixed revisions):** ``settle_store`` ran before the shared read hold
  existed.  A publisher that moved its first replacement fact partition in
  that gap left the committed tree spanning two generations, and the stream
  then read both.  This module reproduces the gap with a real
  prepare/commit file-move process; the barrier only parks timing, it never
  synthesises a scan.
* **B (wrong UNKNOWN_METER, takeover skipped):** a prepared-but-dead
  transaction carrying a new meter was not taken over before the meter-id
  decision, so the first request for that meter was rejected from a
  half-revised directory and the orphaned transaction was left behind.

Derived regressions cover the rest of the same chain: an owner exiting
normally while a reader waits, an owner dying while a reader waits, the
takeover itself being killed, and two consumers being admitted together
(including two readers racing the recovery path, which must not deadlock on a
shared->exclusive lock upgrade).

Every external actor here is a real subprocess with an asserted exit status,
all waits have deadlines, and correctness is checked from the socket peer's
batch sequence and from the final on-disk disposition of the transaction.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ADM_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

WORKER = ADM_DIR / "admission_worker.py"

# The exact status the self-killing workers use; distinct from ordinary
# success/failure so a test can prove the kill really happened.
CRASH_EXIT = 77
ADMISSION_TIMEOUT = 180.0


# ---------------------------------------------------------------------------
# process / server harness
# ---------------------------------------------------------------------------
def _env(barrier_dir: Path | None = None) -> dict:
    env = dict(os.environ)
    parts = [str(ADM_DIR), str(ROOT)]
    if env.get("PYTHONPATH"):
        parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = ":".join(parts)
    if barrier_dir is not None:
        env["MINENERGY_ADMISSION_BARRIER_DIR"] = str(barrier_dir)
        env["MINENERGY_ADMISSION_BARRIER_TIMEOUT"] = str(int(ADMISSION_TIMEOUT))
    return env


def run_worker(*args: str, timeout: float = ADMISSION_TIMEOUT) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(WORKER), *args],
        capture_output=True,
        text=True,
        env=_env(),
        timeout=timeout,
    )


def start_worker(*args: str, barrier_dir: Path | None = None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(WORKER), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_env(barrier_dir),
    )


def wait_for_file(path: Path, timeout: float = ADMISSION_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"timeout waiting for {path}")
        time.sleep(0.02)


def proc_done(proc: subprocess.Popen, timeout: float = ADMISSION_TIMEOUT) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise


class AdmissionFlightServer:
    """A real flight_server subprocess, optionally with the timing barrier."""

    def __init__(self, store: Path, barrier_dir: Path | None = None):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "minenergy.flight_server", "--store", str(store)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_env(barrier_dir),
        )
        line = self.proc.stdout.readline()
        if not line:
            stderr = self.proc.stderr.read()
            raise RuntimeError(f"flight server failed to start: {stderr[-3000:]}")
        info = json.loads(line)
        assert info["status"] == "ready"
        self.uri = info["location"]

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:  # pragma: no cover
                self.proc.kill()
                self.proc.wait(timeout=10)
        for fh in (self.proc.stdout, self.proc.stderr):
            try:
                fh.close()
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


@pytest.fixture
def workdir():
    path = Path(tempfile.mkdtemp(prefix="minenergy-admission-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# store helpers
# ---------------------------------------------------------------------------
def seed_store(work: Path) -> dict:
    store, settings = work / "store", work / "settings.json"
    res = run_worker("seed", "--store", str(store), "--settings", str(settings))
    assert res.returncode == 0, res.stderr
    return {"store": store, "settings": settings}


def _worker_module():
    """Load the subprocess worker as a module regardless of package layout."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("admission_worker_lib", WORKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_revision_batch(work: Path) -> Path:
    """Build the 10 -> 20 revision frame using the same construction code."""
    return _worker_module().revision_frame(work / "revision.parquet")


def make_new_meter_batch(work: Path, meter: str = "M1") -> Path:
    return _worker_module().new_meter_frame(work / f"new-{meter}.parquet", meter)


def txn_state(store: Path) -> list[dict]:
    res = run_worker("txn-state", "--store", str(store))
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)["txn"]


def read_client_result(path: Path) -> dict:
    return json.loads(Path(path).read_text())


def query_doc(meters, start="2025-01-01T00:00:00Z", end="2025-01-03T00:00:00Z",
              batch_size: int = 32) -> dict:
    return {
        "meters": list(meters),
        "start": start,
        "end": end,
        "select": ["meter_id", "local_date", "minute_epoch_us", "energy_wh"],
        "batch_size": batch_size,
    }


def start_client(server: AdmissionFlightServer, work: Path, query: dict,
                 pause: Path | None = None) -> tuple[subprocess.Popen, Path]:
    qfile, out = work / "query.json", work / "client-result.json"
    qfile.write_text(json.dumps(query))
    signal = work / "client-parked.signal"
    kwargs = {}
    args = [
        "client", "--uri", server.uri,
        "--query-json", str(qfile), "--out", str(out),
    ]
    if pause is not None:
        args += ["--pause-file", str(pause), "--signal-file", str(signal)]
    proc = start_worker(*args)
    return proc, out


def assert_no_txns_left(store: Path) -> None:
    leftover = [t for t in txn_state(store) if not t["committed"]]
    assert leftover == [], f"unfinished transaction left behind: {leftover}"


def assert_one_revision_per_date(result: dict, expected_per_date: float) -> None:
    """Socket-peer check: every batch's values belong to one generation.

    Zero-energy rows are legitimate (invalid-only gap minutes carry 0.0), so
    the generation check uses the *nonzero* per-minute value: the seed puts
    the whole day's energy uniformly on 60 valid minutes, hence one scalar
    ``expected_per_date / 60`` across the entire batch stream.  A two-generation
    mixture would expose two distinct nonzero scalars.
    """
    assert result["status"] == "ok", result
    per_date = result["energy_by_meter_date"]
    # exactly two (meter,date) groups for the two-day store, each the expected
    # generation's total, and no group carrying both 10- and 20-era values
    assert set(per_date) == {"M0:2025-01-01", "M0:2025-01-02"}, per_date
    for key, total in per_date.items():
        assert total == pytest.approx(expected_per_date), (key, total)
    nonzero = {
        v for b in result["batches"] for v in b["energy_wh"] if v is not None and v != 0.0
    }
    expected_scalar = expected_per_date / 60.0
    assert len(nonzero) == 1, f"mixed generations in one stream: {sorted(nonzero)}"
    assert next(iter(nonzero)) == pytest.approx(expected_scalar)
    # each batch individually must also be single-generation
    for batch in result["batches"]:
        bvals = {v for v in batch["energy_wh"] if v is not None and v != 0.0}
        assert bvals <= nonzero, batch


# ---------------------------------------------------------------------------
# A: settle -> hold gap exploited by a real publishing process
# ---------------------------------------------------------------------------
def test_A_reader_admits_then_publisher_crashes_mid_publish_no_mix(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_revision_batch(workdir)
    barrier = workdir / "barrier"
    out = workdir / "client-result.json"

    with AdmissionFlightServer(store, barrier_dir=barrier) as srv:
        client, _ = start_client(srv, workdir, query_doc(["M0"]))
        # Admission #1 is the GetFlightInfo metadata call: let it pass
        # untouched (idle store, nothing to recover).
        wait_for_file(barrier / "reached-1")
        (barrier / "release-1").write_text("1\n")
        # Admission #2 is the DoGet transfer call.  The request thread has run
        # settle_store (idle store -> instant return) and is parked before
        # FlightReadHold is taken.
        wait_for_file(barrier / "reached-2")

        # A real writer process runs the full prepare/commit path and kills
        # itself with status 77 right after the first fact partition replace
        # has landed: one date is already generation-20, the other is still
        # generation-10, and a prepared txn remains on disk.  No scan result
        # is faked -- the kill sits inside the real apply_manifest moves.
        writer = run_worker(
            "revise", "--store", str(store), "--settings", str(settings),
            "--batch", str(batch), "--run-id", "ticket-A", "--exit", str(CRASH_EXIT),
        )
        assert writer.returncode == CRASH_EXIT, writer.stderr

        txns = txn_state(store)
        assert len(txns) == 1 and txns[0]["manifest"] and not txns[0]["committed"]

        # Release the transfer admission.  A correct admission re-decides
        # under the shared hold, takes over the dead publish (exclusive lock
        # on a lock-free frame, never an in-place upgrade), then streams one
        # coherent generation.  The old settle-then-lock ordering would take
        # the hold against the already-mixed tree and stream both revisions.
        (barrier / "release-2").write_text("1\n")
        assert proc_done(client) == 0
        result = read_client_result(out)

    # Every batch over the socket carries generation-20 values only.
    assert_one_revision_per_date(result, expected_per_date=20.0)
    # The takeover consumed the orphaned transaction; nothing is left behind.
    assert_no_txns_left(store)


# ---------------------------------------------------------------------------
# B: dead owner's new-meter transaction must be taken over before judging ids
# ---------------------------------------------------------------------------
def test_B_new_meter_in_dead_transaction_is_admitted_not_rejected(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_new_meter_batch(workdir, "M1")

    # Stage a fully valid transaction for M1, then end its owner before any
    # fact moves into the formal tree.
    stager = run_worker(
        "stage-new", "--store", str(store), "--settings", str(settings),
        "--batch", str(batch), "--meter", "M1",
        "--run-id", "ticket-B", "--exit", str(CRASH_EXIT),
    )
    assert stager.returncode == CRASH_EXIT, stager.stderr
    txns = txn_state(store)
    assert len(txns) == 1 and txns[0]["manifest"] and not txns[0]["committed"]

    with AdmissionFlightServer(store) as srv:
        # First-ever request for the new meter must succeed: admission takes
        # the dead transaction over (its partition becomes committed), and the
        # meter decision uses that settled state.
        client, out = start_client(srv, workdir, query_doc(["M1"]))
        assert proc_done(client) == 0
        result = read_client_result(out)
        assert result["status"] == "ok", result
        assert result["rows"] == 60, result
        assert set(result["energy_by_meter_date"]) == {"M1:2025-01-01"}
        assert result["energy_by_meter_date"]["M1:2025-01-01"] == pytest.approx(30.0)

        # A genuinely unknown meter is still rejected, with the parameter
        # marker, and the failed request creates no stream / leaves no txn.
        bad_q = workdir / "bad-query.json"
        bad_q.write_text(json.dumps(query_doc(["M9"])))
        bad = run_worker(
            "client", "--uri", srv.uri, "--query-json", str(bad_q),
            "--out", str(workdir / "bad-result.json"),
        )
        assert bad.returncode != 0
        bad_result = read_client_result(workdir / "bad-result.json")
        assert bad_result["status"] == "error"
        assert "MINENERGY_PARAM_INVALID" in bad_result["message"]
        assert "UNKNOWN_METER" in bad_result["message"]

    assert_no_txns_left(store)


# ---------------------------------------------------------------------------
# regression 1: owner exits normally while a reader is waiting
# ---------------------------------------------------------------------------
def test_owner_exits_normally_while_reader_waits(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_revision_batch(workdir)
    parked, release = workdir / "parked", workdir / "release"
    out = workdir / "client-result.json"

    with AdmissionFlightServer(store) as srv:
        owner = start_worker(
            "publish-park", "--store", str(store), "--settings", str(settings),
            "--batch", str(batch), "--run-id", "owner-clean",
            "--parked-file", str(parked), "--release-file", str(release),
        )
        wait_for_file(parked)  # prepared, owner alive, inside commit
        client, _ = start_client(srv, workdir, query_doc(["M0"]))
        # Give the reader time to reach admission and start waiting on the
        # live prepared publisher.
        time.sleep(0.5)
        assert owner.poll() is None
        # Normal exit: let the parked commit run to completion.
        release.write_text("1\n")
        assert proc_done(owner) == 0
        assert proc_done(client) == 0
        result = read_client_result(out)

    assert_one_revision_per_date(result, expected_per_date=20.0)
    assert_no_txns_left(store)


# ---------------------------------------------------------------------------
# regression 2: owner dies while a reader is waiting inside admission
# ---------------------------------------------------------------------------
def test_owner_dies_while_reader_waits_then_reader_takes_over(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_revision_batch(workdir)
    parked, release = workdir / "parked", workdir / "release"
    out = workdir / "client-result.json"

    with AdmissionFlightServer(store) as srv:
        owner = start_worker(
            "publish-park", "--store", str(store), "--settings", str(settings),
            "--batch", str(batch), "--run-id", "owner-dies",
            "--parked-file", str(parked), "--release-file", str(release),
        )
        wait_for_file(parked)
        client, _ = start_client(srv, workdir, query_doc(["M0"]))
        time.sleep(0.5)
        # Kill the owner instead of releasing it: the prepared txn loses its
        # liveness lock while the reader is still inside the admission loop.
        owner.kill()
        killed_status = proc_done(owner)
        assert killed_status < 0  # terminated by signal, as a real death would
        # Never release the barrier file: progress must come solely from the
        # admission loop noticing the dead owner and replaying the manifest.
        assert proc_done(client, timeout=ADMISSION_TIMEOUT) == 0
        result = read_client_result(out)

    assert_one_revision_per_date(result, expected_per_date=20.0)
    assert_no_txns_left(store)


# ---------------------------------------------------------------------------
# regression 3: the takeover itself is killed, then a request resumes it
# ---------------------------------------------------------------------------
def test_takeover_interrupted_is_resumed_by_next_request(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_revision_batch(workdir)

    first = run_worker(
        "revise", "--store", str(store), "--settings", str(settings),
        "--batch", str(batch), "--run-id", "takeover-victim",
        "--exit", str(CRASH_EXIT),
    )
    assert first.returncode == CRASH_EXIT
    # A recovery process starts the forward replay and is itself killed after
    # its first manifest move (exit 77): same criteria, still resumable.
    rec = run_worker("recover", "--store", str(store), "--exit", str(CRASH_EXIT))
    assert rec.returncode == CRASH_EXIT
    txns = txn_state(store)
    assert len(txns) == 1 and txns[0]["manifest"] and not txns[0]["committed"]

    with AdmissionFlightServer(store) as srv:
        client, out = start_client(srv, workdir, query_doc(["M0"]))
        assert proc_done(client) == 0
        result = read_client_result(out)

    assert_one_revision_per_date(result, expected_per_date=20.0)
    assert_no_txns_left(store)


# ---------------------------------------------------------------------------
# regression 4: two consumers admitted together share the lock, no deadlock
# ---------------------------------------------------------------------------
def test_two_consumers_admitted_concurrently_both_complete(workdir):
    # A long stream keeps each scanner's read permit held while the other
    # consumer is admitted: shared locks must coexist.  60k rows with a wide
    # projection exceeds the gRPC flow-control window, so a client parked
    # after one batch keeps the server-side scanner (and its permit) alive.
    store, settings = workdir / "bulk-store", workdir / "bulk-settings.json"
    res = run_worker("seed-bulk", "--store", str(store),
                     "--settings", str(settings), "--rows", "60000")
    assert res.returncode == 0, res.stderr
    q = query_doc(
        ["B0"], start="2025-01-01T00:00:00Z", end="2025-02-15T00:00:00Z",
        batch_size=128,
    )
    q["select"] = [
        "meter_id", "minute_epoch_us", "energy_wh", "minute_start_utc",
        "local_minute_label", "coverage_status", "quality_reasons",
    ]

    with AdmissionFlightServer(store) as srv:
        # distinct work subdirs so the two clients' control files never collide
        def launch(name: str):
            d = workdir / name
            d.mkdir(parents=True, exist_ok=True)
            pause = d / "pause"
            # The trigger must pre-exist: the client parks after its first
            # batch whenever it sees this file; the release is "pause.release".
            pause.write_text("arm\n")
            proc, out = start_client(srv, d, q, pause=pause)
            return proc, out, pause, d

        p1, o1, pause1, d1 = launch("consumer-1")
        wait_for_file(d1 / "client-parked.signal")
        p2, o2, pause2, d2 = launch("consumer-2")
        wait_for_file(d2 / "client-parked.signal")
        # Both scanners are parked mid-stream holding shared permits at once.
        assert p1.poll() is None and p2.poll() is None
        pause1.with_suffix(".release").write_text("1\n")
        pause2.with_suffix(".release").write_text("1\n")
        assert proc_done(p1) == 0
        assert proc_done(p2) == 0
        r1, r2 = read_client_result(o1), read_client_result(o2)

    assert r1["status"] == r2["status"] == "ok"
    assert r1["rows"] == r2["rows"] == 60_000
    assert r1["energy_total"] == pytest.approx(r2["energy_total"])


# ---------------------------------------------------------------------------
# regression 5: two readers race a dead-publish takeover (no upgrade deadlock)
# ---------------------------------------------------------------------------
def test_two_readers_racing_recovery_both_settle(workdir):
    seed = seed_store(workdir)
    store, settings = seed["store"], seed["settings"]
    batch = make_revision_batch(workdir)
    dead = run_worker(
        "revise", "--store", str(store), "--settings", str(settings),
        "--batch", str(batch), "--run-id", "race-dead",
        "--exit", str(CRASH_EXIT),
    )
    assert dead.returncode == CRASH_EXIT

    with AdmissionFlightServer(store) as srv:
        results = {}
        errors = []

        def consume(name: str):
            d = workdir / name
            d.mkdir(parents=True, exist_ok=True)
            try:
                proc, out = start_client(srv, d, query_doc(["M0"]))
                code = proc_done(proc, timeout=ADMISSION_TIMEOUT)
                results[name] = (code, read_client_result(out))
            except Exception as exc:  # pragma: no cover - failure reporting
                errors.append(repr(exc))

        threads = [threading.Thread(target=consume, args=(f"race-{i}",)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=ADMISSION_TIMEOUT + 30)
            assert not t.is_alive(), "admission deadlocked: a reader never finished"

    assert not errors, errors
    assert len(results) == 2
    for code, result in results.values():
        assert code == 0
        assert result["status"] == "ok", result
        assert_one_revision_per_date(result, expected_per_date=20.0)
    assert_no_txns_left(store)
