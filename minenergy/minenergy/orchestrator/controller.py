"""The controller: executes one run's graph under a single-writer lock.

Execution rights
----------------
A controller takes ``<run_dir>/controller.lock`` with ``flock(LOCK_EX|LOCK_NB)``
before touching state.  The lock is held by the *process*, not recorded as a
PID, so a dead controller releases it automatically and a second live
controller is refused immediately without changing any state.  Only the lock
holder may launch, reap, cancel or finalize.

Recovery (taking over an old run)
---------------------------------
Re-taking a run never means "mark running jobs pending again".  At startup the
controller classifies every in-flight attempt recorded by its predecessor:

* wrapper still alive (``pid`` + ``/proc`` start-time match) → **adopt** it:
  wait for it and record the real outcome from its result file.  The old
  worker is never started twice and its success is never guessed;
* wrapper gone but a **result file** exists → the wrapper survived its worker
  and recorded the real exit; use it;
* wrapper gone and no result file → the outcome is unknowable (the worker may
  have finished, or may still be running orphaned).  The attempt is recorded
  as **interrupted** with whatever worker identity is known; the job waits for
  an explicit rerun, which is only accepted after the old attempt's process
  group is confirmed dead.

Cancellation
------------
``cancel`` first stops new instances from starting (the flag is checked every
loop iteration), then terminates in-flight attempts by process *group*
(SIGTERM, grace, SIGKILL), so a cancelled parent never leaves a spawned child
processing behind it.  Cancellation is idempotent: repeating it changes
nothing and starts nothing.

Exactly-once boundary (declared)
--------------------------------
The orchestrator guarantees that a job marked ``succeeded`` has exactly one
recorded attempt that really exited 0, and that every attempt is recorded
append-only.  It does **not** claim exactly-once side effects for arbitrary
processing: a worker killed mid-write may have partially applied its effects,
and a retried/interrupted job may run its logic again.  Downstream consumers
must rely on the recorded outcomes, not on the mere existence of output files.
"""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import process as proc
from .definition import Definition, load_definition, plan_lines, validate_dates
from .errors import ControllerConflictError, RunStateError
from .store import (
    A_CANCELLED,
    A_FAILED_EXIT,
    A_FAILED_TIMEOUT,
    A_INTERRUPTED,
    A_SUCCEEDED,
    BLOCKED,
    CANCELLED,
    FAILED,
    INTERRUPTED,
    LOCK_NAME,
    RUN_CANCELLED,
    RUN_COMPLETED,
    RUN_FAILED,
    SNAPSHOT_NAME,
    SUCCEEDED,
    RunStore,
    db_path,
)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_DEFINITION = 2
EXIT_CANCELLED = 3
EXIT_INTERRUPTED = 4
EXIT_CONFLICT = 5

REPO_ROOT = Path(__file__).resolve().parents[2]


def _child_env() -> dict:
    """Environment for wrapper processes: make the in-repo package importable."""
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(REPO_ROOT) if not existing else f"{REPO_ROOT}{os.pathsep}{existing}"
    )
    return env


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _attempt_paths(run_dir: Path, date: str, node: str, generation: int, seq: int) -> dict[str, str]:
    log_dir = run_dir / "logs" / f"{date}--{node}"
    stem = f"attempt-g{generation}-{seq}"
    return {
        "stdout": str(log_dir / f"{stem}.stdout.log"),
        "stderr": str(log_dir / f"{stem}.stderr.log"),
        "result": str(log_dir / f"{stem}.result.json"),
        "meta": str(log_dir / f"{stem}.meta.json"),
        "wrapper_log": str(log_dir / f"{stem}.wrapper.log"),
    }


class Controller:
    """Executes (or resumes) the run recorded in ``run_dir``."""

    def __init__(self, run_dir: str | Path, poll_interval: float = 0.05):
        self.run_dir = Path(run_dir)
        self.poll_interval = poll_interval
        self.store = RunStore(self.run_dir)
        run = self.store.get_run()
        self.max_parallel: int = run["max_parallel"]
        self.retry_delay: float = run["retry_delay_seconds"]
        self.term_grace: float = run["term_grace_seconds"]
        self._lock_fh = None
        # attempt_id -> subprocess.Popen (wrappers spawned by this process)
        self._local: dict[int, subprocess.Popen] = {}
        # attempt_id -> (pid, pid_start) adopted from a previous controller
        self._adopted: dict[int, tuple[int, str]] = {}
        self.events: list[str] = []  # human-readable trace, printed by the CLI

    # ---- execution rights ---------------------------------------------------
    def acquire(self) -> None:
        """Take execution rights; refuse cleanly if another controller holds them."""
        lock_path = self.run_dir / LOCK_NAME
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock_path, "a+")
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            raise ControllerConflictError(
                f"another controller holds execution rights for {self.run_dir}"
            ) from exc
        self._lock_fh = fh

    def release(self) -> None:
        if self._lock_fh is not None:
            try:
                fcntl.flock(self._lock_fh, fcntl.LOCK_UN)
            finally:
                self._lock_fh.close()
                self._lock_fh = None

    def _log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        line = f"[{stamp}] {message}"
        self.events.append(line)
        print(line, flush=True)

    # ---- recovery -----------------------------------------------------------
    def recover(self) -> None:
        """Classify every attempt left in flight by a previous controller."""
        for attempt in self.store.active_attempts():
            attempt_id = attempt["attempt_id"]
            label = f"{attempt['date']}/{attempt['node']} attempt#{attempt['seq']}"
            pid, pid_start = attempt["pid"], attempt["pid_start"]

            if pid is None:
                # The predecessor died between recording the attempt and
                # registering the wrapper identity.  The wrapper may still have
                # started and published its own identity in the meta file.
                meta = self._read_meta(attempt, retries=3)
                if meta and proc.pid_alive(meta.get("wrapper_pid"), meta.get("wrapper_pid_start")):
                    self.store.attach_wrapper_identity(
                        attempt_id, meta["wrapper_pid"], meta["wrapper_pid_start"]
                    )
                    self._adopted[attempt_id] = (
                        meta["wrapper_pid"], meta["wrapper_pid_start"]
                    )
                    self._log(f"recover: adopted {label} (wrapper found via meta file)")
                    continue

            elif proc.pid_alive(pid, pid_start):
                self._adopted[attempt_id] = (pid, pid_start)
                self._log(f"recover: adopted {label} (wrapper pid={pid} still alive)")
                continue

            # Wrapper is gone (or never known).  A recorded result is the only
            # acceptable outcome; otherwise the attempt is interrupted.
            result = self._read_result(attempt)
            if result is not None:
                self._log(f"recover: {label} finished while controller was down; recording result")
                self._record_outcome(attempt, result)
                continue
            meta = self._read_meta(attempt, retries=0)
            if meta:
                self.store.record_worker_identity(
                    attempt_id,
                    meta.get("worker_pid"),
                    meta.get("worker_pgid"),
                    meta.get("worker_pid_start") or "",
                )
            self.store.finish_attempt(
                attempt, A_INTERRUPTED,
                "controller lost before the outcome was recorded",
                retry_delay=self.retry_delay,
            )
            self._log(
                f"recover: {label} outcome unknowable -> interrupted"
                " (rerun once the old attempt is confirmed dead)"
            )

    def _read_meta(self, attempt: dict, retries: int = 0) -> dict | None:
        path = attempt.get("meta_path")
        if not path:
            return None
        for _ in range(retries + 1):
            meta = _read_json(Path(path))
            if meta is not None:
                return meta
            time.sleep(0.05)
        return None

    def _read_result(self, attempt: dict) -> dict | None:
        path = attempt.get("result_path")
        return _read_json(Path(path)) if path else None

    # ---- outcome recording --------------------------------------------------
    def _record_outcome(self, attempt: dict, result: dict) -> None:
        if result.get("timed_out"):
            self.store.finish_attempt(
                attempt, A_FAILED_TIMEOUT,
                f"timeout after {result.get('duration_seconds', '?')}s",
                exit_code=result.get("exit_code"), signal=result.get("signal"),
                timed_out=True, retry_delay=self.retry_delay,
            )
        elif result.get("exit_code") == 0:
            self.store.finish_attempt(attempt, A_SUCCEEDED, "exit 0", exit_code=0)
        else:
            code = result.get("exit_code")
            sig = result.get("signal")
            reason = f"exit code {code}" if code is not None else f"signal {sig}"
            self.store.finish_attempt(
                attempt, A_FAILED_EXIT, reason,
                exit_code=code, signal=sig, retry_delay=self.retry_delay,
            )

    # ---- launching ----------------------------------------------------------
    def _launch(self, job: dict) -> bool:
        seq = self.store.attempts_in_generation(job["job_id"], job["generation"]) + 1
        paths = _attempt_paths(
            self.run_dir, job["date"], job["node"], job["generation"], seq
        )
        Path(paths["stdout"]).parent.mkdir(parents=True, exist_ok=True)
        attempt_id = self.store.insert_attempt(job, seq, paths)
        if attempt_id is None:
            # The job changed under us (e.g. an external rerun reset it);
            # next loop iteration re-reads the current state.
            return False
        args = json.loads(job["args_json"])
        wrapper_argv = [
            sys.executable, "-m", "minenergy.orchestrator.wrapper",
            "--result", paths["result"], "--meta", paths["meta"],
            "--timeout", str(job["timeout_seconds"]),
            "--grace", str(self.term_grace),
            "--cwd", job["cwd"],
            "--stdout", paths["stdout"], "--stderr", paths["stderr"],
            "--",
        ] + args
        wrapper_log = open(paths["wrapper_log"], "ab")
        proc_handle = subprocess.Popen(
            wrapper_argv,
            stdin=subprocess.DEVNULL,
            stdout=wrapper_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # the wrapper leads its own group
            env=_child_env(),
            close_fds=True,
        )
        wrapper_log.close()
        pid_start = proc.read_starttime(proc_handle.pid) or ""
        self.store.attach_wrapper_identity(attempt_id, proc_handle.pid, pid_start)
        self._local[attempt_id] = proc_handle
        self._log(
            f"launch {job['date']}/{job['node']} attempt#{seq}"
            f" (wrapper pid={proc_handle.pid})"
        )
        return True

    # ---- reaping ------------------------------------------------------------
    def _reap(self) -> None:
        for attempt_id, handle in list(self._local.items()):
            if handle.poll() is None:
                continue
            del self._local[attempt_id]
            self._finish_reaped(attempt_id)
        for attempt_id, (pid, pid_start) in list(self._adopted.items()):
            if proc.pid_alive(pid, pid_start):
                continue
            del self._adopted[attempt_id]
            self._finish_reaped(attempt_id)

    def _finish_reaped(self, attempt_id: int) -> None:
        attempt = next(
            (a for a in self.store.active_attempts() if a["attempt_id"] == attempt_id),
            None,
        )
        if attempt is None:
            # Already settled (e.g. recorded during recovery); nothing to do.
            return
        label = f"{attempt['date']}/{attempt['node']} attempt#{attempt['seq']}"
        result = self._read_result(attempt)
        if result is None:
            meta = self._read_meta(attempt, retries=0)
            if meta:
                self.store.record_worker_identity(
                    attempt_id,
                    meta.get("worker_pid"),
                    meta.get("worker_pgid"),
                    meta.get("worker_pid_start") or "",
                )
            self.store.finish_attempt(
                attempt, A_INTERRUPTED,
                "wrapper exited without recording an outcome",
                retry_delay=self.retry_delay,
            )
            self._log(f"{label}: wrapper lost the outcome -> interrupted")
            return
        self._record_outcome(attempt, result)
        # Re-read the job state for the trace line.
        job = self.store.get_job(attempt["date"], attempt["node"])
        outcome = (
            "succeeded" if result.get("exit_code") == 0 and not result.get("timed_out")
            else "timed out" if result.get("timed_out")
            else f"failed ({result.get('exit_code') if result.get('exit_code') is not None else 'signal ' + str(result.get('signal'))})"
        )
        self._log(f"{label}: {outcome}; job now {job['status']}")

    # ---- cancellation -------------------------------------------------------
    def _terminate_active_attempts(self, reason: str) -> None:
        """TERM then KILL every in-flight attempt's process group."""
        active = self.store.active_attempts()
        for attempt in active:
            meta = self._read_meta(attempt, retries=10)
            if meta:
                self.store.record_worker_identity(
                    attempt["attempt_id"],
                    meta.get("worker_pid"),
                    meta.get("worker_pgid"),
                    meta.get("worker_pid_start") or "",
                )
        for attempt in self.store.active_attempts():
            extra = []
            if attempt["pid"]:
                extra.append((attempt["pid"], attempt["pid_start"]))
            proc.terminate_group(
                attempt["worker_pgid"],
                self.term_grace,
                extra_pids=extra,
                leader=(attempt["worker_pid"], attempt["worker_pid_start"]),
            )
            # Reap wrapper zombies we parent, so they release their PIDs now.
            handle = self._local.get(attempt["attempt_id"])
            if handle is not None:
                handle.poll()
            self.store.finish_attempt(attempt, A_CANCELLED, reason)
            self._log(
                f"cancelled {attempt['date']}/{attempt['node']}"
                f" attempt#{attempt['seq']} (process group terminated)"
            )
        self._local.clear()
        self._adopted.clear()

    def _handle_cancel(self) -> None:
        self._log("cancel requested: stopping new launches, terminating in-flight attempts")
        self._terminate_active_attempts("run cancelled")
        self.store.cancel_quiescent_jobs()
        self.store.finalize_run(RUN_CANCELLED)

    # ---- main loop ----------------------------------------------------------
    def execute(self) -> int:
        """Run the scheduling loop; returns one of the EXIT_* codes."""
        self.acquire()
        try:
            self.recover()
            while True:
                if self.store.cancel_requested():
                    self._handle_cancel()
                    return EXIT_CANCELLED
                self._reap()
                self.store.promote_and_block()
                self.store.release_due_retries()
                running = len(self._local) + len(self._adopted)
                for job in self.store.ready_jobs():
                    if running >= self.max_parallel:
                        break
                    if self._launch(job):
                        running += 1
                if running == 0 and not self.store.has_pending_work():
                    break
                time.sleep(self.poll_interval)
            return self._finalize()
        finally:
            self.release()
            self.store.close()

    def _finalize(self) -> int:
        report = self.store.status_report()
        counts = report["counts"]
        if counts.get(INTERRUPTED):
            # The run stays active: an operator must confirm the interrupted
            # attempts are dead and rerun them.
            self._log(
                f"run paused: {counts[INTERRUPTED]} job(s) interrupted; "
                "confirm the old attempts are dead and rerun them"
            )
            return EXIT_INTERRUPTED
        if counts.get(FAILED) or counts.get(BLOCKED) or counts.get(CANCELLED):
            self.store.finalize_run(RUN_FAILED)
            self._log(
                f"run finished with failures: "
                + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            )
            return EXIT_FAILED
        unfinished = sum(
            counts.get(state, 0)
            for state in ("pending", "ready", "running", "retry_wait")
        )
        if unfinished:
            # Defensive: the loop only exits with nothing schedulable, so this
            # should be unreachable — never claim completion with work left.
            self._log(f"run paused: {unfinished} job(s) still unfinished")
            return EXIT_INTERRUPTED
        self.store.finalize_run(RUN_COMPLETED)
        self._log(f"run completed: {counts.get(SUCCEEDED, 0)} job(s) succeeded")
        return EXIT_OK


# ---------------------------------------------------------------------------
# Module-level operations (also the Python API surface)
# ---------------------------------------------------------------------------


def create_run(
    definition: Definition,
    dates: list[str],
    run_dir: str | Path,
    max_parallel: int | None = None,
) -> str:
    """Create a run bound to the given definition snapshot; returns the run id.

    The run directory must not already hold a run.  The complete validated
    definition is stored with the run: later edits to the definition file
    cannot change what this run executes.
    """
    run_dir = Path(run_dir)
    if db_path(run_dir).exists():
        raise RunStateError(f"{run_dir} already holds an orchestration run")
    run_dir.mkdir(parents=True, exist_ok=True)
    dates = validate_dates(dates)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    with RunStore(run_dir, create=True) as store:
        store.create_run(run_id, definition, dates, max_parallel=max_parallel)
    (run_dir / SNAPSHOT_NAME).write_text(
        json.dumps(json.loads(definition.canonical_json), indent=2, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )
    return run_id


def execute_run(run_dir: str | Path, poll_interval: float = 0.05) -> int:
    """Acquire execution rights and drive the run to a terminal state."""
    controller = Controller(run_dir, poll_interval=poll_interval)
    return controller.execute()


def start_run(
    definition_path: str | Path,
    dates: list[str],
    run_dir: str | Path,
    max_parallel: int | None = None,
    poll_interval: float = 0.05,
) -> int:
    """Create a run from a definition file and immediately execute it."""
    definition = load_definition(definition_path)
    for line in plan_lines(definition, validate_dates(list(dates)), run_dir):
        print(line, flush=True)
    run_id = create_run(definition, list(dates), run_dir, max_parallel=max_parallel)
    print(f"run_id={run_id}", flush=True)
    return execute_run(run_dir, poll_interval=poll_interval)


def cancel_run(run_dir: str | Path, wait_seconds: float = 30.0) -> str:
    """Cancel the run; idempotent.  Returns a short outcome string.

    The flag is set first (a live controller stops launching new instances at
    once).  If this caller can take execution rights (no live controller), it
    performs the termination itself; otherwise it waits for the live
    controller to finish the cancellation.
    """
    run_dir = Path(run_dir)
    with RunStore(run_dir) as store:
        run = store.get_run()
        if run["status"] in (RUN_COMPLETED, RUN_FAILED):
            return f"run already {run['status']}; nothing to cancel"
        if run["status"] == RUN_CANCELLED:
            return "run already cancelled"
        store.request_cancel()

    controller = Controller(run_dir)
    try:
        controller.acquire()
    except ControllerConflictError:
        # A live controller will see the flag and do the termination.
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            time.sleep(0.1)
            with RunStore(run_dir) as store:
                if store.get_run()["status"] == RUN_CANCELLED:
                    return "cancelled (by the running controller)"
        return "cancel requested; the running controller has not finished yet"
    try:
        # Re-check under the lock: the run may have reached a terminal state
        # between the flag update and the lock acquisition.
        status = controller.store.get_run()["status"]
        if status == RUN_CANCELLED:
            return "run already cancelled"
        if status in (RUN_COMPLETED, RUN_FAILED):
            return f"run already {status}; nothing to cancel"
        controller.recover()  # settle leftover attempts so none are missed
        controller._handle_cancel()
        return "cancelled"
    finally:
        controller.release()
        controller.store.close()


def rerun_job(run_dir: str | Path, date: str, node: str) -> dict:
    """Invalidate one (date, node) job and its same-date descendants.

    Refused while any attempt in the scope is in flight, or while a previously
    interrupted attempt's process group is still detectably alive.  All
    refusal checks happen before any state change.
    """
    run_dir = Path(run_dir)
    with RunStore(run_dir) as store:
        run = store.get_run()
        if run["status"] == RUN_CANCELLED:
            raise RunStateError("run is cancelled; rerun is not allowed")
        return store.rerun_scope(date, node)


def run_status(run_dir: str | Path) -> dict:
    with RunStore(run_dir) as store:
        return store.status_report()
