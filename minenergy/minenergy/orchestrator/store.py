"""SQLite record layer for the orchestrator.

One run directory holds exactly one run::

    <run_dir>/orchestrator.sqlite   this record
    <run_dir>/controller.lock       execution-rights lock (flock, see controller)
    <run_dir>/definition.snapshot.json
    <run_dir>/logs/<date>--<node>/attempt-g<generation>-<seq>.*   per-try logs

Tracked entities:

* the **run** — bound to the complete definition snapshot taken at creation
  (later edits to the definition file cannot change an established run);
* **jobs** — logical ``(date, node)`` instances with their resolved argv;
* **attempts** — every real process try of a job, with start/end, exit code
  or signal, per-attempt stdout/stderr files and a failure reason.  Attempt
  rows and their log files are append-only: history is never overwritten.

A job's success comes only from a recorded attempt whose real exit code was
0 — never from an output file existing or a "done" line in a log.

Job states: ``pending`` (waiting for predecessors), ``ready`` (runnable),
``running`` (an attempt is in flight), ``retry_wait`` (a failed attempt left
retry budget; waiting out the retry delay), ``succeeded``, ``failed``
(attempt budget exhausted), ``blocked`` (a predecessor failed/was cancelled;
``blocked_by`` names the root job), ``cancelled``, ``interrupted`` (the last
attempt's outcome is unknowable; the fact is preserved and the job waits for
an explicit rerun once the old attempt is confirmed dead).

Run states: ``active`` → ``completed`` | ``failed`` | ``cancelled``.
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from .definition import Definition, expand
from .errors import RunNotFoundError, RunStateError

SCHEMA = """
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  definition_sha256 TEXT NOT NULL,
  status TEXT NOT NULL,                -- active / completed / failed / cancelled
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  max_parallel INTEGER NOT NULL,
  retry_delay_seconds REAL NOT NULL,
  term_grace_seconds REAL NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE TABLE jobs (
  job_id INTEGER PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  date TEXT NOT NULL,
  node TEXT NOT NULL,
  status TEXT NOT NULL,
  args_json TEXT NOT NULL,
  cwd TEXT NOT NULL,
  timeout_seconds REAL NOT NULL,
  max_attempts INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0,
  next_retry_at REAL,
  blocked_by TEXT,
  updated_at TEXT NOT NULL,
  UNIQUE(run_id, date, node)
);
CREATE TABLE edges (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  date TEXT NOT NULL,
  src TEXT NOT NULL,
  dst TEXT NOT NULL,
  UNIQUE(run_id, date, src, dst)
);
CREATE TABLE attempts (
  attempt_id INTEGER PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(job_id),
  generation INTEGER NOT NULL,
  seq INTEGER NOT NULL,                -- 1-based within (job, generation)
  status TEXT NOT NULL,                -- launching / running / succeeded /
                                       -- failed_exit / failed_timeout /
                                       -- cancelled / interrupted
  pid INTEGER,                         -- wrapper process pid
  pid_start TEXT,                      -- wrapper /proc starttime token
  worker_pid INTEGER,
  worker_pgid INTEGER,
  worker_pid_start TEXT,
  started_at TEXT,
  ended_at TEXT,
  exit_code INTEGER,
  signal INTEGER,
  timed_out INTEGER NOT NULL DEFAULT 0,
  stdout_path TEXT,
  stderr_path TEXT,
  result_path TEXT,
  meta_path TEXT,
  reason TEXT,
  UNIQUE(job_id, generation, seq)
);
CREATE INDEX idx_attempts_job ON attempts(job_id);
CREATE INDEX idx_jobs_run ON jobs(run_id);
"""

# job states
PENDING = "pending"
READY = "ready"
RUNNING = "running"
RETRY_WAIT = "retry_wait"
SUCCEEDED = "succeeded"
FAILED = "failed"
BLOCKED = "blocked"
CANCELLED = "cancelled"
INTERRUPTED = "interrupted"
JOB_STATES = {
    PENDING, READY, RUNNING, RETRY_WAIT, SUCCEEDED, FAILED, BLOCKED, CANCELLED,
    INTERRUPTED,
}

# attempt states
A_LAUNCHING = "launching"
A_RUNNING = "running"
A_SUCCEEDED = "succeeded"
A_FAILED_EXIT = "failed_exit"
A_FAILED_TIMEOUT = "failed_timeout"
A_CANCELLED = "cancelled"
A_INTERRUPTED = "interrupted"
ACTIVE_ATTEMPT_STATES = (A_LAUNCHING, A_RUNNING)

# run states
RUN_ACTIVE = "active"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"

DB_NAME = "orchestrator.sqlite"
LOCK_NAME = "controller.lock"
SNAPSHOT_NAME = "definition.snapshot.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def db_path(run_dir: str | Path) -> Path:
    return Path(run_dir) / DB_NAME


class RunStore:
    """All reads and writes of one run's orchestration record.

    Every mutation runs inside a ``BEGIN IMMEDIATE`` transaction, so the
    record never shows a half-applied state change to concurrent readers
    (the database is in WAL mode).
    """

    def __init__(self, run_dir: str | Path, create: bool = False):
        self.run_dir = Path(run_dir)
        path = db_path(run_dir)
        if not create and not path.exists():
            raise RunNotFoundError(f"no orchestration record at {self.run_dir}")
        self._conn = sqlite3.connect(str(path), timeout=10.0, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        if create:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "RunStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- transactions -----------------------------------------------------
    def _tx(self):
        """Context manager: BEGIN IMMEDIATE … COMMIT / ROLLBACK."""
        conn = self._conn

        class _Tx:
            def __enter__(self):
                conn.execute("BEGIN IMMEDIATE")
                return conn

            def __exit__(self, exc_type, exc, tb):
                if exc_type is None:
                    conn.execute("COMMIT")
                else:
                    conn.execute("ROLLBACK")
                return False

        return _Tx()

    # ---- run creation -----------------------------------------------------
    def create_run(
        self,
        run_id: str,
        definition: Definition,
        dates: list[str],
        max_parallel: int | None = None,
    ) -> None:
        """Insert the run, every job instance and every edge in one commit."""
        instances = expand(definition, dates, self.run_dir)
        now = utc_now()
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO runs(run_id, name, definition_json, definition_sha256,"
                " status, max_parallel, retry_delay_seconds, term_grace_seconds,"
                " created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    definition.name,
                    definition.canonical_json,
                    definition.sha256,
                    RUN_ACTIVE,
                    max_parallel or definition.limits.max_parallel,
                    definition.limits.retry_delay_seconds,
                    definition.limits.term_grace_seconds,
                    now,
                ),
            )
            for inst in instances:
                status = READY if not inst.depends_on else PENDING
                conn.execute(
                    "INSERT INTO jobs(run_id, date, node, status, args_json, cwd,"
                    " timeout_seconds, max_attempts, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        run_id,
                        inst.date,
                        inst.node,
                        status,
                        json.dumps(list(inst.args)),
                        inst.cwd,
                        inst.timeout_seconds,
                        inst.max_attempts,
                        now,
                    ),
                )
                for dep in inst.depends_on:
                    conn.execute(
                        "INSERT INTO edges(run_id, date, src, dst) VALUES (?,?,?,?)",
                        (run_id, inst.date, dep, inst.node),
                    )

    # ---- reads ------------------------------------------------------------
    def get_run(self) -> dict:
        row = self._conn.execute("SELECT * FROM runs").fetchone()
        if row is None:
            raise RunNotFoundError(f"empty orchestration record at {self.run_dir}")
        return dict(row)

    def list_jobs(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM jobs ORDER BY date, job_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def list_edges(self) -> list[dict]:
        rows = self._conn.execute("SELECT * FROM edges ORDER BY date, src, dst").fetchall()
        return [dict(r) for r in rows]

    def get_job(self, date: str, node: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE date=? AND node=?", (date, node)
        ).fetchone()
        return dict(row) if row else None

    def attempts_for_job(self, job_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM attempts WHERE job_id=? ORDER BY generation, seq",
            (job_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def active_attempts(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT a.*, j.date, j.node FROM attempts a JOIN jobs j ON j.job_id=a.job_id"
            " WHERE a.status IN ('launching','running') ORDER BY a.attempt_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def attempts_in_generation(self, job_id: int, generation: int) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE job_id=? AND generation=?",
            (job_id, generation),
        ).fetchone()
        return int(row["n"])

    def cancel_requested(self) -> bool:
        return bool(self.get_run()["cancel_requested"])

    # ---- scheduling mutations --------------------------------------------
    def insert_attempt(self, job: dict, seq: int, paths: dict[str, str]) -> int | None:
        """Record a new attempt in ``launching`` state and mark the job running.

        The row exists *before* the process is spawned, so a crash between
        spawn and identity registration leaves a discoverable record.  The
        insert is serialised against a concurrent ``rerun_scope`` (both hold
        ``BEGIN IMMEDIATE`` transactions): if the job's generation or status
        changed since the caller read it, the insert is refused and the
        caller must re-read instead of launching against a stale generation.
        """
        now = utc_now()
        with self._tx() as conn:
            current = conn.execute(
                "SELECT status, generation FROM jobs WHERE job_id=?",
                (job["job_id"],),
            ).fetchone()
            if (
                current is None
                or current["status"] != READY
                or current["generation"] != job["generation"]
            ):
                return None  # __exit__ commits nothing new; state untouched
            cur = conn.execute(
                "INSERT INTO attempts(job_id, generation, seq, status, started_at,"
                " stdout_path, stderr_path, result_path, meta_path)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    job["job_id"],
                    job["generation"],
                    seq,
                    A_LAUNCHING,
                    now,
                    paths["stdout"],
                    paths["stderr"],
                    paths["result"],
                    paths["meta"],
                ),
            )
            conn.execute(
                "UPDATE jobs SET status=?, next_retry_at=NULL, updated_at=?"
                " WHERE job_id=?",
                (RUNNING, now, job["job_id"]),
            )
            return int(cur.lastrowid)

    def attach_wrapper_identity(self, attempt_id: int, pid: int, pid_start: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE attempts SET status=?, pid=?, pid_start=? WHERE attempt_id=?",
                (A_RUNNING, pid, pid_start, attempt_id),
            )

    def record_worker_identity(
        self, attempt_id: int, worker_pid: int, worker_pgid: int, worker_start: str
    ) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE attempts SET worker_pid=?, worker_pgid=?, worker_pid_start=?"
                " WHERE attempt_id=?",
                (worker_pid, worker_pgid, worker_start, attempt_id),
            )

    def finish_attempt(
        self,
        attempt: dict,
        status: str,
        reason: str,
        exit_code: int | None = None,
        signal: int | None = None,
        timed_out: bool = False,
        retry_delay: float = 0.0,
    ) -> None:
        """Record an attempt's real outcome and advance the job state.

        * exit code 0 (and only that) moves the job to ``succeeded``;
        * a non-zero exit / timeout consumes budget: ``retry_wait`` while the
          generation still has attempts left, ``failed`` once it is exhausted;
        * ``cancelled`` / ``interrupted`` move the job to the matching state.
        """
        now = utc_now()
        with self._tx() as conn:
            job = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (attempt["job_id"],)
            ).fetchone()
            if job is None:
                raise RunStateError(f"attempt {attempt['attempt_id']} has no job")
            used = conn.execute(
                "SELECT COUNT(*) AS n FROM attempts WHERE job_id=? AND generation=?",
                (job["job_id"], job["generation"]),
            ).fetchone()["n"]
            conn.execute(
                "UPDATE attempts SET status=?, ended_at=?, exit_code=?, signal=?,"
                " timed_out=?, reason=? WHERE attempt_id=?",
                (status, now, exit_code, signal, 1 if timed_out else 0, reason,
                 attempt["attempt_id"]),
            )
            if status == A_SUCCEEDED:
                conn.execute(
                    "UPDATE jobs SET status=?, blocked_by=NULL, updated_at=? WHERE job_id=?",
                    (SUCCEEDED, now, job["job_id"]),
                )
            elif status in (A_FAILED_EXIT, A_FAILED_TIMEOUT):
                if used < job["max_attempts"]:
                    conn.execute(
                        "UPDATE jobs SET status=?, next_retry_at=?, updated_at=?"
                        " WHERE job_id=?",
                        (RETRY_WAIT, time.time() + retry_delay, now, job["job_id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                        (FAILED, now, job["job_id"]),
                    )
            elif status == A_CANCELLED:
                conn.execute(
                    "UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                    (CANCELLED, now, job["job_id"]),
                )
            elif status == A_INTERRUPTED:
                conn.execute(
                    "UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                    (INTERRUPTED, now, job["job_id"]),
                )

    def promote_and_block(self) -> None:
        """Advance pending jobs: predecessors all succeeded -> ready; any
        predecessor failed/blocked/cancelled -> blocked (root cause named)."""
        jobs = self.list_jobs()
        edges = self.list_edges()
        by_key = {(j["date"], j["node"]): j for j in jobs}
        preds: dict[tuple[str, str], list[str]] = {}
        for e in edges:
            preds.setdefault((e["date"], e["dst"]), []).append(e["src"])

        def root_blocker(date: str, node: str, seen: set[str]) -> str | None:
            key = (date, node)
            job = by_key.get(key)
            if job is None or node in seen:
                return None
            seen.add(node)
            if job["status"] in (FAILED, CANCELLED, INTERRUPTED):
                return node
            if job["status"] == BLOCKED:
                return job["blocked_by"]
            for p in preds.get(key, []):
                found = root_blocker(date, p, seen)
                if found:
                    return found
            return None

        now = utc_now()
        with self._tx() as conn:
            for job in jobs:
                if job["status"] != PENDING:
                    continue
                pred_jobs = [by_key[(job["date"], p)] for p in preds.get((job["date"], job["node"]), [])]
                if all(p["status"] == SUCCEEDED for p in pred_jobs):
                    conn.execute(
                        "UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                        (READY, now, job["job_id"]),
                    )
                    continue
                blocker = None
                for p in pred_jobs:
                    blocker = root_blocker(job["date"], p["node"], set())
                    if blocker:
                        break
                if blocker:
                    conn.execute(
                        "UPDATE jobs SET status=?, blocked_by=?, updated_at=? WHERE job_id=?",
                        (BLOCKED, blocker, now, job["job_id"]),
                    )

    def release_due_retries(self) -> None:
        now = utc_now()
        with self._tx() as conn:
            conn.execute(
                "UPDATE jobs SET status=?, next_retry_at=NULL, updated_at=?"
                " WHERE status=? AND next_retry_at IS NOT NULL AND next_retry_at<=?",
                (READY, now, RETRY_WAIT, time.time()),
            )

    def ready_jobs(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status=? ORDER BY date, job_id", (READY,)
        ).fetchall()
        return [dict(r) for r in rows]

    def has_pending_work(self) -> bool:
        """True while any job could still change state on its own."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE status IN (?,?,?)",
            (READY, RUNNING, RETRY_WAIT),
        ).fetchone()
        return int(row["n"]) > 0

    # ---- cancellation -----------------------------------------------------
    def request_cancel(self) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE runs SET cancel_requested=1")

    def cancel_quiescent_jobs(self) -> None:
        """Cancel every job that has no in-flight attempt."""
        now = utc_now()
        with self._tx() as conn:
            conn.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE status IN (?,?,?,?)",
                (CANCELLED, now, PENDING, READY, RETRY_WAIT, INTERRUPTED),
            )

    def finalize_run(self, status: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE runs SET status=?, finished_at=? WHERE status=?",
                (status, utc_now(), RUN_ACTIVE),
            )

    # ---- rerun ------------------------------------------------------------
    def rerun_scope(self, date: str, node: str) -> dict:
        """Invalidate one ``(date, node)`` job and its same-date descendants.

        Every job in the scope gets a new generation (its historical attempts
        stay on record and its log files are never reused), its success
        entitlement is revoked and its state recomputed from the predecessors
        that survive outside the scope.  Returns the affected jobs.
        """
        run = self.get_run()
        if run["status"] == RUN_CANCELLED:
            raise RunStateError("run is cancelled; rerun is not allowed")
        target = self.get_job(date, node)
        if target is None:
            raise RunStateError(f"no job ({date}, {node}) in this run")

        edges = [e for e in self.list_edges() if e["date"] == date]
        scope = {node}
        changed = True
        while changed:
            changed = False
            for e in edges:
                if e["src"] in scope and e["dst"] not in scope:
                    scope.add(e["dst"])
                    changed = True

        jobs = {j["node"]: j for j in self.list_jobs() if j["date"] == date}
        # Refuse while any attempt in the scope is still in flight, or while a
        # previously interrupted attempt's process group is still alive (the
        # old try must be confirmed dead before its work may be re-done).
        from . import process as _proc

        for name in sorted(scope):
            job = jobs[name]
            for att in self.attempts_for_job(job["job_id"]):
                if att["status"] in ACTIVE_ATTEMPT_STATES:
                    raise RunStateError(
                        f"job ({date}, {name}) has an active attempt "
                        f"(#{att['attempt_id']}); rerun refused"
                    )
                if att["status"] == A_INTERRUPTED and _proc.process_group_alive(
                    att["worker_pgid"], att["worker_pid"], att["worker_pid_start"]
                ):
                    raise RunStateError(
                        f"interrupted attempt #{att['attempt_id']} of "
                        f"({date}, {name}) still has live processes; rerun refused"
                    )

        now = utc_now()
        with self._tx() as conn:
            for name in sorted(scope):
                job = jobs[name]
                preds = [e["src"] for e in edges if e["dst"] == name]
                pred_states = [
                    jobs[p]["status"] for p in preds if p not in scope
                ]
                # Predecessors inside the scope are being reset too, so the
                # job must wait for them; predecessors outside keep their
                # (reusable) success.
                waiting = any(p in scope for p in preds) or any(
                    s != SUCCEEDED for s in pred_states
                )
                conn.execute(
                    "UPDATE jobs SET status=?, generation=generation+1,"
                    " next_retry_at=NULL, blocked_by=NULL, updated_at=?"
                    " WHERE job_id=?",
                    (PENDING if waiting else READY, now, job["job_id"]),
                )
            if run["status"] in (RUN_COMPLETED, RUN_FAILED):
                conn.execute(
                    "UPDATE runs SET status=?, finished_at=NULL WHERE run_id=?",
                    (RUN_ACTIVE, run["run_id"]),
                )
        return {
            "date": date,
            "reset": sorted(scope),
            "reused": sorted(
                n for n, j in jobs.items() if n not in scope and j["status"] == SUCCEEDED
            ),
        }

    # ---- reporting --------------------------------------------------------
    def status_report(self) -> dict:
        run = self.get_run()
        jobs = self.list_jobs()
        edges = self.list_edges()
        report_jobs = []
        counts: dict[str, int] = {}
        for job in jobs:
            counts[job["status"]] = counts.get(job["status"], 0) + 1
            attempts = self.attempts_for_job(job["job_id"])
            report_jobs.append(
                {
                    "date": job["date"],
                    "node": job["node"],
                    "status": job["status"],
                    "generation": job["generation"],
                    "max_attempts": job["max_attempts"],
                    "blocked_by": job["blocked_by"],
                    "attempts": [
                        {
                            "attempt_id": a["attempt_id"],
                            "generation": a["generation"],
                            "seq": a["seq"],
                            "status": a["status"],
                            "exit_code": a["exit_code"],
                            "signal": a["signal"],
                            "timed_out": bool(a["timed_out"]),
                            "started_at": a["started_at"],
                            "ended_at": a["ended_at"],
                            "reason": a["reason"],
                            "stdout": a["stdout_path"],
                            "stderr": a["stderr_path"],
                        }
                        for a in attempts
                    ],
                }
            )
        return {
            "run_id": run["run_id"],
            "name": run["name"],
            "status": run["status"],
            "cancel_requested": bool(run["cancel_requested"]),
            "definition_sha256": run["definition_sha256"],
            "max_parallel": run["max_parallel"],
            "created_at": run["created_at"],
            "finished_at": run["finished_at"],
            "counts": counts,
            "jobs": report_jobs,
            "edges": [
                {"date": e["date"], "src": e["src"], "dst": e["dst"]} for e in edges
            ],
        }
