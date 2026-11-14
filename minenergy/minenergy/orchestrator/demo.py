"""Night-shift drill: rebuild the whole orchestration story from an empty dir.

Driven by ``nightshift_demo.sh`` or directly::

    python -m minenergy.orchestrator.demo --workdir /tmp/minenergy-nightshift

Phases (every assertion is printed and re-checkable from the artefacts left in
the workdir — SQLite records, per-attempt logs, trace files, delivery
manifests):

1. real data chain   generate -> energy -> verify -> deliver per date, plus a
                     COMTRADE waveform-conversion branch that fails for one
                     date: the verified dates are never redone, the failure
                     stays isolated to its branch;
2. rerun scoping     rerunning one date's ``check`` re-executes only it and
                     its successors;
3. controlled DAG    fork/join over two interleaved dates, flaky retry,
                     permanent failure isolation, exit-code-only success;
4. timeout           a timed-out spawner leaves no grandchild processing;
5. cancel            cancelling a run kills whole process groups, idempotent;
6. crash takeover    a killed controller's live worker is adopted, never
                     restarted; a lost wrapper leaves an *interrupted* attempt
                     that blocks rerun until the old process is confirmed dead;
7. concurrency       a second controller is refused execution rights;
8. definition safety bad definitions are rejected before anything starts and
                     an established run is bound to its definition snapshot.

Recovery in this drill never deletes or hand-edits the SQLite record: every
takeover goes through ``resume``.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PY = sys.executable
ENV = {
    **os.environ,
    "PYTHONPATH": str(REPO) + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""),
}
D1, D2 = "2025-01-01", "2025-01-02"

FAILURES: list[str] = []


def say(msg: str) -> None:
    print(msg, flush=True)


def check(condition: bool, label: str) -> None:
    say(f"  {'ok  ' if condition else 'FAIL'} {label}")
    if not condition:
        FAILURES.append(label)


def cli(args: list[str], cwd: Path | None = None, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(
        [PY, "-m", "minenergy.orchestrator.cli", *args],
        cwd=cwd, env=ENV, capture_output=True, text=True, **kw,
    )


def module(module: str, args: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(
        [PY, "-m", module, *args], env=ENV, capture_output=True, text=True, **kw
    )


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def db(run_dir: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(run_dir / "orchestrator.sqlite")
    conn.row_factory = sqlite3.Row
    return conn


def job_row(run_dir: Path, date: str, node: str) -> dict:
    with db(run_dir) as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE date=? AND node=?", (date, node)
        ).fetchone()
    return dict(row)


def attempt_rows(run_dir: Path, date: str, node: str) -> list[dict]:
    with db(run_dir) as conn:
        rows = conn.execute(
            "SELECT a.* FROM attempts a JOIN jobs j ON j.job_id=a.job_id"
            " WHERE j.date=? AND j.node=? ORDER BY a.generation, a.seq",
            (date, node),
        ).fetchall()
    return [dict(r) for r in rows]


def run_row(run_dir: Path) -> dict:
    with db(run_dir) as conn:
        return dict(conn.execute("SELECT * FROM runs").fetchone())


def leftover_processes(fragment: str) -> list[str]:
    """Cmdlines under /proc mentioning ``fragment`` (this demo excluded)."""
    me = os.getpid()
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == me:
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if fragment in cmdline:
            found.append(f"{entry.name}:{cmdline.strip()[:100]}")
    return found


def wait_no_processes(fragment: str, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not leftover_processes(fragment):
            return True
        time.sleep(0.1)
    return False


def demo_jobs(*args: str) -> list[str]:
    return [PY, "-m", "minenergy.orchestrator.demo_jobs", *args]


# ---------------------------------------------------------------------------
# phase 1+2: the real data chain
# ---------------------------------------------------------------------------


def real_chain_definition(work: Path) -> dict:
    """generate -> energy -> check -> deliver, plus a waveform branch."""
    def node(name, args, deps=(), **kw):
        return {"name": name, "args": [str(a) for a in args], "depends_on": deps, **kw}

    return {
        "name": "night-shift",
        "limits": {"max_parallel": 3, "retry_delay_seconds": 0.2},
        "nodes": [
            node("samples", [
                PY, "-m", "minenergy.generate", "--out", "{run_dir}/input/{date}",
                "--scenario", "randomized", "--seed", "11", "--batches", "1",
                "--meters", "2", "--points", "10",
            ], timeout_seconds=120),
            node("energy", [
                PY, "-m", "minenergy.cli", "full",
                "--store", "{run_dir}/store/{date}",
                "--settings", "{run_dir}/input/{date}/settings.json",
                "--samples", "{run_dir}/input/{date}/batches/batch_00.parquet",
                "--configs", "{run_dir}/input/{date}/configs_all.parquet",
            ], ["samples"], timeout_seconds=300),
            node("check", [
                PY, "-m", "minenergy.cli", "verify",
                "--store", "{run_dir}/store/{date}",
                "--settings", "{run_dir}/input/{date}/settings.json",
            ], ["energy"], timeout_seconds=300),
            node("deliver", [
                PY, "-m", "minenergy.orchestrator.demo_jobs", "collect",
                "--store", "{run_dir}/store/{date}",
                "--out", "{run_dir}/delivery/{date}.json",
            ], ["check"], timeout_seconds=120),
            # Waveform conversion branch: date 2's recording is corrupt, so
            # this fails there no matter how often it is retried.
            node("waveform", [
                PY, "-m", "minenergy.comtrade.cli", "export",
                "--cfg", work / "comtrade-input" / "{date}" / "rec.cfg",
                "--timezone", "UTC",
                "--out", "{run_dir}/waveform/{date}",
                "--start-us", "-1000000", "--end-us", "1000000",
            ], timeout_seconds=120, max_attempts=2),
            node("waveform_pack", [
                *demo_jobs("ok", "--dir", "{run_dir}/waveform-packed/{date}",
                           "--name", "pack"),
            ], ["waveform"]),
        ],
    }


def phase_real_chain(work: Path) -> Path:
    say("== phase 1: real data chain (generate -> energy -> verify -> deliver)")
    fixtures = work / "comtrade-fixtures"
    made = module("minenergy.comtrade.cli", ["make-fixtures", "--out", str(fixtures)])
    check(made.returncode == 0, "comtrade fixtures generated")
    for date, src in ((D1, "reference/rates_ascii"), (D2, "corrupt/truncated_binary")):
        dst = work / "comtrade-input" / date
        dst.mkdir(parents=True, exist_ok=True)
        for ext in ("cfg", "dat"):
            shutil.copy(fixtures / src / f"rec.{ext}", dst / f"rec.{ext}")

    definition = work / "nightshift.json"
    write_json(definition, real_chain_definition(work))
    run_dir = work / "run-real"

    plan = cli(["plan", "--definition", str(definition), "--dates", D1, D2,
                "--run-dir", str(run_dir)])
    check(plan.returncode == 0, "plan lists instances and dependencies")
    check(plan.stdout.count("argv:") == 12, "plan covers 6 nodes x 2 dates")

    started = time.monotonic()
    proc = cli(["start", "--definition", str(definition), "--dates", D1, D2,
                "--run-dir", str(run_dir)], timeout=600)
    say(f"  (run took {time.monotonic() - started:.1f}s, exit={proc.returncode})")
    check(proc.returncode == 1, "run exits 1: waveform branch failed, chain isolated")

    # Main chain succeeded for BOTH dates; waveform failed only for date 2.
    for date in (D1, D2):
        for node in ("samples", "energy", "check", "deliver"):
            row = job_row(run_dir, date, node)
            check(row["status"] == "succeeded", f"{date}/{node} succeeded")
    w1, w2 = job_row(run_dir, D1, "waveform"), job_row(run_dir, D2, "waveform")
    check(w1["status"] == "succeeded", f"{D1}/waveform converted")
    check(w2["status"] == "failed", f"{D2}/waveform failed on the corrupt recording")
    atts2 = attempt_rows(run_dir, D2, "waveform")
    check(len(atts2) == 2 and all(a["status"] == "failed_exit" and a["exit_code"] == 2
                                  for a in atts2),
          f"{D2}/waveform kept exactly 2 real failed attempts (exit 2)")
    pack2 = job_row(run_dir, D2, "waveform_pack")
    check(pack2["status"] == "blocked" and pack2["blocked_by"] == "waveform",
          f"{D2}/waveform_pack blocked by waveform")
    check(run_row(run_dir)["status"] == "failed", "run state = failed (failure isolated)")

    # The verified dates were NOT redone because of the waveform failure.
    for date in (D1, D2):
        check(len(attempt_rows(run_dir, date, "check")) == 1,
              f"{date}/check ran exactly once despite the branch failure")

    # Final check of the on-disk minute facts (delivery manifests + parquet).
    import polars as pl

    for date in (D1, D2):
        manifest = json.loads((run_dir / "delivery" / f"{date}.json").read_text())
        facts = pl.concat(
            [pl.read_parquet(f) for f in sorted((run_dir / "store" / date / "facts").rglob("*.parquet"))]
        )
        check(manifest["fact_rows"] > 0 and manifest["energy_wh"] > 0,
              f"{date} delivery manifest: {manifest['fact_rows']} rows, "
              f"{manifest['energy_wh']:.3f} Wh")
        check(facts.height == manifest["fact_rows"]
              and abs(facts["energy_wh"].sum() - manifest["energy_wh"]) < 1e-6,
              f"{date} on-disk minute facts match the delivery manifest")
    return run_dir


def phase_rerun_scope(run_dir: Path) -> None:
    say("== phase 2: rerun one date's check node; only it and its successor redo")
    out = cli(["rerun", "--run-dir", str(run_dir), "--date", D1, "--node", "check"])
    check(out.returncode == 0, "rerun accepted")
    check("check" in out.stdout and "deliver" in out.stdout,
          "rerun reports check+deliver as the reset scope")
    proc = cli(["resume", "--run-dir", str(run_dir)], timeout=300)
    check(proc.returncode == 1, "resume re-executes the reset scope (waveform still failed)")
    check(len(attempt_rows(run_dir, D1, "check")) == 2, f"{D1}/check re-executed")
    check(len(attempt_rows(run_dir, D1, "deliver")) == 2, f"{D1}/deliver re-executed")
    check(len(attempt_rows(run_dir, D1, "samples")) == 1, f"{D1}/samples reused (not redone)")
    check(len(attempt_rows(run_dir, D1, "energy")) == 1, f"{D1}/energy reused (not redone)")
    check(len(attempt_rows(run_dir, D2, "check")) == 1, f"{D2}/check untouched")
    check(len(attempt_rows(run_dir, D2, "waveform")) == 2, f"{D2}/waveform untouched")
    # Repeating the resume changes nothing (no new attempts).
    again = cli(["resume", "--run-dir", str(run_dir)], timeout=120)
    check(again.returncode == 1 and len(attempt_rows(run_dir, D1, "check")) == 2,
          "repeated resume makes no new attempts")


# ---------------------------------------------------------------------------
# phase 3: controlled DAG shapes
# ---------------------------------------------------------------------------


def controlled_definition() -> dict:
    def ok(name, deps=(), sleep=0.0):
        args = demo_jobs(
            "ok", "--dir", "{run_dir}/out/{date}", "--name", name,
            "--trace", "{run_dir}/order.log", "--trace-line", f"{name}:{{date}}",
        )
        if sleep:
            args += ["--sleep", str(sleep)]
        return {"name": name, "args": args, "depends_on": deps}

    return {
        "name": "controlled",
        "limits": {"max_parallel": 4, "retry_delay_seconds": 0.1},
        "nodes": [
            ok("a", sleep=1.0),
            ok("b", ["a"], sleep=0.3),
            ok("c", ["a"]),
            ok("d", ["b", "c"]),
            {"name": "flaky",
             "args": demo_jobs("flaky", "--state", "{run_dir}/flaky-{date}.state",
                               "--dir", "{run_dir}/out/{date}", "--name", "flaky",
                               "--trace", "{run_dir}/order.log",
                               "--trace-line", "flaky:{date}"),
             "depends_on": [], "max_attempts": 2},
            {"name": "hopeless", "args": demo_jobs("fail", "--code", "3"),
             "depends_on": [], "max_attempts": 2},
            {"name": "after_hopeless", "args": demo_jobs(
                "ok", "--dir", "{run_dir}/out/{date}", "--name", "after_hopeless"),
             "depends_on": ["hopeless"]},
            {"name": "liar",
             "args": [PY, "-c",
                      "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text('done');"
                      " print('COMPLETE'); sys.exit(3)",
                      "{run_dir}/liar-{date}.marker"],
             "depends_on": [], "max_attempts": 1},
        ],
    }


def phase_controlled(work: Path) -> None:
    say("== phase 3: fork/join, flaky retry, failure isolation, exit-code truth")
    definition = work / "controlled.json"
    write_json(definition, controlled_definition())
    run_dir = work / "run-controlled"
    proc = cli(["start", "--definition", str(definition), "--dates", D1, D2,
                "--run-dir", str(run_dir)], timeout=300)
    check(proc.returncode == 1, "run exits 1 (hopeless + liar failed)")

    # Join waited for both branches, per date.
    for date in (D1, D2):
        join = attempt_rows(run_dir, date, "d")[0]
        for branch in ("b", "c"):
            check(join["started_at"] >= attempt_rows(run_dir, date, branch)[0]["ended_at"],
                  f"{date}/d started only after {branch} ended")

    # The two dates really interleaved (D2 started before D1 finished).
    with db(run_dir) as conn:
        rows = conn.execute(
            "SELECT j.date, MIN(a.started_at) AS first, MAX(a.ended_at) AS last"
            " FROM attempts a JOIN jobs j ON j.job_id=a.job_id GROUP BY j.date"
        ).fetchall()
    spans = {r["date"]: (r["first"], r["last"]) for r in rows}
    check(spans[D2][0] < spans[D1][1], "the two dates ran interleaved")

    # ...without crossing directories.
    for date in (D1, D2):
        names = {p.name for p in (run_dir / "out" / date).glob("*.done.json")}
        check(names == {f"{n}.done.json" for n in ("a", "b", "c", "d", "flaky")},
              f"{date} output directory holds exactly its own artefacts")

    # Flaky: first attempt failed, second succeeded; both kept.
    for date in (D1, D2):
        atts = attempt_rows(run_dir, date, "flaky")
        check([a["status"] for a in atts] == ["failed_exit", "succeeded"],
              f"{date}/flaky kept both attempts (fail then succeed)")
        check(all(Path(a["stdout_path"]).exists() and Path(a["stderr_path"]).exists()
                  for a in atts),
              f"{date}/flaky attempts have separate stdout/stderr files")

    # Hopeless: budget exhausted, successor blocked, other branches unaffected.
    for date in (D1, D2):
        check([a["exit_code"] for a in attempt_rows(run_dir, date, "hopeless")] == [3, 3],
              f"{date}/hopeless exhausted its 2 attempts with real exit code 3")
        row = job_row(run_dir, date, "after_hopeless")
        check(row["status"] == "blocked" and row["blocked_by"] == "hopeless",
              f"{date}/after_hopeless blocked by hopeless")
        check(job_row(run_dir, date, "flaky")["status"] == "succeeded",
              f"{date}/unrelated branch unaffected")

    # Liar printed COMPLETE and left its marker, but exit 3 means failed.
    liar = attempt_rows(run_dir, D1, "liar")[0]
    check(liar["status"] == "failed_exit" and liar["exit_code"] == 3,
          "liar failed despite printing COMPLETE and writing its marker")
    check("COMPLETE" in Path(liar["stdout_path"]).read_text(),
          "liar stdout log captured the completion claim")

    # Execution order (trace file) cross-checked against the SQLite record.
    order = (run_dir / "order.log").read_text().splitlines()
    with db(run_dir) as conn:
        succeeded = conn.execute(
            "SELECT j.date, j.node FROM jobs j WHERE j.status='succeeded'"
        ).fetchall()
    check(sorted(order) == sorted(f"{r['node']}:{r['date']}" for r in succeeded),
          "trace lines match exactly the succeeded jobs in SQLite")
    for date in (D1, D2):
        seq = [line.split(":")[0] for line in order if line.endswith(f":{date}")]
        pos = {name: seq.index(name) for name in ("a", "b", "c", "d")}
        check(pos["a"] < pos["b"] and pos["a"] < pos["c"]
              and pos["b"] < pos["d"] and pos["c"] < pos["d"],
              f"{date} trace order respects the fork/join dependencies")


# ---------------------------------------------------------------------------
# phase 4+5: timeout and cancellation hygiene
# ---------------------------------------------------------------------------


def phase_timeout(work: Path) -> None:
    say("== phase 4: timeout terminates the whole process group")
    definition = work / "timeout.json"
    write_json(definition, {
        "name": "timeout",
        "nodes": [{
            "name": "spawner",
            "args": demo_jobs("spawner", "--seconds", "6",
                              "--dir", "{run_dir}/spawn/{date}"),
            "timeout_seconds": 1, "max_attempts": 1,
        }],
    })
    run_dir = work / "run-timeout"
    proc = cli(["start", "--definition", str(definition), "--dates", D1,
                "--run-dir", str(run_dir)], timeout=120)
    check(proc.returncode == 1, "run exits 1 after the timeout")
    att = attempt_rows(run_dir, D1, "spawner")[0]
    check(att["status"] == "failed_timeout" and att["timed_out"] == 1,
          "attempt recorded as timed out")
    check(att["signal"] == int(signal.SIGTERM), "worker group received SIGTERM first")
    time.sleep(6.5)  # the grandchild would write now if it had survived
    check(not (run_dir / "spawn" / D1 / "grandchild.done").exists(),
          "grandchild never wrote after the timeout")
    check(wait_no_processes(str(run_dir)), "no processing processes left behind")


def phase_cancel(work: Path) -> None:
    say("== phase 5: cancel stops launches and kills process groups")
    definition = work / "cancel.json"
    write_json(definition, {
        "name": "cancel",
        "limits": {"max_parallel": 2},
        "nodes": [
            {"name": "spawner",
             "args": demo_jobs("spawner", "--seconds", "6",
                               "--dir", "{run_dir}/spawn/{date}"),
             "timeout_seconds": 60},
            {"name": "slow",
             "args": demo_jobs("slow", "--seconds", "30",
                               "--dir", "{run_dir}/out/{date}", "--name", "slow"),
             "timeout_seconds": 60},
            {"name": "queued", "depends_on": ["slow"],
             "args": demo_jobs("ok", "--dir", "{run_dir}/out/{date}", "--name", "queued")},
        ],
    })
    run_dir = work / "run-cancel"
    ctrl = subprocess.Popen(
        [PY, "-m", "minenergy.orchestrator.cli", "start",
         "--definition", str(definition), "--dates", D1, "--run-dir", str(run_dir)],
        env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(2.0)
    out = cli(["cancel", "--run-dir", str(run_dir)])
    check(out.returncode == 0 and "cancelled" in out.stdout, "cancel accepted")
    rc = ctrl.wait(timeout=30)
    check(rc == 3, "controller exits 3 (cancelled)")
    check(run_row(run_dir)["status"] == "cancelled", "run state = cancelled")
    check(job_row(run_dir, D1, "queued")["status"] == "cancelled",
          "queued job cancelled without ever starting")
    check(len(attempt_rows(run_dir, D1, "slow")) == 1, "no attempt started after cancel")
    time.sleep(6.5)
    check(not (run_dir / "spawn" / D1 / "grandchild.done").exists(),
          "cancelled spawner's grandchild never wrote")
    check(wait_no_processes(str(run_dir)), "no processing processes left behind")
    again = cli(["cancel", "--run-dir", str(run_dir)])
    check("already cancelled" in again.stdout
          and len(attempt_rows(run_dir, D1, "slow")) == 1,
          "repeated cancel is a no-op and starts nothing")


# ---------------------------------------------------------------------------
# phase 6+7: crash takeover and mutual exclusion
# ---------------------------------------------------------------------------


def slow_definition(seconds: float) -> dict:
    return {
        "name": "crash",
        "nodes": [
            {"name": "slow",
             "args": demo_jobs("slow", "--seconds", str(seconds),
                               "--dir", "{run_dir}/out/{date}", "--name", "slow"),
             "timeout_seconds": 60},
            {"name": "after", "depends_on": ["slow"],
             "args": demo_jobs("ok", "--dir", "{run_dir}/out/{date}", "--name", "after")},
        ],
    }


def start_background(definition: Path, run_dir: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [PY, "-m", "minenergy.orchestrator.cli", "start",
         "--definition", str(definition), "--dates", D1, "--run-dir", str(run_dir)],
        env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def phase_takeover(work: Path) -> None:
    say("== phase 6: kill the controller; the new one takes over without redoing")

    # 6a: controller dies, worker still running -> adopted, never restarted.
    definition = work / "crash-adopt.json"
    write_json(definition, slow_definition(6.0))
    run_dir = work / "run-adopt"
    ctrl = start_background(definition, run_dir)
    time.sleep(1.5)
    ctrl.kill()
    ctrl.wait()
    check(leftover_processes(str(run_dir)) != [], "old worker still alive after -9")
    proc = cli(["resume", "--run-dir", str(run_dir)], timeout=60)
    check(proc.returncode == 0, "resume adopted the live worker and finished")
    atts = attempt_rows(run_dir, D1, "slow")
    check(len(atts) == 1 and atts[0]["status"] == "succeeded" and atts[0]["exit_code"] == 0,
          "slow has exactly one attempt, succeeded with the real exit 0")
    check((run_dir / "out" / D1 / "slow.done.json").exists(),
          "the old worker's own output confirms a single execution")

    # 6b: controller AND wrapper die -> outcome unknowable -> interrupted.
    definition = work / "crash-lost.json"
    write_json(definition, slow_definition(4.0))
    run_dir = work / "run-lost"
    ctrl = start_background(definition, run_dir)
    time.sleep(1.2)
    with db(run_dir) as conn:
        wrapper_pid = conn.execute(
            "SELECT pid FROM attempts WHERE status IN ('launching','running')"
        ).fetchone()["pid"]
    ctrl.kill()
    os.kill(wrapper_pid, signal.SIGKILL)
    ctrl.wait()
    proc = cli(["resume", "--run-dir", str(run_dir)], timeout=60)
    check(proc.returncode == 4, "resume exits 4: interrupted work remains")
    att = attempt_rows(run_dir, D1, "slow")[0]
    check(att["status"] == "interrupted", "lost attempt recorded as interrupted")
    check(job_row(run_dir, D1, "slow")["status"] == "interrupted",
          "job waits for confirmation, not silently retried")

    # While the orphaned worker is alive, rerun is refused; after its death,
    # accepted — and the interruption fact stays on record.
    refused = cli(["rerun", "--run-dir", str(run_dir), "--date", D1, "--node", "slow"])
    check(refused.returncode != 0 and "live processes" in refused.stderr,
          "rerun refused while the old attempt still runs")
    deadline = time.monotonic() + 15
    while leftover_processes(str(run_dir)) and time.monotonic() < deadline:
        time.sleep(0.2)
    ok = cli(["rerun", "--run-dir", str(run_dir), "--date", D1, "--node", "slow"])
    check(ok.returncode == 0, "rerun accepted once the old attempt is dead")
    proc = cli(["resume", "--run-dir", str(run_dir)], timeout=60)
    check(proc.returncode == 0, "run completes after the confirmed rerun")
    statuses = [a["status"] for a in attempt_rows(run_dir, D1, "slow")]
    check(statuses == ["interrupted", "succeeded"],
          "interrupted attempt preserved in history next to the successful retry")


def phase_concurrent(work: Path) -> None:
    say("== phase 7: two controllers, one executor")
    definition = work / "concurrent.json"
    write_json(definition, slow_definition(4.0))
    run_dir = work / "run-concurrent"
    ctrl = start_background(definition, run_dir)
    time.sleep(1.0)
    second = cli(["resume", "--run-dir", str(run_dir)], timeout=30)
    check(second.returncode == 5 and "REFUSED" in second.stderr,
          "second controller refused without touching state")
    rc = ctrl.wait(timeout=60)
    check(rc == 0 and len(attempt_rows(run_dir, D1, "slow")) == 1,
          "first controller finished alone; exactly one attempt")


# ---------------------------------------------------------------------------
# phase 8: definition safety
# ---------------------------------------------------------------------------


def phase_validation(work: Path) -> None:
    say("== phase 8: bad definitions never start half a graph; snapshot binding")
    bad = {
        "cycle": {"nodes": [
            {"name": "a", "args": ["x"], "depends_on": ["b"]},
            {"name": "b", "args": ["x"], "depends_on": ["a"]}]},
        "dangling": {"nodes": [{"name": "a", "args": ["x"], "depends_on": ["ghost"]}]},
        "duplicate": {"nodes": [{"name": "a", "args": ["x"]}, {"name": "a", "args": ["y"]}]},
        "placeholder": {"nodes": [{"name": "a", "args": ["run", "{dat}"]}]},
        "limits": {"limits": {"max_parallel": 0},
                   "nodes": [{"name": "a", "args": ["x"]}]},
        "attempts": {"nodes": [{"name": "a", "args": ["x"], "max_attempts": 0}]},
    }
    for name, doc in bad.items():
        path = work / f"bad-{name}.json"
        write_json(path, doc)
        out = cli(["plan", "--definition", str(path), "--dates", D1,
                   "--run-dir", str(work / f"run-bad-{name}")])
        check(out.returncode == 2 and "DEFINITION REJECTED" in out.stderr,
              f"bad definition rejected: {name}")
        check(not (work / f"run-bad-{name}").exists(),
              f"no run directory created for: {name}")
    dup_dates = work / "dup-dates.json"
    write_json(dup_dates, {"nodes": [{"name": "a", "args": ["x"]}]})
    out = cli(["plan", "--definition", str(dup_dates), "--dates", D1, D1])
    check(out.returncode != 0, "duplicate dates rejected")

    # An established run is bound to its definition snapshot.
    definition = work / "snapshot.json"
    write_json(definition, slow_definition(0.3))
    run_dir = work / "run-snapshot"
    created = cli(["create", "--definition", str(definition), "--dates", D1,
                   "--run-dir", str(run_dir)])
    check(created.returncode == 0, "run created without executing")
    write_json(definition, {"nodes": [{"name": "replaced", "args": ["false"]}]})
    proc = cli(["resume", "--run-dir", str(run_dir)], timeout=60)
    check(proc.returncode == 0 and job_row(run_dir, D1, "slow")["status"] == "succeeded",
          "editing the definition file did not change the established run")


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="minenergy-orchestrator-demo")
    parser.add_argument("--workdir", default="/tmp/minenergy-nightshift")
    args = parser.parse_args(argv)
    work = Path(args.workdir)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    say(f"night-shift drill in {work}")

    run_dir = phase_real_chain(work)
    phase_rerun_scope(run_dir)
    phase_controlled(work)
    phase_timeout(work)
    phase_cancel(work)
    phase_takeover(work)
    phase_concurrent(work)
    phase_validation(work)

    say("")
    if FAILURES:
        say(f"RESULT: {len(FAILURES)} assertion(s) FAILED")
        for failure in FAILURES:
            say(f"  FAIL {failure}")
        return 1
    say("RESULT: all night-shift drill assertions passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
