"""Orchestrator tests: definition validation, execution, retries, isolation,
rerun scoping, timeout/cancel process-group hygiene, crash takeover and
controller mutual exclusion.

Crash and concurrency cases drive real controller subprocesses; the rest use
the in-process API.  All worker processes are the controllable
``minenergy.orchestrator.demo_jobs`` commands.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from minenergy.orchestrator import (
    EXIT_CANCELLED,
    EXIT_CONFLICT,
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    DefinitionError,
    RunStateError,
    cancel_run,
    create_run,
    definition_from_dict,
    execute_run,
    load_definition,
    rerun_job,
    run_status,
    validate_dates,
)
from minenergy.orchestrator import process as proc
from minenergy.orchestrator.store import RunStore

REPO = Path(__file__).resolve().parents[2]
DEMO = ["-m", "minenergy.orchestrator.demo_jobs"]
D1 = "2025-01-01"
D2 = "2025-01-02"


def py(*args: str) -> list[str]:
    return [sys.executable, *args]


def ok_job(run_dir: str, date: str, name: str, **extra) -> dict:
    return {
        "name": name,
        "args": py(
            *DEMO, "ok", "--dir", f"{run_dir}/out/{date}", "--name", name
        ),
        **extra,
    }


def write_def(tmp_path: Path, nodes: list[dict], limits: dict | None = None,
              name: str = "test") -> Path:
    doc = {"name": name, "limits": limits or {}, "nodes": nodes}
    path = tmp_path / "definition.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def run_def(tmp_path: Path, nodes, dates=(D1,), limits=None, name="test") -> Path:
    definition = load_definition(write_def(tmp_path, nodes, limits, name))
    run_dir = tmp_path / "run"
    create_run(definition, list(dates), run_dir)
    return run_dir


def job(report: dict, date: str, node: str) -> dict:
    return next(j for j in report["jobs"] if j["date"] == date and j["node"] == node)


def attempts(report: dict, date: str, node: str) -> list[dict]:
    return job(report, date, node)["attempts"]


# ---------------------------------------------------------------------------
# definition validation
# ---------------------------------------------------------------------------


def _node(name, deps=(), args=None, **kw):
    return {"name": name, "args": args or ["echo", "hi"], "depends_on": list(deps), **kw}


def test_definition_cycle_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", ["b"]), _node("b", ["a"])]})
    assert any("cycle" in p for p in exc.value.problems)


def test_definition_dangling_dependency_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", ["ghost"])]})
    assert any("dangling" in p for p in exc.value.problems)


def test_definition_duplicate_node_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a"), _node("a")]})
    assert any("duplicate node name" in p for p in exc.value.problems)


def test_definition_unknown_placeholder_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", args=["run", "{dat}"])]})
    assert any("unknown placeholder" in p for p in exc.value.problems)


def test_definition_unbalanced_brace_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", args=["run", "{date"])]})
    assert any("unbalanced" in p for p in exc.value.problems)


def test_definition_known_placeholders_accepted():
    d = definition_from_dict(
        {"nodes": [_node("a", args=["run", "{date}", "{run_dir}"], cwd="{run_dir}")]}
    )
    assert d.nodes[0].args == ("run", "{date}", "{run_dir}")


@pytest.mark.parametrize(
    "limits,fragment",
    [
        ({"max_parallel": 0}, "max_parallel"),
        ({"default_timeout_seconds": -1}, "default_timeout_seconds"),
        ({"default_max_attempts": 0}, "default_max_attempts"),
    ],
)
def test_definition_invalid_limits_rejected(limits, fragment):
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"limits": limits, "nodes": [_node("a")]})
    assert any(fragment in p for p in exc.value.problems)


def test_node_invalid_limits_rejected():
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", timeout_seconds=0)]})
    assert any("timeout_seconds" in p for p in exc.value.problems)
    with pytest.raises(DefinitionError) as exc:
        definition_from_dict({"nodes": [_node("a", max_attempts=0)]})
    assert any("max_attempts" in p for p in exc.value.problems)


def test_dates_validated():
    assert validate_dates(["2025-02-28", "2024-02-29"]) == ["2025-02-28", "2024-02-29"]
    with pytest.raises(DefinitionError):
        validate_dates(["2025-02-30"])  # not a real date
    with pytest.raises(DefinitionError):
        validate_dates(["2025-1-1"])  # not YYYY-MM-DD
    with pytest.raises(DefinitionError):
        validate_dates(["2025-01-01", "2025-01-01"])  # duplicate


def test_bad_definition_creates_nothing(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"nodes": [_node("a", ["b"]), _node("b", ["a"])]}))
    with pytest.raises(DefinitionError):
        load_definition(path)
    # A rejected definition must not start half a graph.
    assert not (tmp_path / "run").exists()


# ---------------------------------------------------------------------------
# basic execution: fork/join over two dates
# ---------------------------------------------------------------------------


def fork_join_nodes(run_dir: str, date_ph: str = "{date}") -> list[dict]:
    def ok(name, deps=(), sleep=0.0):
        args = py(
            *DEMO, "ok", "--dir", f"{{run_dir}}/out/{date_ph}", "--name", name,
            "--trace", "{run_dir}/order.log", "--trace-line", f"{name}:{date_ph}",
        )
        if sleep:
            args += ["--sleep", str(sleep)]
        return {"name": name, "args": args, "depends_on": list(deps)}

    return [
        ok("a"),
        ok("b", ["a"], sleep=0.3),
        ok("c", ["a"]),
        ok("d", ["b", "c"]),
    ]


def test_two_dates_fork_join(tmp_path):
    run_dir = run_def(tmp_path, fork_join_nodes("{run_dir}"), dates=(D1, D2))
    assert execute_run(run_dir) == EXIT_OK
    report = run_status(run_dir)
    assert report["status"] == "completed"
    assert report["counts"] == {"succeeded": 8}

    # The join node started only after both its branches ended (per date).
    for date in (D1, D2):
        join_start = attempts(report, date, "d")[0]["started_at"]
        for branch in ("b", "c"):
            branch_end = attempts(report, date, branch)[0]["ended_at"]
            assert join_start >= branch_end, f"{date}: join did not wait for {branch}"

    # Dates never crossed directories.
    for date in (D1, D2):
        out = run_dir / "out" / date
        assert {p.name for p in out.glob("*.done.json")} == {
            f"{n}.done.json" for n in "abcd"
        }
    # Execution order respects dependencies (from the shared trace file).
    order = (run_dir / "order.log").read_text().splitlines()
    for date in (D1, D2):
        seq = [line.split(":")[0] for line in order if line.endswith(f":{date}")]
        assert seq[0] == "a"
        assert seq.index("d") > seq.index("b")
        assert seq.index("d") > seq.index("c")


def test_max_parallel_never_exceeded(tmp_path):
    nodes = [
        {
            "name": f"w{i}",
            "args": py(*DEMO, "slow", "--seconds", "0.6",
                       "--dir", "{run_dir}/out/{date}", "--name", f"w{i}"),
        }
        for i in range(6)
    ]
    run_dir = run_def(tmp_path, nodes, limits={"max_parallel": 2})
    peak = {"n": 0}
    stop = threading.Event()

    def monitor():
        store = RunStore(run_dir)
        while not stop.is_set():
            peak["n"] = max(peak["n"], len(store.active_attempts()))
            time.sleep(0.01)
        store.close()

    t = threading.Thread(target=monitor)
    t.start()
    assert execute_run(run_dir) == EXIT_OK
    stop.set()
    t.join()
    assert peak["n"] <= 2
    assert peak["n"] >= 1  # the monitor really observed the run


# ---------------------------------------------------------------------------
# retries, failure isolation, exit-code-only success
# ---------------------------------------------------------------------------


def test_flaky_retried_and_attempts_kept(tmp_path):
    nodes = [
        {
            "name": "flaky",
            "args": py(*DEMO, "flaky", "--state", "{run_dir}/flaky-{date}.state",
                       "--dir", "{run_dir}/out/{date}", "--name", "flaky"),
            "max_attempts": 2,
        }
    ]
    run_dir = run_def(tmp_path, nodes, limits={"retry_delay_seconds": 0.05})
    assert execute_run(run_dir) == EXIT_OK
    atts = attempts(run_status(run_dir), D1, "flaky")
    assert [a["status"] for a in atts] == ["failed_exit", "succeeded"]
    assert atts[0]["exit_code"] == 1
    assert atts[1]["exit_code"] == 0
    # Every attempt keeps its own stdout/stderr files.
    for a in atts:
        assert Path(a["stdout"]).exists()
        assert Path(a["stderr"]).exists()


def test_permanent_failure_isolated_and_successor_blocked(tmp_path):
    nodes = [
        {"name": "hopeless",
         "args": py(*DEMO, "fail", "--code", "3"), "max_attempts": 2},
        {"name": "after", "depends_on": ["hopeless"],
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/out/{date}", "--name", "after")},
        {"name": "unrelated",
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/out/{date}", "--name", "unrelated")},
    ]
    run_dir = run_def(tmp_path, nodes, dates=(D1, D2),
                      limits={"retry_delay_seconds": 0.05})
    assert execute_run(run_dir) == EXIT_FAILED
    report = run_status(run_dir)
    assert report["status"] == "failed"
    for date in (D1, D2):
        assert len(attempts(report, date, "hopeless")) == 2  # budget exhausted
        assert job(report, date, "hopeless")["status"] == "failed"
        blocked = job(report, date, "after")
        assert blocked["status"] == "blocked"
        assert blocked["blocked_by"] == "hopeless"
        assert blocked["attempts"] == []  # never started
        # The unrelated branch still ran.
        assert job(report, date, "unrelated")["status"] == "succeeded"


def test_success_requires_real_exit_zero(tmp_path):
    # Prints a completion line and writes an output file, but exits 3: the
    # job must be failed — success comes only from the real exit code.
    marker = "{run_dir}/liar-{date}.marker"
    nodes = [
        {
            "name": "liar",
            "args": py("-c", "import pathlib,sys; "
                             "pathlib.Path(sys.argv[1]).write_text('done'); "
                             "print('COMPLETE'); sys.exit(3)", marker),
            "max_attempts": 1,
        }
    ]
    run_dir = run_def(tmp_path, nodes)
    assert execute_run(run_dir) == EXIT_FAILED
    report = run_status(run_dir)
    assert job(report, D1, "liar")["status"] == "failed"
    assert (run_dir / f"liar-{D1}.marker").exists()  # output exists, still failed
    att = attempts(report, D1, "liar")[0]
    assert att["exit_code"] == 3
    assert "COMPLETE" in Path(att["stdout"]).read_text()


# ---------------------------------------------------------------------------
# rerun scoping
# ---------------------------------------------------------------------------


def test_rerun_resets_only_target_and_successors(tmp_path):
    run_dir = run_def(tmp_path, fork_join_nodes("{run_dir}"), dates=(D1, D2))
    assert execute_run(run_dir) == EXIT_OK

    result = rerun_job(run_dir, D1, "b")
    assert sorted(result["reset"]) == ["b", "d"]
    assert sorted(result["reused"]) == ["a", "c"]

    assert execute_run(run_dir) == EXIT_OK
    report = run_status(run_dir)
    # b and d of D1 ran again (new generation, history preserved).
    assert [a["generation"] for a in attempts(report, D1, "b")] == [0, 1]
    assert [a["generation"] for a in attempts(report, D1, "d")] == [0, 1]
    # Predecessors and the other date were NOT re-executed.
    assert len(attempts(report, D1, "a")) == 1
    assert len(attempts(report, D1, "c")) == 1
    for node in "abcd":
        assert len(attempts(report, D2, node)) == 1
    assert report["status"] == "completed"


def test_rerun_refused_with_active_attempt(tmp_path):
    nodes = [
        {"name": "slow",
         "args": py(*DEMO, "slow", "--seconds", "5",
                    "--dir", "{run_dir}/out/{date}", "--name", "slow"),
         "timeout_seconds": 30},
    ]
    run_dir = run_def(tmp_path, nodes)
    done = threading.Event()
    holder: dict = {}

    def drive():
        holder["rc"] = execute_run(run_dir)
        done.set()

    t = threading.Thread(target=drive)
    t.start()
    # Wait until the attempt is really in flight, then rerun must refuse.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with RunStore(run_dir) as store:
            if store.active_attempts():
                break
        time.sleep(0.05)
    else:
        raise AssertionError("attempt never started")
    with pytest.raises(RunStateError):
        rerun_job(run_dir, D1, "slow")
    cancel_run(run_dir)
    done.wait(10)
    t.join()
    assert holder["rc"] == EXIT_CANCELLED


def test_rerun_refused_on_cancelled_run(tmp_path):
    nodes = [
        {"name": "slow",
         "args": py(*DEMO, "slow", "--seconds", "10",
                    "--dir", "{run_dir}/out/{date}", "--name", "slow"),
         "timeout_seconds": 60},
    ]
    run_dir = run_def(tmp_path, nodes)
    done = threading.Event()
    holder: dict = {}

    def drive():
        holder["rc"] = execute_run(run_dir)
        done.set()

    t = threading.Thread(target=drive)
    t.start()
    time.sleep(1.0)
    assert cancel_run(run_dir) == "cancelled (by the running controller)"
    done.wait(15)
    t.join()
    assert holder["rc"] == EXIT_CANCELLED
    assert run_status(run_dir)["status"] == "cancelled"
    with pytest.raises(RunStateError):
        rerun_job(run_dir, D1, "slow")


def test_repeated_resume_makes_no_new_attempts(tmp_path):
    run_dir = run_def(tmp_path, fork_join_nodes("{run_dir}"))
    assert execute_run(run_dir) == EXIT_OK
    assert execute_run(run_dir) == EXIT_OK  # nothing left to do
    report = run_status(run_dir)
    for node in "abcd":
        assert len(attempts(report, D1, node)) == 1


# ---------------------------------------------------------------------------
# timeout and cancellation: no leftover processing
# ---------------------------------------------------------------------------


def test_timeout_kills_process_group(tmp_path):
    nodes = [
        {"name": "spawner",
         "args": py(*DEMO, "spawner", "--seconds", "6",
                    "--dir", "{run_dir}/spawn/{date}"),
         "timeout_seconds": 1, "max_attempts": 1},
    ]
    run_dir = run_def(tmp_path, nodes)
    assert execute_run(run_dir) == EXIT_FAILED
    att = attempts(run_status(run_dir), D1, "spawner")[0]
    assert att["status"] == "failed_timeout"
    assert att["timed_out"] is True
    assert att["signal"] == signal.SIGTERM
    # The grandchild (which would write at ~6s) was killed with the group.
    time.sleep(6.5)
    assert not (run_dir / "spawn" / D1 / "grandchild.done").exists()


def test_cancel_terminates_groups_and_is_idempotent(tmp_path):
    nodes = [
        {"name": "spawner",
         "args": py(*DEMO, "spawner", "--seconds", "6",
                    "--dir", "{run_dir}/spawn/{date}"),
         "timeout_seconds": 60},
        {"name": "slow",
         "args": py(*DEMO, "slow", "--seconds", "30",
                    "--dir", "{run_dir}/out/{date}", "--name", "slow"),
         "timeout_seconds": 60},
        {"name": "queued", "depends_on": ["slow"],
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/out/{date}", "--name", "queued")},
    ]
    run_dir = run_def(tmp_path, nodes, limits={"max_parallel": 2})
    holder: dict = {}
    done = threading.Event()

    def drive():
        holder["rc"] = execute_run(run_dir)
        done.set()

    t = threading.Thread(target=drive)
    t.start()
    time.sleep(1.5)
    assert cancel_run(run_dir) == "cancelled (by the running controller)"
    done.wait(15)
    t.join()
    assert holder["rc"] == EXIT_CANCELLED

    report = run_status(run_dir)
    assert report["status"] == "cancelled"
    assert job(report, D1, "spawner")["status"] == "cancelled"
    assert job(report, D1, "slow")["status"] == "cancelled"
    assert job(report, D1, "queued")["status"] == "cancelled"
    # No new instances started after the flag; no processing left behind.
    time.sleep(6.5)
    assert not (run_dir / "spawn" / D1 / "grandchild.done").exists()
    # Repeated cancel is a no-op and starts nothing.
    assert cancel_run(run_dir) == "run already cancelled"
    assert len(attempts(run_status(run_dir), D1, "slow")) == 1


# ---------------------------------------------------------------------------
# crash takeover (real controller subprocesses)
# ---------------------------------------------------------------------------


def cli_env() -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO)
    return env


def start_controller(tmp_path: Path, dates=(D1,), extra=()) -> subprocess.Popen:
    return subprocess.Popen(
        py("-m", "minenergy.orchestrator.cli", "start",
           "--definition", str(tmp_path / "definition.json"),
           "--dates", *dates,
           "--run-dir", str(tmp_path / "run"), *extra),
        cwd=tmp_path, env=cli_env(),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def resume_controller(tmp_path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        py("-m", "minenergy.orchestrator.cli", "resume",
           "--run-dir", str(tmp_path / "run")),
        cwd=tmp_path, env=cli_env(), capture_output=True, text=True, timeout=120,
    )


def slow_def(tmp_path: Path, seconds: float, extra_nodes=()) -> None:
    nodes = [
        {"name": "slow",
         "args": py(*DEMO, "slow", "--seconds", str(seconds),
                    "--dir", "{run_dir}/out/{date}", "--name", "slow"),
         "timeout_seconds": 60},
        *extra_nodes,
    ]
    write_def(tmp_path, nodes)


def test_takeover_adopts_live_worker(tmp_path):
    slow_def(tmp_path, 6.0)
    ctrl = start_controller(tmp_path)
    time.sleep(1.5)
    ctrl.kill()  # controller dies; wrapper + worker keep running
    ctrl.wait()

    result = resume_controller(tmp_path)
    assert result.returncode == EXIT_OK, result.stdout + result.stderr
    report = run_status(tmp_path / "run")
    # The old attempt was adopted: exactly one attempt, real exit code 0.
    atts = attempts(report, D1, "slow")
    assert len(atts) == 1
    assert atts[0]["status"] == "succeeded"
    assert atts[0]["exit_code"] == 0
    # The worker's own output proves it ran to completion exactly once.
    assert (tmp_path / "run" / "out" / D1 / "slow.done.json").exists()


def test_takeover_uses_result_file_when_wrapper_finished(tmp_path):
    slow_def(tmp_path, 2.0)
    ctrl = start_controller(tmp_path)
    time.sleep(1.0)
    ctrl.kill()
    ctrl.wait()
    time.sleep(2.5)  # wrapper finishes and records the result meanwhile

    result = resume_controller(tmp_path)
    assert result.returncode == EXIT_OK
    atts = attempts(run_status(tmp_path / "run"), D1, "slow")
    assert len(atts) == 1
    assert atts[0]["status"] == "succeeded"


def test_interrupted_attempt_preserved_then_rerun(tmp_path):
    slow_def(tmp_path, 3.0)
    ctrl = start_controller(tmp_path)
    time.sleep(1.2)
    # Kill the controller AND the wrapper: the orphaned worker's outcome can
    # no longer be recorded by anyone.
    with RunStore(tmp_path / "run") as store:
        wrapper_pid = store.active_attempts()[0]["pid"]
    ctrl.kill()
    os.kill(wrapper_pid, signal.SIGKILL)
    ctrl.wait()

    result = resume_controller(tmp_path)
    assert result.returncode == EXIT_INTERRUPTED
    report = run_status(tmp_path / "run")
    att = attempts(report, D1, "slow")[0]
    assert att["status"] == "interrupted"
    assert job(report, D1, "slow")["status"] == "interrupted"

    # While the orphaned worker is still alive, rerun must be refused.  The
    # worker identity is carried on the attempt row in the DB.
    with RunStore(tmp_path / "run") as store:
        row = store.attempts_for_job(store.get_job(D1, "slow")["job_id"])[0]
    assert proc.process_group_alive(
        row["worker_pgid"], row["worker_pid"], row["worker_pid_start"]
    )
    with pytest.raises(RunStateError):
        rerun_job(tmp_path / "run", D1, "slow")

    # After the old worker exits (3s total), the rerun is accepted and the
    # interruption fact stays on record.
    time.sleep(3.0)
    rerun_job(tmp_path / "run", D1, "slow")
    result = resume_controller(tmp_path)
    assert result.returncode == EXIT_OK
    report = run_status(tmp_path / "run")
    statuses = [a["status"] for a in attempts(report, D1, "slow")]
    assert statuses == ["interrupted", "succeeded"]


def test_concurrent_controllers_single_executor(tmp_path):
    slow_def(tmp_path, 4.0)
    ctrl = start_controller(tmp_path)
    time.sleep(1.0)
    # A second controller is refused immediately and changes nothing.
    conflict = resume_controller(tmp_path)
    assert conflict.returncode == EXIT_CONFLICT
    assert "REFUSED" in conflict.stderr
    ctrl.wait(timeout=30)
    assert ctrl.returncode == EXIT_OK
    report = run_status(tmp_path / "run")
    assert len(attempts(report, D1, "slow")) == 1
    assert report["status"] == "completed"


def test_snapshot_binding_definition_cannot_change(tmp_path):
    slow_def(tmp_path, 0.5)
    definition = load_definition(tmp_path / "definition.json")
    run_dir = tmp_path / "run"
    create_run(definition, [D1], run_dir)
    # Editing the definition file after creation must not affect the run.
    write_def(tmp_path, [
        {"name": "replaced",
         "args": py(*DEMO, "fail", "--code", "9"), "max_attempts": 1}
    ])
    result = resume_controller(tmp_path)
    assert result.returncode == EXIT_OK
    report = run_status(run_dir)
    assert {j["node"] for j in report["jobs"]} == {"slow"}
    assert report["status"] == "completed"


# ---------------------------------------------------------------------------
# status reporting
# ---------------------------------------------------------------------------


def test_status_distinguishes_all_states(tmp_path):
    nodes = [
        {"name": "bad", "args": py(*DEMO, "fail", "--code", "2"), "max_attempts": 1},
        {"name": "downstream", "depends_on": ["bad"],
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/o/{date}", "--name", "downstream")},
        {"name": "good",
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/o/{date}", "--name", "good")},
    ]
    run_dir = run_def(tmp_path, nodes)
    assert execute_run(run_dir) == EXIT_FAILED
    report = run_status(run_dir)
    states = {j["node"]: j["status"] for j in report["jobs"]}
    assert states == {"bad": "failed", "downstream": "blocked", "good": "succeeded"}
    # Attempt rows carry start/end, exit code and per-attempt log files.
    att = attempts(report, D1, "bad")[0]
    assert att["exit_code"] == 2
    assert att["started_at"] and att["ended_at"]
    assert Path(att["stdout"]).exists() and Path(att["stderr"]).exists()


def test_run_dir_reuse_rejected(tmp_path):
    run_dir = run_def(tmp_path, fork_join_nodes("{run_dir}"))
    definition = load_definition(tmp_path / "definition.json")
    with pytest.raises(RunStateError):
        create_run(definition, [D1], run_dir)


def test_plan_lists_instances_and_dependencies(tmp_path):
    from minenergy.orchestrator import plan_lines

    definition = load_definition(write_def(tmp_path, fork_join_nodes("{run_dir}")))
    lines = plan_lines(definition, [D1, D2], tmp_path / "run")
    text = "\n".join(lines)
    # Every instance of both dates with resolved placeholders and dependencies.
    assert text.count("argv:") == 8
    assert "deps=[b,c]" in text
    assert str(tmp_path / "run") in text
    assert "{date}" not in text and "{run_dir}" not in text


def test_status_during_execution_shows_running_and_retry_wait(tmp_path):
    nodes = [
        {"name": "slow",
         "args": py(*DEMO, "slow", "--seconds", "2",
                    "--dir", "{run_dir}/o/{date}", "--name", "slow"),
         "timeout_seconds": 30},
        {"name": "waiting", "depends_on": ["slow"],
         "args": py(*DEMO, "ok", "--dir", "{run_dir}/o/{date}", "--name", "waiting")},
        {"name": "flaky",
         "args": py(*DEMO, "flaky", "--state", "{run_dir}/flaky-{date}.state"),
         "max_attempts": 2},
    ]
    run_dir = run_def(tmp_path, nodes, limits={"retry_delay_seconds": 2.0})
    done = threading.Event()

    def drive():
        execute_run(run_dir)
        done.set()

    t = threading.Thread(target=drive)
    t.start()
    seen: set[str] = set()
    deadline = time.monotonic() + 15
    while not done.is_set() and time.monotonic() < deadline:
        states = {j["node"]: j["status"] for j in run_status(run_dir)["jobs"]}
        seen.update(states.values())
        if states.get("slow") == "running":
            seen.add("observed-running")
        if states.get("waiting") == "pending":
            seen.add("observed-pending")
        if states.get("flaky") == "retry_wait":
            seen.add("observed-retry-wait")
        time.sleep(0.02)
    t.join()
    assert "observed-running" in seen
    assert "observed-pending" in seen
    assert "observed-retry-wait" in seen
    assert run_status(run_dir)["status"] == "completed"
