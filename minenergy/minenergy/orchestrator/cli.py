"""Command-line interface for the local data-job orchestrator.

    minenergy-orchestrate plan    --definition D --dates YYYY-MM-DD...
    minenergy-orchestrate create  --definition D --dates ... --run-dir DIR
    minenergy-orchestrate start   --definition D --dates ... --run-dir DIR
    minenergy-orchestrate resume  --run-dir DIR
    minenergy-orchestrate status  --run-dir DIR [--json]
    minenergy-orchestrate cancel  --run-dir DIR
    minenergy-orchestrate rerun   --run-dir DIR --date YYYY-MM-DD --node NAME

``plan`` validates and lists the instances/dependencies without creating
anything.  ``create`` records the run (definition snapshot) without executing.
``start`` creates and executes in the foreground.  ``resume`` takes over an
existing run.  Exit codes: 0 completed, 1 finished with failures, 2 bad
definition/usage, 3 cancelled, 4 interrupted jobs remain, 5 another
controller holds execution rights.
"""
from __future__ import annotations

import argparse
import json
import sys

from .controller import (
    EXIT_CONFLICT,
    EXIT_DEFINITION,
    cancel_run,
    create_run,
    execute_run,
    rerun_job,
    run_status,
    start_run,
)
from .definition import load_definition, plan_lines, validate_dates
from .errors import (
    ControllerConflictError,
    DefinitionError,
    OrchestratorError,
    RunNotFoundError,
    RunStateError,
)


def cmd_plan(args) -> int:
    definition = load_definition(args.definition)
    dates = validate_dates(args.dates)
    for line in plan_lines(definition, dates, args.run_dir or "<run_dir>"):
        print(line)
    return 0


def cmd_create(args) -> int:
    definition = load_definition(args.definition)
    dates = validate_dates(args.dates)
    for line in plan_lines(definition, dates, args.run_dir):
        print(line)
    run_id = create_run(definition, dates, args.run_dir, max_parallel=args.max_parallel)
    print(f"run_id={run_id}")
    print(f"run recorded at {args.run_dir} (not started; use 'resume' to execute)")
    return 0


def cmd_start(args) -> int:
    return start_run(
        args.definition, args.dates, args.run_dir, max_parallel=args.max_parallel
    )


def cmd_resume(args) -> int:
    return execute_run(args.run_dir)


def cmd_status(args) -> int:
    report = run_status(args.run_dir)
    if args.json:
        print(json.dumps(report, indent=2))
        return 0
    print(
        f"run {report['run_id']} ({report['name']}): {report['status']}"
        f"  definition={report['definition_sha256'][:16]}…"
        f"  max_parallel={report['max_parallel']}"
    )
    counts = report["counts"]
    if counts:
        print("  counts: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    for job in report["jobs"]:
        line = f"  {job['date']}  {job['node']:<16} {job['status']}"
        if job["blocked_by"]:
            line += f" (blocked by {job['blocked_by']})"
        line += f"  attempts={len(job['attempts'])}/{job['max_attempts']} gen={job['generation']}"
        print(line)
        for att in job["attempts"]:
            bits = [f"    g{att['generation']}#{att['seq']} {att['status']}"]
            if att["exit_code"] is not None:
                bits.append(f"exit={att['exit_code']}")
            if att["signal"] is not None:
                bits.append(f"signal={att['signal']}")
            if att["timed_out"]:
                bits.append("timed_out")
            if att["reason"]:
                bits.append(f"({att['reason']})")
            bits.append(f"{att['started_at']} → {att['ended_at'] or '…'}")
            print(" ".join(bits))
    return 0


def cmd_cancel(args) -> int:
    outcome = cancel_run(args.run_dir)
    print(outcome)
    return 0


def cmd_rerun(args) -> int:
    result = rerun_job(args.run_dir, args.date, args.node)
    print(
        f"reset [{', '.join(result['reset'])}] for {result['date']};"
        f" reused predecessors: [{', '.join(result['reused'])}]"
    )
    print("use 'resume' to execute the reset jobs")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy-orchestrate")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_definition_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--definition", required=True, help="job definition JSON")
        p.add_argument("--dates", nargs="+", required=True, help="YYYY-MM-DD data dates")

    p = sub.add_parser("plan", help="validate and list instances without creating anything")
    add_definition_args(p)
    p.add_argument("--run-dir", default=None, help="only used to resolve {run_dir} in the listing")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("create", help="record a run without executing it")
    add_definition_args(p)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--max-parallel", type=int, default=None)
    p.set_defaults(func=cmd_create)

    p = sub.add_parser("start", help="create a run and execute it in the foreground")
    add_definition_args(p)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--max-parallel", type=int, default=None)
    p.set_defaults(func=cmd_start)

    p = sub.add_parser("resume", help="take over an existing run and execute it")
    p.add_argument("--run-dir", required=True)
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("status", help="show run/job/attempt state")
    p.add_argument("--run-dir", required=True)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("cancel", help="cancel the whole run (idempotent)")
    p.add_argument("--run-dir", required=True)
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("rerun", help="invalidate one date+node and its same-date successors")
    p.add_argument("--run-dir", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--node", required=True)
    p.set_defaults(func=cmd_rerun)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except DefinitionError as exc:
        print("DEFINITION REJECTED:", file=sys.stderr)
        for problem in exc.problems:
            print(f"  - {problem}", file=sys.stderr)
        return EXIT_DEFINITION
    except ControllerConflictError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_CONFLICT
    except (RunStateError, RunNotFoundError, OrchestratorError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_DEFINITION


if __name__ == "__main__":
    raise SystemExit(main())
