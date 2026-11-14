"""Small controllable processing commands for tests and the night-shift drill.

Each subcommand is a real program the orchestrator launches as a worker:

    ok        --dir D --name N [--sleep S] [--trace FILE --trace-line L]
    fail      --code N [--trace FILE --trace-line L]
    flaky     --state FILE [--fail-times N] [--dir D --name N]
    slow      --seconds S --dir D --name N
    spawner   --seconds S --dir D
    collect   --store D --out FILE
    append    --file F --line L

``flaky`` fails the first N invocations (counter kept in a state file) and
succeeds afterwards — the "first try fails, retry succeeds" shape.
``spawner`` re-spawns a grandchild that would outlive a naive parent kill, to
prove cancellation terminates the whole process group.
``collect`` reads the on-disk minute facts of a minenergy store and writes a
delivery manifest — the "hand to downstream" step of the real data chain.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _trace(path: str | None, line: str | None) -> None:
    if path and line:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{line}\n")


def _write_done(directory: str, name: str, extra: dict | None = None) -> None:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    payload = {"name": name, "at": _utc_now(), "pid": os.getpid()}
    if extra:
        payload.update(extra)
    (d / f"{name}.done.json").write_text(json.dumps(payload) + "\n", encoding="utf-8")


def cmd_ok(args) -> int:
    if args.sleep:
        time.sleep(args.sleep)
    _write_done(args.dir, args.name)
    _trace(args.trace, args.trace_line)
    return 0


def cmd_fail(args) -> int:
    _trace(args.trace, args.trace_line)
    print(f"failing with exit code {args.code}", file=sys.stderr)
    return args.code


def cmd_flaky(args) -> int:
    state = Path(args.state)
    state.parent.mkdir(parents=True, exist_ok=True)
    count = int(state.read_text().strip()) if state.exists() else 0
    count += 1
    state.write_text(str(count), encoding="utf-8")
    if count <= args.fail_times:
        print(f"flaky: invocation {count} fails", file=sys.stderr)
        return 1
    if args.dir and args.name:
        _write_done(args.dir, args.name, extra={"invocation": count})
    _trace(args.trace, args.trace_line)
    return 0


def cmd_slow(args) -> int:
    d = Path(args.dir)
    d.mkdir(parents=True, exist_ok=True)
    heartbeat = d / f"{args.name}.heartbeat"
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        heartbeat.write_text(_utc_now() + "\n", encoding="utf-8")
        time.sleep(0.1)
    _write_done(args.dir, args.name)
    return 0


def cmd_spawner(args) -> int:
    """Spawn a grandchild that writes a file after ``seconds``, then wait.

    The grandchild stays in the same process group (no new session), so a
    group-targeted termination kills it together with this parent; a
    parent-only kill would leave it running and the file would appear.
    """
    d = Path(args.dir)
    d.mkdir(parents=True, exist_ok=True)
    marker = d / "grandchild.done"
    child = subprocess.Popen(
        [
            sys.executable, "-c",
            "import time, pathlib, sys;"
            f"time.sleep({args.seconds});"
            f"pathlib.Path({str(marker)!r}).write_text('grandchild survived\\n')",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (d / "grandchild.pid").write_text(str(child.pid), encoding="utf-8")
    try:
        child.wait(timeout=args.seconds + 30)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
    return 0


def cmd_collect(args) -> int:
    """Read on-disk minute facts and write a delivery manifest."""
    import polars as pl  # local import: only this command needs it

    store = Path(args.store)
    files = sorted(store.rglob("facts/**/data.parquet"))
    frames = [pl.read_parquet(f) for f in files]
    facts = pl.concat(frames) if frames else pl.DataFrame()
    summary = {
        "store": str(store),
        "collected_at": _utc_now(),
        "fact_rows": facts.height,
        "meters": sorted(facts["meter_id"].unique().to_list()) if facts.height else [],
        "local_dates": (
            sorted(str(d) for d in facts["local_date"].unique().to_list())
            if facts.height else []
        ),
        "energy_wh": float(facts["energy_wh"].sum()) if facts.height else 0.0,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


def cmd_append(args) -> int:
    _trace(args.file, args.line)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy-orchestrator-demo-jobs")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ok")
    p.add_argument("--dir", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--sleep", type=float, default=0.0)
    p.add_argument("--trace", default=None)
    p.add_argument("--trace-line", default=None)
    p.set_defaults(func=cmd_ok)

    p = sub.add_parser("fail")
    p.add_argument("--code", type=int, default=1)
    p.add_argument("--trace", default=None)
    p.add_argument("--trace-line", default=None)
    p.set_defaults(func=cmd_fail)

    p = sub.add_parser("flaky")
    p.add_argument("--state", required=True)
    p.add_argument("--fail-times", type=int, default=1)
    p.add_argument("--dir", default=None)
    p.add_argument("--name", default=None)
    p.add_argument("--trace", default=None)
    p.add_argument("--trace-line", default=None)
    p.set_defaults(func=cmd_flaky)

    p = sub.add_parser("slow")
    p.add_argument("--seconds", type=float, required=True)
    p.add_argument("--dir", required=True)
    p.add_argument("--name", required=True)
    p.set_defaults(func=cmd_slow)

    p = sub.add_parser("spawner")
    p.add_argument("--seconds", type=float, required=True)
    p.add_argument("--dir", required=True)
    p.set_defaults(func=cmd_spawner)

    p = sub.add_parser("collect")
    p.add_argument("--store", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_collect)

    p = sub.add_parser("append")
    p.add_argument("--file", required=True)
    p.add_argument("--line", required=True)
    p.set_defaults(func=cmd_append)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
