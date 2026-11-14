"""Per-attempt process wrapper: timeout enforcement and outcome recording.

The controller never launches the processing command directly; it launches
this wrapper (``python -m minenergy.orchestrator.wrapper``), which:

1. spawns the worker in its **own session** (new process group, pgid = worker
   pid) so the worker and anything *it* spawns can be terminated as one group;
2. atomically writes a **meta file** with the wrapper's and the worker's
   ``(pid, starttime)`` identities and the worker pgid — the identity tokens a
   later controller uses to recognise these processes without trusting a bare
   PID;
3. waits for the worker, enforcing the timeout: on expiry it SIGTERMs the
   worker's process group, waits out the grace period, then SIGKILLs it;
4. atomically writes a **result file** with the worker's real exit code or
   terminating signal — the only accepted proof of how the attempt ended.

The result file is the commit point of an attempt.  If the wrapper dies
before writing it, the outcome is unknowable and the attempt must be recorded
as interrupted — never guessed.

Usage::

    python -m minenergy.orchestrator.wrapper \
        --result PATH --meta PATH --timeout SECONDS --grace SECONDS \
        --cwd DIR --stdout PATH --stderr PATH -- ARGV...
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _read_starttime(pid: int) -> str | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    close = text.rfind(")")
    if close < 0:
        return None
    fields = text[close + 1 :].split()
    return fields[19] if len(fields) > 19 else None


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _parse_args(argv: list[str]) -> tuple[dict, list[str]]:
    if "--" not in argv:
        raise SystemExit("wrapper: missing '--' before the worker argv")
    sep = argv.index("--")
    opts, worker = argv[:sep], argv[sep + 1 :]
    if not worker:
        raise SystemExit("wrapper: empty worker argv")
    parsed: dict[str, str] = {}
    key = None
    for token in opts:
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
        elif key is not None:
            parsed[key] = token
            key = None
        else:
            raise SystemExit(f"wrapper: stray option value {token!r}")
    for required in ("result", "meta", "timeout", "cwd", "stdout", "stderr"):
        if required not in parsed:
            raise SystemExit(f"wrapper: missing --{required}")
    return parsed, worker


def main(argv: list[str] | None = None) -> int:
    opts, worker_argv = _parse_args(list(sys.argv[1:] if argv is None else argv))
    result_path = Path(opts["result"])
    meta_path = Path(opts["meta"])
    timeout = float(opts["timeout"])
    grace = float(opts.get("grace", "3.0"))

    out_path = Path(opts["stdout"])
    err_path = Path(opts["stderr"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    err_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = _utc_now()
    t0 = time.monotonic()
    with open(out_path, "wb") as out_fh, open(err_path, "wb") as err_fh:
        proc = subprocess.Popen(
            worker_argv,
            cwd=opts["cwd"],
            stdin=subprocess.DEVNULL,
            stdout=out_fh,
            stderr=err_fh,
            start_new_session=True,  # worker leads its own process group
        )
        # Publish identities before anything can go wrong: a controller that
        # loses track of this wrapper can still find the worker group.
        _atomic_write_json(
            meta_path,
            {
                "wrapper_pid": os.getpid(),
                "wrapper_pid_start": _read_starttime(os.getpid()),
                "worker_pid": proc.pid,
                "worker_pgid": proc.pid,  # session leader: pgid == pid
                "worker_pid_start": _read_starttime(proc.pid),
                "argv": worker_argv,
                "cwd": opts["cwd"],
                "started_at": started_at,
            },
        )

        timed_out = False
        returncode: int | None = None
        deadline = t0 + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            try:
                returncode = proc.wait(timeout=min(remaining, 0.5))
                break
            except subprocess.TimeoutExpired:
                continue
        if timed_out:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                returncode = proc.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                returncode = proc.wait()

    ended_at = _utc_now()
    assert returncode is not None
    result = {
        "exit_code": returncode if returncode >= 0 else None,
        "signal": -returncode if returncode < 0 else None,
        "timed_out": timed_out,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": round(time.monotonic() - t0, 6),
    }
    # The result file is the attempt's commit point; write it atomically.
    _atomic_write_json(result_path, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
