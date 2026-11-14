"""Linux process identification and process-group termination.

Identity boundary (declared, Linux-specific): a process is identified by
``(pid, starttime)`` where ``starttime`` is field 22 of ``/proc/<pid>/stat``
(jiffies after boot).  A bare PID is never trusted, because the kernel recycles
PIDs; the start-time token makes a recycled PID fail the identity check.  This
is *not* a distributed primitive: it is reliable for recognising local
processes on one Linux host, which is all this orchestrator claims.

Process-group termination: workers are always started in their own session
(``start_new_session=True``), so the worker and every child it spawns stays in
one process group and ``os.killpg`` reaches all of them — cancelling a run
never leaves a spawned grandchild writing behind a dead parent.
"""
from __future__ import annotations

import os
import signal
import time
from pathlib import Path


def read_starttime(pid: int) -> str | None:
    """Field 22 of /proc/<pid>/stat, or None if the process is gone.

    The comm field (field 2) may contain spaces and parentheses, so parsing
    resumes after the last ')'.
    """
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    close = text.rfind(")")
    if close < 0:
        return None
    fields = text[close + 1 :].split()
    # fields[0] is now field 3 (state); field 22 (starttime) is index 19.
    if len(fields) <= 19:
        return None
    return fields[19]


def read_state(pid: int) -> str | None:
    """Field 3 of /proc/<pid>/stat (R, S, Z, …), or None if gone."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    close = text.rfind(")")
    if close < 0:
        return None
    fields = text[close + 1 :].split()
    return fields[0] if fields else None


def pid_alive(pid: int | None, starttime: str | None) -> bool:
    """True iff a process with exactly this (pid, starttime) identity exists
    and is not a zombie (a zombie runs no code and is dead for our purposes)."""
    if pid is None or pid <= 0 or not starttime:
        return False
    current = read_starttime(pid)
    if current is None or current != starttime:
        return False
    return read_state(pid) != "Z"


def pgid_alive(pgid: int | None) -> bool:
    """True iff any process in the group exists (os.killpg probe)."""
    if pgid is None or pgid <= 0:
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def process_group_alive(pgid: int | None, known_pid: int | None, known_start: str | None) -> bool:
    """Group-liveness check hardened against PGID reuse.

    The group counts as alive only if the group exists *and* the recorded
    leader identity still matches (or no leader identity was recorded).
    """
    if not pgid_alive(pgid):
        return False
    if known_pid is None or not known_start:
        return True
    return pid_alive(known_pid, known_start)


def terminate_group(
    pgid: int | None,
    grace_seconds: float,
    extra_pids: list[tuple[int | None, str | None]] | None = None,
    leader: tuple[int | None, str | None] | None = None,
) -> None:
    """SIGTERM the group, wait up to ``grace_seconds``, then SIGKILL survivors.

    ``extra_pids`` are additional individually identified processes (e.g. the
    wrapper, which lives in its own group) that get the same TERM→KILL
    treatment.  ``leader`` is the group leader's (pid, starttime) identity;
    when given, group liveness is judged zombie-aware through it.  Never
    raises: termination is best-effort and the caller records the outcome.
    """
    targets = [(pid, start) for pid, start in (extra_pids or []) if pid]

    def group_gone() -> bool:
        if not pgid:
            return True
        if leader and leader[0]:
            return not process_group_alive(pgid, leader[0], leader[1])
        return not pgid_alive(pgid)

    if pgid and pgid_alive(pgid):
        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    for pid, _start in targets:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

    deadline = time.monotonic() + max(grace_seconds, 0.0)
    while time.monotonic() < deadline:
        if group_gone() and all(not pid_alive(pid, start) for pid, start in targets):
            return
        time.sleep(0.02)

    if pgid and pgid_alive(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    for pid, _start in targets:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    # Give the kernel a moment to reap after SIGKILL.
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if group_gone() and all(not pid_alive(pid, start) for pid, start in targets):
            return
        time.sleep(0.02)
