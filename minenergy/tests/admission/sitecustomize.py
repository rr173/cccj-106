"""Timing-only barrier loaded by the *test* Flight server subprocess.

The regression suite launches ``python -m minenergy.flight_server`` with this
directory first on ``PYTHONPATH``.  Python imports ``sitecustomize`` at
startup; when ``MINENERGY_ADMISSION_BARRIER_DIR`` is set we install a hook on
``minenergy.flight.admission_hook`` that parks the admission sequence at the
exact failure-A window: ``settle_store`` has finished, but no shared read hold
has been taken yet.

Admissions are counted *per server process* because one socket client crosses
the boundary twice -- once for ``GetFlightInfo``, once for ``DoGet`` -- and
the failure-A regression must exploit the second (transfer) admission's gap.
The n-th park waits for a file named ``release-<n>`` and announces itself via
``reached-<n>``, so the orchestrator can let the metadata admission pass and
arm the writer against the transfer admission.

This arranges *timing only*.  No scan result, directory listing or lock state
is synthesised: once a barrier releases, the real admission code re-decides
everything from the real on-disk transaction directories.  A production
launch sets no environment variable and loads a no-op hook.
"""
from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path


def _install() -> None:
    barrier = os.environ.get("MINENERGY_ADMISSION_BARRIER_DIR")
    if not barrier:
        return
    base = Path(barrier)
    timeout = float(os.environ.get("MINENERGY_ADMISSION_BARRIER_TIMEOUT", "120"))
    counter = {"n": 0}
    lock = threading.Lock()

    def hook(point: str) -> None:
        # Only the settle->hold gap is pausable; loop re-entries inside one
        # admission ("admission-loop-top"/"admission-granted") must not count.
        if point != "after-settle":
            return
        with lock:
            counter["n"] += 1
            n = counter["n"]
        base.mkdir(parents=True, exist_ok=True)
        (base / f"reached-{n}").write_text("1\n", encoding="utf-8")
        deadline = time.monotonic() + timeout
        while not (base / f"release-{n}").exists():
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"admission barrier #{n} timed out waiting for release-{n}"
                )
            time.sleep(0.01)

    from minenergy import flight  # local import: keep startup lazy

    flight.admission_hook = hook


try:
    _install()
except Exception as exc:  # pragma: no cover - fail loudly in test logs
    print(f"sitecustomize admission barrier install failed: {exc!r}", file=sys.stderr)
    raise
