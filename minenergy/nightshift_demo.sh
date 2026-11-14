#!/usr/bin/env bash
# Night-shift drill for the local data-job orchestrator.
#
#   ./nightshift_demo.sh [/scratch/dir]
#
# Rebuilds everything from an empty directory: a real data chain per date
# (generate -> energy -> verify -> deliver) with a COMTRADE waveform branch
# that fails for one date, then controlled DAG shapes (fork/join, flaky
# retry, permanent-failure isolation, exit-code-only success), timeout and
# cancellation process-group hygiene, crash takeover with adoption and
# interrupted attempts, controller mutual exclusion, and definition safety.
# Every assertion is printed as it is checked; the artefacts (SQLite records,
# per-attempt logs, trace files, delivery manifests) stay in the workdir.
#
# The drill never deletes or hand-edits the orchestration record: every
# recovery goes through `minenergy-orchestrate resume`.
#
# Afterwards the full pytest suite (including the orchestrator tests) runs as
# the project regression.  Exit code 0 means drill + regression all passed.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-nightshift}"
PY="${PYTHON:-python}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
export PYTHONPATH="$HERE${PYTHONPATH:+:$PYTHONPATH}"

echo "== night-shift drill (orchestrator end-to-end)"
"$PY" -m minenergy.orchestrator.demo --workdir "$WORK"

echo
echo "== project regression (pytest, includes tests/orchestrator)"
"$PY" -m pytest "$HERE/tests" -q

echo
echo "RESULT: night-shift drill and project regression both passed"
