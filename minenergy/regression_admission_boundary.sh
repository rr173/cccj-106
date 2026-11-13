#!/usr/bin/env bash
# Request-admission regression for the Flight fact service.
#
#   ./regression_admission_boundary.sh [/scratch/dir]
#
# Drives the tests in tests/admission/test_admission_boundary.py, each of
# which spawns real subprocesses (a flight_server over a real TCP socket, a
# writer that really runs prepare/commit, self-killing with os._exit(77),
# and socket clients that consume DoGet batch by batch):
#
#   A  settle->hold gap: a publisher replaces the first fact partition and
#      dies (exit 77) before the reader's permit exists; the reader must take
#      the dead publish over and stream one generation, never 10+20 mixed.
#   B  a new meter staged in a dead transaction must be taken over before the
#      UNKNOWN_METER decision; a genuinely unknown meter is still rejected.
#   R1 owner exits normally while a reader waits inside admission
#   R2 owner dies while a reader waits; the reader itself takes the txn over
#   R3 the takeover process is itself killed (exit 77); a later request
#      resumes it from the unchanged manifest/COMMITTED criteria
#   R4 two consumers hold shared read permits concurrently (no deadlock)
#   R5 two readers race one dead-publish takeover: no shared->exclusive
#      upgrade interlock; both settle against the same coherent round
#
# Every child leaves an asserted exit status and every wait has a deadline.
# Exit code 0 means every admission scenario held.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PYTHON:-python3}"
export PYTHONPATH="$HERE${PYTHONPATH:+:$PYTHONPATH}"
WORK="${1:-/tmp/minenergy-admission-regression}"
mkdir -p "$WORK"

echo "== request-admission boundary regression =="
echo "scratch: $WORK"
echo

# pytest owns the per-test temp directories; $WORK holds this run's log.
"$PY" -m pytest "$HERE/tests/admission/test_admission_boundary.py" -v \
  --basetemp="$WORK/pytest-tmp" 2>&1 | tee "$WORK/pytest.log"
status=${PIPESTATUS[0]}

echo
if [ "$status" -eq 0 ]; then
  echo "RESULT: all admission scenarios held (no mixed-revision stream,"
  echo "        dead transactions are taken over before meter decisions,"
  echo "        concurrent readers never deadlock)"
else
  echo "RESULT: FAILURES — see $WORK/pytest.log"
fi
exit "$status"
