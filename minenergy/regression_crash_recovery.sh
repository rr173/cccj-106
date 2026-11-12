#!/usr/bin/env bash
# Crash regression for the cross-file commit protocol.
#
#   ./regression_crash_recovery.sh [/scratch/dir]
#
# Seeds a store with two meters whose intervals cross local-day boundaries
# (several interval files, several fact partitions and one change report per
# job), runs the revision job in a real child process that dies with
# os._exit(77) at each breakpoint, then re-runs the *original* command and
# prints, per breakpoint:
#
#   exit1     raw exit status of the crashed child (77 == crashed as intended)
#   recover   exit/recovery statuses when recovery itself is interrupted
#   committed whether a complete run.json exists for the job
#   noop      whether the retry itself was a business no-op (expected only for
#             crashes after the prepare boundary, where recovery did the work)
#   ok        whether disk (facts, intervals, canonical, raw, partitions and
#             the change report) converged to the uninterrupted clean run
#   detail    retry exit, post-success invocation exit, and that repeated
#             invocation is a no-op
#
# Exit code 0 means every breakpoint converged.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-crash-regression}"
PY="${PYTHON:-python3}"
export PYTHONPATH="$HERE${PYTHONPATH:+:$PYTHONPATH}"
WORKER="$HERE/tests/crash_worker.py"
rm -rf "$WORK"; mkdir -p "$WORK"
FAIL=0

seed_frames() { "$PY" "$WORKER" seed --store "$1/store" --batch-dir "$1/frames" >/dev/null 2>&1; }

# clean reference: the same job runs to completion, uninterrupted.
seed_frames "$WORK/clean"
"$PY" "$WORKER" job --store "$WORK/clean/store" --settings "$WORK/clean/frames/settings.json" \
  --batch "$WORK/clean/frames/revision.parquet" --run-id clean >/dev/null 2>&1
"$PY" "$WORKER" query --store "$WORK/clean/store" > "$WORK/clean/state.json"

# one helper decides convergence against that clean state.
cat > "$WORK/check.py" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
clean = json.load(open(sys.argv[2]))
run_id = sys.argv[3]
problems = []
if state["invariants"]:
    problems.append("invariants:" + ",".join(state["invariants"]))
if state["txn_dirs"]:
    problems.append("leftover-txn-dirs")
for key in ("facts_rows", "interval_rows", "raw_samples", "canonical_samples",
            "canonical_counters", "partitions", "energy_by_meter"):
    if state[key] != clean[key]:
        problems.append(key)
for part, energy in clean["facts_by_partition"].items():
    got = state["facts_by_partition"].get(part)
    if got is None or abs(got - energy) > 1e-6:
        problems.append("partition:" + part)
meta = state["runs"].get(run_id, {})
rep = state["affected"].get(run_id, {})
if not rep.get("has_run_json"):
    problems.append("no-committed-run.json")
if rep.get("intervals_rows") != clean["runs"]["clean"]["affected_intervals"]:
    problems.append("change-report-intervals")
if rep.get("minutes_rows") != clean["runs"]["clean"]["affected_minutes"]:
    problems.append("change-report-minutes")
if meta.get("no_op") and meta.get("reused_commit") is not True:
    problems.append("unfinished-work-reported-as-noop")
print("OK" if not problems else "FAIL:" + ";".join(problems))
PY

printf '%-32s %-6s %-9s %-10s %-5s %-5s %s\n' \
  "breakpoint" "exit1" "recover" "committed" "noop" "ok" "detail"

BREAKPOINTS=(
  raw-after-append after-canonical
  before-first-intervals after-first-intervals
  before-first-facts after-first-facts
  before-affected after-affected
  before-run-meta after-run-meta
  before-prepare after-prepare
  publish-partial:1 publish-partial:2 publish-partial:3
  after-publish-before-marker after-commit
)

read_field() {  # run.json -> "noop=<yes/no>"
  "$PY" - "$1" <<'PY'
import json, os, sys
p = sys.argv[1]
print("yes" if os.path.exists(p) and json.load(open(p)).get("no_op") else "no")
PY
}

for point in "${BREAKPOINTS[@]}"; do
  crash_at="${point%%:*}"
  after="${point#*:}"; [ "$after" = "$point" ] && after=1
  run_id="crash-${point//:/_}"
  dir="$WORK/$run_id"
  mkdir -p "$dir"
  seed_frames "$dir"

  "$PY" "$WORKER" job --store "$dir/store" --settings "$dir/frames/settings.json" \
    --batch "$dir/frames/revision.parquet" --run-id "$run_id" \
    --crash-at "$crash_at" --after "$after" >/dev/null 2>&1
  exit1=$?

  recover_status="-"
  if [ "$crash_at" = "publish-partial" ] || [ "$crash_at" = "after-publish-before-marker" ]; then
    # Exercise recovery being interrupted as well, then resumed by another process.
    "$PY" "$WORKER" recover --store "$dir/store" --crash-at recovery-partial --after 1 >/dev/null 2>&1
    rec1=$?
    "$PY" "$WORKER" recover --store "$dir/store" >/dev/null 2>&1
    rec2=$?
    recover_status="$rec1/$rec2"
  fi

  # Re-run exactly the original command in a fresh process.
  "$PY" "$WORKER" job --store "$dir/store" --settings "$dir/frames/settings.json" \
    --batch "$dir/frames/revision.parquet" --run-id "$run_id" >/dev/null 2>&1
  retry=$?

  committed="-"; noop="-"
  if [ -f "$dir/store/affected/$run_id/run.json" ]; then
    committed="yes"
    noop=$(read_field "$dir/store/affected/$run_id/run.json")
  fi

  # A further invocation after the successful commit must be a business no-op.
  "$PY" "$WORKER" job --store "$dir/store" --settings "$dir/frames/settings.json" \
    --batch "$dir/frames/revision.parquet" --run-id after-ok >/dev/null 2>&1
  after_status=$?
  noop2=$(read_field "$dir/store/affected/after-ok/run.json")

  "$PY" "$WORKER" query --store "$dir/store" > "$dir/state.json"
  check=$("$PY" "$WORK/check.py" "$dir/state.json" "$WORK/clean/state.json" "$run_id")

  status="ok"; [ "$check" = "OK" ] || status="FAIL"
  detail="retry_exit=$retry after_exit=$after_status second_noop=$noop2"
  printf '%-32s %-6s %-9s %-10s %-5s %-5s %s\n' \
    "$point" "$exit1" "$recover_status" "$committed" "$noop" "$status" "$detail"
  if [ "$exit1" != "77" ] || [ "$status" = "FAIL" ] || [ "$noop2" != "yes" ] \
     || [ "$retry" != "0" ] || [ "$after_status" != "0" ]; then
    FAIL=1
    echo "    $check"
  fi
done

echo
if [ "$FAIL" -eq 0 ]; then
  echo "RESULT: all breakpoints converged to the clean committed state"
else
  echo "RESULT: FAILURES — see the rows marked FAIL above"
fi
exit "$FAIL"
