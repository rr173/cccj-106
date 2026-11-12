#!/usr/bin/env bash
# Reproduce the full acceptance flow from a clean state.
#
#   ./demo.sh                 # uses /tmp/minenergy-demo
#   ./demo.sh /path/to/work   # custom scratch dir
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-demo}"
PY="${PYTHON:-python}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
export PYTHONPATH="$HERE"

echo "== generating unordered multi-batch input"
"$PY" -m minenergy.generate --out "$WORK/samples" --scenario randomized \
  --seed 7 --batches 4 --meters 3 --points 40 >/dev/null

echo "== generating the six exact fixtures"
"$PY" -m minenergy.generate --out "$WORK/fixtures" --scenario all --seed 7 >/dev/null

echo "== full run (first batch)"
"$PY" -m minenergy.cli full --store "$WORK/store" --settings "$WORK/samples/settings.json" \
  --samples "$WORK/samples/batches/batch_00.parquet" \
  --configs "$WORK/samples/configs_all.parquet"

echo "== incremental runs (remaining batches)"
"$PY" -m minenergy.cli incremental --store "$WORK/store" --settings "$WORK/samples/settings.json" \
  --samples "$WORK/samples/batches/batch_01.parquet" \
            "$WORK/samples/batches/batch_02.parquet" \
            "$WORK/samples/batches/batch_03.parquet"

echo "== verify against the independent reference implementation"
"$PY" -m minenergy.cli verify --store "$WORK/store" --settings "$WORK/samples/settings.json"

echo "== three-way comparison (incremental / full replay / reference)"
"$PY" -m minenergy.compare --store "$WORK/compare" --settings "$WORK/samples/settings.json" \
  --configs "$WORK/samples/configs_all.parquet" \
  --batches "$WORK"/samples/batches/*.parquet

echo "== fixtures: each is verified against the reference"
for d in "$WORK"/fixtures/*/; do
  name=$(basename "$d")
  "$PY" -m minenergy.ingest --store "$WORK/fixture_stores/$name" --settings "$d/settings.json" \
    --batch-dir "$d/batches" --configs "$d/configs_all.parquet" --fresh >/dev/null
  "$PY" -m minenergy.cli verify --store "$WORK/fixture_stores/$name" --settings "$d/settings.json" \
    | sed "s/^/  $name: /"
done

echo "== pytest suite"
"$PY" -m pytest "$HERE/tests" -q
