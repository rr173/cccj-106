#!/usr/bin/env bash
# Arrow Flight acceptance/demo flow: build a real store, start the real
# server on a real port, then drive it with the demo client.
#
#   ./flight_demo.sh                 # uses /tmp/minenergy-flight-demo
#   ./flight_demo.sh /path/to/work   # custom scratch dir
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-flight-demo}"
PY="${PYTHON:-python3}"
export PYTHONPATH="$HERE"

echo "== building a store that spans many partitions, row groups and batches"
"$PY" -m minenergy.flight_demo_data --out "$WORK" --force

STORE="$WORK/store"
READY="$WORK/server.ready"
rm -f "$READY"
echo "== starting the Flight server in the foreground (background here)"
"$PY" -u -m minenergy.flight_server --store "$STORE" >"$READY" 2>"$WORK/server.err" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# wait for the ready line, then read the actual port
for _ in $(seq 1 200); do
  if [ -s "$READY" ] && head -c 1 "$READY" | grep -q '{'; then break; fi
  sleep 0.05
done
URI=$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["location"])' "$READY")
echo "   server: $URI (pid $SERVER_PID)"

echo "== discover the fact schema"
"$PY" -m minenergy.flight_client --uri "$URI" --schema

echo "== narrow range with a column projection"
"$PY" -m minenergy.flight_client --uri "$URI" \
  --meters M0 --start 2024-01-02T03:00:00Z --end 2024-01-02T04:00:00Z \
  --select meter_id minute_start_utc energy_wh valid_seconds --batch-size 20

echo "== empty result still reports the requested schema"
"$PY" -m minenergy.flight_client --uri "$URI" \
  --meters M0 --start 1999-01-01T00:00:00Z --end 1999-01-02T00:00:00Z \
  --select meter_id energy_wh --json | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print("   rows=", d["rows"], "columns=", [c["name"] for c in d["schema"]])'

echo "== rejected parameter (unknown column) is an INVALID_ARGUMENT RPC error"
"$PY" - "$URI" <<'PY'
import sys, json
from minenergy import flight_client as fc
import pyarrow.flight as fl
uri = sys.argv[1]
client = fc.connect(uri)
res = fc.expect_param_error(
    lambda: client.do_get(
        fl.Ticket(b'{"meters":["M0"],"start":"2024-01-01T00:00:00Z",'
                  b'"end":"2024-01-02T00:00:00Z","select":["nope"]}')
    ).read_all()
)
print("   ", res["error_type"], "| param-error:", res["is_param_error"],
      "| INVALID_ARGUMENT:", res["grpc_invalid_argument"])
client.close()
PY

echo "== two consumers on different meters run concurrently"
"$PY" - "$URI" <<'PY'
import sys, json, threading
from minenergy import flight_client as fc
uri = sys.argv[1]
client = fc.connect(uri)
out = {}
def run(name, meter):
    out[name] = fc.query_records(client, {
        "meters": [meter],
        "start": "2024-01-01T00:00:00Z", "end": "2024-01-02T00:00:00Z",
        "select": ["meter_id", "energy_wh"], "batch_size": 512,
    })
threads = [threading.Thread(target=run, args=(m, m)) for m in ("M0", "M2")]
for t in threads: t.start()
for t in threads: t.join()
for m in ("M0", "M2"):
    meters = {r["meter_id"] for r in out[m]["data"]}
    print(f"    {m}: rows={out[m]['rows']} meters_seen={sorted(meters)}")
client.close()
PY

echo "== server-side scan observations (per query)"
"$PY" - "$URI" <<'PY'
import sys, json
from minenergy import flight_client as fc
client = fc.connect(sys.argv[1])
stats = fc.server_stats(client)
fields = ["meters", "files_considered", "files_pruned_by_date", "files_opened",
          "files_scanned", "row_groups_total", "row_groups_selected",
          "row_groups_pruned", "batches", "rows_returned", "bytes_returned",
          "peak_batch_bytes", "peak_memory_bytes"]
for rec in stats["recent_queries"]:
    print("   ", json.dumps({k: rec[k] for k in fields}, default=str))
print("   open parquet files:", stats["open_parquet_files"], "| active:",
      len(stats["active_queries"]), "| server peak RSS:",
      stats["peak_memory_bytes"])
client.close()
PY

echo
echo "== all good; stopping the server"
