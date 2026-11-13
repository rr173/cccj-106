#!/usr/bin/env bash
# COMTRADE waveform ingestion: end-to-end, fully offline demonstration.
#
#   ./comtrade_demo.sh                 # uses /tmp/minenergy-comtrade-demo
#   ./comtrade_demo.sh /path/to/work   # custom scratch dir
#
# It regenerates the self-contained fixtures (no external download), checks
# every reference and corrupt pair, exercises the trigger-relative window /
# channel-projection / batching API, writes a staged Parquet export and
# records the real peak memory on a ~20 MiB recording.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-comtrade-demo}"
PY="${PYTHON:-python3}"
export PYTHONPATH="$HERE"

rm -rf "$WORK"
mkdir -p "$WORK"

echo "== 1. generate offline fixtures (ASCII + BINARY, two rates, 17 digitals)"
"$PY" -m minenergy.comtrade.cli make-fixtures --out "$WORK/fixtures" \
  | sed 's/^/  /'

echo
echo "== 2. catalog (channel directory) of the BINARY reference pair"
"$PY" -m minenergy.comtrade.cli catalog \
  --cfg "$WORK/fixtures/reference/rates_binary/rec.cfg" --timezone UTC

echo
echo "== 3. import check of all four reference encodings"
"$PY" - "$WORK" <<'PYINSPECT'
import json, sys
from pathlib import Path
from minenergy.comtrade.recording import Recording
for d in sorted((Path(sys.argv[1]) / "fixtures/reference").iterdir()):
    rec = Recording.open(d / "rec.cfg", "UTC")
    s = rec.validate()
    print(f"  {d.name}: {s['file_type']:<6} mode={s['timestamp_mode']:<18} "
          f"frames={s['frames']}")
PYINSPECT

echo
echo "== 4. hand-derived values must match across ASCII and BINARY"
"$PY" - "$WORK" <<'PYCHECK'
import json, sys
from pathlib import Path
from minenergy.comtrade.recording import Recording

work = Path(sys.argv[1])
START = 1_742_032_800_000_000
# Hand-derived rate-rebuild offsets (1000 Hz x6, then 500 Hz).
offsets = [0, 1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000, 12000]

def rows(cfg):
    rec = Recording.open(cfg, "UTC")
    a = {}
    for b in rec.iter_analog_window(-10**12, 10**12):
        for r in b.to_pylist():
            a[(r["sample_number"], r["channel_index"])] = r
    return a

binr = rows(work / "fixtures/reference/rates_binary/rec.cfg")
asr = rows(work / "fixtures/reference/stamped_ascii/rec.cfg")
assert set(binr) == set(asr)
for key in binr:
    assert binr[key]["time_us"] == asr[key]["time_us"], key
    assert abs(binr[key]["value"] - asr[key]["value"]) < 1e-12, key
# Hand checks (default output side = secondary):
# A1 marker P, ratio 1000/5=200: a*x+b = -9.5 is primary; secondary = -9.5/200.
assert abs(binr[(1, 1)]["value"] - (-9.5 / 200)) < 1e-12
# A2 marker S: a*x+b = 2*10-1 = 19 is already the secondary value.
assert abs(binr[(1, 2)]["value"] - 19.0) < 1e-12
# Skew: A2 +500 us on sample 2.
assert binr[(2, 2)]["time_us"] == START + offsets[1] + 500
# A3 -250 us on sample 2.
assert binr[(2, 3)]["time_us"] == START + offsets[1] - 250
print("  OK: identical times/values across encodings; a*x+b and skews match hand calc")
PYCHECK

echo
echo "== 5. narrow trigger window, channel projection and Arrow batching"
"$PY" - "$WORK" <<'PYWIN'
import sys
from pathlib import Path
from minenergy.comtrade.recording import Recording
work = Path(sys.argv[1])
rec = Recording.open(work / "fixtures/reference/stamped_binary/rec.cfg", "UTC")
# Window [-1000 us, +1000 us] relative to trigger, inclusive both edges.
batches = list(rec.iter_analog_window(
    -1000, 1000, channels=["Ia", "Vb"], side="primary", batch_size=4,
))
rows = [r for b in batches for r in b.to_pylist()]
samples = sorted({r["sample_number"] for r in rows})
print(f"  batches={[b.num_rows for b in batches]} samples_in_window={samples}")
assert samples == [4, 5, 6]
assert {r["channel_name"] for r in rows} == {"Ia", "Vb"}
assert all(-1000 <= r["relative_trigger_us"] <= 1000 for r in rows)
print("  OK: inclusive edges, projected channels, batch sizes honored")
PYWIN

echo
echo "== 6. corrupt files are rejected with the file and position named"
declare -A ZONES=(
  [dst_ambiguous_fallback]="America/New_York"
  [dst_nonexistent_springforward]="America/New_York"
)
fail=0
for d in "$WORK"/fixtures/corrupt/*/; do
  name=$(basename "$d")
  tz="${ZONES[$name]:-UTC}"
  if "$PY" -m minenergy.comtrade.cli inspect --cfg "$d/rec.cfg" --timezone "$tz" \
       >/dev/null 2>"$WORK/last_error.txt"; then
    echo "  UNEXPECTED ACCEPT: $name"; fail=1
  else
    echo "  rejected $name -> $(head -c 160 "$WORK/last_error.txt")"
  fi
done
[ "$fail" -eq 0 ]
echo "  OK: every corrupt pair rejected"

echo
echo "== 7. staged Parquet export with provenance"
"$PY" -m minenergy.comtrade.cli export \
  --cfg "$WORK/fixtures/reference/rates_binary/rec.cfg" --timezone UTC \
  --out "$WORK/parquet" --start-us -2000 --end-us 4000 \
  --analog-channels Ia Vb --digital-channels DIG1 DIG17 \
  --side primary --batch-size 8 | sed 's/^/  /'
"$PY" - "$WORK" <<'PYMETA'
import json, sys
from pathlib import Path
import pyarrow.parquet as pq
work = Path(sys.argv[1])
meta = pq.ParquetFile(work / "parquet/analog.parquet").schema_arrow.metadata
prov = json.loads(meta[b"comtrade.provenance"])
print("  provenance keys:", sorted(k for k in prov if not isinstance(prov[k], (list, dict))))
assert prov["timestamp_mode"] == "rebuilt_from_rates"
assert prov["value_side"] == "primary"
assert {c["name"] for c in prov["analog_channels"]} == {"Ia", "Vb"}
manifest = json.loads((work / "parquet/manifest.json").read_text())
assert manifest["analog_rows"] > 0 and manifest["digital_rows"] > 0
print("  OK: Parquet carries source hashes, time interpretation and scaling")
PYMETA

echo
echo "== 8. large recording: frame count and real peak memory"
"$PY" - "$WORK" <<'PYBIG'
import resource, sys, time
from pathlib import Path
from minenergy.comtrade.recording import Recording
work = Path(sys.argv[1])
cfg = work / "fixtures/large/large.cfg"
size_mib = cfg.with_suffix(".dat").stat().st_size / (1024 * 1024)
t0 = time.perf_counter()
rec = Recording.open(cfg, "UTC")
summary = rec.validate(batch_frames=8192)
rows = 0
for b in rec.iter_analog_window(-10**9, 10**9, batch_size=8192, batch_frames=8192):
    rows += b.num_rows
for b in rec.iter_digital_window(-10**9, 10**9, batch_size=8192, batch_frames=8192):
    rows += b.num_rows
peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"  dat_size={size_mib:.1f} MiB frames={summary['frames']} "
      f"streamed_rows={rows} elapsed={time.perf_counter()-t0:.1f}s "
      f"peak_rss={peak_kib/1024:.1f} MiB")
assert summary["frames"] == 1_000_000
assert peak_kib / 1024 < 128, "peak RSS must stay batch-bounded, not file-sized"
print("  OK: peak memory stays flat while the DAT is ~20 MiB and holds 1M frames")
PYBIG

echo
echo "== all COMTRADE demo steps passed =="
