#!/usr/bin/env bash
# 配电网追溯（gridtrace）值班验收演示：
# 在一个全新临时目录里重建案例 -> 四个工单终端核对 -> Parquet 落盘 -> pytest。
#
#   ./gridtrace_demo.sh                 # 使用 /tmp/minenergy-gridtrace-demo
#   ./gridtrace_demo.sh /path/to/work   # 自定义工作目录
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-gridtrace-demo}"
PY="${PYTHON:-python}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
export PYTHONPATH="$HERE"

rm -rf "$WORK"
mkdir -p "$WORK"

echo "== 1. 在新目录重建值班案例（模型 + 操作簿 JSON）"
"$PY" -m minenergy.gridtrace.cli make-case --out "$WORK/case"

echo
echo "== 2. 四个工单一条命令演示（含演练无污染核对），并落 Parquet"
"$PY" -m minenergy.gridtrace.cli demo --work "$WORK/demo" \
      --parquet-dir "$WORK/parquet"

echo
echo "== 3. 三个业务操作的独立终端核对"
"$PY" -m minenergy.gridtrace.cli history \
      --model "$WORK/case/grid_model.json" --ledger "$WORK/case/operations.json" \
      --meter M9 --start 2026-01-01T00:00:00Z --end 2026-01-01T07:00:00Z

"$PY" -m minenergy.gridtrace.cli drill \
      --model "$WORK/case/grid_model.json" --ledger "$WORK/case/operations.json" \
      --at 2026-01-01T04:30:00Z --remove-lines P1 P2

echo
echo "== 4. gridtrace 测试（手算工单 / 拒绝案例 / 随机独立对照 / Arrow+CLI）"
"$PY" -m pytest "$HERE/tests/gridtrace" -q

echo
echo "演示完成，工作目录: $WORK"
