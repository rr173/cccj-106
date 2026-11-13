#!/usr/bin/env bash
# OPC UA 在线采集（opccollect）现场断线演练：
#   真实本地 OPC UA 服务（asyncua，非回调桩）-> 独立 SQLite 接收历史
#   -> 另一个进程读回 PyArrow 核对 -> 真实断线/占位 namespace/失败注入 -> 回归。
#
#   ./opccollect_demo.sh                 # 使用 /tmp/minenergy-opccollect-demo
#   ./opccollect_demo.sh /path/to/work   # 自定义工作目录
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${1:-/tmp/minenergy-opccollect-demo}"
PY="${PYTHON:-python}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
export PYTHONPATH="$HERE"

rm -rf "$WORK"
mkdir -p "$WORK"

echo "== 1. 现场断线演练（真实子进程：服务端夹具 + 采集命令 + 跨进程查询，含全部断言）"
"$PY" -m minenergy.opccollect.demo --workdir "$WORK"

echo
echo "== 2. 独立查看连接空档与采集段（只读，可在采集进行时并发执行）"
"$PY" -m minenergy.opccollect.cli inspect --db "$WORK/history.sqlite" | head -60

echo
echo "== 3. opccollect 测试（契约/类型边界、接收语义、断线重连、故障注入、Arrow 读出）"
"$PY" -m pytest "$HERE/tests/opccollect" -q

echo
echo "演练完成，工作目录: $WORK"
