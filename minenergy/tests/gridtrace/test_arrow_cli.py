"""PyArrow 接出、Parquet 落盘与 CLI 端到端核对。"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from minenergy.gridtrace.arrow import (
    DRILL_SCHEMA,
    HISTORY_SCHEMA,
    ROUTE_SCHEMA,
    drill_to_arrow,
    history_to_arrow,
    routes_to_arrow,
    write_parquet,
)
from minenergy.gridtrace.cli import main as cli_main
from minenergy.gridtrace.loader import ledger_from_dict, model_from_dict
from minenergy.gridtrace.scenario import (
    T1, T4, T7, build_ledger_dict, build_model_dict, write_case_files,
)
from minenergy.gridtrace.snapshot import Replay
from minenergy.gridtrace.tracing import drill, source_history, trace_routes


@pytest.fixture(scope="module")
def replay():
    model = model_from_dict(build_model_dict())
    ledger = ledger_from_dict(build_ledger_dict(), model)
    return Replay(model, ledger)


def test_history_arrow_schema_and_parquet_roundtrip(replay, tmp_path):
    segments = source_history(replay, "M3", T1, T7)
    table = history_to_arrow("M3", segments)
    assert table.schema.equals(HISTORY_SCHEMA)
    assert table.num_rows == len(segments)
    path = write_parquet(table, tmp_path / "history.parquet")
    back = pq.read_table(path)
    assert back.schema.equals(HISTORY_SCHEMA)
    assert back.column("source_ids").to_pylist() == [list(s.sources) for s in segments]
    # 失电段 powered=False 且来源为空列表
    dead_rows = back.filter(pa_equal(back, "powered", False)).to_pylist()
    assert dead_rows and all(row["source_ids"] == [] for row in dead_rows)


def test_routes_arrow_and_drill_arrow(replay, tmp_path):
    routes = trace_routes(replay, "M5", T4)
    route_table = routes_to_arrow("M5", replay.at(_us(T4)).epoch_us, routes)
    assert route_table.schema.equals(ROUTE_SCHEMA)
    assert route_table.num_rows == 2  # SA、SB 两源可达

    result = drill(replay, T4, remove_lines=("P1", "P2"))
    drill_table = drill_to_arrow(result)
    assert drill_table.schema.equals(DRILL_SCHEMA)
    assert drill_table.num_rows == len(result.rows)
    path = write_parquet(drill_table, tmp_path / "drill.parquet")
    back = pq.read_table(path).to_pylist()
    m5 = next(row for row in back if row["meter_id"] == "M5")
    assert m5["category"] == "dead"
    assert m5["after_sources"] == []
    assert m5["removed_lines"] == ["P1", "P2"]


def test_load_from_local_json_files(tmp_path):
    paths = write_case_files(tmp_path / "case")
    from minenergy.gridtrace.loader import load_ledger, load_model
    model = load_model(paths["model"])
    ledger = load_ledger(paths["ledger"], model)
    replay = Replay(model, ledger)
    assert {s.sources for s in source_history(replay, "M1", T1, T4)}
    # JSON 文件就是普通本地文件，可被标准库直接重读
    json.loads(paths["model"].read_text(encoding="utf-8"))


def test_cli_history_trace_drill_json(tmp_path, capsys):
    paths = write_case_files(tmp_path / "case")
    rc = cli_main(["history", "--model", str(paths["model"]),
                   "--ledger", str(paths["ledger"]), "--meter", "M3",
                   "--start", T1, "--end", T7, "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert any(not row["powered"] for row in payload)  # 含失电段
    assert payload[-1]["end"].endswith("Z")

    rc = cli_main(["trace", "--model", str(paths["model"]),
                   "--ledger", str(paths["ledger"]), "--meter", "M6",
                   "--at", "2026-01-01T05:30:00Z", "--json"])
    routes = json.loads(capsys.readouterr().out)
    sa = next(r for r in routes if r["source"] == "SA")
    assert sa["edges"] == ["E7"]
    assert sa["node_names"] == ["4号接点", "1号母线"]

    rc = cli_main(["drill", "--model", str(paths["model"]),
                   "--ledger", str(paths["ledger"]),
                   "--at", T4, "--remove-lines", "P1", "P2", "--json"])
    drill_payload = json.loads(capsys.readouterr().out)
    m5 = next(r for r in drill_payload["rows"] if r["meter"] == "M5")
    assert m5["category"] == "dead"


def test_cli_parquet_outputs(tmp_path):
    paths = write_case_files(tmp_path / "case")
    out = tmp_path / "h.parquet"
    rc = cli_main(["history", "--model", str(paths["model"]),
                   "--ledger", str(paths["ledger"]), "--meter", "M3",
                   "--start", T1, "--end", T7, "--parquet", str(out)])
    assert rc == 0 and out.exists()
    assert pq.read_table(out).num_rows > 0


def test_cli_demo_rebuilds_in_fresh_tempdir(tmp_path):
    """一条命令在新临时目录重建案例并跑完四工单，附 Parquet 产物。"""
    work = tmp_path / "demo"
    pdir = tmp_path / "parquet"
    rc = cli_main(["demo", "--work", str(work), "--parquet-dir", str(pdir)])
    assert rc == 0
    assert (work / "case" / "grid_model.json").exists()
    assert (work / "case" / "operations.json").exists()
    for name in ("history_m3.parquet", "routes_m5.parquet", "drill.parquet"):
        assert (pdir / name).exists()
        assert pq.read_table(pdir / name).num_rows > 0


def test_cli_make_case_and_bad_input_exit_code(tmp_path, capsys):
    out = tmp_path / "freshcase"
    rc = cli_main(["make-case", "--out", str(out)])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert Path(payload["model"]).exists()

    # 坏输入退出码 2，错误信息带定位
    bad = tmp_path / "bad_model.json"
    bad.write_text(json.dumps({"base_time": "2026-01-01T00:00:00",
                               "nodes": [], "lines": [], "sources": [],
                               "meters": []}), encoding="utf-8")
    rc = cli_main(["history", "--model", str(bad),
                   "--ledger", str(out / "operations.json"),
                   "--meter", "M3", "--start", T1, "--end", T7])
    assert rc == 2
    err = capsys.readouterr().err
    assert "base_time" in err and "缺少 UTC 偏移" in err


def test_cli_module_invocation_subprocess(tmp_path):
    """以 `python -m minenergy.gridtrace.cli` 真实子进程方式核对演示入口。"""
    work = tmp_path / "sub"
    proc = subprocess.run(
        [sys.executable, "-m", "minenergy.gridtrace.cli", "demo",
         "--work", str(work)],
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "工单" in proc.stdout and "无污染" in proc.stdout


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def _us(text: str) -> int:
    from minenergy.gridtrace.timeutil import parse_offset_time
    return parse_offset_time(text, location="test")


def pa_equal(table, column, value):
    import pyarrow.compute as pc
    return pc.equal(table[column], value)
