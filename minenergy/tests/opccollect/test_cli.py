"""CLI 端到端测试：真实子进程跑 run/query/inspect/fixture。

这些用例与 demo 走同一条命令路径（``python -m minenergy.opccollect.cli``），
验证参数解析、退出码、机器可读 JSON 与跨进程只读访问。
"""
from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from minenergy.opccollect.demo import (
    ControlClient,
    fixture_cmd,
    query_cmd,
    run_cmd,
    write_config,
)
from minenergy.opccollect.server_fixture import BUSINESS_URI

REPO_ROOT = Path(__file__).resolve().parents[2]


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _Proc:
    def __init__(self, cmd):
        self.proc = subprocess.Popen(
            cmd, cwd=REPO_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def read_ready(self):
        line = self.proc.stdout.readline()
        assert line, f"fixture died: {self.proc.stderr.read()}"
        return json.loads(line)

    def wait(self, timeout=20):
        try:
            out, err = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, err = self.proc.communicate()
            pytest.fail(f"process timed out; stderr={err}")
        return self.proc.returncode, out, err

    def terminate(self):
        self.proc.terminate()


@pytest.fixture
def live_fixture():
    opc_port, control_port = _free_port(), _free_port()
    proc = _Proc(fixture_cmd(opc_port, control_port))
    ready = proc.read_ready()
    yield f"opc.tcp://127.0.0.1:{opc_port}", control_port, ready
    proc.terminate()
    proc.wait()


def _run_json(cmd, expect=0):
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    assert proc.returncode == expect, (
        f"{' '.join(cmd)}\nstdout={proc.stdout}\nstderr={proc.stderr}"
    )
    return json.loads(proc.stdout)


def test_cli_run_query_inspect_roundtrip(tmp_path, live_fixture):
    endpoint, control_port, _ = live_fixture
    config = write_config(tmp_path / "cfg.json", endpoint)
    db = tmp_path / "h.sqlite"

    collector = _Proc(run_cmd(config, db, 3000))
    time.sleep(1.2)
    ctrl = ControlClient(control_port)
    ctrl.write("meter_total", 4242)
    ctrl.write("tags", ["p", "q"])
    code, out, err = collector.wait()
    assert code == 0, err
    summary = json.loads(out)
    assert summary["status"] == "completed"
    assert summary["received_count"] == summary["committed_count"]
    assert summary["complete"] is True

    meter = _run_json(query_cmd(db, "meter_total"))
    assert meter["value_arrow_type"] == "int64"
    assert meter["identity"]["namespace_uri"] == BUSINESS_URI
    assert meter["rows"] >= 2

    tags = _run_json(query_cmd(db, "tags"))
    assert tags["value_arrow_type"] == "list<item: string>"

    inspect = _run_json([
        sys.executable, "-m", "minenergy.opccollect.cli",
        "inspect", "--db", str(db),
    ])
    assert {c["alias"] for c in inspect["contracts"]}
    assert inspect["segments"][0]["pid"] > 0
    assert inspect["segments"][0]["end_kind"] == "completed"
    # 连接空档信息可见
    assert inspect["connection_events"][0]["kind"] == "session_connected"


def test_cli_seq_range_filter_and_parquet_export(tmp_path, live_fixture):
    endpoint, control_port, _ = live_fixture
    config = write_config(tmp_path / "cfg.json", endpoint)
    db = tmp_path / "h.sqlite"
    collector = _Proc(run_cmd(config, db, 2500))
    time.sleep(1.0)
    ctrl = ControlClient(control_port)
    for i in range(5):
        ctrl.write("meter_total", 100 + i)
    code, _, err = collector.wait()
    assert code == 0, err

    # 编号范围过滤
    payload = _run_json(
        query_cmd(db, "meter_total", first_seq=2, last_seq=3)
    )
    assert payload["node_seq_range"] == [2, 3]

    # parquet 导出保留契约 Arrow 类型与 metadata
    out_parquet = tmp_path / "meter.parquet"
    import pyarrow.parquet as pq

    _run_json(query_cmd(
        db, "meter_total", format="parquet", out=out_parquet,
    ))
    table = pq.read_table(out_parquet)
    assert table.schema.field("value").type.__str__() == "int64"
    assert table.schema.metadata[b"opccollect.identifier"] == b"numeric:1001"


def test_cli_bad_config_exits_2(tmp_path, live_fixture):
    endpoint, _, _ = live_fixture
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({
        "endpoint": endpoint,
        "nodes": [{"alias": "x", "namespace_uri": "urn:missing",
                   "identifier": 1, "value_type": "Float"}],
    }))
    proc = subprocess.run(
        [sys.executable, "-m", "minenergy.opccollect.cli", "run",
         "--config", str(bad), "--db", str(tmp_path / "x.sqlite"),
         "--run-ms", "1000"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    assert proc.returncode == 2
    assert "ConfigError" in proc.stderr or "ConfigError" in proc.stdout


def test_cli_uri_missing_failure_is_persisted(tmp_path):
    # 起一个不含业务 URI 的夹具
    opc_port, control_port = _free_port(), _free_port()
    fixture = _Proc(fixture_cmd(
        opc_port, control_port, omit_business_uri=True,
    ))
    fixture.read_ready()
    try:
        endpoint = f"opc.tcp://127.0.0.1:{opc_port}"
        config = write_config(tmp_path / "cfg.json", endpoint)
        db = tmp_path / "h.sqlite"
        proc = subprocess.run(
            run_cmd(config, db, 4000),
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert proc.returncode == 2
        payload = json.loads(proc.stdout)
        assert payload["error_code"] == "UriMissingError"
        # 段已落库为 failed
        inspect = _run_json([
            sys.executable, "-m", "minenergy.opccollect.cli",
            "inspect", "--db", str(db),
        ])
        assert inspect["segments"][-1]["end_kind"] == "failed"
    finally:
        fixture.terminate()
        fixture.wait()
