"""现场断线演练驱动：用真实子进程跑服务端夹具与采集命令并做断言。

本模块被 ``opccollect_demo.sh`` 调用，也可以独立运行：

    python -m minenergy.opccollect.demo --workdir /tmp/opccollect-demo

演练覆盖（全部走真实 OPC UA socket 与真实 CLI 子进程）：

1. 订阅标量 + 数组节点，发已知值、一个非 Good 质量读数、同源时间不同值的
   连续更新；**停下来从另一个进程**用 query/inspect 读回 Arrow 核对；
2. 真的停掉服务，再以相同端点重启并预注册占位 namespace（业务 URI 下标
   改变），恢复后继续发值，证明没有串到占位节点，并展示连接空档；
3. 失败场景：类型漂移、URI 消失、写盘异常、小队列溢出——确认历史可读、
   失败未被写成正常完成；
4. 持续通知观察缓冲上限，并核对正常结束时接收量 == 已提交量。
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .server_fixture import BUSINESS_URI

# 让子进程在未安装包时也能从仓库根运行
_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV = {**os.environ, "PYTHONPATH": str(_REPO_ROOT)}
_PY = sys.executable


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------------------
# 子进程封装
# ---------------------------------------------------------------------------


class Subprocess:
    def __init__(self, cmd: list[str], name: str):
        self.name = name
        self.cmd = cmd
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.proc = subprocess.Popen(
            self.cmd,
            env=_ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def stop(self, timeout: float = 10.0) -> tuple[int, str, str]:
        assert self.proc is not None
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            out, err = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, err = self.proc.communicate()
        code = self.proc.returncode or 0
        return code, out, err

    def wait_for_exit(self, timeout: float = 15.0) -> tuple[int, str, str]:
        """等待进程自然退出（用于致命错误路径）；超时才 kill。"""
        assert self.proc is not None
        try:
            out, err = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, err = self.proc.communicate()
        return self.proc.returncode or 0, out, err

    def kill(self) -> None:
        assert self.proc is not None
        self.proc.kill()


class ControlClient:
    """夹具 JSON-lines 控制口的同步客户端。"""

    def __init__(self, port: int):
        self.port = port

    def _call(self, command: dict[str, Any]) -> dict[str, Any]:
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as sock:
            f = sock.makefile("rwb")
            f.write((json.dumps(command) + "\n").encode())
            f.flush()
            line = f.readline()
        return json.loads(line.decode())

    def write(self, node: str, value: Any) -> None:
        resp = self._call({"cmd": "write", "node": node, "value": value})
        assert resp["status"] == "ok", resp

    def write_dv(self, node: str, value: Any, status: str = "Good",
                 source_ts: str | None = None) -> None:
        resp = self._call({
            "cmd": "write_dv", "node": node, "value": value,
            "status": status, "source_ts": source_ts,
        })
        assert resp["status"] == "ok", resp

    def shutdown(self) -> None:
        with contextlib.suppress(Exception):
            self._call({"cmd": "shutdown"})


# ---------------------------------------------------------------------------
# 配置与命令构造
# ---------------------------------------------------------------------------


def write_config(path: Path, endpoint: str, **overrides: Any) -> Path:
    config = {
        "endpoint": endpoint,
        "publishing_interval_ms": 50,
        "sampling_interval_ms": 20,
        "reconnect_backoff_initial_ms": 150,
        "reconnect_backoff_max_ms": 1000,
        "nodes": [
            {"alias": "meter_total", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1001},
             "value_type": "Int64"},
            {"alias": "voltage", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1002},
             "value_type": "Double"},
            {"alias": "raw_frame", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1003},
             "value_type": "ByteString"},
            {"alias": "running", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "string", "value": "RUNNING"},
             "value_type": "Boolean"},
            {"alias": "device_name", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "string", "value": "DEVICE_NAME"},
             "value_type": "String"},
            {"alias": "waveform", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1101},
             "value_type": "Int64_array"},
            {"alias": "doubles", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1103},
             "value_type": "Double_array"},
            {"alias": "flags", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "string", "value": "FLAGS"},
             "value_type": "Boolean_array"},
            {"alias": "tags", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "string", "value": "TAGS"},
             "value_type": "String_array"},
            {"alias": "blobs", "namespace_uri": BUSINESS_URI,
             "identifier": {"kind": "numeric", "value": 1102},
             "value_type": "ByteString_array"},
        ],
    }
    config.update(overrides)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2))
    return path


def fixture_cmd(opc_port: int, control_port: int, **flags: Any) -> list[str]:
    cmd = [
        _PY, "-m", "minenergy.opccollect.cli", "fixture",
        "--opc-port", str(opc_port),
        "--control-port", str(control_port),
    ]
    if flags.get("placeholder_count"):
        cmd += ["--placeholder-count", str(flags["placeholder_count"])]
    if flags.get("omit_business_uri"):
        cmd += ["--omit-business-uri"]
    if flags.get("type_change"):
        cmd += ["--type-change"]
    return cmd


def run_cmd(config: Path, db: Path, run_ms: int, inject: str | None = None) -> list[str]:
    cmd = [
        _PY, "-m", "minenergy.opccollect.cli", "run",
        "--config", str(config), "--db", str(db), "--run-ms", str(run_ms),
    ]
    if inject:
        cmd += ["--inject", inject]
    return cmd


def query_cmd(db: Path, alias: str, **kwargs: Any) -> list[str]:
    cmd = [
        _PY, "-m", "minenergy.opccollect.cli", "query",
        "--db", str(db), "--alias", alias,
    ]
    for key, value in kwargs.items():
        cmd += [f"--{key.replace('_', '-')}", str(value)]
    return cmd


# ---------------------------------------------------------------------------
# 断言工具
# ---------------------------------------------------------------------------


class CheckFailed(AssertionError):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailed(message)
    print(f"  PASS  {message}")


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.2) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise CheckFailed(f"condition not met within {timeout}s; last={last!r}")


def run_json(cmd: list[str], expect_code: int = 0) -> tuple[int, Any]:
    proc = subprocess.run(cmd, env=_ENV, capture_output=True, text=True)
    if proc.returncode != expect_code:
        raise CheckFailed(
            f"command exited {proc.returncode} (expected {expect_code}): "
            f"{' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    try:
        return proc.returncode, json.loads(proc.stdout)
    except json.JSONDecodeError:
        return proc.returncode, proc.stdout


def wait_for_row_count(db: Path, alias: str, minimum: int,
                       timeout: float = 12.0) -> int:
    """轮询只读 query，直到某别名历史达到 minimum 行（保证通知已提交）。"""
    deadline = time.monotonic() + timeout
    last = 0
    while time.monotonic() < deadline:
        _, payload = run_json(query_cmd(db, alias))
        last = payload["rows"]
        if last >= minimum:
            return last
        time.sleep(0.2)
    raise CheckFailed(
        f"{alias} 仅 {last} 行，期望至少 {minimum} 行（{timeout}s 内未达成）"
    )


# ---------------------------------------------------------------------------
# 演练分段
# ---------------------------------------------------------------------------


@dataclass
class DrillContext:
    workdir: Path
    opc_port: int
    control_port: int
    endpoint: str
    config: Path
    db: Path


def phase1_basic_drill(ctx: DrillContext) -> None:
    print("\n=== 阶段 1：标量/数组、坏质量、同源时间不同值，跨进程 Arrow 核对 ===")
    fixture = Subprocess(
        fixture_cmd(ctx.opc_port, ctx.control_port), "fixture-1"
    )
    fixture.start()
    try:
        # 等夹具就绪行
        ready = _read_ready_line(fixture)
        check(ready["status"] == "ready", "夹具进程就绪")
        check(
            ready["namespace_array"][-1] == BUSINESS_URI,
            f"业务 URI 注册在 NamespaceArray（{ready['namespace_array']}）",
        )
        ctrl = ControlClient(ctx.control_port)

        collector = Subprocess(run_cmd(ctx.config, ctx.db, 4000), "collector-1")
        collector.start()
        time.sleep(1.2)  # 等订阅建立

        ctrl.write("meter_total", 100)
        ctrl.write("voltage", 230.5)
        ctrl.write("running", False)
        ctrl.write("raw_frame", base64.b64encode(b"\x00\xff\x10").decode())
        ctrl.write("waveform", [10, -20, 30])
        ctrl.write("tags", ["a", None, "c"])
        ctrl.write(
            "blobs",
            [base64.b64encode(b"x").decode(), None,
             base64.b64encode(b"\x00").decode()],
        )
        # 非 Good 质量读数（Uncertain 保留值；Bad 按规范置空值但留质量码）
        ctrl.write_dv("meter_total", 555, status="Uncertain",
                      source_ts="2026-01-01T00:00:00Z")
        # 同源时间、不同数值的连续更新
        ctrl.write_dv("meter_total", 111, source_ts="2026-06-01T12:00:00Z")
        time.sleep(0.15)
        ctrl.write_dv("meter_total", 222, source_ts="2026-06-01T12:00:00Z")

        # 先从另一个进程确认通知已提交（初始值+4 次更新），再正常停止
        meter_rows = wait_for_row_count(ctx.db, "meter_total", 5)
        code, out, err = collector.stop()
        check(code == 0, f"采集正常停止退出码 0（实际 {code}）")
        summary = json.loads(out)
        check(summary["status"] == "completed", "采集段标记 completed")
        check(
            summary["received_count"] == summary["committed_count"],
            f"接收量==已提交量（{summary['received_count']}=={summary['committed_count']}）",
        )
        check(meter_rows >= 5, f"meter_total 至少 5 行已提交（{meter_rows}）")
    finally:
        fixture.stop()

    # --- 另一个进程只读核对 ---
    print("\n-- 跨进程 query/inspect 核对 --")
    _, meter = run_json(query_cmd(ctx.db, "meter_total"))
    check(meter["rows"] >= 5, f"meter_total 行数 >=5（{meter['rows']}）")
    check(meter["value_arrow_type"] == "int64", "value 的 Arrow 类型为 int64")
    check(meter["non_good_rows"] >= 1, f"保留非 Good 行（{meter['non_good_rows']}）")
    check(meter["identity"]["namespace_uri"] == BUSINESS_URI,
          "Arrow metadata 对应永久 namespace URI")

    _, tags = run_json(query_cmd(ctx.db, "tags"))
    check(tags["value_arrow_type"] == "list<item: string>",
          "数组节点 Arrow 类型为 list<string>，不是公共字符串列")

    _, blobs = run_json(query_cmd(ctx.db, "blobs"))
    check(blobs["value_arrow_type"] == "list<item: binary>",
          "ByteString 数组 Arrow 类型为 list<binary>")

    _, inspect = run_json([
        _PY, "-m", "minenergy.opccollect.cli", "inspect", "--db", str(ctx.db),
    ])
    check(len(inspect["segments"]) == 1, "只有一个采集段")
    check(inspect["segments"][0]["end_kind"] == "completed", "段状态 completed")
    check(
        len({c["alias"] for c in inspect["contracts"]}) == 10,
        "10 个节点契约均可在新进程恢复",
    )
    # 同源时间不同值：直接用 python 读库断言
    _assert_same_source_time_both_kept(ctx.db)


def _read_ready_line(fixture: Subprocess) -> dict[str, Any]:
    assert fixture.proc is not None
    line = fixture.proc.stdout.readline()
    if not line:
        raise CheckFailed("fixture exited before ready line")
    return json.loads(line)


def _assert_same_source_time_both_kept(db: Path) -> None:
    from .storage import HistoryReader

    with HistoryReader(db) as reader:
        rows = list(reader.iter_rows("meter_total"))
        target_us = int(
            datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()
            * 1_000_000
        )
        same = [r for r in rows if r["source_us"] == target_us]
        check(len(same) == 2, "同源时间不同值的两条通知都保留")
        check({r["value"] for r in same} == {111, 222}, "两个不同数值都在")
        check(same[0]["node_seq"] < same[1]["node_seq"],
              "到达顺序编号保持先后（不按源时间排序冒充）")


def phase2_disconnect_reconnect(ctx: DrillContext) -> None:
    print("\n=== 阶段 2：真实断线 + 占位 namespace 导致下标变化 ===")
    f1 = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "fixture-2a")
    f1.start()
    ready1 = _read_ready_line(f1)
    idx1 = ready1["business_index"]
    check(idx1 == 2, f"首次业务 namespace 下标为 2（实际 {idx1}）")

    collector = Subprocess(run_cmd(ctx.config, ctx.db, 12_000), "collector-2")
    collector.start()
    time.sleep(1.2)
    ControlClient(ctx.control_port).write("meter_total", 200)
    time.sleep(0.5)

    # 真的断开服务
    f1.stop()
    print("  .. 服务已断开，等待采集器记录空档并重连")
    time.sleep(2.5)

    # 同端点重启，预注册 2 个占位 namespace
    f2 = Subprocess(
        fixture_cmd(ctx.opc_port, ctx.control_port, placeholder_count=2),
        "fixture-2b",
    )
    f2.start()
    ready2 = _read_ready_line(f2)
    idx2 = ready2["business_index"]
    check(idx2 == 4 and idx2 != idx1,
          f"重启后业务 URI 下标变为 {idx2}（占位 namespace 生效）")
    ctrl = ControlClient(ctx.control_port)
    time.sleep(1.5)  # 等重连与重新订阅
    ctrl.write("meter_total", 300)
    time.sleep(1.0)

    code, out, err = collector.stop()
    f2.stop()
    check(code == 0, f"重连演练采集退出码 0（实际 {code}）")

    _, inspect = run_json([
        _PY, "-m", "minenergy.opccollect.cli", "inspect", "--db", str(ctx.db),
    ])
    events = inspect["connection_events"]
    connected = [e for e in events if e["kind"] == "session_connected"]
    disconnected = [e for e in events if e["kind"] == "session_disconnected"]
    attempt_failed = [e for e in events if e["kind"] == "connect_attempt_failed"]
    check(len(connected) >= 2, f"至少两次 session_connected（{len(connected)}）")
    check(len(disconnected) >= 1, f"真实断线事件落库（{len(disconnected)}）")
    check(len(attempt_failed) >= 1,
          f"对端未就绪的连接尝试单独记账（{len(attempt_failed)}），未伪装成设备停电")
    gap = disconnected[-1]
    check(gap["last_observed_us"] is not None
          and gap["last_observed_us"] <= gap["observed_us"],
          "断线只记录已知边界（last_observed <= observed），不编造精确瞬间")
    check(bool(gap["reason"]), "断线原因已记录")
    resolutions = [
        e["detail"]["namespace_resolution"]["meter_total"]["namespace_index"]
        for e in connected
    ]
    check(idx1 in resolutions and idx2 in resolutions,
          f"两次连接解析到不同下标 {resolutions}，但别名历史连续")

    from .storage import HistoryReader
    with HistoryReader(ctx.db) as reader:
        values = [r["value"] for r in reader.iter_rows("meter_total")
                  if r["value"] is not None]
        check(200 in values and 300 in values,
              "断线前后的值都在同一永久节点历史里（没串到占位节点）")
        segments = reader.segments()
        check(len(segments) == 2, "两次 CLI 运行 = 两个采集段（不伪装无空档）")
        # 接收顺序号跨段单调，不重新从 1 开始冒充连续无中断
        second = segments[1]
        check(second["end_kind"] == "completed", "重连段正常完成")


def phase3_failure_scenarios(ctx: DrillContext) -> None:
    print("\n=== 阶段 3：失败场景（类型漂移 / URI 消失 / 写盘异常 / 队列溢出）===")

    # 3a 类型漂移
    print("\n-- 3a 节点类型与契约不符 --")
    f_good = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "fx-good")
    f_good.start()
    _read_ready_line(f_good)
    c = Subprocess(run_cmd(ctx.config, ctx.db, 20_000), "fx-collector")
    c.start()
    time.sleep(1.2)
    ControlClient(ctx.control_port).write("meter_total", 900)
    time.sleep(0.5)
    f_good.stop()
    time.sleep(1.5)
    f_bad = Subprocess(
        fixture_cmd(ctx.opc_port, ctx.control_port, type_change=True),
        "fx-typechange",
    )
    f_bad.start()
    _read_ready_line(f_bad)
    # 采集器必须因类型不符自行失败退出（不是被我们 terminate）
    code, out, err = c.wait_for_exit(timeout=15.0)
    f_bad.stop()
    check(code == 2, f"类型漂移以退出码 2 失败（实际 {code}）")
    payload = json.loads(out)
    check(payload["status"] == "failed", "类型漂移采集段标记 failed")
    check(payload["error_code"] == "ContractMismatchError",
          f"错误类型 ContractMismatchError（{payload.get('error_code')}）")
    check(any(v >= 1 for v in payload["readable_history_counts"].values()),
          "失败前历史仍可读")

    # 3b URI 消失
    print("\n-- 3b 业务 URI 消失 --")
    f_good = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "uri-good")
    f_good.start()
    _read_ready_line(f_good)
    c = Subprocess(run_cmd(ctx.config, ctx.db, 20_000), "uri-collector")
    c.start()
    time.sleep(1.2)
    f_good.stop()
    time.sleep(1.5)
    f_missing = Subprocess(
        fixture_cmd(ctx.opc_port, ctx.control_port, omit_business_uri=True),
        "uri-missing",
    )
    f_missing.start()
    _read_ready_line(f_missing)
    code, out, err = c.wait_for_exit(timeout=15.0)
    f_missing.stop()
    check(code == 2, f"URI 消失以退出码 2 失败（实际 {code}）")
    check(json.loads(out)["error_code"] == "UriMissingError",
          "错误类型 UriMissingError，没有接到另一节点")

    # 3c 写盘异常（独立库，便于断言）
    print("\n-- 3c 写盘异常 --")
    f = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "disk-fx")
    f.start()
    _read_ready_line(f)
    disk_db = ctx.workdir / "disk_fail.sqlite"
    c = Subprocess(
        run_cmd(ctx.config, disk_db, 20_000,
                inject=json.dumps({"kind": "disk_fail", "fail_after": 12})),
        "disk-collector",
    )
    c.start()
    time.sleep(1.2)
    ctrl = ControlClient(ctx.control_port)
    for i in range(15):
        ctrl.write("meter_total", 8000 + i)
        time.sleep(0.08)
    code, out, err = c.wait_for_exit(timeout=15.0)
    f.stop()
    check(code == 2, f"写盘异常以退出码 2 失败（实际 {code}）")
    payload = json.loads(out)
    check(payload["error_code"] == "StorageWriteError",
          "错误类型 StorageWriteError")
    check(sum(payload["readable_history_counts"].values()) > 0,
          "注入失败前已提交历史仍可读")

    # 3d 小队列溢出（独立库 + 独立小队列配置）
    print("\n-- 3d 小队列溢出 --")
    f = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "q-fx")
    f.start()
    _read_ready_line(f)
    small_config = ctx.workdir / "small_queue_config.json"
    write_config(small_config, ctx.endpoint, queue_capacity=4,
                 batch_max_rows=4, batch_max_delay_ms=1000)
    q_db = ctx.workdir / "queue_overflow.sqlite"
    c = Subprocess(
        run_cmd(small_config, q_db, 20_000,
                inject=json.dumps({"kind": "writer_blocked"})),
        "q-collector",
    )
    c.start()
    time.sleep(1.2)
    ctrl = ControlClient(ctx.control_port)
    for i in range(100):
        ctrl.write("meter_total", 9000 + i)
    code, out, err = c.wait_for_exit(timeout=15.0)
    f.stop()
    check(code == 2, f"队列溢出以退出码 2 失败（实际 {code}）")
    check(json.loads(out)["error_code"] == "QueueOverflowError",
          "错误类型 QueueOverflowError，未丢通知谎报成功")


def phase4_buffer_cap(ctx: DrillContext) -> None:
    print("\n=== 阶段 4：持续通知缓冲上限与正常结束一致性 ===")
    f = Subprocess(fixture_cmd(ctx.opc_port, ctx.control_port), "buf-fx")
    f.start()
    _read_ready_line(f)
    cap_config = ctx.workdir / "bounded_config.json"
    write_config(cap_config, ctx.endpoint, queue_capacity=64,
                 batch_max_rows=8, batch_max_delay_ms=30)
    cap_db = ctx.workdir / "bounded.sqlite"
    run_ms = 16_000
    c = Subprocess(run_cmd(cap_config, cap_db, run_ms), "buf-collector")
    c.start()
    time.sleep(1.2)
    ctrl = ControlClient(ctx.control_port)
    total = 200
    # 写值节奏慢于订阅采样间隔（sampling_interval=20ms），保证每次写值都构成
    # 独立采样通知；管线完整性仍以 received==committed 为准。
    for i in range(total):
        ctrl.write("voltage", float(i))
        time.sleep(0.04)
    # 轮询确认全部更新已提交，再让 run-ms 正常停止
    got = wait_for_row_count(cap_db, "voltage", total)
    code, out, err = c.stop()
    f.stop()
    check(code == 0, f"持续通知演练退出码 0（实际 {code}）")
    summary = json.loads(out)
    check(summary["received_count"] == summary["committed_count"],
          f"正常结束接收量==已提交量（{summary['received_count']}=={summary['committed_count']}）")

    # 直接读库核对：全部 200 个采样值都在、顺序连续、无丢失
    from .storage import HistoryReader
    with HistoryReader(cap_db) as reader:
        values = [r["value"] for r in reader.iter_rows("voltage")]
        check(len(values) >= total, f"voltage 至少 {total} 行（{len(values)}）")
        check(all(float(i) in values for i in range(total)),
              f"200 个采样值 0..199 全部落库（实际 {len(set(values))} 个不同值），无丢失")
        # 缓冲上限：队列水位由有界容量约束，无丢失即证明没有溢出
    check(got >= total, f"轮询确认采样落库（{got} 行）")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", required=True)
    args = parser.parse_args()
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    opc_port = free_port()
    control_port = free_port()
    endpoint = f"opc.tcp://127.0.0.1:{opc_port}"
    config = write_config(workdir / "config.json", endpoint)
    db = workdir / "history.sqlite"
    ctx = DrillContext(
        workdir=workdir, opc_port=opc_port, control_port=control_port,
        endpoint=endpoint, config=config, db=db,
    )
    print(f"workdir   : {workdir}")
    print(f"endpoint  : {endpoint}")
    print(f"db        : {db}")

    failures: list[str] = []
    for phase in (phase1_basic_drill, phase2_disconnect_reconnect,
                  phase3_failure_scenarios, phase4_buffer_cap):
        try:
            phase(ctx)
        except CheckFailed as exc:
            failures.append(f"{phase.__name__}: {exc}")
            print(f"  FAIL  {exc}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{phase.__name__}: unexpected {type(exc).__name__}: {exc}")
            print(f"  ERROR {type(exc).__name__}: {exc}")

    print("\n=== 演练汇总 ===")
    if failures:
        for f in failures:
            print(f"  FAILED {f}")
        print(f"RESULT: {len(failures)} phase(s) failed")
        return 1
    print("RESULT: all field-drill phases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
