"""测试与演示支持：端口选择、标准契约与 CollectorConfig 构造。

放在包内是为了让 pytest 与演示脚本以普通包导入方式复用，避免测试目录的
导入路径技巧；生产采集路径不依赖本模块。
"""
from __future__ import annotations

import socket

from .collector import CollectorSettings
from .contract import CollectorConfig, NodeContract
from .server_fixture import BUSINESS_URI

#: 测试默认的快速节奏设置（字段均属于 CollectorSettings）。
FAST_SETTINGS = dict(
    liveness_poll_ms=50.0,
    drain_timeout_ms=10_000.0,
    request_timeout_ms=3_000.0,
)


def free_port() -> int:
    """向系统申请一个当前空闲 TCP 端口（存在极小竞态，测试环境可接受）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def standard_contracts(uri: str = BUSINESS_URI) -> tuple[NodeContract, ...]:
    """与 :func:`server_fixture.default_nodes` 一一对应的契约清单。"""
    return (
        NodeContract("meter_total", uri, 1001, "Int64"),
        NodeContract("voltage", uri, 1002, "Double"),
        NodeContract("raw_frame", uri, 1003, "ByteString"),
        NodeContract("running", uri, "RUNNING", "Boolean"),
        NodeContract("device_name", uri, "DEVICE_NAME", "String"),
        NodeContract("waveform", uri, 1101, "Int64_array"),
        NodeContract("doubles", uri, 1103, "Double_array"),
        NodeContract("flags", uri, "FLAGS", "Boolean_array"),
        NodeContract("tags", uri, "TAGS", "String_array"),
        NodeContract("blobs", uri, 1102, "ByteString_array"),
    )


def make_config(
    endpoint: str,
    contracts: tuple[NodeContract, ...] | None = None,
    **overrides: object,
) -> CollectorConfig:
    """构造节奏更快的测试用采集配置（退避/发布间隔都调小）。"""
    kwargs: dict[str, object] = {
        "publishing_interval_ms": 50.0,
        "sampling_interval_ms": 20.0,
        "reconnect_backoff_initial_ms": 100.0,
        "reconnect_backoff_max_ms": 400.0,
    }
    kwargs.update(overrides)
    return CollectorConfig(
        endpoint=endpoint,
        nodes=contracts or standard_contracts(),
        **kwargs,  # type: ignore[arg-type]
    )


def fast_settings(config: CollectorConfig) -> CollectorSettings:
    """从测试配置生成设置，并叠加快速轮询节奏。"""
    settings = CollectorSettings.from_config(config)
    for key, value in FAST_SETTINGS.items():
        setattr(settings, key, value)
    return settings
