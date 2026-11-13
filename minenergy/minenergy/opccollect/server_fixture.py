"""可重建的本地 OPC UA 服务端夹具（真实协议，不是回调桩）。

用途：

* pytest 里以 :class:`FixtureServer` 在进程内启动真实 ``opc.tcp`` 服务；
* 演示时以 ``python -m minenergy.opccollect.cli fixture`` 启动独立进程，
  通过 JSON-lines 控制口发写值/坏质量/停机指令；
* 支持重启时预注册占位 namespace（让业务 URI 的下标改变）、省略业务 URI
  （模拟 URI 消失）、把同标识节点改成另一类型（模拟契约类型漂移）。

夹具节点覆盖标量与一维数组、数值型与字符串型标识符：

================ =========== ==================
节点别名          标识符      类型
================ =========== ==================
meter_total      i=1001      Int64
voltage          i=1002      Double
raw_frame        i=1003      ByteString
running          s=RUNNING   Boolean
device_name      s=DEVICE_NAME String
waveform         i=1101      Int64 数组
doubles          i=1103      Double 数组
flags            s=FLAGS     Boolean 数组
tags             s=TAGS      String 数组
blobs            i=1102      ByteString 数组
================ =========== ==================
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from asyncua import Server, ua

logger = logging.getLogger("minenergy.opccollect.fixture")

#: 业务 namespace 的永久 URI（采集配置引用它，而不是下标）。
BUSINESS_URI = "urn:minenergy:opccollect:field"
#: 占位 namespace 用的 URI 模板。
PLACEHOLDER_URI_TEMPLATE = "urn:minenergy:opccollect:placeholder:{index}"


@dataclass(frozen=True, slots=True)
class FixtureNode:
    alias: str
    identifier: int | str
    variant_type: ua.VariantType
    initial: Any
    is_array: bool = False

    @property
    def type_name(self) -> str:
        scalar = _VARIANT_TO_NAME[self.variant_type]
        return f"{scalar}_array" if self.is_array else scalar


_VARIANT_TO_NAME = {
    ua.VariantType.Int64: "Int64",
    ua.VariantType.Double: "Double",
    ua.VariantType.Boolean: "Boolean",
    ua.VariantType.String: "String",
    ua.VariantType.ByteString: "ByteString",
}


def default_nodes(*, type_change: bool = False) -> list[FixtureNode]:
    """夹具的标准节点清单；``type_change`` 时 meter_total 改成 String。"""
    meter_type = ua.VariantType.String if type_change else ua.VariantType.Int64
    meter_initial = "" if type_change else 0
    return [
        FixtureNode("meter_total", 1001, meter_type, meter_initial),
        FixtureNode("voltage", 1002, ua.VariantType.Double, 0.0),
        FixtureNode("raw_frame", 1003, ua.VariantType.ByteString, b""),
        FixtureNode("running", "RUNNING", ua.VariantType.Boolean, True),
        FixtureNode("device_name", "DEVICE_NAME", ua.VariantType.String, "FIXTURE-1"),
        FixtureNode("waveform", 1101, ua.VariantType.Int64, [], is_array=True),
        FixtureNode("doubles", 1103, ua.VariantType.Double, [], is_array=True),
        FixtureNode("flags", "FLAGS", ua.VariantType.Boolean, [], is_array=True),
        FixtureNode("tags", "TAGS", ua.VariantType.String, [], is_array=True),
        FixtureNode("blobs", 1102, ua.VariantType.ByteString, [], is_array=True),
    ]


# ---------------------------------------------------------------------------
# JSON 控制值 <-> Python / OPC UA 值
# ---------------------------------------------------------------------------

def json_to_value(spec: Any, node: FixtureNode) -> Any:
    """把控制命令里的 JSON 值转成 Python 值（bytes 走 base64）。"""
    if spec is None:
        return None
    if node.is_array:
        if not isinstance(spec, list):
            raise ValueError(f"node {node.alias} expects an array")
        return [_json_scalar(element, node.variant_type) for element in spec]
    return _json_scalar(spec, node.variant_type)


def _json_scalar(spec: Any, variant_type: ua.VariantType) -> Any:
    if spec is None:
        return None
    if variant_type == ua.VariantType.Int64:
        if isinstance(spec, bool) or not isinstance(spec, int):
            raise ValueError(f"expected integer, got {spec!r}")
        return spec
    if variant_type == ua.VariantType.Double:
        if isinstance(spec, bool) or not isinstance(spec, int | float):
            raise ValueError(f"expected number, got {spec!r}")
        return float(spec)
    if variant_type == ua.VariantType.Boolean:
        if not isinstance(spec, bool):
            raise ValueError(f"expected boolean, got {spec!r}")
        return spec
    if variant_type == ua.VariantType.String:
        if not isinstance(spec, str):
            raise ValueError(f"expected string, got {spec!r}")
        return spec
    if variant_type == ua.VariantType.ByteString:
        if not isinstance(spec, str):
            raise ValueError("ByteString must be base64 string")
        return base64.b64decode(spec)
    raise ValueError(f"unsupported variant type {variant_type}")


def _parse_source_ts(spec: str | None) -> datetime | None:
    if spec is None:
        return None
    text = spec.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError("source_ts must carry a timezone offset (use Z or +hh:mm)")
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# 进程内夹具（pytest 用）
# ---------------------------------------------------------------------------


class FixtureServer:
    """真实 OPC UA 服务端夹具的进程内封装。"""

    def __init__(
        self,
        *,
        port: int = 0,
        host: str = "127.0.0.1",
        business_uri: str = BUSINESS_URI,
        placeholder_count: int = 0,
        include_business_uri: bool = True,
        type_change: bool = False,
        nodes: list[FixtureNode] | None = None,
    ):
        self.host = host
        self.port = port
        self.business_uri = business_uri
        self.placeholder_count = placeholder_count
        self.include_business_uri = include_business_uri
        self.type_change = type_change
        self._nodes_spec = nodes if nodes is not None else default_nodes(
            type_change=type_change
        )
        self._server: Server | None = None
        self._node_handles: dict[str, Any] = {}
        self._business_index: int | None = None

    @property
    def endpoint(self) -> str:
        return f"opc.tcp://{self.host}:{self.port}"

    @property
    def business_namespace_index(self) -> int:
        if self._business_index is None:
            raise RuntimeError("business namespace is not registered on this fixture")
        return self._business_index

    async def start(self) -> None:
        server = Server()
        await server.init()
        server.set_endpoint(self.endpoint)
        server.set_server_name("minenergy-opccollect-fixture")
        # 先注册占位 namespace，迫使业务 URI 的下标随占位数量变化
        for i in range(self.placeholder_count):
            await server.register_namespace(
                PLACEHOLDER_URI_TEMPLATE.format(index=i)
            )
        if self.include_business_uri:
            self._business_index = await server.register_namespace(
                self.business_uri
            )
            ns_index = self._business_index
            objects = server.nodes.objects
            for spec in self._nodes_spec:
                node_id = ua.NodeId(spec.identifier, ns_index)
                variable = await objects.add_variable(
                    node_id, spec.alias, spec.initial, spec.variant_type
                )
                await variable.set_writable()
                self._node_handles[spec.alias] = variable
        self._server = server
        await server.start()
        logger.info("fixture OPC UA server listening on %s", self.endpoint)

    async def stop(self) -> None:
        if self._server is not None:
            with contextlib.suppress(Exception):
                await self._server.stop()
            self._server = None

    async def __aenter__(self) -> "FixtureServer":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    def spec(self, alias: str) -> FixtureNode:
        for node in self._nodes_spec:
            if node.alias == alias:
                return node
        raise KeyError(f"fixture has no node {alias!r}")

    async def write_value(self, alias: str, json_spec: Any) -> None:
        """普通写值（服务器补时间戳，质量 Good）。

        空值边界遵循协议：String/ByteString 标量与可空元素数组可以写类型化
        null；Int64/Double/Boolean 标量没有线上 null 编码，写 null 会被
        服务器以 BadTypeMismatch 拒绝——这类"无值"在现场只能以非 Good
        质量码表达，请用 :meth:`write_data_value`。
        """
        spec = self.spec(alias)
        value = json_to_value(json_spec, spec)
        node = self._node_handles[alias]
        if value is None:
            if spec.variant_type in (ua.VariantType.String, ua.VariantType.ByteString):
                # 类型化 null：VariantType 仍是 String/ByteString，Value=None
                await node.write_value(ua.Variant(None, spec.variant_type))
            else:
                raise ValueError(
                    f"node {alias} ({spec.type_name}) cannot carry a Good null: "
                    "numeric/boolean scalars have no OPC UA binary null encoding; "
                    "use write_data_value with a non-Good status instead"
                )
        else:
            await node.write_value(value, spec.variant_type)

    async def write_data_value(
        self,
        alias: str,
        json_spec: Any,
        *,
        status_name: str = "Good",
        source_ts: datetime | None = None,
    ) -> None:
        """带显式质量码与源时间的写值，用于坏质量/同源时间不同值场景。

        注意 OPC UA Part 4 §7.7.1 的规范行为：写入的 StatusCode 为 Bad 时，
        服务器必须忽略值并把它置成 Null（采集端因此收到 Null Variant + 原
        质量码）；Uncertain 不是 Bad，值会原样保留。夹具不绕过该规则，因为
        现场真实设备与合规服务器就是这样表现的。
        """
        spec = self.spec(alias)
        value = json_to_value(json_spec, spec)
        node = self._node_handles[alias]
        status_code = _status_code_by_name(status_name)
        if value is None:
            if spec.variant_type in (ua.VariantType.String, ua.VariantType.ByteString):
                variant = ua.Variant(None, spec.variant_type)
            else:
                # 数值/布尔标量的"设备无值"：Null 型 Variant + 非 Good 质量码。
                # 服务器按 Part 4 §7.7.1 对 Bad 码置空值，Uncertain 不允许配 Null
                # 型 Variant（类型检查先于状态分支）。
                if status_name not in ("Good",) and _status_is_bad(status_code):
                    variant = ua.Variant(None)
                else:
                    raise ValueError(
                        f"node {alias} ({spec.type_name}) null requires a Bad "
                        "status code; numeric/boolean scalars have no Good/Uncertain "
                        "null encoding"
                    )
        else:
            variant = ua.Variant(value, spec.variant_type)
        dv = ua.DataValue(
            variant,
            StatusCode=ua.StatusCode(status_code),
            SourceTimestamp=source_ts,
        )
        await node.write_value(dv)

    async def namespace_array(self) -> list[str]:
        assert self._server is not None
        return await self._server.get_namespace_array()


def _status_code_by_name(name: str) -> int:
    if name == "Good":
        return ua.StatusCodes.Good
    code = getattr(ua.StatusCodes, name, None)
    if code is None:
        raise ValueError(f"unknown OPC UA status name {name!r}")
    return code


def _status_is_bad(code: int) -> bool:
    """OPC UA StatusCode 的最高位（bit 31）为 1 即 Bad；Uncertain 是 bit 30。"""
    return bool(code & 0x80000000)


# ---------------------------------------------------------------------------
# 独立进程夹具 + JSON-lines 控制口（演示 shell 用）
# ---------------------------------------------------------------------------


class ControlledFixture:
    """带 TCP JSON-lines 控制口的夹具进程主体。"""

    def __init__(
        self,
        *,
        opc_port: int,
        control_port: int,
        host: str = "127.0.0.1",
        placeholder_count: int = 0,
        include_business_uri: bool = True,
        type_change: bool = False,
    ):
        self.fixture = FixtureServer(
            port=opc_port,
            host=host,
            placeholder_count=placeholder_count,
            include_business_uri=include_business_uri,
            type_change=type_change,
        )
        self.control_port = control_port
        self.host = host
        self._stop = asyncio.Event()

    async def serve(self) -> None:
        await self.fixture.start()
        server = await asyncio.start_server(
            self._handle_control, self.host, self.control_port
        )
        actual_control = server.sockets[0].getsockname()[1]
        # 启动行：机器可读，演示脚本据此确认就绪与真实端口
        print(
            json.dumps(
                {
                    "status": "ready",
                    "endpoint": self.fixture.endpoint,
                    "control_port": actual_control,
                    "business_index": self.fixture._business_index,
                    "namespace_array": await self.fixture.namespace_array(),
                }
            ),
            flush=True,
        )
        try:
            await self._stop.wait()
        finally:
            server.close()
            with contextlib.suppress(Exception):
                await server.wait_closed()
            await self.fixture.stop()

    async def _handle_control(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while not self._stop.is_set():
                line = await reader.readline()
                if not line:
                    return
                command = json.loads(line.decode("utf-8"))
                response = await self._dispatch(command)
                writer.write(
                    (json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8")
                )
                await writer.drain()
                if command.get("cmd") == "shutdown":
                    self._stop.set()
                    return
        except Exception as exc:  # noqa: BLE001 - 控制口错误回给驱动方
            writer.write(
                (
                    json.dumps({"status": "error", "error": str(exc)}) + "\n"
                ).encode("utf-8")
            )
            await writer.drain()

    async def _dispatch(self, command: dict[str, Any]) -> dict[str, Any]:
        cmd = command.get("cmd")
        if cmd == "ping":
            return {"status": "ok"}
        if cmd == "info":
            return {
                "status": "ok",
                "endpoint": self.fixture.endpoint,
                "namespace_array": await self.fixture.namespace_array(),
                "business_index": self.fixture._business_index,
            }
        if cmd == "write":
            await self.fixture.write_value(command["node"], command.get("value"))
            return {"status": "ok", "node": command["node"]}
        if cmd == "write_dv":
            source_ts = _parse_source_ts(command.get("source_ts"))
            await self.fixture.write_data_value(
                command["node"],
                command.get("value"),
                status_name=command.get("status", "Good"),
                source_ts=source_ts,
            )
            return {"status": "ok", "node": command["node"]}
        if cmd == "shutdown":
            return {"status": "shutting-down"}
        return {"status": "error", "error": f"unknown command {cmd!r}"}


async def run_controlled_fixture(
    *,
    opc_port: int,
    control_port: int,
    placeholder_count: int,
    include_business_uri: bool,
    type_change: bool,
) -> None:
    controlled = ControlledFixture(
        opc_port=opc_port,
        control_port=control_port,
        placeholder_count=placeholder_count,
        include_business_uri=include_business_uri,
        type_change=type_change,
    )
    await controlled.serve()
