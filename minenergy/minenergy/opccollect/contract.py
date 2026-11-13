"""节点契约：永久身份（namespace URI + 标识符）与值类型绑定。

关键规则（对应需求）：

* 节点身份只由 ``namespace_uri`` + ``identifier`` 组成，identifier 只允许
  数值型（OPC UA i=）或字符串型（s=）；服务器当前分配的 namespace 下标**不进
  契约**，每次（重）连接都重新读 NamespaceArray 解析；
* 每个节点有一个使用者查询用的稳定别名 ``alias``，库内唯一；
* 契约显式声明期望值类型 ``value_type``，覆盖 Int64 / Double / Boolean /
  String / ByteString 及它们的一维数组；
* 契约可以序列化进 SQLite，新进程打开同一个历史库即可恢复全部类型绑定与
  永久身份，导出的 Arrow RecordBatch 仍能对应回配置身份。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
from asyncua import ua

from .errors import (
    ConfigError,
    ContractMismatchError,
    UnsupportedValueError,
    UriMissingError,
)

# ---------------------------------------------------------------------------
# 支持的值类型
# ---------------------------------------------------------------------------

#: 契约允许的全部类型名。``*_array`` 表示 OPC UA 一维数组（ValueRank=1）。
CONTRACT_TYPES: tuple[str, ...] = (
    "Int64",
    "Double",
    "Boolean",
    "String",
    "ByteString",
    "Int64_array",
    "Double_array",
    "Boolean_array",
    "String_array",
    "ByteString_array",
)

# 契约类型 -> 期望的 asyncua VariantType（数组看元素类型）
_SCALAR_VARIANT_TYPE: dict[str, ua.VariantType] = {
    "Int64": ua.VariantType.Int64,
    "Double": ua.VariantType.Double,
    "Boolean": ua.VariantType.Boolean,
    "String": ua.VariantType.String,
    "ByteString": ua.VariantType.ByteString,
}

# 契约类型 -> 值列的 Arrow 类型（实际类型由节点契约决定，不共用字符串列）
ARROW_TYPE: dict[str, pa.DataType] = {
    "Int64": pa.int64(),
    "Double": pa.float64(),
    "Boolean": pa.bool_(),
    "String": pa.string(),
    "ByteString": pa.binary(),
    "Int64_array": pa.list_(pa.int64()),
    "Double_array": pa.list_(pa.float64()),
    "Boolean_array": pa.list_(pa.bool_()),
    "String_array": pa.list_(pa.string()),
    "ByteString_array": pa.list_(pa.binary()),
}

# 哪些契约允许数组元素为空（OPC UA 二进制可空元素类型，见模块 docstring 边界）
_NULLABLE_ELEMENT_TYPES = frozenset({"String", "ByteString"})

# 标量空值在 OPC UA 线上的可达形态：
#  - String / ByteString 标量可以是类型化空值；
#  - 数值/布尔标量的"空"只能以 Null 型 Variant + 非 Good 质量码到达
#    （OPC UA 二进制没有数值型 null 编码，asyncua 服务端也无法序列化
#     数值数组里的 null 元素——这是协议边界，不是本实现的限制）。
_NULLABLE_SCALAR_TYPES = frozenset({"String", "ByteString"})

_IDENTIFIER_NAME = re.compile(r"^[A-Za-z0-9_.\-:]+$")


@dataclass(frozen=True, slots=True)
class NodeContract:
    """单个订阅节点的永久契约。

    .. attribute:: alias
        使用者查询用的稳定别名，库内唯一。
    .. attribute:: namespace_uri
        永久 namespace 身份；每次连接用它在 NamespaceArray 里换真实下标。
    .. attribute:: identifier
        数值标识符（``int``，对应 i=）或字符串标识符（``str``，对应 s=）。
    .. attribute:: value_type
        :data:`CONTRACT_TYPES` 之一，决定期望 Variant 形状与 Arrow 列类型。
    """

    alias: str
    namespace_uri: str
    identifier: int | str
    value_type: str

    @property
    def is_array(self) -> bool:
        return self.value_type.endswith("_array")

    @property
    def scalar_type_name(self) -> str:
        return self.value_type[: -len("_array")] if self.is_array else self.value_type

    @property
    def expected_variant_type(self) -> ua.VariantType:
        return _SCALAR_VARIANT_TYPE[self.scalar_type_name]

    @property
    def arrow_type(self) -> pa.DataType:
        return ARROW_TYPE[self.value_type]

    @property
    def elements_nullable(self) -> bool:
        return self.is_array and self.scalar_type_name in _NULLABLE_ELEMENT_TYPES

    def to_node_id(self, namespace_index: int) -> ua.NodeId:
        """把契约定位到某个真实 namespace 下标下的 NodeId。"""
        return ua.NodeId(self.identifier, namespace_index)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "namespace_uri": self.namespace_uri,
            "identifier": self.identifier,
            "identifier_kind": "numeric" if isinstance(self.identifier, int) else "string",
            "value_type": self.value_type,
        }

    # -- 运行时形状校验 -----------------------------------------------------

    def validate_variant(self, variant: ua.Variant) -> None:
        """对一次通知里的 Variant 做契约形状校验。

        空值（Null 型 Variant）允许通过，交给存储层按空值边界落库；
        类型不符、多维数组、不可空元素为 null 都抛
        :class:`~minenergy.opccollect.errors.UnsupportedValueError` /
        :class:`~minenergy.opccollect.errors.ContractMismatchError`，
        绝不猜成字符串继续收。
        """
        # 显式 Null：标量空值的合法线上形态（通常伴随非 Good 质量码）
        if variant.VariantType == ua.VariantType.Null:
            return
        expected = self.expected_variant_type
        if variant.VariantType != expected:
            raise ContractMismatchError(
                f"variant type {variant.VariantType.name} does not match contract "
                f"{self.value_type} (expected {expected.name})",
                alias=self.alias,
            )
        if self.is_array:
            if not variant.is_array:
                raise ContractMismatchError(
                    f"expected 1-d array of {self.scalar_type_name} but received a "
                    f"scalar {variant.VariantType.name}",
                    alias=self.alias,
                )
            self._validate_dimensions(variant)
            if variant.Value is not None:
                self._validate_elements(variant.Value)
        else:
            if variant.is_array:
                raise ContractMismatchError(
                    f"expected scalar {self.value_type} but received an array",
                    alias=self.alias,
                )

    def _validate_dimensions(self, variant: ua.Variant) -> None:
        dims = variant.Dimensions
        if dims is not None and len(dims) > 1:
            raise UnsupportedValueError(
                f"multi-dimensional array {list(dims)} is not supported; only 1-d "
                f"arrays are contracted",
                alias=self.alias,
            )
        # 嵌套 list/tuple 同样视为多维（空列表时 Dimensions 可能为 None/[0]）
        value = variant.Value
        if isinstance(value, list | tuple) and value:
            first = value[0]
            if isinstance(first, list | tuple):
                raise UnsupportedValueError(
                    "nested (multi-dimensional) array is not supported; only 1-d "
                    "arrays are contracted",
                    alias=self.alias,
                )

    def _validate_elements(self, value: list | tuple) -> None:
        for pos, element in enumerate(value):
            if element is None and not self.elements_nullable:
                raise UnsupportedValueError(
                    f"null element at position {pos} cannot be encoded for "
                    f"{self.scalar_type_name} arrays over OPC UA binary; only "
                    f"String/ByteString array elements are nullable",
                    alias=self.alias,
                )
            if element is not None and not isinstance(
                element, self._python_element_type()
            ):
                raise ContractMismatchError(
                    f"element at position {pos} has Python type "
                    f"{type(element).__name__}, expected {self.scalar_type_name}",
                    alias=self.alias,
                )

    def _python_element_type(self) -> tuple[type, ...]:
        return _PYTHON_TYPES[self.scalar_type_name]


_PYTHON_TYPES: dict[str, tuple[type, ...]] = {
    "Int64": (int,),
    "Double": (float, int),
    "Boolean": (bool,),
    "String": (str,),
    "ByteString": (bytes, bytearray),
}


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """一份完整的采集配置：端点 + 节点契约清单。"""

    endpoint: str
    nodes: tuple[NodeContract, ...]
    publishing_interval_ms: float = 100.0
    sampling_interval_ms: float = 50.0
    # 有界退避（毫秒）：初始、上限
    reconnect_backoff_initial_ms: float = 200.0
    reconnect_backoff_max_ms: float = 5_000.0
    # 有界接收队列与每批提交规模
    queue_capacity: int = 10_000
    batch_max_rows: int = 500
    batch_max_delay_ms: float = 200.0
    extra: dict[str, Any] = field(default_factory=dict)

    def contract_by_alias(self) -> dict[str, NodeContract]:
        return {c.alias: c for c in self.nodes}

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "publishing_interval_ms": self.publishing_interval_ms,
            "sampling_interval_ms": self.sampling_interval_ms,
            "reconnect_backoff_initial_ms": self.reconnect_backoff_initial_ms,
            "reconnect_backoff_max_ms": self.reconnect_backoff_max_ms,
            "queue_capacity": self.queue_capacity,
            "batch_max_rows": self.batch_max_rows,
            "batch_max_delay_ms": self.batch_max_delay_ms,
            "nodes": [c.to_json_dict() for c in self.nodes],
        }


# ---------------------------------------------------------------------------
# 配置载入与校验
# ---------------------------------------------------------------------------

def load_config(path: str | Path) -> CollectorConfig:
    """从 JSON 文件载入并完整校验配置。

    配置形态::

        {
          "endpoint": "opc.tcp://127.0.0.1:4840",
          "nodes": [
            {"alias": "meter_total",
             "namespace_uri": "urn:minenergy:field",
             "identifier": {"kind": "numeric", "value": 1001},
             "value_type": "Int64"},
            {"alias": "status_word",
             "namespace_uri": "urn:minenergy:field",
             "identifier": {"kind": "string", "value": "STATUS"},
             "value_type": "Boolean"}
          ],
          "queue_capacity": 2000
        }

    纯字符串/纯数字 identifier 也接受（按 JSON 类型推断 kind）。
    """
    p = Path(path)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {p}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {p}: {exc}") from exc
    return config_from_dict(raw)


def config_from_dict(raw: dict[str, Any]) -> CollectorConfig:
    if not isinstance(raw, dict):
        raise ConfigError("top-level config must be a JSON object")
    endpoint = raw.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint:
        raise ConfigError("endpoint must be a non-empty opc.tcp URL string")
    if not endpoint.startswith("opc.tcp://"):
        raise ConfigError(f"endpoint must start with opc.tcp://, got {endpoint!r}")

    nodes_raw = raw.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        raise ConfigError("nodes must be a non-empty array")

    contracts: list[NodeContract] = []
    seen_aliases: set[str] = set()
    seen_identities: set[tuple[str, int | str]] = set()
    for pos, item in enumerate(nodes_raw):
        contract = _parse_node(item, pos)
        if contract.alias in seen_aliases:
            raise ConfigError(f"duplicate alias {contract.alias!r}")
        identity = (contract.namespace_uri, contract.identifier)
        if identity in seen_identities:
            raise ConfigError(
                f"duplicate node identity {contract.namespace_uri!r} + "
                f"{contract.identifier!r} (aliases must share one contract)"
            )
        seen_aliases.add(contract.alias)
        seen_identities.add(identity)
        contracts.append(contract)

    kwargs: dict[str, Any] = {}
    for key in (
        "publishing_interval_ms",
        "sampling_interval_ms",
        "reconnect_backoff_initial_ms",
        "reconnect_backoff_max_ms",
    ):
        if key in raw:
            _require_positive_number(raw, key)
            kwargs[key] = float(raw[key])
    for key in ("queue_capacity", "batch_max_rows"):
        if key in raw:
            _require_positive_int(raw, key)
            kwargs[key] = int(raw[key])
    if "batch_max_delay_ms" in raw:
        _require_positive_number(raw, "batch_max_delay_ms")
        kwargs["batch_max_delay_ms"] = float(raw["batch_max_delay_ms"])
    if (
        "reconnect_backoff_initial_ms" in kwargs
        and "reconnect_backoff_max_ms" in kwargs
        and kwargs["reconnect_backoff_initial_ms"]
        > kwargs["reconnect_backoff_max_ms"]
    ):
        raise ConfigError(
            "reconnect_backoff_initial_ms must be <= reconnect_backoff_max_ms"
        )

    known = {
        "endpoint", "nodes", "publishing_interval_ms", "sampling_interval_ms",
        "reconnect_backoff_initial_ms", "reconnect_backoff_max_ms",
        "queue_capacity", "batch_max_rows", "batch_max_delay_ms",
    }
    extra = {k: v for k, v in raw.items() if k not in known}
    return CollectorConfig(
        endpoint=endpoint, nodes=tuple(contracts), extra=extra, **kwargs
    )


def _parse_node(item: Any, pos: int) -> NodeContract:
    where = f"nodes[{pos}]"
    if not isinstance(item, dict):
        raise ConfigError(f"{where} must be an object")
    alias = item.get("alias")
    if not isinstance(alias, str) or not alias.strip():
        raise ConfigError(f"{where}.alias must be a non-empty string")
    if not _IDENTIFIER_NAME.match(alias):
        raise ConfigError(
            f"{where}.alias {alias!r} may only contain letters, digits and _.-:"
        )
    uri = item.get("namespace_uri")
    if not isinstance(uri, str) or not uri:
        raise ConfigError(f"{where}.namespace_uri must be a non-empty string")
    value_type = item.get("value_type")
    if value_type not in CONTRACT_TYPES:
        raise ConfigError(
            f"{where}.value_type {value_type!r} is not supported; choose one of "
            f"{', '.join(CONTRACT_TYPES)}"
        )
    identifier = _parse_identifier(item.get("identifier"), where)
    return NodeContract(
        alias=alias,
        namespace_uri=uri,
        identifier=identifier,
        value_type=value_type,
    )


def _parse_identifier(raw: Any, where: str) -> int | str:
    if isinstance(raw, bool):
        raise ConfigError(f"{where}.identifier boolean is neither numeric nor string")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        if not raw:
            raise ConfigError(f"{where}.identifier string must not be empty")
        return raw
    if isinstance(raw, dict):
        kind = raw.get("kind")
        value = raw.get("value")
        if kind == "numeric":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(
                    f"{where}.identifier numeric value must be an integer"
                )
            return value
        if kind == "string":
            if not isinstance(value, str) or not value:
                raise ConfigError(
                    f"{where}.identifier string value must be a non-empty string"
                )
            return value
        raise ConfigError(
            f"{where}.identifier.kind must be 'numeric' or 'string', got {kind!r}"
        )
    raise ConfigError(
        f"{where}.identifier must be a number, string or "
        f"{{kind, value}} object, got {type(raw).__name__}"
    )


def _require_positive_number(raw: dict, key: str) -> None:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ConfigError(f"{key} must be a positive number, got {value!r}")


def _require_positive_int(raw: dict, key: str) -> None:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{key} must be a positive integer, got {value!r}")


# ---------------------------------------------------------------------------
# NamespaceArray 解析（下标永远不进契约）
# ---------------------------------------------------------------------------

def resolve_namespace_index(
    namespace_array: Any, namespace_uri: str
) -> int:
    """在服务器 NamespaceArray 中定位 URI 的真实下标。

    找不到时抛 :class:`UriMissingError`——调用方必须结束本次采集，不能猜下标。
    下标 0/1 是 OPC UA 保留命名空间，业务 URI 不应落在那里，但仍以数组内容
    为准而不是硬编码。
    """
    if not isinstance(namespace_array, list | tuple) or not namespace_array:
        raise UriMissingError(
            f"server returned no usable NamespaceArray while resolving {namespace_uri!r}"
        )
    matches = [i for i, uri in enumerate(namespace_array) if uri == namespace_uri]
    if not matches:
        raise UriMissingError(
            f"namespace URI {namespace_uri!r} is absent from the server "
            f"NamespaceArray {list(namespace_array)!r}; refusing to bind this "
            f"contract to any other node"
        )
    if len(matches) > 1:
        # OPC UA 允许数组中出现重复 URI；取最小下标并记录这个事实，
        # 仍由 URI（而非下标）承担身份。
        return min(matches)
    return matches[0]
