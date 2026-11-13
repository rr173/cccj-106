"""配置载入、契约校验与类型边界（纯单元测试，不启服务）。"""
from __future__ import annotations

import json

import pyarrow as pa
import pytest
from asyncua import ua

from minenergy.opccollect.contract import (
    ARROW_TYPE,
    CONTRACT_TYPES,
    NodeContract,
    config_from_dict,
    resolve_namespace_index,
)
from minenergy.opccollect.errors import (
    ConfigError,
    ContractMismatchError,
    UnsupportedValueError,
    UriMissingError,
)


def _node_dict(**overrides):
    base = {
        "alias": "m",
        "namespace_uri": "urn:x",
        "identifier": {"kind": "numeric", "value": 1},
        "value_type": "Int64",
    }
    base.update(overrides)
    return base


def test_minimal_config_loads_and_defaults(tmp_path):
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps({
        "endpoint": "opc.tcp://127.0.0.1:4840",
        "nodes": [_node_dict()],
    }))
    from minenergy.opccollect.contract import load_config

    loaded = load_config(cfg_path)
    assert loaded.endpoint == "opc.tcp://127.0.0.1:4840"
    assert loaded.nodes[0].identifier == 1
    assert loaded.queue_capacity > 0 and loaded.batch_max_rows > 0


def test_string_identifier_and_bare_identifier_forms():
    cfg = config_from_dict({
        "endpoint": "opc.tcp://h:1",
        "nodes": [
            _node_dict(alias="a", identifier="TAG-1", value_type="String"),
            _node_dict(alias="b", identifier=42, value_type="Double"),
        ],
    })
    assert cfg.nodes[0].identifier == "TAG-1"
    assert cfg.nodes[1].identifier == 42


def test_all_contract_types_have_distinct_arrow_columns():
    assert len(CONTRACT_TYPES) == 10
    for name in CONTRACT_TYPES:
        contract = NodeContract("a", "urn:x", 1, name)
        assert contract.arrow_type == ARROW_TYPE[name]
    # 数组与标量 Arrow 类型不同，不共用字符串列
    assert ARROW_TYPE["Int64"] == pa.int64()
    assert ARROW_TYPE["Int64_array"] == pa.list_(pa.int64())
    assert ARROW_TYPE["ByteString"] == pa.binary()


@pytest.mark.parametrize(
    "bad",
    [
        {"endpoint": "http://nope"},
        {"endpoint": "opc.tcp://x", "nodes": []},
        {"endpoint": "opc.tcp://x", "nodes": [_node_dict(value_type="Float")]},
        {"endpoint": "opc.tcp://x", "nodes": [_node_dict(alias="")]},
        {"endpoint": "opc.tcp://x", "nodes": [_node_dict(namespace_uri="")]},
        {"endpoint": "opc.tcp://x",
         "nodes": [_node_dict(identifier={"kind": "guid", "value": "x"})]},
        {"endpoint": "opc.tcp://x",
         "nodes": [_node_dict(identifier={"kind": "numeric", "value": "1"})]},
        {"endpoint": "opc.tcp://x",
         "nodes": [_node_dict(identifier=True)]},
    ],
)
def test_config_rejections(bad):
    with pytest.raises(ConfigError):
        config_from_dict(bad)


def test_duplicate_alias_and_identity_rejected():
    with pytest.raises(ConfigError):
        config_from_dict({
            "endpoint": "opc.tcp://x",
            "nodes": [_node_dict(alias="a"), _node_dict(alias="a")],
        })
    with pytest.raises(ConfigError):
        config_from_dict({
            "endpoint": "opc.tcp://x",
            "nodes": [
                _node_dict(alias="a"),
                _node_dict(alias="b"),
            ],
        })


def test_bad_bound_settings_rejected():
    with pytest.raises(ConfigError):
        config_from_dict({
            "endpoint": "opc.tcp://x",
            "nodes": [_node_dict()],
            "queue_capacity": 0,
        })
    with pytest.raises(ConfigError):
        config_from_dict({
            "endpoint": "opc.tcp://x",
            "nodes": [_node_dict()],
            "reconnect_backoff_initial_ms": 9000,
            "reconnect_backoff_max_ms": 1000,
        })


def test_namespace_resolution_finds_uri_and_rejects_missing():
    ns = ["http://opcfoundation.org/UA/", "urn:server", "urn:biz"]
    assert resolve_namespace_index(ns, "urn:biz") == 2
    with pytest.raises(UriMissingError):
        resolve_namespace_index(ns, "urn:gone")


def test_namespace_resolution_duplicate_uri_takes_min_index():
    ns = ["urn:biz", "urn:other", "urn:biz"]
    assert resolve_namespace_index(ns, "urn:biz") == 0


# ---------------------------------------------------------------------------
# 运行时 Variant 形状校验
# ---------------------------------------------------------------------------


def _v(value, vtype, is_array=False, dims=None):
    return ua.Variant(value, vtype, is_array=is_array, Dimensions=dims)


def test_scalar_contract_accepts_matching_type_and_null():
    c = NodeContract("a", "urn:x", 1, "Int64")
    c.validate_variant(_v(5, ua.VariantType.Int64))
    c.validate_variant(ua.Variant(None))  # Null 型放行，交给存储层
    with pytest.raises(ContractMismatchError):
        c.validate_variant(_v(5.0, ua.VariantType.Double))
    with pytest.raises(ContractMismatchError):
        c.validate_variant(_v([1], ua.VariantType.Int64, is_array=True))


def test_array_contract_requires_one_dimensional_shape():
    c = NodeContract("a", "urn:x", 1, "Int64_array")
    c.validate_variant(_v([1, 2], ua.VariantType.Int64, is_array=True))
    c.validate_variant(_v([], ua.VariantType.Int64, is_array=True))
    with pytest.raises(ContractMismatchError):
        c.validate_variant(_v(1, ua.VariantType.Int64))
    # 多维数组：显式 Dimensions
    with pytest.raises(UnsupportedValueError):
        c.validate_variant(
            ua.Variant([[1, 2], [3, 4]], ua.VariantType.Int64,
                       Dimensions=[2, 2])
        )
    # 嵌套 list 同样按多维拒绝
    with pytest.raises(UnsupportedValueError):
        c.validate_variant(
            _v([[1, 2]], ua.VariantType.Int64, is_array=True)
        )


def test_null_element_boundaries():
    # String 数组允许 null 元素
    str_contract = NodeContract("a", "urn:x", 1, "String_array")
    str_contract.validate_variant(
        _v(["x", None], ua.VariantType.String, is_array=True)
    )
    # Int64 数组的 null 元素不可编码，明确报错而不是猜成字符串
    int_contract = NodeContract("a", "urn:x", 1, "Int64_array")
    with pytest.raises(UnsupportedValueError):
        int_contract.validate_variant(
            _v([1, None], ua.VariantType.Int64, is_array=True)
        )


def test_wrong_element_type_reported_with_alias():
    c = NodeContract("meter", "urn:x", 1, "Boolean_array")
    with pytest.raises(ContractMismatchError) as info:
        c.validate_variant(
            _v([True, 1], ua.VariantType.Boolean, is_array=True)
        )
    assert info.value.alias == "meter"


def test_node_id_uses_resolved_index_not_cached_identity():
    c = NodeContract("a", "urn:biz", "TAG", "String")
    nid = c.to_node_id(7)
    assert nid.NamespaceIndex == 7
    assert nid.Identifier == "TAG"
    nid2 = c.to_node_id(3)  # 服务器重启后下标变化
    assert nid2.NamespaceIndex == 3
    assert nid2.Identifier == "TAG"  # 永久身份不变
