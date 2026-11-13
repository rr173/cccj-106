"""OPC UA 在线采集管线（与既有能耗 store 完全分离）。

本包只负责"原样留痕"：

* 按 *namespace URI + 标识符* 的永久身份订阅节点，绝不把服务器当前分配的
  namespace 下标当作身份（下标在服务器重启后可能变化）；
* 把每次通知的 DataValue（源时间、服务器时间、完整 StatusCode、采集机实际
  收到时间）原样写入独立的 SQLite 历史；
* 接收顺序单独编号，不按源时间重排；同源时间不同值的通知两条都保留；
* 不计算分钟用量、不做录波解析、不做配电推演、不提供网页。

模块划分：

contract.py  节点契约（永久身份 + 值类型绑定）与配置载入/校验
errors.py    带节点与原因的错误类型
storage.py   独立 SQLite 历史（采集段、连接事件、通知记录）与 Arrow 读出
collector.py asyncua 客户端：NamespaceArray 解析、订阅、退避重连、有界队列
server_fixture.py 可重建的本地 OPC UA 服务端夹具（含占位 namespace 场景）
cli.py       接收命令 / 查询示例 / 夹具控制
"""
from __future__ import annotations

from .contract import (
    CONTRACT_TYPES,
    NodeContract,
    CollectorConfig,
    load_config,
)
from .errors import (
    OpcCollectError,
    ConfigError,
    ContractMismatchError,
    UriMissingError,
    UnsupportedValueError,
    QueueOverflowError,
    StorageWriteError,
)
from .storage import HistoryReader, HistoryStore, read_record_batches
from .collector import Collector, CollectorSettings, RunResult

__all__ = [
    "CONTRACT_TYPES",
    "NodeContract",
    "CollectorConfig",
    "load_config",
    "OpcCollectError",
    "ConfigError",
    "ContractMismatchError",
    "UriMissingError",
    "UnsupportedValueError",
    "QueueOverflowError",
    "StorageWriteError",
    "HistoryStore",
    "HistoryReader",
    "read_record_batches",
    "Collector",
    "CollectorSettings",
    "RunResult",
]
