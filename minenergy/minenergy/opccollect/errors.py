"""采集管线的错误类型。

所有"本次采集不能再继续"的错误都带节点别名与具体原因，CLI 以退出码 2 报告；
已经写入 SQLite 的历史永远保留，失败的采集段以 ``failed`` 结束而不是伪装成
正常完成。
"""
from __future__ import annotations


class OpcCollectError(Exception):
    """采集管线错误基类。"""

    def __init__(self, message: str, *, alias: str | None = None,
                 code: str | None = None):
        self.message = message
        self.alias = alias
        self.code = code or self.__class__.__name__
        prefix = f"[{alias}] " if alias is not None else ""
        super().__init__(f"{self.code}: {prefix}{message}")


class ConfigError(OpcCollectError):
    """配置文件本身不合法（未知类型、重名、标识符形状错误等）。"""


class UriMissingError(OpcCollectError):
    """服务器 NamespaceArray 中找不到契约声明的 namespace URI。

    按需求此时必须明确结束本次采集：不允许猜一个下标、不允许接到另一节点。
    """


class ContractMismatchError(OpcCollectError):
    """节点实际通知的值形状/类型与契约不符，或 NodeId 解析失败。"""


class UnsupportedValueError(OpcCollectError):
    """收到的 Variant 落在契约支持边界之外（多维数组、未知 VariantType 等）。

    不猜成字符串继续收：直接报错并让本次采集显式失败。
    """


class QueueOverflowError(OpcCollectError):
    """有界接收队列被写满，通知无法保证完整接收。"""


class StorageWriteError(OpcCollectError):
    """SQLite 写入失败（磁盘错误、注入失败等）。"""


class ShutdownError(OpcCollectError):
    """停止过程中仍有未确认写入，不能宣称完整接收。"""
