"""独立 SQLite 接收历史（与既有能耗 store 完全分开，不参与其跨文件事务）。

三张核心表：

* ``contracts``       节点契约（永久身份 + 类型绑定），新进程据此恢复
* ``segments``        每次采集进程一段；正常结束 ``completed``，显式失败
                      ``failed``，进程被杀则留在 ``running``（不伪造无空档）
* ``connection_events``  连接/断开事件；断开无法定位精确瞬间时同时记录
                      ``last_observed_us`` 与 ``observed_us`` 两个已知边界
* ``notifications``   每条通知一行，接收顺序单独编号，值按契约类型分列存储，
                      不用一个公共字符串列混装

时间一律存 UTC 微秒整数；设备时间（源/服务器时间）允许 ``NULL``，**禁止**用
接收时间回填。
"""
from __future__ import annotations

import json
import sqlite3
import struct
import threading
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from .contract import NodeContract
from .errors import StorageWriteError

SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# 时间戳工具
# ---------------------------------------------------------------------------


def now_us() -> int:
    """采集机时钟：UTC 微秒。"""
    return time.time_ns() // 1_000


def datetime_to_us(value: Any) -> int | None:
    """OPC UA 时间戳（tz-aware datetime）-> UTC 微秒；``None`` 保持 ``None``。"""
    if value is None:
        return None
    # asyncua 给出的是带 UTC tzinfo 的 datetime
    return int(value.timestamp() * 1_000_000)


# ---------------------------------------------------------------------------
# 值编码：按契约类型分列，保留整数精度、字节内容与空值边界
# ---------------------------------------------------------------------------

#: notifications 表中承载 value 的列，按契约类型固定。
VALUE_COLUMN: dict[str, str] = {
    "Int64": "value_int",
    "Double": "value_real",
    "Boolean": "value_bool",
    "String": "value_text",
    "ByteString": "value_blob",
    "Int64_array": "value_json",
    "Double_array": "value_json",
    "Boolean_array": "value_json",
    "String_array": "value_json",
    "ByteString_array": "value_blob",
}

_BLOB_ARRAY_HEADER = struct.Struct("<Q")
_BLOB_ARRAY_ELEM = struct.Struct("<BI")  # present flag, byte length


def encode_value(contract: NodeContract, value: Any) -> tuple[str, Any]:
    """把校验过的 Python 值编码成 ``(列名, 列值)``。

    * 空值 -> 对应列写 ``None``（配合 ``value_is_null=1``）；
    * 空数组 -> JSON ``'[]'`` 或零元素 BLOB，与 ``None``（空数组值）明确区分；
    * Int64 走 SQLite INTEGER 保精度；ByteString 走 BLOB 保字节；
    * ByteString 数组用长度前缀帧编码，元素级 ``null`` 可原样还原。
    """
    column = VALUE_COLUMN[contract.value_type]
    if value is None:
        return column, None
    if contract.value_type == "Int64":
        return column, int(value)
    if contract.value_type == "Double":
        return column, float(value)
    if contract.value_type == "Boolean":
        return column, 1 if value else 0
    if contract.value_type == "String":
        return column, str(value)
    if contract.value_type == "ByteString":
        return column, bytes(value)
    if contract.value_type == "ByteString_array":
        return column, _encode_bytes_array(value)
    # 其余一维数组：JSON 文本（Int64 整数精度不丢；Double 的 NaN/Inf 由
    # Python json 的非标准标记原样往返；空数组为 '[]'）
    return column, json.dumps(list(value), ensure_ascii=False)


def _encode_bytes_array(values: Sequence[bytes | None]) -> bytes:
    out = bytearray()
    out += _BLOB_ARRAY_HEADER.pack(len(values))
    for element in values:
        if element is None:
            out += _BLOB_ARRAY_ELEM.pack(0, 0)
        else:
            raw = bytes(element)
            out += _BLOB_ARRAY_ELEM.pack(1, len(raw))
            out += raw
    return bytes(out)


def _decode_bytes_array(blob: bytes) -> list[bytes | None]:
    (count,) = _BLOB_ARRAY_HEADER.unpack_from(blob, 0)
    offset = _BLOB_ARRAY_HEADER.size
    result: list[bytes | None] = []
    for _ in range(count):
        present, length = _BLOB_ARRAY_ELEM.unpack_from(blob, offset)
        offset += _BLOB_ARRAY_ELEM.size
        if present == 0:
            result.append(None)
        else:
            result.append(blob[offset:offset + length])
            offset += length
    if offset != len(blob):
        raise StorageWriteError(
            f"ByteString array blob has {len(blob) - offset} trailing bytes; "
            "stored history is corrupt"
        )
    return result


def decode_value(contract: NodeContract, column: str | None, raw: Any) -> Any:
    """把数据库列值还原成喂给 PyArrow 的 Python 值（保留空值/空数组边界）。"""
    if raw is None:
        return None
    if contract.value_type == "Int64":
        return int(raw)
    if contract.value_type == "Double":
        return float(raw)
    if contract.value_type == "Boolean":
        return bool(raw)
    if contract.value_type == "String":
        return str(raw)
    if contract.value_type == "ByteString":
        return bytes(raw)
    if contract.value_type == "ByteString_array":
        return _decode_bytes_array(bytes(raw))
    return json.loads(raw)


# ---------------------------------------------------------------------------
# 行结构（采集线程 -> 写线程的载荷）
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class NotificationRow:
    alias: str
    segment_id: int         # 所属采集段
    node_seq: int          # 该节点的接收顺序号（到达顺序，不按源时间排）
    arrival_seq: int       # 全局接收顺序号
    arrival_us: int        # 采集机实际收到通知的时间
    source_us: int | None  # 设备源时间；缺失就是 None，不回填
    server_us: int | None
    status_code: int       # 完整 StatusCode（uint32 原样）
    status_name: str | None
    usable: bool           # 仅 StatusCode == Good 才可用于正常读数加工
    value_is_null: bool
    value_column: str
    value_raw: Any


@dataclass(slots=True)
class SegmentHandle:
    segment_id: int
    started_us: int


# ---------------------------------------------------------------------------
# 写端：独占连接 + 写线程，队列有界、每批提交有界
# ---------------------------------------------------------------------------

class HistoryStore:
    """采集进程内的写端句柄（单写者）。

    读侧请用 :func:`open_reader` / :func:`read_record_batches`，它们以只读
    URI 打开独立连接，可以在另一个进程里并发查询。
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    # -- 初始化 -------------------------------------------------------------

    def _create_schema(self) -> None:
        c = self._conn
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS contracts (
                alias           TEXT PRIMARY KEY,
                namespace_uri   TEXT NOT NULL,
                identifier_kind TEXT NOT NULL CHECK (identifier_kind IN ('numeric','string')),
                identifier      TEXT NOT NULL,
                value_type      TEXT NOT NULL,
                created_segment INTEGER NOT NULL,
                UNIQUE (namespace_uri, identifier_kind, identifier)
            );
            CREATE TABLE IF NOT EXISTS segments (
                segment_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                started_us        INTEGER NOT NULL,
                ended_us          INTEGER,
                end_kind          TEXT NOT NULL CHECK (end_kind IN ('running','completed','failed')),
                end_reason        TEXT,
                endpoint          TEXT NOT NULL,
                config_json       TEXT NOT NULL,
                pid               INTEGER NOT NULL,
                received_count    INTEGER NOT NULL DEFAULT 0,
                committed_count   INTEGER NOT NULL DEFAULT 0,
                enqueued_count    INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS connection_events (
                event_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                segment_id       INTEGER NOT NULL REFERENCES segments(segment_id),
                kind             TEXT NOT NULL CHECK (kind IN
                                   ('session_connected','session_disconnected',
                                    'connect_attempt_failed')),
                observed_us      INTEGER NOT NULL,
                last_observed_us INTEGER,
                reason           TEXT,
                detail           TEXT
            );
            CREATE TABLE IF NOT EXISTS notifications (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                segment_id    INTEGER NOT NULL REFERENCES segments(segment_id),
                alias         TEXT NOT NULL,
                node_seq      INTEGER NOT NULL,
                arrival_seq   INTEGER NOT NULL,
                arrival_us    INTEGER NOT NULL,
                source_us     INTEGER,
                server_us     INTEGER,
                status_code   INTEGER NOT NULL,
                status_name   TEXT,
                usable        INTEGER NOT NULL CHECK (usable IN (0,1)),
                value_is_null INTEGER NOT NULL CHECK (value_is_null IN (0,1)),
                value_int     INTEGER,
                value_real    REAL,
                value_bool    INTEGER,
                value_text    TEXT,
                value_json    TEXT,
                value_blob    BLOB,
                UNIQUE (segment_id, alias, node_seq)
            );
            CREATE INDEX IF NOT EXISTS idx_notif_alias_seq
                ON notifications(alias, node_seq);
            CREATE INDEX IF NOT EXISTS idx_notif_segment
                ON notifications(segment_id);
            CREATE INDEX IF NOT EXISTS idx_conn_segment
                ON connection_events(segment_id, event_id);
            """
        )
        c.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- 契约登记（新进程可恢复） -------------------------------------------

    def register_contracts(
        self, contracts: Sequence[NodeContract], segment_id: int
    ) -> None:
        """登记契约；与库里已有契约冲突时直接报错，不允许改类型混写。"""
        with self._lock:
            for contract in contracts:
                kind = "numeric" if isinstance(contract.identifier, int) else "string"
                existing = self._conn.execute(
                    "SELECT namespace_uri, identifier_kind, identifier, value_type "
                    "FROM contracts WHERE alias = ?",
                    (contract.alias,),
                ).fetchone()
                if existing is not None:
                    uri, ikind, ident, vtype = existing
                    if (
                        uri != contract.namespace_uri
                        or ikind != kind
                        or ident != str(contract.identifier)
                        or vtype != contract.value_type
                    ):
                        raise StorageWriteError(
                            f"contract for alias {contract.alias!r} changed vs history "
                            f"db: stored=({uri!r},{ikind}:{ident!r},{vtype}) new="
                            f"({contract.namespace_uri!r},{kind}:{contract.identifier!r},"
                            f"{contract.value_type}); refusing to mix identities/types"
                        )
                    continue
                try:
                    self._conn.execute(
                        "INSERT INTO contracts(alias, namespace_uri, identifier_kind, "
                        "identifier, value_type, created_segment) VALUES (?,?,?,?,?,?)",
                        (
                            contract.alias,
                            contract.namespace_uri,
                            kind,
                            str(contract.identifier),
                            contract.value_type,
                            segment_id,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # 同一永久身份被另一个别名重复登记
                    raise StorageWriteError(
                        f"identity {contract.namespace_uri!r} + "
                        f"{kind}:{contract.identifier!r} is already registered to "
                        f"another alias; one permanent node may not carry two contracts"
                    ) from exc

    # -- 采集段与连接事件 ---------------------------------------------------

    def begin_segment(self, endpoint: str, config_json: str) -> SegmentHandle:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO segments(started_us, end_kind, endpoint, config_json, pid) "
                "VALUES (?, 'running', ?, ?, ?)",
                (now_us(), endpoint, config_json, _pid()),
            )
            return SegmentHandle(segment_id=int(cur.lastrowid), started_us=now_us())

    def end_segment(
        self,
        handle: SegmentHandle,
        kind: str,
        reason: str | None,
        received_count: int,
        committed_count: int,
        enqueued_count: int,
    ) -> None:
        if kind not in ("completed", "failed"):
            raise ValueError("segment end kind must be completed or failed")
        with self._lock:
            self._conn.execute(
                "UPDATE segments SET ended_us=?, end_kind=?, end_reason=?, "
                "received_count=?, committed_count=?, enqueued_count=? "
                "WHERE segment_id=?",
                (
                    now_us(),
                    kind,
                    reason,
                    received_count,
                    committed_count,
                    enqueued_count,
                    handle.segment_id,
                ),
            )

    def record_connection(
        self,
        segment_id: int,
        kind: str,
        observed_us: int,
        last_observed_us: int | None = None,
        reason: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> int:
        detail_text = json.dumps(detail, ensure_ascii=False) if detail else None
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO connection_events(segment_id, kind, observed_us, "
                "last_observed_us, reason, detail) VALUES (?,?,?,?,?,?)",
                (segment_id, kind, observed_us, last_observed_us, reason, detail_text),
            )
            return int(cur.lastrowid)

    # -- 通知批量写入 --------------------------------------------------------

    def max_sequences(self) -> tuple[int, dict[str, int]]:
        """取历史全局最大到达序号与各别名最大节点序号。

        新采集段据此继续编号：接收顺序号在整个历史库内单调，不会因为进程
        重启而从 1 重新开始（段仍独立记录，但序号是连续的接收历史）。
        """
        max_arrival = self._conn.execute(
            "SELECT COALESCE(MAX(arrival_seq), 0) FROM notifications"
        ).fetchone()[0]
        node_max: dict[str, int] = {}
        for alias, value in self._conn.execute(
            "SELECT alias, MAX(node_seq) FROM notifications GROUP BY alias"
        ):
            node_max[alias] = value
        return int(max_arrival), node_max

    def insert_notifications(self, rows: Sequence[NotificationRow]) -> int:
        """单事务批量插入；任何失败抛 :class:`StorageWriteError`，不吞异常。

        每批规模由调用方有界控制；整批要么提交要么回滚，不会出现半批可见。
        """
        if not rows:
            return 0
        params = [
            (
                r.segment_id, r.alias, r.node_seq, r.arrival_seq, r.arrival_us,
                r.source_us, r.server_us, r.status_code, r.status_name,
                1 if r.usable else 0, 1 if r.value_is_null else 0,
                r.value_raw if r.value_column == "value_int" else None,
                r.value_raw if r.value_column == "value_real" else None,
                r.value_raw if r.value_column == "value_bool" else None,
                r.value_raw if r.value_column == "value_text" else None,
                r.value_raw if r.value_column == "value_json" else None,
                r.value_raw if r.value_column == "value_blob" else None,
            )
            for r in rows
        ]
        try:
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self._conn.executemany(
                        "INSERT INTO notifications(segment_id, alias, node_seq, "
                        "arrival_seq, arrival_us, source_us, server_us, status_code, "
                        "status_name, usable, value_is_null, value_int, value_real, "
                        "value_bool, value_text, value_json, value_blob) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        params,
                    )
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
        except sqlite3.Error as exc:
            raise StorageWriteError(f"sqlite insert failed: {exc}") from exc
        return len(rows)


def _pid() -> int:
    import os
    return os.getpid()


# ---------------------------------------------------------------------------
# 读端：只读连接，可在另一个进程使用
# ---------------------------------------------------------------------------


class HistoryReader:
    """只读历史查询句柄（独立连接，允许与写端/其他读者并发）。"""

    def __init__(self, path: str | Path):
        uri = Path(path).absolute().as_uri() + "?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self._conn.execute("PRAGMA query_only=ON")
        self.path = str(path)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "HistoryReader":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def contracts(self) -> list[NodeContract]:
        rows = self._conn.execute(
            "SELECT alias, namespace_uri, identifier_kind, identifier, value_type "
            "FROM contracts ORDER BY alias"
        ).fetchall()
        result = []
        for alias, uri, kind, ident, vtype in rows:
            identifier = int(ident) if kind == "numeric" else ident
            result.append(
                NodeContract(
                    alias=alias,
                    namespace_uri=uri,
                    identifier=identifier,
                    value_type=vtype,
                )
            )
        return result

    def contract(self, alias: str) -> NodeContract:
        rows = [c for c in self.contracts() if c.alias == alias]
        if not rows:
            raise KeyError(f"alias {alias!r} not found in history {self.path}")
        return rows[0]

    def segments(self) -> list[dict[str, Any]]:
        cols = (
            "segment_id", "started_us", "ended_us", "end_kind", "end_reason",
            "endpoint", "pid", "received_count", "committed_count",
            "enqueued_count",
        )
        return [
            dict(zip(cols, row))
            for row in self._conn.execute(
                f"SELECT {', '.join(cols)} FROM segments ORDER BY segment_id"
            )
        ]

    def connection_events(self, segment_id: int | None = None) -> list[dict[str, Any]]:
        if segment_id is None:
            rows = self._conn.execute(
                "SELECT event_id, segment_id, kind, observed_us, last_observed_us, "
                "reason, detail FROM connection_events ORDER BY event_id"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT event_id, segment_id, kind, observed_us, last_observed_us, "
                "reason, detail FROM connection_events WHERE segment_id=? "
                "ORDER BY event_id",
                (segment_id,),
            ).fetchall()
        return [
            {
                "event_id": r[0], "segment_id": r[1], "kind": r[2],
                "observed_us": r[3], "last_observed_us": r[4],
                "reason": r[5], "detail": json.loads(r[6]) if r[6] else None,
            }
            for r in rows
        ]

    def count(self, alias: str) -> int:
        return int(
            self._conn.execute(
                "SELECT COUNT(*) FROM notifications WHERE alias=?", (alias,)
            ).fetchone()[0]
        )

    def seq_bounds(self, alias: str) -> tuple[int | None, int | None]:
        row = self._conn.execute(
            "SELECT MIN(node_seq), MAX(node_seq) FROM notifications WHERE alias=?",
            (alias,),
        ).fetchone()
        return row[0], row[1]

    def iter_rows(
        self,
        alias: str,
        first_seq: int | None = None,
        last_seq: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        contract = self.contract(alias)
        column = VALUE_COLUMN[contract.value_type]
        query = (
            "SELECT node_seq, arrival_seq, arrival_us, source_us, server_us, "
            "status_code, status_name, usable, value_is_null, "
            f"{column} FROM notifications WHERE alias=? "
            "AND (? IS NULL OR node_seq >= ?) "
            "AND (? IS NULL OR node_seq <= ?) ORDER BY node_seq"
        )
        cursor = self._conn.execute(
            query, (alias, first_seq, first_seq, last_seq, last_seq)
        )
        for row in cursor:
            (node_seq, arrival_seq, arrival_us, source_us, server_us,
             status_code, status_name, usable, value_is_null, raw) = row
            value = decode_value(contract, column, raw)
            yield {
                "alias": alias,
                "node_seq": node_seq,
                "arrival_seq": arrival_seq,
                "arrival_us": arrival_us,
                "source_us": source_us,
                "server_us": server_us,
                "status_code": status_code,
                "status_name": status_name,
                "usable": bool(usable),
                "value_is_null": bool(value_is_null),
                "value": value,
            }

    def read_record_batches(
        self,
        alias: str,
        first_seq: int | None = None,
        last_seq: int | None = None,
        batch_size: int = 1024,
    ) -> Iterator[pa.RecordBatch]:
        """按节点契约的 Arrow 类型读出 :class:`pa.RecordBatch` 流。

        value 列类型由契约决定（整数精度/字节内容/空值都保留），schema
        metadata 带配置里的永久身份，新进程导出仍能对应回 namespace URI +
        标识符。
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        contract = self.contract(alias)
        buffer: list[dict[str, Any]] = []

        def flush() -> pa.RecordBatch:
            return build_record_batch(contract, buffer)

        for row in self.iter_rows(alias, first_seq, last_seq):
            buffer.append(row)
            if len(buffer) >= batch_size:
                yield flush()
                buffer = []
        if buffer:
            yield flush()


def record_batch_schema(contract: NodeContract) -> pa.Schema:
    """节点契约对应的固定 RecordBatch schema（含永久身份 metadata）。"""
    return pa.schema(
        [
            pa.field("alias", pa.string()),
            pa.field("node_seq", pa.int64()),
            pa.field("arrival_seq", pa.int64()),
            pa.field("arrival_us", pa.int64()),
            pa.field("source_us", pa.int64()),
            pa.field("server_us", pa.int64()),
            pa.field("status_code", pa.int64()),
            pa.field("status_name", pa.string()),
            pa.field("usable", pa.bool_()),
            pa.field("value_is_null", pa.bool_()),
            pa.field("value", contract.arrow_type),
        ],
        metadata={
            b"opccollect.alias": contract.alias.encode(),
            b"opccollect.namespace_uri": contract.namespace_uri.encode(),
            b"opccollect.identifier": (
                ("numeric:" if isinstance(contract.identifier, int) else "string:")
                + str(contract.identifier)
            ).encode(),
            b"opccollect.value_type": contract.value_type.encode(),
        },
    )


def build_record_batch(
    contract: NodeContract, rows: list[dict[str, Any]]
) -> pa.RecordBatch:
    """按契约 Arrow 类型构造 RecordBatch（schema 带永久身份 metadata）。"""
    values = [r["value"] for r in rows]
    value_array = pa.array(values, type=contract.arrow_type)
    columns = {
        "alias": pa.array([r["alias"] for r in rows], type=pa.string()),
        "node_seq": pa.array([r["node_seq"] for r in rows], type=pa.int64()),
        "arrival_seq": pa.array(
            [r["arrival_seq"] for r in rows], type=pa.int64()
        ),
        "arrival_us": pa.array(
            [r["arrival_us"] for r in rows], type=pa.int64()
        ),
        "source_us": pa.array(
            [r["source_us"] for r in rows], type=pa.int64()
        ),
        "server_us": pa.array(
            [r["server_us"] for r in rows], type=pa.int64()
        ),
        "status_code": pa.array(
            [r["status_code"] for r in rows], type=pa.int64()
        ),
        "status_name": pa.array(
            [r["status_name"] for r in rows], type=pa.string()
        ),
        "usable": pa.array([r["usable"] for r in rows], type=pa.bool_()),
        "value_is_null": pa.array(
            [r["value_is_null"] for r in rows], type=pa.bool_()
        ),
        "value": value_array,
    }
    return pa.RecordBatch.from_pydict(columns, schema=record_batch_schema(contract))


# 向后兼容的静态方法
HistoryReader._build_batch = staticmethod(build_record_batch)


def open_reader(path: str | Path) -> HistoryReader:
    return HistoryReader(path)


def read_record_batches(
    path: str | Path,
    alias: str,
    first_seq: int | None = None,
    last_seq: int | None = None,
    batch_size: int = 1024,
) -> Iterator[pa.RecordBatch]:
    """便捷函数：只读打开历史库并产出某节点的 RecordBatch 流。"""
    with HistoryReader(path) as reader:
        yield from reader.read_record_batches(
            alias, first_seq, last_seq, batch_size
        )
