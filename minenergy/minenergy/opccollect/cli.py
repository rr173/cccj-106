"""命令行入口：接收命令、查询示例与服务端夹具控制。

子命令::

  run       启动一次在线采集（真实 OPC UA 协议），写入独立 SQLite 历史
  query     按节点 + 接收编号范围读出 PyArrow RecordBatch（可打印/落 parquet）
  inspect   查看采集段、连接空档事件与契约恢复情况
  fixture   启动带 JSON-lines 控制口的本地 OPC UA 服务端夹具进程

契约类致命错误（URI 消失、类型不符、不支持的形状、队列溢出、写盘失败）
统一以退出码 2 结束，采集段记为 ``failed``；正常停止退出码 0。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa

from .collector import Collector, CollectorSettings
from .contract import CollectorConfig, load_config
from .errors import OpcCollectError
from .faults import BlockingHistoryStore, FailingHistoryStore
from .server_fixture import run_controlled_fixture
from .storage import HistoryReader, HistoryStore, read_record_batches


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minenergy-opccollect",
        description="OPC UA 在线采集管线（原样留痕，不做分钟用量计算）",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="启动一次在线采集")
    run_p.add_argument("--config", required=True, help="采集配置 JSON")
    run_p.add_argument("--db", required=True, help="独立 SQLite 历史库路径")
    run_p.add_argument(
        "--run-ms",
        type=int,
        default=None,
        help="可选：运行指定毫秒后正常停止（演示/测试用）",
    )
    run_p.add_argument(
        "--inject",
        default=None,
        help=(
            "故障注入 JSON（演示用）："
            '{"kind":"disk_fail","fail_after":N} 或 '
            '{"kind":"writer_blocked"}'
        ),
    )
    run_p.set_defaults(func=cmd_run)

    query_p = sub.add_parser("query", help="按节点与接收编号范围读出 RecordBatch")
    query_p.add_argument("--db", required=True)
    query_p.add_argument("--alias", required=True)
    query_p.add_argument("--first-seq", type=int, default=None)
    query_p.add_argument("--last-seq", type=int, default=None)
    query_p.add_argument("--batch-size", type=int, default=1024)
    query_p.add_argument(
        "--format", choices=("summary", "json", "parquet"), default="summary"
    )
    query_p.add_argument("--out", default=None, help="parquet 输出路径")
    query_p.set_defaults(func=cmd_query)

    inspect_p = sub.add_parser("inspect", help="查看采集段/连接事件/契约")
    inspect_p.add_argument("--db", required=True)
    inspect_p.add_argument(
        "--segment-id", type=int, default=None, help="只看某个采集段"
    )
    inspect_p.set_defaults(func=cmd_inspect)

    fixture_p = sub.add_parser("fixture", help="启动本地 OPC UA 服务端夹具")
    fixture_p.add_argument("--opc-port", type=int, required=True)
    fixture_p.add_argument("--control-port", type=int, required=True)
    fixture_p.add_argument("--placeholder-count", type=int, default=0)
    fixture_p.add_argument(
        "--omit-business-uri",
        action="store_true",
        help="不注册业务 namespace（模拟服务器重启后 URI 消失）",
    )
    fixture_p.add_argument(
        "--type-change",
        action="store_true",
        help="把 meter_total 节点改成 String 类型（模拟契约类型漂移）",
    )
    fixture_p.set_defaults(func=cmd_fixture)
    return parser


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _build_store(db_path: str, inject: str | None) -> HistoryStore:
    if not inject:
        return HistoryStore(db_path)
    spec = json.loads(inject)
    kind = spec.get("kind")
    if kind == "disk_fail":
        return FailingHistoryStore(db_path, fail_after=int(spec["fail_after"]))
    if kind == "writer_blocked":
        return BlockingHistoryStore(db_path, block=True)
    raise OpcCollectError(f"unknown injection kind {kind!r}")


async def _run_async(args: argparse.Namespace) -> int:
    config: CollectorConfig = load_config(args.config)
    settings = CollectorSettings.from_config(config)
    store = _build_store(args.db, args.inject)
    collector = Collector(config, store, settings)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        stop_event.set()
        collector.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with _suppress_not_in_main_thread():
            loop.add_signal_handler(sig, _signal_handler)

    stop_timer: asyncio.TimerHandle | None = None
    if args.run_ms is not None:
        def _on_timer() -> None:
            stop_event.set()
            collector.request_stop()

        stop_timer = loop.call_later(args.run_ms / 1000.0, _on_timer)

    try:
        result = await collector.run()
    except OpcCollectError as exc:
        # _finalize 已经把采集段记为 failed；这里给机器可读摘要后退出码 2
        failed = _summarize_failed(store, config, exc)
        print(json.dumps(failed, ensure_ascii=False, indent=2))
        return 2
    finally:
        if stop_timer is not None:
            stop_timer.cancel()
        store.close()

    print(json.dumps({
        "status": "completed",
        "segment_id": result.segment_id,
        "received_count": result.received_count,
        "committed_count": result.committed_count,
        "enqueued_count": result.enqueued_count,
        "connection_count": result.connection_count,
        "disconnect_count": result.disconnect_count,
        "complete": result.complete,
    }, ensure_ascii=False, indent=2))
    return 0


def _summarize_failed(
    store: HistoryStore, config: CollectorConfig, exc: OpcCollectError
) -> dict[str, Any]:
    # 用只读连接复核：失败段确实落库且历史仍可读
    with HistoryReader(store.path) as reader:
        segments = reader.segments()
        latest = segments[-1] if segments else None
        counts = {c.alias: reader.count(c.alias) for c in reader.contracts()}
    return {
        "status": "failed",
        "error": str(exc),
        "error_code": exc.code,
        "alias": exc.alias,
        "segment": latest,
        "readable_history_counts": counts,
    }


class _suppress_not_in_main_thread:
    def __enter__(self):
        return self

    def __exit__(self, *exc: Any) -> bool:
        return isinstance(exc[1], NotImplementedError)


def cmd_run(args: argparse.Namespace) -> int:
    try:
        return asyncio.run(_run_async(args))
    except OpcCollectError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)},
                         ensure_ascii=False), file=sys.stderr)
        return 2


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


def cmd_query(args: argparse.Namespace) -> int:
    if args.format == "parquet":
        if not args.out:
            raise OpcCollectError("--out is required for parquet format")
        import pyarrow.parquet as pq

        writer = None
        total = 0
        for batch in read_record_batches(
            args.db, args.alias, args.first_seq, args.last_seq, args.batch_size
        ):
            if writer is None:
                writer = pq.ParquetWriter(args.out, batch.schema)
            writer.write_batch(batch)
            total += batch.num_rows
        if writer is None:
            # 空结果也要写出正确 schema：先取契约 schema
            with HistoryReader(args.db) as reader:
                contract = reader.contract(args.alias)
            from .storage import record_batch_schema
            empty_schema = record_batch_schema(contract)
            empty = _empty_batch(empty_schema)
            writer = pq.ParquetWriter(args.out, empty.schema)
            writer.write_batch(empty)
        writer.close()
        print(json.dumps({"status": "ok", "rows": total, "out": args.out}))
        return 0

    batches = list(
        read_record_batches(
            args.db, args.alias, args.first_seq, args.last_seq, args.batch_size
        )
    )
    if not batches:
        with HistoryReader(args.db) as reader:
            contract = reader.contract(args.alias)
        print(json.dumps({
            "alias": args.alias,
            "rows": 0,
            "value_arrow_type": str(contract.arrow_type),
            "identity": {
                "namespace_uri": contract.namespace_uri,
                "identifier": contract.identifier,
            },
        }, ensure_ascii=False, indent=2))
        return 0
    table = pa.Table.from_batches(batches)
    if args.format == "json":
        print(table.to_pydict())
        return 0
    # summary：人类可读核对
    contract_meta = batches[0].schema.metadata or {}
    print(json.dumps({
        "alias": args.alias,
        "rows": table.num_rows,
        "value_arrow_type": str(table.schema.field("value").type),
        "identity": {
            "namespace_uri": contract_meta.get(
                b"opccollect.namespace_uri", b"?"
            ).decode(),
            "identifier": contract_meta.get(
                b"opccollect.identifier", b"?"
            ).decode(),
            "value_type": contract_meta.get(
                b"opccollect.value_type", b"?"
            ).decode(),
        },
        "node_seq_range": [
            table.column("node_seq")[0].as_py(),
            table.column("node_seq")[-1].as_py(),
        ],
        "non_good_rows": sum(
            1 for usable in table.column("usable").to_pylist() if not usable
        ),
        "null_value_rows": sum(
            1 for is_null in table.column("value_is_null").to_pylist() if is_null
        ),
    }, ensure_ascii=False, indent=2))
    return 0


def _empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    """按 schema 构造 0 行 RecordBatch（空结果仍带完整类型）。"""
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


def cmd_inspect(args: argparse.Namespace) -> int:
    with HistoryReader(args.db) as reader:
        payload: dict[str, Any] = {
            "db": str(Path(args.db).absolute()),
            "contracts": [
                {
                    "alias": c.alias,
                    "namespace_uri": c.namespace_uri,
                    "identifier": c.identifier,
                    "value_type": c.value_type,
                    "arrow_type": str(c.arrow_type),
                    "rows": reader.count(c.alias),
                    "node_seq_bounds": reader.seq_bounds(c.alias),
                }
                for c in reader.contracts()
            ],
            "segments": reader.segments(),
            "connection_events": reader.connection_events(args.segment_id),
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


# ---------------------------------------------------------------------------
# fixture
# ---------------------------------------------------------------------------


def cmd_fixture(args: argparse.Namespace) -> int:
    asyncio.run(
        run_controlled_fixture(
            opc_port=args.opc_port,
            control_port=args.control_port,
            placeholder_count=args.placeholder_count,
            include_business_uri=not args.omit_business_uri,
            type_change=args.type_change,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except OpcCollectError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyError as exc:
        # 未知别名等查询错误
        print(f"QueryError: {exc}", file=sys.stderr)
        return 2
    except sqlite3.Error as exc:
        # 历史库打不开/损坏等
        print(f"StorageError: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
