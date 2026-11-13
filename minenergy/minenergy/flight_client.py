"""Demo Arrow Flight client for the minenergy fact service.

Examples::

    # discover the full fact schema, print each column's Arrow type
    python -m minenergy.flight_client --uri grpc://127.0.0.1:8815 --schema

    # narrow time range with a column projection, consume DoGet batches
    python -m minenergy.flight_client --uri grpc://127.0.0.1:8815 \
        --meters M0 --start 2025-01-01T11:04:00Z --end 2025-01-01T12:04:00Z \
        --select meter_id minute_start_utc energy_wh valid_seconds --batch-size 64

    # machine-readable result (one JSON document)
    python -m minenergy.flight_client ... --json

The module also exposes ``discover_schema``, ``run_query`` and
``query_records`` so the acceptance checks can drive the same code path a
person would.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Iterable

import pyarrow as pa
import pyarrow.flight as fl


@dataclass
class QueryResult:
    query_id: str | None
    schema: pa.Schema
    batches: list[pa.RecordBatch] = field(default_factory=list)
    batch_metadata: list[dict] = field(default_factory=list)

    @property
    def rows(self) -> int:
        return sum(b.num_rows for b in self.batches)

    def table(self) -> pa.Table:
        return pa.Table.from_batches(self.batches, schema=self.schema)

    def records(self) -> list[dict]:
        return self.table().to_pylist()

    def column_types(self) -> dict[str, str]:
        return {f.name: str(f.type) for f in self.schema}


def connect(uri: str) -> fl.FlightClient:
    if uri.startswith("grpc://") or uri.startswith("grpc+tls://"):
        return fl.FlightClient(uri)
    host, _, port = uri.rpartition(":")
    return fl.FlightClient((host or "127.0.0.1", int(port)))


def discover_schema(client: fl.FlightClient) -> pa.Schema:
    return client.get_schema(fl.FlightDescriptor.for_path("facts")).schema


def discover_query_schema(client: fl.FlightClient, query: dict) -> pa.Schema:
    descriptor = fl.FlightDescriptor.for_command(_query_bytes(query))
    return client.get_flight_info(descriptor).schema


def _query_bytes(query: dict | str) -> bytes:
    if isinstance(query, str):
        return query.encode()
    return json.dumps(query, separators=(",", ":")).encode()


def query_records(client: fl.FlightClient, query: dict) -> dict:
    """Run a small query and return its rows plus a description of the RPC."""
    info = client.get_flight_info(fl.FlightDescriptor.for_command(_query_bytes(query)))
    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)
    meta = json.loads(info.app_metadata.decode()) if info.app_metadata else {}
    result = QueryResult(
        query_id=meta.get("query_id"),
        schema=reader.schema,
        batches=[],
    )
    for chunk in reader:
        if chunk.data is not None:
            result.batches.append(chunk.data)
            result.batch_metadata.append(
                json.loads(chunk.app_metadata.to_pybytes().decode()) if chunk.app_metadata else {}
            )
    return {
        "query_id": result.query_id,
        "schema": [{"name": f.name, "type": str(f.type)} for f in result.schema],
        "rows": result.rows,
        "batches": len(result.batches),
        "batch_rows": [b.num_rows for b in result.batches],
        "batch_metadata": result.batch_metadata,
        "data": result.records(),
    }


def run_query(
    client: fl.FlightClient,
    query: dict,
    limit_batches: int | None = None,
) -> QueryResult:
    """Submit the query and stream its batches (optionally stop early)."""
    info = client.get_flight_info(fl.FlightDescriptor.for_command(_query_bytes(query)))
    reader = client.do_get(info.endpoints[0].ticket)
    meta = json.loads(info.app_metadata.decode()) if info.app_metadata else {}
    result = QueryResult(query_id=meta.get("query_id"), schema=reader.schema)
    for chunk in reader:
        if chunk.data is None:
            continue
        result.batches.append(chunk.data)
        result.batch_metadata.append(
            json.loads(chunk.app_metadata.to_pybytes().decode()) if chunk.app_metadata else {}
        )
        if limit_batches is not None and len(result.batches) >= limit_batches:
            break
    return result


def _json_body(body) -> dict:
    if hasattr(body, "to_pybytes"):
        body = body.to_pybytes()
    return json.loads(body)


def server_stats(client: fl.FlightClient) -> dict:
    replies = client.do_action(fl.Action("minenergy.stats", json.dumps({"limit": 32}).encode()))
    return _json_body(next(iter(replies)).body)


def server_health(client: fl.FlightClient) -> dict:
    replies = client.do_action(fl.Action("minenergy.health", b""))
    return _json_body(next(iter(replies)).body)


def expect_param_error(fn) -> dict:
    """Run an RPC that must fail; returns ``{error_type, message}``.

    Success is reported as ``{"error_type": None}`` so callers can assert a
    clean rejection rather than an accidental success.
    """
    try:
        value = fn()
        if hasattr(value, "read_all"):
            value.read_all()
        elif hasattr(value, "read"):
            list(value)
        return {"error_type": None, "message": "no error"}
    except Exception as exc:  # the RPC layer's exception family is pyarrow-specific
        message = str(exc)
        return {
            "error_type": type(exc).__name__,
            "message": message,
            "is_param_error": "MINENERGY_PARAM_INVALID" in message,
            "grpc_invalid_argument": "INVALID_ARGUMENT" in message,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy.flight_client")
    parser.add_argument("--uri", default="grpc://127.0.0.1:8815", help="server URI")
    parser.add_argument("--schema", action="store_true", help="print the full fact schema")
    parser.add_argument("--meters", nargs="+", default=None)
    parser.add_argument("--start", default=None, help="inclusive ISO-8601 timestamp with offset")
    parser.add_argument("--end", default=None, help="exclusive ISO-8601 timestamp with offset")
    parser.add_argument("--select", nargs="+", default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--json", action="store_true", help="emit one JSON document")
    parser.add_argument("--max-rows", type=int, default=20, help="rows shown by the text view")
    parser.add_argument("--stats", action="store_true", help="also print server stats")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = connect(args.uri)
    try:
        if args.schema:
            schema = discover_schema(client)
            if args.json:
                print(json.dumps([{"name": f.name, "type": str(f.type)} for f in schema]))
            else:
                print(f"{len(schema)} fact columns on {args.uri}:")
                for f in schema:
                    print(f"  {f.name:<24} {f.type}")
            return 0
        if not (args.meters and args.start and args.end):
            print("query needs --meters, --start and --end (or use --schema)", file=sys.stderr)
            return 2
        query = {"meters": args.meters, "start": args.start, "end": args.end}
        if args.select:
            query["select"] = args.select
        if args.batch_size is not None:
            query["batch_size"] = args.batch_size
        result = query_records(client, query)
        if args.stats:
            result["server_stats"] = server_stats(client)
        if args.json:
            print(json.dumps(result, default=str))
            return 0
        print(f"query {result['query_id']}: {result['rows']} rows in {result['batches']} batches")
        for f in result["schema"]:
            print(f"  column {f['name']:<24} {f['type']}")
        print(f"  batch sizes: {result['batch_rows'][:12]}")
        for row in result["data"][: args.max_rows]:
            print("  ", row)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
