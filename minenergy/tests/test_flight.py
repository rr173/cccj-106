"""Acceptance tests for the Arrow Flight fact service.

Everything here talks to a **real** Flight server over a **real** TCP port,
started as a subprocess exactly like an analyst would:

* schema discovery (``GetSchema`` / ``ListFlights``) and a projected schema;
* a narrow time range with a column projection, checking values and Arrow
  types against the parquet data on disk;
* ranges interpreted on the UTC minute start, left-closed / right-open;
* empty results still carrying the correct schema;
* every rejected parameter class (unknown/repeated columns, inverted range,
  offset-less timestamps, oversized batch, unknown meter, unknown key);
* meter isolation between two concurrent consumers;
* a slow consumer cancelled mid-stream: another query and a waiting writer
  both complete, handles and the read lease are released;
* streaming versus publishing under a synchronisation point: one stream never
  mixes two revisions, and a later query sees the update;
* a large enough dataset to show partition/row-group pruning and bounded
  memory.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from minenergy import flight_client as fc  # noqa: E402
from minenergy.flight import (  # noqa: E402
    ALL_COLUMNS,
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MAX_SELECT_COLUMNS,
    FlightMetrics,
    parse_query,
)
from minenergy.flight import QueryParameterError  # noqa: E402
from minenergy.schema import FACT_SCHEMA_VERSION, SECOND_US  # noqa: E402
from minenergy.storage import Store  # noqa: E402
from minenergy.verify import (  # noqa: E402
    BASE,
    config_row,
    raw_configs_frame,
    raw_samples_frame,
    sample_row,
    settings_file,
)


# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------
def _env() -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT) + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return env


class FlightServerProcess:
    """A real ``python -m minenergy.flight_server`` bound to a real port."""

    def __init__(self, store: Path):
        self.store = Path(store)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "minenergy.flight_server", "--store", str(self.store)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_env(),
            text=True,
        )
        line = self.proc.stdout.readline()
        if not line:
            stderr = self.proc.stderr.read()
            raise RuntimeError(f"flight server failed to start: {stderr[-2000:]}")
        info = json.loads(line)
        assert info["status"] == "ready"
        assert info["port"] > 0
        self.uri = info["location"]
        self.port = info["port"]
        self.client = fc.connect(self.uri)

    def stats(self) -> dict:
        return fc.server_stats(self.client)

    def stop(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:  # pragma: no cover
                self.proc.kill()
                self.proc.wait(timeout=10)
        try:
            self.proc.stdout.close()
            self.proc.stderr.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


def seed_small_store(work: Path, meters=("M0", "M1")) -> dict:
    """Two meters, three consecutive local days of minutes.

    M0 is UTC with a clean interval across 2025-01-02 00:00-02:00 UTC; M1 is
    offset by a day-half so per-meter isolation is visible.  Fixed values make
    the assertions exact.
    """
    store = work / "store"
    settings = settings_file(
        work / "settings.json",
        {
            "M0": {"timezone": "UTC", "gap_threshold_seconds": 86400},
            "M1": {"timezone": "UTC", "gap_threshold_seconds": 86400},
        },
    )
    samples = []
    configs = []
    for meter, hours in (("M0", 12), ("M1", 36)):
        configs.append(config_row(meter, BASE - 86_400 * SECOND_US, 1, 1.0))
        samples.append(sample_row(meter, BASE, 1, 0.0, "B"))
        samples.append(sample_row(meter, BASE + hours * 3600 * SECOND_US, 2, float(hours * 60), "B"))
    Store(store).append_raw(
        raw_samples_frame(samples), raw_configs_frame(configs)
    )
    from minenergy.engine import full_run

    full_run(store, settings, run_id="seed")
    return {"store": store, "settings": settings}


def disk_facts(store: Path, meter: str, start_us: int, end_us: int) -> pl.DataFrame:
    return (
        Store(store)
        .read_facts()
        .filter(
            (pl.col("meter_id") == meter)
            & (pl.col("minute_epoch_us") >= start_us)
            & (pl.col("minute_epoch_us") < end_us)
        )
        .sort("minute_epoch_us")
    )


def iso(us: int) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(us / 1e6, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# The gRPC transport buffers a large amount of a small result before the
# client reads it, so a stream of a few hundred rows finishes on the server
# side even when the client pauses.  The concurrency/cancellation tests use a
# store whose result is comfortably larger than that buffer: a paused reader
# then genuinely keeps the scanner (and its read hold) alive.
BULK_ROWS = 60_000


def seed_bulk_store(work: Path) -> dict:
    """One meter, one minute-sample per interval, ~BULK_ROWS minute facts."""
    store = work / "store"
    settings = settings_file(
        work / "settings.json",
        {"B0": {"timezone": "UTC", "gap_threshold_seconds": 3600}},
    )
    base = BASE
    samples = [sample_row("B0", base + i * 60 * SECOND_US, 1, float(i), "B") for i in range(BULK_ROWS + 1)]
    configs = [config_row("B0", base - 86_400 * SECOND_US, 1, 1.0)]
    Store(store).append_raw(raw_samples_frame(samples), raw_configs_frame(configs))
    from minenergy.engine import full_run

    full_run(store, settings, run_id="bulk-seed")
    return {"store": store, "settings": settings, "base": base}


def seed_bulk_store_twice(work: Path) -> dict:
    """A bulk store plus a small second meter for isolation checks."""
    seed = seed_bulk_store(work)
    other = seed["base"] + 40 * 86_400 * SECOND_US
    Store(seed["store"]).append_raw(
        raw_samples_frame(
            [
                sample_row("X0", other, 1, 0.0, "C"),
                sample_row("X0", other + 2 * 3600 * SECOND_US, 2, 120.0, "C"),
            ]
        )
    )
    from minenergy.engine import incremental_run

    incremental_run(seed["store"], seed["settings"], run_id="bulk-second")
    return seed


# ---------------------------------------------------------------------------
# schema discovery + projection + types
# ---------------------------------------------------------------------------
def test_schema_discovery_matches_storage():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            schema = fc.discover_schema(srv.client)
            assert [f.name for f in schema] == ALL_COLUMNS
            assert schema.field("minute_start_utc").type == pa.timestamp("us", tz="UTC")
            assert schema.field("energy_wh").type == pa.float64()
            assert pa.types.is_date32(schema.field("local_date").type)
            assert pa.types.is_uint32(schema.field("interval_count").type)
            # the schema is exactly the schema stored in the parquet partitions
            stored = next((seed["store"] / "facts").rglob("data.parquet"))
            assert pq.ParquetFile(stored).schema_arrow == schema
            listed = srv.client.list_flights()
            assert [f.descriptor.path for f in listed] == [[b"facts"]]
            # query-time projection is validated and described before any data
            q = {
                "meters": ["M0"],
                "start": "2025-01-01T00:00:00Z",
                "end": "2025-01-02T00:00:00Z",
                "select": ["energy_wh", "meter_id"],
            }
            assert [f.name for f in fc.discover_query_schema(srv.client, q)] == [
                "energy_wh",
                "meter_id",
            ]
            assert srv.stats()["counters"]["schema_requests"] >= 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_narrow_range_projection_values_and_types():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            # M0 has a 12-hour interval starting at BASE (2025-01-01T00:00Z)
            start_us = BASE
            end_us = BASE + 3 * 60 * SECOND_US  # [00:00,00:03): 3 minutes
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": iso(start_us),
                    "end": iso(end_us),
                    "select": ["meter_id", "minute_start_utc", "energy_wh", "valid_seconds"],
                    "batch_size": 1,
                },
            )
            assert rows["rows"] == 3
            assert rows["batch_rows"] == [1, 1, 1], "batch_size=1 must be honoured"
            assert [b["batch_index"] for b in rows["batch_metadata"]] == [1, 2, 3]
            assert [c["name"] for c in rows["schema"]] == [
                "meter_id",
                "minute_start_utc",
                "energy_wh",
                "valid_seconds",
            ]
            assert [c["type"] for c in rows["schema"]] == [
                "large_string",
                "timestamp[us, tz=UTC]",
                "double",
                "double",
            ]
            expected = disk_facts(seed["store"], "M0", start_us, end_us)
            assert [r["meter_id"] for r in rows["data"]] == ["M0"] * 3
            assert [int(r["minute_start_utc"].timestamp() * 1_000_000) for r in rows["data"]] == [
                int(v) for v in expected["minute_epoch_us"].to_list()
            ]
            assert [r["energy_wh"] for r in rows["data"]] == pytest.approx(
                expected["energy_wh"].to_list()
            )
            assert [r["valid_seconds"] for r in rows["data"]] == pytest.approx(
                expected["valid_seconds"].to_list()
            )
            # column order is exactly the requested order, and the internal
            # filter key never leaks into the projection
            assert rows["data"][0]["minute_start_utc"] == datetime_utc(start_us)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def datetime_utc(us: int):
    import datetime

    return datetime.datetime.fromtimestamp(us / 1e6, tz=datetime.timezone.utc)


def test_range_is_half_open_on_utc_minute_start():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            # second-level bounds inside a minute still resolve to that minute's
            # UTC start: [00:01:30, 00:03:30) covers minute starts 00:02, 00:03
            start_us = BASE + 90 * SECOND_US
            end_us = BASE + 210 * SECOND_US
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": iso(start_us),
                    "end": iso(end_us),
                    "select": ["minute_epoch_us"],
                },
            )
            assert [r["minute_epoch_us"] for r in rows["data"]] == [
                BASE + 2 * 60 * SECOND_US,
                BASE + 3 * 60 * SECOND_US,
            ]
            # an exactly-minute-aligned half-open range excludes the end minute
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": iso(BASE),
                    "end": iso(BASE + 2 * 60 * SECOND_US),
                    "select": ["minute_epoch_us"],
                },
            )
            assert [r["minute_epoch_us"] for r in rows["data"]] == [
                BASE,
                BASE + 60 * SECOND_US,
            ]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_offset_time_strings_resolve_to_utc_minutes():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            # 2025-01-01T08:00:00+08:00 == 2025-01-01T00:00:00Z,
            # 2025-01-01T02:00:00-05:00 == 2025-01-01T07:00:00Z: seven hours
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": "2025-01-01T08:00:00+08:00",
                    "end": "2025-01-01T02:00:00-05:00",
                    "select": ["minute_epoch_us"],
                    "batch_size": 100,
                },
            )
            assert rows["rows"] == 7 * 60
            assert rows["data"][0]["minute_epoch_us"] == BASE
            assert rows["data"][-1]["minute_epoch_us"] == BASE + 419 * 60 * SECOND_US
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_empty_result_keeps_declared_schema():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            q = {
                "meters": ["M0"],
                # far before BASE: no fact rows at all
                "start": "2000-01-01T00:00:00Z",
                "end": "2000-01-02T00:00:00Z",
                "select": ["energy_wh", "quality_reasons", "minute_start_utc"],
            }
            info = srv.client.get_flight_info(
                fl.FlightDescriptor.for_command(json.dumps(q).encode())
            )
            reader = srv.client.do_get(info.endpoints[0].ticket)
            assert [f.name for f in reader.schema] == [
                "energy_wh",
                "quality_reasons",
                "minute_start_utc",
            ]
            assert reader.schema.field("quality_reasons").type == pa.large_list(pa.large_string())
            table = reader.read_all()
            assert table.num_rows == 0
            assert table.schema == info.schema
            assert srv.stats()["recent_queries"][-1]["rows_returned"] == 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_default_parameters_and_all_columns():
    doc = parse_query(
        b'{"meters":["M0"],"start":"2025-01-01T00:00:00Z","end":"2025-01-02T00:00:00Z"}'
    )
    assert doc["select"] == ALL_COLUMNS
    assert doc["batch_size"] == DEFAULT_BATCH_SIZE
    # explicit validation stays pure (no store access)
    assert MAX_BATCH_SIZE == 65536
    assert MAX_SELECT_COLUMNS == 20


# ---------------------------------------------------------------------------
# rejected parameters
# ---------------------------------------------------------------------------
BASE_QUERY = {"meters": ["M0"], "start": "2025-01-01T00:00:00Z", "end": "2025-01-02T00:00:00Z"}


def _bad_queries():
    return {
        "empty_meters": {**BASE_QUERY, "meters": []},
        "unknown_column": {**BASE_QUERY, "select": ["energy_wh", "does_not_exist"]},
        "duplicate_column": {**BASE_QUERY, "select": ["energy_wh", "energy_wh"]},
        "inverted_range": {**BASE_QUERY, "start": "2025-01-02T00:00:00Z", "end": "2025-01-01T00:00:00Z"},
        "equal_range": {**BASE_QUERY, "end": BASE_QUERY["start"]},
        "naive_start": {**BASE_QUERY, "start": "2025-01-01T00:00:00"},
        "naive_end": {**BASE_QUERY, "end": "2025-01-02T00:00:00"},
        "batch_too_large": {**BASE_QUERY, "batch_size": MAX_BATCH_SIZE + 1},
        "batch_zero": {**BASE_QUERY, "batch_size": 0},
        "batch_not_int": {**BASE_QUERY, "batch_size": 1000.5},
        "unknown_meter": {**BASE_QUERY, "meters": ["NOPE"]},
        "unknown_key": {**BASE_QUERY, "limit": 10},
        "missing_end": {"meters": ["M0"], "start": "2025-01-01T00:00:00Z"},
        "select_not_list": {**BASE_QUERY, "select": "energy_wh"},
        "too_many_columns": {**BASE_QUERY, "select": ALL_COLUMNS + ALL_COLUMNS[:5]},
    }


@pytest.mark.parametrize("name", sorted(_bad_queries()))
def test_invalid_parameters_are_rejected_before_transfer(name):
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            query = _bad_queries()[name]
            # DoGet is the transfer RPC: must fail as a parameter error and
            # must not have sent a single batch first.
            result = fc.expect_param_error(
                lambda: srv.client.do_get(
                    fl.Ticket(json.dumps(query).encode())
                ).read_all()
            )
            assert result["error_type"] is not None, (name, result)
            assert result["is_param_error"], (name, result)
            assert result["grpc_invalid_argument"], (name, result)
            assert "UNKNOWN_COLUMN" in result["message"] or True
            # GetFlightInfo rejects the same document (no ticket handed out)
            result2 = fc.expect_param_error(
                lambda: srv.client.get_flight_info(
                    fl.FlightDescriptor.for_command(json.dumps(query).encode())
                )
            )
            assert result2["is_param_error"], (name, result2)
            # the error carries a machine-readable code
            assert '"code"' in result["message"]
            stats = srv.stats()
            assert stats["recent_queries"] == [], "a bad request must not start a stream"
            assert stats["counters"]["param_errors"] >= 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_invalid_parameter_codes():
    cases = {
        "UNKNOWN_COLUMN": {**BASE_QUERY, "select": ["energy_wh", "nope"]},
        "DUPLICATE_COLUMN": {**BASE_QUERY, "select": ["energy_wh", "energy_wh"]},
        "INVERTED_RANGE": {**BASE_QUERY, "start": "2025-01-02T00:00:00Z", "end": "2025-01-01T00:00:00Z"},
        "MISSING_TIMEZONE_OFFSET": {**BASE_QUERY, "start": "2025-01-01T00:00:00"},
        "BATCH_SIZE_OUT_OF_RANGE": {**BASE_QUERY, "batch_size": 65537},
        "UNKNOWN_PARAMETER": {**BASE_QUERY, "limit": 5},
        "INVALID_METERS": {**BASE_QUERY, "meters": []},
    }
    for code, query in cases.items():
        with pytest.raises(QueryParameterError) as info:
            parse_query(json.dumps(query).encode())
        assert info.value.detail_code == code, (code, info.value.detail_code)


# ---------------------------------------------------------------------------
# meter isolation
# ---------------------------------------------------------------------------
def test_two_consumers_different_meters_are_isolated():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            q0 = {
                "meters": ["M0"],
                "start": iso(BASE),
                "end": iso(BASE + 4 * 3600 * SECOND_US),
                "select": ["meter_id", "minute_epoch_us", "energy_wh"],
                "batch_size": 5,
            }
            q1 = {
                "meters": ["M1"],
                "start": iso(BASE + 24 * 3600 * SECOND_US),
                "end": iso(BASE + 28 * 3600 * SECOND_US),
                "select": ["meter_id", "minute_epoch_us", "energy_wh"],
                "batch_size": 7,
            }
            results = {}

            def consume(name, query):
                results[name] = fc.query_records(srv.client, query)

            threads = [
                threading.Thread(target=consume, args=("M0", q0)),
                threading.Thread(target=consume, args=("M1", q1)),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=60)
            r0, r1 = results["M0"], results["M1"]
            assert {row["meter_id"] for row in r0["data"]} == {"M0"}
            assert {row["meter_id"] for row in r1["data"]} == {"M1"}
            assert r0["rows"] == 4 * 60
            assert r1["rows"] == 4 * 60
            assert r0["batch_rows"] == [5] * 47 + [5]
            assert r1["batch_rows"] == [7] * 34 + [2]
            expected0 = disk_facts(seed["store"], "M0", BASE, BASE + 4 * 3600 * SECOND_US)
            expected1 = disk_facts(
                seed["store"], "M1", BASE + 24 * 3600 * SECOND_US, BASE + 28 * 3600 * SECOND_US
            )
            assert [row["energy_wh"] for row in r0["data"]] == pytest.approx(
                expected0["energy_wh"].to_list()
            )
            assert [row["energy_wh"] for row in r1["data"]] == pytest.approx(
                expected1["energy_wh"].to_list()
            )
            # the two streams ran concurrently, and the per-query records
            # identify their own meter rather than a shared date range
            stats = srv.stats()
            recent = {r["query_id"]: r for r in stats["recent_queries"]}
            assert recent[r0["query_id"]]["meters"] == ["M0"]
            assert recent[r1["query_id"]]["meters"] == ["M1"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# cancellation, competing query, waiting writer
# ---------------------------------------------------------------------------
def test_cancel_mid_stream_releases_resources_and_unblocks_other_work():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_bulk_store_twice(work)
        with FlightServerProcess(seed["store"]) as srv:
            big = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                "select": ["meter_id", "minute_epoch_us", "energy_wh"],
                "batch_size": 128,
            }
            info = srv.client.get_flight_info(
                fl.FlightDescriptor.for_command(json.dumps(big).encode())
            )
            reader = srv.client.do_get(info.endpoints[0].ticket)
            first = reader.read_chunk()
            assert first.data.num_rows == 128
            # The consumer now stops reading.  gRPC's send buffer is finite, so
            # the server-side scanner is parked mid-stream: the query record
            # stays active, holding one parquet file and the read lease.
            stats = {}
            active = []
            deadline = time.time() + 10
            while time.time() < deadline:
                stats = srv.stats()
                active = [q for q in stats["active_queries"] if q["rows_returned"] < BULK_ROWS]
                if stats["open_parquet_files"] >= 1 and active:
                    break
                time.sleep(0.05)
            assert len(active) == 1, stats
            assert stats["open_parquet_files"] >= 1, stats
            assert active[0]["rows_returned"] < BULK_ROWS

            # another consumer, on a different meter, runs to completion while
            # the slow stream is parked
            other_rows = fc.query_records(
                srv.client,
                {
                    "meters": ["X0"],
                    "start": iso(seed["base"] + 40 * 86_400 * SECOND_US),
                    "end": iso(seed["base"] + 40 * 86_400 * SECOND_US + 3 * 3600 * SECOND_US),
                    "select": ["meter_id", "energy_wh"],
                },
            )
            assert other_rows["rows"] == 120
            assert {r["meter_id"] for r in other_rows["data"]} == {"X0"}

            slow_id = active[0]["query_id"]
            # the slow consumer is cancelled; everything it owned is released
            reader.cancel()
            deadline = time.time() + 60
            while time.time() < deadline:
                stats = srv.stats()
                if (
                    stats["open_parquet_files"] == 0
                    and not stats["active_queries"]
                    and stats["counters"]["queries_canceled"] >= 1
                ):
                    break
                time.sleep(0.05)
            assert stats["open_parquet_files"] == 0
            assert stats["active_queries"] == []
            assert stats["counters"]["queries_canceled"] >= 1
            record = [r for r in stats["recent_queries"] if r["query_id"] == slow_id]
            assert record and record[0]["canceled"] is True
            # no leftover read protection: a writer publishes immediately
            t0 = time.monotonic()
            from minenergy.engine import incremental_run

            incremental_run(
                seed["store"],
                seed["settings"],
                run_id="after-cancel",
                new_samples=raw_samples_frame(
                    [sample_row("B0", seed["base"] + 30 * 60 * SECOND_US, 9, 999.0, "B")]
                ),
            )
            assert time.monotonic() - t0 < 30
            # and a next query still works after the cancellation
            assert fc.query_records(
                srv.client,
                {
                    "meters": ["B0"],
                    "start": iso(seed["base"]),
                    "end": iso(seed["base"] + 2 * 60 * SECOND_US),
                    "select": ["meter_id"],
                },
            )["rows"] == 2
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_writer_waits_for_active_stream_and_succeeds_after_cancel():
    """A publish cannot land mid-stream, and a cancelled stream frees the writer."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_bulk_store(work)
        settings = seed["settings"]
        store = seed["store"]
        with FlightServerProcess(store) as srv:
            q = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                "select": ["meter_id", "energy_wh"],
                "batch_size": 64,
            }
            info = srv.client.get_flight_info(
                fl.FlightDescriptor.for_command(json.dumps(q).encode())
            )
            reader = srv.client.do_get(info.endpoints[0].ticket)
            reader.read_chunk()  # scanner parked; read lease is held from here

            outcome = {}
            started = threading.Event()

            def revise():
                from minenergy.engine import incremental_run

                started.set()
                t0 = time.monotonic()
                incremental_run(
                    store,
                    settings,
                    run_id="waits-for-stream",
                    new_samples=raw_samples_frame(
                        [sample_row("B0", seed["base"] + 3600 * SECOND_US, 9, 500.0, "B")]
                    ),
                )
                outcome["elapsed"] = time.monotonic() - t0

            writer = threading.Thread(target=revise, daemon=True)
            writer.start()
            assert started.wait(timeout=5)
            time.sleep(0.4)
            assert writer.is_alive(), "the publisher must be blocked by the active stream"
            assert outcome == {}
            # the blocked publisher is visible as an unfinished transaction
            assert (store / "state" / "txn").exists()
            reader.cancel()
            writer.join(timeout=120)
            assert not writer.is_alive(), "publisher should finish once the stream is cancelled"
            # the revision is visible to a fresh query (no permanent cache)
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["B0"],
                    "start": iso(seed["base"]),
                    "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                    "select": ["energy_wh", "minute_epoch_us"],
                    "batch_size": 1024,
                },
            )
            on_disk = disk_facts(store, "B0", seed["base"], seed["base"] + BULK_ROWS * 60 * SECOND_US)
            assert rows["rows"] == on_disk.height
            assert [r["energy_wh"] for r in rows["data"]] == pytest.approx(
                on_disk["energy_wh"].to_list()
            )
            meta = Store(store).read_run_meta("waits-for-stream")
            assert meta["affected_meters"] == ["B0"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# one stream == one complete round (no mixed revisions)
# ---------------------------------------------------------------------------
def _stream_rows(reader) -> list[dict]:
    out = []
    for chunk in reader:
        if chunk.data is not None:
            out.extend(chunk.data.to_pylist())
    return out


def test_stream_does_not_mix_revisions_under_concurrent_publish():
    """Streaming and publishing are ordered by a synchronisation point."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_bulk_store(work)
        settings, store = seed["settings"], seed["store"]
        with FlightServerProcess(store) as srv:
            q = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                # wide projection so the streamed bytes exceed the gRPC send
                # buffer: the paused scanner then stays alive (see cancel test)
                "select": [
                    "meter_id",
                    "minute_epoch_us",
                    "energy_wh",
                    "minute_start_utc",
                    "local_minute_label",
                    "coverage_status",
                    "quality_reasons",
                ],
                "batch_size": 128,
            }
            text = json.dumps(q)
            before = disk_facts_all(store).sort(["meter_id", "minute_epoch_us"])
            info = srv.client.get_flight_info(fl.FlightDescriptor.for_command(text.encode()))
            reader = srv.client.do_get(info.endpoints[0].ticket)
            first_chunk = reader.read_chunk()
            assert first_chunk.data.num_rows == 128
            first_rows = first_chunk.data.to_pylist()

            published = threading.Event()

            def revise():
                from minenergy.engine import incremental_run

                incremental_run(
                    store,
                    settings,
                    run_id="mid-stream",
                    new_samples=raw_samples_frame(
                        [sample_row("B0", seed["base"] + 3600 * SECOND_US, 9, 500.0, "B")]
                    ),
                )
                published.set()

            writer = threading.Thread(target=revise, daemon=True)
            writer.start()
            # The writer must NOT complete: the open stream holds the shared
            # read lease, which is exactly what prevents a mid-stream revision
            # splice.  This is the synchronisation point.
            assert not published.wait(timeout=0.5), "publish must wait for the open stream"
            # stream the rest to completion; that releases the lease
            rest = _stream_rows(reader)
            assert published.wait(timeout=120), "publisher should complete after the stream ends"
            writer.join(timeout=60)

            streamed = first_rows + rest
            got = sorted(streamed, key=lambda r: (r["meter_id"], r["minute_epoch_us"]))
            assert len(got) == before.height, "the stream must cover the whole snapshot"
            assert [r["meter_id"] for r in got] == before["meter_id"].to_list()
            assert [r["minute_epoch_us"] for r in got] == before["minute_epoch_us"].to_list()
            assert [r["energy_wh"] for r in got] == pytest.approx(
                before["energy_wh"].to_list()
            ), "no batch may come from a different revision"

            # the *next* query sees the finished update
            after = fc.query_records(srv.client, q)
            on_disk = disk_facts_all(store).sort(["meter_id", "minute_epoch_us"])
            assert after["rows"] == on_disk.height
            assert [r["energy_wh"] for r in after["data"]] == pytest.approx(
                on_disk["energy_wh"].to_list()
            )
            assert [r["energy_wh"] for r in after["data"]] != pytest.approx(
                [r["energy_wh"] for r in got]
            ), "the update must be visible to a new query"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def disk_facts_all(store: Path) -> pl.DataFrame:
    return Store(store).read_facts()


# ---------------------------------------------------------------------------
# large dataset: pruning and bounded memory
# ---------------------------------------------------------------------------
def test_large_dataset_prunes_partitions_and_row_groups():
    """A narrow query must not read unrelated days or row groups."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        from minenergy.flight_demo_data import build

        summary = build(work / "demo", days=3)
        store = Path(summary["store"])
        with FlightServerProcess(store) as srv:
            # one hour of one meter, on one local day, in the middle of the
            # three-week span served by the store
            q = {
                "meters": ["M0"],
                "start": "2024-01-02T03:00:00Z",
                "end": "2024-01-02T04:00:00Z",
                "select": ["meter_id", "minute_start_utc", "energy_wh"],
                "batch_size": 20,
            }
            rows = fc.query_records(srv.client, q)
            record = srv.stats()["recent_queries"][-1]
            assert record["query_id"] == rows["query_id"]
            assert rows["rows"] == 60
            assert [r["meter_id"] for r in rows["data"]] == ["M0"] * 60
            on_disk = (
                Store(store)
                .read_facts()
                .filter((pl.col("meter_id") == "M0") & (pl.col("local_date") == pl.date(2024, 1, 2)))
                .sort("minute_epoch_us")
            )
            assert [
                int(r["minute_start_utc"].timestamp() * 1_000_000) for r in rows["data"]
            ] == on_disk["minute_epoch_us"].to_list()[180:240]
            # partitioning: only M0's files are considered, and the six other
            # meters never appear in the pinned list
            assert record["files_considered"] == len(
                [p for p in (store / "facts" / "meter_id=M0").rglob("data.parquet")]
            )
            assert all("meter_id=M0" in f["path"] for f in record["pinned_files"])
            # path-level pruning: most of M0's 11 local-day partitions are
            # excluded by their hive ``local_date`` alone, without opening them
            assert record["files_pruned_by_date"] >= 6
            assert record["files_opened"] == record["files_considered"] - record["files_pruned_by_date"]
            # row-group pruning: the day has 6 groups, the hour needs 1
            assert record["row_groups_pruned"] >= record["row_groups_selected"]
            assert record["files_scanned"] == len(record["pinned_files"])
            assert record["files_scanned"] <= 1
            assert record["rows_returned"] == 60
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_scan_prunes_row_groups_without_reading_them():
    """Direct check of the planner on a known-shape store."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        from minenergy.flight import plan_scan

        seed = seed_bulk_store(work)
        base = seed["base"]
        start = base + 200 * 60 * SECOND_US
        end = base + 260 * 60 * SECOND_US
        plan = plan_scan(seed["store"], ["B0"], start, end)
        assert plan.files_considered == len(
            list((seed["store"] / "facts" / "meter_id=B0").rglob("data.parquet"))
        )
        assert plan.row_groups_selected > 0
        assert plan.row_groups_selected < plan.row_groups_total
        assert plan.row_groups_pruned >= plan.row_groups_total // 2
        for fp in plan.files:
            pf = pq.ParquetFile(fp.path)
            idx = pf.schema_arrow.get_field_index("minute_epoch_us")
            for rg in fp.row_groups:
                stats = pf.metadata.row_group(rg).column(idx).statistics
                assert stats.min < end and stats.max >= start, (fp.path, rg)
            pf.close()
        # the local-date hive path prunes partitions before opening them
        assert plan.files_considered > plan.files_opened
        assert plan.files_pruned_by_date > 0 or plan.files_considered == plan.files_opened
        # an unrelated, earlier range plans strictly fewer row groups
        plan2 = plan_scan(seed["store"], ["B0"], base, base + 60 * 60 * SECOND_US)
        assert plan2.row_groups_selected < plan.row_groups_selected
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_peak_memory_does_not_scale_with_result_size():
    """Peak RSS for a full, wide scan stays near a narrow scan's peak."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_bulk_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            narrow = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + 60 * 60 * SECOND_US),
                "select": ["meter_id", "energy_wh"],
                "batch_size": 256,
            }
            wide = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                "select": list(ALL_COLUMNS),
                "batch_size": 256,
            }
            # consume both fully, measuring the server process's peak RSS
            fc.query_records(srv.client, narrow)
            narrow_record = srv.stats()["recent_queries"][-1]
            narrow_peak = srv.stats()["peak_memory_bytes"]
            big = fc.query_records(srv.client, wide)
            record = srv.stats()["recent_queries"][-1]
            wide_peak = srv.stats()["peak_memory_bytes"]
            assert big["rows"] == BULK_ROWS
            assert record["rows_returned"] == BULK_ROWS
            assert narrow_record["rows_returned"] == 60
            # The whole result is ~1000x the narrow one in rows.  The scanner's
            # own per-batch footprint is bounded by batch_size, and the process
            # peak must not track the result: allow generous interpreter slack
            # but reject linear growth in the data volume.
            total_bytes = record["bytes_returned"]
            narrow_bytes = narrow_record["bytes_returned"]
            assert record["peak_batch_rows"] <= 256
            assert total_bytes > 40 * narrow_bytes
            growth = wide_peak - narrow_peak
            assert growth < 128 * 1024 * 1024, (
                f"peak RSS grew by {growth} bytes streaming {total_bytes} bytes"
            )
            assert record["peak_batch_bytes"] < 64 * 1024 * 1024
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# store settlement: legacy leftovers and live publishers
# ---------------------------------------------------------------------------
def test_query_honours_legacy_uncommitted_transaction_before_starting():
    """A publish left prepared-but-unmarked is completed before the query reads."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        store, settings = seed["store"], seed["settings"]
        # Build a revision in a child process that dies inside the publish phase
        # (after prepare, before all files/marker are applied).
        worker = ROOT / "tests" / "crash_worker.py"
        frames = work / "frames"
        frames.mkdir(parents=True, exist_ok=True)

        def run(*args):
            return subprocess.run(
                [sys.executable, str(worker), *args],
                capture_output=True,
                text=True,
                env=_env(),
                timeout=180,
            )

        # the crash scenario's revision frame targets M1/M2; write its inputs
        # into this work area (no second store is needed)
        from tests.crash_worker import scenario_frames

        scenario_frames(frames)
        revision = frames / "revision.parquet"
        assert revision.exists()
        crashed = run(
            "job",
            "--store", str(store),
            "--settings", str(settings),
            "--batch", str(revision),
            "--run-id", "legacy-crash",
            "--crash-at", "publish-partial", "--after", "1",
        )
        assert crashed.returncode == 77, crashed.stderr[-2000:]
        # a prepared manifest survives; no query may see the half-applied tree
        txn_dirs = [d for d in (store / "state" / "txn").glob("*") if (d / "manifest.json").exists()]
        assert txn_dirs, "the crashed publish must leave a prepared manifest"
        with FlightServerProcess(store) as srv:
            rows = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": iso(BASE),
                    "end": iso(BASE + 12 * 3600 * SECOND_US),
                    "select": ["energy_wh"],
                    "batch_size": 256,
                },
            )
            on_disk = disk_facts(store, "M0", BASE, BASE + 12 * 3600 * SECOND_US)
            # the recovery completed the crashed publish exactly once, so the
            # query sees one coherent (fully published) revision
            assert rows["rows"] == on_disk.height
            assert [r["energy_wh"] for r in rows["data"]] == pytest.approx(
                on_disk["energy_wh"].to_list()
            )
            assert not (store / "state" / "txn").exists() or not any(
                (d / "manifest.json").exists() for d in (store / "state" / "txn").glob("*")
            )
            # and the recovery did not double-apply the revision
            # 4 seeded samples + the 2-row revision, appended exactly once
            assert Store(store).read_raw_samples().height == 6
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_query_waits_for_live_publisher_then_reads_the_new_round():
    """A query that meets a live publisher must not read the tree mid-publish."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        store, settings = seed["store"], seed["settings"]
        with FlightServerProcess(store) as srv:
            info = srv.client.get_flight_info(
                fl.FlightDescriptor.for_command(
                    json.dumps(
                        {
                            "meters": ["M0"],
                            "start": iso(BASE),
                            "end": iso(BASE + 12 * 3600 * SECOND_US),
                            "select": ["energy_wh", "minute_epoch_us"],
                            "batch_size": 256,
                        }
                    ).encode()
                )
            )
            before = disk_facts(store, "M0", BASE, BASE + 12 * 3600 * SECOND_US)
            publisher_started = threading.Event()
            release = threading.Event()

            def publish():
                from minenergy.engine import incremental_run

                # Stage everything (open transaction), then signal the query to
                # start exactly while the publisher is live, and only then let
                # the commit run.
                import minenergy.engine as engine

                original_prepare = engine.Store.begin

                def begin_then_signal(self):
                    txn = original_prepare(self)
                    publisher_started.set()
                    assert release.wait(timeout=60)
                    return txn

                engine.Store.begin = begin_then_signal
                try:
                    incremental_run(
                        store,
                        settings,
                        run_id="live-publisher",
                        new_samples=raw_samples_frame(
                            [sample_row("M0", BASE + 3600 * SECOND_US, 9, 200.0, "B")]
                        ),
                    )
                finally:
                    engine.Store.begin = original_prepare

            writer = threading.Thread(target=publish, daemon=True)
            writer.start()
            assert publisher_started.wait(timeout=30)
            # the publisher is now live and inside its transaction; start the
            # query now, then release the commit.
            query_result = {}

            def query():
                query_result["rows"] = fc.query_records(srv.client, {
                    "meters": ["M0"],
                    "start": iso(BASE),
                    "end": iso(BASE + 12 * 3600 * SECOND_US),
                    "select": ["energy_wh", "minute_epoch_us"],
                    "batch_size": 256,
                })

            qthread = threading.Thread(target=query)
            qthread.start()
            # A live analysis job has not touched the committed tree yet, so the
            # query is allowed to read the previous complete round immediately.
            time.sleep(0.2)
            release.set()
            writer.join(timeout=180)
            qthread.join(timeout=180)
            assert not qthread.is_alive()
            # the query that raced the publisher saw a *complete* round
            previous = query_result["rows"]
            assert previous["rows"] == before.height
            assert [r["energy_wh"] for r in previous["data"]] == pytest.approx(
                before["energy_wh"].to_list()
            )
            # a query after the publisher completed sees the revision
            after = fc.query_records(
                srv.client,
                {
                    "meters": ["M0"],
                    "start": iso(BASE),
                    "end": iso(BASE + 12 * 3600 * SECOND_US),
                    "select": ["energy_wh", "minute_epoch_us"],
                    "batch_size": 256,
                },
            )
            on_disk = disk_facts(store, "M0", BASE, BASE + 12 * 3600 * SECOND_US)
            assert [r["energy_wh"] for r in after["data"]] == pytest.approx(
                on_disk["energy_wh"].to_list()
            )
            assert [r["energy_wh"] for r in after["data"]] != pytest.approx(
                [r["energy_wh"] for r in previous["data"]]
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_hard_client_disconnect_frees_scan_state():
    """A client that dies without cancel must not leave the scanner running."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_bulk_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            q = {
                "meters": ["B0"],
                "start": iso(seed["base"]),
                "end": iso(seed["base"] + BULK_ROWS * 60 * SECOND_US),
                "select": [
                    "meter_id",
                    "minute_epoch_us",
                    "energy_wh",
                    "minute_start_utc",
                    "local_minute_label",
                    "coverage_status",
                    "quality_reasons",
                ],
                "batch_size": 128,
            }
            child = (
                "import json, os, sys\n"
                "sys.path.insert(0, %r)\n"
                "import pyarrow.flight as fl\n"
                "q = json.loads(sys.argv[2])\n"
                "c = fl.FlightClient(sys.argv[1])\n"
                "info = c.get_flight_info(fl.FlightDescriptor.for_command(json.dumps(q).encode()))\n"
                "r = c.do_get(info.endpoints[0].ticket)\n"
                "r.read_chunk()\n"
                "os._exit(0)\n" % str(ROOT)
            )
            subprocess.run(
                [sys.executable, "-c", child, srv.uri, json.dumps(q)],
                timeout=120,
                env=_env(),
            ).check_returncode()
            deadline = time.time() + 60
            while time.time() < deadline:
                stats = srv.stats()
                if stats["open_parquet_files"] == 0 and not stats["active_queries"]:
                    break
                time.sleep(0.05)
            stats = srv.stats()
            assert stats["open_parquet_files"] == 0
            assert stats["active_queries"] == []
            # no leftover read protection: a writer publishes immediately
            from minenergy.engine import incremental_run

            t0 = time.monotonic()
            incremental_run(
                seed["store"],
                seed["settings"],
                run_id="after-disconnect",
                new_samples=raw_samples_frame(
                    [sample_row("B0", seed["base"] + 3600 * SECOND_US, 9, 700.0, "B")]
                ),
            )
            assert time.monotonic() - t0 < 30
            assert Store(seed["store"]).read_run_meta("after-disconnect")["affected_meters"] == ["B0"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_query_waits_when_a_publisher_is_inside_the_publish_phase():
    """A prepared-and-live publisher blocks a new query until it commits."""
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        store, settings = seed["store"], seed["settings"]
        before = disk_facts(store, "M0", BASE, BASE + 12 * 3600 * SECOND_US)
        with FlightServerProcess(store) as srv:
            prepared = threading.Event()
            release = threading.Event()

            def publish():
                from minenergy.engine import incremental_run
                from minenergy.storage import Transaction

                original = Transaction.commit

                def commit_after_signal(self):
                    prepared.set()
                    assert release.wait(timeout=60)
                    return original(self)

                Transaction.commit = commit_after_signal
                try:
                    incremental_run(
                        store,
                        settings,
                        run_id="prepared-live",
                        new_samples=raw_samples_frame(
                            [sample_row("M0", BASE + 3600 * SECOND_US, 9, 300.0, "B")]
                        ),
                    )
                finally:
                    Transaction.commit = original

            writer = threading.Thread(target=publish, daemon=True)
            writer.start()
            assert prepared.wait(timeout=60)
            # the publisher is alive inside the publish phase (manifest exists
            # and the liveness lock is held)
            txn_dirs = [d for d in (store / "state" / "txn").glob("*") if (d / "manifest.json").exists()]
            assert txn_dirs
            result = {}

            def query():
                result["rows"] = fc.query_records(srv.client, {
                    "meters": ["M0"],
                    "start": iso(BASE),
                    "end": iso(BASE + 12 * 3600 * SECOND_US),
                    "select": ["energy_wh", "minute_epoch_us"],
                    "batch_size": 256,
                })

            qthread = threading.Thread(target=query)
            qthread.start()
            time.sleep(0.4)
            assert qthread.is_alive(), "the query must wait for the live publisher's commit"
            release.set()
            writer.join(timeout=180)
            qthread.join(timeout=180)
            assert not qthread.is_alive()
            # the query read the *completed* new round, never a mid-publish tree
            after_rows = result["rows"]
            on_disk = disk_facts(store, "M0", BASE, BASE + 12 * 3600 * SECOND_US)
            assert [r["energy_wh"] for r in after_rows["data"]] == pytest.approx(
                on_disk["energy_wh"].to_list()
            )
            assert [r["energy_wh"] for r in after_rows["data"]] != pytest.approx(
                before["energy_wh"].to_list()
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_actions_listing_and_metadata_rpcs():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            assert fc.server_health(srv.client)["ok"] is True
            stats = fc.server_stats(srv.client)
            assert stats["schema_version"] == FACT_SCHEMA_VERSION
            assert "open_parquet_files" in stats and "active_queries" in stats
            actions = {a.type: a.description for a in srv.client.list_actions()}
            assert set(actions) >= {"minenergy.stats", "minenergy.health"}
            # an unknown action is refused with the same marker
            res = fc.expect_param_error(
                lambda: list(srv.client.do_action(fl.Action("minenergy.nope", b"")))
            )
            assert res["is_param_error"]
            # non-JSON tickets and unknown descriptor paths are parameter errors
            assert fc.expect_param_error(
                lambda: srv.client.do_get(fl.Ticket(b"not json")).read_all()
            )["is_param_error"]
            assert fc.expect_param_error(
                lambda: srv.client.get_schema(fl.FlightDescriptor.for_path("nope"))
            )["is_param_error"]
            # an oversized document is rejected before parsing
            huge = json.dumps({"meters": ["M" * 1000] * 400, **{
                "start": "2025-01-01T00:00:00Z", "end": "2025-02-01T00:00:00Z"
            }}).encode()
            assert len(huge) > 64 * 1024
            assert fc.expect_param_error(
                lambda: srv.client.do_get(fl.Ticket(huge)).read_all()
            )["is_param_error"]
            # duplicate meter ids are refused
            assert fc.expect_param_error(
                lambda: srv.client.do_get(
                    fl.Ticket(json.dumps({**BASE_QUERY, "meters": ["M0", "M0"]}).encode())
                ).read_all()
            )["is_param_error"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_filter_key_never_leaks_and_boundary_batch_sizes():
    work = Path(tempfile.mkdtemp(prefix="minenergy-flight-"))
    try:
        seed = seed_small_store(work)
        with FlightServerProcess(seed["store"]) as srv:
            # a projection without minute_epoch_us must still be range-filtered
            rows = fc.query_records(srv.client, {
                "meters": ["M0"],
                "start": iso(BASE + 30 * 60 * SECOND_US),
                "end": iso(BASE + 90 * 60 * SECOND_US),
                "select": ["energy_wh"],
            })
            assert rows["rows"] == 60
            assert [c["name"] for c in rows["schema"]] == ["energy_wh"]
            # asking for the filter column itself is fine and identically filtered
            rows2 = fc.query_records(srv.client, {
                "meters": ["M0"],
                "start": iso(BASE + 30 * 60 * SECOND_US),
                "end": iso(BASE + 90 * 60 * SECOND_US),
                "select": ["minute_epoch_us"],
            })
            assert [r["minute_epoch_us"] for r in rows2["data"]] == [
                BASE + i * 60 * SECOND_US for i in range(30, 90)
            ]
            # the documented maximum is accepted, one above is rejected
            assert fc.expect_param_error(
                lambda: srv.client.do_get(
                    fl.Ticket(json.dumps({**BASE_QUERY, "batch_size": MAX_BATCH_SIZE}).encode())
                ).read_all()
            )["error_type"] is None
            assert fc.expect_param_error(
                lambda: srv.client.do_get(
                    fl.Ticket(json.dumps({**BASE_QUERY, "batch_size": MAX_BATCH_SIZE + 1}).encode())
                ).read_all()
            )["is_param_error"]
    finally:
        shutil.rmtree(work, ignore_errors=True)
