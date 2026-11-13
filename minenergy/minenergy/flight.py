"""Arrow Flight service over the committed minute-fact parquet tree.

Design goals
------------
* **On-demand scanning.**  The service never materialises the result in
  Polars/Pandas first.  It prunes by meter partition (``meter_id=`` directory),
  then by the hive ``local_date=`` path, then by parquet row-group statistics on
  ``minute_epoch_us``, and only then reads each selected row group in bounded
  batches, filters the range on the raw int64 column, projects the requested
  columns and yields ``RecordBatch`` chunks.  Only the filter key is read in
  addition to the projection, and it never leaks into the output when it was not
  requested.
* **Bounded memory.**  Work is bounded per batch; the scanner never accumulates
  the result.  Peak memory therefore does not grow with the total result size.
  gRPC itself provides the backpressure: while a client does not consume, the
  generator parks before the next batch.
* **Query-scoped consistency behind one admission boundary.**  Every request
  first calls :func:`admit_read`, which (1) settles the store using the
  existing publish protocol *before* looking at the tree -- a half-published
  round is completed, a live publisher is waited for -- and only then (2)
  takes a *shared* hold on the store's publish lock (``state/.txn.lock``, the
  file the commit protocol flocks exclusively) and (3) lets the meter-id
  decision and every directory scan happen while that hold is held.  Because
  a fact file leaves the staging area only while a publisher holds the
  exclusive side of that lock, the shared hold freezes the committed tree for
  the stream's whole life: a publish cannot land between two batches of the
  same stream, and a request can never mix two revisions (the failure where
  recovery ran first, but a publisher moved the first replacement partition
  before the read lock was taken).  Recovery needs the exclusive lock, so it
  always runs on a lock-free stack frame -- the shared hold is released first
  and re-acquired afterwards; the lock is never upgraded in place, which
  avoids the shared/exclusive upgrade deadlock when several readers are
  active.  The hold is released in the stream's ``finally`` clause, so a
  client that stops reading / cancels / disconnects lets the waiting
  publisher proceed.  Nothing is cached: the next request re-lists the tree
  and sees the update that was published meanwhile.
* **Cancellation.**  Client ``cancel``, a disconnected client and server
  shutdown all close the generator; the ``finally`` path closes every parquet
  handle and releases the read hold.  No background work outlives the request.

Parameters (JSON query) and limits
----------------------------------
====================  ==============================  ===========================
key                   meaning                         default / limit
====================  ==============================  ===========================
``meters``            non-empty array of meter ids;   required, <= 256 ids, no
                      each id must have a stored      repeats, each id must
                      ``meter_id=`` partition         exist
``start``             inclusive lower bound on the     required; ISO-8601 with an
                      fact row's UTC minute start     explicit UTC offset (``Z``
                      (``minute_epoch_us``)           or ``+hh:mm``/``+hhmm``)
``end``               exclusive upper bound on the    required; same format;
                      same key                        must be > ``start``
``select``            column names to return, in      ``null``/omitted means all
                      exactly the requested order;    fact columns in storage
                      every name must exist;          order; <= 20 columns
                      repeats are rejected
``batch_size``        target rows per RecordBatch     ``8192``; allowed 1..65536
====================  ==============================  ===========================

Unknown keys, non-array or empty ``meters``, naive timestamps, inverted
ranges, unknown or repeated columns and out-of-range batch sizes are rejected
during validation -- before the scanner exists -- so a bad request never emits
a partial stream.  Parameter failures raise an RPC error whose message begins
``MINENERGY_PARAM_INVALID:`` followed by a JSON body
(``{"code": ..., "message": ...}``); ``DoGet`` surfaces it as gRPC
``INVALID_ARGUMENT``.  The same prefix is used by ``GetFlightInfo`` and
``GetSchema``.

RPC surface
-----------
``GetSchema``
    * ``FlightDescriptor.for_path("facts")`` -> full canonical fact schema.
    * ``FlightDescriptor.for_command(bytes)`` -> projected query schema.
``GetFlightInfo``
    * command descriptor = query JSON -> validated ``FlightInfo`` (projected
      schema, one ticket endpoint, ``app_metadata`` with the query id).
``DoGet``
    * ticket = query JSON (canonicalised by ``GetFlightInfo``) -> stream of
      ``RecordBatch``; each chunk's ``app_metadata`` carries ``query_id`` and
      ``batch_index``.
``DoAction``
    * ``minenergy.stats`` -> counters and the recent query records (files
      considered/pruned/scanned, row groups pruned, rows/bytes sent, peak
      memory, live scanner state).
    * ``minenergy.health`` -> liveness JSON.
``ListActions`` -> the two actions above.
``ListFlights`` -> one ``facts`` flight describing the full schema.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import polars as pl

from .schema import FACT_SCHEMA_VERSION, MINUTE_DTYPES
from .storage import Transaction, recover_pending

PARAM_ERROR_PREFIX = "MINENERGY_PARAM_INVALID:"

DEFAULT_BATCH_SIZE = 8192
MAX_BATCH_SIZE = 65536
MAX_SELECT_COLUMNS = 20
MAX_METERS = 256
MAX_QUERY_BYTES = 64 * 1024
MAX_RECENT_QUERIES = 32

ALL_COLUMNS = list(MINUTE_DTYPES)
FILTER_COLUMN = "minute_epoch_us"
QUERY_KEYS = {"meters", "start", "end", "select", "batch_size"}

ACTION_STATS = "minenergy.stats"
ACTION_HEALTH = "minenergy.health"


class QueryParameterError(ValueError):
    """An invalid query document; never emitted after the stream has begun."""

    def __init__(self, code: str, message: str, **details):
        self.detail_code = code
        self.message = message
        self.details = details
        payload = {
            "code": code,
            "message": message,
            **{k: v for k, v in details.items() if v is not None},
        }
        super().__init__(f"{PARAM_ERROR_PREFIX} {json.dumps(payload, sort_keys=True)}")


# ---------------------------------------------------------------------------
# query parsing / validation
# ---------------------------------------------------------------------------
def _parse_timestamp(value, field_name: str) -> int:
    """ISO-8601 string with an explicit offset -> integer UTC microseconds."""
    if not isinstance(value, str) or not value.strip():
        raise QueryParameterError(
            "INVALID_TIMESTAMP",
            f"{field_name} must be a non-empty ISO-8601 string",
            field=field_name,
        )
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise QueryParameterError(
            "INVALID_TIMESTAMP",
            f"{field_name} is not ISO-8601: {exc}",
            field=field_name,
            value=text,
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QueryParameterError(
            "MISSING_TIMEZONE_OFFSET",
            f"{field_name} must carry an explicit UTC offset (Z or +hh:mm)",
            field=field_name,
            value=text,
        )
    return int(parsed.timestamp() * 1_000_000)


def parse_query(payload: bytes | str | dict) -> dict:
    """Validate and normalise a query document.  Pure: touches no store.

    An optional ``query_id`` key (added to the ticket by ``GetFlightInfo``) is
    ignored by structural validation and handled by ``parse_ticket``.
    """
    if isinstance(payload, dict):
        doc = payload
        if not isinstance(doc, dict):
            raise QueryParameterError("INVALID_QUERY", "query must be a JSON object")
        return _normalise(doc)
    if isinstance(payload, bytes):
        if len(payload) > MAX_QUERY_BYTES:
            raise QueryParameterError(
                "QUERY_TOO_LARGE",
                f"query must be at most {MAX_QUERY_BYTES} bytes",
                limit=MAX_QUERY_BYTES,
            )
        text = payload.decode("utf-8", errors="replace")
    else:
        text = payload
    text = text.strip()
    if not text:
        raise QueryParameterError("INVALID_QUERY", "query must not be empty")
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise QueryParameterError(
            "INVALID_QUERY", f"query is not valid JSON: {exc}"
        ) from None
    if not isinstance(doc, dict):
        raise QueryParameterError("INVALID_QUERY", "query must be a JSON object")
    return _normalise(doc)


def _normalise(doc: dict) -> dict:
    unknown = sorted(set(doc) - QUERY_KEYS)
    if unknown:
        raise QueryParameterError(
            "UNKNOWN_PARAMETER",
            "unsupported query keys",
            keys=unknown,
            supported=sorted(QUERY_KEYS),
        )

    if "meters" not in doc:
        raise QueryParameterError("MISSING_PARAMETER", "meters is required", field="meters")
    meters = doc["meters"]
    if not isinstance(meters, list) or not meters:
        raise QueryParameterError(
            "INVALID_METERS", "meters must be a non-empty array of strings", field="meters"
        )
    if len(meters) > MAX_METERS:
        raise QueryParameterError(
            "TOO_MANY_METERS",
            f"at most {MAX_METERS} meters per query",
            field="meters",
            limit=MAX_METERS,
        )
    for m in meters:
        if not isinstance(m, str) or not m:
            raise QueryParameterError(
                "INVALID_METER_ID",
                "every meter id must be a non-empty string",
                field="meters",
            )
    if len(set(meters)) != len(meters):
        raise QueryParameterError(
            "DUPLICATE_METER", "meter ids must not repeat", field="meters"
        )

    for key in ("start", "end"):
        if key not in doc:
            raise QueryParameterError(
                "MISSING_PARAMETER", f"{key} is required", field=key
            )
    start_us = _parse_timestamp(doc["start"], "start")
    end_us = _parse_timestamp(doc["end"], "end")
    if end_us <= start_us:
        raise QueryParameterError(
            "INVERTED_RANGE",
            "end must be strictly after start",
            field="end",
            start=doc["start"],
            end=doc["end"],
        )

    select = doc.get("select")
    if select is None:
        columns = list(ALL_COLUMNS)
    else:
        if not isinstance(select, list) or not select:
            raise QueryParameterError(
                "INVALID_SELECT",
                "select must be a non-empty array of column names",
                field="select",
            )
        for c in select:
            if not isinstance(c, str) or not c:
                raise QueryParameterError(
                    "INVALID_SELECT",
                    "select entries must be non-empty strings",
                    field="select",
                )
        if len(select) > MAX_SELECT_COLUMNS:
            raise QueryParameterError(
                "TOO_MANY_COLUMNS",
                f"at most {MAX_SELECT_COLUMNS} columns per query",
                field="select",
                limit=MAX_SELECT_COLUMNS,
            )
        dupes = sorted({c for c in select if select.count(c) > 1})
        if dupes:
            raise QueryParameterError(
                "DUPLICATE_COLUMN",
                "select must not repeat a column",
                field="select",
                columns=dupes,
            )
        unknown_cols = sorted(set(select) - set(ALL_COLUMNS))
        if unknown_cols:
            raise QueryParameterError(
                "UNKNOWN_COLUMN",
                "select refers to unknown fact columns",
                field="select",
                columns=unknown_cols,
                available=ALL_COLUMNS,
            )
        columns = list(select)

    batch_size = doc.get("batch_size", DEFAULT_BATCH_SIZE)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise QueryParameterError(
            "INVALID_BATCH_SIZE", "batch_size must be an integer", field="batch_size"
        )
    if batch_size < 1 or batch_size > MAX_BATCH_SIZE:
        raise QueryParameterError(
            "BATCH_SIZE_OUT_OF_RANGE",
            f"batch_size must be between 1 and {MAX_BATCH_SIZE}",
            field="batch_size",
            value=batch_size,
            limit=MAX_BATCH_SIZE,
        )
    return {
        "meters": list(meters),
        "start": doc["start"],
        "end": doc["end"],
        "start_us": start_us,
        "end_us": end_us,
        "select": columns,
        "batch_size": batch_size,
    }


def canonical_query_text(doc: dict) -> str:
    """Stable JSON used as the command, so GetFlightInfo and DoGet agree."""
    return json.dumps(
        {
            "meters": doc["meters"],
            "start": doc["start"],
            "end": doc["end"],
            "select": doc["select"],
            "batch_size": doc["batch_size"],
        },
        separators=(",", ":"),
        sort_keys=True,
    )


TICKET_QUERY_ID = "query_id"


def ticket_with_query_id(text: str, query_id: str) -> str:
    """Ticket body: the canonical query plus the id GetFlightInfo described.

    ``DoGet`` accepts either this form or a bare query document, so a client
    that builds its own ticket still works.
    """
    doc = json.loads(text)
    doc[TICKET_QUERY_ID] = query_id
    return json.dumps(doc, separators=(",", ":"), sort_keys=True)


def parse_ticket(payload: bytes | str) -> tuple[dict, str | None]:
    """Parse a DoGet ticket -> (normalised query, optional query id).

    A ticket produced by ``GetFlightInfo`` carries the described ``query_id``
    alongside the query, so the stream record and the FlightInfo metadata
    share one identity.  A bare query document is also accepted.
    """
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    text = text.strip()
    if len(text) > MAX_QUERY_BYTES:
        raise QueryParameterError(
            "QUERY_TOO_LARGE",
            f"query must be at most {MAX_QUERY_BYTES} bytes",
            limit=MAX_QUERY_BYTES,
        )
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return parse_query(text), None
    query_id = None
    if isinstance(raw, dict) and TICKET_QUERY_ID in raw:
        query_id = raw.pop(TICKET_QUERY_ID)
        if not isinstance(query_id, str) or not query_id:
            raise QueryParameterError(
                "INVALID_QUERY",
                "query_id must be a non-empty string",
                field=TICKET_QUERY_ID,
            )
    return parse_query(raw if isinstance(raw, dict) else text), query_id


def full_schema() -> pa.Schema:
    """Canonical Arrow schema of a fact row (independent of any partition)."""
    return pl.DataFrame(schema=MINUTE_DTYPES).to_arrow().schema


_FULL_SCHEMA = full_schema()


def projected_schema(columns: list[str]) -> pa.Schema:
    return pa.schema([_FULL_SCHEMA.field(c) for c in columns])


# ---------------------------------------------------------------------------
# read hold (shares the existing publish lock)
# ---------------------------------------------------------------------------
def _txn_dirs(root: Path) -> list[Path]:
    base = Path(root) / "state" / "txn"
    if not base.exists():
        return []
    try:
        return [
            d for d in sorted(base.iterdir())
            if d.is_dir() and not d.name.startswith(".")
        ]
    except OSError:
        return []


def _txn_is_live(directory: Path) -> bool:
    """Is a process still holding this transaction's liveness lock?"""
    lock_path = directory / ".lock"
    if not lock_path.exists():
        return False
    try:
        import fcntl
    except ImportError:  # pragma: no cover
        return False
    try:
        fh = open(lock_path, "a+")
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False
    finally:
        fh.close()


def _txn_state(root: Path) -> list[Path]:
    """Transaction directories that still need an admission decision.

    A ``COMMITTED`` directory is finished work whose only remaining step is
    housekeeping; it never changed a file after its marker appeared, so it is
    not part of the admission decision.  Everything else (prepared manifests
    without the marker, and un-prepared staging directories) is returned: the
    caller must establish, under the read hold, whether the owner is alive.
    """
    return [
        d
        for d in _txn_dirs(root)
        if not (d / Transaction.COMMITTED).exists()
    ]


def _blocking_txns(root: Path) -> tuple[list[Path], list[Path]]:
    """Transaction dirs that block admission, split into live vs. dead ones.

    Only a **prepared** directory (manifest present, no ``COMMITTED`` marker)
    can have moved files into the committed tree, so only prepared
    directories can make the tree a two-revision mixture.  Un-prepared
    directories never touched the tree and are irrelevant to the decision.
    """
    live: list[Path] = []
    dead: list[Path] = []
    for d in _txn_state(root):
        if not (d / "manifest.json").exists():
            continue
        (live if _txn_is_live(d) else dead).append(d)
    return live, dead


# Test seam: the concurrency regression injects timing hooks through this
# hook.  Production callers never set it, so the hot path stays branch-free.
admission_hook = None


def _hook(point: str) -> None:
    if admission_hook is not None:
        admission_hook(point)


def settle_store(root: Path, timeout: float = 300.0) -> None:
    """Settle leftovers *before* a read hold exists (first attempt only).

    This is the unlocked pre-pass: on an idle or analysis-only store it costs
    nothing, but a directory that changes between this call and the lock
    acquisition is re-decided by :func:`admit_read` *while the read hold is
    held*.  Never call recovery from here while holding the read lock:
    recovery needs the exclusive side of the same lock file.
    """
    deadline = time.monotonic() + timeout
    while True:
        dirs = _txn_dirs(root)
        if not dirs:
            return
        prepared_live = [
            d for d in dirs
            if (d / "manifest.json").exists()
            and not (d / Transaction.COMMITTED).exists()
            and _txn_is_live(d)
        ]
        if prepared_live:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    "timed out waiting for a live publisher to finish: "
                    + ", ".join(d.name for d in prepared_live)
                )
            time.sleep(0.01)
            continue
        if any((d / "manifest.json").exists() and not (d / Transaction.COMMITTED).exists()
               for d in dirs):
            # A prepared-and-dead job (or a COMMITTED leftover alongside one):
            # let the store's own protocol finish/discard it under its lock.
            recover_pending(root)
            return
        if any(_txn_is_live(d) for d in dirs):
            # Only live analysis jobs: the committed tree is still the last
            # complete round, so this stream may start.  Their eventual commit
            # is blocked by the shared read hold taken right after this.
            return
        # Only dead leftovers remain; discard them with the normal protocol.
        recover_pending(root)
        return


def admit_read(root: Path, timeout: float = 300.0) -> "FlightReadHold":
    """Grant a read permit only once txn state and tree contents agree.

    This is the request-admission boundary.  It returns a
    :class:`FlightReadHold` that must stay open for the whole transfer; the
    meter-existence decision (and every later directory scan) is valid only
    while that hold is held.

    Key invariant, derived from the commit protocol in ``storage.py``:

    * a file leaves ``state/txn/<id>/staged/`` for its committed path only
      inside ``Transaction.commit``/``apply_manifest``, which run under the
      **exclusive** flock on ``state/.txn.lock``;
    * the returned hold takes the **shared** flock on that very file.

    Therefore, while the hold is open the committed tree is frozen: no commit
    can move another fact partition into place.  Admission under the hold has
    exactly two legal outcomes:

    1. no prepared-but-unmarked transaction exists -> the frozen tree is one
       complete committed round, admit immediately.  A live *un-prepared*
       writer is harmless: it has touched only its private staging area, and
       its future commit blocks on this shared hold;
    2. a prepared-but-unmarked transaction exists.  If its owner is alive it is
       inside the publish phase and holds the exclusive lock, so we cannot be
       holding the shared lock concurrently -- release, wait for it to finish,
       then re-lock and re-decide.  If every such owner is dead the publish
       died half-way (the mixed two-revision tree from failure A): release the
       shared lock, let ``recover_pending`` replay the manifest forward under
       the exclusive lock, then re-lock and re-decide.

    The hold is never upgraded (shared -> exclusive): recovery always runs on
    a fresh, lock-free stack frame, so two readers cannot deadlock against
    each other or against a writer.  The loop terminates because every retry
    is preceded either by a commit (a finite job reaching its ``COMMITTED``
    marker) or by a recovery (removing/finishing a dead directory).
    """
    root = Path(root)
    deadline = time.monotonic() + timeout
    # Unlocked pre-pass: handles the common idle-store case without taking
    # any lock, and waits out a publisher that is already committing.
    settle_store(root, timeout=timeout)
    # Test seam for the exact failure-A window: the store has been settled and
    # no read hold exists yet, so another process can still publish here.
    # Production leaves ``admission_hook`` as None and never blocks.
    _hook("after-settle")
    while True:
        _hook("admission-loop-top")
        hold = FlightReadHold(root)
        live, dead = _blocking_txns(root)
        if not live and not dead:
            # The tree observed under the shared hold is one complete round
            # and cannot change until this hold is released.
            _hook("admission-granted")
            return hold
        # Someone is/was publishing.  Recovery needs the exclusive side of
        # this same lock file, so release first -- never upgrade in place.
        hold.release()
        if dead:
            # Prepared-and-dead: finish or discard under the store lock, then
            # re-admit against the now-settled tree (failure B's takeover).
            recover_pending(root)
        else:
            # Prepared-and-live: the publisher is inside its publish phase.
            # Wait for it (bounded), then re-check; if it died in the
            # meantime the next iteration takes the recovery branch.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    "timed out waiting for a live publisher to finish: "
                    + ", ".join(d.name for d in live)
                )
            waited_until = min(deadline, time.monotonic() + min(1.0, remaining))
            while time.monotonic() < waited_until:
                still_live, _ = _blocking_txns(root)
                if not still_live:
                    break
                time.sleep(0.01)
        if time.monotonic() > deadline:
            raise RuntimeError("timed out during read admission")


class FlightReadHold:
    """A shared (reader) hold on the store's commit lock.

    The commit protocol publishes under an exclusive ``flock`` on
    ``state/.txn.lock``.  A stream takes a *shared* ``flock`` on the very same
    file for its whole life, so a publish cannot commit between two batches of
    one stream; if a publisher is already active the query waits for it.  The
    hold is per file descriptor, so it never conflicts with the writer's own
    descriptor in the same process except through the intended exclusion.

    Releasing it (``release()``, called from the stream's ``finally``) wakes
    the waiting publisher, so a cancelled / abandoned stream leaves no read
    protection behind.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self.path = self.root / "state" / ".txn.lock"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+")
        try:
            import fcntl

            fcntl.flock(self._handle, fcntl.LOCK_SH)
        except ImportError:  # pragma: no cover - non-POSIX
            pass
        self.released = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        handle, self._handle = self._handle, None
        try:
            import fcntl

            fcntl.flock(handle, fcntl.LOCK_UN)
        except ImportError:  # pragma: no cover
            pass
        try:
            handle.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# scan planning (partition + row group pruning)
# ---------------------------------------------------------------------------
@dataclass
class FilePlan:
    path: Path
    row_groups: list[int]


@dataclass
class ScanPlan:
    files: list[FilePlan] = field(default_factory=list)
    files_considered: int = 0
    files_opened: int = 0
    files_pruned_by_date: int = 0
    row_groups_total: int = 0
    row_groups_selected: int = 0
    row_groups_pruned: int = 0


def partition_meters(root: Path) -> dict[str, list[Path]]:
    """meter id -> committed fact partition files (from hive directory names)."""
    base = Path(root) / "facts"
    out: dict[str, list[Path]] = {}
    if not base.exists():
        return out
    for meter_dir in sorted(base.glob("meter_id=*")):
        if not meter_dir.is_dir():
            continue
        meter = unquote(meter_dir.name.split("=", 1)[1])
        files = sorted(meter_dir.rglob("data.parquet"))
        if files:
            out[meter] = files
    return out


def _partition_local_date(path: Path) -> date | None:
    """The ``local_date=`` hive component of a fact partition path."""
    for part in reversed(path.parts):
        if part.startswith("local_date="):
            try:
                return date.fromisoformat(part.split("=", 1)[1])
            except ValueError:
                return None
    return None


def _plausible_dates(start_us: int, end_us: int) -> set[date] | None:
    """UTC dates a minute-start in [start,end) can fall on.

    Local dates can lag or lead the UTC date by at most a day (time zones
    span UTC-12..UTC+14), so only ``[utc_start - 1 day, utc_end + 1 day]``
    are possible.  ``None`` means "do not prune by path" (an absurdly large
    range, or one whose endpoints cannot be represented as dates).
    """
    span_us = end_us - start_us
    if span_us > 400 * 86_400 * 1_000_000:  # > ~13 months: stop enumerating
        return None
    try:
        first = datetime.fromtimestamp(start_us / 1e6, tz=timezone.utc).date()
        last = datetime.fromtimestamp((end_us - 1) / 1e6, tz=timezone.utc).date()
    except (OverflowError, OSError, ValueError):
        return None
    out = set()
    day = first - timedelta(days=1)
    stop = last + timedelta(days=1)
    while day <= stop:
        out.add(day)
        day += timedelta(days=1)
    return out


def validate_meters(root: Path, meters: list[str]) -> None:
    """Cheap pre-stream check: every requested meter has a committed partition.

    Done before the response starts so an unknown meter is a parameter error,
    never an empty-looking stream.
    """
    known = sorted(partition_meters(Path(root)))
    unknown = sorted(set(meters) - set(known))
    if unknown:
        raise QueryParameterError(
            "UNKNOWN_METER",
            "no committed fact data for the requested meter(s)",
            field="meters",
            meters=unknown,
            available=known,
        )


def plan_scan(
    root: Path,
    meters: list[str],
    start_us: int,
    end_us: int,
    by_meter: dict[str, list[Path]] | None = None,
) -> ScanPlan:
    """Prune by meter partition, by ``local_date=`` path, then by row-group stats.

    The local-date step never opens a file: it reads the hive directory name,
    which is exact for the UTC span plus the time-zone slack.  Row-group
    statistics then trim the surviving files to the minute range.
    """
    by_meter = partition_meters(root) if by_meter is None else by_meter
    known = sorted(by_meter)
    unknown = sorted(set(meters) - set(by_meter))
    if unknown:
        raise QueryParameterError(
            "UNKNOWN_METER",
            "no committed fact data for the requested meter(s)",
            field="meters",
            meters=unknown,
            available=known,
        )
    possible_dates = _plausible_dates(start_us, end_us)
    plan = ScanPlan()
    for meter in meters:
        for path in by_meter[meter]:
            plan.files_considered += 1
            if possible_dates is not None:
                local_date = _partition_local_date(path)
                if local_date is not None and local_date not in possible_dates:
                    plan.files_pruned_by_date += 1
                    continue
            plan.files_opened += 1
            pf = pq.ParquetFile(path)
            try:
                md = pf.metadata
                plan.row_groups_total += md.num_row_groups
                idx = pf.schema_arrow.get_field_index(FILTER_COLUMN)
                if idx < 0:
                    raise QueryParameterError(
                        "MISSING_FILTER_COLUMN",
                        f"partition {path} has no {FILTER_COLUMN} column",
                    )
                kept: list[int] = []
                for rg in range(md.num_row_groups):
                    stats = md.row_group(rg).column(idx).statistics
                    if stats is None or not stats.has_min_max:
                        kept.append(rg)  # cannot prune without statistics
                        continue
                    lo, hi = int(stats.min), int(stats.max)
                    if hi < start_us or lo >= end_us:
                        plan.row_groups_pruned += 1
                        continue
                    kept.append(rg)
                    plan.row_groups_selected += 1
                if kept:
                    plan.files.append(FilePlan(path=path, row_groups=kept))
            finally:
                pf.close()
    return plan


# ---------------------------------------------------------------------------
# scan request: the streaming generator
# ---------------------------------------------------------------------------
class ScanRequest:
    """One validated query plus the resources it owns while streaming.

    ``stream()`` is a generator.  Its ``finally`` clause closes every parquet
    handle, releases the shared read hold and records the terminal state, so
    client cancel / disconnect / server shutdown free exactly what this
    request created.
    """

    def __init__(self, server, doc: dict, query_id: str | None = None, hold=None):
        self.server = server
        self.root = server.store_root
        self.doc = doc
        self.query_id = query_id or uuid.uuid4().hex
        self.schema = projected_schema(doc["select"])
        self.plan: ScanPlan | None = None
        # A permit granted by ``admit_read`` before the transfer started.  When
        # present it is reused (the admission boundary is crossed exactly once
        # per request); when absent the generator admits itself, which keeps
        # ScanRequest usable as a standalone, in-process scanner.
        self.hold: FlightReadHold | None = hold
        self._handles: list[pq.ParquetFile] = []
        self._started = time.perf_counter()
        self.record = {
            "query_id": self.query_id,
            "meters": doc["meters"],
            "start": doc["start"],
            "end": doc["end"],
            "select": doc["select"],
            "batch_size": doc["batch_size"],
            "started_at": _utc_now(),
            "finished_at": None,
            "elapsed_seconds": None,
            "files_considered": 0,
            "files_opened": 0,
            "files_pruned_by_date": 0,
            "files_scanned": 0,
            "row_groups_total": 0,
            "row_groups_selected": 0,
            "row_groups_pruned": 0,
            "pinned_files": [],
            "batches": 0,
            "rows_returned": 0,
            "bytes_returned": 0,
            "peak_batch_bytes": 0,
            "peak_batch_rows": 0,
            "peak_memory_bytes": 0,
            "canceled": False,
            "error": None,
        }

    # -- lifecycle -------------------------------------------------------
    def stream(self):
        completed = False
        try:
            # 1. Cross the admission boundary as a single decision: settle any
            #    dead transaction (recover under the exclusive lock, never by
            #    upgrading this reader's shared lock), wait out a live
            #    publisher, and only then hold the shared read lock.  The
            #    permit guarantees that the tree seen by the planner -- and by
            #    every batch emitted below -- is one complete committed round
            #    and stays frozen until this generator finishes.  When the RPC
            #    handler already admitted the request, its permit is reused.
            if self.hold is None:
                self.hold = admit_read(self.root)
            # 2. Plan against the settled, frozen tree.  Meter existence is
            #    therefore decided from the same revision the stream reads.
            plan = plan_scan(
                self.root, self.doc["meters"], self.doc["start_us"], self.doc["end_us"]
            )
            self.plan = plan
            self.record.update(
                files_considered=plan.files_considered,
                files_opened=plan.files_opened,
                files_pruned_by_date=plan.files_pruned_by_date,
                row_groups_total=plan.row_groups_total,
                row_groups_selected=plan.row_groups_selected,
                row_groups_pruned=plan.row_groups_pruned,
            )
            self.server.metrics.activate(self.record)
            # 4. Pin the exact files and their identity.  An atomic replace by
            #    a rogue writer (one that ignores the shared lock) is detected
            #    instead of mixing revisions silently.
            pinned = []
            for fp in plan.files:
                st = os.stat(fp.path)
                pinned.append((fp.path, st.st_size, st.st_mtime_ns))
            self.record["pinned_files"] = [
                {"path": str(p.relative_to(self.root)), "size": size}
                for p, size, _ in pinned
            ]
            yield from self._scan(pinned)
            completed = True
        except QueryParameterError:
            self.record["error"] = "QueryParameterError"
            raise
        except GeneratorExit:
            # The generator was closed while it still had pinned files: this
            # is the client's cancel / disconnect path (the transport closes
            # the stream instead of asking for the next batch).  Set the flag
            # here, because closing a generator may not reach the ``finally``
            # block at all.
            if not completed and self.record["error"] is None:
                self.record["canceled"] = True
            raise
        except Exception as exc:
            self.record["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            # A generator closed before its natural end is a cancellation even
            # when the transport never surfaced it through ``is_cancelled``
            # (closing a parked generator raises GeneratorExit, which may not
            # even reach this frame).  An errored or fully drained stream is
            # recorded as such instead.
            if not completed and self.record["error"] is None:
                self.record["canceled"] = True
            self.record["finished_at"] = _utc_now()
            self.record["elapsed_seconds"] = round(time.perf_counter() - self._started, 4)
            self._release()

    def _release(self) -> None:
        if getattr(self, "_released", False):
            return
        self._released = True
        self.server.metrics.deactivate(self.query_id)
        for handle in self._handles:
            try:
                handle.close()
                self.server.metrics.file_opened(-1)
            except Exception:  # pragma: no cover - defensive
                pass
        self._handles = []
        if self.hold is not None:
            self.hold.release()
            self.hold = None
        self.server.record_query(self.record)

    def cancelled(self) -> bool:
        ctx = getattr(self, "context", None)
        try:
            return bool(ctx and ctx.is_cancelled())
        except Exception:  # pragma: no cover - defensive
            return False

    # -- data path -------------------------------------------------------
    def _scan(self, pinned):
        want = self.doc["select"]
        need_key = FILTER_COLUMN not in want
        read_columns = list(want) + ([FILTER_COLUMN] if need_key else [])
        start_us, end_us = self.doc["start_us"], self.doc["end_us"]
        batch_size = self.doc["batch_size"]
        plan_by_path = {fp.path: fp for fp in self.plan.files}
        for path, size, mtime_ns in pinned:
            try:
                st = os.stat(path)
            except FileNotFoundError:
                raise RuntimeError(f"pinned partition vanished during scan: {path}") from None
            if st.st_size != size or st.st_mtime_ns != mtime_ns:
                raise RuntimeError(f"pinned partition changed during scan: {path}")
            fp = plan_by_path[path]
            self.record["files_scanned"] += 1
            parquet_file = pq.ParquetFile(path)
            self._handles.append(parquet_file)
            self.server.metrics.file_opened(1)
            try:
                for batch in parquet_file.iter_batches(
                    batch_size=batch_size,
                    row_groups=list(fp.row_groups),
                    columns=read_columns,
                    use_threads=False,
                ):
                    if self.cancelled():
                        self.record["canceled"] = True
                        return
                    shaped = self._shape(batch, want, start_us, end_us)
                    if shaped.num_rows:
                        yield self._emit(shaped)
                        # gRPC buffers a surprising amount; check cancellation
                        # again *after* the chunk was handed to the transport,
                        # so a client that cancels while a batch is in flight is
                        # noticed on the next iteration without reading further
                        # data.
                        if self.cancelled():
                            self.record["canceled"] = True
                            return
            finally:
                parquet_file.close()
                self._handles.remove(parquet_file)
                self.server.metrics.file_opened(-1)

    def _shape(self, batch, want, start_us, end_us):
        """Filter [start,end) on the raw minute key, then project in order."""
        key_idx = batch.schema.get_field_index(FILTER_COLUMN)
        keys = batch.column(key_idx)
        mask = pc.and_(
            pc.greater_equal(keys, pa.scalar(start_us, pa.int64())),
            pc.less(keys, pa.scalar(end_us, pa.int64())),
        )
        if pc.all(mask).as_py() is False:
            # A row group was selected by statistics, so only its edges fall
            # outside the range; take() keeps the output row order identical
            # to the parquet order.
            batch = batch.take(pc.indices_nonzero(mask))
        if not want:
            return batch
        out = batch.select(want)
        # The declared types match storage exactly: large_* containers are
        # what the fact writer emits, and the schema is derived from the same
        # canonical definition, so a cast is only a guard for old stores.
        return out if out.schema == self.schema else out.cast(self.schema)

    def _emit(self, batch):
        self.record["batches"] += 1
        self.record["rows_returned"] += batch.num_rows
        nbytes = sum(int(col.nbytes) for col in batch.columns)
        self.record["bytes_returned"] += nbytes
        self.record["peak_batch_bytes"] = max(self.record["peak_batch_bytes"], nbytes)
        self.record["peak_batch_rows"] = max(self.record["peak_batch_rows"], batch.num_rows)
        self.record["peak_memory_bytes"] = max(
            self.record["peak_memory_bytes"], FlightMetrics.peak_memory_bytes()
        )
        self.server.metrics.update_active(self.record)
        meta = json.dumps(
            {
                "query_id": self.query_id,
                "batch_index": self.record["batches"],
                "rows": batch.num_rows,
                "columns": [f.name for f in batch.schema],
            },
            separators=(",", ":"),
        ).encode()
        return batch, meta


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# metrics / server state (usable without gRPC)
# ---------------------------------------------------------------------------
class FlightMetrics:
    def __init__(self, store_root: str | Path):
        self.store_root = Path(store_root).resolve()
        self.started_at = _utc_now()
        self._lock = threading.Lock()
        self._records: list[dict] = []
        self._all_records: list[dict] = []
        self._active: dict[str, dict] = {}
        self._open_files = 0
        self._queried = 0
        self.counters = {
            "queries_started": 0,
            "queries_completed": 0,
            "queries_canceled": 0,
            "queries_failed": 0,
            "param_errors": 0,
            "schema_requests": 0,
            "flight_info_requests": 0,
            "do_get_requests": 0,
        }

    # -- live state (a scanner is only "active" while it holds resources) --
    def activate(self, record: dict) -> None:
        with self._lock:
            self._active[record["query_id"]] = dict(record)

    def update_active(self, record: dict) -> None:
        with self._lock:
            if record["query_id"] in self._active:
                self._active[record["query_id"]] = dict(record)

    def deactivate(self, query_id: str) -> None:
        with self._lock:
            self._active.pop(query_id, None)

    def file_opened(self, delta: int = 1) -> None:
        with self._lock:
            self._open_files = max(0, self._open_files + delta)

    def active_queries(self) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._active.values()]

    def note_param_error(self) -> None:
        with self._lock:
            self.counters["param_errors"] += 1

    def note_request(self, kind: str) -> None:
        with self._lock:
            key = f"{kind}_requests"
            if key in self.counters:
                self.counters[key] += 1

    def begin_query(self) -> None:
        with self._lock:
            self.counters["queries_started"] += 1

    def record_query(self, record: dict) -> None:
        with self._lock:
            self._records.append(dict(record))
            self._all_records.append(dict(record))
            self._queried += 1
            if self._queried > 512:
                del self._all_records[:-256]
                self._queried = len(self._all_records)
            if record.get("canceled"):
                self.counters["queries_canceled"] += 1
            elif record.get("error"):
                self.counters["queries_failed"] += 1
            else:
                self.counters["queries_completed"] += 1
            del self._records[:-MAX_RECENT_QUERIES]

    def recent_queries(self, limit: int | None = None) -> list[dict]:
        with self._lock:
            records = [dict(r) for r in self._records]
        return records[-limit:] if limit is not None else records

    def all_queries(self) -> list[dict]:
        """Every completed query of this server process (for acceptance checks)."""
        with self._lock:
            return [dict(r) for r in self._all_records]

    @staticmethod
    def peak_memory_bytes() -> int:
        try:
            with open("/proc/self/status", "r", encoding="ascii") as fh:
                for line in fh:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) * 1024
        except OSError:
            pass
        try:
            import resource

            return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024
        except Exception:  # pragma: no cover
            return 0

    def stats_payload(self, limit: int = MAX_RECENT_QUERIES) -> dict:
        with self._lock:
            active = [dict(r) for r in self._active.values()]
            open_files = self._open_files
        return {
            "started_at": self.started_at,
            "store": str(self.store_root),
            "schema_version": FACT_SCHEMA_VERSION,
            "peak_memory_bytes": self.peak_memory_bytes(),
            "open_parquet_files": open_files,
            "active_queries": active,
            "counters": dict(self.counters),
            "recent_queries": self.recent_queries(limit),
        }

    def health_payload(self) -> dict:
        return {
            "ok": True,
            "store": str(self.store_root),
            "store_exists": self.store_root.exists(),
            "schema_version": FACT_SCHEMA_VERSION,
            "columns": ALL_COLUMNS,
        }


class FlightServiceBase:
    """Shared RPC logic; the gRPC subclass is created on demand.

    Keeping the handlers here means the query/validation/scanning behaviour can
    be exercised without binding a socket; ``make_flight_server`` adds the
    ``pyarrow.flight.FlightServerBase`` shell.
    """

    metrics: FlightMetrics

    def _query_from_command(self, descriptor) -> dict:
        if descriptor is None or not descriptor.command:
            raise QueryParameterError(
                "INVALID_DESCRIPTOR",
                "GetFlightInfo/GetSchema must carry the query as a command descriptor",
            )
        return parse_query(descriptor.command)

    def _admit_and_validate(self, doc: dict) -> FlightReadHold:
        """Cross the admission boundary, then judge meter ids under the hold.

        Admission (settle + shared read hold) must precede the meter check:
        a new meter can be sitting in a dead transaction's staging area, and
        rejecting it from the un-settled directory would both answer from the
        wrong revision (failure B) and leave the takeover undone.  The
        returned permit must be kept alive for the subsequent transfer
        (``DoGet``) or released once the metadata RPC is answered
        (``GetFlightInfo``).
        """
        hold = admit_read(self.metrics.store_root)
        try:
            validate_meters(self.metrics.store_root, doc["meters"])
        except BaseException:
            hold.release()
            raise
        return hold

    def _flight_info(self, descriptor):
        import pyarrow.flight as fl

        self.metrics.note_request("flight_info")
        doc = self._query_from_command(descriptor)
        # Admit first so the UNKNOWN_METER decision uses a settled directory:
        # a prepared-but-dead job adding a meter is taken over (its partition
        # moved into place) before the partition list is read.  The metadata
        # RPC streams no data, so the permit is released once the decision is
        # made; the later DoGet re-admits and gets a permit for the transfer.
        hold = self._admit_and_validate(doc)
        try:
            text = canonical_query_text(doc)
            ticket_text = ticket_with_query_id(text, uuid.uuid4().hex)
            query_id = json.loads(ticket_text)["query_id"]
            info = fl.FlightInfo(
                projected_schema(doc["select"]),
                fl.FlightDescriptor.for_command(text),
                [fl.FlightEndpoint(fl.Ticket(ticket_text), [])],
                app_metadata=json.dumps(
                    {
                        "query_id": query_id,
                        "query": json.loads(text),
                        "schema_version": FACT_SCHEMA_VERSION,
                    },
                    separators=(",", ":"),
                ).encode(),
            )
            return info
        finally:
            hold.release()

    def _descriptor_schema(self, descriptor):
        import pyarrow.flight as fl

        self.metrics.note_request("schema")
        if descriptor is None:
            raise QueryParameterError("INVALID_DESCRIPTOR", "descriptor is required")
        path = [p.decode() if isinstance(p, (bytes, bytearray)) else str(p)
                for p in (descriptor.path or [])]
        if path:
            if path == ["facts"]:
                return fl.SchemaResult(full_schema())
            if path == ["schema"]:
                return fl.SchemaResult(full_schema())
            raise QueryParameterError(
                "UNKNOWN_FLIGHT",
                "unknown descriptor path",
                path=path,
                available=[["facts"]],
            )
        if not descriptor.command:
            return fl.SchemaResult(full_schema())
        doc = parse_query(descriptor.command)
        return fl.SchemaResult(projected_schema(doc["select"]))

    def _do_get_stream(self, context, ticket):
        import pyarrow.flight as fl

        self.metrics.note_request("do_get")
        try:
            doc, query_id = parse_ticket(ticket.ticket)
        except QueryParameterError:
            self.metrics.note_param_error()
            raise
        # Admission happens *before* the GeneratorStream exists: parameter
        # failures (including UNKNOWN_METER judged from the settled tree) are
        # therefore raised before a single batch could be transferred, and the
        # granted permit outlives this call -- ScanRequest releases it in its
        # own ``finally`` when the transfer ends / is cancelled / errors out.
        try:
            hold = self._admit_and_validate(doc)
        except QueryParameterError:
            self.metrics.note_param_error()
            raise
        self.metrics.begin_query()
        request = ScanRequest(self, doc, query_id=query_id, hold=hold)
        request.context = context
        gen = request.stream()

        def guarded():
            # Metadata-only move: the client sees the schema even for an empty
            # result, because GeneratorStream declares it up front.
            yield from gen

        return fl.GeneratorStream(request.schema, guarded())

    def _do_action(self, context, action):
        kind = action.type
        if kind == ACTION_STATS:
            limit = MAX_RECENT_QUERIES
            if action.body:
                try:
                    limit = int(json.loads(action.body.decode())["limit"])
                except Exception:
                    limit = MAX_RECENT_QUERIES
            payload = self.metrics.stats_payload(limit)
        elif kind == ACTION_HEALTH:
            payload = self.metrics.health_payload()
        else:
            import pyarrow.flight as fl

            raise fl.FlightServerError(
                f"{PARAM_ERROR_PREFIX} "
                + json.dumps({"code": "UNKNOWN_ACTION", "message": kind})
            )
        body = json.dumps(payload, separators=(",", ":"), default=str).encode()

        def one():
            yield body

        return one()

    def _list_actions(self):
        import pyarrow.flight as fl

        return [
            fl.ActionType(ACTION_STATS, "JSON snapshot of counters and recent queries"),
            fl.ActionType(ACTION_HEALTH, "JSON liveness document"),
        ]

    def _list_flights(self, criteria: bytes | None = None):
        """Describe the fact table as a single Flight (for discoverability)."""
        import pyarrow.flight as fl

        descriptor = fl.FlightDescriptor.for_path("facts")
        return [
            fl.FlightInfo(
                full_schema(),
                descriptor,
                [fl.FlightEndpoint(fl.Ticket(b"facts"), [])],
                total_records=None,
                app_metadata=json.dumps(
                    {"schema_version": FACT_SCHEMA_VERSION, "format": "parquet-minutes"},
                    separators=(",", ":"),
                ).encode(),
            )
        ]

    def record_query(self, record: dict) -> None:
        self.metrics.record_query(record)


# ---------------------------------------------------------------------------
# gRPC server shell
# ---------------------------------------------------------------------------
def make_flight_server(store_root: str | Path, location=None):
    import pyarrow.flight as fl

    class FlightService(FlightServiceBase, fl.FlightServerBase):
        def __init__(self, root, location=None):
            self.metrics = FlightMetrics(root)
            self.store_root = Path(root).resolve()
            fl.FlightServerBase.__init__(self, location)

        def get_schema(self, context, descriptor):
            try:
                return self._descriptor_schema(descriptor)
            except QueryParameterError:
                self.metrics.note_param_error()
                raise

        def get_flight_info(self, context, descriptor):
            try:
                return self._flight_info(descriptor)
            except QueryParameterError:
                self.metrics.note_param_error()
                raise

        def do_get(self, context, ticket):
            try:
                return self._do_get_stream(context, ticket)
            except QueryParameterError:
                self.metrics.note_param_error()
                raise

        def do_action(self, context, action):
            return self._do_action(context, action)

        def list_actions(self, context):
            return self._list_actions()

        def list_flights(self, context, criteria):
            return self._list_flights(criteria)

    return FlightService(store_root, location)


def serve_forever(store_root: str | Path, host: str = "127.0.0.1", port: int = 0):
    """Create and bind a Flight server; the caller decides how to run it."""
    return make_flight_server(store_root, (host, port))


# ---------------------------------------------------------------------------
# CLI helper shared with the demo client
# ---------------------------------------------------------------------------
def store_root_or_die(path: str) -> Path:
    root = Path(path).resolve()
    if not (root / "facts").exists():
        raise SystemExit(f"{root} does not look like a minenergy store (no facts/ tree)")
    return root


__all__ = [
    "ACTION_HEALTH",
    "ACTION_STATS",
    "ALL_COLUMNS",
    "DEFAULT_BATCH_SIZE",
    "FILTER_COLUMN",
    "MAX_BATCH_SIZE",
    "MAX_METERS",
    "MAX_QUERY_BYTES",
    "MAX_SELECT_COLUMNS",
    "PARAM_ERROR_PREFIX",
    "FlightMetrics",
    "FlightReadHold",
    "QueryParameterError",
    "ScanRequest",
    "admission_hook",
    "admit_read",
    "canonical_query_text",
    "full_schema",
    "make_flight_server",
    "parse_query",
    "parse_ticket",
    "plan_scan",
    "projected_schema",
    "serve_forever",
    "settle_store",
    "store_root_or_die",
    "validate_meters",
]
