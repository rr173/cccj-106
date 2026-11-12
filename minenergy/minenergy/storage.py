"""Parquet layout for raw inputs, canonical revisions and minute facts.

Layout under the store root::

    raw/samples.parquet
    raw/configs.parquet
    canonical/samples.parquet
    canonical/configs.parquet
    canonical/sample_conflicts.parquet
    canonical/config_conflicts.parquet
    intervals/meter_id=<id>/data.parquet
    facts/meter_id=<id>/timezone=<tz>/local_date=<date>/data.parquet
    affected/<run_id>/intervals.parquet
    affected/<run_id>/minutes.parquet
    affected/<run_id>/run.json
    state/settings.json
    state/txn/<txid>/...                  # only while a job is in flight

Every individual write is atomic (temporary file + ``os.replace``) so an
interrupted run never leaves a half-written parquet file.  A *job*, however,
touches many files and those individual writes are not enough: a process that
dies between two of them leaves a mixture of old and new files.  Jobs therefore
run inside a transaction:

1. every write and delete is diverted to ``state/txn/<txid>/staged/`` and
   recorded in the transaction's in-memory operation set.  Reads of the raw and
   canonical tables see the job's own staged writes; reads of the committed
   partition trees (intervals/facts) see the committed baseline;
2. when the job has produced *all* of its outputs, ``Transaction.prepare()``
   atomically writes ``manifest.json`` listing every target and deletion.  The
   manifest is the "all work is ready" boundary and its *existence* is the
   recovery criterion;
3. ``Transaction.commit()`` holds the store lock, moves staged files to their
   final paths, applies deletions, and then atomically writes the ``COMMITTED``
   marker -- the single commit point of the whole job -- and drops the directory;
4. ``recover_pending()`` (called by every entry point before it reads or
   writes) takes the same store lock and repairs what is left:
   * ``COMMITTED`` leftovers are deleted (never replayed: a later job may have
     replaced the same files);
   * a manifest without a marker is finished by replaying it forward (moves of
     staged files that are gone are skipped, deletions are idempotent);
   * a directory without a manifest never touched anything outside ``staged/``
     and is discarded.

A crash during analysis leaves no manifest, so nothing outside the staging
area was changed; a crash during publish leaves a complete manifest, so the
job is completed, never half-applied.  The next start always begins from the
last committed baseline, which is why a crash after the canonical write can no
longer masquerade as a no-op.

Directories written before this protocol (no ``state/txn``) need no
conversion: their current files are simply the committed baseline of the first
transaction.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone as dt_timezone
from pathlib import Path
from urllib.parse import quote, unquote

import polars as pl

try:  # POSIX only; the fallback keeps the library importable elsewhere.
    import fcntl
except ImportError:  # pragma: no cover - Windows without fcntl
    fcntl = None

from .schema import (
    AFFECTED_INTERVAL_DTYPES,
    AFFECTED_MINUTE_DTYPES,
    CANONICAL_CONFIG_DTYPES,
    CANONICAL_SAMPLE_DTYPES,
    CONFLICT_CONFIG_DTYPES,
    CONFLICT_SAMPLE_DTYPES,
    INTERVAL_DTYPES,
    MINUTE_DTYPES,
    RAW_CONFIG_COLUMNS,
    RAW_CONFIG_DTYPES,
    RAW_SAMPLE_COLUMNS,
    RAW_SAMPLE_DTYPES,
)


def _write_atomic(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".parquet", dir=path.parent)
    os.close(fd)
    try:
        df.write_parquet(tmp, compression="zstd", statistics=True)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _write_atomic_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        Path(tmp).write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _now_iso() -> str:
    return datetime.now(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Transaction:
    """A single job's staged writes; see the module docstring for the protocol.

    While a transaction is open every ``Store`` mutation is diverted here:
    parquet payloads are written to ``<txn>/staged/`` and target paths and
    deletions are recorded in ``ops.json``.  ``Store`` reads of *raw and
    canonical* tables see this transaction's own staged writes (so a job's
    append/revision is visible to its own comparison), while trees of committed
    partitions (intervals/facts) are read from their committed baseline.
    ``commit``/``abort`` close the transaction; dropping it without either
    leaves a directory that only reaches the discoverable state after
    ``prepare``.
    """

    PREPARE = "PREPARE"
    COMMITTED = "COMMITTED"

    def __init__(self, root: Path, txid: str | None = None):
        # An absolute root keeps relative targets stable even if the process
        # changes working directory between staging and publishing.
        self.root = Path(root).resolve()
        self.txid = txid or f"{datetime.now(dt_timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
        self.dir = self.root / "state" / "txn" / self.txid
        self.staged_dir = self.dir / "staged"
        self._owner: Store | None = None
        self.dir.mkdir(parents=True, exist_ok=True)
        self.ops: dict[str, dict] = {}
        self._closed = False
        self._prepared = False
        # Liveness lock: held while this process owns the transaction.  It is
        # created *before* the staging area so recovery never sees a staging
        # directory whose owner is not yet identifiable.  Recovery only discards
        # an un-prepared directory whose lock it can take; a live job's staging
        # area is never touched.  A transaction opened to replay an existing
        # manifest does not need it.
        self._lock_fh = None
        if fcntl is not None and not self.manifest_path.exists():
            self._lock_fh = open(self.dir / ".lock", "a+")
            fcntl.flock(self._lock_fh, fcntl.LOCK_EX)
        self.staged_dir.mkdir(parents=True, exist_ok=True)

    # ---- paths ----------------------------------------------------------
    def _rel(self, target: Path) -> str:
        return Path(target).resolve().relative_to(self.root.resolve()).as_posix()

    def _abs(self, rel: str) -> Path:
        return self.root / rel

    def _staged_path(self, rel: str) -> Path:
        # Rel paths come from _rel(target) under the store root and never contain
        # "..", so writing them under staged/ cannot escape the staging area.
        return self.staged_dir / rel

    @property
    def manifest_path(self) -> Path:
        return self.dir / "manifest.json"

    @property
    def committed_marker(self) -> Path:
        return self.dir / self.COMMITTED

    # ---- recording ------------------------------------------------------
    def put_parquet(self, df: pl.DataFrame, target: Path) -> None:
        rel = self._rel(target)
        staged = self._staged_path(rel)
        _write_atomic(df, staged)
        self.ops[rel] = {"action": "write", "staged": "staged/" + rel}

    def staged_for(self, target: Path) -> Path | None:
        """The staged replacement for ``target`` in this transaction, if any."""
        op = self.ops.get(self._rel(target))
        if op is None or op["action"] != "write":
            return None
        return self.dir / op["staged"]

    def put_text(self, text: str, target: Path) -> None:
        rel = self._rel(target)
        staged = self._staged_path(rel)
        _write_atomic_text(text, staged)
        self.ops[rel] = {"action": "write", "staged": "staged/" + rel}

    def delete(self, target: Path) -> None:
        rel = self._rel(target)
        self.ops[rel] = {"action": "delete", "staged": None}

    # ---- phases ---------------------------------------------------------
    def prepare(self, meta: dict | None = None) -> None:
        """Freeze the operation set; nothing new may be staged after this."""
        payload = {
            "txid": self.txid,
            "status": self.PREPARE,
            "created_at": _now_iso(),
            # Sorted so a replayed commit moves files in a deterministic order.
            "targets": [{"rel": rel, **op} for rel, op in sorted(self.ops.items())],
            "meta": meta or {},
        }
        _write_atomic_text(json.dumps(payload, indent=2, default=str) + "\n", self.manifest_path)
        self._prepared = True

    def commit(self) -> None:
        """Publish every staged file; the marker is the job's single commit point."""
        if not self._prepared:
            raise RuntimeError("commit without prepare")
        with _store_lock(self.root):
            self.apply_manifest()
            _write_atomic_text("ok\n", self.committed_marker)
        self._closed = True
        self.cleanup()

    def apply_manifest(self) -> None:
        payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        # Writes first, deletions only after every new file is in place: an
        # interrupted publish then has at least the intended new files in place,
        # never a deletion that precedes its replacement.  Replaying a partially
        # applied manifest is idempotent: a staged file that is gone was already
        # moved, and a repeated deletion is a no-op.
        for op in payload["targets"]:
            if op["action"] != "write":
                continue
            staged = self.dir / op["staged"]
            target = self._abs(op["rel"])
            if not staged.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
        for op in payload["targets"]:
            if op["action"] != "delete":
                continue
            target = self._abs(op["rel"])
            target.unlink(missing_ok=True)
            self._prune_empty_parents(target)

    def _prune_empty_parents(self, path: Path) -> None:
        # Remove now-empty hive directories left behind by a deletion.  Only
        # directories strictly below the store root are candidates; the store
        # root itself and anything above it is never removed.
        root = self.root.resolve()
        parent = path.parent
        while parent.resolve() != root and root in parent.resolve().parents:
            if parent.exists() and parent.is_dir() and not any(parent.iterdir()):
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
            else:
                break

    def cleanup(self) -> None:
        if fcntl is not None and self._lock_fh is not None:
            try:
                fcntl.flock(self._lock_fh, fcntl.LOCK_UN)
            finally:
                self._lock_fh.close()
                self._lock_fh = None
        shutil.rmtree(self.dir, ignore_errors=True)
        if self._owner is not None and self._owner._txn is self:
            self._owner._txn = None

    def abort(self) -> None:
        """Discard an attempt that never reached the publish phase.

        Once a manifest exists the job is recoverable; deleting it would hide a
        half-applied publish, so the directory is left for ``recover_pending``.
        """
        self._closed = True
        if not self._prepared:
            self.cleanup()


def recover_pending(root: Path) -> list[str]:
    """Repair or discard every transaction directory left by an earlier process.

    Must be called before reading or writing the store (the engine calls it at
    the start of every run).

    * ``COMMITTED`` marker present -> the job finished; only leftovers to remove.
    * ``PREPARE`` manifest -> the job died inside the publish phase; finish it
      (idempotent forward replay) and mark it committed.
    * no manifest -> the job died while staging and no target outside
      ``staged/`` was touched; discard the directory.  No manifest can be
      partial: it is written with an atomic replace.

    A live process is never disturbed: each transaction holds a per-directory
    liveness lock while it stages, and a prepared job is replayed under the
    store lock.  Recovery therefore never discards a staging area that is still
    owned by a live process.

    Returns the txids that were handled, newest first, for reporting.
    """
    base = Path(root) / "state" / "txn"
    if not base.exists():
        return []
    with _store_lock(root):
        return _recover_locked(base)

def _recover_locked(base: Path) -> list[str]:
    handled: list[str] = []
    # Newest first: a later job's committed files must not be reverted by an
    # older job's still-pending manifest.
    for d in sorted(base.iterdir(), reverse=True):
        if not d.is_dir() or d.name.startswith("."):
            continue
        txid = d.name
        if (d / Transaction.COMMITTED).exists():
            # The job finished; replaying its manifest would undo a later job's
            # writes, so only the leftover directory is removed.
            _remove_if_unlocked(d)
            handled.append(txid)
            continue
        if not (d / "manifest.json").exists():
            # Never prepared: the job (if it is still alive) holds the liveness
            # lock, so this directory is either dead or in use.  Discard only
            # when the lock is free.
            if _remove_if_unlocked(d):
                handled.append(txid)
            continue
        # Prepared but not marked: the job died inside the publish phase.  The
        # manifest is complete (atomic write), so finish it forward.  A process
        # that dies here again leaves the manifest for the next recovery.
        txn = Transaction(base.parent.parent, txid)
        txn._prepared = True
        txn.apply_manifest()
        _write_atomic_text("ok\n", txn.committed_marker)
        txn.cleanup()
        handled.append(txid)
    return handled


def _remove_if_unlocked(directory: Path) -> bool:
    """Remove a transaction directory unless a live process holds its lock."""
    lock = directory / ".lock"
    if fcntl is None or not lock.exists():
        shutil.rmtree(directory, ignore_errors=True)
        return True
    with open(lock, "a+") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        try:
            # Re-check under the lock: the owner may have published/cleaned up
            # between our first look and the lock acquisition.
            if not directory.exists():
                return False
            shutil.rmtree(directory, ignore_errors=True)
            return True
        finally:
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
            except OSError:
                pass


@contextmanager
def _store_lock(root: Path):
    """Serialise recovery and publishing within one process.

    Unlike the per-transaction liveness lock, this one is global to the store:
    ``recover_pending`` holds it so a manifest replay cannot interleave with a
    commit in another thread, and ``Transaction.commit`` holds it while it
    publishes and removes its own directory.
    """
    if fcntl is None:  # pragma: no cover
        yield
        return
    lock_path = Path(root) / "state" / ".txn.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _merge_raw(
    existing: pl.DataFrame, incoming: pl.DataFrame, dtypes: dict, columns: list[str]
) -> pl.DataFrame:
    """Append a batch to the raw log exactly once.

    Retransmitting the same batch is a normal retry after a crash, so identical
    rows are not appended a second time.  The whole-row JSON encoding is the
    comparison key, so duplicate rows *within* a batch collapse as well; rows
    that are already in the log are dropped instead of being appended again.
    """
    incoming = incoming.select(columns).cast(dtypes)
    if existing.height == 0:
        return incoming
    key = pl.struct([pl.col(c) for c in columns]).struct.json_encode()
    novel = (
        incoming.with_columns(key.alias("_row"))
        .unique(subset=["_row"], keep="first", maintain_order=True)
        .join(
            existing.with_columns(key.alias("_row")).select("_row").unique(),
            on="_row",
            how="anti",
        )
        .drop("_row")
    )
    if novel.height == 0:
        return existing
    return pl.concat([existing, novel], how="vertical_relaxed").cast(dtypes)


def read_parquet_tree(root: Path, dtypes: dict[str, pl.DataType]) -> pl.DataFrame:
    """Concatenate every parquet under ``root``; empty/missing tree is empty."""
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return pl.DataFrame(schema=dtypes)
    parts = [pl.read_parquet(f) for f in files]
    merged = pl.concat(parts, how="vertical_relaxed")
    present = [c for c in dtypes if c in merged.columns]
    return merged.select(present).cast({k: dtypes[k] for k in present})


class Store:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._txn: Transaction | None = None

    # ---- transactions ----------------------------------------------------
    @property
    def transaction(self) -> Transaction | None:
        return self._txn

    def begin(self) -> Transaction:
        self.recover()
        self._txn = Transaction(self.root)
        self._txn._owner = self
        return self._txn

    def recover(self) -> list[str]:
        """Finish or discard work left by an earlier process (see module docs)."""
        return recover_pending(self.root)

    def _put(self, df: pl.DataFrame, path: Path) -> None:
        if self._txn is not None:
            self._txn.put_parquet(df, path)
        else:
            _write_atomic(df, path)

    def _put_text(self, text: str, path: Path) -> None:
        if self._txn is not None:
            self._txn.put_text(text, path)
        else:
            _write_atomic_text(text, path)

    def _delete(self, path: Path) -> None:
        if self._txn is not None:
            self._txn.delete(path)
        else:
            path.unlink(missing_ok=True)

    # ---- raw -------------------------------------------------------------
    @property
    def raw_samples_path(self) -> Path:
        return self.root / "raw" / "samples.parquet"

    @property
    def raw_configs_path(self) -> Path:
        return self.root / "raw" / "configs.parquet"

    def append_raw(
        self, samples: pl.DataFrame | None = None, configs: pl.DataFrame | None = None
    ) -> None:
        if samples is not None and samples.height:
            existing = self.read_raw_samples()
            merged = _merge_raw(existing, samples, RAW_SAMPLE_DTYPES, RAW_SAMPLE_COLUMNS)
            self._put(merged, self.raw_samples_path)
        if configs is not None and configs.height:
            existing = self.read_raw_configs()
            merged = _merge_raw(existing, configs, RAW_CONFIG_DTYPES, RAW_CONFIG_COLUMNS)
            self._put(merged, self.raw_configs_path)

    def read_raw_samples(self) -> pl.DataFrame:
        return self._read_table(self.raw_samples_path, RAW_SAMPLE_DTYPES)

    def read_raw_configs(self) -> pl.DataFrame:
        return self._read_table(self.raw_configs_path, RAW_CONFIG_DTYPES)

    def _read_table(self, path: Path, dtypes: dict[str, pl.DataType]) -> pl.DataFrame:
        """Read ``path`` through the open transaction (own writes are visible)."""
        if self._txn is not None:
            staged = self._txn.staged_for(path)
            if staged is not None:
                return pl.read_parquet(staged).cast(dtypes)
        if not path.exists():
            return pl.DataFrame(schema=dtypes)
        return pl.read_parquet(path).cast(dtypes)

    def clear_raw(self) -> None:
        for p in (self.raw_samples_path, self.raw_configs_path):
            self._delete(p)

    # ---- canonical -------------------------------------------------------
    def write_canonical(
        self,
        samples: pl.DataFrame,
        configs: pl.DataFrame,
        sample_conflicts: pl.DataFrame,
        config_conflicts: pl.DataFrame,
    ) -> None:
        self._put(samples.cast(CANONICAL_SAMPLE_DTYPES), self.root / "canonical" / "samples.parquet")
        self._put(configs.cast(CANONICAL_CONFIG_DTYPES), self.root / "canonical" / "configs.parquet")
        self._put(
            sample_conflicts.cast(CONFLICT_SAMPLE_DTYPES),
            self.root / "canonical" / "sample_conflicts.parquet",
        )
        self._put(
            config_conflicts.cast(CONFLICT_CONFIG_DTYPES),
            self.root / "canonical" / "config_conflicts.parquet",
        )

    def read_canonical_samples(self) -> pl.DataFrame:
        return self._read_table(
            self.root / "canonical" / "samples.parquet", CANONICAL_SAMPLE_DTYPES
        )

    def read_canonical_configs(self) -> pl.DataFrame:
        return self._read_table(
            self.root / "canonical" / "configs.parquet", CANONICAL_CONFIG_DTYPES
        )

    def read_sample_conflicts(self) -> pl.DataFrame:
        p = self.root / "canonical" / "sample_conflicts.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema=CONFLICT_SAMPLE_DTYPES)

    def read_config_conflicts(self) -> pl.DataFrame:
        p = self.root / "canonical" / "config_conflicts.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema=CONFLICT_CONFIG_DTYPES)

    # ---- intervals -------------------------------------------------------
    def read_intervals(self) -> pl.DataFrame:
        return read_parquet_tree(self.root / "intervals", INTERVAL_DTYPES)

    def write_intervals_for_meter(self, meter_id: str, intervals: pl.DataFrame) -> Path:
        path = self.root / "intervals" / f"meter_id={meter_id}" / "data.parquet"
        self._put(intervals.cast(INTERVAL_DTYPES), path)
        return path

    def replace_all_intervals(self, intervals: pl.DataFrame) -> None:
        base = self.root / "intervals"
        if base.exists():
            for f in base.rglob("*.parquet"):
                self._delete(f)
        for meter_id in sorted(set(intervals["meter_id"].to_list())) if intervals.height else []:
            self.write_intervals_for_meter(
                meter_id, intervals.filter(pl.col("meter_id") == meter_id)
            )

    # ---- minute facts ----------------------------------------------------
    @staticmethod
    def _partition_path(root: Path, meter_id: str, timezone: str, local_date: date) -> Path:
        return (
            root
            / f"meter_id={meter_id}"
            / f"timezone={quote(timezone, safe='')}"
            / f"local_date={local_date.isoformat()}"
            / "data.parquet"
        )

    def existing_fact_partitions(self) -> dict[tuple[str, str], Path]:
        base = self.root / "facts"
        out: dict[tuple[str, str], Path] = {}
        if not base.exists():
            return out
        for path in base.rglob("data.parquet"):
            parts = dict(p.split("=", 1) for p in path.parent.parts[-3:])
            out[(unquote(parts["meter_id"]), parts["local_date"])] = path
        return out

    def read_facts(self) -> pl.DataFrame:
        return read_parquet_tree(self.root / "facts", MINUTE_DTYPES)

    def write_fact_partitions(self, facts: pl.DataFrame) -> list[Path]:
        written = []
        if facts.height == 0:
            return written
        facts = facts.cast(MINUTE_DTYPES)
        for keys, part in facts.partition_by(
            ["meter_id", "timezone", "local_date"],
            as_dict=True,
            maintain_order=True,
        ).items():
            meter_id, timezone, local_date = keys
            path = self._partition_path(self.root / "facts", meter_id, timezone, local_date)
            self._put(part, path)
            written.append(path)
        return written

    def replace_all_fact_partitions(self, facts: pl.DataFrame) -> list[Path]:
        base = self.root / "facts"
        if base.exists():
            for f in base.rglob("*.parquet"):
                self._delete(f)
        return self.write_fact_partitions(facts)

    def delete_fact_partition(self, meter_id: str, local_date_iso: str) -> str | None:
        partitions = self.existing_fact_partitions()
        path = partitions.get((meter_id, local_date_iso))
        if path is None:
            return None
        if self._txn is not None:
            # Empty parent directories are pruned by the publish phase.
            self._txn.delete(path)
            return str(path)
        path.unlink(missing_ok=True)
        for parent in (path.parent, path.parent.parent, path.parent.parent.parent):
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        return str(path)

    # ---- reports and state ----------------------------------------------
    def write_affected(self, run_id: str, intervals: pl.DataFrame, minutes: pl.DataFrame) -> None:
        base = self.root / "affected" / run_id
        self._put(intervals.cast(AFFECTED_INTERVAL_DTYPES), base / "intervals.parquet")
        self._put(minutes.cast(AFFECTED_MINUTE_DTYPES), base / "minutes.parquet")

    def write_run_meta(self, run_id: str, meta: dict) -> None:
        base = self.root / "affected" / run_id
        self._put_text(json.dumps(meta, indent=2, default=str) + "\n", base / "run.json")

    def read_run_meta(self, run_id: str) -> dict:
        p = self.root / "affected" / run_id / "run.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    def list_runs(self) -> list[str]:
        base = self.root / "affected"
        if not base.exists():
            return []
        return sorted(p.name for p in base.iterdir() if p.is_dir())

    def _state_path(self) -> Path:
        return self.root / "state" / "settings.json"

    def write_settings_fingerprint(self, fingerprints: dict[str, tuple[str, int]]) -> None:
        p = self._state_path()
        payload = {m: {"timezone": tz, "gap_threshold_seconds": g} for m, (tz, g) in fingerprints.items()}
        self._put_text(json.dumps(payload, indent=2) + "\n", p)

    def read_settings_fingerprint(self) -> dict[str, dict]:
        p = self._state_path()
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
