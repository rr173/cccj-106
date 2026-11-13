"""High-level recording facade: physical scaling, windows, Arrow batches.

A :class:`Recording` pairs a parsed CFG with the caller-supplied IANA zone and
turns streamed DAT frames into Arrow ``RecordBatch`` objects without ever
holding the whole recording in memory.

Physical values
---------------
Raw int16 readings are first restored with ``y = a*x + b`` into engineering
units on the side declared by the CFG P/S marker (the conventional COMTRADE
meaning of the coefficients).  The other side follows that marker, so the
conversion direction always matches how the recorder labelled the channel:

* ``P`` channels: ``y`` is already the primary value and
  ``secondary = primary * secondary_ratio / primary_ratio``;
* ``S`` channels: ``y`` is already the secondary value and
  ``primary = secondary * primary_ratio / secondary_ratio``;
* non-finite or non-positive ratios were rejected at parse time, and every
  result is checked to be finite, so a bad ratio can never surface as an
  infinity.

Each channel's own skew (microseconds) is added to *its* sampling instant; a
single shared timestamp is never applied across channels.  Digital bits are
unpacked in CFG channel order (least significant bit of a group word maps to
the group's first channel) and both the actual 0/1 and the deviation from the
CFG normal state are exposed.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow as pa

from .clock import (
    ResolvedClock,
    build_clock,
    interpret_timestamp,
)
from .config import AnalogChannel, ComtradeConfig, DigitalChannel, parse_cfg
from .errors import ChannelError, FrameSequenceError, TimeError
from .frames import FrameStream, RawFrame

MICRO = 1_000_000

ANALOG_SCHEMA = pa.schema([
    pa.field("sample_number", pa.int64()),
    pa.field("frame_timestamp_us", pa.int64()),
    pa.field("time_us", pa.int64()),  # frame time + this channel's skew
    pa.field("time_utc", pa.timestamp("us", tz="UTC")),
    pa.field("relative_trigger_us", pa.int64()),
    pa.field("channel_index", pa.int32()),
    pa.field("channel_name", pa.string()),
    pa.field("value", pa.float64()),
    pa.field("raw_value", pa.int32()),
    pa.field("unit", pa.string()),
    pa.field("phase", pa.string()),
    pa.field("ps_marker", pa.string()),
    pa.field("value_side", pa.string()),  # "primary" | "secondary"
    pa.field("skew_us", pa.float64()),
])

DIGITAL_SCHEMA = pa.schema([
    pa.field("sample_number", pa.int64()),
    pa.field("time_us", pa.int64()),  # base frame time; digital channels carry no skew
    pa.field("time_utc", pa.timestamp("us", tz="UTC")),
    pa.field("relative_trigger_us", pa.int64()),
    pa.field("channel_index", pa.int32()),
    pa.field("channel_name", pa.string()),
    pa.field("state", pa.int8()),  # actual 0/1 on the wire
    pa.field("normal_state", pa.int8()),
    pa.field("deviates_from_normal", pa.bool_()),
    pa.field("phase", pa.string()),
])


@dataclass(frozen=True)
class ChannelRef:
    kind: str       # "analog" | "digital"
    index: int      # 1-based An/Dn number
    name: str


@dataclass(slots=True)
class _ScanState:
    mode: str = "unknown"          # "dat_timestamp" | "rebuilt_from_rates"
    expected_sample: int = 1
    last_base_us: int | None = None
    frames_seen: int = 0
    any_nonzero: bool = False


def restore_marked_side(channel: AnalogChannel, raw: int) -> float:
    """Apply ``a*x + b`` to obtain engineering units on the CFG-marked side.

    A ``P`` marker means the restored value is primary; an ``S`` marker means
    it is secondary.  The opposite side is derived in :func:`convert_side`.
    """
    value = channel.a * raw + channel.b
    if not math.isfinite(value):
        raise ChannelError(
            f"a*x+b produced a non-finite value ({value}) for channel "
            f"A{channel.index} {channel.name!r} with raw {raw}",
        )
    return value


# Backwards-compatible alias: the math is the same, the side is named by the
# channel's P/S marker rather than assumed to be secondary.
restore_secondary = restore_marked_side


def convert_side(channel: AnalogChannel, marked_value: float, side: str) -> float:
    """Convert the restored marked-side value to the requested output side."""
    ratio = channel.primary_ratio / channel.secondary_ratio
    if not math.isfinite(ratio) or ratio <= 0:
        raise ChannelError(
            f"primary/secondary ratio is not a positive finite number for "
            f"A{channel.index}: {ratio}",
        )
    if channel.ps_marker == "P":
        primary = marked_value
        result = primary if side == "primary" else primary / ratio
    else:  # "S": restored value is already the secondary-side reading
        secondary = marked_value
        result = secondary * ratio if side == "primary" else secondary
    if not math.isfinite(result):
        raise ChannelError(
            f"conversion to {side} value produced a non-finite result for "
            f"A{channel.index} (raw channel, marker {channel.ps_marker})",
        )
    return result


def unpack_digital_words(words: tuple[int, ...],
                         digitals: tuple[DigitalChannel, ...]
                         ) -> list[int]:
    """Unpack packed uint16 words in CFG channel order.

    The least significant bit of word 0 is digital channel D1; bit 15 is D16,
    then word 1 carries D17..D32 and so on.
    """
    states: list[int] = []
    for d in digitals:
        group = (d.index - 1) // 16
        bit = (d.index - 1) % 16
        states.append((words[group] >> bit) & 1)
    return states


class Recording:
    """A validated CFG/DAT pair bound to an IANA timezone."""

    def __init__(self, config: ComtradeConfig, timezone: str):
        self.config = config
        self.timezone = timezone
        self.clock: ResolvedClock = build_clock(config, timezone)
        self._mode: str | None = None

    # ------------------------------------------------------------------ load
    @classmethod
    def open(cls, cfg_path: str | Path, timezone: str,
             dat_path: str | Path | None = None) -> "Recording":
        return cls(parse_cfg(cfg_path, dat_path=dat_path), timezone)

    @property
    def timestamp_mode(self) -> str | None:
        """``dat_timestamp`` / ``rebuilt_from_rates`` once DAT was scanned."""
        return self._mode

    # --------------------------------------------------------------- catalog
    def catalog(self) -> list[ChannelRef]:
        out = [ChannelRef("analog", c.index, c.name) for c in self.config.analogs]
        out += [ChannelRef("digital", c.index, c.name) for c in self.config.digitals]
        return out

    def _resolve_channels(self, select, kind: str
                          ) -> tuple[list, list[int]]:
        channels = self.config.analogs if kind == "analog" else self.config.digitals
        if not select:
            chosen = list(channels)
        else:
            by_name = {c.name: c for c in channels}
            by_index = {c.index: c for c in channels}
            chosen = []
            for ref in select:
                if isinstance(ref, int):
                    key = ref
                    found = by_index.get(key)
                else:
                    key = str(ref)
                    found = by_name.get(key)
                    if found is None and key.isdigit():
                        found = by_index.get(int(key))
                if found is None:
                    raise ChannelError(
                        f"no {kind} channel matching {ref!r}; available: "
                        + ", ".join(f"{c.index}:{c.name}" for c in channels),
                    )
                if found not in chosen:
                    chosen.append(found)
        positions = [c.index - 1 for c in chosen]
        return chosen, positions

    # ------------------------------------------------------------- frame time
    def _base_time(self, frame: RawFrame, state: _ScanState) -> int:
        if state.mode == "rebuilt_from_rates":
            return self.clock.start_epoch_us + self.clock.offset_for_sample(
                frame.sample_number,
            )
        if frame.raw_timestamp == 0:
            # Legal only for the first frame in timestamp mode: offset zero.
            return self.clock.start_epoch_us
        return interpret_timestamp(
            frame.raw_timestamp, self.clock, frame.sample_number,
            self.config.dat_path,
        )

    def _check_frame(self, frame: RawFrame, state: _ScanState) -> int:
        """Sequence/timestamp checks shared by every streaming entry point."""
        if frame.sample_number != state.expected_sample:
            raise FrameSequenceError(
                f"sample number jump: expected {state.expected_sample}, "
                f"frame carries {frame.sample_number}",
                path=self.config.dat_path, frame=state.frames_seen,
                sample=frame.sample_number,
            )
        # Online enforcement of the all-or-nothing zero-timestamp rule:
        # all-zero files use rate rebuild; otherwise a zero timestamp is legal
        # only at the first frame (offset zero from start).  A zero on any
        # later frame is rejected -- the reader never switches algorithms.
        if frame.raw_timestamp == 0:
            if state.frames_seen > 0 and state.any_nonzero:
                raise TimeError(
                    "mixed zero/non-zero DAT timestamps: frame "
                    f"{state.frames_seen} (sample {frame.sample_number}) has "
                    "timestamp 0 while other frames carry non-zero "
                    "timestamps; cannot switch silently between DAT "
                    "timestamps and CFG rate rebuild",
                    path=self.config.dat_path, frame=state.frames_seen,
                    sample=frame.sample_number,
                )
            if state.mode == "unknown":
                # Tentative: stays "rebuilt" only if every frame stays zero;
                # the first later non-zero frame flips it to timestamp mode.
                state.mode = "rebuilt_from_rates"
        else:
            state.any_nonzero = True
            if state.mode in ("unknown", "rebuilt_from_rates"):
                state.mode = "dat_timestamp"
        base_us = self._base_time(frame, state)
        if state.last_base_us is not None and base_us < state.last_base_us:
            raise TimeError(
                f"time regression: frame time {base_us} is earlier than the "
                f"previous frame time {state.last_base_us}",
                path=self.config.dat_path, frame=state.frames_seen,
                sample=frame.sample_number,
            )
        state.last_base_us = base_us
        state.expected_sample += 1
        state.frames_seen += 1
        return base_us

    # ------------------------------------------------------------------ scan
    def validate(self, batch_frames: int = 4096) -> dict:
        """Full bounded-memory import check; returns a summary dict."""
        state = _ScanState()
        with FrameStream(self.config, batch_frames=batch_frames) as stream:
            for batch in stream:
                for frame in batch:
                    self._check_frame(frame, state)
        self._finalize_scan(state)
        return self.inspection_summary(state)

    def _finalize_scan(self, state: _ScanState) -> None:
        if state.frames_seen == 0:
            raise FrameSequenceError(
                "DAT contains no frames", path=self.config.dat_path,
            )
        if state.frames_seen != self.config.last_end_sample:
            raise FrameSequenceError(
                f"DAT has {state.frames_seen} frames but the CFG's last "
                f"cumulative end sample is {self.config.last_end_sample}",
                path=self.config.dat_path, sample=self.config.last_end_sample,
            )
        self._mode = state.mode
        self.clock = replace(self.clock, timestamp_mode=state.mode)

    def inspection_summary(self, state: _ScanState | None = None) -> dict:
        return {
            "cfg_file": str(self.config.cfg_path),
            "dat_file": str(self.config.dat_path),
            "station_name": self.config.station_name,
            "recorder_id": self.config.recorder_id,
            "revision": self.config.revision,
            "file_type": self.config.file_type,
            "timezone": self.timezone,
            "timestamp_mode": self._mode or "unknown",
            "timemult": str(self.clock.timemult),
            "frames": state.frames_seen if state else None,
            "n_analog": self.config.n_analog,
            "n_digital": self.config.n_digital,
            "n_digital_groups": self.config.n_digital_groups,
            "start_epoch_us": self.clock.start_epoch_us,
            "trigger_epoch_us": self.clock.trigger_epoch_us,
            "trigger_offset_us": self.clock.trigger_offset_us,
            "sample_rates": [
                {"rate_hz": str(seg.rate_hz),
                 "end_sample": seg.end_sample,
                 "start_offset_us": seg.start_offset_us}
                for seg in self.clock.segments
            ],
        }

    # ---------------------------------------------------------------- window
    def iter_analog_window(self, start_rel_us: int, end_rel_us: int,
                           channels=None, side: str = "secondary",
                           batch_size: int = 8192,
                           batch_frames: int = 4096):
        """Yield analog ``RecordBatch`` rows whose *skewed* time is in window.

        Both window edges are inclusive and expressed relative to the trigger
        instant in microseconds.  Filtering uses ``frame_time + channel skew``,
        so two channels with different skews are judged independently.
        """
        if side not in ("primary", "secondary"):
            raise ChannelError("side must be 'primary' or 'secondary'")
        if end_rel_us < start_rel_us:
            raise TimeError("window end must not precede window start")
        chosen, positions = self._resolve_channels(channels, "analog")
        state = _ScanState()
        cols = {name: [] for name in ANALOG_SCHEMA.names}
        emitted = 0

        def flush():
            nonlocal emitted
            if not cols["sample_number"]:
                return None
            batch = pa.RecordBatch.from_pydict(cols, schema=ANALOG_SCHEMA)
            emitted += batch.num_rows
            for name in cols:
                cols[name] = []
            return batch

        with FrameStream(self.config, batch_frames=batch_frames) as stream:
            for frame_batch in stream:
                for frame in frame_batch:
                    base_us = self._check_frame(frame, state)
                    for pos, channel in zip(positions, chosen):
                        raw = frame.analog_raw[pos]
                        marked = restore_marked_side(channel, raw)
                        value = convert_side(channel, marked, side)
                        skew_us = int(round(channel.skew_us))
                        time_us = base_us + skew_us
                        rel = time_us - self.clock.trigger_epoch_us
                        if start_rel_us <= rel <= end_rel_us:
                            cols["sample_number"].append(frame.sample_number)
                            cols["frame_timestamp_us"].append(base_us)
                            cols["time_us"].append(time_us)
                            cols["time_utc"].append(time_us)
                            cols["relative_trigger_us"].append(rel)
                            cols["channel_index"].append(channel.index)
                            cols["channel_name"].append(channel.name)
                            cols["value"].append(value)
                            cols["raw_value"].append(raw)
                            cols["unit"].append(channel.unit)
                            cols["phase"].append(channel.phase)
                            cols["ps_marker"].append(channel.ps_marker)
                            cols["value_side"].append(side)
                            cols["skew_us"].append(channel.skew_us)
                            if len(cols["sample_number"]) >= batch_size:
                                out = flush()
                                if out is not None:
                                    yield out
        self._finalize_scan(state)
        tail = flush()
        if tail is not None:
            yield tail

    def iter_digital_window(self, start_rel_us: int, end_rel_us: int,
                            channels=None, batch_size: int = 8192,
                            batch_frames: int = 4096):
        """Yield digital ``RecordBatch`` rows inside the inclusive window."""
        if end_rel_us < start_rel_us:
            raise TimeError("window end must not precede window start")
        chosen, positions = self._resolve_channels(channels, "digital")
        state = _ScanState()
        cols = {name: [] for name in DIGITAL_SCHEMA.names}

        def flush():
            if not cols["sample_number"]:
                return None
            batch = pa.RecordBatch.from_pydict(cols, schema=DIGITAL_SCHEMA)
            for name in cols:
                cols[name] = []
            return batch

        with FrameStream(self.config, batch_frames=batch_frames) as stream:
            for frame_batch in stream:
                for frame in frame_batch:
                    base_us = self._check_frame(frame, state)
                    rel_base = base_us - self.clock.trigger_epoch_us
                    if not (start_rel_us <= rel_base <= end_rel_us):
                        continue
                    states = unpack_digital_words(frame.digital_words,
                                                  self.config.digitals)
                    for pos, channel in zip(positions, chosen):
                        state_value = states[pos]
                        cols["sample_number"].append(frame.sample_number)
                        cols["time_us"].append(base_us)
                        cols["time_utc"].append(base_us)
                        cols["relative_trigger_us"].append(rel_base)
                        cols["channel_index"].append(channel.index)
                        cols["channel_name"].append(channel.name)
                        cols["state"].append(state_value)
                        cols["normal_state"].append(channel.normal_state)
                        cols["deviates_from_normal"].append(
                            state_value != channel.normal_state,
                        )
                        cols["phase"].append(channel.phase)
                        if len(cols["sample_number"]) >= batch_size:
                            out = flush()
                            if out is not None:
                                yield out
        self._finalize_scan(state)
        tail = flush()
        if tail is not None:
            yield tail
