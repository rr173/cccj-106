"""Streaming DAT frame decoders for ASCII and BINARY recordings.

The decoder never materialises the whole DAT and never turns every sample into
a long-lived Python object graph: BINARY files are read in fixed-size chunks
and decoded with the stdlib ``array`` module (native little-endian layout is
checked at import time), ASCII files are read one line at a time.  Raw frames
are handed off in bounded lists; only the current batch lives in memory.

Structural problems (a trailing partial frame, an ASCII line whose field count
does not match the CFG channel/group declaration) are rejected here with the
file and frame position attached.  Semantic checks (sample numbers, timestamps,
clock ordering) live in :mod:`minenergy.comtrade.recording`.
"""
from __future__ import annotations

import struct
import sys
from dataclasses import dataclass

from .config import ComtradeConfig
from .errors import FrameLayoutError, FrameSequenceError

if sys.byteorder != "little":  # pragma: no cover - the spec mandates LE frames
    raise RuntimeError(
        "BINARY COMTRADE frames are little-endian; this platform is "
        f"{sys.byteorder}-endian and is not supported by the streaming reader"
    )

_HEADER_BYTES = 8  # uint32 sample number + uint32 timestamp


@dataclass(slots=True)
class RawFrame:
    sample_number: int
    raw_timestamp: int
    analog_raw: tuple[int, ...]     # one signed int16 per analog channel
    digital_words: tuple[int, ...]  # one uint16 per packed digital group


class FrameStream:
    """Yields :class:`RawFrame` lists of at most ``batch_frames`` items."""

    def __init__(self, config: ComtradeConfig, batch_frames: int = 4096):
        if batch_frames <= 0:
            raise ValueError("batch_frames must be positive")
        self.config = config
        self.batch_frames = int(batch_frames)
        self.path = config.dat_path
        self._fh = None
        if config.file_type == "BINARY":
            self.frame_size = (
                _HEADER_BYTES
                + 2 * config.n_analog
                + 2 * config.n_digital_groups
            )
            self._struct = struct.Struct(
                "<II" + "h" * config.n_analog + "H" * config.n_digital_groups
            )
            self._iter = self._iter_binary
        else:
            self.frame_size = None
            self._struct = None
            self._iter = self._iter_ascii
        self.frames_emitted = 0

    def __enter__(self) -> "FrameStream":
        try:
            self._fh = open(self.path, "rb")
        except OSError as exc:
            raise FrameLayoutError(f"cannot open DAT: {exc}", path=self.path) from None
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __iter__(self):
        if self._fh is None:
            raise RuntimeError("FrameStream must be used as a context manager")
        return self._iter()

    # ------------------------------------------------------------------ BINARY
    def _iter_binary(self):
        cfg = self.config
        na, ng = cfg.n_analog, cfg.n_digital_groups
        size = self.frame_size
        st = self._struct
        data = self._fh.read(size * self.batch_frames)
        frame_index = 0
        while data:
            if len(data) % size != 0:
                consumed = self.frames_emitted * size
                raise FrameSequenceError(
                    f"trailing partial frame: {len(data) % size} trailing bytes "
                    f"after {frame_index} complete frames in this read "
                    f"(byte offset {consumed}); a BINARY frame must be exactly "
                    f"{size} bytes (8 header + 2*{na} analog + 2*{ng} digital "
                    "groups). The CFG channel/group declaration does not match "
                    "the physical frame layout",
                    path=self.path, frame=frame_index,
                )
            n_frames = len(data) // size
            batch: list[RawFrame] = []
            for k in range(n_frames):
                row = st.unpack_from(data, k * size)
                batch.append(RawFrame(
                    sample_number=row[0],
                    raw_timestamp=row[1],
                    analog_raw=row[2:2 + na] if na else (),
                    digital_words=row[2 + na:] if ng else (),
                ))
            frame_index += n_frames
            self.frames_emitted += n_frames
            yield batch
            data = self._fh.read(size * self.batch_frames)

    # ------------------------------------------------------------------- ASCII
    def _iter_ascii(self):
        cfg = self.config
        na, ng = cfg.n_analog, cfg.n_digital_groups
        expected_fields = 2 + na + ng
        batch: list[RawFrame] = []
        frame_index = 0
        for line_no, raw_line in enumerate(self._fh, start=1):
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split(b",")
            if len(parts) != expected_fields:
                raise FrameLayoutError(
                    f"ASCII frame {frame_index} (file line {line_no}) has "
                    f"{len(parts)} fields but the CFG declares {na} analog "
                    f"channels and {ng} digital groups, requiring "
                    f"{expected_fields} fields per frame "
                    "(sample,timestamp,analogs...,digital groups...)",
                    path=self.path, frame=frame_index, line=line_no,
                )
            try:
                sample_number = int(parts[0])
                raw_timestamp = int(parts[1])
                analog_tuple = tuple(int(x) for x in parts[2:2 + na]) if na else ()
                word_tuple = tuple(int(x) for x in parts[2 + na:]) if ng else ()
            except ValueError as exc:
                raise FrameLayoutError(
                    f"ASCII frame {frame_index} (file line {line_no}) contains "
                    f"a non-integer value: {exc}",
                    path=self.path, frame=frame_index, line=line_no,
                ) from None
            for pos, value in enumerate(analog_tuple):
                if not -(1 << 15) <= value < (1 << 15):
                    raise FrameLayoutError(
                        f"analog channel {pos + 1} value {value} does not fit "
                        f"the int16 range in frame {frame_index}",
                        path=self.path, frame=frame_index,
                    )
            for pos, value in enumerate(word_tuple):
                if not 0 <= value < (1 << 16):
                    raise FrameLayoutError(
                        f"digital group {pos + 1} value {value} does not fit "
                        f"the uint16 range in frame {frame_index}",
                        path=self.path, frame=frame_index,
                    )
            batch.append(RawFrame(sample_number, raw_timestamp,
                                  analog_tuple, word_tuple))
            frame_index += 1
            if len(batch) >= self.batch_frames:
                self.frames_emitted += len(batch)
                yield batch
                batch = []
        if batch:
            self.frames_emitted += len(batch)
            yield batch
