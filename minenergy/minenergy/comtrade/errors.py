"""Errors raised by the COMTRADE ingestion path.

Every error that means "this recording cannot be trusted" is a
:class:`ComtradeError` and carries the offending file plus, where it makes
sense, the line/frame/sample position at which the problem was detected so the
CLI can point the maintenance engineer at the exact place.
"""
from __future__ import annotations

from pathlib import Path


class ComtradeError(Exception):
    """Base class for every rejected recording."""

    def __init__(self, message: str, *, path: str | Path | None = None,
                 line: int | None = None, frame: int | None = None,
                 sample: int | None = None, code: str | None = None):
        self.message = message
        self.path = str(path) if path is not None else None
        self.line = line
        self.frame = frame
        self.sample = sample
        self.code = code or self.__class__.__name__
        super().__init__(self.render())

    def render(self) -> str:
        bits = []
        if self.path:
            bits.append(self.path)
        if self.line is not None:
            bits.append(f"CFG line {self.line}")
        if self.frame is not None:
            bits.append(f"frame {self.frame}")
        if self.sample is not None:
            bits.append(f"sample {self.sample}")
        location = " [" + ", ".join(bits) + "]" if bits else ""
        return f"{self.code}: {self.message}{location}"


class CfgParseError(ComtradeError):
    """The CFG text itself is malformed or violates the 1999 layout."""


class FrameLayoutError(ComtradeError):
    """Declared channel/group counts do not match the physical DAT layout."""


class TimeError(ComtradeError):
    """Timestamp interpretation failed (mixed zeros, clock regression ...)."""


class ClockZoneError(ComtradeError):
    """The CFG wall-clock time is ambiguous or missing in the given zone."""


class FrameSequenceError(ComtradeError):
    """Sample numbers skip, or the DAT ends with a partial frame."""


class ChannelError(ComtradeError):
    """Channel metadata (ratio/skew/P-S marker) cannot be used."""
