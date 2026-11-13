"""Time interpretation for COMTRADE recordings.

Two time bases appear in a COMTRADE pair:

* the CFG gives *naive wall-clock* start/trigger times plus per-segment sample
  rates and cumulative inclusive end-sample numbers;
* each DAT frame carries its own timestamp in microseconds.

Rules implemented here (exactly the ones that are easy to get wrong):

1. a non-zero DAT timestamp is multiplied by ``timemult`` and interpreted as
   microseconds; only the **all-zero** case allows rebuilding time from the CFG
   sample rates;
2. if only *some* timestamps are zero (other than the first point, whose zero is
   legal in timestamp mode as "no offset from start"), the file is rejected --
   the reader never silently switches algorithms;
3. rate-based rebuild treats end-sample numbers as inclusive cumulative counts:
   the first point sits at offset 0 and the first point of a later segment gets
   the sum of ``sample_count / rate`` of every preceding whole segment;
4. clock regression, sample-number skips and trailing partial frames are
   reported with their position and reject the whole import;
5. the CFG carries no zone, so the caller supplies an IANA name.  A wall time
   that is repeated (fall-back DST) or skipped (spring-forward) in that zone is
   reported as ambiguous/non-existent and never guessed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from fractions import Fraction
from zoneinfo import ZoneInfo

from .config import ComtradeConfig
from .errors import ClockZoneError, TimeError

MICRO = 1_000_000


@dataclass(frozen=True)
class RateSegment:
    index: int
    prev_end_sample: int  # cumulative sample number just before this segment
    end_sample: int       # inclusive cumulative 1-based end sample
    rate_hz: Fraction
    start_offset_us: int  # offset of the segment's first sample from start

    def offset_of(self, sample_number: int) -> int:
        index_in_segment = sample_number - self.prev_end_sample - 1
        delta = Fraction(index_in_segment, self.rate_hz) * MICRO
        return self.start_offset_us + int(delta)


@dataclass(frozen=True)
class ResolvedClock:
    """Everything needed to turn frame positions into UTC microseconds."""

    start_epoch_us: int
    trigger_epoch_us: int
    trigger_offset_us: int
    zone_name: str
    timemult: Fraction
    timestamp_mode: str = "unknown"  # finalized while the DAT is streamed
    segments: tuple[RateSegment, ...] = field(default=())

    @property
    def last_end_sample(self) -> int:
        return self.segments[-1].end_sample

    def offset_for_sample(self, sample_number: int) -> int:
        for seg in self.segments:
            if sample_number <= seg.end_sample:
                return seg.offset_of(sample_number)
        raise TimeError(
            f"sample {sample_number} is beyond the last CFG end sample "
            f"{self.last_end_sample}", sample=sample_number,
        )


def _wall_to_epoch(wall: datetime, zone_name: str, what: str,
                   cfg_path) -> tuple[int, int]:
    """Resolve a naive wall time in ``zone_name``.

    Returns ``(epoch_us, utc_offset_seconds)``.  Repeated times (DST fall-back)
    and non-existent times (spring-forward) are reported rather than guessed.
    """
    try:
        zone = ZoneInfo(zone_name)
    except Exception as exc:  # ZoneInfoNotFoundError and friends
        raise ClockZoneError(
            f"unknown IANA timezone {zone_name!r}: {exc}", path=cfg_path,
        ) from None

    # Both DST pathologies make fold 0 and fold 1 resolve to different
    # offsets; they are told apart by round-tripping through UTC.  A repeated
    # fall-back wall time still maps back to itself (two real instants share
    # the wall clock), while a skipped spring-forward wall time lands on a
    # different local time (no real instant carries it).
    offsets = set()
    for fold in (0, 1):
        candidate = wall.replace(fold=fold, tzinfo=zone)
        offsets.add(candidate.utcoffset())
    aware = wall.replace(tzinfo=zone)
    roundtrip = aware.astimezone(ZoneInfo("UTC")).astimezone(zone)
    exists = roundtrip.replace(tzinfo=None) == wall
    if len(offsets) > 1 and exists:
        raise ClockZoneError(
            f"{what} wall time {wall.isoformat()} is ambiguous in zone "
            f"{zone_name} (repeated during DST fall-back); the CFG carries no "
            f"UTC offset, so the caller must disambiguate instead of guessing",
            path=cfg_path,
        )
    if not exists:
        raise ClockZoneError(
            f"{what} wall time {wall.isoformat()} does not exist in zone "
            f"{zone_name} (skipped by a DST spring-forward); refusing to guess "
            f"an offset", path=cfg_path,
        )
    offset = aware.utcoffset()
    epoch_us = int(aware.timestamp() * MICRO)
    return epoch_us, int(offset.total_seconds())


def build_clock(config: ComtradeConfig, timezone: str) -> ResolvedClock:
    start_epoch, _ = _wall_to_epoch(
        config.start_wall, timezone, "start", config.cfg_path,
    )
    trigger_epoch, _ = _wall_to_epoch(
        config.trigger_wall, timezone, "trigger", config.cfg_path,
    )
    if trigger_epoch < start_epoch:
        raise TimeError(
            f"trigger time {config.trigger_wall.isoformat()} precedes start "
            f"time {config.start_wall.isoformat()} in {timezone}",
            path=config.cfg_path,
        )

    segments: list[RateSegment] = []
    cursor_us = 0
    prev_end = 0
    for i, rate_seg in enumerate(config.rates):
        rate = Fraction(str(rate_seg.rate_hz))
        segments.append(RateSegment(
            index=i, prev_end_sample=prev_end,
            end_sample=rate_seg.end_sample, rate_hz=rate,
            start_offset_us=cursor_us,
        ))
        count = rate_seg.end_sample - prev_end
        cursor_us += int(Fraction(count, rate) * MICRO)
        prev_end = rate_seg.end_sample

    return ResolvedClock(
        start_epoch_us=start_epoch,
        trigger_epoch_us=trigger_epoch,
        trigger_offset_us=trigger_epoch - start_epoch,
        zone_name=timezone,
        timemult=config.timemult,
        segments=tuple(segments),
    )


def interpret_timestamp(raw_timestamp: int, clock: ResolvedClock,
                        sample_number: int, path) -> int:
    """Absolute epoch microseconds for a non-zero DAT timestamp."""
    if raw_timestamp <= 0:
        raise TimeError(
            "zero DAT timestamp outside the all-zero / first-point cases",
            path=path, sample=sample_number,
        )
    offset_us = int(Fraction(raw_timestamp) * clock.timemult)
    return clock.start_epoch_us + offset_us
