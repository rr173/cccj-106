"""Parser and typed representation of an IEEE C37.111-1999 CFG file.

The CFG file is plain comma-separated text.  Only the 1999 revision layout is
accepted::

    station_name,rec_dev_id,revision_year
    TT,##A,##D
    An,ch_id,ph,ccbm,units,a,b,skew,min,max,primary,secondary,PS   (analog lines)
    Dn,ch_id,ph,ccbm,y                                            (digital lines)
    nrates
    samp,endsamp                                                  (nrates lines)
    dd/mm/yyyy,hh:mm:ss[.ffffff]                                  (start)
    dd/mm/yyyy,hh:mm:ss[.ffffff]                                  (trigger)
    FT                                                            (ASCII / BINARY)
    timemult

Everything that can be checked without reading the DAT is checked here; layout
cross-checks against actual frames live in :mod:`minenergy.comtrade.frames`.
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import datetime
from fractions import Fraction
from pathlib import Path

from .errors import CfgParseError, ChannelError

SUPPORTED_REVISION = "1999"
SUPPORTED_FILE_TYPES = ("ASCII", "BINARY")


@dataclass(frozen=True)
class AnalogChannel:
    index: int  # 1-based ``An`` number, equals position among analog channels
    name: str
    phase: str
    component: str
    unit: str
    a: float
    b: float
    skew_us: float
    min_raw: float
    max_raw: float
    primary_ratio: float
    secondary_ratio: float
    ps_marker: str  # "P" -> ratios describe a primary-side channel; "S" secondary

    @property
    def kind(self) -> str:
        return "analog"


@dataclass(frozen=True)
class DigitalChannel:
    index: int  # 1-based ``Dn`` number, equals position among digital channels
    name: str
    phase: str
    component: str
    normal_state: int  # 0 or 1, the "normal" definition from the CFG

    @property
    def kind(self) -> str:
        return "digital"


@dataclass(frozen=True)
class SampleRate:
    rate_hz: float
    end_sample: int  # inclusive cumulative 1-based sample number


@dataclass(frozen=True)
class ComtradeConfig:
    cfg_path: Path
    station_name: str
    recorder_id: str
    revision: str
    analogs: tuple[AnalogChannel, ...]
    digitals: tuple[DigitalChannel, ...]
    rates: tuple[SampleRate, ...]
    start_wall: datetime  # naive wall-clock time, zone supplied by the caller
    trigger_wall: datetime
    file_type: str
    timemult: Fraction
    dat_path: Path
    n_digital_groups: int = field(compare=False, default=0)

    @property
    def n_analog(self) -> int:
        return len(self.analogs)

    @property
    def n_digital(self) -> int:
        return len(self.digitals)

    @property
    def n_total(self) -> int:
        return self.n_analog + self.n_digital

    @property
    def last_end_sample(self) -> int:
        return self.rates[-1].end_sample


def _read_rows(text: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text), skipinitialspace=True)
    return [[cell.strip() for cell in row] for row in reader if row]


def _require_fields(row: list[str], n: int, path: Path, line: int,
                    what: str) -> list[str]:
    if len(row) != n:
        raise CfgParseError(
            f"{what} expects {n} comma-separated fields, found {len(row)}",
            path=path, line=line,
        )
    return row


def _to_float(value: str, what: str, path: Path, line: int) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise CfgParseError(f"{what} must be numeric, got {value!r}",
                            path=path, line=line) from None
    if out != out or out in (float("inf"), float("-inf")):
        raise CfgParseError(f"{what} must be finite, got {value!r}",
                            path=path, line=line)
    return out


def _parse_wall(row: list[str], path: Path, line: int, what: str) -> datetime:
    _require_fields(row, 2, path, line, what)
    date_part, time_part = row
    try:
        day, month, year = (int(x) for x in date_part.split("/"))
    except ValueError:
        raise CfgParseError(
            f"{what} date must be dd/mm/yyyy, got {date_part!r}",
            path=path, line=line,
        ) from None
    if "." in time_part:
        head, frac = time_part.split(".", 1)
        if not frac.isdigit() or not (1 <= len(frac) <= 6):
            raise CfgParseError(
                f"{what} fractional seconds must be 1..6 digits, got {time_part!r}",
                path=path, line=line,
            )
        fmt = "%H:%M:%S"
    else:
        head, frac = time_part, ""
        fmt = "%H:%M:%S"
    try:
        clock = datetime.strptime(head, fmt)
    except ValueError:
        raise CfgParseError(
            f"{what} time must be hh:mm:ss, got {time_part!r}",
            path=path, line=line,
        ) from None
    micro = int((frac + "000000")[:6]) if frac else 0
    try:
        return datetime(year, month, day, clock.hour, clock.minute,
                        clock.second, micro)
    except ValueError as exc:
        raise CfgParseError(f"{what} is not a valid calendar time: {exc}",
                            path=path, line=line) from None


def _parse_analog(row: list[str], path: Path, line: int,
                  position: int) -> AnalogChannel:
    # An,ch_id,ph,ccbm,units,a,b,skew,min,max,primary,secondary,PS
    _require_fields(row, 13, path, line, f"analog channel line {position}")
    tag = row[0]
    if not tag or tag[0].upper() != "A" or not tag[1:].isdigit():
        raise CfgParseError(f"analog channel tag must look like An, got {tag!r}",
                            path=path, line=line)
    index = int(tag[1:])
    if index != position:
        raise CfgParseError(
            f"analog channel numbering must be consecutive from 1: "
            f"expected A{position}, got {tag}", path=path, line=line,
        )
    a = _to_float(row[5], "analog scaling a", path, line)
    b = _to_float(row[6], "analog scaling b", path, line)
    skew = _to_float(row[7], "analog channel skew", path, line)
    vmin = _to_float(row[8], "analog min value", path, line)
    vmax = _to_float(row[9], "analog max value", path, line)
    primary = _to_float(row[10], "primary ratio", path, line)
    secondary = _to_float(row[11], "secondary ratio", path, line)
    marker = row[12].upper()
    if marker not in ("P", "S"):
        raise ChannelError(
            f"P/S marker must be P or S, got {row[12]!r} (channel {tag})",
            path=path, line=line,
        )
    for label, value in (("primary ratio", primary), ("secondary ratio", secondary)):
        if value <= 0:
            raise ChannelError(
                f"{label} must be a positive finite number, got {value} "
                f"(channel {tag}); refusing a ratio that would produce infinities",
                path=path, line=line,
            )
    return AnalogChannel(
        index=index, name=row[1], phase=row[2], component=row[3],
        unit=row[4].strip('"'), a=a, b=b, skew_us=skew, min_raw=vmin,
        max_raw=vmax, primary_ratio=primary, secondary_ratio=secondary,
        ps_marker=marker,
    )


def _parse_digital(row: list[str], path: Path, line: int,
                   position: int) -> DigitalChannel:
    # Dn,ch_id,ph,ccbm,y
    _require_fields(row, 5, path, line, f"digital channel line {position}")
    tag = row[0]
    if not tag or tag[0].upper() != "D" or not tag[1:].isdigit():
        raise CfgParseError(f"digital channel tag must look like Dn, got {tag!r}",
                            path=path, line=line)
    index = int(tag[1:])
    if index != position:
        raise CfgParseError(
            f"digital channel numbering must be consecutive from 1: "
            f"expected D{position}, got {tag}", path=path, line=line,
        )
    if row[4] not in ("0", "1"):
        raise CfgParseError(
            f"digital normal state must be 0 or 1, got {row[4]!r} (channel {tag})",
            path=path, line=line,
        )
    return DigitalChannel(
        index=index, name=row[1], phase=row[2], component=row[3],
        normal_state=int(row[4]),
    )


def _resolve_dat(cfg_path: Path) -> Path:
    """Find the sibling DAT regardless of extension casing."""
    for candidate in sorted(cfg_path.parent.iterdir()):
        if candidate.stem == cfg_path.stem and candidate.suffix.lower() == ".dat":
            return candidate
    raise CfgParseError(
        f"no sibling .dat file found for {cfg_path.name} "
        f"(extension casing is irrelevant)", path=cfg_path,
    )


def parse_cfg(cfg_path: str | Path, dat_path: str | Path | None = None) -> ComtradeConfig:
    path = Path(cfg_path)
    if not path.is_file():
        raise CfgParseError(f"CFG file does not exist: {path}", path=path)
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise CfgParseError(f"CFG is not valid UTF-8 text: {exc}", path=path) from None

    rows = _read_rows(text)
    if len(rows) < 7:
        raise CfgParseError("CFG has too few lines to be a valid C37.111-1999 file",
                            path=path)

    line_no = 1
    header = _require_fields(rows[0], 3, path, line_no, "station header line")
    station, recorder, revision = header
    if revision != SUPPORTED_REVISION:
        raise CfgParseError(
            f"only IEEE C37.111-{SUPPORTED_REVISION} is supported "
            f"(see format boundary notes); revision field is {revision!r}",
            path=path, line=line_no,
        )

    line_no = 2
    counts = _require_fields(rows[1], 3, path, line_no, "channel count line")
    total_tag, analog_tag, digital_tag = (c.upper() for c in counts)
    if not total_tag.isdigit() or not analog_tag.endswith("A") or not digital_tag.endswith("D"):
        raise CfgParseError(
            f"channel count line must look like 'TT,##A,##D', got {counts!r}",
            path=path, line=line_no,
        )
    n_total, n_analog, n_digital = int(total_tag), int(analog_tag[:-1]), int(digital_tag[:-1])
    if n_analog + n_digital != n_total or n_total < 0:
        raise CfgParseError(
            f"channel counts inconsistent: total={n_total}, "
            f"analog={n_analog}, digital={n_digital}",
            path=path, line=line_no,
        )

    cursor = 2
    analogs: list[AnalogChannel] = []
    for position in range(1, n_analog + 1):
        analogs.append(_parse_analog(rows[cursor], path, cursor + 1, position))
        cursor += 1
    digitals: list[DigitalChannel] = []
    for position in range(1, n_digital + 1):
        digitals.append(_parse_digital(rows[cursor], path, cursor + 1, position))
        cursor += 1

    nrates_row = rows[cursor]
    _require_fields(nrates_row, 1, path, cursor + 1, "sample-rate count line")
    if not nrates_row[0].isdigit() or int(nrates_row[0]) <= 0:
        raise CfgParseError(
            f"nrates must be a positive integer, got {nrates_row[0]!r}",
            path=path, line=cursor + 1,
        )
    nrates = int(nrates_row[0])
    cursor += 1

    rates: list[SampleRate] = []
    prev_end = 0
    for seg in range(nrates):
        line = cursor + 1
        row = _require_fields(rows[cursor], 2, path, line, f"sample-rate line {seg + 1}")
        rate = _to_float(row[0], "sample rate", path, line)
        if rate <= 0:
            raise CfgParseError(f"sample rate must be positive, got {row[0]!r}",
                                path=path, line=line)
        if not row[1].lstrip("-").isdigit():
            raise CfgParseError(f"end sample must be an integer, got {row[1]!r}",
                                path=path, line=line)
        end_sample = int(row[1])
        if end_sample <= prev_end:
            raise CfgParseError(
                f"end sample numbers must be strictly increasing cumulative "
                f"counts: segment {seg + 1} ends at {end_sample}, previous "
                f"segment ended at {prev_end}", path=path, line=line,
            )
        rates.append(SampleRate(rate, end_sample))
        prev_end = end_sample
        cursor += 1

    if len(rows) - cursor != 4:
        raise CfgParseError(
            f"expected exactly 4 trailing lines (start, trigger, file type, "
            f"timemult) after {nrates} sample-rate lines, found "
            f"{len(rows) - cursor}", path=path, line=cursor + 1,
        )
    start_wall = _parse_wall(rows[cursor], path, cursor + 1, "start time")
    trigger_wall = _parse_wall(rows[cursor + 1], path, cursor + 2, "trigger time")
    ft_row = _require_fields(rows[cursor + 2], 1, path, cursor + 3, "file type line")
    file_type = ft_row[0].upper()
    if file_type not in SUPPORTED_FILE_TYPES:
        raise CfgParseError(
            f"file type must be one of {SUPPORTED_FILE_TYPES}, got {ft_row[0]!r}",
            path=path, line=cursor + 3,
        )
    tm_row = _require_fields(rows[cursor + 3], 1, path, cursor + 4, "timemult line")
    try:
        timemult = Fraction(tm_row[0])
    except ValueError:
        raise CfgParseError(f"timemult must be numeric, got {tm_row[0]!r}",
                            path=path, line=cursor + 4) from None
    if timemult <= 0:
        raise CfgParseError(f"timemult must be positive, got {tm_row[0]!r}",
                            path=path, line=cursor + 4)

    dat = Path(dat_path) if dat_path else _resolve_dat(path)
    if not dat.is_file():
        raise CfgParseError(f"DAT file does not exist: {dat}", path=path)

    n_groups = (n_digital + 15) // 16
    return ComtradeConfig(
        cfg_path=path, station_name=station, recorder_id=recorder,
        revision=revision, analogs=tuple(analogs), digitals=tuple(digitals),
        rates=tuple(rates), start_wall=start_wall, trigger_wall=trigger_wall,
        file_type=file_type, timemult=timemult, dat_path=dat,
        n_digital_groups=n_groups,
    )
