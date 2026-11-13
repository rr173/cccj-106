"""CFG parser, ratio validation and clock-resolution unit tests."""
from __future__ import annotations

from fractions import Fraction

import pytest

from minenergy.comtrade.clock import build_clock
from minenergy.comtrade.config import parse_cfg
from minenergy.comtrade.errors import (
    CfgParseError,
    ChannelError,
    ClockZoneError,
    TimeError,
)
from minenergy.comtrade.recording import (
    convert_side,
    restore_secondary,
    unpack_digital_words,
)
from minenergy.comtrade.config import AnalogChannel, DigitalChannel


def _write_cfg(tmp_path, text, dat_bytes=b""):
    cfg = tmp_path / "x.cfg"
    dat = tmp_path / "x.dat"
    cfg.write_text(text, encoding="utf-8")
    dat.write_bytes(dat_bytes)
    return cfg


MINIMAL = """SUB,DEV,1999
2,1A,1D
A1,IA,A,,A,0.1,0.5,0,-32768,32767,1000,5,P
D1,TRIP,,,0
1
1000,2
01/01/2025,00:00:00.000000
01/01/2025,00:00:00.001000
BINARY
1
"""


def test_parses_minimal_cfg(tmp_path):
    cfg = _write_cfg(tmp_path, MINIMAL)
    parsed = parse_cfg(cfg)
    assert parsed.station_name == "SUB"
    assert parsed.n_analog == 1 and parsed.n_digital == 1
    assert parsed.n_digital_groups == 1
    assert parsed.timemult == Fraction(1)
    assert parsed.rates[0].end_sample == 2
    assert parsed.file_type == "BINARY"


def test_rejects_non_1999_revision(tmp_path):
    cfg = _write_cfg(tmp_path, MINIMAL.replace("1999", "2013"))
    with pytest.raises(CfgParseError, match="only IEEE C37.111-1999"):
        parse_cfg(cfg)


def test_rejects_bad_ratio(tmp_path):
    text = MINIMAL.replace("1000,5,P", "0,5,P")
    cfg = _write_cfg(tmp_path, text)
    with pytest.raises(ChannelError, match="primary ratio"):
        parse_cfg(cfg)


def test_rejects_bad_ps_marker(tmp_path):
    text = MINIMAL.replace("1000,5,P", "1000,5,X")
    cfg = _write_cfg(tmp_path, text)
    with pytest.raises(ChannelError, match="P/S marker"):
        parse_cfg(cfg)


def test_rejects_non_increasing_end_samples(tmp_path):
    text = MINIMAL.replace(
        "1\n1000,2\n", "2\n1000,2\n500,2\n",
    )
    cfg = _write_cfg(tmp_path, text)
    with pytest.raises(CfgParseError, match="strictly increasing"):
        parse_cfg(cfg)


def test_timemult_fraction_is_exact(tmp_path):
    text = MINIMAL.replace("\nBINARY\n1\n", "\nBINARY\n1.5\n")
    cfg = _write_cfg(tmp_path, text)
    parsed = parse_cfg(cfg)
    assert parsed.timemult == Fraction(3, 2)


def test_digital_packing_order():
    digitals = tuple(DigitalChannel(i, f"D{i}", "", "", 0) for i in range(1, 18))
    states = unpack_digital_words((0x8001, 0x0001), digitals)
    assert states[0] == 1       # D1: bit 0 word 0
    assert states[15] == 1      # D16: bit 15 word 0
    assert states[16] == 1      # D17: bit 0 word 1
    assert states[1] == 0
    assert states[14] == 0


def test_restore_and_conversion_are_finite():
    p_chan = AnalogChannel(1, "x", "", "", "A", 0.1, 0.5, 0, 0, 0,
                           1000.0, 5.0, "P")
    s_chan = AnalogChannel(2, "y", "", "", "V", 2.0, -1.0, 0, 0, 0,
                           6600.0, 110.0, "S")
    sec_p = restore_secondary(p_chan, 100)  # 10.5
    assert convert_side(p_chan, sec_p, "primary") == 10.5
    assert convert_side(p_chan, sec_p, "secondary") == 10.5 / 200
    sec_s = restore_secondary(s_chan, 10)  # 19.0
    assert convert_side(s_chan, sec_s, "secondary") == 19.0
    assert convert_side(s_chan, sec_s, "primary") == 19.0 * 60


def test_clock_resolves_unambiguous_zone(tmp_path):
    cfg = parse_cfg(_write_cfg(tmp_path, MINIMAL))
    clock = build_clock(cfg, "UTC")
    assert clock.trigger_offset_us == 1000
    assert clock.start_epoch_us + 1000 == clock.trigger_epoch_us


def test_clock_rejects_ambiguous_and_missing(tmp_path):
    fall = MINIMAL.replace(
        "01/01/2025,00:00:00.000000", "02/11/2025,01:30:00.000000",
    ).replace(
        "01/01/2025,00:00:00.001000", "02/11/2025,01:30:00.001000",
    )
    with pytest.raises(ClockZoneError, match="ambiguous"):
        build_clock(parse_cfg(_write_cfg(tmp_path, fall)), "America/New_York")
    spring = MINIMAL.replace(
        "01/01/2025,00:00:00.000000", "09/03/2025,02:30:00.000000",
    ).replace(
        "01/01/2025,00:00:00.001000", "09/03/2025,02:30:00.001000",
    )
    with pytest.raises(ClockZoneError, match="does not exist"):
        build_clock(parse_cfg(_write_cfg(tmp_path, spring)),
                    "America/New_York")


def test_trigger_before_start_is_rejected(tmp_path):
    text = MINIMAL.replace(
        "01/01/2025,00:00:00.001000", "31/12/2024,23:59:59.999000",
    )
    with pytest.raises(TimeError, match="trigger time .* precedes start"):
        build_clock(parse_cfg(_write_cfg(tmp_path, text)), "UTC")


def test_ascii_file_type_accepted(tmp_path):
    text = MINIMAL.replace("BINARY", "ASCII")
    parsed = parse_cfg(_write_cfg(tmp_path, text))
    assert parsed.file_type == "ASCII"


def test_unknown_file_type_rejected(tmp_path):
    text = MINIMAL.replace("BINARY", "BINARY32")
    with pytest.raises(CfgParseError, match="file type"):
        parse_cfg(_write_cfg(tmp_path, text))
