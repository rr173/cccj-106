"""Every corrupt fixture must be rejected with file and position attached."""
from __future__ import annotations

import pytest

from minenergy.comtrade.errors import (
    ClockZoneError,
    FrameLayoutError,
    FrameSequenceError,
    TimeError,
)
from minenergy.comtrade.recording import Recording

CORRUPT_EXPECTATIONS = {
    "truncated_binary": (FrameSequenceError, "partial frame"),
    "truncated_ascii": (FrameLayoutError, "fields"),
    "sample_skip_binary": (FrameSequenceError, "sample number jump"),
    "sample_skip_ascii": (FrameSequenceError, "sample number jump"),
    "wrong_channel_count_binary": (FrameSequenceError, "partial frame"),
    "wrong_channel_count_ascii": (FrameLayoutError, "fields"),
    "mixed_zero_timestamps_binary": (TimeError, "mixed zero"),
    "mixed_zero_timestamps_ascii": (TimeError, "mixed zero"),
    "time_regression_binary": (TimeError, "regression"),
}


@pytest.mark.parametrize("name,expectation", sorted(CORRUPT_EXPECTATIONS.items()))
def test_corrupt_pair_rejected(corrupt_root, name, expectation):
    exc_type, fragment = expectation
    cfg = corrupt_root / name / "rec.cfg"
    rec = Recording.open(cfg, "UTC")
    with pytest.raises(exc_type) as excinfo:
        rec.validate()
    rendered = excinfo.value.render()
    assert fragment in rendered
    assert "rec.dat" in rendered or "rec.cfg" in rendered
    # Positional detail must be present for frame-level failures.
    if exc_type in (FrameSequenceError, TimeError, FrameLayoutError):
        assert "frame" in rendered or "line" in rendered


def test_dst_fallback_is_ambiguous(corrupt_root):
    with pytest.raises(ClockZoneError, match="ambiguous"):
        Recording.open(
            corrupt_root / "dst_ambiguous_fallback" / "rec.cfg",
            "America/New_York",
        )


def test_dst_spring_forward_is_nonexistent(corrupt_root):
    with pytest.raises(ClockZoneError, match="does not exist"):
        Recording.open(
            corrupt_root / "dst_nonexistent_springforward" / "rec.cfg",
            "America/New_York",
        )


def test_unknown_timezone_is_rejected(reference_root):
    with pytest.raises(ClockZoneError, match="unknown IANA timezone"):
        Recording.open(reference_root / "rates_binary" / "rec.cfg",
                       "Not/AZone")


def test_missing_dat_is_rejected(tmp_path, reference_root):
    cfg_src = reference_root / "rates_binary" / "rec.cfg"
    cfg_dst = tmp_path / "lonely.cfg"
    cfg_dst.write_text(cfg_src.read_text(), encoding="utf-8")
    from minenergy.comtrade.errors import CfgParseError
    with pytest.raises(CfgParseError, match="no sibling .dat"):
        Recording.open(cfg_dst, "UTC")


def test_extension_casing_is_ignored(tmp_path, reference_root):
    cfg_src = reference_root / "rates_binary" / "rec.cfg"
    dat_src = reference_root / "rates_binary" / "rec.dat"
    (tmp_path / "wave.CFG").write_text(cfg_src.read_text(), encoding="utf-8")
    (tmp_path / "wave.DAT").write_bytes(dat_src.read_bytes())
    rec = Recording.open(tmp_path / "wave.CFG", "UTC")
    assert rec.validate()["frames"] == 10
