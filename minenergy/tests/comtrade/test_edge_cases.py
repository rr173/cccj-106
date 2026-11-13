"""Additional edge cases built as tiny one-off recordings."""
from __future__ import annotations

import math

import pytest

from minenergy.comtrade.errors import (
    ChannelError,
    FrameSequenceError,
)
from minenergy.comtrade.fixtures import (
    Waveform,
    render_ascii_dat,
    render_binary_dat,
    render_cfg,
)
from minenergy.comtrade.recording import Recording


def _pair(tmp_path, wave, file_type, *, dat_bytes=None, cfg_text=None):
    cfg = tmp_path / "rec.cfg"
    dat = tmp_path / "rec.dat"
    cfg.write_text(cfg_text or render_cfg(wave, file_type), encoding="utf-8")
    if dat_bytes is None:
        dat_bytes = (render_binary_dat(wave) if file_type == "BINARY"
                     else render_ascii_dat(wave))
    dat.write_bytes(dat_bytes)
    return cfg


def _wave(timestamps, *, rates=((1000.0, 4),), timemult="1",
          analog_specs=None, analog_raw=None, digital_normal=(0,),
          digital_words=None):
    n = len(timestamps)
    analog_specs = analog_specs or (
        ("Ia", "A", "A", 0.1, 0.0, 0.0, 100.0, 5.0, "S"),
    )
    analog_raw = analog_raw or tuple((i,) for i in range(n))
    digital_words = digital_words or tuple((0,) for _ in range(n))
    return Waveform(
        analog_specs=tuple(analog_specs),
        analog_raw=tuple(analog_raw),
        digital_normal=tuple(digital_normal),
        digital_words=tuple(digital_words),
        rates=tuple(rates),
        timestamps=tuple(timestamps),
        start_wall=(2025, 1, 1, 0, 0, 0, 0),
        trigger_wall=(2025, 1, 1, 0, 0, 0, 1000),
        timemult=timemult,
    )


def test_fractional_timemult_is_applied_exactly(tmp_path):
    # timemult 1.5: raw stamp 2000 -> 3000 us offset.
    wave = _wave((0, 2000, 4000, 6000), timemult="1.5",
                 rates=((1000.0, 4),))
    cfg = _pair(tmp_path, wave, "BINARY")
    rec = Recording.open(cfg, "UTC")
    rows = []
    for b in rec.iter_analog_window(-10**12, 10**12):
        rows.extend(b.to_pylist())
    base = {r["sample_number"]: r["frame_timestamp_us"] for r in rows}
    start = rec.clock.start_epoch_us
    assert [base[s] - start for s in (1, 2, 3, 4)] == [0, 3000, 6000, 9000]


def test_empty_dat_is_rejected(tmp_path):
    wave = _wave((0,), rates=((1000.0, 1),), analog_raw=((0),))
    cfg = _pair(tmp_path, wave, "BINARY", dat_bytes=b"")
    rec = Recording.open(cfg, "UTC")
    with pytest.raises(FrameSequenceError, match="no frames"):
        rec.validate()


def test_frame_count_mismatch_with_cfg_end_sample(tmp_path):
    # CFG says 4 frames, DAT only carries 3.
    wave = _wave((0, 0, 0), rates=((1000.0, 4),))
    cfg = _pair(tmp_path, wave, "BINARY")
    rec = Recording.open(cfg, "UTC")
    with pytest.raises(FrameSequenceError, match="last cumulative end sample"):
        rec.validate()


def test_first_frame_zero_in_timestamp_mode_is_offset_zero(tmp_path):
    # Explicit timestamps but the first point is zero: legal, means offset 0.
    wave = _wave((0, 1000, 2000, 3000))
    cfg = _pair(tmp_path, wave, "ASCII")
    rec = Recording.open(cfg, "UTC")
    summary = rec.validate()
    assert summary["timestamp_mode"] == "dat_timestamp"


def test_bad_ratio_never_produces_infinity(tmp_path):
    # A ratio pair that is finite but extreme must still yield finite values;
    # a zero/negative ratio is rejected at parse time, so craft a huge one and
    # confirm conversion stays finite.
    specs = (("Ia", "A", "A", 1.0, 0.0, 0.0, 1e150, 1.0, "S"),)
    wave = _wave((0, 1000), analog_specs=specs,
                 analog_raw=((1,), (2,)), rates=((1000.0, 2),))
    cfg = _pair(tmp_path, wave, "BINARY")
    rec = Recording.open(cfg, "UTC")
    rows = []
    for b in rec.iter_analog_window(-10**12, 10**12, side="primary"):
        rows.extend(b.to_pylist())
    # 1e150 * raw is still finite; overflow to inf would raise ChannelError.
    assert all(math.isfinite(r["value"]) for r in rows)


def test_non_positive_ratio_rejected_at_parse(tmp_path):
    specs = (("Ia", "A", "A", 1.0, 0.0, 0.0, 0.0, 5.0, "S"),)
    wave = _wave((0,), analog_specs=specs, analog_raw=((1,),),
                 rates=((1000.0, 1),))
    cfg_path = _pair(tmp_path, wave, "BINARY")
    from minenergy.comtrade.config import parse_cfg
    with pytest.raises(ChannelError, match="primary ratio"):
        parse_cfg(cfg_path)


def test_binary_and_ascii_roundtrip_agree_on_custom_wave(tmp_path):
    wave = _wave((0, 1000, 2000), analog_raw=((-5,), (0,), (7,)),
                 rates=((1000.0, 3),))
    cfg_a_dir = tmp_path / "a"
    cfg_a_dir.mkdir()
    cfg_a = _pair(cfg_a_dir, wave, "ASCII")
    cfg_b_dir = tmp_path / "b"
    cfg_b_dir.mkdir()
    cfg_b = _pair(cfg_b_dir, wave, "BINARY")

    def vals(cfg):
        rec = Recording.open(cfg, "UTC")
        out = []
        for b in rec.iter_analog_window(-10**12, 10**12):
            out += [(r["sample_number"], r["raw_value"], r["time_us"])
                    for r in b.to_pylist()]
        return out
    assert vals(cfg_a) == vals(cfg_b)
