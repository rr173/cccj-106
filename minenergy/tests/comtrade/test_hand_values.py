"""Hand-derived value checks for the reference waveform.

Every expected number below is derived by hand from the fixture definition in
``minenergy/comtrade/fixtures.py``.  The same waveform exists in four encodings
(all-zero vs explicit timestamps x ASCII vs BINARY); decoded values must be
identical across all four.
"""
from __future__ import annotations

import pytest

from minenergy.comtrade.fixtures import (
    RATE_OFFSETS_US,
    STAMPED_RAW_TS,
)
from minenergy.comtrade.recording import Recording
from minenergy.comtrade.config import parse_cfg

START_EPOCH_US = 1_742_032_800 * 1_000_000
TRIGGER_EPOCH_US = START_EPOCH_US + 4_000


def _read_all(recording, kind, **kwargs):
    method = (recording.iter_analog_window if kind == "analog"
              else recording.iter_digital_window)
    rows = []
    for batch in method(-10**12, 10**12, **kwargs):
        rows.extend(batch.to_pylist())
    return rows


def test_fixture_timestamp_vectors_are_hand_derived():
    # Guard against accidental drift of the fixture constants themselves.
    assert RATE_OFFSETS_US == [0, 1000, 2000, 3000, 4000, 5000,
                               6000, 8000, 10000, 12000]
    assert STAMPED_RAW_TS == [0, 500, 1000, 1500, 2000, 2500,
                              3000, 4000, 5000, 6000]


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_modes_and_frame_counts(reference_root, name):
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    summary = rec.validate()
    assert summary["frames"] == 10
    assert summary["n_analog"] == 3
    assert summary["n_digital"] == 17
    assert summary["n_digital_groups"] == 2
    expected_mode = ("rebuilt_from_rates" if name.startswith("rates")
                     else "dat_timestamp")
    assert summary["timestamp_mode"] == expected_mode
    assert rec.timestamp_mode == expected_mode
    assert rec.clock.start_epoch_us == START_EPOCH_US
    assert rec.clock.trigger_epoch_us == TRIGGER_EPOCH_US
    assert rec.clock.trigger_offset_us == 4000


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_segment_boundaries_are_cumulative_inclusive(reference_root, name):
    cfg = parse_cfg(reference_root / name / "rec.cfg")
    rec = Recording(cfg, "UTC")
    segs = rec.clock.segments
    assert len(segs) == 2
    # First segment: samples 1..6 at 1000 Hz, first point offset zero.
    assert segs[0].prev_end_sample == 0
    assert segs[0].end_sample == 6
    assert segs[0].start_offset_us == 0
    # Second segment starts at 6 samples / 1000 Hz = 6000 us later.
    assert segs[1].prev_end_sample == 6
    assert segs[1].end_sample == 10
    assert segs[1].start_offset_us == 6000
    # Hand offsets for every sample.
    for sample, offset in enumerate(RATE_OFFSETS_US, start=1):
        assert rec.clock.offset_for_sample(sample) == offset


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_analog_base_times_identical_across_encodings(reference_root, name):
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    rows = _read_all(rec, "analog", channels=[1], side="secondary")
    # Channel 1 has zero skew, so frame_timestamp_us == time_us.
    base = {r["sample_number"]: r["frame_timestamp_us"] for r in rows}
    for sample, offset in enumerate(RATE_OFFSETS_US, start=1):
        assert base[sample] == START_EPOCH_US + offset
    assert all(r["time_us"] == r["frame_timestamp_us"] for r in rows)


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_analog_physical_values_marked_side(reference_root, name):
    """a*x+b restores engineering units on the channel's declared P/S side."""
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    # Requesting the marked side means values equal a*x+b for every channel.
    rows_pri = _read_all(rec, "analog", side="primary")
    rows_sec = _read_all(rec, "analog", side="secondary")
    pri = {(r["sample_number"], r["channel_index"]): r for r in rows_pri}
    sec = {(r["sample_number"], r["channel_index"]): r for r in rows_sec}
    raw = {
        1: (-100, 10, 1000),
        2: (-50, -20, -2000),
        5: (100, -40, -10000),
        9: (32767, 80, 30000),
        10: (-32768, -90, -30000),
    }
    coeff = {1: (0.1, 0.5), 2: (2.0, -1.0), 3: (0.001, 0.0)}
    for sample, raws in raw.items():
        for ch_idx, x in enumerate(raws, start=1):
            a, b = coeff[ch_idx]
            restored = a * x + b
            if ch_idx == 1:  # marker P: primary is the marked side
                assert pri[(sample, ch_idx)]["value"] == pytest.approx(restored)
            else:            # marker S: secondary is the marked side
                assert sec[(sample, ch_idx)]["value"] == pytest.approx(restored)
    assert pri[(9, 1)]["raw_value"] == 32767
    assert sec[(10, 1)]["raw_value"] == -32768


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_primary_secondary_direction_follows_ps_marker(reference_root, name):
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    rows_sec = _read_all(rec, "analog", side="secondary")
    rows_pri = _read_all(rec, "analog", side="primary")
    sec = {(r["sample_number"], r["channel_index"]): r["value"] for r in rows_sec}
    pri = {(r["sample_number"], r["channel_index"]): r["value"] for r in rows_pri}
    for sample in range(1, 11):
        # A1 marker P, ratio 1000/5 = 200: primary is marked (a*x+b),
        # secondary = primary / 200, i.e. primary = secondary * 200.
        assert pri[(sample, 1)] == pytest.approx(sec[(sample, 1)] * 200)
        # A2 marker S, ratio 6600/110 = 60: primary = secondary * 60.
        assert pri[(sample, 2)] == pytest.approx(sec[(sample, 2)] * 60)
        # A3 marker S, ratio 300/5 = 60.
        assert pri[(sample, 3)] == pytest.approx(sec[(sample, 3)] * 60)
    # Hand-check one absolute value each side.
    # A1 sample 1: raw -100 -> primary -9.5; secondary -9.5/200 = -0.0475.
    assert pri[(1, 1)] == pytest.approx(-9.5)
    assert sec[(1, 1)] == pytest.approx(-0.0475)
    # A2 sample 1: raw 10 -> secondary 19; primary 1140.
    assert sec[(1, 2)] == pytest.approx(19.0)
    assert pri[(1, 2)] == pytest.approx(1140.0)
    # No infinities anywhere even at int16 extremes.
    import math
    assert all(math.isfinite(v) for v in list(pri.values()) + list(sec.values()))


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_per_channel_skew_is_applied_to_own_instant(reference_root, name):
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    rows = _read_all(rec, "analog", side="secondary")
    by = {(r["sample_number"], r["channel_index"]): r for r in rows}
    for sample, offset in enumerate(RATE_OFFSETS_US, start=1):
        # A1 skew 0, A2 +500, A3 -250.
        assert by[(sample, 1)]["time_us"] == START_EPOCH_US + offset
        assert by[(sample, 2)]["time_us"] == START_EPOCH_US + offset + 500
        assert by[(sample, 3)]["time_us"] == START_EPOCH_US + offset - 250
        # relative_trigger_us follows the skewed instant.
        assert by[(sample, 2)]["relative_trigger_us"] == offset + 500 - 4000


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_digital_bit_order_and_normal_state(reference_root, name):
    rec = Recording.open(reference_root / name / "rec.cfg", "UTC")
    rows = _read_all(rec, "digital")
    by = {(r["sample_number"], r["channel_index"]): r for r in rows}
    # Frame 2: word0 = 0x0002 -> D2 = 1 only within D1..D16; word1 = 0 -> D17 = 0.
    assert by[(2, 2)]["state"] == 1
    assert by[(2, 1)]["state"] == 0
    assert by[(2, 16)]["state"] == 0
    assert by[(2, 17)]["state"] == 0
    # Frame 3: word0 = 0x8000 -> D16 (bit 15) = 1.
    assert by[(3, 16)]["state"] == 1
    assert by[(3, 15)]["state"] == 0
    # Frame 4: word0 = 0xAAAA (bits 1,3,..) and word1 = 1 -> D17 = 1.
    assert by[(4, 17)]["state"] == 1
    assert by[(4, 1)]["state"] == 0  # bit 0 of 0xAAAA is 0
    assert by[(4, 2)]["state"] == 1  # bit 1 is 1
    assert by[(4, 16)]["state"] == 1  # bit 15 of 0xAAAA is 1
    # Frame 9: 0xFFFE -> D1 = 0, D2..D16 = 1; D17 word = 0.
    assert by[(9, 1)]["state"] == 0
    assert by[(9, 16)]["state"] == 1
    assert by[(9, 17)]["state"] == 0
    # Normal-state deviation: D2 is normally 1, D1 and D17 are normally 0.
    assert by[(2, 2)]["deviates_from_normal"] is False   # state 1 == normal 1
    assert by[(3, 16)]["deviates_from_normal"] is True   # state 1 vs normal 0
    assert by[(3, 17)]["deviates_from_normal"] is True   # state 0 vs normal 1
    assert by[(4, 17)]["deviates_from_normal"] is False  # state 1 == normal 1
    assert by[(2, 2)]["normal_state"] == 1
    assert by[(2, 1)]["normal_state"] == 0


@pytest.mark.parametrize("name", [
    "rates_ascii", "rates_binary", "stamped_ascii", "stamped_binary",
])
def test_encodings_produce_identical_rows(reference_root, name, all_reference_pairs):
    def materialise(cfg):
        rec = Recording.open(cfg, "UTC")
        analog = sorted(
            (r["sample_number"], r["channel_index"], r["time_us"], r["value"])
            for r in _read_all(rec, "analog", side="secondary")
        )
        rec2 = Recording.open(cfg, "UTC")
        digital = sorted(
            (r["sample_number"], r["channel_index"], r["state"])
            for r in _read_all(rec2, "digital")
        )
        return analog, digital

    baseline_a, baseline_d = materialise(all_reference_pairs["rates_binary"])
    got_a, got_d = materialise(all_reference_pairs[name])
    assert got_a == baseline_a
    assert got_d == baseline_d
