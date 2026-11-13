"""Trigger-relative windows, channel projection and Arrow batch boundaries."""
from __future__ import annotations

import pytest

from minenergy.comtrade.errors import ChannelError, TimeError
from minenergy.comtrade.recording import ANALOG_SCHEMA, DIGITAL_SCHEMA, Recording


def _rows(recording, kind, **kwargs):
    method = (recording.iter_analog_window if kind == "analog"
              else recording.iter_digital_window)
    out = []
    for batch in method(**kwargs):
        out.extend(batch.to_pylist())
    return out


def test_narrow_window_around_trigger_inclusive_edges(reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    # Base offsets around trigger (4000): samples 4,5,6 at 3000,4000,5000.
    rows = _rows(rec, "analog", start_rel_us=-1000, end_rel_us=1000,
                 channels=[1])
    samples = sorted({r["sample_number"] for r in rows})
    assert samples == [4, 5, 6]
    # Both edges are inclusive: -1000 (sample 4) and +1000 (sample 6) included.
    rels = sorted({r["relative_trigger_us"] for r in rows})
    assert rels == [-1000, 0, 1000]


def test_skew_moves_a_channel_across_the_window_edge(reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    # Window ending exactly at the un-skewed sample-3 instant, rel = -2000.
    rows_a1 = _rows(rec, "analog", start_rel_us=-10**9, end_rel_us=-2000,
                    channels=[1])
    assert {r["sample_number"] for r in rows_a1} == {1, 2, 3}
    # A2 carries +500 us skew, so sample 3's skewed rel is -1500 > -2000:
    # it must fall out of the same window, proving filtering uses skewed time.
    rows_a2 = _rows(rec, "analog", start_rel_us=-10**9, end_rel_us=-2000,
                    channels=[2])
    assert {r["sample_number"] for r in rows_a2} == {1, 2}
    # A3 carries -250 us skew: sample 4 (base rel -1000) skews to -1250,
    # still inside a window ending at -1250 inclusive.
    rows_a3 = _rows(rec, "analog", start_rel_us=-10**9, end_rel_us=-1250,
                    channels=[3])
    assert {r["sample_number"] for r in rows_a3} == {1, 2, 3, 4}


def test_digital_window_uses_base_frame_time(reference_root):
    rec = Recording.open(reference_root / "stamped_binary" / "rec.cfg", "UTC")
    rows = _rows(rec, "digital", start_rel_us=0, end_rel_us=4000)
    # Base rel 0 (sample 5), 1000 (6), 2000 (7) and 4000 (sample 8, second
    # segment at 500 Hz); the +4000 edge is inclusive.
    assert sorted({r["sample_number"] for r in rows}) == [5, 6, 7, 8]


def test_channel_projection_by_index_and_name(reference_root):
    rec = Recording.open(reference_root / "rates_ascii" / "rec.cfg", "UTC")
    rows = _rows(rec, "analog", start_rel_us=-10**12, end_rel_us=10**12,
                 channels=["Vb", 3])
    assert sorted({r["channel_index"] for r in rows}) == [2, 3]
    assert sorted({r["channel_name"] for r in rows}) == ["Ic", "Vb"]
    digital = _rows(rec, "digital", start_rel_us=-10**12, end_rel_us=10**12,
                    channels=[17, "DIG1"])
    assert sorted({r["channel_index"] for r in digital}) == [1, 17]


def test_unknown_channel_is_rejected(reference_root):
    rec = Recording.open(reference_root / "rates_ascii" / "rec.cfg", "UTC")
    with pytest.raises(ChannelError):
        list(rec.iter_analog_window(-1, 1, channels=["Nope"]))
    with pytest.raises(ChannelError):
        list(rec.iter_digital_window(-1, 1, channels=[99]))


def test_inverted_window_is_rejected(reference_root):
    rec = Recording.open(reference_root / "rates_ascii" / "rec.cfg", "UTC")
    with pytest.raises(TimeError):
        list(rec.iter_analog_window(100, 0))


def test_arrow_batches_respect_batch_size_and_schema(reference_root):
    rec = Recording.open(reference_root / "stamped_ascii" / "rec.cfg", "UTC")
    batches = list(rec.iter_analog_window(
        -10**12, 10**12, batch_size=7,
    ))
    # 10 frames x 3 analog channels = 30 rows; 7 per batch -> 5 batches
    # (four full, one of two).
    assert [b.num_rows for b in batches] == [7, 7, 7, 7, 2]
    for b in batches:
        assert b.schema.equals(ANALOG_SCHEMA)
    digital_batches = list(rec.iter_digital_window(
        -10**12, 10**12, batch_size=25,
    ))
    # 10 x 17 = 170 rows -> six full batches of 25 plus a tail of 20.
    assert [b.num_rows for b in digital_batches] == [25, 25, 25, 25, 25, 25, 20]
    for b in digital_batches:
        assert b.schema.equals(DIGITAL_SCHEMA)


def test_empty_window_keeps_schema_and_emits_nothing(reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    batches = list(rec.iter_analog_window(10**9, 10**10))
    assert batches == []


def test_catalog_lists_all_channels(reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    catalog = rec.catalog()
    assert [(c.kind, c.index) for c in catalog[:3]] == [
        ("analog", 1), ("analog", 2), ("analog", 3),
    ]
    assert (catalog[-1].kind, catalog[-1].index) == ("digital", 17)
