"""Bounded-memory guarantee: streaming must not retain the whole DAT.

Two complementary measurements back the guarantee:

1. process peak RSS (``resource.getrusage``, no instrumentation overhead) on
   the ~20 MiB / 1,000,000-frame recording versus the 10-frame fixture, using
   identical batch settings; a reader that swallowed the file would scale its
   RSS with file size, while a batch-bounded reader stays within a small
   constant factor;
2. the single largest Python allocation seen by ``tracemalloc`` during a full
   validation scan.  It must stay in the batch-buffer range regardless of how
   many frames the file holds -- a whole-DAT buffer would show up as one
   allocation proportional to the file size.
"""
from __future__ import annotations

import gc
import resource
import tracemalloc

from minenergy.comtrade.fixtures import build_large_recording, build_reference_pairs
from minenergy.comtrade.recording import Recording


def _peak_rss(cfg_path):
    gc.collect()
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rec = Recording.open(cfg_path, "UTC")
    rec.validate(batch_frames=4096)
    # A narrow window and a full-window pass both stream in bounded batches;
    # the full pass proves batches are released rather than accumulated.
    for batch in rec.iter_analog_window(
        -10**9, 10**9, batch_size=2048, batch_frames=4096,
    ):
        assert batch.num_rows <= 2048
    for batch in rec.iter_digital_window(
        -10**9, 10**9, batch_size=2048, batch_frames=4096,
    ):
        assert batch.num_rows <= 2048
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return max(after - before, 0), after


def test_peak_rss_does_not_scale_with_file_size(reference_root, large_cfg):
    small_cfg = reference_root / "rates_binary" / "rec.cfg"
    _, small_rss = _peak_rss(small_cfg)
    delta_large, large_rss = _peak_rss(large_cfg)
    # The large file holds ~100,000x more frames but its *incremental* RSS over
    # the small-file run must stay in the low tens of MiB, nowhere near the
    # linear growth a whole-file read would produce.
    assert delta_large < 64 * 1024, (  # ru_maxrss is KiB on Linux
        f"large recording added {delta_large} KiB peak RSS; streaming must be "
        "batch-bounded"
    )
    assert large_rss < 8 * max(small_rss, 1), (
        f"peak RSS scaled with file size: small={small_rss} KiB, "
        f"large={large_rss} KiB"
    )


def test_no_single_allocation_holds_the_whole_dat(large_cfg):
    rec = Recording.open(large_cfg, "UTC")
    tracemalloc.start()
    try:
        rec.validate(batch_frames=4096)
        _, peak = tracemalloc.get_traced_memory()
        snapshot = tracemalloc.take_snapshot()
    finally:
        tracemalloc.stop()
    biggest = max(stat.size for stat in snapshot.statistics("filename"))
    # One frame in the large file is 22 bytes; the whole DAT is ~22 MiB.
    # A batch of 4096 frames is well under 1 MiB, so any allocation above
    # 4 MiB means data beyond a single batch is being retained.
    assert biggest < 4 * 1024 * 1024, (
        f"largest allocation {biggest} bytes exceeds a batch buffer; the DAT "
        "may be retained whole"
    )
    assert peak < 16 * 1024 * 1024


def test_large_recording_streams_expected_frame_count(large_cfg):
    rec = Recording.open(large_cfg, "UTC")
    summary = rec.validate(batch_frames=8192)
    assert summary["frames"] == 1_000_000
    assert summary["timestamp_mode"] == "dat_timestamp"
    # A narrow trigger-relative window returns only matching rows.
    total = sum(
        b.num_rows for b in rec.iter_analog_window(0, 10_000, channels=[1])
    )
    # Trigger offset is 4000 us; 1000 Hz -> rel times 0..10000 inclusive give
    # 11 samples for the single-rate large recording.
    assert total == 11
