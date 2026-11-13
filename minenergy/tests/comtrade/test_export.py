"""Parquet export: staging, provenance metadata and no half imports."""
from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from minenergy.comtrade.export import MANIFEST_NAME, export_parquet
from minenergy.comtrade.recording import Recording


def _read_table(path):
    return pq.read_table(path).to_pylist()


def test_export_writes_both_tables_with_provenance(tmp_path, reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    out = tmp_path / "export"
    counts = export_parquet(
        rec, out, start_rel_us=-2000, end_rel_us=4000,
        side="secondary", batch_size=5,
    )
    assert counts["analog_rows"] > 0 and counts["digital_rows"] > 0
    analog = _read_table(out / "analog.parquet")
    digital = _read_table(out / "digital.parquet")
    # Window semantics survive the Parquet round trip.
    assert all(-2000 <= r["relative_trigger_us"] <= 4000 for r in analog)
    assert all(-2000 <= r["relative_trigger_us"] <= 4000 for r in digital)
    manifest = json.loads((out / MANIFEST_NAME).read_text())
    assert manifest["format"] == "IEEE C37.111-1999 COMTRADE"
    assert manifest["timezone"] == "UTC"
    assert manifest["timestamp_mode"] == "rebuilt_from_rates"
    assert manifest["timemult"] == "2"
    assert manifest["value_side"] == "secondary"
    assert manifest["cfg_sha256"] and manifest["dat_sha256"]
    # Provenance is embedded in the Parquet schema metadata too.
    schema_meta = pq.ParquetFile(out / "analog.parquet").schema_arrow.metadata
    decoded = json.loads(schema_meta[b"comtrade.provenance"])
    assert "a*raw + b" in decoded["physical_conversion"]
    assert "P marker => primary" in decoded["physical_conversion"]
    channel_meta = {c["index"]: c for c in decoded["analog_channels"]}
    assert channel_meta[2]["skew_us"] == 500.0
    assert channel_meta[1]["ps_marker"] == "P"
    assert channel_meta[3]["ps_marker"] == "S"


def test_export_validates_before_writing(tmp_path, corrupt_root):
    rec = Recording.open(
        corrupt_root / "sample_skip_binary" / "rec.cfg", "UTC",
    )
    out = tmp_path / "should_not_exist"
    with pytest.raises(Exception):
        export_parquet(rec, out, start_rel_us=-10**9, end_rel_us=10**9)
    # Nothing published, no staging directory left behind.
    assert not out.exists()
    leftovers = [p for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]
    assert leftovers == []


def test_overwrite_guards_and_replaces(tmp_path, reference_root):
    rec = Recording.open(reference_root / "stamped_ascii" / "rec.cfg", "UTC")
    out = tmp_path / "export"
    export_parquet(rec, out, start_rel_us=0, end_rel_us=0)
    with pytest.raises(FileExistsError):
        export_parquet(rec, out, start_rel_us=0, end_rel_us=0)
    export_parquet(rec, out, start_rel_us=0, end_rel_us=0, overwrite=True)
    assert (out / MANIFEST_NAME).exists()


def test_exported_primary_values_match_streaming(tmp_path, reference_root):
    rec = Recording.open(reference_root / "rates_ascii" / "rec.cfg", "UTC")
    out = tmp_path / "primary"
    export_parquet(rec, out, start_rel_us=-10**12, end_rel_us=10**12,
                   side="primary", analog_channels=["Ia"])
    rows = _read_table(out / "analog.parquet")
    # A1 marker P with ratio 200: primary == restored a*x+b.
    first = min(rows, key=lambda r: (r["sample_number"], r["channel_index"]))
    assert first["value"] == pytest.approx(0.1 * -100 + 0.5)
    assert all(r["value_side"] == "primary" for r in rows)


def test_channel_projection_is_reflected_in_manifest(tmp_path, reference_root):
    rec = Recording.open(reference_root / "rates_binary" / "rec.cfg", "UTC")
    out = tmp_path / "projected"
    export_parquet(rec, out, start_rel_us=-10**12, end_rel_us=10**12,
                   analog_channels=[1], digital_channels=["DIG17"])
    manifest = json.loads((out / MANIFEST_NAME).read_text())
    assert [c["index"] for c in manifest["analog_channels"]] == [1]
    assert [c["index"] for c in manifest["digital_channels"]] == [17]
    analog = _read_table(out / "analog.parquet")
    digital = _read_table(out / "digital.parquet")
    assert {r["channel_index"] for r in analog} == {1}
    assert {r["channel_index"] for r in digital} == {17}
