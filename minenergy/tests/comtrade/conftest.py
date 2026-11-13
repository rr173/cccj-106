"""Shared fixtures: materialise the offline COMTRADE waveforms once per session."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from minenergy.comtrade.fixtures import (
    build_corrupt_pairs,
    build_large_recording,
    build_reference_pairs,
)


@pytest.fixture(scope="session")
def fixture_root() -> Path:
    root = Path(tempfile.mkdtemp(prefix="comtrade-fixtures-"))
    build_reference_pairs(root / "reference")
    build_corrupt_pairs(root / "corrupt")
    build_large_recording(root / "large")
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def reference_root(fixture_root) -> Path:
    return fixture_root / "reference"


@pytest.fixture(scope="session")
def corrupt_root(fixture_root) -> Path:
    return fixture_root / "corrupt"


@pytest.fixture(scope="session")
def large_cfg(fixture_root) -> Path:
    return fixture_root / "large" / "large.cfg"


@pytest.fixture(scope="session")
def all_reference_pairs(reference_root) -> dict[str, Path]:
    return {name: reference_root / name / "rec.cfg"
            for name in ("rates_ascii", "rates_binary",
                         "stamped_ascii", "stamped_binary")}
