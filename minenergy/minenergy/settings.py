"""Meter-level settings: IANA timezone and the configurable gap threshold."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SETTINGS = {
    "defaults": {"timezone": "UTC", "gap_threshold_seconds": 3600},
    "meters": {},
}


@dataclass(frozen=True)
class MeterSettings:
    timezone: str
    gap_threshold_seconds: int

    @property
    def gap_threshold_us(self) -> int:
        return int(self.gap_threshold_seconds) * 1_000_000


def load_settings(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        return json.loads(json.dumps(DEFAULT_SETTINGS))
    data = json.loads(p.read_text(encoding="utf-8"))
    defaults = dict(DEFAULT_SETTINGS["defaults"])
    defaults.update(data.get("defaults") or {})
    meters = {str(k): dict(v) for k, v in (data.get("meters") or {}).items()}
    return {"defaults": defaults, "meters": meters}


def meter_settings(settings: dict, meter_id: str) -> MeterSettings:
    defaults = settings["defaults"]
    override = settings["meters"].get(meter_id, {})
    tz = override.get("timezone", defaults["timezone"])
    gap = override.get("gap_threshold_seconds", defaults["gap_threshold_seconds"])
    if gap <= 0:
        raise ValueError(f"gap_threshold_seconds must be positive for {meter_id}")
    return MeterSettings(timezone=str(tz), gap_threshold_seconds=int(gap))


def settings_fingerprints(settings: dict, meter_ids) -> dict[str, tuple[str, int]]:
    out = {}
    for m in sorted(meter_ids):
        ms = meter_settings(settings, m)
        out[m] = (ms.timezone, ms.gap_threshold_seconds)
    return out


# ISO-8601 timestamps must carry an explicit UTC offset (Z or +hh:mm / +hhmm).
_OFFSET_TAIL = re.compile(r"(?:Z|z|[+-]\d{2}:?\d{2})$")


def has_explicit_offset(value: str | None) -> bool:
    return bool(value) and bool(_OFFSET_TAIL.search(value.strip()))
