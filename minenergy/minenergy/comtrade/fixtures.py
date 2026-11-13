"""Self-contained COMTRADE fixtures, generated without any external download.

The small reference waveform is deliberately tiny (10 frames, 3 analog and 17
digital channels) so every interesting value can be derived by hand:

* two sample-rate segments: 1000 Hz for samples 1..6, then 500 Hz for
  samples 7..10;
* distinct per-channel skews (0 / +500 / -250 microseconds);
* analog raw values cover positive, negative and both int16 extremes;
* 17 digital channels force two packed uint16 groups, including the group
  boundary (D16 = bit 15 of word 0, D17 = bit 0 of word 1);
* the same waveform is materialised four ways: all-zero DAT timestamps
  (CFG rate rebuild) vs explicit timestamps with ``timemult=2``, each in ASCII
  and BINARY;
* corrupt variants cover truncation, sample-number skips, wrong channel
  counts, mixed zero timestamps, clock regression and DST ambiguity.

A separate large fixture (~20 MiB BINARY) backs the bounded-memory check.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

# Hand-designed reference waveform -------------------------------------------
N_FRAMES = 10
ANALOG_SPECS = [
    # name, phase, unit, a, b, skew_us, primary, secondary, PS
    ("Ia", "A", "A", 0.1, 0.5, 0.0, 1000.0, 5.0, "P"),
    ("Vb", "B", "V", 2.0, -1.0, 500.0, 6600.0, 110.0, "S"),
    ("Ic", "C", "A", 0.001, 0.0, -250.0, 300.0, 5.0, "S"),
]
ANALOG_RAW = [
    # A1 includes int16 extremes; all three channels span +/- signs.
    (-100, 10, 1000),
    (-50, -20, -2000),
    (0, 0, 0),
    (50, 30, 5000),
    (100, -40, -10000),
    (200, 50, 15000),
    (300, 60, -20000),
    (-300, -70, 25000),
    (32767, 80, 30000),
    (-32768, -90, -30000),
]
# 17 digital channels: normal states alternate, two are normally-closed (1).
DIGITAL_NORMAL = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
# Packed words per frame (word0 = D1..D16 LSB-first, word1 carries D17).
DIGITAL_WORDS = [
    (0x0000, 0),
    (0x0002, 0),    # D2 = 1 (its normal state); D1 stays 0
    (0x8000, 0),    # D16 = 1
    (0xAAAA, 1),    # D17 = 1 plus alternating D1..D16
    (0x5555, 0),
    (0xFFFF, 1),
    (0x0000, 0),
    (0x0003, 1),    # D1,D2 = 1 and D17 = 1
    (0xFFFE, 0),    # D1 = 0, D2..D16 = 1
    (0x0000, 1),
]
# Rate segments: 1000 Hz through sample 6, 500 Hz through sample 10.
RATES = [(1000.0, 6), (500.0, 10)]
# Hand-derived frame offsets in microseconds.
RATE_OFFSETS_US = [0, 1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000, 12000]
# Explicit-timestamp variant: timemult = 2, so raw stamps are offset / 2.
TIMEMULT = "2"
STAMPED_RAW_TS = [off // 2 for off in RATE_OFFSETS_US]

START_WALL = (2025, 3, 15, 10, 0, 0, 0)        # unambiguous in every zone
TRIGGER_OFFSET_US = 4000                       # trigger lands on sample 5
TRIGGER_WALL = (2025, 3, 15, 10, 0, 0, 4000)
FIXTURE_ZONE = "UTC"


@dataclass(frozen=True)
class Waveform:
    analog_specs: tuple
    analog_raw: tuple
    digital_normal: tuple
    digital_words: tuple
    rates: tuple
    timestamps: tuple  # raw DAT timestamps, one per frame
    start_wall: tuple
    trigger_wall: tuple
    timemult: str
    station: str = "SUBST1"
    recorder: str = "RELAY-A"

    @property
    def n_analog(self) -> int:
        return len(self.analog_specs)

    @property
    def n_digital(self) -> int:
        return len(self.digital_normal)


def reference_waveform(timestamps: tuple[int, ...]) -> Waveform:
    return Waveform(
        analog_specs=tuple(ANALOG_SPECS),
        analog_raw=tuple(ANALOG_RAW),
        digital_normal=tuple(DIGITAL_NORMAL),
        digital_words=tuple(DIGITAL_WORDS),
        rates=tuple(RATES),
        timestamps=tuple(timestamps),
        start_wall=START_WALL,
        trigger_wall=TRIGGER_WALL,
        timemult=TIMEMULT,
    )


def _wall_fields(wall: tuple) -> str:
    year, month, day, hour, minute, second, micro = wall
    date_part = f"{day:02d}/{month:02d}/{year}"
    time_part = f"{hour:02d}:{minute:02d}:{second:02d}.{micro:06d}"
    return f"{date_part},{time_part}"


def render_cfg(wave: Waveform, file_type: str) -> str:
    na, nd = wave.n_analog, wave.n_digital
    lines = [f"{wave.station},{wave.recorder},1999", f"{na + nd},{na}A,{nd}D"]
    for i, (name, phase, unit, a, b, skew, prim, sec, marker) in enumerate(
        wave.analog_specs, start=1,
    ):
        vmin = -32768
        vmax = 32767
        lines.append(
            f"A{i},{name},{phase},cbm{phase},{unit},{a},{b},{skew},"
            f"{vmin},{vmax},{prim},{sec},{marker}"
        )
    for i, normal in enumerate(wave.digital_normal, start=1):
        lines.append(f"D{i},DIG{i},,,{normal}")
    lines.append(str(len(wave.rates)))
    for rate, end_sample in wave.rates:
        rate_text = str(int(rate)) if float(rate).is_integer() else str(rate)
        lines.append(f"{rate_text},{end_sample}")
    lines.append(_wall_fields(wave.start_wall))
    lines.append(_wall_fields(wave.trigger_wall))
    lines.append(file_type)
    lines.append(wave.timemult)
    return "\n".join(lines) + "\n"


def render_ascii_dat(wave: Waveform, sample_numbers=None,
                     timestamps=None) -> bytes:
    nums = sample_numbers or list(range(1, len(wave.analog_raw) + 1))
    stamps = timestamps if timestamps is not None else list(wave.timestamps)
    out = []
    for i, (sample, ts) in enumerate(zip(nums, stamps)):
        fields = [str(sample), str(ts)]
        fields += [str(v) for v in wave.analog_raw[i]]
        fields += [str(w) for w in wave.digital_words[i]]
        out.append(",".join(fields))
    return ("\n".join(out) + "\n").encode("ascii")


def render_binary_dat(wave: Waveform, sample_numbers=None,
                      timestamps=None) -> bytes:
    na = wave.n_analog
    ng = (wave.n_digital + 15) // 16
    st = struct.Struct("<II" + "h" * na + "H" * ng)
    nums = sample_numbers or list(range(1, len(wave.analog_raw) + 1))
    stamps = timestamps if timestamps is not None else list(wave.timestamps)
    parts = []
    for i, (sample, ts) in enumerate(zip(nums, stamps)):
        parts.append(st.pack(sample, ts, *wave.analog_raw[i],
                             *wave.digital_words[i]))
    return b"".join(parts)


def _write_pair(directory: Path, stem: str, wave: Waveform, file_type: str,
                cfg_text: str | None = None, dat_bytes: bytes | None = None,
                cfg_suffix: str = ".cfg", dat_suffix: str = ".dat") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    cfg_path = directory / f"{stem}{cfg_suffix}"
    dat_path = directory / f"{stem}{dat_suffix}"
    cfg_path.write_text(cfg_text or render_cfg(wave, file_type), encoding="utf-8")
    dat_path.write_bytes(
        dat_bytes
        if dat_bytes is not None
        else (render_binary_dat(wave) if file_type == "BINARY"
              else render_ascii_dat(wave))
    )
    return cfg_path


def build_reference_pairs(base: Path) -> dict[str, Path]:
    """Materialise the reference waveform in all four encodings."""
    made: dict[str, Path] = {}
    zero_ts = tuple(0 for _ in RATE_OFFSETS_US)
    stamped_ts = tuple(STAMPED_RAW_TS)
    layouts = [
        ("rates_ascii", reference_waveform(zero_ts), "ASCII"),
        ("rates_binary", reference_waveform(zero_ts), "BINARY"),
        ("stamped_ascii", reference_waveform(stamped_ts), "ASCII"),
        ("stamped_binary", reference_waveform(stamped_ts), "BINARY"),
    ]
    for name, wave, file_type in layouts:
        directory = base / name
        made[name] = _write_pair(directory, "rec", wave, file_type)
    return made


def build_corrupt_pairs(base: Path) -> dict[str, Path]:
    """Each corrupt pair lives alone in its own directory."""
    zero_ts = tuple(0 for _ in RATE_OFFSETS_US)
    stamped_ts = tuple(STAMPED_RAW_TS)
    made: dict[str, Path] = {}
    n = N_FRAMES

    # 1. truncated BINARY: drop the last three bytes of the final frame.
    wave = reference_waveform(zero_ts)
    good = render_binary_dat(wave)
    d = base / "truncated_binary"
    made["truncated_binary"] = _write_pair(
        d, "rec", wave, "BINARY", dat_bytes=good[:-3],
    )

    # 1b. truncated ASCII: remove fields from the final frame line.
    good_ascii = render_ascii_dat(wave)
    ascii_lines = good_ascii.rstrip(b"\n").split(b"\n")
    last_line = b",".join(ascii_lines[-1].split(b",")[:3])
    truncated_ascii = b"\n".join(ascii_lines[:-1] + [last_line]) + b"\n"
    d = base / "truncated_ascii"
    made["truncated_ascii"] = _write_pair(
        d, "rec", wave, "ASCII", dat_bytes=truncated_ascii,
    )

    # 2. sample-number skip: frame index 4 (sample 5) jumps to 6, both encodings.
    nums = list(range(1, n + 1))
    nums[4] = 6
    d = base / "sample_skip_binary"
    made["sample_skip_binary"] = _write_pair(
        d, "rec", wave, "BINARY",
        dat_bytes=render_binary_dat(wave, sample_numbers=nums),
    )
    d = base / "sample_skip_ascii"
    made["sample_skip_ascii"] = _write_pair(
        d, "rec", wave, "ASCII",
        dat_bytes=render_ascii_dat(wave, sample_numbers=nums),
    )

    # 3. wrong channel count: CFG declares 2 analog channels while frames hold 3.
    cfg_lines = render_cfg(wave, "BINARY").splitlines()
    # Line 1 (index 0) is the station header; line 2 is the channel count.
    cfg_lines[1] = f"{2 + wave.n_digital},2A,{wave.n_digital}D"
    # Drop one analog channel definition (A3) so the declaration is internally
    # consistent but the DAT still carries three analog readings per frame.
    wrong_cfg = "\n".join(line for line in cfg_lines if not line.startswith("A3,")) + "\n"
    d = base / "wrong_channel_count_binary"
    made["wrong_channel_count_binary"] = _write_pair(
        d, "rec", wave, "BINARY", cfg_text=wrong_cfg,
    )
    d = base / "wrong_channel_count_ascii"
    # The ASCII CFG must itself declare ASCII so the text decoder is selected;
    # its DAT lines still carry three analog fields, i.e. one too many.
    wrong_cfg_ascii = wrong_cfg.replace("\nBINARY\n", "\nASCII\n")
    made["wrong_channel_count_ascii"] = _write_pair(
        d, "rec", wave, "ASCII", cfg_text=wrong_cfg_ascii,
        dat_bytes=render_ascii_dat(wave),
    )

    # 4. mixed zero timestamps: sample 4 is zero while others are explicit.
    wave_s = reference_waveform(stamped_ts)
    mixed = list(stamped_ts)
    mixed[3] = 0
    d = base / "mixed_zero_timestamps_binary"
    made["mixed_zero_timestamps_binary"] = _write_pair(
        d, "rec", wave_s, "BINARY",
        dat_bytes=render_binary_dat(wave_s, timestamps=mixed),
    )
    d = base / "mixed_zero_timestamps_ascii"
    made["mixed_zero_timestamps_ascii"] = _write_pair(
        d, "rec", wave_s, "ASCII",
        dat_bytes=render_ascii_dat(wave_s, timestamps=mixed),
    )

    # 5. clock regression: explicit timestamps decrease at frame index 6.
    regressive = list(stamped_ts)
    regressive[6] = regressive[5] - 100
    d = base / "time_regression_binary"
    made["time_regression_binary"] = _write_pair(
        d, "rec", wave_s, "BINARY",
        dat_bytes=render_binary_dat(wave_s, timestamps=regressive),
    )

    # 6a. DST fall-back ambiguity in America/New_York (2025-11-02 01:30 local).
    fall_wave = Waveform(
        analog_specs=wave.analog_specs, analog_raw=wave.analog_raw,
        digital_normal=wave.digital_normal, digital_words=wave.digital_words,
        rates=wave.rates, timestamps=zero_ts,
        start_wall=(2025, 11, 2, 1, 30, 0, 0),
        trigger_wall=(2025, 11, 2, 1, 30, 0, 4000),
        timemult=TIMEMULT,
    )
    d = base / "dst_ambiguous_fallback"
    made["dst_ambiguous_fallback"] = _write_pair(
        d, "rec", fall_wave, "BINARY",
    )

    # 6b. DST spring-forward: 02:30 local does not exist on 2025-03-09 in NY.
    spring_wave = Waveform(
        analog_specs=wave.analog_specs, analog_raw=wave.analog_raw,
        digital_normal=wave.digital_normal, digital_words=wave.digital_words,
        rates=wave.rates, timestamps=zero_ts,
        start_wall=(2025, 3, 9, 2, 30, 0, 0),
        trigger_wall=(2025, 3, 9, 2, 30, 0, 4000),
        timemult=TIMEMULT,
    )
    d = base / "dst_nonexistent_springforward"
    made["dst_nonexistent_springforward"] = _write_pair(
        d, "rec", spring_wave, "BINARY",
    )
    return made


def build_large_recording(base: Path, n_frames: int = 1_000_000) -> Path:
    """~20 MiB BINARY recording: 4 analog + 17 digital channels, 1000 Hz."""
    analog_specs = (
        ("Ia", "A", "A", 0.05, 0.0, 0.0, 1000.0, 5.0, "S"),
        ("Ib", "B", "A", 0.05, 0.0, 100.0, 1000.0, 5.0, "S"),
        ("Ic", "C", "A", 0.05, 0.0, -100.0, 1000.0, 5.0, "S"),
        ("In", "N", "A", 0.05, 0.0, 0.0, 1000.0, 5.0, "P"),
    )
    digital_normal = tuple(DIGITAL_NORMAL)
    rates = ((1000.0, n_frames),)
    wave = Waveform(
        analog_specs=analog_specs,
        analog_raw=tuple(),  # generated frame-by-frame below
        digital_normal=digital_normal,
        digital_words=tuple(),
        rates=rates,
        timestamps=tuple(),
        start_wall=START_WALL,
        trigger_wall=TRIGGER_WALL,
        timemult="1",
    )
    directory = base
    directory.mkdir(parents=True, exist_ok=True)
    cfg_path = directory / "large.cfg"
    dat_path = directory / "large.dat"
    cfg_path.write_text(render_large_cfg(wave, n_frames), encoding="utf-8")
    write_large_dat(dat_path, n_frames, len(analog_specs),
                    (len(digital_normal) + 15) // 16)
    return cfg_path


def render_large_cfg(wave: Waveform, n_frames: int) -> str:
    na, nd = wave.n_analog, wave.n_digital
    lines = [f"{wave.station},{wave.recorder},1999", f"{na + nd},{na}A,{nd}D"]
    for i, (name, phase, unit, a, b, skew, prim, sec, marker) in enumerate(
        wave.analog_specs, start=1,
    ):
        lines.append(
            f"A{i},{name},{phase},cbm{phase},{unit},{a},{b},{skew},"
            f"-32768,32767,{prim},{sec},{marker}"
        )
    for i, normal in enumerate(wave.digital_normal, start=1):
        lines.append(f"D{i},DIG{i},,,{normal}")
    lines.append("1")
    lines.append(f"1000,{n_frames}")
    lines.append(_wall_fields(wave.start_wall))
    lines.append(_wall_fields(wave.trigger_wall))
    lines.append("BINARY")
    lines.append(wave.timemult)
    return "\n".join(lines) + "\n"


def write_large_dat(path: Path, n_frames: int, n_analog: int,
                    n_groups: int) -> None:
    st = struct.Struct("<II" + "h" * n_analog + "H" * n_groups)
    chunk_frames = 16_384
    with open(path, "wb") as fh:
        for start in range(1, n_frames + 1, chunk_frames):
            buf = bytearray()
            for sample in range(start, min(start + chunk_frames, n_frames + 1)):
                analogs = tuple(
                    ((sample * 7 + k * 13) % 2001) - 1000
                    for k in range(n_analog)
                )
                words = tuple(
                    (sample >> k) & 0xFFFF for k in range(n_groups)
                )
                buf += st.pack(sample, (sample - 1) * 1000, *analogs, *words)
            fh.write(buf)


def build_all(base: str | Path) -> dict[str, Path]:
    base = Path(base)
    made = build_reference_pairs(base / "reference")
    made.update(build_corrupt_pairs(base / "corrupt"))
    made["large"] = build_large_recording(base / "large")
    return made
