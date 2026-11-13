"""Command-line entry points for the COMTRADE ingestion module.

``minenergy-comtrade catalog``   show the channel directory of a CFG/DAT pair
``minenergy-comtrade inspect``   full bounded-memory import check
``minenergy-comtrade export``    stream a trigger-relative window to Parquet
``minenergy-comtrade make-fixtures`` regenerate the offline fixture set

Every rejected file is reported with the file path and the offending
line/frame/sample position, and the process exits with status 2.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

from .errors import ComtradeError
from .export import export_parquet
from .fixtures import build_all
from .recording import Recording


def _open_recording(args) -> Recording:
    return Recording.open(args.cfg, args.timezone, dat_path=args.dat)


def _print_catalog(recording: Recording) -> None:
    cfg = recording.config
    print(f"station={cfg.station_name} recorder={cfg.recorder_id} "
          f"revision={cfg.revision} encoding={cfg.file_type} "
          f"timemult={recording.clock.timemult}")
    print(f"start={cfg.start_wall.isoformat()} "
          f"trigger={cfg.trigger_wall.isoformat()} zone={recording.timezone}")
    print(f"analog channels ({cfg.n_analog}):")
    for c in cfg.analogs:
        print(
            f"  A{c.index:<2} {c.name:<6} phase={c.phase or '-':<2} "
            f"unit={c.unit or '-':<4} a={c.a:<8} b={c.b:<8} "
            f"skew_us={c.skew_us:<8} ratio={c.primary_ratio}/{c.secondary_ratio} "
            f"marker={c.ps_marker}",
        )
    print(f"digital channels ({cfg.n_digital}, "
          f"{cfg.n_digital_groups} packed word group(s)):")
    for c in cfg.digitals:
        print(f"  D{c.index:<2} {c.name:<6} phase={c.phase or '-':<2} "
              f"normal_state={c.normal_state}")
    print("sample-rate segments:")
    for seg in cfg.rates:
        print(f"  rate={seg.rate_hz} Hz, cumulative end sample={seg.end_sample}")


def cmd_catalog(args) -> int:
    recording = _open_recording(args)
    _print_catalog(recording)
    return 0


def cmd_inspect(args) -> int:
    recording = _open_recording(args)
    summary = recording.validate(batch_frames=args.batch_frames)
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_catalog(recording)
        print("import check: OK")
        print(json.dumps(
            {k: v for k, v in summary.items() if k not in ("analog_channels",)},
            indent=2, sort_keys=True, default=str,
        ))
    return 0


def cmd_export(args) -> int:
    recording = _open_recording(args)
    analog_select = args.analog_channels or None
    digital_select = args.digital_channels or None
    counts = export_parquet(
        recording, args.out,
        start_rel_us=args.start_us, end_rel_us=args.end_us,
        analog_channels=analog_select, digital_channels=digital_select,
        side=args.side, batch_size=args.batch_size,
        batch_frames=args.batch_frames, overwrite=args.overwrite,
    )
    print(json.dumps(counts, indent=2, sort_keys=True))
    manifest = Path(counts["manifest"])
    table_files = [Path(counts["out_dir"]) / name for name in counts["files"]]
    for p in table_files:
        pf = pq.ParquetFile(p)
        print(f"{p.name}: rows={pf.metadata.num_rows} "
              f"row_groups={pf.metadata.num_row_groups} "
              f"columns={len(pf.schema_arrow.names)}")
    print(f"manifest: {manifest}")
    return 0


def cmd_make_fixtures(args) -> int:
    made = build_all(args.out)
    for name, cfg in sorted(made.items()):
        print(f"{name}: {cfg}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minenergy-comtrade",
        description="IEEE C37.111-1999 COMTRADE waveform ingestion",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(sp: argparse.ArgumentParser, need_zone: bool = True) -> None:
        sp.add_argument("--cfg", required=True, help="path to the .cfg file")
        sp.add_argument("--dat", default=None,
                        help="explicit .dat path (default: sibling of --cfg, "
                             "extension casing ignored)")
        sp.add_argument("--timezone", required=need_zone,
                        help="IANA timezone for the CFG wall-clock times")
        sp.add_argument("--batch-frames", type=int, default=4096,
                        help="DAT frames decoded per read")

    sp = sub.add_parser("catalog", help="print the channel directory")
    add_common(sp)
    sp.set_defaults(func=cmd_catalog)

    sp = sub.add_parser("inspect", help="full import check without writing data")
    add_common(sp)
    sp.add_argument("--json", action="store_true", help="machine-readable summary")
    sp.set_defaults(func=cmd_inspect)

    sp = sub.add_parser("export", help="stream a trigger-relative window to Parquet")
    add_common(sp)
    sp.add_argument("--out", required=True, help="output directory")
    sp.add_argument("--start-us", type=int, required=True,
                    help="inclusive window start, microseconds relative to trigger")
    sp.add_argument("--end-us", type=int, required=True,
                    help="inclusive window end, microseconds relative to trigger")
    sp.add_argument("--analog-channels", nargs="*", default=None,
                    help="analog channel names or 1-based indexes (default: all)")
    sp.add_argument("--digital-channels", nargs="*", default=None,
                    help="digital channel names or 1-based indexes (default: all)")
    sp.add_argument("--side", choices=("primary", "secondary"),
                    default="secondary")
    sp.add_argument("--batch-size", type=int, default=8192,
                    help="Arrow RecordBatch / Parquet row-group target rows")
    sp.add_argument("--overwrite", action="store_true")
    sp.set_defaults(func=cmd_export)

    sp = sub.add_parser("make-fixtures", help="regenerate the offline fixtures")
    sp.add_argument("--out", required=True)
    sp.set_defaults(func=cmd_make_fixtures)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ComtradeError as exc:
        print(f"REJECTED: {exc.render()}", file=sys.stderr)
        return 2
    except (FileExistsError, NotADirectoryError, FileNotFoundError) as exc:
        print(f"REJECTED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
