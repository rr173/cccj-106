"""Foreground Arrow Flight server for an existing minenergy store.

Usage::

    python -m minenergy.flight_server --store /tmp/minenergy-demo/store
    python -m minenergy.flight_server --store DIR --host 127.0.0.1 --port 8815

Prints one ``MINENERGY_FLIGHT_READY`` JSON line as soon as the socket is bound
(so scripts can wait for a real port rather than guessing), then blocks until
SIGINT/SIGTERM.  Publishing (``minenergy.cli incremental`` etc.) keeps working
against the same store while the server runs: the service is read-only.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from .flight import make_flight_server, store_root_or_die


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minenergy.flight_server")
    parser.add_argument("--store", required=True, help="existing minenergy store root")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="TCP port; 0 (default) lets the OS pick one (printed on startup)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = store_root_or_die(args.store)
    server = make_flight_server(root, (args.host, args.port))
    stop = threading.Event()

    def _handler(signum, frame):  # noqa: ARG001
        stop.set()
        # shutdown() must not be called from the handler thread itself in an
        # implementation callback; a worker thread is safe.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    print(
        json.dumps(
            {
                "status": "ready",
                "store": str(root),
                "host": args.host,
                "port": server.port,
                "location": f"grpc://{args.host}:{server.port}",
            }
        ),
        flush=True,
    )
    server.serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
