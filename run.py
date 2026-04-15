"""
Unified entry point for ai-quant-lab.

Dispatches to one of five modes:

  server     Start the FastAPI HTTP server via uvicorn
  pipeline   Run a config-driven YAML batch-fetch pipeline
  fetch      Fetch historical data for a single symbol (CLI)
  ticker     Stream live NIFTY 50 tick data into tick_data (runs until Ctrl+C)
  simulator  Start the Kite WebSocket Simulator + management API (no Zerodha needed)

Usage
-----
    python run.py server [--host HOST] [--port PORT] [--reload]
    python run.py pipeline configs/daily_nifty50_fetch.yaml [--dry-run] [--workers N]
    python run.py fetch --symbol RELIANCE --from 2025-01-01 --to 2025-03-31 \\
                        [--interval day] [--exchange NSE] [--output FILE.csv]
                        [--oi] [--continuous]
    python run.py ticker [--mode full|quote|ltp] [--workers N]
    python run.py simulator [--host 0.0.0.0] [--port 8765] [--force-open]
                            [--mgmt-port 8766]
"""

from __future__ import annotations

import argparse
import sys

from log_config import configure_logging

configure_logging()


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def _run_server(args: argparse.Namespace) -> None:
    try:
        import uvicorn
    except ImportError:
        sys.exit("ERROR: uvicorn is not installed.  Run: pip install uvicorn")
    uvicorn.run(
        "api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_config=None,   # let our JSON handler own all log output
    )


def _run_pipeline(args: argparse.Namespace) -> None:
    from pipeline import load_config, run_pipeline
    cfg = load_config(args.config)
    run_pipeline(cfg, dry_run=args.dry_run, workers=args.workers)


def _run_simulator(args: argparse.Namespace) -> None:
    """
    Start the Kite Simulator WebSocket server and (optionally) the management API.

    The simulator broadcasts realistic tick frames without a Zerodha subscription.
    Set KITE_USE_SIMULATOR=true in .env and run this to test the full pipeline locally.
    """
    import threading
    import sys
    import os

    # Ensure the simulator package is importable
    sim_dir = os.path.join(os.path.dirname(__file__), "simulator")
    if sim_dir not in sys.path:
        sys.path.insert(0, sim_dir)

    # Start management API in a background thread (optional but convenient)
    if args.mgmt_port:
        try:
            import uvicorn
            from simulator.management_api import app as mgmt_app

            def _run_mgmt():
                uvicorn.run(
                    mgmt_app,
                    host=args.host,
                    port=args.mgmt_port,
                    log_config=None,
                )

            t = threading.Thread(target=_run_mgmt, name="mgmt-api", daemon=True)
            t.start()
            print(
                f"Management API started → http://{args.host}:{args.mgmt_port}/docs\n"
                f"  Create a key: POST http://{args.host}:{args.mgmt_port}/keys"
            )
        except ImportError:
            print("WARNING: uvicorn not installed — management API not started.")

    # Start the simulator WebSocket server (blocking)
    print(
        f"\nKite Simulator → ws://{args.host}:{args.port}"
        + ("  [force-open mode]" if args.force_open else "")
    )
    print("Press Ctrl+C to stop.\n")

    import asyncio
    from simulator.kite_simulator import _main as sim_main

    try:
        asyncio.run(sim_main(args.host, args.port, args.force_open,
                             from_timestamp=getattr(args, "from_timestamp", None),
                             to_timestamp=getattr(args, "to_timestamp", None)))
    except KeyboardInterrupt:
        print("\nSimulator stopped.")


def _run_ticker(args: argparse.Namespace) -> None:
    import signal
    import time
    from database import ensure_tick_tables

    ensure_tick_tables()

    if getattr(args, "sim", False):
        from ws_ticker_sim import SimTickerManager
        manager = SimTickerManager()
        source = "Kite Simulator"
    else:
        from ws_ticker import WsTickerManager
        manager = WsTickerManager(mode=args.mode, num_workers=args.workers)
        source = "Zerodha KiteTicker"

    manager.start_scheduler()   # auto-start at 09:15 IST, auto-stop at 15:30 IST

    # Also start immediately if called manually outside scheduler
    status = manager.start()
    print(f"Ticker {status}. Streaming from {source} → tick_data table.")
    print("Press Ctrl+C to stop.\n")

    stop_event = False

    def _handle_stop(sig, frame):
        nonlocal stop_event
        stop_event = True

    signal.signal(signal.SIGINT,  _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    try:
        while not stop_event:
            time.sleep(5)
            print(
                f"  stored={manager.ticks_stored:,}  "
                f"dropped={manager.ticks_dropped}  "
                f"connected={manager.is_connected}  "
                f"queue={manager.queue_depth}"
            )
    finally:
        manager.stop_scheduler()
        print(f"\nStopped. Total ticks stored: {manager.ticks_stored:,}")


def _run_fetch(args: argparse.Namespace) -> None:
    # Reconstruct sys.argv so main.py's argparse sees the right flags
    argv = [
        "fetch",
        "--symbol",   args.symbol,
        "--from",     args.from_date,
        "--to",       args.to_date,
        "--interval", args.interval,
        "--exchange", args.exchange,
    ]
    if args.output:
        argv += ["--output", args.output]
    if args.oi:
        argv.append("--oi")
    if args.continuous:
        argv.append("--continuous")
    sys.argv = argv

    from main import main as fetch_main
    fetch_main()


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run.py",
        description="Unified ai-quant-lab entry point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = p.add_subparsers(dest="command", required=True)

    # ── server ────────────────────────────────────────────────────────────────
    sv = sub.add_parser(
        "server",
        help="Start the FastAPI HTTP server",
        description="Launch uvicorn serving api:app",
    )
    sv.add_argument("--host",   default="0.0.0.0",  help="Bind host (default: 0.0.0.0)")
    sv.add_argument("--port",   type=int, default=8000, help="Bind port (default: 8000)")
    sv.add_argument("--reload", action="store_true", help="Enable auto-reload (dev only)")

    # ── pipeline ──────────────────────────────────────────────────────────────
    pl = sub.add_parser(
        "pipeline",
        help="Run a config-driven YAML batch pipeline",
        description="Read a YAML config and fetch OHLCV data for all listed instruments",
    )
    pl.add_argument("config", help="Path to a YAML pipeline config file")
    pl.add_argument("--dry-run", action="store_true",
                    help="Print what would be fetched without hitting the API or DB")
    pl.add_argument("--workers", "-w", type=int, default=4, metavar="N",
                    help="Parallel worker threads (default: 4)")

    # ── ticker ────────────────────────────────────────────────────────────────
    tk = sub.add_parser(
        "ticker",
        help="Stream live NIFTY 50 tick data into tick_data (runs until Ctrl+C)",
        description="Connect to Kite WebSocket and store real-time ticks for all watched symbols",
    )
    tk.add_argument(
        "--mode", default="full", choices=["full", "quote", "ltp"],
        help="Tick subscription mode: full=OHLC+depth+OI, quote=OHLC, ltp=price only (default: full)",
    )
    tk.add_argument(
        "--workers", "-w", type=int, default=2, metavar="N",
        help="Parallel DB-writer threads (default: 2)",
    )
    tk.add_argument(
        "--sim", action="store_true",
        help="Use the Kite Simulator instead of real Zerodha KiteTicker",
    )

    # ── simulator ─────────────────────────────────────────────────────────────
    sm = sub.add_parser(
        "simulator",
        help="Start the Kite WebSocket Simulator (no Zerodha subscription needed)",
        description=(
            "Launch the local Kite Simulator WebSocket server. "
            "Set KITE_USE_SIMULATOR=true in .env so the ticker connects to it "
            "instead of the real Zerodha KiteTicker."
        ),
    )
    sm.add_argument("--host",       default="0.0.0.0",
                    help="Bind host (default: 0.0.0.0)")
    sm.add_argument("--port",       type=int, default=8765,
                    help="WebSocket port (default: 8765)")
    sm.add_argument("--force-open", action="store_true",
                    help="Always stream live ticks regardless of IST time / holidays")
    sm.add_argument("--mgmt-port",  type=int, default=8766, metavar="PORT",
                    help="Management API port (default: 8766, 0 to disable)")
    sm.add_argument("--from",       dest="from_timestamp", default=None,
                    metavar="TIMESTAMP",
                    help="Start replay from this timestamp, e.g. '2024-06-01' or '2024-06-01 09:15:00'")
    sm.add_argument("--to",         dest="to_timestamp",   default=None,
                    metavar="TIMESTAMP",
                    help="Stop replay at this timestamp, e.g. '2024-06-01' or '2024-06-01 15:30:00'")

    # ── fetch ─────────────────────────────────────────────────────────────────
    fe = sub.add_parser(
        "fetch",
        help="Fetch historical data for a single symbol",
        description="Single-symbol OHLCV fetch — checks DB first, calls Kite API only when needed",
    )
    fe.add_argument("--symbol",     "-s", required=True,
                    help='Trading symbol, e.g. RELIANCE, "NIFTY 50"')
    fe.add_argument("--from",       dest="from_date", required=True, metavar="YYYY-MM-DD",
                    help="Start date (inclusive)")
    fe.add_argument("--to",         dest="to_date",   required=True, metavar="YYYY-MM-DD",
                    help="End date (inclusive)")
    fe.add_argument("--interval",   "-i", default="day",
                    help="Candle interval: minute/5minute/15minute/day/… (default: day)")
    fe.add_argument("--exchange",   "-e", default="NSE",
                    help="Exchange: NSE, BSE, NFO, MCX (default: NSE)")
    fe.add_argument("--output",     "-o", metavar="FILE.csv",
                    help="Save results to a CSV file (optional)")
    fe.add_argument("--oi",         action="store_true",
                    help="Include open-interest column (F&O)")
    fe.add_argument("--continuous", action="store_true",
                    help="Fetch continuous futures/options data")

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = _build_parser()
    args   = parser.parse_args()

    if args.command == "server":
        _run_server(args)
    elif args.command == "pipeline":
        _run_pipeline(args)
    elif args.command == "fetch":
        _run_fetch(args)
    elif args.command == "ticker":
        _run_ticker(args)
    elif args.command == "simulator":
        _run_simulator(args)
