"""
kite_simulator.py — MySQL-backed OHLCV replay WebSocket simulator
==================================================================

Reads real historical OHLCV candles from the stock_data table in MySQL
and replays them through WebSocket at 1-second intervals — one candle
timestamp per second — so connected clients receive data exactly as if
it were a live feed.

Flow
----
  MySQL stock_data
       └── loaded at startup (all instruments, all timestamps)
             └── broadcast via WebSocket every 1 second
                   └── ws_ticker_sim.py receives → saves to MySQL tick_data

Run
---
    python run.py simulator           # one candle per second
    python run.py simulator --force-open   # same (force-open has no effect here)

Pre-requisite
-------------
    Historical data must already be in stock_data.
    Fetch it first:
        python run.py fetch --symbol RELIANCE --from 2024-01-01 --to 2024-12-31
        python run.py pipeline configs/daily_nifty50_fetch.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import struct
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytz
import websockets
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Path setup — auth.py lives in the same directory as this file
# ---------------------------------------------------------------------------

_SIM_DIR = Path(__file__).resolve().parent
if str(_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(_SIM_DIR))

try:
    import auth as auth_store
    AUTH_AVAILABLE = True
except ImportError:
    auth_store     = None
    AUTH_AVAILABLE = False
    logger.warning("auth.py not found — all connections accepted (dev mode).")

# Load .env from ai-quant-lab root to get MYSQL_URL
load_dotenv(dotenv_path=_SIM_DIR.parent / ".env")
MYSQL_URL = os.getenv("MYSQL_URL", "")

# ---------------------------------------------------------------------------
# Binary frame constants  (Zerodha QUOTE-mode wire format)
# ---------------------------------------------------------------------------

_PKT_FMT   = ">iiiiiiqiiiiii"   # token, ltq, avg, last, prev_close, change, vol, bq, sq, o, h, l, c
_PKT_LEN   = 184                 # padded packet length (matches real Zerodha)
HEARTBEAT_BYTE = struct.pack("B", 33)    # 0x21

# ---------------------------------------------------------------------------
# Global in-memory replay state (loaded once at startup)
# ---------------------------------------------------------------------------

# instrument_token → {"symbol": str, "exchange": str}
_instruments: dict[int, dict] = {}

# Ordered list of (timestamp, [candle_dict, ...])
# candle_dict keys: token, open, high, low, close, volume, prev_close, change
_replay_data: list[tuple] = []

# JSON manifest sent to every client on connect
_manifest: str = ""

# Connected clients: ws → set[instrument_token]
_connections: dict = {}


# ---------------------------------------------------------------------------
# MySQL data loader
# ---------------------------------------------------------------------------

def _load_from_mysql() -> None:
    """
    Load all OHLCV candles into memory, grouped by timestamp.

    Priority:
      1. stock_data  — historical OHLCV candles (preferred)
      2. tick_data   — fallback if stock_data is empty
      3. Exit with error if both are empty
    """
    global _instruments, _replay_data, _manifest

    if not MYSQL_URL:
        raise RuntimeError(
            "MYSQL_URL is not set in .env. "
            "Add: MYSQL_URL=mysql://user:pass@host:3306/dbname"
        )

    import pymysql
    import pymysql.cursors

    parsed = urlparse(MYSQL_URL)
    conn = pymysql.connect(
        host            = parsed.hostname,
        port            = parsed.port or 3306,
        user            = parsed.username,
        password        = parsed.password,
        database        = parsed.path.lstrip("/"),
        cursorclass     = pymysql.cursors.DictCursor,
        connect_timeout = 15,
    )

    try:
        cursor = conn.cursor()

        # ── Try stock_data first ──────────────────────────────────────────────
        cursor.execute("""
            SELECT DISTINCT instrument_token, symbol
              FROM stock_data
             WHERE instrument_token IS NOT NULL
               AND symbol           IS NOT NULL
             ORDER BY symbol
        """)
        for row in cursor.fetchall():
            _instruments[int(row["instrument_token"])] = {
                "symbol":   row["symbol"].upper(),
                "exchange": "NSE",
            }

        if _instruments:
            cursor.execute("""
                SELECT instrument_token, timestamp,
                       open, high, low, close, volume
                  FROM stock_data
                 WHERE instrument_token IS NOT NULL
                 ORDER BY timestamp ASC, instrument_token ASC
            """)
            raw_rows = cursor.fetchall()
            source = "stock_data"
        else:
            raw_rows = []
            source   = None

        # ── Fallback to tick_data if stock_data is empty ─────────────────────
        if not raw_rows:
            logger.warning("stock_data is empty — trying tick_data as fallback.")
            _instruments.clear()

            cursor.execute("""
                SELECT DISTINCT instrument_token, symbol, exchange
                  FROM tick_data
                 WHERE instrument_token IS NOT NULL
                   AND symbol           IS NOT NULL
                 ORDER BY symbol
            """)
            for row in cursor.fetchall():
                _instruments[int(row["instrument_token"])] = {
                    "symbol":   row["symbol"].upper(),
                    "exchange": row.get("exchange", "NSE") or "NSE",
                }

            if _instruments:
                cursor.execute("""
                    SELECT instrument_token,
                           captured_at  AS timestamp,
                           open, high, low, close,
                           volume
                      FROM tick_data
                     WHERE instrument_token IS NOT NULL
                     ORDER BY captured_at ASC, instrument_token ASC
                """)
                raw_rows = cursor.fetchall()
                source   = "tick_data"

        cursor.close()
    finally:
        conn.close()

    if not raw_rows:
        raise RuntimeError(
            "Both stock_data and tick_data are empty. "
            "Fetch historical data first:\n"
            "  python run.py pipeline configs/daily_nifty50_fetch.yaml"
        )

    logger.info("Loading replay data from %s (%d instruments).", source, len(_instruments))

    # Group by timestamp and compute change vs previous close
    prev_close: dict[int, float] = {}
    by_ts: dict = defaultdict(list)

    for row in raw_rows:
        token = int(row["instrument_token"])
        ts    = row["timestamp"]           # datetime object from PyMySQL
        o     = float(row["open"]   or 0)
        h     = float(row["high"]   or 0)
        l     = float(row["low"]    or 0)
        c     = float(row["close"]  or 0)
        v     = int(row["volume"]   or 0)
        pc    = prev_close.get(token, o)   # fallback to open on first candle
        change = round((c - pc) / max(pc, 0.01) * 100, 4)

        by_ts[ts].append({
            "token":      token,
            "open":       o,
            "high":       h,
            "low":        l,
            "close":      c,
            "volume":     v,
            "prev_close": pc,
            "change":     change,
        })
        prev_close[token] = c

    _replay_data = sorted(by_ts.items(), key=lambda x: x[0])

    # Build instrument manifest JSON (sent to clients on connect)
    _manifest = json.dumps({
        "type": "instruments",
        "data": [
            {
                "instrument_token": tok,
                "tradingsymbol":    meta["symbol"],
                "exchange":         meta["exchange"],
            }
            for tok, meta in _instruments.items()
        ],
    })

    logger.info(
        "Loaded %d instruments, %d unique timestamps from %s.",
        len(_instruments), len(_replay_data), source,
    )


# ---------------------------------------------------------------------------
# Binary frame builder
# ---------------------------------------------------------------------------

def _pack_candle(c: dict) -> bytes:
    """Pack one OHLCV candle dict into a 184-byte Zerodha QUOTE-mode packet."""
    def p(v): return int(round(v * 100))

    body = struct.pack(
        _PKT_FMT,
        c["token"],
        0,                       # last_traded_qty  (not in OHLCV data)
        p(c["close"]),           # avg_traded_price  → use close
        p(c["close"]),           # last_price        → close of candle
        p(c["prev_close"]),      # previous close
        int(c["change"] * 100),  # change in hundredths of a percent
        c["volume"],             # volume  (long long)
        0,                       # buy_qty  (not in OHLCV data)
        0,                       # sell_qty (not in OHLCV data)
        p(c["open"]),
        p(c["high"]),
        p(c["low"]),
        p(c["close"]),
    )
    return body + b"\x00" * (_PKT_LEN - len(body))


def _build_frame(candles: list[dict], subscribed: set[int]) -> bytes:
    """Build a multi-packet binary frame for the subscribed tokens."""
    packets = [
        struct.pack(">H", _PKT_LEN) + _pack_candle(c)
        for c in candles
        if c["token"] in subscribed
    ]
    if not packets:
        return b""
    return struct.pack(">H", len(packets)) + b"".join(packets)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _authenticate(ws) -> bool:
    qs      = parse_qs(urlparse(ws.request.path).query)
    api_key = qs.get("api_key",      [""])[0]
    token   = qs.get("access_token", [""])[0]
    if not api_key or not token:
        return False
    if not AUTH_AVAILABLE:
        return True   # dev mode — accept everything
    return auth_store.validate(api_key, token)


# ---------------------------------------------------------------------------
# Per-connection handler
# ---------------------------------------------------------------------------

async def _handle(ws, force_open: bool = False) -> None:
    """Authenticate the client, send the manifest, then handle subscriptions."""
    if not _authenticate(ws):
        logger.warning("Auth failed from %s", ws.remote_address)
        await ws.close(4001, "Invalid api_key or access_token")
        return

    # Default: subscribe to all instruments
    _connections[ws] = set(_instruments.keys())
    logger.info("Client connected: %s  (total=%d)", ws.remote_address, len(_connections))

    # Send instrument manifest so the client builds its token map
    await ws.send(_manifest)

    # Handle subscribe / unsubscribe / mode messages from client
    try:
        async for raw in ws:
            if isinstance(raw, bytes):
                continue
            try:
                msg = json.loads(raw)
                a   = msg.get("a", "")
                v   = msg.get("v", [])
                if a == "subscribe":
                    _connections[ws].update(
                        int(t) for t in v if int(t) in _instruments
                    )
                elif a == "unsubscribe":
                    _connections[ws].difference_update(int(t) for t in v)
                elif a == "mode":
                    # v = ["quote", [token, token, ...]]
                    tokens = v[1] if len(v) > 1 else []
                    _connections[ws].update(
                        int(t) for t in tokens if int(t) in _instruments
                    )
            except (json.JSONDecodeError, ValueError):
                pass

    except websockets.exceptions.ConnectionClosed as exc:
        logger.info("Client disconnected: %s  code=%s", ws.remote_address, exc.code)
    finally:
        _connections.pop(ws, None)
        logger.info("Client removed (total=%d)", len(_connections))


# ---------------------------------------------------------------------------
# Replay broadcast loop
# ---------------------------------------------------------------------------

async def _replay_loop(force_open: bool, from_timestamp: str | None = None, to_timestamp: str | None = None) -> None:
    """
    Broadcast one candle-timestamp per second to all connected clients.

    Each iteration:
      1. Take the next (timestamp, candles) pair from _replay_data.
      2. Send a JSON header with the original candle timestamp so clients
         know what time period the data represents.
      3. Send the binary tick frame to every subscribed client.
      4. Advance the replay index — stops and shuts down when all data is replayed.
    """
    if not _replay_data:
        logger.error("No data to replay. Fetch historical data first.")
        return

    total     = len(_replay_data)
    idx       = 0
    end_idx   = total   # exclusive upper bound — default: replay everything

    from dateutil.parser import parse as _parse_dt

    def _naive(ts):
        return ts.replace(tzinfo=None) if hasattr(ts, "tzinfo") else ts

    # Find starting index when --from is specified
    if from_timestamp:
        try:
            target = _naive(_parse_dt(from_timestamp))
            for i, (ts, _) in enumerate(_replay_data):
                if _naive(ts) >= target:
                    idx = i
                    break
            else:
                logger.warning(
                    "--from %s is beyond the last timestamp — starting from beginning.",
                    from_timestamp,
                )
        except Exception as exc:
            logger.warning("Could not parse --from '%s': %s — starting from beginning.", from_timestamp, exc)

    # Find ending index when --to is specified
    if to_timestamp:
        try:
            target = _naive(_parse_dt(to_timestamp))
            for i, (ts, _) in enumerate(_replay_data):
                if _naive(ts) > target:
                    end_idx = i
                    break
            else:
                end_idx = total   # --to is at or beyond last timestamp — play to end
            if end_idx <= idx:
                logger.warning(
                    "--to %s is before or equal to --from — no data to replay.",
                    to_timestamp,
                )
                return
        except Exception as exc:
            logger.warning("Could not parse --to '%s': %s — playing to end.", to_timestamp, exc)

    first_ts    = _replay_data[idx][0]
    last_ts     = _replay_data[end_idx - 1][0]
    first_ts_str = first_ts.isoformat() if hasattr(first_ts, "isoformat") else str(first_ts)
    last_ts_str  = last_ts.isoformat()  if hasattr(last_ts,  "isoformat") else str(last_ts)
    replay_count = end_idx - idx

    logger.info(
        "Replay loop started — %d timestamps to broadcast (index %d→%d)  "
        "%s  →  %s.  Press Ctrl+C to stop.",
        replay_count, idx, end_idx - 1, first_ts_str, last_ts_str,
    )

    while True:
        await asyncio.sleep(1.0)

        if not _connections:
            continue   # no clients connected — keep looping, don't advance index

        ts, candles = _replay_data[idx]

        # Timestamp context (clients can log / display the original candle time)
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        context_msg = json.dumps({
            "type":      "candle_tick",
            "timestamp": ts_str,
            "index":     idx + 1,
            "total":     total,
        })

        dead = []
        for ws, subscribed in list(_connections.items()):
            try:
                await ws.send(context_msg)
                frame = _build_frame(candles, subscribed)
                if frame:
                    await ws.send(frame)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _connections.pop(ws, None)

        idx += 1
        if idx >= end_idx:
            logger.info(
                "All %d timestamps replayed — simulator shutting down.", replay_count
            )
            # Notify all connected clients that replay is finished
            done_msg = json.dumps({"type": "replay_done", "total": total})
            for ws in list(_connections):
                try:
                    await ws.send(done_msg)
                    await ws.close()
                except Exception:
                    pass
            _connections.clear()
            return


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def _main(host: str, port: int, force_open: bool, from_timestamp: str | None = None, to_timestamp: str | None = None) -> None:
    _load_from_mysql()

    if AUTH_AVAILABLE:
        default = auth_store.ensure_default_key()
        if default:
            print("\n" + "=" * 60)
            print("  FIRST-RUN DEFAULT CREDENTIALS  (save these now!)")
            print(f"  api_key      = {default['api_key']}")
            print(f"  access_token = {default['access_tokens'][0]}")
            print("=" * 60 + "\n")

    logger.info(
        "MySQL Replay Simulator → ws://%s:%d  |  %d instruments  |  %d timestamps",
        host, port, len(_instruments), len(_replay_data),
    )

    async def handler(ws):
        await _handle(ws, force_open=force_open)

    async with websockets.serve(handler, host, port, ping_interval=None, max_size=None):
        await _replay_loop(force_open, from_timestamp=from_timestamp, to_timestamp=to_timestamp)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MySQL OHLCV Replay Simulator — replays stock_data at 1 candle/sec"
    )
    p.add_argument("--host",       default="0.0.0.0")
    p.add_argument("--port",       default=8765, type=int)
    p.add_argument("--force-open", action="store_true",
                   help="Ignored (kept for CLI compatibility)")
    p.add_argument("--log-level",  default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level   = getattr(logging, args.log_level),
        format  = "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
    try:
        asyncio.run(_main(args.host, args.port, args.force_open))
    except KeyboardInterrupt:
        logger.info("Simulator stopped.")


if __name__ == "__main__":
    main()
