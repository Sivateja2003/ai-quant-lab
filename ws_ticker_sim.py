"""
ws_ticker_sim.py — Simulator WebSocket client for ai-quant-lab
===============================================================

Connects to the Kite Simulator WebSocket, receives replayed OHLCV tick
frames every second, and stores them into the CLIENT's own MySQL database.

Configuration (.env)
--------------------
    # Simulator server connection
    KITE_SIM_URL=ws://INSTRUCTOR_IP:8765
    KITE_SIM_API_KEY=sim_xxx          # from instructor's management API
    KITE_SIM_ACCESS_TOKEN=sat_yyy

    # YOUR OWN database where ticks get stored
    CLIENT_DB_URL=mysql://your_user:your_pass@your_host:3306/your_db
    # If CLIENT_DB_URL is blank, falls back to MYSQL_URL

Architecture
------------
    Instructor's DB (cohort_main)
          │
    kite_simulator.py  ──── WebSocket broadcast every 1 second
          │                  ws://INSTRUCTOR_IP:8765
          ▼
    SimTickerManager  (this file)
    ├── receives binary tick frames
    ├── parses OHLCV data
    ├── queues rows (bounded queue, max 10 000)
    └── 2 consumer threads ──► CLIENT_DB_URL → tick_data table
                                (YOUR database)

Usage
-----
    python run.py ticker --sim
"""

from __future__ import annotations

import json
import os
import queue
import struct
import threading
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import pytz
import websocket
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from log_config import get_logger

logger = get_logger(__name__)
IST    = pytz.timezone("Asia/Kolkata")

_ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=_ENV_FILE)

# Simulator WebSocket credentials
SIM_URL          = os.getenv("KITE_SIM_URL",          "ws://localhost:8765")
SIM_API_KEY      = os.getenv("KITE_SIM_API_KEY",      "")
SIM_ACCESS_TOKEN = os.getenv("KITE_SIM_ACCESS_TOKEN", "")

# Client's own database — where received ticks are stored
# Falls back to MYSQL_URL if CLIENT_DB_URL is not set
CLIENT_DB_URL = (
    os.getenv("CLIENT_DB_URL", "").strip()
    or os.getenv("MYSQL_URL", "").strip()
)

MARKET_OPEN_HOUR,  MARKET_OPEN_MIN  = 9,  15
MARKET_CLOSE_HOUR, MARKET_CLOSE_MIN = 15, 30

# Binary frame constants
_FRAME_HDR = 2
_PKT_HDR   = 2
_PKT_FMT   = ">iiiiiiqiiiiii"
_PKT_MIN   = 60

# Tuning
_STOP_SENTINEL  = None
QUEUE_MAX_SIZE  = 10_000
BATCH_MAX       = 500
FLUSH_TIMEOUT   = 0.5
FLUSH_RETRY_MAX = 3
NUM_WORKERS     = 2

# ---------------------------------------------------------------------------
# Client DB connection pool  (separate from database.py — uses CLIENT_DB_URL)
# ---------------------------------------------------------------------------

_client_pool      = None
_client_pool_lock = threading.Lock()

_CREATE_TICK_DATA_SQL = """
CREATE TABLE IF NOT EXISTS tick_data (
    id                   BIGINT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    instrument_token     INT              NOT NULL,
    symbol               VARCHAR(50)      NOT NULL,
    exchange             VARCHAR(20)      NOT NULL DEFAULT '',
    captured_at          DATETIME(3)      NOT NULL,
    last_price           DECIMAL(14,4)    NOT NULL,
    open                 DECIMAL(14,4),
    high                 DECIMAL(14,4),
    low                  DECIMAL(14,4),
    close                DECIMAL(14,4),
    volume               BIGINT           DEFAULT 0,
    buy_quantity         INT              DEFAULT 0,
    sell_quantity        INT              DEFAULT 0,
    change_pct           DECIMAL(10,4),
    UNIQUE KEY uq_tick (instrument_token, captured_at, last_price),
    INDEX idx_symbol_captured (symbol, captured_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _get_client_pool():
    """Return (or lazily create) the connection pool for CLIENT_DB_URL."""
    global _client_pool
    if _client_pool:
        return _client_pool

    with _client_pool_lock:
        if _client_pool:
            return _client_pool
        if not CLIENT_DB_URL:
            logger.error(
                "CLIENT_DB_URL is not set in .env. "
                "Ticks will NOT be stored. "
                "Add: CLIENT_DB_URL=mysql://user:pass@host:3306/dbname"
            )
            return None
        try:
            import pymysql
            from dbutils.pooled_db import PooledDB

            p = urlparse(CLIENT_DB_URL)
            _client_pool = PooledDB(
                creator        = pymysql,
                mincached      = 1,
                maxcached      = 3,
                maxconnections = 5,
                blocking       = True,
                host           = p.hostname,
                port           = p.port or 3306,
                user           = p.username,
                password       = p.password,
                database       = p.path.lstrip("/"),
                connect_timeout = 10,
                cursorclass    = pymysql.cursors.DictCursor,
            )
            # Ensure tick_data table exists in client's database
            conn   = _client_pool.connection()
            cursor = conn.cursor()
            cursor.execute(_CREATE_TICK_DATA_SQL)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(
                "Client DB pool ready → %s:%s/%s",
                p.hostname, p.port or 3306, p.path.lstrip("/"),
            )
        except Exception as exc:
            logger.error("Client DB pool failed: %s", exc)
            _client_pool = None

    return _client_pool


def _save_ticks_to_client_db(ticks: list[dict]) -> int:
    """Insert tick rows into the client's own tick_data table."""
    if not ticks:
        return 0
    pool = _get_client_pool()
    if not pool:
        return 0

    saved = 0
    try:
        conn   = pool.connection()
        cursor = conn.cursor()
        for t in ticks:
            cursor.execute(
                """
                INSERT IGNORE INTO tick_data
                    (instrument_token, symbol, exchange, captured_at,
                     last_price, open, high, low, close,
                     volume, buy_quantity, sell_quantity, change_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(t["instrument_token"]),
                    t["symbol"].upper(),
                    t.get("exchange", "NSE"),
                    t["captured_at"],
                    float(t["last_price"]),
                    float(t["open"])   if t.get("open")   is not None else None,
                    float(t["high"])   if t.get("high")   is not None else None,
                    float(t["low"])    if t.get("low")    is not None else None,
                    float(t["close"])  if t.get("close")  is not None else None,
                    int(t.get("volume") or 0),
                    int(t.get("buy_quantity")  or 0),
                    int(t.get("sell_quantity") or 0),
                    float(t["change"]) if t.get("change") is not None else None,
                ),
            )
            saved += cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as exc:
        logger.error("Client DB save error: %s", exc)

    return saved


# ---------------------------------------------------------------------------
# Binary frame parser  (mirrors kite_simulator._pack_candle in reverse)
# ---------------------------------------------------------------------------

def _parse_frame(data: bytes) -> list[dict]:
    """Parse a Zerodha QUOTE-mode binary frame → list of raw tick dicts."""
    if len(data) <= 1:
        return []   # heartbeat

    try:
        num = struct.unpack_from(">H", data, 0)[0]
    except struct.error:
        return []

    ticks  = []
    offset = _FRAME_HDR

    for _ in range(num):
        if offset + _PKT_HDR > len(data):
            break
        pkt_len = struct.unpack_from(">H", data, offset)[0]
        offset += _PKT_HDR
        pkt     = data[offset: offset + pkt_len]
        offset += pkt_len

        if len(pkt) < _PKT_MIN:
            continue

        try:
            (token, _ltq, _avg, last_price, prev_close,
             change, volume, buy_qty, sell_qty,
             o, h, l, c) = struct.unpack_from(_PKT_FMT, pkt, 0)

            ticks.append({
                "instrument_token": token,
                "last_price":       last_price / 100.0,
                "volume_traded":    volume,
                "buy_quantity":     buy_qty,
                "sell_quantity":    sell_qty,
                "change":           change / 100.0,
                "ohlc": {
                    "open":  o / 100.0,
                    "high":  h / 100.0,
                    "low":   l / 100.0,
                    "close": c / 100.0,
                },
            })
        except struct.error as exc:
            logger.warning("Parse error: %s", exc)

    return ticks


# ---------------------------------------------------------------------------
# SimTickerManager
# ---------------------------------------------------------------------------

class SimTickerManager:
    """
    Connects to the Kite Simulator WebSocket and stores received ticks
    into the client's own MySQL database (CLIENT_DB_URL).

    Identical public interface to WsTickerManager so run.py/api.py
    can use it transparently.
    """

    def __init__(self) -> None:
        self._running      = threading.Event()
        self._thread:        Optional[threading.Thread]       = None
        self._ws:            Optional[websocket.WebSocketApp] = None
        self._ws_lock        = threading.Lock()

        self._token_map:     dict[int, dict] = {}
        self._map_lock       = threading.Lock()

        self._tick_queue:    queue.Queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self._store_threads: list[threading.Thread] = []

        self._scheduler      = BackgroundScheduler(timezone=IST)

        self._ticks_stored   = 0
        self._ticks_dropped  = 0
        self._last_tick_at:  Optional[datetime] = None

    # -------------------------------------------------------------------------
    # Scheduler
    # -------------------------------------------------------------------------

    def start_scheduler(self) -> None:
        self._scheduler.add_job(
            self._on_market_open,
            CronTrigger(day_of_week="mon-fri",
                        hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN,
                        timezone=IST),
            id="sim_market_open", replace_existing=True,
        )
        self._scheduler.add_job(
            self._on_market_close,
            CronTrigger(day_of_week="mon-fri",
                        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN,
                        timezone=IST),
            id="sim_market_close", replace_existing=True,
        )
        self._scheduler.start()
        logger.info("Sim ticker scheduler started.")

    def stop_scheduler(self) -> None:
        self.stop()
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def _on_market_open(self) -> None:
        try:
            self.start()
        except Exception as exc:
            logger.error("Sim start failed: %s", exc)

    def _on_market_close(self) -> None:
        self.stop()

    # -------------------------------------------------------------------------
    # Start / Stop
    # -------------------------------------------------------------------------

    def start(self) -> str:
        if self._running.is_set():
            return "already_running"

        if not SIM_API_KEY or not SIM_ACCESS_TOKEN:
            raise RuntimeError(
                "KITE_SIM_API_KEY and KITE_SIM_ACCESS_TOKEN must be set in .env.\n"
                "Get them from your instructor: POST http://INSTRUCTOR_IP:8766/keys"
            )

        # Warm up the client DB pool (creates tick_data table if needed)
        _get_client_pool()

        # Drain leftover queue items
        while not self._tick_queue.empty():
            try:
                self._tick_queue.get_nowait()
            except queue.Empty:
                break

        self._running.set()

        self._store_threads = [
            threading.Thread(target=self._store_loop,
                             name=f"sim-store-{i}", daemon=True)
            for i in range(NUM_WORKERS)
        ]
        for t in self._store_threads:
            t.start()

        self._thread = threading.Thread(
            target=self._connect_loop, name="sim-ticker", daemon=True
        )
        self._thread.start()

        db_info = urlparse(CLIENT_DB_URL)
        logger.info(
            "Simulator ticker started → %s  |  storing to %s:%s/%s",
            SIM_URL,
            db_info.hostname, db_info.port or 3306,
            db_info.path.lstrip("/"),
        )
        return "started"

    def stop(self) -> str:
        if not self._running.is_set():
            return "not_running"

        self._running.clear()
        with self._ws_lock:
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass

        for _ in range(NUM_WORKERS):
            self._tick_queue.put(_STOP_SENTINEL)
        for t in self._store_threads:
            if t.is_alive():
                t.join(timeout=10)
        self._store_threads = []

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)

        logger.info("Sim ticker stopped. stored=%d  dropped=%d",
                    self._ticks_stored, self._ticks_dropped)
        return "stopped"

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running.is_set()

    @property
    def scheduler_running(self) -> bool:
        return self._scheduler.running

    @property
    def is_connected(self) -> bool:
        return self._running.is_set() and self._ws is not None

    @property
    def ticks_stored(self) -> int:
        return self._ticks_stored

    @property
    def ticks_dropped(self) -> int:
        return self._ticks_dropped

    @property
    def queue_depth(self) -> int:
        return self._tick_queue.qsize()

    @property
    def last_tick_at(self) -> Optional[str]:
        return self._last_tick_at.isoformat() if self._last_tick_at else None

    @property
    def subscribed_symbols(self) -> list[str]:
        with self._map_lock:
            return [m["symbol"] for m in self._token_map.values()]

    # -------------------------------------------------------------------------
    # WebSocket
    # -------------------------------------------------------------------------

    def _connect_loop(self) -> None:
        auth_url = (
            f"{SIM_URL}"
            f"?api_key={SIM_API_KEY}"
            f"&access_token={SIM_ACCESS_TOKEN}"
        )
        while self._running.is_set():
            ws = websocket.WebSocketApp(
                auth_url,
                on_open    = self._on_open,
                on_message = self._on_message,
                on_error   = self._on_error,
                on_close   = self._on_close,
            )
            with self._ws_lock:
                self._ws = ws
            try:
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error("WS error: %s", exc)
            finally:
                with self._ws_lock:
                    if self._ws is ws:
                        self._ws = None

            if self._running.is_set():
                logger.info("Reconnecting in 5s…")
                time.sleep(5)

    def _on_open(self, ws) -> None:
        logger.info("Connected to simulator at %s.", SIM_URL)
        with self._map_lock:
            tokens = list(self._token_map.keys())
        if tokens:
            ws.send(json.dumps({"a": "subscribe", "v": tokens}))
            ws.send(json.dumps({"a": "mode", "v": ["quote", tokens]}))

    def _on_message(self, ws, data) -> None:
        # JSON text frame
        if isinstance(data, str):
            try:
                msg  = json.loads(data)
                kind = msg.get("type", "")

                if kind == "instruments":
                    with self._map_lock:
                        for inst in msg.get("data", []):
                            self._token_map[inst["instrument_token"]] = {
                                "symbol":   inst["tradingsymbol"],
                                "exchange": inst["exchange"],
                            }
                    logger.info("Instrument map loaded: %d symbols.", len(self._token_map))
                    with self._map_lock:
                        tokens = list(self._token_map.keys())
                    ws.send(json.dumps({"a": "subscribe", "v": tokens}))
                    ws.send(json.dumps({"a": "mode", "v": ["quote", tokens]}))

                elif kind == "candle_tick":
                    logger.debug(
                        "Replaying candle %d/%d  ts=%s",
                        msg.get("index", 0), msg.get("total", 0),
                        msg.get("timestamp", "?"),
                    )

                elif kind == "market_open":
                    logger.info("Market OPEN.")

                elif kind == "market_closed":
                    logger.info("Market CLOSED. Next open: %s", msg.get("next_open", "?"))

            except Exception as exc:
                logger.warning("JSON parse error: %s", exc)
            return

        # Binary tick frame
        raw_ticks = _parse_frame(data)
        if not raw_ticks:
            return

        captured_at = datetime.now(IST).replace(tzinfo=None)
        rows: list[dict] = []

        with self._map_lock:
            for rt in raw_ticks:
                meta = self._token_map.get(rt["instrument_token"])
                if not meta:
                    continue
                ohlc = rt.get("ohlc", {})
                rows.append({
                    "symbol":           meta["symbol"],
                    "exchange":         meta["exchange"],
                    "instrument_token": rt["instrument_token"],
                    "last_price":       rt["last_price"],
                    "volume":           rt.get("volume_traded", 0),
                    "buy_quantity":     rt.get("buy_quantity",  0),
                    "sell_quantity":    rt.get("sell_quantity", 0),
                    "open":             ohlc.get("open"),
                    "high":             ohlc.get("high"),
                    "low":              ohlc.get("low"),
                    "close":            ohlc.get("close"),
                    "change":           rt.get("change"),
                    "captured_at":      captured_at,
                })

        if not rows:
            return

        self._last_tick_at = datetime.now(IST)
        try:
            self._tick_queue.put_nowait(rows)
        except queue.Full:
            self._ticks_dropped += len(rows)
            logger.warning("Queue full — dropped %d ticks. Total dropped: %d",
                           len(rows), self._ticks_dropped)

    def _on_error(self, ws, err) -> None:
        if "4001" in str(err):
            logger.error("Auth rejected (4001). Check KITE_SIM_API_KEY / KITE_SIM_ACCESS_TOKEN.")
        else:
            logger.error("WS error: %s", err)

    def _on_close(self, ws, code, msg) -> None:
        with self._ws_lock:
            if self._ws is ws:
                self._ws = None
        if code == 4001:
            logger.error("Auth rejected (4001) — stopping.")
            self._running.clear()
            for _ in range(NUM_WORKERS):
                self._tick_queue.put(_STOP_SENTINEL)
        else:
            logger.info("WS closed (code=%s). Will reconnect.", code)

    # -------------------------------------------------------------------------
    # Consumer threads — drain queue and write to CLIENT_DB_URL
    # -------------------------------------------------------------------------

    def _store_loop(self) -> None:
        accumulated: list[dict] = []

        while True:
            try:
                first = self._tick_queue.get(timeout=FLUSH_TIMEOUT)
            except queue.Empty:
                if accumulated:
                    self._flush(accumulated)
                    accumulated = []
                if not self._running.is_set():
                    break
                continue

            if first is _STOP_SENTINEL:
                if accumulated:
                    self._flush(accumulated)
                break

            accumulated.extend(first)

            while len(accumulated) < BATCH_MAX:
                try:
                    more = self._tick_queue.get_nowait()
                except queue.Empty:
                    break
                if more is _STOP_SENTINEL:
                    self._flush(accumulated)
                    return
                accumulated.extend(more)

            if len(accumulated) >= BATCH_MAX:
                self._flush(accumulated)
                accumulated = []

    def _flush(self, ticks: list[dict]) -> None:
        if not ticks:
            return
        last_exc = None
        for attempt in range(1, FLUSH_RETRY_MAX + 1):
            try:
                saved = _save_ticks_to_client_db(ticks)
                self._ticks_stored += saved
                logger.debug("Stored %d tick(s).", saved)
                return
            except Exception as exc:
                last_exc = exc
                if attempt < FLUSH_RETRY_MAX:
                    time.sleep(0.1 * (2 ** (attempt - 1)))
        logger.error("Flush failed after %d attempts — %d tick(s) lost: %s",
                     FLUSH_RETRY_MAX, len(ticks), last_exc)


# Module-level singleton
ticker_manager = SimTickerManager()
