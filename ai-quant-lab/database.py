"""
MySQL database helpers for storing and retrieving historical candle data.

Tables used:
  stock_data      — OHLCV candles keyed by (instrument_token, timestamp)
  fetch_log       — tracks which (symbol, exchange, interval, from_date, to_date)
                    queries have been fetched; used for exact-range duplicate detection
  watched_symbols — symbols to poll every second during market hours
  tick_data       — 1-second real-time snapshots captured during market hours
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    print("ERROR: PyMySQL is not installed.\nRun: pip install pymysql")
    sys.exit(1)

try:
    from dbutils.pooled_db import PooledDB
except ImportError:
    print("ERROR: dbutils is not installed.\nRun: pip install dbutils")
    sys.exit(1)

_pool: PooledDB | None = None


def _get_pool() -> PooledDB:
    global _pool
    if _pool is not None:
        return _pool

    load_dotenv(dotenv_path=ENV_FILE)
    url = os.getenv("MYSQL_URL", "")
    if not url:
        sys.exit("ERROR: MYSQL_URL not set in .env file.")

    parsed   = urlparse(url)
    host     = parsed.hostname
    port     = parsed.port or 3306
    user     = parsed.username
    password = parsed.password
    database = parsed.path.lstrip("/")

    try:
        _pool = PooledDB(
            creator=pymysql,
            mincached=2,        # keep 2 connections open at idle
            maxcached=5,        # max idle connections kept in pool
            maxconnections=10,  # hard cap on total connections
            blocking=True,      # wait instead of raising when pool is full
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )
        print(f"[DB] Pool created: {user}@{host}:{port}/{database}")
        return _pool
    except Exception as exc:
        sys.exit(f"ERROR: Cannot create DB pool: {exc}")


def _get_connection():
    return _get_pool().connection()


_CREATE_FETCH_LOG_SQL = """
CREATE TABLE IF NOT EXISTS fetch_log (
    symbol        VARCHAR(50)  NOT NULL,
    exchange      VARCHAR(20)  NOT NULL,
    interval_type VARCHAR(20)  NOT NULL,
    from_date     DATE         NOT NULL,
    to_date       DATE         NOT NULL,
    fetched_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, exchange, interval_type, from_date, to_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_table() -> None:
    """Verify stock_data is accessible and create fetch_log if needed."""
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM stock_data LIMIT 1")
    cursor.execute(_CREATE_FETCH_LOG_SQL)
    conn.commit()
    cursor.close()
    conn.close()


def data_exists(
    symbol: str,
    exchange: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> bool:
    """
    Return True only if this exact query (symbol+exchange+interval+date range)
    was previously fetched and logged in fetch_log.
    """
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt FROM fetch_log
         WHERE symbol        = %s
           AND exchange      = %s
           AND interval_type = %s
           AND from_date     <= %s
           AND to_date       >= %s
        """,
        (
            symbol.upper(),
            exchange.upper(),
            interval,
            from_dt.date(),
            to_dt.date(),
        ),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return int(row["cnt"]) > 0


def save_to_db(
    df: pd.DataFrame,
    symbol: str,
    instrument_token: int,
    exchange: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> int:
    """
    Insert candles into stock_data and log the query in fetch_log.
    Duplicate candle rows (instrument_token, timestamp) are silently skipped.

    Returns
    -------
    Number of candle rows actually inserted.
    """
    if df.empty:
        return 0

    conn   = _get_connection()
    cursor = conn.cursor()
    rows_inserted = 0

    for ts, row in df.iterrows():
        cursor.execute(
            """
            INSERT IGNORE INTO stock_data
                (instrument_token, symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                instrument_token,
                symbol.upper(),
                ts.to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]),
            ),
        )
        rows_inserted += cursor.rowcount

    # Log the fetched query range so future identical queries are detected
    cursor.execute(
        """
        INSERT IGNORE INTO fetch_log
            (symbol, exchange, interval_type, from_date, to_date)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            symbol.upper(),
            exchange.upper(),
            interval,
            from_dt.date(),
            to_dt.date(),
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()
    return rows_inserted


# ---------------------------------------------------------------------------
# Real-time tick tables
# ---------------------------------------------------------------------------

_CREATE_WATCHED_SYMBOLS_SQL = """
CREATE TABLE IF NOT EXISTS watched_symbols (
    symbol    VARCHAR(50) NOT NULL,
    exchange  VARCHAR(20) NOT NULL DEFAULT 'NSE',
    added_at  DATETIME    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_tick_tables() -> None:
    """Create watched_symbols table if it does not exist."""
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(_CREATE_WATCHED_SYMBOLS_SQL)
    conn.commit()
    cursor.close()
    conn.close()


def get_watched_symbols() -> list[dict]:
    """Return all rows from watched_symbols as a list of dicts."""
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, exchange, added_at FROM watched_symbols ORDER BY added_at")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "symbol":   row["symbol"],
            "exchange": row["exchange"],
            "added_at": row["added_at"].isoformat() if row["added_at"] else None,
        }
        for row in rows
    ]


def add_watched_symbol(symbol: str, exchange: str = "NSE") -> bool:
    """
    Insert a symbol into watched_symbols.
    Returns True if inserted, False if it already existed.
    """
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT IGNORE INTO watched_symbols (symbol, exchange) VALUES (%s, %s)",
        (symbol.upper(), exchange.upper()),
    )
    inserted = cursor.rowcount > 0
    conn.commit()
    cursor.close()
    conn.close()
    return inserted


def remove_watched_symbol(symbol: str, exchange: str = "NSE") -> bool:
    """
    Delete a symbol from watched_symbols.
    Returns True if deleted, False if not found.
    """
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM watched_symbols WHERE symbol = %s AND exchange = %s",
        (symbol.upper(), exchange.upper()),
    )
    deleted = cursor.rowcount > 0
    conn.commit()
    cursor.close()
    conn.close()
    return deleted


def save_ticks(ticks: list[dict]) -> int:
    """
    Bulk-insert real-time 1-second tick snapshots into stock_data
    (same table used for historical candles).

    Each dict must have: symbol, instrument_token, captured_at, last_price
    Optional keys:       open, high, low, close, volume

    Returns number of rows inserted.
    """
    if not ticks:
        return 0

    conn   = _get_connection()
    cursor = conn.cursor()
    rows_inserted = 0

    for t in ticks:
        cursor.execute(
            """
            INSERT IGNORE INTO stock_data
                (instrument_token, symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(t["instrument_token"]),
                t["symbol"].upper(),
                t["captured_at"],
                float(t["open"])  if t.get("open")  is not None else float(t["last_price"]),
                float(t["high"])  if t.get("high")  is not None else float(t["last_price"]),
                float(t["low"])   if t.get("low")   is not None else float(t["last_price"]),
                float(t["last_price"]),
                int(t.get("volume") or 0),
            ),
        )
        rows_inserted += cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()
    return rows_inserted
