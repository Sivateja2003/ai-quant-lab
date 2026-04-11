"""
Zerodha Kite Connect — Historical data fetcher.

Given a trading symbol, exchange, date range, and candle interval this module:
  1. Looks up the instrument token for the symbol.
  2. Calls kite.historical_data() to retrieve OHLCV candles.
  3. Returns a pandas DataFrame and optionally saves it to CSV.
"""

from __future__ import annotations

import sys
from datetime import datetime, date
from typing import Union

import pandas as pd
from kiteconnect import KiteConnect

# Candle intervals supported by Kite Connect
VALID_INTERVALS = {
    "minute", "3minute", "5minute", "10minute", "15minute",
    "30minute", "60minute", "day",
}

# Maximum lookback in calendar days per interval (Kite Connect limits)
INTERVAL_MAX_DAYS = {
    "minute": 60,
    "3minute": 100,
    "5minute": 100,
    "10minute": 100,
    "15minute": 200,
    "30minute": 200,
    "60minute": 400,
    "day": 2000,
}


def lookup_instrument_token(
    kite: KiteConnect,
    symbol: str,
    exchange: str = "NSE",
) -> int:
    """
    Return the instrument_token for `symbol` on `exchange`.

    Parameters
    ----------
    kite     : authenticated KiteConnect instance
    symbol   : trading symbol, e.g. "RELIANCE", "NIFTY 50"
    exchange : exchange segment, e.g. "NSE", "BSE", "NFO", "MCX"
    """
    instruments = kite.instruments(exchange=exchange)
    symbol_upper = symbol.upper().strip()

    matches = [
        inst for inst in instruments
        if inst["tradingsymbol"].upper() == symbol_upper
    ]

    if not matches:
        # Provide a helpful list of close matches
        close = [
            inst["tradingsymbol"]
            for inst in instruments
            if symbol_upper in inst["tradingsymbol"].upper()
        ][:10]
        hint = f"\n  Possible matches: {close}" if close else ""
        sys.exit(
            f"ERROR: Symbol '{symbol}' not found on {exchange}.{hint}\n"
            f"Check the symbol name or try a different exchange (NSE, BSE, NFO, MCX)."
        )

    if len(matches) > 1:
        print(
            f"WARNING: Multiple instruments match '{symbol}' on {exchange}. "
            f"Using the first: {matches[0]['tradingsymbol']} "
            f"(token={matches[0]['instrument_token']}, "
            f"name='{matches[0]['name']}')."
        )

    token: int = matches[0]["instrument_token"]
    return token


def fetch_historical_data(
    kite: KiteConnect,
    symbol: str,
    from_date: Union[str, date, datetime],
    to_date: Union[str, date, datetime],
    interval: str = "day",
    exchange: str = "NSE",
    continuous: bool = False,
    oi: bool = False,
) -> pd.DataFrame:
    """
    Fetch OHLCV candle data for a symbol and return a DataFrame.

    Parameters
    ----------
    kite       : authenticated KiteConnect instance
    symbol     : trading symbol, e.g. "RELIANCE"
    from_date  : start date  — "YYYY-MM-DD" string, date, or datetime
    to_date    : end date    — "YYYY-MM-DD" string, date, or datetime
    interval   : candle interval — one of VALID_INTERVALS
    exchange   : exchange segment, default "NSE"
    continuous : True for continuous data (futures/options)
    oi         : True to include open interest column

    Returns
    -------
    pd.DataFrame with columns: date, open, high, low, close, volume[, oi]
    """
    interval = interval.lower().strip()
    if interval not in VALID_INTERVALS:
        sys.exit(
            f"ERROR: Invalid interval '{interval}'. "
            f"Choose from: {sorted(VALID_INTERVALS)}"
        )

    # Normalise dates to datetime objects for the SDK
    from_dt = _to_datetime(from_date, is_start=True)
    to_dt   = _to_datetime(to_date,   is_start=False)

    if from_dt > to_dt:
        sys.exit("ERROR: from_date must be earlier than to_date.")

    delta_days = (to_dt - from_dt).days
    max_days = INTERVAL_MAX_DAYS[interval]
    if delta_days > max_days:
        print(
            f"WARNING: The requested range ({delta_days} days) exceeds the "
            f"Kite Connect limit of {max_days} days for '{interval}' candles. "
            f"The API may return partial data or raise an error."
        )

    instrument_token = lookup_instrument_token(kite, symbol, exchange)

    print(
        f"Fetching {interval} candles for {symbol} ({exchange}) "
        f"from {from_dt.date()} to {to_dt.date()} "
        f"[token={instrument_token}] …"
    )

    records = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=interval,
        continuous=continuous,
        oi=oi,
    )

    if not records:
        print("No data returned for the given parameters.")
        return pd.DataFrame(), instrument_token

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df.sort_index(inplace=True)

    print(f"Retrieved {len(df)} candles.")
    return df, instrument_token


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_datetime(value: Union[str, date, datetime], is_start: bool) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        time_part = "00:00:00" if is_start else "23:59:59"
        return datetime.strptime(f"{value} {time_part}", "%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(value, fmt)
                if fmt == "%Y-%m-%d":
                    dt = dt.replace(hour=0 if is_start else 23,
                                    minute=0 if is_start else 59,
                                    second=0 if is_start else 59)
                return dt
            except ValueError:
                continue
        sys.exit(f"ERROR: Cannot parse date '{value}'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.")
    sys.exit(f"ERROR: Unsupported date type: {type(value)}")
