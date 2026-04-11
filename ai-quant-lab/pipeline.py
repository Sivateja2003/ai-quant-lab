"""
Config-driven pipeline runner.

Reads a YAML config file and fetches OHLCV data for every instrument listed,
storing results in the database.  The Python code never changes — only the
YAML config changes when you need different stocks, dates, or intervals.

Usage
-----
    python pipeline.py configs/daily_nifty50_fetch.yaml
    python pipeline.py configs/intraday_5min.yaml
    python pipeline.py configs/daily_nifty50_fetch.yaml --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path

import yaml
import pandas as pd

from auth import get_authenticated_kite
from database import ensure_table, data_exists, save_to_db
from fetcher import fetch_historical_data, _to_datetime, INTERVAL_MAX_DAYS, VALID_INTERVALS


# ---------------------------------------------------------------------------
# Config loading & validation
# ---------------------------------------------------------------------------

REQUIRED_KEYS = {"pipeline_name", "data_source", "date_range"}

def load_config(path: str) -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        sys.exit(f"ERROR: Config file not found: {path}")
    with cfg_path.open("r") as f:
        cfg = yaml.safe_load(f)
    _validate(cfg, path)
    return cfg


def _validate(cfg: dict, path: str) -> None:
    missing = REQUIRED_KEYS - set(cfg.keys())
    if missing:
        sys.exit(f"ERROR: Config '{path}' is missing required keys: {missing}")

    src = cfg["data_source"]
    if "instruments" not in src or not src["instruments"]:
        sys.exit(f"ERROR: Config '{path}' must have at least one instrument under data_source.instruments")

    dr = cfg["date_range"]
    for key in ("start", "end", "interval"):
        if key not in dr:
            sys.exit(f"ERROR: Config '{path}' is missing date_range.{key}")

    interval = dr["interval"]
    if interval not in VALID_INTERVALS:
        sys.exit(
            f"ERROR: Invalid interval '{interval}' in '{path}'. "
            f"Valid options: {sorted(VALID_INTERVALS)}"
        )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(cfg: dict, dry_run: bool = False) -> dict:
    name        = cfg.get("pipeline_name", "unnamed")
    description = cfg.get("description", "")
    src         = cfg["data_source"]
    dr          = cfg["date_range"]

    exchange   = src.get("exchange", "NSE").upper()
    interval   = dr["interval"]
    from_dt    = _to_datetime(str(dr["start"]), is_start=True)
    to_dt      = _to_datetime(str(dr["end"]),   is_start=False)
    symbols    = [s.strip().upper() for s in src["instruments"]]
    continuous = src.get("continuous", False)
    oi         = src.get("oi", False)

    retry_cfg    = cfg.get("retry", {})
    max_attempts = int(retry_cfg.get("max_attempts", 3))
    backoff_secs = float(retry_cfg.get("backoff_seconds", 2))

    print(f"\n{'='*60}")
    print(f"Pipeline : {name}")
    if description:
        print(f"           {description}")
    print(f"Exchange : {exchange}  |  Interval: {interval}")
    print(f"Range    : {from_dt.date()} → {to_dt.date()}")
    print(f"Symbols  : {len(symbols)} instruments")
    print(f"{'='*60}")

    if dry_run:
        print("[dry-run] Symbols that would be fetched:")
        for s in symbols:
            print(f"  - {s}")
        print("[dry-run] No data fetched or saved.")
        return {}

    ensure_table()
    kite  = get_authenticated_kite()
    chunk = timedelta(days=INTERVAL_MAX_DAYS.get(interval, 60) - 1)

    results: dict[str, list] = {"fetched": [], "skipped": [], "failed": []}

    for symbol in symbols:
        if data_exists(symbol, exchange, interval, from_dt, to_dt):
            print(f"  [SKIP]  {symbol} — already in DB")
            results["skipped"].append(symbol)
            continue

        success = False
        for attempt in range(1, max_attempts + 1):
            try:
                all_frames, token = [], None
                cur = from_dt

                while cur <= to_dt:
                    end = min(cur + chunk, to_dt)
                    df, tok = fetch_historical_data(
                        kite=kite,
                        symbol=symbol,
                        from_date=cur,
                        to_date=end,
                        interval=interval,
                        exchange=exchange,
                        continuous=continuous,
                        oi=oi,
                    )
                    if not df.empty:
                        all_frames.append(df)
                        token = tok
                    cur = end + timedelta(days=1)

                if all_frames:
                    full_df = pd.concat(all_frames)
                    full_df = full_df[~full_df.index.duplicated(keep="first")].sort_index()
                    rows = save_to_db(
                        full_df, symbol, token, exchange,
                        interval, from_dt, to_dt,
                    )
                    print(f"  [OK]    {symbol} — {rows} rows saved")
                else:
                    print(f"  [WARN]  {symbol} — no data returned from API")

                results["fetched"].append(symbol)
                success = True
                break

            except Exception as exc:
                if attempt < max_attempts:
                    print(f"  [RETRY] {symbol} (attempt {attempt}/{max_attempts}): {exc} — retrying in {backoff_secs}s")
                    time.sleep(backoff_secs)
                else:
                    print(f"  [FAIL]  {symbol} — {exc}")
                    results["failed"].append(symbol)

    # Summary
    print(f"\n{'='*60}")
    print(f"Done: {len(results['fetched'])} fetched, "
          f"{len(results['skipped'])} skipped, "
          f"{len(results['failed'])} failed")
    if results["failed"]:
        print(f"Failed symbols: {results['failed']}")
    print(f"{'='*60}\n")

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run a config-driven OHLCV fetch pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("config", help="Path to a YAML pipeline config file")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be fetched without hitting the API or DB",
    )
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    cfg  = load_config(args.config)
    run_pipeline(cfg, dry_run=args.dry_run)
