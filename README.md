# AI Quant Lab — Zerodha Kite Connect

A production-grade data platform for collecting and storing Indian stock market data via the [Zerodha Kite Connect API](https://kite.trade/docs/connect/v3/). It handles both **historical OHLCV candles** (stored in `stock_data`) and **real-time WebSocket tick data** (stored in `tick_data`) for all 50 NIFTY 50 stocks.

---

## Table of Contents

1. [Project Architecture](#project-architecture)
2. [Project Structure](#project-structure)
3. [Prerequisites & Installation](#prerequisites--installation)
4. [Authentication Setup](#authentication-setup)
5. [Running the Project](#running-the-project)
   - [1. Start the API Server](#1-start-the-api-server)
   - [2. Collect Historical Data (Pipeline)](#2-collect-historical-data-pipeline)
   - [3. Collect Historical Data (Single Symbol)](#3-collect-historical-data-single-symbol)
   - [4. Stream Live Tick Data](#4-stream-live-tick-data)
6. [Database Schema](#database-schema)
7. [API Endpoints](#api-endpoints)
8. [Configuration Files (YAML)](#configuration-files-yaml)
9. [How Data Flows](#how-data-flows)
10. [File Reference](#file-reference)

---

## Project Architecture

```
                     ┌─────────────────────────────────┐
                     │        Zerodha Kite Connect      │
                     │   Historical API + WebSocket WS  │
                     └────────────┬────────────────┬───┘
                                  │                │
                       REST API   │                │  WebSocket
                     (historical) │                │  (live ticks)
                                  ▼                ▼
                          ┌──────────────┐  ┌─────────────────────┐
                          │  fetcher.py  │  │    ws_ticker.py      │
                          │  (OHLCV)     │  │  (KiteTicker WS)    │
                          └──────┬───────┘  └──────────┬──────────┘
                                 │                     │
                                 ▼                     ▼
                          ┌─────────────────────────────────┐
                          │          database.py             │
                          │     (MySQL connection pool)      │
                          └──────────┬──────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
              stock_data         tick_data      fetch_log
            (historical)       (live ticks)   (audit trail)
```

---

## Project Structure

```
ai-quant-lab/
├── run.py                   ← Unified entry point (all commands go through here)
├── main.py                  ← Single-symbol historical fetch (called by run.py)
├── api.py                   ← FastAPI REST server
├── pipeline.py              ← Config-driven batch OHLCV pipeline
├── ws_ticker.py             ← WebSocket live tick streamer (KiteTicker)
├── fetcher.py               ← Kite API data fetching logic
├── auth.py                  ← Authentication & session management
├── database.py              ← MySQL connection pool, schema, all DB functions
├── log_config.py            ← Structured JSON logging
├── exceptions.py            ← Custom exception classes
├── requirements.txt
├── .env                     ← Your credentials (never commit this)
├── .env.example             ← Template for .env
└── configs/
    ├── daily_nifty50_fetch.yaml   ← All 50 NIFTY 50 stocks, daily candles
    ├── daily_fetch.yaml           ← Custom daily fetch template
    └── intraday_5min.yaml         ← 5-minute intraday fetch template
```

---

## Prerequisites & Installation

**Requirements:**
- Python 3.10+
- A Zerodha Kite Connect developer account and app
- MySQL database (the project uses AWS RDS by default)

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Create your `.env` file:**
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
KITE_API_KEY=your_api_key
KITE_API_SECRET=your_api_secret
KITE_ACCESS_TOKEN=               # filled in automatically after login

DB_HOST=your-rds-endpoint.rds.amazonaws.com
DB_PORT=3306
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_database_name
```

---

## Authentication Setup

Kite Connect requires a browser-based OAuth login every day. The `access_token` expires at midnight and must be refreshed each trading day.

### Step 1 — Get the login URL

```bash
python auth.py --login-url
```

Open the printed URL in your browser. Log in with your Zerodha credentials.

### Step 2 — Exchange the request token

After logging in, Kite redirects your browser to a URL like:
```
https://your-redirect-url/?request_token=abcXYZ123&action=login&status=success
```

Copy the `request_token` value and run:
```bash
python auth.py --generate-session abcXYZ123
```

This saves the `KITE_ACCESS_TOKEN` to your `.env` automatically. You only need to repeat this once per day.

> **Tip:** You can also authenticate via the API server — see [API Endpoints](#api-endpoints).

---

## Running the Project

All commands go through `run.py`. There are four modes:

```
python run.py server    → Start the FastAPI HTTP server
python run.py pipeline  → Run a YAML config batch pipeline
python run.py fetch     → Fetch a single symbol's historical data
python run.py ticker    → Stream live NIFTY 50 tick data
```

---

### 1. Start the API Server

```bash
python run.py server
```

Options:
```bash
python run.py server --host 0.0.0.0 --port 8000   # default
python run.py server --reload                       # auto-reload on code changes (dev)
```

Once running, open `http://localhost:8000/docs` in your browser for the interactive Swagger UI.

---

### 2. Collect Historical Data (Pipeline)

The pipeline reads a YAML config and fetches OHLCV candles for every listed symbol. It:
- Checks what's already in the database (gap detection)
- Only fetches the missing date ranges
- Uses up to 4 parallel threads
- Retries failed symbols with exponential backoff
- Writes permanently failed symbols to a dead-letter queue (`pipeline_errors` table)

**Fetch all 50 NIFTY 50 stocks (daily candles, 2021–2026):**
```bash
python run.py pipeline configs/daily_nifty50_fetch.yaml
```

**Preview what would be fetched without actually fetching:**
```bash
python run.py pipeline configs/daily_nifty50_fetch.yaml --dry-run
```

**Use more parallel threads:**
```bash
python run.py pipeline configs/daily_nifty50_fetch.yaml --workers 8
```

**Run a custom config:**
```bash
python run.py pipeline configs/intraday_5min.yaml
```

The pipeline is **idempotent** — running it twice won't re-fetch data that's already in the database.

---

### 3. Collect Historical Data (Single Symbol)

For fetching one symbol at a time:

```bash
python run.py fetch --symbol RELIANCE --from 2025-01-01 --to 2025-03-31
```

Options:
```bash
python run.py fetch \
  --symbol RELIANCE \
  --from 2025-01-01 \
  --to 2025-03-31 \
  --interval day \        # minute, 5minute, 15minute, day, etc.
  --exchange NSE \        # NSE, BSE, NFO, MCX
  --output reliance.csv \ # optional: save to CSV
  --oi                    # include open interest (F&O only)
```

**Examples:**
```bash
# Daily candles for INFY on BSE
python run.py fetch --symbol INFY --exchange BSE --from 2025-01-01 --to 2025-03-31 --interval day

# 5-minute candles for NIFTY 50 index
python run.py fetch --symbol "NIFTY 50" --from 2025-03-24 --to 2025-03-31 --interval 5minute

# NIFTY Futures (continuous) with open interest
python run.py fetch --symbol NIFTY --exchange NFO --from 2025-01-01 --to 2025-03-31 --oi --continuous
```

Data is stored in the `stock_data` table. If the data already exists in the DB, it skips the API call.

---

### 4. Stream Live Tick Data

This connects to Kite's WebSocket and streams real-time tick data for all symbols in the `watched_symbols` table (all 50 NIFTY 50 stocks by default). Data is stored in the `tick_data` table.

```bash
python run.py ticker
```

Options:
```bash
python run.py ticker \
  --mode full \    # full (OHLC + depth + OI), quote (OHLC), ltp (price only)
  --workers 2      # number of parallel DB-writer threads
```

**What happens when you run ticker:**
1. Ensures all DB tables exist
2. Creates a `WsTickerManager` with the specified mode and worker count
3. Registers automatic **market-hours scheduling**:
   - `08:30 IST Mon–Fri` → Refreshes the instrument master cache from Kite API
   - `09:15 IST Mon–Fri` → Automatically connects and starts streaming
   - `15:30 IST Mon–Fri` → Automatically disconnects
4. Also starts immediately if run manually outside market hours
5. Prints live stats every 5 seconds:
   ```
     stored=1,234  dropped=0  connected=True  queue=0
   ```
6. Press `Ctrl+C` to stop cleanly

**How tick data is collected (producer-consumer design):**
```
KiteTicker WebSocket thread     Bounded Queue        N DB-writer threads
(on_ticks callback)             (max 10,000)         (_store_loop)
  └─ put_nowait() ─────────────► [batch]  ──────────► executemany → tick_data
     (drops if full)             [batch]
```
- The WebSocket thread never blocks — if the DB is slow and the queue fills up, new ticks are dropped (counted in `ticks_dropped`)
- Worker threads batch up to 500 ticks per MySQL `INSERT` for efficiency
- Failed inserts retry up to 3 times with exponential backoff

---

## Database Schema

### `stock_data` — Historical OHLCV Candles

Stores historical candles fetched from the Kite REST API.

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `instrument_token` | INT | Kite instrument token |
| `symbol` | VARCHAR(50) | Trading symbol (e.g. RELIANCE) |
| `exchange` | VARCHAR(20) | Exchange (NSE, BSE, NFO, MCX) |
| `interval` | VARCHAR(20) | Candle interval (day, 5minute, etc.) |
| `timestamp` | DATETIME | Candle open time |
| `open` | DECIMAL(14,4) | Open price |
| `high` | DECIMAL(14,4) | High price |
| `low` | DECIMAL(14,4) | Low price |
| `close` | DECIMAL(14,4) | Close price |
| `volume` | BIGINT | Volume traded |
| `oi` | BIGINT | Open interest (F&O) |

---

### `tick_data` — Real-Time Live Ticks

Stores live WebSocket tick data captured during market hours.

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `instrument_token` | INT | Kite instrument token |
| `symbol` | VARCHAR(50) | Trading symbol |
| `exchange` | VARCHAR(20) | Exchange |
| `captured_at` | DATETIME(3) | When the tick was received (millisecond precision) |
| `last_price` | DECIMAL(14,4) | Last traded price |
| `open` | DECIMAL(14,4) | OHLC open (current session) |
| `high` | DECIMAL(14,4) | OHLC high |
| `low` | DECIMAL(14,4) | OHLC low |
| `close` | DECIMAL(14,4) | Previous session close |
| `volume` | BIGINT | Total volume traded today |
| `buy_quantity` | INT | Total pending buy quantity |
| `sell_quantity` | INT | Total pending sell quantity |
| `change_pct` | DECIMAL(10,4) | % change from previous close |
| `last_traded_quantity` | INT | Quantity in last trade |
| `avg_traded_price` | DECIMAL(14,4) | Volume-weighted average price |
| `oi` | BIGINT | Open interest |
| `oi_day_high` | BIGINT | OI day high |
| `oi_day_low` | BIGINT | OI day low |
| `last_trade_time` | DATETIME(3) | Time of last trade |
| `exchange_timestamp` | DATETIME(3) | Exchange-side timestamp |
| `depth` | JSON | Full bid/ask market depth (5 levels each) |

Indexes: `(symbol, captured_at)`, `(instrument_token, captured_at)`

---

### `watched_symbols` — Live Ticker Watch List

Symbols whose live data is collected by the WebSocket ticker. Pre-populated with all 50 NIFTY 50 stocks.

| Column | Type | Description |
|---|---|---|
| `id` | INT PK | Auto-increment |
| `symbol` | VARCHAR(50) | Trading symbol |
| `exchange` | VARCHAR(20) | Exchange (default: NSE) |
| `added_at` | DATETIME | When the symbol was added |

---

### `fetch_log` — Pipeline Audit Trail

Records every successful OHLCV fetch so the pipeline can detect gaps and avoid re-fetching.

| Column | Type | Description |
|---|---|---|
| `id` | INT PK | Auto-increment |
| `symbol` | VARCHAR(50) | Trading symbol |
| `exchange` | VARCHAR(20) | Exchange |
| `interval` | VARCHAR(20) | Candle interval |
| `from_date` | DATE | Fetch range start |
| `to_date` | DATE | Fetch range end |
| `rows_saved` | INT | Number of candles saved |
| `fetched_at` | DATETIME | When the fetch completed |

---

### `instrument_master` — Kite Instrument Cache

Caches the full Kite instrument list (refreshed at 08:30 IST daily) so WebSocket token resolution doesn't need a live API call on connect.

| Column | Type | Description |
|---|---|---|
| `instrument_token` | INT PK | Kite instrument token |
| `tradingsymbol` | VARCHAR(50) | Trading symbol |
| `exchange` | VARCHAR(20) | Exchange |
| `name` | VARCHAR(200) | Company name |
| `last_price` | DECIMAL(14,4) | Last known price |
| `expiry` | DATE | Expiry date (F&O) |
| `strike` | DECIMAL(14,4) | Strike price (options) |
| `lot_size` | INT | Lot size |
| `instrument_type` | VARCHAR(20) | EQ, FUT, CE, PE, etc. |
| `segment` | VARCHAR(20) | NSE, NFO, etc. |
| `updated_at` | DATETIME | Last cache refresh time |

---

### `order_updates` — Order Event Log

Stores order lifecycle events received from Kite's WebSocket (filled, rejected, cancelled, etc.).

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `order_id` | VARCHAR(50) | Kite order ID |
| `status` | VARCHAR(50) | Order status (COMPLETE, REJECTED, etc.) |
| `symbol` | VARCHAR(50) | Trading symbol |
| `payload` | JSON | Full raw order update data |
| `received_at` | DATETIME | When the event was received |

---

### `pipeline_errors` — Dead-Letter Queue

Symbols that failed all retry attempts in a pipeline run. Used for monitoring and manual retry.

| Column | Type | Description |
|---|---|---|
| `id` | INT PK | Auto-increment |
| `pipeline_name` | VARCHAR(100) | Pipeline that failed |
| `symbol` | VARCHAR(50) | Symbol that failed |
| `exchange` | VARCHAR(20) | Exchange |
| `interval` | VARCHAR(20) | Candle interval |
| `from_date` | DATE | Failed range start |
| `to_date` | DATE | Failed range end |
| `error_msg` | TEXT | Last exception message |
| `created_at` | DATETIME | When the failure was logged |

---

## API Endpoints

Start the server with `python run.py server`, then open `http://localhost:8000/docs` for the full interactive Swagger UI.

### Auth

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/auth/login-url` | Get the Kite login URL to open in a browser |
| `POST` | `/auth/session` | Exchange a `request_token` for an `access_token` |

**POST /auth/session body:**
```json
{ "request_token": "your_request_token_from_redirect" }
```

### Candles (Historical Data)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/candles` | Fetch OHLCV candles — checks DB first, calls API if missing |
| `GET` | `/candles/enriched` | Candles with derived columns (range, body size, is_bullish, daily_return_pct) |

**GET /candles query parameters:**
```
symbol=RELIANCE&from=2025-01-01&to=2025-03-31&interval=day&exchange=NSE
```

### Ticker (Watch List Management)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/ticker/watch` | List all watched symbols |
| `POST` | `/ticker/watch` | Add a symbol to the watch list |
| `DELETE` | `/ticker/watch/{symbol}` | Remove a symbol from the watch list |
| `POST` | `/ticker/start` | Manually start the WebSocket ticker |
| `POST` | `/ticker/stop` | Manually stop the WebSocket ticker |
| `GET` | `/ticker/status` | Check if ticker is running and scheduled times |

**POST /ticker/watch body:**
```json
{ "symbol": "RELIANCE", "exchange": "NSE" }
```

### Ticks (Live Data Query)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/ticks` | Query stored tick snapshots for a symbol |

**GET /ticks query parameters:**
```
symbol=RELIANCE&from=2025-04-10 09:15:00&to=2025-04-10 10:00:00&limit=3600
```

---

## Configuration Files (YAML)

Pipeline behavior is entirely controlled by YAML config files — no code changes needed for different stocks, dates, or intervals.

**`configs/daily_nifty50_fetch.yaml`** — All 50 NIFTY 50 stocks, daily candles:
```yaml
pipeline_name: "daily_nifty50_fetch"
description: "Fetch daily OHLCV data for Nifty 50 stocks"

data_source:
  api: "kite_connect"
  exchange: "NSE"
  instruments:
    - "RELIANCE"
    - "HDFCBANK"
    - "TCS"
    # ... all 50 NIFTY 50 symbols

date_range:
  start: "2021-01-01"
  end: "2026-04-10"
  interval: "day"

retry:
  max_attempts: 3
  backoff_seconds: 2
```

**Creating a custom config** for intraday data:
```yaml
pipeline_name: "my_intraday_fetch"

data_source:
  exchange: "NSE"
  instruments:
    - "RELIANCE"
    - "INFY"

date_range:
  start: "2025-01-01"
  end: "2025-03-31"
  interval: "5minute"   # minute, 3minute, 5minute, 15minute, 30minute, 60minute, day

retry:
  max_attempts: 3
  backoff_seconds: 2
```

**Supported intervals and their API limits:**

| Interval | Max days per API call |
|---|---|
| `minute` | 60 days |
| `3minute`, `5minute`, `10minute` | 100 days |
| `15minute`, `30minute` | 200 days |
| `60minute` | 400 days |
| `day` | 2000 days |

The pipeline automatically chunks large date ranges into API-safe segments.

---

## How Data Flows

### Historical Data Flow

```
1. python run.py pipeline configs/daily_nifty50_fetch.yaml
         │
         ▼
2. pipeline.py reads YAML config
   → validates symbols, dates, interval
         │
         ▼
3. For each symbol (up to 4 in parallel):
   find_missing_date_ranges()
   → queries fetch_log to find gaps not yet in stock_data
         │
         ▼
4. For each missing gap:
   fetch_historical_data() via Kite REST API
   → auto-chunked into API-safe segments
   → rate-limited (max 3 concurrent API calls)
         │
         ▼
5. save_to_db()
   → INSERT INTO stock_data
   → INSERT INTO fetch_log (marks the range as covered)
         │
         ▼
6. Failed symbols → push_to_dlq() → pipeline_errors table
```

### Live Tick Data Flow

```
1. python run.py ticker
         │
         ▼
2. WsTickerManager.start()
   → get_authenticated_kite()
   → KiteTicker WebSocket connects to Kite
         │
         ▼
3. on_connect callback:
   → get_watched_symbols() from DB (50 NIFTY 50 stocks)
   → resolve symbols → instrument_tokens
     (from instrument_master DB cache, or Kite API on cache miss)
   → ws.subscribe(tokens) with mode=full/quote/ltp
         │
         ▼
4. Market ticks arrive (sub-second):
   on_ticks() → put_nowait() → bounded Queue (max 10,000 batches)
   [if queue full → ticks_dropped counter increments]
         │
         ▼
5. N worker threads (_store_loop):
   → batch up to 500 ticks
   → _normalize_ticks() maps wire format → DB columns
   → save_ticks() → INSERT INTO tick_data (executemany)
   → retry up to 3x on MySQL errors
         │
         ▼
6. Automatic scheduling (APScheduler):
   08:30 IST → refresh instrument_master cache
   09:15 IST → start() WebSocket
   15:30 IST → stop() WebSocket + drain queue
```

---

## File Reference

| File | Purpose |
|---|---|
| `run.py` | Unified CLI entry point — dispatches to server/pipeline/fetch/ticker |
| `main.py` | Single-symbol historical fetch logic |
| `api.py` | FastAPI server with all REST endpoints |
| `pipeline.py` | Config-driven batch OHLCV fetcher with gap detection and retry |
| `ws_ticker.py` | WebSocket live tick streamer (`WsTickerManager`) |
| `fetcher.py` | Kite API calls, date chunking, instrument token resolution |
| `auth.py` | OAuth login URL, session generation, `get_authenticated_kite()` |
| `database.py` | MySQL connection pool, all table schemas, all DB read/write functions |
| `log_config.py` | Structured JSON logging with correlation IDs |
| `exceptions.py` | Custom exception types |
| `configs/` | YAML pipeline config files |
| `.env` | Credentials — Kite API keys and MySQL connection details |
