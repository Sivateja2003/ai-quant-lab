# Kite Simulator — Complete Guide

---

## What Is the Simulator?

The simulator replays real historical OHLCV data from the instructor's MySQL database
through a WebSocket server at **1 candle per second**. Students connect to it and store
the received ticks into their own local MySQL database — no Zerodha subscription needed.

```
Instructor's AWS RDS (cohort_main)
      └── stock_data table (50+ instruments, historical candles)
            └── python run.py simulator        ← Terminal 1 (server)
                  └── WebSocket  ws://localhost:8765
                        └── python run.py ticker --sim   ← Terminal 2 (client)
                              └── YOUR local MySQL → tick_data table
                                  (localhost:3306/trading)
```

**Key rules:**
- The simulator reads from `stock_data` (instructor's DB)
- The ticker client stores into `tick_data` (your own DB)
- Both must run at the same time in separate terminals
- The simulator stops automatically when all data is replayed

---

## Step 1 — Install Dependencies

Run once in your activated virtual environment:

```powershell
pip install websocket-client dbutils websockets python-dateutil
```

---

## Step 2 — Create Your Local Database

Run once in MySQL:

```sql
CREATE DATABASE trading;
```

The `tick_data` table is created **automatically** the first time the ticker client connects.

---

## Step 3 — Configure `.env`

Open `ai-quant-lab/.env` and set these fields:

```env
# Simulator WebSocket URL — localhost if running the simulator on your own machine
KITE_SIM_URL=ws://localhost:8765

# Simulator credentials — auto-generated on first run (see Step 4)
KITE_SIM_API_KEY=sim_d56eb8597d46d2ad2e3f45e4ee137476
KITE_SIM_ACCESS_TOKEN=sat_8181ab8a36b2c50b49250f3f42887acfef7f44410de6e697

# Your own local database where simulator ticks are stored
CLIENT_DB_URL=mysql://root:yourpassword@localhost:3306/trading
```

> If `CLIENT_DB_URL` is left blank, ticks fall back to `MYSQL_URL` (the shared instructor DB).

---

## Step 4 — Fetch Data Into stock_data First

The simulator can only replay data that exists in `stock_data`.
It **does not fetch from Zerodha on its own** — you must populate `stock_data` first.

### Option A — Fetch one symbol (daily candles)

```powershell
python run.py fetch --symbol RELIANCE --from "2025-01-01" --to "2026-04-15"
```

### Option B — Fetch one symbol (minute candles for one day)

```powershell
python run.py fetch --symbol RELIANCE --from "2026-04-15" --to "2026-04-15" --interval minute
```

This fetches 375 minute candles (09:15 to 15:29) for RELIANCE only.

### Option C — Fetch all 50 NIFTY stocks (daily candles, multi-year)

```powershell
python run.py pipeline configs/daily_nifty50_fetch.yaml --workers 4
```

Fetches daily candles for all 50 stocks from 2021 to 2026. Use this for long-term backtesting.

### Option D — Fetch all 50 NIFTY stocks (minute candles for one day)  ← Recommended

```powershell
python run.py pipeline configs/minute_nifty50_fetch.yaml --workers 4
```

Fetches 375 minute candles × 50 stocks = ~18,750 rows for a single trading day.
Use this to simulate a full market day with all 50 instruments.

> **Important:** If you only fetch RELIANCE, the simulator will only stream RELIANCE data.
> To stream all 50 stocks, you must fetch all 50 stocks first.

---

## Step 5 — Run the Simulator (Two Terminals)

### Terminal 1 — Start the simulator server

```powershell
python run.py simulator
```

**On first run**, credentials are printed — copy them to `.env`:

```
============================================================
  FIRST-RUN DEFAULT CREDENTIALS  (save these now!)
  api_key      = sim_d56eb8597d46d2ad2e3f45e4ee137476
  access_token = sat_8181ab8a36b2c50b49250f3f42887acfef7f44410de6e697
============================================================
```

**Expected output after startup:**

```
Management API started → http://0.0.0.0:8766/docs
Kite Simulator → ws://0.0.0.0:8765
Loaded 50 instruments, 18750 unique timestamps from MySQL.
Replay loop started — 18750 timestamps, 1 second per candle.
Starting from index 0 (2026-04-15T09:15:00) → (2026-04-15T15:29:00).
```

Keep this terminal running. Do not close it.

### Terminal 2 — Start the ticker client

Open a new terminal, activate the venv, then run:

```powershell
python run.py ticker --sim
```

**Expected output:**

```
Ticker started. Streaming from Kite Simulator → tick_data table.
  stored=50   dropped=0  connected=True  queue=0
  stored=100  dropped=0  connected=True  queue=0
  stored=150  dropped=0  connected=True  queue=0
  ...
```

`stored` increments by ~50 every second (one row per instrument per candle).

---

## Replay a Specific Time Range

Use `--from` and `--to` to replay a specific portion of data (24-hour clock):

```powershell
# Replay from market open only
python run.py simulator --from "2026-04-15 09:15:00"

# Replay a specific window (e.g. 1:15 PM to 3:17 PM)
python run.py simulator --from "2026-04-15 13:15:00" --to "2026-04-15 15:17:00"

# Replay a full date range
python run.py simulator --from "2026-04-01" --to "2026-04-15"
```

**Time format is 24-hour clock:**
- `09:15:00` = 9:15 AM (market open)
- `15:30:00` = 3:30 PM (market close)

The simulator logs exactly what it will replay:
```
Replay loop started — 375 timestamps to broadcast (index 0→374)
  2026-04-15T09:15:00  →  2026-04-15T15:29:00.
```

---

## Data Source Priority

The simulator checks tables in this order:

| Priority | Table | Contains |
|----------|-------|----------|
| 1st | `stock_data` | Historical OHLCV fetched via `fetch` / `pipeline` |
| 2nd | `tick_data` | Fallback — previously stored simulator ticks |
| Error | — | Both empty → exits with message |

Log output tells you which source was used:
```
Loading replay data from stock_data (50 instruments).
```
or
```
WARNING  stock_data is empty — trying tick_data as fallback.
Loading replay data from tick_data (50 instruments).
```

---

## Getting and Managing Simulator Credentials

Credentials are stored in `config/api_keys.json`.

### View existing credentials

```powershell
type config\api_keys.json
```

### Create a new key for a student (while simulator is running)

```powershell
curl -X POST http://localhost:8766/keys `
     -H "Content-Type: application/json" `
     -d "{\"label\": \"student1\"}"
```

Returns a new `api_key` and `access_token`. Give each student their own pair.

### Management API endpoints

| Method | URL | Description |
|--------|-----|-------------|
| `POST` | `/keys` | Create a new key |
| `GET` | `/keys` | List all keys |
| `POST` | `/keys/{api_key}/token` | Issue a new access token |
| `POST` | `/keys/{api_key}/revoke` | Revoke a key's tokens |
| `DELETE` | `/keys/{api_key}` | Delete a key |
| `GET` | `/status` | Active connections, replay progress |
| `GET` | `/instruments` | List all instruments being replayed |

Full interactive docs: `http://localhost:8766/docs`

---

## Stopping the Simulator

Press `Ctrl+C` in both terminals. The simulator also stops automatically when all
data in the selected time range has been replayed.

### If ports remain occupied after a crash

```powershell
# Find the process holding port 8765
netstat -ano | findstr :8765

# Kill it (replace 6708 with the actual PID from the output above)
taskkill /PID 6708 /F
```

---

## Verifying Data Was Stored

Connect to your local MySQL (`trading` database) and run:

```sql
USE trading;

-- Total ticks stored
SELECT COUNT(*) AS total FROM tick_data;

-- How many ticks per symbol (should be equal for all 50 stocks)
SELECT symbol, COUNT(*) AS ticks
FROM tick_data
GROUP BY symbol
ORDER BY ticks DESC;

-- Latest ticks
SELECT symbol, captured_at, last_price, open, high, low, close, volume
FROM tick_data
ORDER BY captured_at DESC
LIMIT 20;

-- Check a specific symbol
SELECT captured_at, last_price, volume
FROM tick_data
WHERE symbol = 'RELIANCE'
ORDER BY captured_at ASC;
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `[WinError 10061] No connection could be made` | Simulator server not running | Start `python run.py simulator` in Terminal 1 first |
| `Invalid api_key or access_token` | Wrong credentials in `.env` | Check `config/api_keys.json` and copy correct values to `.env` |
| `Ensure ping_interval > ping_timeout` | Old websocket-client config | Already fixed in code |
| `[Errno 10048] Only one usage of each socket address` | Previous simulator still running | Run `taskkill /PID <PID> /F` to free the port |
| `stored=0` after connecting | Auth failed or server not running | Check both terminals are running and credentials match |
| Only RELIANCE data stored | Only RELIANCE was fetched | Run `pipeline configs/minute_nifty50_fetch.yaml` to fetch all 50 stocks |
| `stock_data is empty` | No data fetched yet | Run `fetch` or `pipeline` command first |

---

## All Commands Reference

| Command | What It Does | Where Data Goes |
|---------|-------------|-----------------|
| `python run.py fetch --symbol RELIANCE --from "2026-04-15" --to "2026-04-15" --interval minute` | Fetch minute candles for one stock | `stock_data` (cohort DB) |
| `python run.py fetch --symbol RELIANCE --from "2025-01-01" --to "2026-04-15"` | Fetch daily candles for one stock | `stock_data` (cohort DB) |
| `python run.py pipeline configs/daily_nifty50_fetch.yaml --workers 4` | Fetch daily candles for all 50 stocks | `stock_data` (cohort DB) |
| `python run.py pipeline configs/minute_nifty50_fetch.yaml --workers 4` | Fetch minute candles for all 50 stocks | `stock_data` (cohort DB) |
| `python run.py simulator` | Start simulator WebSocket server | Reads from `stock_data` |
| `python run.py simulator --from "2026-04-15 09:15:00"` | Start simulator from a specific time | Reads from `stock_data` |
| `python run.py simulator --from "2026-04-15 09:15:00" --to "2026-04-15 15:30:00"` | Replay a specific time window | Reads from `stock_data` |
| `python run.py ticker --sim` | Connect to simulator, store ticks | `tick_data` (your local DB) |
| `python run.py ticker` | Live Zerodha data (market hours only) | `tick_data` (cohort DB) |
| `python run.py server` | Start FastAPI HTTP server | — |

Each mode is fully independent — running the simulator does not affect live Zerodha data collection or historical fetching.
