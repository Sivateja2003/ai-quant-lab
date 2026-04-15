"""
management_api.py — REST API for managing Kite Simulator API keys
==================================================================

Run alongside kite_simulator.py to create/manage API keys.

Endpoints
---------
POST   /keys                  Create a new api_key + access_token
GET    /keys                  List all keys (tokens hidden)
POST   /keys/{api_key}/token  Issue an additional access_token for a key
POST   /keys/{api_key}/revoke Deactivate a key immediately
DELETE /keys/{api_key}        Permanently delete a key

GET    /connect               Return your personal connect_url (pass api_key in header)
GET    /status                Server health
GET    /instruments           List simulated instruments + base prices

Run
---
    uvicorn simulator.management_api:app --host 0.0.0.0 --port 8766 --reload

Then open http://localhost:8766/docs
"""

from __future__ import annotations

import os
from datetime import datetime

import pytz
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

import auth as auth_store

# ---------------------------------------------------------------------------
# Instrument list — mirrors kite_simulator.INSTRUMENTS
# ---------------------------------------------------------------------------

INSTRUMENTS = [
    {"token": 738561,  "symbol": "RELIANCE",   "exchange": "NSE", "base_price": 2950.0},
    {"token": 341249,  "symbol": "HDFCBANK",   "exchange": "NSE", "base_price": 1680.0},
    {"token": 408065,  "symbol": "INFY",       "exchange": "NSE", "base_price": 1450.0},
    {"token": 2953217, "symbol": "TCS",        "exchange": "NSE", "base_price": 3350.0},
    {"token": 1270529, "symbol": "ICICIBANK",  "exchange": "NSE", "base_price": 1050.0},
    {"token": 969473,  "symbol": "SBIN",       "exchange": "NSE", "base_price":  800.0},
    {"token": 315393,  "symbol": "WIPRO",      "exchange": "NSE", "base_price":  460.0},
    {"token": 1195009, "symbol": "TATAMOTORS", "exchange": "NSE", "base_price":  920.0},
    {"token": 134657,  "symbol": "AXISBANK",   "exchange": "NSE", "base_price": 1100.0},
    {"token": 779521,  "symbol": "SUNPHARMA",  "exchange": "NSE", "base_price": 1600.0},
]

SIM_HOST = os.getenv("SIM_HOST", "localhost")
SIM_PORT = os.getenv("SIM_PORT", "8765")

app = FastAPI(
    title="Kite Simulator — Management API",
    description=(
        "Create and manage API keys for the Kite WebSocket Simulator.\n\n"
        "**Quick start:**\n"
        "1. `POST /keys` with a label to get your credentials\n"
        "2. Copy the `connect_url` from the response\n"
        "3. Add the credentials to `.env` as `KITE_SIM_API_KEY` and `KITE_SIM_ACCESS_TOKEN`\n\n"
        "Ticks flow during Indian market hours (Mon–Fri 09:15–15:30 IST).\n"
        "Use `--force-open` flag on the simulator for 24/7 testing."
    ),
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CreateKeyRequest(BaseModel):
    label: str = ""
    model_config = {"json_schema_extra": {"example": {"label": "my-client"}}}


class KeyCreatedResponse(BaseModel):
    api_key:       str
    access_token:  str
    label:         str
    created_at:    str
    active:        bool
    connect_url:   str
    note:          str


class KeySummary(BaseModel):
    api_key:     str
    label:       str
    token_count: int
    created_at:  str
    active:      bool


class NewTokenResponse(BaseModel):
    api_key:      str
    access_token: str
    connect_url:  str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect_url(api_key: str, access_token: str) -> str:
    return f"ws://{SIM_HOST}:{SIM_PORT}?api_key={api_key}&access_token={access_token}"


# ---------------------------------------------------------------------------
# Key management
# ---------------------------------------------------------------------------

@app.post("/keys", response_model=KeyCreatedResponse, tags=["Keys"],
          summary="Create a new API key + access token")
def create_key(body: CreateKeyRequest):
    """
    Creates a new `api_key` and one `access_token`.

    Copy the credentials into `.env`:
        KITE_SIM_API_KEY=<api_key>
        KITE_SIM_ACCESS_TOKEN=<access_token>

    **The `access_token` is shown only once.**
    """
    record = auth_store.create_key(label=body.label)
    token  = record["access_tokens"][0]
    url    = _connect_url(record["api_key"], token)

    return KeyCreatedResponse(
        api_key      = record["api_key"],
        access_token = token,
        label        = record["label"],
        created_at   = record["created_at"],
        active       = record["active"],
        connect_url  = url,
        note=(
            "Save access_token now — it will not be shown again. "
            "Add to .env: KITE_SIM_API_KEY=" + record["api_key"] +
            "  KITE_SIM_ACCESS_TOKEN=" + token
        ),
    )


@app.get("/keys", response_model=list[KeySummary], tags=["Keys"],
         summary="List all API keys")
def list_keys():
    """Returns all keys. Access tokens are never returned here."""
    return auth_store.list_keys()


@app.post("/keys/{api_key}/token", response_model=NewTokenResponse, tags=["Keys"],
          summary="Issue an additional access token")
def issue_token(api_key: str):
    """Generates a new `access_token` for an existing key."""
    token = auth_store.issue_token(api_key)
    if token is None:
        raise HTTPException(status_code=404,
                            detail=f"api_key '{api_key}' not found or inactive.")
    return NewTokenResponse(
        api_key      = api_key,
        access_token = token,
        connect_url  = _connect_url(api_key, token),
    )


@app.post("/keys/{api_key}/revoke", tags=["Keys"],
          summary="Revoke (deactivate) a key")
def revoke_key(api_key: str):
    """Deactivates the key. Active connections will be dropped."""
    if not auth_store.revoke_key(api_key):
        raise HTTPException(status_code=404, detail=f"api_key '{api_key}' not found.")
    return {"api_key": api_key, "status": "revoked"}


@app.delete("/keys/{api_key}", tags=["Keys"],
            summary="Permanently delete a key")
def delete_key(api_key: str):
    """Removes the key and all its tokens permanently."""
    if not auth_store.delete_key(api_key):
        raise HTTPException(status_code=404, detail=f"api_key '{api_key}' not found.")
    return {"api_key": api_key, "status": "deleted"}


# ---------------------------------------------------------------------------
# Connect helper
# ---------------------------------------------------------------------------

@app.get("/connect", tags=["Connect"], summary="Get your personal connect URL")
def get_connect_url(x_api_key: str = Header(..., description="Your api_key")):
    """
    Returns a ready-to-use `connect_url` for your key.
    Issue a token first via `POST /keys/{api_key}/token` then pass here.
    """
    keys = {k["api_key"]: k for k in auth_store.list_keys()}
    if x_api_key not in keys:
        raise HTTPException(status_code=404, detail="api_key not found.")
    if not keys[x_api_key]["active"]:
        raise HTTPException(status_code=403, detail="api_key is revoked.")

    return {
        "api_key":               x_api_key,
        "ws_host":               SIM_HOST,
        "ws_port":               int(SIM_PORT),
        "connect_url_template":  f"ws://{SIM_HOST}:{SIM_PORT}?api_key={x_api_key}&access_token=<your_token>",
        "hint": "Replace <your_token> with a token from POST /keys/{api_key}/token",
    }


# ---------------------------------------------------------------------------
# Status / instruments
# ---------------------------------------------------------------------------

@app.get("/status", tags=["Info"], summary="Health check")
def status():
    """Returns server health and current IST time."""
    IST = pytz.timezone("Asia/Kolkata")
    now = datetime.now(IST)

    is_weekday = now.weekday() < 5
    open_time  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    market_open = is_weekday and open_time <= now < close_time

    return {
        "status":          "ok",
        "server_time_ist": now.strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_open":     market_open,
        "ws_endpoint":     f"ws://{SIM_HOST}:{SIM_PORT}",
        "mgmt_docs":       f"http://{SIM_HOST}:8766/docs",
    }


@app.get("/instruments", tags=["Info"], summary="List simulated instruments")
def list_instruments():
    """Returns the 10 simulated NSE instruments with their base prices."""
    return INSTRUMENTS


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")
