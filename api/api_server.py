from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import os


REDIS_URL = os.getenv("REDIS_URL", "")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is missing")

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=5,
    socket_connect_timeout=5,
)

app = FastAPI(title="Option Chain API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_json(key: str) -> dict[str, Any] | None:
    raw = redis_client.get(key)
    if not raw:
        return None
    return json.loads(raw)


@app.get("/health")
def health() -> dict[str, Any]:
    collector = get_json("health:collector")
    return {
        "status": "ok",
        "api_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "collector": collector,
    }


@app.get("/latest/nifty")
def latest_nifty() -> dict[str, Any]:
    data = get_json("latest:nifty")
    if not data:
        raise HTTPException(status_code=404, detail="No NIFTY data available")
    return data


@app.get("/latest/sensex")
def latest_sensex() -> dict[str, Any]:
    data = get_json("latest:sensex")
    if not data:
        raise HTTPException(status_code=404, detail="No SENSEX data available")
    return data


@app.get("/latest/{symbol}")
def latest_symbol(symbol: str) -> dict[str, Any]:
    symbol_key = symbol.strip().lower()
    if symbol_key not in {"nifty", "sensex"}:
        raise HTTPException(status_code=400, detail="Supported symbols: nifty, sensex")

    data = get_json(f"latest:{symbol_key}")
    if not data:
        raise HTTPException(status_code=404, detail=f"No {symbol.upper()} data available")
    return data
