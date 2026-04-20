from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests

import upstox_ws
from state_store import StateStore


REQUEST_TIMEOUT = 12

NIFTY_INSTRUMENT = "NSE_INDEX|Nifty 50"
SENSEX_INSTRUMENT = "BSE_INDEX|SENSEX"

NIFTY_EXPIRY = os.getenv("NIFTY_EXPIRY", "2026-04-21")
SENSEX_EXPIRY = os.getenv("SENSEX_EXPIRY", "2026-04-24")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")


def log(msg: str) -> None:
    print(f"[COLLECTOR] {msg}", flush=True)


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


SESSION = get_session()


def get_response_json(
    url: str,
    *,
    token: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = SESSION.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_chain(token: str, instrument: str, expiry: str) -> tuple[pd.DataFrame, list[str], float | None]:
    data = get_response_json(
        "https://api.upstox.com/v2/option/chain",
        token=token,
        params={"instrument_key": instrument, "expiry_date": expiry},
    )

    raw_rows = data.get("data", [])
    rows: list[dict[str, Any]] = []
    keys: list[str] = []
    spot: float | None = None

    for item in raw_rows:
        strike = safe_float(item.get("strike_price"))
        row_spot = safe_float(item.get("underlying_spot_price"))
        if row_spot is not None:
            spot = row_spot

        pcr = safe_float(item.get("pcr"))

        call = item.get("call_options") or {}
        put = item.get("put_options") or {}

        ce_md = call.get("market_data") or {}
        pe_md = put.get("market_data") or {}

        ce_gk = call.get("option_greeks") or {}
        pe_gk = put.get("option_greeks") or {}

        ce_key = call.get("instrument_key")
        pe_key = put.get("instrument_key")

        if ce_key:
            keys.append(ce_key)
        if pe_key:
            keys.append(pe_key)

        ce_oi = safe_float(ce_md.get("oi"))
        pe_oi = safe_float(pe_md.get("oi"))
        ce_prev_oi = safe_float(ce_md.get("prev_oi"))
        pe_prev_oi = safe_float(pe_md.get("prev_oi"))

        rows.append(
            {
                "STRIKE": strike,
                "SPOT": row_spot,
                "PCR": pcr,
                "CE_KEY": ce_key,
                "CE_OI": ce_oi,
                "CE_PREV_OI": ce_prev_oi,
                "CE_CHG_OI": None if ce_oi is None or ce_prev_oi is None else ce_oi - ce_prev_oi,
                "CE_VOLUME": safe_float(ce_md.get("volume")),
                "CE_IV": safe_float(ce_gk.get("iv")),
                "CE_DELTA": safe_float(ce_gk.get("delta")),
                "CE_GAMMA": safe_float(ce_gk.get("gamma")),
                "CE_THETA": safe_float(ce_gk.get("theta")),
                "CE_VEGA": safe_float(ce_gk.get("vega")),
                "CE_LTP": safe_float(ce_md.get("ltp")),
                "CE_BID": safe_float(ce_md.get("bid_price")),
                "CE_ASK": safe_float(ce_md.get("ask_price")),
                "CE_BID_QTY": safe_float(ce_md.get("bid_qty")),
                "CE_ASK_QTY": safe_float(ce_md.get("ask_qty")),
                "PE_KEY": pe_key,
                "PE_OI": pe_oi,
                "PE_PREV_OI": pe_prev_oi,
                "PE_CHG_OI": None if pe_oi is None or pe_prev_oi is None else pe_oi - pe_prev_oi,
                "PE_VOLUME": safe_float(pe_md.get("volume")),
                "PE_IV": safe_float(pe_gk.get("iv")),
                "PE_DELTA": safe_float(pe_gk.get("delta")),
                "PE_GAMMA": safe_float(pe_gk.get("gamma")),
                "PE_THETA": safe_float(pe_gk.get("theta")),
                "PE_VEGA": safe_float(pe_gk.get("vega")),
                "PE_LTP": safe_float(pe_md.get("ltp")),
                "PE_BID": safe_float(pe_md.get("bid_price")),
                "PE_ASK": safe_float(pe_md.get("ask_price")),
                "PE_BID_QTY": safe_float(pe_md.get("bid_qty")),
                "PE_ASK_QTY": safe_float(pe_md.get("ask_qty")),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("STRIKE").reset_index(drop=True)

    return df, sorted(set(keys)), spot


class LiveChain:
    def __init__(self, symbol: str, instrument: str, expiry: str) -> None:
        self.symbol = symbol
        self.instrument = instrument
        self.expiry = expiry

        self.df = pd.DataFrame()
        self.ce_key_to_idx: dict[str, int] = {}
        self.pe_key_to_idx: dict[str, int] = {}
        self.spot: float | None = None
        self.last_bootstrap_ts: float = 0.0

    def bootstrap(self, token: str) -> list[str]:
        df, keys, spot = fetch_chain(token, self.instrument, self.expiry)
        if df.empty:
            raise RuntimeError(f"{self.symbol}: empty chain response")

        self.df = df
        self.spot = spot
        self.ce_key_to_idx = {}
        self.pe_key_to_idx = {}

        for idx, row in df.iterrows():
            ce_key = row.get("CE_KEY")
            pe_key = row.get("PE_KEY")
            if ce_key:
                self.ce_key_to_idx[ce_key] = idx
            if pe_key:
                self.pe_key_to_idx[pe_key] = idx

        self.last_bootstrap_ts = time.time()
        return keys

    def apply_ticks(self) -> None:
        if self.df.empty:
            return

        ticks = upstox_ws.ticks
        for instrument_key, tick in ticks.items():
            if instrument_key in self.ce_key_to_idx:
                idx = self.ce_key_to_idx[instrument_key]
                if tick.get("last_price") is not None:
                    self.df.at[idx, "CE_LTP"] = tick["last_price"]
                if tick.get("volume") is not None:
                    self.df.at[idx, "CE_VOLUME"] = tick["volume"]
                if tick.get("oi") is not None:
                    self.df.at[idx, "CE_OI"] = tick["oi"]
                    prev_oi = self.df.at[idx, "CE_PREV_OI"]
                    self.df.at[idx, "CE_CHG_OI"] = None if pd.isna(prev_oi) else tick["oi"] - prev_oi

            elif instrument_key in self.pe_key_to_idx:
                idx = self.pe_key_to_idx[instrument_key]
                if tick.get("last_price") is not None:
                    self.df.at[idx, "PE_LTP"] = tick["last_price"]
                if tick.get("volume") is not None:
                    self.df.at[idx, "PE_VOLUME"] = tick["volume"]
                if tick.get("oi") is not None:
                    self.df.at[idx, "PE_OI"] = tick["oi"]
                    prev_oi = self.df.at[idx, "PE_PREV_OI"]
                    self.df.at[idx, "PE_CHG_OI"] = None if pd.isna(prev_oi) else tick["oi"] - prev_oi

    def to_payload(self) -> dict[str, Any]:
        if self.df.empty:
            return {
                "symbol": self.symbol,
                "timestamp": now_str(),
                "spot": self.spot,
                "pcr": None,
                "rows": [],
            }

        total_ce_oi = self.df["CE_OI"].fillna(0).sum()
        total_pe_oi = self.df["PE_OI"].fillna(0).sum()
        pcr = (total_pe_oi / total_ce_oi) if total_ce_oi else None

        return {
            "symbol": self.symbol,
            "timestamp": now_str(),
            "spot": self.spot,
            "pcr": pcr,
            "expiry": self.expiry,
            "rows": self.df.where(pd.notnull(self.df), None).to_dict(orient="records"),
        }


def main() -> None:
    if not UPSTOX_ACCESS_TOKEN:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing")

    store = StateStore()

    nifty = LiveChain("NIFTY", NIFTY_INSTRUMENT, NIFTY_EXPIRY)
    sensex = LiveChain("SENSEX", SENSEX_INSTRUMENT, SENSEX_EXPIRY)

    log("Bootstrapping option chains...")
    nifty_keys = nifty.bootstrap(UPSTOX_ACCESS_TOKEN)
    sensex_keys = sensex.bootstrap(UPSTOX_ACCESS_TOKEN)

    all_keys = sorted(set(nifty_keys + sensex_keys))

    log(f"Starting websocket for {len(all_keys)} instruments")
    upstox_ws.start_ws(UPSTOX_ACCESS_TOKEN)
    time.sleep(1.0)
    upstox_ws.subscribe(all_keys)

    last_bootstrap = 0.0
    last_publish = 0.0

    while True:
        try:
            now = time.time()

            # periodic correction from REST
            if now - last_bootstrap >= 30:
                nifty.bootstrap(UPSTOX_ACCESS_TOKEN)
                sensex.bootstrap(UPSTOX_ACCESS_TOKEN)
                upstox_ws.subscribe(sorted(set(nifty.ce_key_to_idx.keys()) | set(nifty.pe_key_to_idx.keys()) | set(sensex.ce_key_to_idx.keys()) | set(sensex.pe_key_to_idx.keys())))
                last_bootstrap = now
                log("REST correction refresh complete")

            nifty.apply_ticks()
            sensex.apply_ticks()

            # publish latest state every second
            if now - last_publish >= 1:
                store.set_latest_chain("NIFTY", nifty.to_payload())
                store.set_latest_chain("SENSEX", sensex.to_payload())
                store.set_health(
                    {
                        "status": "ok",
                        "timestamp": now_str(),
                        "connected": upstox_ws.connected,
                    }
                )
                last_publish = now
                log("Published latest NIFTY/SENSEX")

            time.sleep(0.25)

        except Exception as exc:
            log(f"Loop error: {exc}")
            time.sleep(2)


if __name__ == "__main__":
    main()
