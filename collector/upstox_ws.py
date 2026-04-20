from __future__ import annotations

import json
import threading
import time
from typing import Any

import websocket


ws_app: websocket.WebSocketApp | None = None
ws_thread: threading.Thread | None = None

connected = False
connecting = False

ticks: dict[str, dict[str, Any]] = {}
subscribed_keys: set[str] = set()

_state_lock = threading.Lock()


def _log(msg: str) -> None:
    print(f"[UPSTOX_WS] {msg}", flush=True)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_tick_payload(feed_value: dict[str, Any]) -> dict[str, Any]:
    ltpc = feed_value.get("ltpc", {}) or {}
    oi_block = feed_value.get("oi", {}) or {}

    return {
        "last_price": _safe_float(ltpc.get("ltp")),
        "volume": _safe_float(ltpc.get("volume")),
        "oi": _safe_float(oi_block.get("oi")),
        "raw": feed_value,
        "updated_at": time.time(),
    }


def on_message(ws: websocket.WebSocketApp, message: str) -> None:
    global ticks

    try:
        data = json.loads(message)
    except Exception as exc:
        _log(f"JSON decode error: {exc}")
        return

    feeds = data.get("feeds")
    if not isinstance(feeds, dict):
        return

    with _state_lock:
        for instrument_key, feed_value in feeds.items():
            if not isinstance(feed_value, dict):
                continue

            new_tick = _extract_tick_payload(feed_value)
            prev = ticks.get(instrument_key, {})

            ticks[instrument_key] = {
                "last_price": new_tick["last_price"] if new_tick["last_price"] is not None else prev.get("last_price"),
                "volume": new_tick["volume"] if new_tick["volume"] is not None else prev.get("volume"),
                "oi": new_tick["oi"] if new_tick["oi"] is not None else prev.get("oi"),
                "raw": new_tick["raw"],
                "updated_at": new_tick["updated_at"],
            }


def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    global connected, connecting
    connected = False
    connecting = False
    _log(f"ERROR: {error}")


def on_close(ws: websocket.WebSocketApp, close_status_code: int | None, close_msg: str | None) -> None:
    global connected, connecting
    connected = False
    connecting = False
    _log(f"CLOSED: code={close_status_code}, msg={close_msg}")


def _send_subscribe(keys: list[str]) -> None:
    global ws_app

    if not ws_app:
        raise RuntimeError("WebSocket not initialized")

    payload = {
        "guid": "option-chain-live",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": keys,
        },
    }
    ws_app.send(json.dumps(payload))


def on_open(ws: websocket.WebSocketApp) -> None:
    global connected, connecting
    connected = True
    connecting = False
    _log("CONNECTED")

    if subscribed_keys:
        try:
            _send_subscribe(sorted(subscribed_keys))
            _log(f"Re-subscribed {len(subscribed_keys)} instruments")
        except Exception as exc:
            _log(f"Re-subscribe failed: {exc}")


def start_ws(token: str) -> None:
    global ws_app, ws_thread, connected, connecting

    if connected or connecting:
        return

    connecting = True

    url = "wss://api.upstox.com/v2/feed/market-data-feed"
    headers = [
        f"Authorization: Bearer {token}",
        "Accept: */*",
    ]

    ws_app = websocket.WebSocketApp(
        url,
        header=headers,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    def _runner() -> None:
        global connected, connecting
        while True:
            try:
                ws_app.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                    skip_utf8_validation=True,
                )
            except Exception as exc:
                _log(f"run_forever exception: {exc}")

            connected = False
            connecting = False
            time.sleep(3)

    ws_thread = threading.Thread(target=_runner, daemon=True)
    ws_thread.start()

    for _ in range(20):
        if connected:
            return
        time.sleep(0.25)


def subscribe(keys: list[str]) -> None:
    global subscribed_keys

    clean_keys = sorted({k for k in keys if k})
    if not clean_keys:
        return

    with _state_lock:
        new_keys = [k for k in clean_keys if k not in subscribed_keys]
        subscribed_keys.update(clean_keys)

    if not new_keys:
        return

    if not connected or not ws_app:
        _log("Subscribe deferred; socket not connected yet")
        return

    try:
        _send_subscribe(new_keys)
        _log(f"Subscribed {len(new_keys)} instruments")
    except Exception as exc:
        _log(f"Subscribe failed: {exc}")
