from __future__ import annotations

import json
import os
from typing import Any

import redis


REDIS_URL = os.getenv("REDIS_URL", "")


class StateStore:
    def __init__(self) -> None:
        if not REDIS_URL:
            raise RuntimeError("REDIS_URL is missing")

        self.client = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

    def set_json(self, key: str, value: dict[str, Any], ex: int | None = None) -> None:
        self.client.set(key, json.dumps(value), ex=ex)

    def get_json(self, key: str) -> dict[str, Any] | None:
        raw = self.client.get(key)
        if not raw:
            return None
        return json.loads(raw)

    def set_latest_chain(self, symbol: str, payload: dict[str, Any]) -> None:
        self.set_json(f"latest:{symbol.lower()}", payload)

    def get_latest_chain(self, symbol: str) -> dict[str, Any] | None:
        return self.get_json(f"latest:{symbol.lower()}")

    def set_health(self, payload: dict[str, Any]) -> None:
        self.set_json("health:collector", payload, ex=120)

    def get_health(self) -> dict[str, Any] | None:
        return self.get_json("health:collector")
