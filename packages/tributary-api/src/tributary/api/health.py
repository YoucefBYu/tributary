"""`/livez` and `/readyz` bodies (`observability/v0` 2, component api). Unauthenticated (D11), fixed codes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .auth.jwks import KeySet
from .publisher import ChannelsPublisher

LIVE: dict[str, Any] = {"v": 0, "status": "alive", "component": "api"}
DB_CACHE_S = 2.0
DB_TIMEOUT_S = 2.0


class Readiness:
    """The database probe is cached for 2 s and bounded by 2 s, so an unauthenticated caller cannot turn
    `/readyz` into a database load generator."""

    def __init__(self, ping: Callable[[], Awaitable[None]], keys: KeySet, publisher: ChannelsPublisher) -> None:
        self._ping = ping
        self._keys = keys
        self._publisher = publisher
        self._at = -1e9
        self._ok = False
        self._lock = asyncio.Lock()

    async def _database_ok(self) -> bool:
        async with self._lock:
            if time.monotonic() - self._at < DB_CACHE_S:
                return self._ok
            try:
                await asyncio.wait_for(self._ping(), timeout=DB_TIMEOUT_S)
                self._ok = True
            except Exception:
                self._ok = False
            self._at = time.monotonic()
            return self._ok

    async def body(self) -> tuple[int, dict[str, Any]]:
        db_ok = await self._database_ok()
        keys_ok = self._keys.usable
        checks: list[dict[str, Any]] = [
            {"name": "database", "ok": db_ok} | ({} if db_ok else {"reason": "unreachable"}),
            {"name": "oidc_keys", "ok": keys_ok} | ({} if keys_ok else {"reason": "keys_unavailable"}),
        ]
        ready = db_ok and keys_ok
        body: dict[str, Any] = {
            "v": 0,
            "component": "api",
            "status": "ready" if ready else "not_ready",
            "checks": checks,
        }
        if self._publisher.degraded:
            body["degraded"] = ["channels_publisher_unavailable"]
        return (200 if ready else 503), body
