"""The key-set wrapper (ADR-009 D5, D13): a snapshot of the issuer's signing keys that one background task
replaces atomically, so **no request ever waits on the network**, except one that presents an unknown `kid`
(and that wait is bounded and rate limited).

Why not `jwt.PyJWKClient`: it fetches with blocking `urllib` and a 30 s timeout under a lock, keeps no
stale keys, and starts its cooldown only after a success, so a failing IdP is retried on every request
(`security-reviewer`, PyJWT 2.15.1). Here:

* the fetch runs in a worker thread (`asyncio.to_thread`) with its own 5 s deadline;
* a failed fetch never replaces the snapshot, and failures back off exponentially with full jitter;
* an unknown `kid` triggers at most one refresh per 60 s, **counting failed attempts**, and none while the
  key set is in backoff;
* a snapshot older than `max_stale_s` is not used at all (every token is refused).

PyJWT is used here only to parse JWKs (`jwt.PyJWK`). The clock, the random source, the sleep and the
fetch function are injected, so every rule is unit-tested without a network or a real clock.
"""

from __future__ import annotations

import asyncio
import contextlib
import http.client
import json
import random
import ssl
import time
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import jwt

MAX_BODY_BYTES = 256 * 1024
MAX_KEYS = 32
MIN_RSA_BITS = 2048
UNKNOWN_KID_MIN_INTERVAL_S = 60.0
BACKOFF_BASE_S = 5.0
BACKOFF_CAP_S = 300.0
#: The background loop sleeps in slices so a backoff shorter than the proactive schedule is honoured.
SLEEP_SLICE_S = 5.0

#: ADR-009 D3: the allow-list, and the key type each algorithm needs.
ALLOWED_ALGORITHMS: Mapping[str, str] = {"RS256": "RSA", "PS256": "RSA", "ES256": "EC"}


class JwksError(Exception):
    """A fetch or parse failure. The message is a fixed sentence (it may reach a log line)."""


class Fetch(Protocol):
    def __call__(self) -> bytes: ...


@dataclass(frozen=True, slots=True)
class KeyEntry:
    """One usable signing key."""

    kid: str
    kty: str
    key: Any  # a `cryptography` public key object; opaque to callers
    jwk_alg: str | None

    def permits(self, alg: str) -> bool:
        """`kty` must match the algorithm, and a JWK that names an `alg` only verifies that one (D3)."""
        return ALLOWED_ALGORITHMS.get(alg) == self.kty and self.jwk_alg in (None, alg)


@dataclass(frozen=True, slots=True)
class Snapshot:
    keys: Mapping[str, KeyEntry]
    fetched_at: float


def parse_jwks(raw: bytes) -> dict[str, KeyEntry]:
    """Parse a JWKS document into usable keys. Anything oversized, duplicated or empty fails the fetch."""
    if len(raw) > MAX_BODY_BYTES:
        raise JwksError("key set response too large")
    try:
        doc = json.loads(raw)
    except ValueError:
        raise JwksError("key set is not JSON") from None
    keys = doc.get("keys") if isinstance(doc, dict) else None
    if not isinstance(keys, list):
        raise JwksError("key set has no keys array")
    if len(keys) > MAX_KEYS:
        raise JwksError("key set has too many keys")
    out: dict[str, KeyEntry] = {}
    for jwk in keys:
        entry = _parse_one(jwk)
        if entry is None:
            continue
        if entry.kid in out:
            raise JwksError("key set repeats a kid")
        out[entry.kid] = entry
    if not out:
        raise JwksError("key set has no usable key")
    return out


def _parse_one(jwk: object) -> KeyEntry | None:
    if not isinstance(jwk, dict):
        return None
    kid, kty = jwk.get("kid"), jwk.get("kty")
    if not isinstance(kid, str) or not kid or len(kid) > 256 or kty not in ("RSA", "EC"):
        return None
    if jwk.get("use") not in (None, "sig"):
        return None
    jwk_alg = jwk.get("alg")
    if jwk_alg is not None and (not isinstance(jwk_alg, str) or ALLOWED_ALGORITHMS.get(jwk_alg) != kty):
        return None
    if kty == "EC" and jwk.get("crv") != "P-256":
        return None
    try:
        key = jwt.PyJWK(jwk).key
    except Exception:
        return None
    if kty == "RSA" and getattr(key, "key_size", 0) < MIN_RSA_BITS:
        return None
    return KeyEntry(kid=kid, kty=kty, key=key, jwk_alg=jwk_alg)


class KeySet:
    """The snapshot holder and its refresh schedule."""

    def __init__(
        self,
        fetch: Fetch,
        *,
        refresh_s: float,
        max_stale_s: float,
        timeout_s: float,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_event: Callable[[str], None] | None = None,
    ) -> None:
        self._fetch = fetch
        self._refresh_s = refresh_s
        self._max_stale_s = max_stale_s
        self._timeout_s = timeout_s
        self._clock = clock
        self._rng = rng if rng is not None else random.Random()  # noqa: S311 - jitter, not security
        self._sleep = sleep
        self._on_event = on_event
        self._snapshot: Snapshot | None = None
        self._failures = 0
        self._next_attempt_at = 0.0  # first attempt immediately
        self._last_unknown_kid_at: float | None = None
        self._inflight: asyncio.Task[bool] | None = None
        self._task: asyncio.Task[None] | None = None
        #: A dedicated, small pool: a stuck fetch can occupy at most these threads, never the loop's default
        #: executor that the rest of the process (database lookups, DNS) shares.
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tb-jwks")
        #: Attempts by outcome ("ok", "failed", "timeout"), for the future refresh metric (set 1.8 part 2).
        self.outcomes: dict[str, int] = {"ok": 0, "failed": 0, "timeout": 0}

    # -- reads (no I/O) --------------------------------------------------------------------------

    @property
    def usable(self) -> bool:
        snap = self._snapshot
        return snap is not None and self._clock() - snap.fetched_at <= self._max_stale_s

    def get(self, kid: str) -> KeyEntry | None:
        snap = self._snapshot
        if snap is None or self._clock() - snap.fetched_at > self._max_stale_s:
            return None
        return snap.keys.get(kid)

    @property
    def failures(self) -> int:
        return self._failures

    # -- refresh ---------------------------------------------------------------------------------

    def _backoff(self) -> float:
        ceiling = min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2 ** (self._failures - 1))
        return self._rng.uniform(0.0, ceiling)  # full jitter

    async def refresh(self) -> bool:
        """One fetch, single-flight: callers arriving during a fetch share its outcome."""
        task = self._inflight
        if task is None or task.done():
            task = self._inflight = asyncio.get_running_loop().create_task(self._attempt())
        return await asyncio.shield(task)

    async def _attempt(self) -> bool:
        outcome = "ok"
        try:
            # The fetcher carries its own deadline; this is the backstop. An abandoned worker thread ends at
            # its socket timeout. The snapshot is replaced only after a full parse.
            raw = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(self._executor, self._fetch), timeout=self._timeout_s + 1.0
            )
            keys = parse_jwks(raw)
        except TimeoutError:
            outcome = "timeout"
        except Exception:
            outcome = "failed"
        now = self._clock()
        self.outcomes[outcome] += 1
        if outcome == "ok":
            self._snapshot = Snapshot(keys=keys, fetched_at=now)
            self._failures = 0
            self._next_attempt_at = now + self._refresh_s
        else:
            self._failures += 1
            self._next_attempt_at = now + self._backoff()
        if self._on_event is not None:
            self._on_event(f"jwks_refresh_{outcome}")
        return outcome == "ok"

    async def refresh_for_unknown_kid(self) -> None:
        """An unknown `kid` asks for a refresh, waiting at most `timeout_s` for it. At most one trigger per
        60 s, **failures included**, and none during backoff (D5 5). A refresh already in flight is joined,
        not counted."""
        task = self._inflight
        if task is not None and not task.done():
            await self._bounded(task)
            return
        now = self._clock()
        if self._failures > 0 and now < self._next_attempt_at:
            return
        last = self._last_unknown_kid_at
        if last is not None and now - last < UNKNOWN_KID_MIN_INTERVAL_S:
            return
        self._last_unknown_kid_at = now
        task = self._inflight = asyncio.get_running_loop().create_task(self._attempt())
        await self._bounded(task)

    async def _bounded(self, task: asyncio.Task[bool]) -> None:
        # On timeout the caller proceeds with the snapshot it has; the attempt finishes on its own.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), timeout=self._timeout_s)

    # -- background task -------------------------------------------------------------------------

    async def run(self) -> None:
        """The proactive loop. Cancel it to stop."""
        while True:
            delay = self._next_attempt_at - self._clock()
            if delay > 0:
                await self._sleep(min(delay, SLEEP_SLICE_S))
                continue
            await self.refresh()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self.run(), name="tb-jwks-refresh")

    async def stop(self) -> None:
        for task in (self._task, self._inflight):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._task, self._inflight):
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):  # shutting down
                    await task
        self._task = None
        self._executor.shutdown(wait=False, cancel_futures=True)


# -- production fetchers ----------------------------------------------------------------------------


def _https_url(url: str) -> bool:
    parts = urlsplit(url)
    return parts.scheme == "https" and bool(parts.hostname)


class _HeldConnection(http.client.HTTPSConnection):
    """`getresponse` closes the connection when the server will close it (HTTP/1.0 or `Connection: close`),
    after which `sock.settimeout` fails. Holding it open until `release` lets the reader reset the timeout to
    the time left before every read."""

    def close(self) -> None:
        pass

    def release(self) -> None:
        super().close()


def _get(url: str, *, deadline: float, ssl_context: ssl.SSLContext | None) -> bytes:
    """GET `url` (https only), at most `MAX_BODY_BYTES`, finished by the monotonic `deadline`.

    `http.client` directly, not `urllib`: the socket timeout is reset to the time LEFT before every read, so
    a server that drips one byte at a time cannot stretch the fetch past the deadline (a per-read timeout
    alone would). No redirect is followed: anything but 200 fails, so an https URL cannot become another URL."""
    if not _https_url(url):
        raise JwksError("not an https URL")
    parts = urlsplit(url)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise JwksError("deadline exceeded")
    context = ssl_context if ssl_context is not None else ssl.create_default_context()
    conn = _HeldConnection(parts.hostname or "", parts.port or 443, timeout=remaining, context=context)
    try:
        conn.request(
            "GET",
            (parts.path or "/") + (f"?{parts.query}" if parts.query else ""),
            headers={"Accept": "application/json"},
        )
        sock = conn.sock
        response = conn.getresponse()
        if response.status != 200:
            raise JwksError("unexpected status")
        chunks: list[bytes] = []
        size = 0
        while True:
            left = deadline - time.monotonic()
            if left <= 0 or sock is None:
                raise JwksError("deadline exceeded")
            sock.settimeout(left)
            chunk = response.read1(16 * 1024)
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                raise JwksError("response too large")
            chunks.append(chunk)
    except (http.client.HTTPException, OSError, ValueError):
        raise JwksError("fetch failed") from None
    finally:
        conn.release()


class HttpsJwksFetcher:
    """Discovery then JWKS over verified TLS (system trust store; no `requests`, no `certifi`, D8).

    With `jwks_uri` set, discovery is skipped. Otherwise the discovery document's `issuer` must equal the
    configured issuer exactly and its `jwks_uri` must be https (D13)."""

    def __init__(
        self,
        issuer: str,
        *,
        jwks_uri: str | None,
        timeout_s: float,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._issuer = issuer
        self._jwks_uri = jwks_uri
        self._timeout_s = timeout_s
        self._ssl = ssl_context

    def __call__(self) -> bytes:
        deadline = time.monotonic() + self._timeout_s
        uri = self._jwks_uri
        if uri is None:
            url = self._issuer.rstrip("/") + "/.well-known/openid-configuration"
            try:
                doc = json.loads(_get(url, deadline=deadline, ssl_context=self._ssl))
            except ValueError:
                raise JwksError("discovery is not JSON") from None
            if not isinstance(doc, dict) or doc.get("issuer") != self._issuer:
                raise JwksError("discovery issuer mismatch")
            uri = doc.get("jwks_uri")
            if not isinstance(uri, str) or not _https_url(uri):
                raise JwksError("discovery jwks_uri is not https")
        return _get(uri, deadline=deadline, ssl_context=self._ssl)


class FileJwksFetcher:
    """ADR-009 D9 (b): a static JWKS file, only for profile `lite` on loopback (enforced in `config`)."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def __call__(self) -> bytes:
        try:
            with self._path.open("rb") as handle:
                raw = handle.read(MAX_BODY_BYTES + 1)
        except OSError:
            raise JwksError("key file unreadable") from None
        if len(raw) > MAX_BODY_BYTES:
            raise JwksError("key file too large")
        return raw
