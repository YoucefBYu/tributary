"""The key-set wrapper (ADR-009 D5) with a fake clock, a fake fetcher and a fake sleep: no network, no real time."""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time

import pytest
from api_kit import public_jwk
from cryptography.hazmat.primitives.asymmetric import rsa
from tributary.api.auth.jwks import (
    BACKOFF_CAP_S,
    MAX_BODY_BYTES,
    FileJwksFetcher,
    HttpsJwksFetcher,
    JwksError,
    KeySet,
    parse_jwks,
)


def _rng(seed: int) -> random.Random:
    return random.Random(seed)  # noqa: S311 - seeded jitter in a test


KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
GOOD = json.dumps({"keys": [public_jwk(KEY, "k1")]}).encode()


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Fetcher:
    def __init__(self, *outcomes: bytes | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.last: bytes | Exception = GOOD

    def __call__(self) -> bytes:
        self.calls += 1
        if self.outcomes:
            self.last = self.outcomes.pop(0)
        if isinstance(self.last, Exception):
            raise self.last
        return self.last


def make(fetch: Fetcher, clock: Clock, **kw: float) -> KeySet:
    return KeySet(
        fetch,
        refresh_s=300,
        max_stale_s=kw.get("max_stale_s", 3600),
        timeout_s=kw.get("timeout_s", 5),
        clock=clock,
        rng=_rng(7),
    )


def test_success_installs_a_snapshot_and_schedules_the_next_refresh() -> None:
    async def go() -> None:
        clock, fetch = Clock(), Fetcher(GOOD)
        ks = make(fetch, clock)
        assert not ks.usable and ks.get("k1") is None  # nothing before the first success
        assert await ks.refresh()
        assert ks.usable and ks.get("k1") is not None
        assert ks.outcomes["ok"] == 1

    asyncio.run(go())


def test_failure_keeps_the_old_snapshot_and_backs_off_with_full_jitter() -> None:
    async def go() -> None:
        clock, fetch = Clock(), Fetcher(GOOD)
        ks = make(fetch, clock)
        await ks.refresh()
        fetch.outcomes = [JwksError("x")] * 20
        delays = []
        for n in range(1, 9):
            before = clock.now
            assert not await ks.refresh()
            delays.append(ks._next_attempt_at - before)
            assert 0 <= delays[-1] <= min(BACKOFF_CAP_S, 5 * 2 ** (n - 1))
            assert ks.get("k1") is not None  # a failed fetch never replaces the snapshot
        assert ks.failures == 8
        assert max(delays) > 5  # it grows past the base

    asyncio.run(go())


def test_a_failing_fetcher_is_retried_on_the_schedule_not_per_request() -> None:
    """The loop sleeps until `next_attempt_at`; looking keys up in between never fetches."""

    async def go() -> None:
        clock, fetch = Clock(), Fetcher(JwksError("down"))
        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)
            clock.now += seconds
            await asyncio.sleep(0)

        ks = KeySet(fetch, refresh_s=300, max_stale_s=3600, timeout_s=5, clock=clock, rng=_rng(1), sleep=fake_sleep)
        task = asyncio.get_running_loop().create_task(ks.run())
        for _ in range(200):
            await asyncio.sleep(0)
            ks.get("k1")  # request-path reads
        task.cancel()
        calls_in_200_ticks = fetch.calls
        assert 1 <= calls_in_200_ticks <= 8, calls_in_200_ticks  # bounded by backoff, not by the 200 lookups
        assert all(s <= 5.0 for s in slept)  # slices

    asyncio.run(go())


def test_unknown_kid_refreshes_once_per_minute_counting_failures_and_never_in_backoff() -> None:
    async def go() -> None:
        clock, fetch = Clock(), Fetcher(GOOD)
        ks = make(fetch, clock)
        await ks.refresh()
        base = fetch.calls
        fetch.outcomes = [JwksError("down")] * 10
        await ks.refresh_for_unknown_kid()
        assert fetch.calls == base + 1  # the failed attempt counts
        for _ in range(5):
            await ks.refresh_for_unknown_kid()
        assert fetch.calls == base + 1  # in backoff and inside 60 s: nothing
        clock.now += 59
        await ks.refresh_for_unknown_kid()
        assert fetch.calls == base + 1
        clock.now += 400  # past both the backoff and the 60 s window
        await ks.refresh_for_unknown_kid()
        assert fetch.calls == base + 2

    asyncio.run(go())


def test_single_flight_callers_share_one_fetch() -> None:
    async def go() -> None:
        clock = Clock()
        gate = threading.Event()

        def slow() -> bytes:
            gate.wait(2)
            return GOOD

        ks = KeySet(slow, refresh_s=300, max_stale_s=3600, timeout_s=5, clock=clock)
        t = asyncio.get_running_loop().create_task
        results = [t(ks.refresh()) for _ in range(5)]
        await asyncio.sleep(0.05)
        gate.set()
        assert all(await asyncio.gather(*results))
        assert ks.outcomes["ok"] == 1

    asyncio.run(go())


def test_a_slow_fetcher_does_not_block_the_event_loop() -> None:
    async def go() -> None:
        ks = KeySet(lambda: (time.sleep(0.6), GOOD)[1], refresh_s=300, max_stale_s=3600, timeout_s=5)
        ticks = 0
        task = asyncio.get_running_loop().create_task(ks.refresh())
        started = time.monotonic()
        while not task.done():
            await asyncio.sleep(0.01)
            ticks += 1
        assert time.monotonic() - started >= 0.5
        assert ticks > 20  # the loop kept turning during the 0.6 s fetch

    asyncio.run(go())


def test_a_fetch_past_the_timeout_is_a_timeout_outcome() -> None:
    async def go() -> None:
        ks = KeySet(lambda: (time.sleep(1.5), GOOD)[1], refresh_s=300, max_stale_s=3600, timeout_s=0.2)
        assert not await ks.refresh()
        assert ks.outcomes["timeout"] == 1

    asyncio.run(go())


def test_snapshot_older_than_the_limit_is_not_used() -> None:
    async def go() -> None:
        clock, fetch = Clock(), Fetcher(GOOD)
        ks = make(fetch, clock, max_stale_s=300)
        await ks.refresh()
        clock.now += 300
        assert ks.usable and ks.get("k1") is not None
        clock.now += 1
        assert not ks.usable and ks.get("k1") is None

    asyncio.run(go())


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b'{"keys": 1}',
        b'{"keys": []}',
        b'{"keys": [{"kty": "oct", "kid": "a", "k": "AA"}]}',
        b'{"keys": [{"kty": "RSA"}]}',
        b"x" * (MAX_BODY_BYTES + 1),
        json.dumps({"keys": [public_jwk(KEY, "dup"), public_jwk(KEY, "dup")]}).encode(),
        json.dumps({"keys": [public_jwk(KEY, f"k{i}") for i in range(33)]}).encode(),
    ],
)
def test_bad_key_sets_fail_the_fetch(raw: bytes) -> None:
    with pytest.raises(JwksError):
        parse_jwks(raw)


def test_unusable_keys_are_skipped_not_fatal() -> None:
    enc = public_jwk(KEY, "enc")
    enc["use"] = "enc"
    mismatch = public_jwk(KEY, "alg-mismatch", alg="ES256")
    keys = parse_jwks(json.dumps({"keys": [enc, mismatch, public_jwk(KEY, "ok", alg="RS256")]}).encode())
    assert set(keys) == {"ok"}
    assert keys["ok"].permits("RS256") and not keys["ok"].permits("PS256") and not keys["ok"].permits("ES256")


def test_file_fetcher_reads_and_caps(tmp_path) -> None:  # type: ignore[no-untyped-def]
    f = tmp_path / "jwks.json"
    f.write_bytes(GOOD)
    assert FileJwksFetcher(str(f))() == GOOD
    f.write_bytes(b"x" * (MAX_BODY_BYTES + 1))
    with pytest.raises(JwksError):
        FileJwksFetcher(str(f))()
    with pytest.raises(JwksError):
        FileJwksFetcher(str(tmp_path / "missing"))()


def test_https_fetcher_discovery_rules(idp) -> None:  # type: ignore[no-untyped-def]
    ctx = idp.client_context()
    ok = HttpsJwksFetcher(idp.issuer, jwks_uri=None, timeout_s=3, ssl_context=ctx)
    assert set(parse_jwks(ok())) == {"rsa-1", "ec-1"}
    idp.issuer_override = "https://other.example"  # D13: discovery issuer must equal the configured one
    try:
        with pytest.raises(JwksError):
            ok()
    finally:
        idp.issuer_override = None
    idp.jwks_uri_override = "http://127.0.0.1:1/jwks"  # D13: jwks_uri must be https
    try:
        with pytest.raises(JwksError):
            ok()
    finally:
        idp.jwks_uri_override = None
    with pytest.raises(JwksError):  # the system trust store does not trust the test certificate
        HttpsJwksFetcher(idp.issuer, jwks_uri=None, timeout_s=3)()
    with pytest.raises(JwksError):
        HttpsJwksFetcher("http://127.0.0.1:1", jwks_uri=None, timeout_s=3)()
    idp.fail = True
    try:
        with pytest.raises(JwksError):
            ok()
    finally:
        idp.fail = False


def test_fetch_runs_on_the_dedicated_executor_not_the_default_one() -> None:
    names: list[str] = []

    def fetch() -> bytes:
        names.append(threading.current_thread().name)
        return GOOD

    async def go() -> None:
        ks = KeySet(fetch, refresh_s=300, max_stale_s=3600, timeout_s=5)
        assert await ks.refresh()
        await ks.stop()

    asyncio.run(go())
    assert names and all(n.startswith("tb-jwks") for n in names)


def test_a_dripping_server_cannot_stretch_the_fetch_past_the_total_deadline(idp) -> None:  # type: ignore[no-untyped-def]
    """The deadline is total, not per read: one byte every 0.2 s would never trip a per-read timeout."""
    import ssl
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Drip(BaseHTTPRequestHandler):
        def log_message(self, *a: object) -> None:
            pass

        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            try:
                for _ in range(100000):
                    self.wfile.write(b"x")
                    self.wfile.flush()
                    time.sleep(0.2)
            except OSError:
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Drip)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(idp.cert_file, idp.key_file)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        fetcher = HttpsJwksFetcher(
            "https://x.example",
            jwks_uri=f"https://127.0.0.1:{server.server_address[1]}/jwks",
            timeout_s=1.0,
            ssl_context=idp.client_context(),
        )
        started = time.monotonic()
        with pytest.raises(JwksError):
            fetcher()
        assert time.monotonic() - started < 2.5
    finally:
        server.shutdown()
        server.server_close()
