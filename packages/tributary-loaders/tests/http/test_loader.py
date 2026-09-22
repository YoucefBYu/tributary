"""`HttpLoader.deliver`: classification, auth headers, the circuit breaker wired in, and the
idempotency-key-header duplicate-delivery test (invariant 4: every loader documents and tests its
idempotency technique keyed by `tb-msg-id`).
"""

from __future__ import annotations

import asyncio

import pytest
from tributary.loaders.http.circuit_breaker import State
from tributary.loaders.http.client import Request, Response, TransportError
from tributary.loaders.http.config import AuthSpec, CircuitBreakerSpec, HttpDestinationConfig
from tributary.loaders.http.loader import HttpLoader, classify_status
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_message(msg_id: str | None = None) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "dest",
        },
        payload=b"MSH|...\r",
    )


def make_config(**overrides: object) -> HttpDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "dest",
        "url": "https://example.org/ingest",
        "method": "POST",
        "timeout_s": 2.0,
        "headers": {},
        "auth": None,
        "circuit_breaker": None,
        "idempotency_technique": "none",
        "idempotency_header": None,
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return HttpDestinationConfig(**base)  # type: ignore[arg-type]


class FakeSecretResolver:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self.values = values

    def resolve_text(self, ref: str, key: str) -> str:
        return self.values[(ref, key)]


@pytest.mark.parametrize(
    ("status", "expected"),
    [(200, "success"), (201, "success"), (429, "http-429"), (500, "http-5xx"), (404, "http-4xx")],
)
def test_classify_status(status: int, expected: str) -> None:
    assert classify_status(status) == expected


def test_deliver_success_sends_no_idempotency_header_by_default() -> None:
    seen: list[Request] = []

    def fake_send(req: Request) -> Response:
        seen.append(req)
        return Response(200)

    loader = HttpLoader(make_config(), send=fake_send)
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.success is True
    assert "Idempotency-Key" not in seen[0].headers


def test_deliver_5xx_is_failure_and_retryable() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(503))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.success is False
    assert outcome.retryable is True
    assert outcome.reason == "http-5xx"


def test_deliver_4xx_is_failure_and_not_retryable() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(422))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.success is False
    assert outcome.retryable is False
    assert outcome.reason == "http-4xx"


def test_deliver_transport_error_is_retryable() -> None:
    def raiser(req: Request) -> Response:
        raise TransportError("connection refused")

    loader = HttpLoader(make_config(), send=raiser)
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.retryable is True
    assert outcome.reason == "transport-error"


def test_basic_auth_header() -> None:
    seen: list[Request] = []

    def fake_send(req: Request) -> Response:
        seen.append(req)
        return Response(200)

    cfg = make_config(auth=AuthSpec("basic", "creds-ref"))
    resolver = FakeSecretResolver({("creds-ref", "credentials"): "alice:s3cret"})
    loader = HttpLoader(cfg, secret_resolver=resolver, send=fake_send)
    asyncio.run(loader.deliver(make_message()))
    import base64

    expected = "Basic " + base64.b64encode(b"alice:s3cret").decode()
    assert seen[0].headers["Authorization"] == expected


def test_bearer_auth_header() -> None:
    seen: list[Request] = []

    def fake_send(req: Request) -> Response:
        seen.append(req)
        return Response(200)

    cfg = make_config(auth=AuthSpec("bearer", "token-ref"))
    resolver = FakeSecretResolver({("token-ref", "token"): "tok-abc"})
    loader = HttpLoader(cfg, secret_resolver=resolver, send=fake_send)
    asyncio.run(loader.deliver(make_message()))
    assert seen[0].headers["Authorization"] == "Bearer tok-abc"


def test_circuit_breaker_opens_after_repeated_failures_and_skips_the_network_call() -> None:
    calls = []

    def fake_send(req: Request) -> Response:
        calls.append(req)
        return Response(500)

    cfg = make_config(circuit_breaker=CircuitBreakerSpec(failure_threshold=2, open_seconds=60))
    loader = HttpLoader(cfg, send=fake_send)
    asyncio.run(loader.deliver(make_message()))
    asyncio.run(loader.deliver(make_message()))
    assert loader.breaker.state is State.OPEN
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "circuit-open"
    assert len(calls) == 2  # the third call never reached the network


def test_duplicate_delivery_is_harmless() -> None:
    """The idempotency-key-header technique: this loader must send the SAME header value for every
    attempt at the same message. Proven against a stub destination that applies the same rule real
    idempotent APIs do (dedupe on the header, return the original result on a repeat) — the
    `contracts/channel-spec/v0` walking-skeleton example's own stub does this same thing ("Test stub
    counts deliveries; duplicates are the assertion, not a defect")."""
    applied: set[str] = set()
    effects = {"count": 0}

    def stub_destination(req: Request) -> Response:
        key = req.headers["Idempotency-Key"]
        if key not in applied:
            applied.add(key)
            effects["count"] += 1  # the side effect a real destination would apply exactly once
        return Response(201)  # both the original and the duplicate get a success response

    cfg = make_config(idempotency_technique="idempotency-key-header", idempotency_header="Idempotency-Key")
    loader = HttpLoader(cfg, send=stub_destination)
    message = make_message()  # one message, one tb-msg-id, delivered twice (a blocking retry)

    first = asyncio.run(loader.deliver(message))
    second = asyncio.run(loader.deliver(message))  # simulates a redelivery of the identical record

    assert first.success is True
    assert second.success is True
    assert effects["count"] == 1  # the underlying effect happened once, even though deliver() ran twice


def test_two_different_messages_get_two_different_idempotency_keys() -> None:
    seen: list[str] = []

    def fake_send(req: Request) -> Response:
        seen.append(req.headers["Idempotency-Key"])
        return Response(200)

    cfg = make_config(idempotency_technique="idempotency-key-header", idempotency_header="Idempotency-Key")
    loader = HttpLoader(cfg, send=fake_send)
    asyncio.run(loader.deliver(make_message()))
    asyncio.run(loader.deliver(make_message()))
    assert seen[0] != seen[1]
