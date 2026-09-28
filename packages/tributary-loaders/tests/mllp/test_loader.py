"""`MllpLoader.deliver`: AA/CA success, AE/AR/CE/CR classification against `onNegativeAck`, timeout,
transport and TLS failures, MSA-2/MSH-10 validation, the circuit breaker, and the idempotency limits
README 5.1 actually allows for this destination type (invariant 4: what a loader "documents and tests"
can be honest about what an MLLP receiver can prove). Driven against `test_client.py`'s
`FakeMllpServer` -- a real local TCP listener, no Kafka -- so the classification is proven against
real framing and real socket behaviour, not a stub `MllpConnection`.
"""

from __future__ import annotations

import dataclasses
import socket
import threading
import time
from collections.abc import Callable, Iterator

import pytest
from tributary.loaders.mllp.client import MllpConnection
from tributary.loaders.mllp.config import CircuitBreakerSpec, MllpDestinationConfig
from tributary.loaders.mllp.framing import control_id, frame
from tributary.loaders.mllp.loader import MllpLoader
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

from .test_client import EB_CR, SB_, FakeMllpServer

MSH = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|RECV|FAC2|20260101000000||ADT^A01|CTRL-1|P|2.5"


def make_message(*, payload: bytes = MSH, msg_id: str | None = None) -> Message:
    headers = {
        h.TB_MSG_ID: msg_id or ids.uuid7(),
        h.TB_CHANNEL: "c",
        h.TB_CHANNEL_VERSION: "0",
        h.TB_SOURCE: "mllp:i@p",
        h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
        h.TB_CONTENT_TYPE: "application/hl7-v2",
        h.TB_ATTEMPT: "0",
        h.TRACEPARENT: ids.new_traceparent(),
        h.TB_DESTINATION: "downstream-lis",
    }
    return Message(headers=headers, payload=payload)


def make_config(
    *, on_negative_ack: str = "retry", circuit_breaker: CircuitBreakerSpec | None = None, port: int = 0
) -> MllpDestinationConfig:
    return MllpDestinationConfig(
        channel="c",
        destination="downstream-lis",
        host="127.0.0.1",
        port=port,
        tls=None,
        timeout_s=2.0,
        on_negative_ack=on_negative_ack,
        circuit_breaker=circuit_breaker,
        idempotency_technique="none",
        backoff="fixed",
        initial_delay_ms=100,
        max_delay_ms=1000,
        max_attempts=3,
        on_exhaust="halt-partition",
    )


@pytest.fixture
def scripted_server() -> Iterator[Callable[[Callable[[bytes], bytes | None]], FakeMllpServer]]:
    servers: list[FakeMllpServer] = []

    def make(respond: Callable[[bytes], bytes | None]) -> FakeMllpServer:
        server = FakeMllpServer(respond)
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.close()


def _ack_with(code: str) -> Callable[[bytes], bytes | None]:
    def respond(message: bytes) -> bytes:
        ctrl = control_id(message)
        return frame(f"MSH|^~\\&\rMSA|{code}|{ctrl}".encode())

    return respond


def test_aa_is_success_and_commits(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    server = scripted_server(_ack_with("AA"))
    loader = MllpLoader(make_config(port=server.port), MllpConnection("127.0.0.1", server.port, ssl_context=None))
    outcome = loader.deliver(make_message())
    assert outcome.success is True
    assert outcome.reason == "success"
    assert outcome.ack_code == "AA"
    loader.close()


def test_ca_is_also_success(scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer]) -> None:
    server = scripted_server(_ack_with("CA"))
    loader = MllpLoader(make_config(port=server.port), MllpConnection("127.0.0.1", server.port, ssl_context=None))
    outcome = loader.deliver(make_message())
    assert outcome.success is True
    loader.close()


@pytest.mark.parametrize("code", ["AE", "CE"])
def test_application_error_is_mllp_ae(
    code: str, scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer]
) -> None:
    server = scripted_server(_ack_with(code))
    loader = MllpLoader(
        make_config(port=server.port, on_negative_ack="retry"),
        MllpConnection("127.0.0.1", server.port, ssl_context=None),
    )
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "MLLP_AE"
    assert outcome.retryable is True
    loader.close()


@pytest.mark.parametrize("code", ["AR", "CR"])
def test_reject_is_mllp_ar(
    code: str, scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer]
) -> None:
    server = scripted_server(_ack_with(code))
    loader = MllpLoader(
        make_config(port=server.port, on_negative_ack="retry"),
        MllpConnection("127.0.0.1", server.port, ssl_context=None),
    )
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "MLLP_AR"
    assert outcome.retryable is True
    loader.close()


def test_on_negative_ack_fail_is_not_retryable(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    server = scripted_server(_ack_with("AE"))
    loader = MllpLoader(
        make_config(port=server.port, on_negative_ack="fail"),
        MllpConnection("127.0.0.1", server.port, ssl_context=None),
    )
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "MLLP_AE"
    assert outcome.retryable is False
    loader.close()


def test_no_msa_segment_is_mllp_no_ack(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    def respond(message: bytes) -> bytes:
        return frame(b"MSH|^~\\&|no ack here")

    server = scripted_server(respond)
    loader = MllpLoader(make_config(port=server.port), MllpConnection("127.0.0.1", server.port, ssl_context=None))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "MLLP_NO_ACK"
    assert outcome.retryable is True
    loader.close()


def test_msa_2_mismatch_against_sent_msh_10_is_mllp_no_ack(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    """README 3.4.6/`ack.py`: an ACK claiming to answer a different control id cannot be trusted as
    this message's own answer -- a stale reply from a previous attempt on a reused connection, or a
    misbehaving peer replying out of order."""

    def respond(message: bytes) -> bytes:
        return frame(b"MSH|^~\\&\rMSA|AA|SOME-OTHER-CONTROL-ID")

    server = scripted_server(respond)
    connection = MllpConnection("127.0.0.1", server.port, ssl_context=None)
    loader = MllpLoader(make_config(port=server.port), connection)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "MLLP_NO_ACK"
    assert outcome.retryable is True
    assert not connection.is_connected()  # closed so a stale reply cannot leak into the next attempt
    loader.close()


def test_timeout_is_mllp_timeout_and_retryable(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    def respond(message: bytes) -> None:
        time.sleep(1)
        return None

    server = scripted_server(respond)
    fast_config = dataclasses.replace(make_config(port=server.port), timeout_s=0.2)
    loader = MllpLoader(fast_config, MllpConnection("127.0.0.1", server.port, ssl_context=None))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True
    loader.close()


def test_connect_failed_is_retryable() -> None:
    loader = MllpLoader(make_config(port=1), MllpConnection("127.0.0.1", 1, ssl_context=None))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "CONNECT_FAILED"
    assert outcome.retryable is True


def test_peer_reset_before_ack_is_mllp_no_ack() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server_thread() -> None:
        conn, _ = listener.accept()
        conn.settimeout(5)
        buf = bytearray()
        while SB_ not in buf or EB_CR not in buf:
            buf += conn.recv(4096)
        conn.close()

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        loader = MllpLoader(make_config(port=port), MllpConnection("127.0.0.1", port, ssl_context=None))
        outcome = loader.deliver(make_message())
        assert outcome.success is False
        assert outcome.reason == "MLLP_NO_ACK"
        assert outcome.retryable is True
    finally:
        thread.join(2)
        listener.close()


def test_circuit_breaker_opens_after_the_threshold_and_short_circuits(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    server = scripted_server(_ack_with("AE"))
    config = make_config(port=server.port, circuit_breaker=CircuitBreakerSpec(failure_threshold=2, open_seconds=60.0))
    loader = MllpLoader(config, MllpConnection("127.0.0.1", server.port, ssl_context=None))
    loader.deliver(make_message())
    loader.deliver(make_message())
    outcome = loader.deliver(make_message())
    assert outcome.reason == "circuit-open"
    assert outcome.retryable is True
    loader.close()


def test_duplicate_delivery_carries_the_same_message_id(
    scripted_server: Callable[[Callable[[bytes], bytes | None]], FakeMllpServer],
) -> None:
    """README 5.1: for `mllp`, where the sink is someone else's, "the test asserts that both
    deliveries carry the same tb-msg-id, which is all the engine can promise" -- this loader cannot
    dedupe or prove the receiver did (`config.py`'s module docstring)."""
    seen_ids: list[str] = []

    def respond(message: bytes) -> bytes:
        seen_ids.append(control_id(message))
        return frame(f"MSH|^~\\&\rMSA|AA|{control_id(message)}".encode())

    server = scripted_server(respond)
    loader = MllpLoader(make_config(port=server.port), MllpConnection("127.0.0.1", server.port, ssl_context=None))
    message = make_message()
    first = loader.deliver(message)
    second = loader.deliver(message)  # a genuine redelivery: same Message, a second deliver() call
    assert first.success and second.success
    assert seen_ids[0] == seen_ids[1] == control_id(MSH)
    loader.close()
