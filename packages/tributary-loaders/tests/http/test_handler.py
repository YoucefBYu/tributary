"""`HttpRecordHandler`: translates `HttpLoader.deliver` outcomes into the
`tributary.runtime.consumer.RecordHandler` protocol (return on success, raise `DeliveryError`
otherwise) that `ConsumerLoop` drives. `ConsumerLoop` itself (retry/DLQ/halt/commit) is
`tributary-runtime`'s own tested territory; this is only the adapter."""

from __future__ import annotations

import pytest
from tributary.loaders.http.client import Request, Response
from tributary.loaders.http.config import HttpDestinationConfig
from tributary.loaders.http.handler import HttpRecordHandler
from tributary.loaders.http.loader import HttpLoader
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_record() -> ConsumedRecord:
    message = Message(
        headers={
            h.TB_MSG_ID: ids.uuid7(),
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
    return ConsumedRecord(topic="dev.c.out.dest", partition=0, offset=1, message=message)


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


def test_handle_returns_normally_on_success() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(200))
    handler = HttpRecordHandler(loader)
    try:
        handler.handle(make_record())  # must not raise
    finally:
        handler.close()


def test_handle_raises_retryable_delivery_error_on_5xx() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(503))
    handler = HttpRecordHandler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "HTTP_5XX"
        assert exc_info.value.retryable is True
    finally:
        handler.close()


def test_handle_raises_non_retryable_delivery_error_on_4xx() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(422))
    handler = HttpRecordHandler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "HTTP_4XX"
        assert exc_info.value.retryable is False
    finally:
        handler.close()


def test_handle_can_be_called_more_than_once_on_the_same_handler() -> None:
    """The persistent event loop is reused across records, not recreated per call."""
    calls: list[Request] = []

    def fake_send(req: Request) -> Response:
        calls.append(req)
        return Response(200)

    loader = HttpLoader(make_config(), send=fake_send)
    handler = HttpRecordHandler(loader)
    try:
        handler.handle(make_record())
        handler.handle(make_record())
    finally:
        handler.close()
    assert len(calls) == 2
