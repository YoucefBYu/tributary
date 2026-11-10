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
from tributary.runtime.spec import Destination, Idempotency, RetryPolicy
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


class FakeSpecSource:
    """Stands in for `tributary.runtime.channels.LiveSpecSource`: `current()` returns an object
    with `.name` and `.destination(name)`, which is all `HttpRecordHandler` reads."""

    def __init__(self, channel: str, destination: Destination) -> None:
        self.name = channel
        self._destination = destination
        self.closed = False

    def current(self) -> FakeSpecSource:
        return self

    def destination(self, name: str) -> Destination:
        assert name == self._destination.name
        return self._destination

    def close(self) -> None:
        self.closed = True


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "dest",
        "type": "http",
        "params": {"url": "https://example.org/ingest", "method": "POST", "timeoutMs": 2000},
        "idempotency": Idempotency(technique="none", note="test"),
        "retry": RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def make_handler(loader: HttpLoader, *, destination: Destination | None = None) -> HttpRecordHandler:
    dest = destination if destination is not None else make_destination()
    return HttpRecordHandler(loader, spec_source=FakeSpecSource("c", dest), destination=dest.name)


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
    handler = make_handler(loader)
    try:
        handler.handle(make_record())  # must not raise
    finally:
        handler.close()


def test_handle_raises_retryable_delivery_error_on_5xx() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(503))
    handler = make_handler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "HTTP_503"
        assert exc_info.value.retryable is True
    finally:
        handler.close()


def test_handle_raises_non_retryable_delivery_error_on_4xx() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(422))
    handler = make_handler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "HTTP_422"
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
    handler = make_handler(loader)
    try:
        handler.handle(make_record())
        handler.handle(make_record())
    finally:
        handler.close()
    assert len(calls) == 2


def test_handle_refreshes_config_from_the_spec_source_each_call() -> None:
    """A republished spec's URL takes effect on the next record, with no restart."""
    seen_urls: list[str] = []

    def fake_send(req: Request) -> Response:
        seen_urls.append(req.url)
        return Response(200)

    loader = HttpLoader(make_config(), send=fake_send)
    source = FakeSpecSource("c", make_destination())
    handler = HttpRecordHandler(loader, spec_source=source, destination="dest")
    try:
        handler.handle(make_record())
        source._destination = make_destination(
            params={"url": "https://new.example.org/ingest", "method": "POST", "timeoutMs": 2000}
        )
        handler.handle(make_record())
    finally:
        handler.close()
    assert seen_urls == ["https://example.org/ingest", "https://new.example.org/ingest"]


def test_close_closes_the_spec_source_too() -> None:
    loader = HttpLoader(make_config(), send=lambda req: Response(200))
    source = FakeSpecSource("c", make_destination())
    handler = HttpRecordHandler(loader, spec_source=source, destination="dest")
    handler.close()
    assert source.closed is True


def test_metric_code_and_dlq_code_agree_for_every_http_status() -> None:
    from tributary.loaders.http.loader import classify_status

    for status, code in ((301, "HTTP_REDIRECT_REFUSED"), (404, "HTTP_404"), (429, "HTTP_429"), (503, "HTTP_503")):
        loader = HttpLoader(make_config(), send=lambda req, s=status: Response(s))
        handler = make_handler(loader)
        try:
            with pytest.raises(DeliveryError) as exc_info:
                handler.handle(make_record())
            assert exc_info.value.error_code == classify_status(status) == code
            assert exc_info.value.retryable is (status in (429, 503))
        finally:
            handler.close()
