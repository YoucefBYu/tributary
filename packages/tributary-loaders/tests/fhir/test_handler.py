"""`FhirRecordHandler`: translates `FhirLoader.deliver` outcomes into the `RecordHandler` protocol."""

from __future__ import annotations

from typing import Any

import pytest
from tributary.loaders.fhir.handler import FhirRecordHandler
from tributary.loaders.fhir.loader import FhirLoader
from tributary.loaders.http.client import Response
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .test_loader import FakeSend, make_config, make_message, ok


class FakeSpecSource:
    def __init__(self) -> None:
        self.closed = False

    def current(self) -> Any:
        return self

    def close(self) -> None:
        self.closed = True


def record() -> ConsumedRecord:
    return ConsumedRecord(topic="t", partition=0, offset=0, message=make_message())


def test_success_returns_normally() -> None:
    handler = FhirRecordHandler(
        FhirLoader(make_config(), send=FakeSend(ok())), spec_source=FakeSpecSource(), destination="d"
    )  # type: ignore[arg-type]
    handler.handle(record())
    assert handler.metrics_snapshot()["delivery_success"] == 1


@pytest.mark.parametrize(("status", "code", "retryable"), [(503, "HTTP_503", True), (412, "HTTP_412", False)])
def test_failure_raises_delivery_error_with_the_code(status: int, code: str, retryable: bool) -> None:
    handler = FhirRecordHandler(
        FhirLoader(make_config(), send=FakeSend(Response(status))),
        spec_source=FakeSpecSource(),
        destination="d",  # type: ignore[arg-type]
    )
    with pytest.raises(DeliveryError) as exc_info:
        handler.handle(record())
    assert exc_info.value.error_code == code
    assert exc_info.value.retryable is retryable


def test_close_closes_the_spec_source() -> None:
    source = FakeSpecSource()
    FhirRecordHandler(FhirLoader(make_config(), send=FakeSend(ok())), spec_source=source, destination="d").close()  # type: ignore[arg-type]
    assert source.closed
