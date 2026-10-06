"""`context_extension`: the `tributary.context` registrant a worker loads to give steps `ctx.fhir`."""

from __future__ import annotations

from importlib.metadata import entry_points

from fhir.resources.R4B.patient import Patient
from tributary.fhir import FhirContext, Identifier
from tributary.fhir.bundle import BundleEntry
from tributary.fhir.runtime import context_extension
from tributary.sdk import Message, ReplyContext
from tributary.sdk import headers as h
from tributary.sdk.fakes import FakeContext
from tributary.sdk.ids import new_traceparent, uuid7


def _message(msg_id: str | None = None) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "1",
            h.TB_SOURCE: "test",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: new_traceparent(),
        },
        payload=b"{}",
    )


def _urls(ctx: FhirContext, *values: str) -> list[str | None]:
    entries: list[BundleEntry] = [
        ctx.fhir.upsert(Patient(), identifier=Identifier(system="urn:t", value=value)) for value in values
    ]
    return [entry.fullUrl for entry in entries]


def test_entry_point_is_registered() -> None:
    points = {p.name: p.value for p in entry_points(group="tributary.context")}
    assert points["fhir"] == "tributary.fhir.runtime:context_extension"


def test_extension_attaches_fhir_terminology_and_keeps_reply() -> None:
    ctx = context_extension()(FakeContext(), _message())
    assert isinstance(ctx, FhirContext)
    assert isinstance(ctx, ReplyContext)
    ctx.set_attr("a", "1")
    ctx.vars["k"] = 1
    assert ctx.attrs()["a"] == "1"


def test_full_urls_are_deterministic_per_message() -> None:
    message = _message()
    first = _urls(context_extension()(FakeContext(), message), "1", "2")
    again = _urls(context_extension()(FakeContext(), message), "1", "2")
    assert first == again
    assert len(set(first)) == 2
    assert _urls(context_extension()(FakeContext(), _message()), "1", "2") != first


def test_in_place_retry_reproduces_the_same_full_urls() -> None:
    """One ctx spans step retries: a partial first attempt must not shift the second attempt's ids."""
    message = _message()
    clean = _urls(context_extension()(FakeContext(), message), "1", "2")
    ctx = context_extension()(FakeContext(), message)
    _urls(ctx, "1")  # attempt 1 fails after the first entry
    assert _urls(ctx, "1", "2") == clean
