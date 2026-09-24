"""`attach`: composing a base `Context` with `.fhir`/`.terminology`, delegating every other member."""

from __future__ import annotations

from datetime import UTC, datetime

from tributary.fhir.bundle import LocalFhir, sequential_id_factory
from tributary.fhir.context import FhirContext, attach
from tributary.fhir.terminology import LocalTerminology
from tributary.sdk.fakes import FakeContext


def _attached(**base_kwargs: object) -> FhirContext:
    base = FakeContext(**base_kwargs)  # type: ignore[arg-type]
    return attach(base, fhir=LocalFhir(id_factory=sequential_id_factory()), terminology=LocalTerminology())


def test_isinstance_check_against_the_runtime_checkable_protocol() -> None:
    ctx = _attached()
    assert isinstance(ctx, FhirContext)


def test_fhir_and_terminology_are_attached() -> None:
    ctx = _attached()
    assert isinstance(ctx.fhir, LocalFhir)
    assert isinstance(ctx.terminology, LocalTerminology)


def test_every_base_context_member_is_delegated() -> None:
    fixed_clock = datetime(2026, 1, 1, tzinfo=UTC)
    ctx = _attached(channel="adt-to-fhir", channel_version=3, clock=fixed_clock)
    assert ctx.channel == "adt-to-fhir"
    assert ctx.channel_version == 3
    assert ctx.now() == fixed_clock
    assert ctx.instance
    assert ctx.attempt == 0
    assert ctx.vars == {}


def test_set_attr_and_attrs_delegate_to_the_base() -> None:
    ctx = _attached()
    ctx.set_attr("hl7_type", "ADT_A01")
    assert ctx.attrs()["hl7_type"] == "ADT_A01"


def test_set_usr_and_usr_headers_delegate_to_the_base() -> None:
    ctx = _attached()
    ctx.set_usr("my-flag", "yes")
    assert ctx.usr_headers()["my-flag"] == "yes"


def test_log_secrets_and_kv_are_the_same_objects_as_the_base() -> None:
    base = FakeContext()
    ctx = attach(base, fhir=LocalFhir(), terminology=LocalTerminology())
    assert ctx.log is base.log
    assert ctx.secrets is base.secrets
    assert ctx.kv is base.kv
