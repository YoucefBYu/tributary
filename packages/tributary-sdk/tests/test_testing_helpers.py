"""The helpers that make the W1-3 verify line true: a step can be tested with no Kafka.

The last test in this file is the verify line itself, asserted rather than asserted-in-prose: after
importing the whole SDK and running a pipeline, no Kafka client and nothing from `spikes/` has been
imported (invariant 7, and the Phase 0 ground rule that spike code stays out of `packages/`).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

import pytest
from tributary.sdk import Drop, Message, step
from tributary.sdk.errors import AttrError, KeyValueConflictError, LogFieldError, SecretNotFoundError, UsrHeaderError
from tributary.sdk.fakes import DEFAULT_NOW, FakeContext, FakeKeyValueStore, FakeSecrets
from tributary.sdk.testing import fixture_loader, make_message, run_pipeline, run_step


@step
def annotate(msg: Message, ctx: FakeContext) -> Message:
    ctx.set_attr("trigger", "A01")
    ctx.set_usr("batch-id", "b-4471")
    ctx.log.info("annotated", message_id=msg.msg_id, attempt=msg.attempt)
    ctx.vars["seen"] = True
    return msg.with_header("tb-destination", "fhir-server")


@step
def drop_everything(msg: Message) -> Drop:
    return Drop(reason="unsupported trigger event")


@step
def must_not_run(msg: Message) -> Message:
    raise AssertionError("a step after a Drop must not run")


def test_run_step_supplies_a_fake_ctx() -> None:
    outcome = run_step(annotate, make_message())
    assert isinstance(outcome, Message)
    assert outcome.destination == "fhir-server"


def test_run_step_accepts_raw_bytes_as_the_message() -> None:
    outcome = run_step(annotate, b"synthetic")
    assert isinstance(outcome, Message)
    assert outcome.payload == b"synthetic"


def test_a_supplied_ctx_records_what_the_step_did() -> None:
    ctx = FakeContext(channel="adt-to-fhir", destination="fhir-server")
    run_step(annotate, make_message(), ctx=ctx)

    assert ctx.attrs() == {"trigger": "A01"}
    assert ctx.usr_headers() == {"batch-id": "b-4471"}
    assert ctx.log.events() == ["annotated"]
    assert ctx.vars["seen"] is True


def test_run_pipeline_stops_at_the_first_drop() -> None:
    outcome = run_pipeline([annotate, drop_everything, must_not_run], make_message())
    assert isinstance(outcome, Drop)


def test_run_pipeline_shares_one_ctx_across_steps() -> None:
    ctx = FakeContext()
    run_pipeline([annotate, annotate], make_message(), ctx=ctx)
    assert ctx.vars["seen"] is True
    assert len(ctx.log.records) == 2


def test_the_fake_clock_is_fixed_so_a_stamped_step_is_reproducible() -> None:
    ctx = FakeContext()
    assert ctx.now() == DEFAULT_NOW
    moving = FakeContext(clock=lambda: datetime(2030, 5, 1, tzinfo=UTC))
    assert moving.now().year == 2030


def test_the_fake_enforces_the_same_rules_as_the_runtime() -> None:
    """A permissive fake turns a passing test into evidence that a PHI leak is fine."""
    ctx = FakeContext()
    with pytest.raises(AttrError):
        ctx.set_attr("mrn", "123456")
    with pytest.raises(AttrError):
        ctx.set_attr("reason", "admitted for observation")
    with pytest.raises(UsrHeaderError):
        ctx.set_usr("patient-id", "123456")
    with pytest.raises(LogFieldError):
        ctx.log.info("leak", payload=b"MSH|^~\\&|")
    assert ctx.attrs() == {}


def test_attrs_caps_apply_to_the_accumulated_object_not_one_call() -> None:
    ctx = FakeContext()
    for index in range(16):
        ctx.set_attr(f"code{index}", "v")
    with pytest.raises(AttrError):
        ctx.set_attr("one_too_many", "v")
    assert len(ctx.attrs()) == 16


def test_fake_secrets_and_kv() -> None:
    secrets = FakeSecrets({"fhir-oauth": "token"})
    assert secrets.get("fhir-oauth") == "token"
    with pytest.raises(SecretNotFoundError):
        secrets.get("missing")

    kv = FakeKeyValueStore({"cursor": "0"})
    assert kv.get("cursor") == ("0", 1)
    assert kv.put("cursor", "1", expected_version=1) == 2
    with pytest.raises(KeyValueConflictError):
        kv.put("cursor", "2", expected_version=1)
    assert kv.get("absent") is None


def test_make_message_produces_a_contract_shaped_envelope() -> None:
    message = make_message()
    assert message.msg_id and message.channel and message.attempt == 0
    assert message.received_at.endswith("Z")
    assert message.traceparent is not None and message.traceparent.startswith("00-")


def test_make_message_payload_is_never_hl7_shaped() -> None:
    """CLAUDE.md: synthetic only, and an HL7-looking default would also trip the fixture hook."""
    assert b"MSH" not in (make_message().payload or b"")


def test_fixture_loader_reads_bytes_and_refuses_to_escape_its_directory(tmp_path: object) -> None:
    from pathlib import Path

    root = Path(str(tmp_path))
    (root / "sample.txt").write_bytes(b"synthetic")
    load = fixture_loader(root)
    assert load("sample.txt") == b"synthetic"
    with pytest.raises(ValueError, match="escapes"):
        load("../outside.txt")


def test_a_step_runs_with_no_kafka_client_and_no_spike_code_imported() -> None:
    """The W1-3 verify line, as an assertion: no confluent_kafka, no tbcommon, nothing from spikes/."""
    run_pipeline([annotate], make_message())
    forbidden = [name for name in sys.modules if name.split(".")[0] in {"confluent_kafka", "tbcommon", "spikes"}]
    assert forbidden == []


def test_the_sdk_declares_no_third_party_dependencies() -> None:
    """The SDK must be importable inside a bundle that has no Kafka client at all."""
    import tomllib
    from pathlib import Path

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    assert tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["dependencies"] == []
