"""Gate requirement 10: schema-projection tests over the REAL emitters.

Phase 0 never validated spike output against the contracts, which is how `tb-x-stage`, a v4 binary
`tb-msg-id` and non-conformant topic names went unnoticed (gate section 6, findings G1-G3). So every
message here is built by the shipping code — `make_message`, `Message.with_header`,
`record_projection` — and validated against `contracts/envelope/v0/envelope.schema.json` itself. A
fixture hand-written to match the schema would prove nothing about the emitter.

Every headline check has a **mutation control**: a deliberately broken message that must make the same
check go red. A green test whose check cannot fail is indistinguishable from a passing system
(PHASE-0-DECISION section 4).

Failures are reported as `(json_path, keyword)` pairs only. A `ValidationError` message embeds the
failing instance, and these instances carry headers (invariant 6).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import EnvelopeError, HeaderValueError, UsrHeaderError
from tributary.sdk.ids import new_correlation_id, new_traceparent, uuid7
from tributary.sdk.projection import FORBIDDEN, REQUIRED, TopicClass, record_projection, validate_for_produce
from tributary.sdk.testing import make_message

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
SCHEMA_PATH = CONTRACTS / "envelope" / "v0" / "envelope.schema.json"

PLAUSIBLE: dict[str, str] = {
    h.TB_DESTINATION: "fhir-server",
    h.TB_STAGE: "sent",
    h.TB_ERROR_CODE: "HTTP_503",
    h.TB_RETRY_NOT_BEFORE: "2026-09-20T11:22:38.123Z",
    h.TB_CORRELATION_ID: "b7d3b3f4-3d8f-4a2b-9c1e-2f5a6b7c8d9e",
}


@pytest.fixture(scope="module")
def validator() -> Draft202012Validator:
    assert SCHEMA_PATH.is_file(), "envelope schema not found: the whole suite would pass vacuously"
    return Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def failures(validator: Draft202012Validator, instance: Any) -> set[tuple[str, str]]:
    """(json_path, keyword) pairs. The instance itself is never rendered."""
    return {(error.json_path, str(error.validator)) for error in validator.iter_errors(instance)}


# -- messages built by the shipping emitters ------------------------------------------------------


def raw_message() -> Message:
    return make_message(b"synthetic-payload", key=b"SYNTH-KEY-0001")


def out_message() -> Message:
    return make_message(b"transformed", key=b"SYNTH-KEY-0001", destination="fhir-server")


def retry_message() -> Message:
    return (
        make_message(b"transformed", key=b"SYNTH-KEY-0001", destination="fhir-server", attempt=1)
        .with_header(h.TB_STAGE, "pending")
        .with_header(h.TB_ERROR_CODE, "HTTP_503")
        .with_header(h.TB_RETRY_NOT_BEFORE, "2026-09-20T11:22:38.123Z")
    )


def dlq_message() -> Message:
    msg_id = uuid7()
    return (
        make_message(b"transformed", key=msg_id.encode("utf-8"), msg_id=msg_id, destination="fhir-server")
        .with_header(h.TB_STAGE, "transformed")
        .with_header(h.TB_ERROR_CODE, "TRANSFORM_ERROR")
    )


def reply_message() -> Message:
    correlation = new_correlation_id()
    return make_message(
        b"MSA|AA|",
        key=correlation.encode("utf-8"),
        correlation_id=correlation,
        content_type="application/hl7-v2+er7",
        traceparent=new_traceparent(),
    )


CASES: dict[TopicClass, Any] = {
    "raw": raw_message,
    "out": out_message,
    "retry": retry_message,
    "dlq": dlq_message,
    "reply": reply_message,
}


def cases() -> Iterator[tuple[TopicClass, Message]]:
    for topic_class, build in CASES.items():
        yield topic_class, build()


# -- the checks -----------------------------------------------------------------------------------


@pytest.mark.parametrize("topic_class", sorted(CASES))
def test_emitted_messages_validate_against_the_envelope_schema(
    validator: Draft202012Validator, topic_class: TopicClass
) -> None:
    projection = record_projection(
        CASES[topic_class](), topic_class=topic_class, topic=f"dev.test-channel.{topic_class}"
    )
    assert failures(validator, projection) == set()


@pytest.mark.parametrize("topic_class", sorted(CASES))
def test_emitted_messages_pass_the_producer_side_checks(topic_class: TopicClass) -> None:
    validate_for_produce(CASES[topic_class](), topic_class=topic_class)


def test_claim_checked_record_validates_with_a_null_value() -> None:
    message = make_message(
        None,
        key=b"SYNTH-KEY-0001",
        headers={h.TB_CLAIM_CHECK: "s3://tb-claims/dev/test-channel/2026/09/20/01920000-0000-7000-8000-000000000001"},
    )
    validate_for_produce(message, topic_class="raw")
    projection = record_projection(message, topic_class="raw")
    assert projection["payload_present"] is False
    assert projection["payload_size"] == 0


def test_the_projection_carries_no_key_value_and_no_payload_bytes() -> None:
    """Contract section 1.1: a validator error embeds the instance, and the key is usually PHI."""
    message = make_message(b"PAYLOAD-MARKER-9f2a", key=b"KEY-MARKER-9f2a")
    rendered = json.dumps(record_projection(message, topic_class="raw"))
    assert "KEY-MARKER" not in rendered
    assert "PAYLOAD-MARKER" not in rendered
    assert '"key_size": 15' in rendered


def test_no_experimental_header_survives_into_a_released_package() -> None:
    """Gate section 6: the spikes' `tb-x-stage` is legal in a spike and must never reach packages/."""
    for _topic_class, message in cases():
        assert not [name for name in message.headers if h.is_experimental(name)]


# -- mutation controls: each check above must be able to go red ------------------------------------


@pytest.mark.parametrize(
    ("topic_class", "header"),
    [(topic_class, header) for topic_class, headers in REQUIRED.items() for header in headers],
)
def test_dropping_a_required_header_fails_the_schema_and_the_producer_check(
    validator: Draft202012Validator, topic_class: TopicClass, header: str
) -> None:
    """Proves the REQUIRED table is not weaker than the schema's conditional rules."""
    broken = CASES[topic_class]().without_header(header)
    assert failures(validator, record_projection(broken, topic_class=topic_class))
    with pytest.raises(EnvelopeError):
        validate_for_produce(broken, topic_class=topic_class)


@pytest.mark.parametrize(
    ("topic_class", "header"),
    [(topic_class, header) for topic_class, headers in FORBIDDEN.items() for header in headers],
)
def test_adding_a_forbidden_header_fails_the_schema_and_the_producer_check(
    validator: Draft202012Validator, topic_class: TopicClass, header: str
) -> None:
    broken = CASES[topic_class]().with_header(header, PLAUSIBLE[header])
    assert failures(validator, record_projection(broken, topic_class=topic_class))
    with pytest.raises(EnvelopeError):
        validate_for_produce(broken, topic_class=topic_class)


def test_an_oversized_payload_fails_both_checks(validator: Draft202012Validator) -> None:
    broken = make_message(b"x" * 1_000_001, key=b"SYNTH-KEY-0001")
    assert ("$.payload_size", "maximum") in failures(validator, record_projection(broken, topic_class="raw"))
    with pytest.raises(EnvelopeError):
        validate_for_produce(broken, topic_class="raw")


def test_a_claim_check_with_inline_bytes_fails_both_checks(validator: Draft202012Validator) -> None:
    broken = make_message(b"inline", key=b"K", headers={h.TB_CLAIM_CHECK: "s3://tb-claims/dev/ch/2026/09/20/id"})
    assert failures(validator, record_projection(broken, topic_class="raw"))
    with pytest.raises(EnvelopeError):
        validate_for_produce(broken, topic_class="raw")


def test_an_unregistered_tb_header_fails_both_checks(validator: Draft202012Validator) -> None:
    broken = make_message(key=b"K").with_header("tb-msgid", "typo")
    assert failures(validator, record_projection(broken, topic_class="raw"))
    with pytest.raises(EnvelopeError):
        validate_for_produce(broken, topic_class="raw")


def test_an_experimental_header_is_schema_legal_but_refused_by_the_producer(validator: Draft202012Validator) -> None:
    """`tb-x-*` is reserved for spikes, so only the producer check can catch it — and it must."""
    broken = make_message(key=b"K").with_header("tb-x-seq", "17")
    assert failures(validator, record_projection(broken, topic_class="raw")) == set()
    with pytest.raises(EnvelopeError) as caught:
        validate_for_produce(broken, topic_class="raw")
    assert "tb-x-" in str(caught.value)


@pytest.mark.parametrize(
    ("topic_class", "build"),
    [
        ("dlq", lambda: dlq_message().with_key(b"not-the-message-id")),
        ("reply", lambda: reply_message().with_key(b"not-the-correlation-id")),
    ],
)
def test_per_class_key_identity_is_enforced_in_code(topic_class: TopicClass, build: Any) -> None:
    """Envelope contract 1.1: these rules cannot be in the schema, because the key is not projected."""
    with pytest.raises(EnvelopeError):
        validate_for_produce(build(), topic_class=topic_class)


def test_a_null_key_is_refused_on_an_ordered_channel_and_allowed_when_ordering_is_none() -> None:
    message = make_message(key=None)
    with pytest.raises(EnvelopeError):
        validate_for_produce(message, topic_class="raw", ordering="per-key")
    validate_for_produce(message, topic_class="raw", ordering="none")


def test_header_byte_budgets_are_enforced() -> None:
    over_value = make_message(key=b"K").with_header(h.TB_SOURCE, "s" * 1025)
    with pytest.raises(EnvelopeError):
        validate_for_produce(over_value, topic_class="raw")

    # Built through the constructor, not with_header: the produce-time check is the layer under test.
    over_usr = Message(headers={**make_message(key=b"K").headers, "tb-usr-batch": "b" * 257}, key=b"K")
    with pytest.raises(EnvelopeError):
        validate_for_produce(over_usr, topic_class="raw")

    crowded = make_message(key=b"K")
    for index in range(30):
        crowded = crowded.with_header(f"tb-usr-f{index}", "v")
    with pytest.raises(EnvelopeError):
        validate_for_produce(crowded, topic_class="raw")


@pytest.mark.parametrize("name", ["tb-usr-mrn", "tb-usr-patient-id", "tb-usr-dob"])
def test_an_identifier_named_usr_header_cannot_reach_the_producer_by_any_route(name: str) -> None:
    """Regression, code-reviewer High (wave 1).

    The deny-list used to be enforced only by `with_usr`/`ctx.set_usr`, so
    `with_header("tb-usr-mrn", ...)` walked past it and `validate_for_produce` checked only the byte
    size. Both routes are closed now: the copy-on-write setter refuses it, and the produce-time check —
    the layer every producer goes through — refuses it even when the message was built by hand.
    """
    base = make_message(key=b"SYNTH-KEY-0001")
    with pytest.raises(UsrHeaderError):
        base.with_header(name, "999-88-7777")

    smuggled = Message(headers={**base.headers, name: "999-88-7777"}, payload=base.payload, key=base.key)
    with pytest.raises(EnvelopeError):
        validate_for_produce(smuggled, topic_class="raw")


@pytest.mark.parametrize("bad", [chr(0x7F), "seq" + chr(0x7F), chr(0x00), "a" + chr(0x1B)])
def test_a_control_character_in_a_usr_header_cannot_reach_the_producer(bad: str) -> None:
    """Follow-up to the same High: DEL (0x7F) was checked by `with_header` but not by the guard itself,
    so a hand-built message reached `validate_for_produce` unrejected (code-reviewer, wave 1)."""
    base = make_message(key=b"SYNTH-KEY-0001")
    # Either error is a correct refusal: with_header's own control-character check may fire before the
    # tb-usr guard. What matters is that neither route lets the value through.
    with pytest.raises((HeaderValueError, UsrHeaderError)):
        base.with_header("tb-usr-seq", bad)

    smuggled = Message(headers={**base.headers, "tb-usr-seq": bad}, payload=base.payload, key=base.key)
    with pytest.raises(EnvelopeError):
        validate_for_produce(smuggled, topic_class="raw")


def test_a_well_formed_usr_header_still_passes() -> None:
    """The guard has to let the legitimate case through, or channels route around it."""
    message = make_message(key=b"SYNTH-KEY-0001").with_header("tb-usr-batch-id", "b-4471")
    validate_for_produce(message, topic_class="raw")


def test_the_schema_check_itself_can_go_red(validator: Draft202012Validator) -> None:
    """The control for the control: an empty object must fail, or the validator is not wired up."""
    assert failures(validator, {})
