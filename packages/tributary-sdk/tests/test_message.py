"""`Message`: immutability, typed header access, and the PHI rules that apply to the record key.

The repr tests are not style checks. The record key is usually a direct patient identifier, and pytest
prints the repr of every value in a failing assertion, so a `Message` that showed its key would put PHI
into CI output — which `contracts/envelope/v0/README.md` section 4 lists first among the surfaces
invariant 6 covers.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderValueError, MissingHeaderError, PayloadError, UsrHeaderError
from tributary.sdk.testing import make_message


def test_headers_are_read_only() -> None:
    message = make_message()
    with pytest.raises(TypeError):
        message.headers["tb-channel"] = "other"  # type: ignore[index]


def test_mutators_return_a_new_message_and_leave_the_original_alone() -> None:
    original = make_message(b"before", key=b"key-1")
    changed = original.with_payload(b"after", content_type="application/fhir+json")

    assert original.payload == b"before"
    assert original.content_type == "application/json"
    assert changed.payload == b"after"
    assert changed.content_type == "application/fhir+json"
    assert changed.key == original.key
    assert changed.msg_id == original.msg_id


def test_typed_accessors_parse_headers() -> None:
    message = make_message(channel_version=7, attempt=2)
    assert message.channel == "test-channel"
    assert message.channel_version == 7
    assert message.attempt == 2
    assert message.received_at_time.year == 2026
    assert message.destination is None


def test_missing_required_header_is_typed_not_a_keyerror() -> None:
    message = Message(headers={"tb-channel": "demo"})
    with pytest.raises(MissingHeaderError):
        _ = message.msg_id


def test_header_values_must_be_text() -> None:
    with pytest.raises(HeaderValueError):
        Message(headers={"tb-channel": b"demo"})  # type: ignore[dict-item]


def test_empty_header_value_is_refused() -> None:
    """The contract says omit the header; an empty value that means "absent" is a parsing trap."""
    with pytest.raises(HeaderValueError):
        make_message().with_header(h.TB_DESTINATION, "")


def test_header_value_cannot_carry_crlf() -> None:
    with pytest.raises(HeaderValueError):
        make_message().with_header(h.TB_SOURCE, "mllp-receiver\r\nInjected: yes")


def test_with_attempt_formats_the_contract_value() -> None:
    message = make_message().with_attempt(3)
    assert message.headers[h.TB_ATTEMPT] == "3"
    assert message.attempt == 3
    with pytest.raises(HeaderValueError):
        message.with_attempt(-1)


def test_usr_headers_are_validated_and_round_trip() -> None:
    message = make_message().with_usr("batch-id", "b-4471")
    assert message.headers["tb-usr-batch-id"] == "b-4471"
    assert message.usr_headers() == {"batch-id": "b-4471"}


def test_usr_header_named_after_an_identifier_is_refused() -> None:
    with pytest.raises(UsrHeaderError):
        make_message().with_usr("patient-id", "12345")


def test_experimental_headers_are_tolerated_inbound_and_strippable() -> None:
    """Envelope contract 1.2 against 2.1: accept `tb-x-*` on the way in, never hand one on."""
    inbound = Message(headers={**make_message().headers, "tb-x-seq": "17", "tb-usr-batch-id": "b-4471"})
    assert inbound.get("tb-x-seq") == "17", "a consumer must not reject a record for an unknown header"

    outgoing = inbound.without_experimental()
    assert outgoing.get("tb-x-seq") is None
    assert outgoing.headers["tb-usr-batch-id"] == "b-4471", "the channel-defined space is propagated"
    assert outgoing.msg_id == inbound.msg_id


def test_stripping_nothing_returns_the_same_object() -> None:
    message = make_message()
    assert message.without_experimental() is message


def test_text_refuses_to_guess_the_hl7_charset() -> None:
    """HL7 v2 in the field is frequently ISO-8859-1; guessing UTF-8 corrupts names (envelope 3.1)."""
    message = make_message(b"MSH|^~\\&|", content_type="application/hl7-v2+er7")
    with pytest.raises(PayloadError):
        message.text()
    assert message.text(encoding="iso-8859-1").startswith("MSH")


def test_text_on_a_claim_checked_record_is_typed() -> None:
    message = make_message(None, headers={h.TB_CLAIM_CHECK: "s3://bucket/dev/ch/2026/09/20/id"})
    with pytest.raises(PayloadError):
        message.text()


def test_equality_covers_headers_payload_and_key() -> None:
    fixed = {"msg_id": "01920000-0000-7000-8000-000000000001", "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    one = make_message(b"same", key=b"k", **fixed)  # type: ignore[arg-type]
    two = make_message(b"same", key=b"k", **fixed)  # type: ignore[arg-type]
    assert one == two
    assert one != two.with_key(b"other")


@given(
    key=st.binary(min_size=8, max_size=64),
    payload=st.binary(min_size=8, max_size=256),
)
def test_repr_never_leaks_the_key_or_the_payload(key: bytes, payload: bytes) -> None:
    """Invariant 6: the key is PHI, the payload is PHI, and pytest prints reprs on failure.

    Eight bytes minimum: a one-byte key like `b"0"` occurs inside a UUID by coincidence, which would
    make the test flaky rather than wrong.
    """
    message = make_message(payload, key=key)
    rendered = f"{message!r} {message!s}"
    assert key.decode("latin-1") not in rendered
    assert payload.decode("latin-1") not in rendered
    assert repr(key) not in rendered
    assert str(len(payload)) in rendered  # the size is what a debugger actually needs


def test_repr_reports_ids_and_sizes() -> None:
    message = make_message(b"1234", key=b"PID-SYNTH-1")
    rendered = repr(message)
    assert message.msg_id in rendered
    assert "payload=4B" in rendered
    assert "key=11B" in rendered
    assert "PID-SYNTH-1" not in rendered
