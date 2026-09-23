"""`tributary.hl7v2.step.parse`: the registered pipeline step."""

from __future__ import annotations

import pytest
from tributary.hl7v2.errors import Hl7Error, MalformedMessageError
from tributary.hl7v2.message import Hl7Message
from tributary.hl7v2.step import ENCODING_VARS_KEY, VARS_KEY, parse
from tributary.sdk.fakes import FakeContext
from tributary.sdk.testing import make_message

ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
    b"PID|1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M\r"
)


def test_returns_the_message_unchanged() -> None:
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    result = parse(message, ctx)
    assert result is message
    assert result.payload == ADT_A01


def test_stashes_the_parsed_message_in_ctx_vars() -> None:
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    parse(message, ctx)
    hl7 = ctx.vars[VARS_KEY]
    assert isinstance(hl7, Hl7Message)
    assert hl7["PID.5.1"] == "SYNTHPATIENT"


def test_sets_a_structural_attr_for_the_message_type() -> None:
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    parse(message, ctx)
    assert ctx.attrs()["hl7_type"] == "ADT_A01"


def test_logs_no_field_content() -> None:
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    parse(message, ctx)
    for record in ctx.log.records:
        assert "SYNTHPATIENT" not in repr(record.fields)
        assert "9000001" not in repr(record.fields)


def test_malformed_payload_raises_typed_error() -> None:
    message = make_message(b"not an hl7 message", content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    with pytest.raises(MalformedMessageError):
        parse(message, ctx)


def test_claim_checked_payload_raises_typed_error() -> None:
    message = make_message(None, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    with pytest.raises(Hl7Error):
        parse(message, ctx)


def test_encoding_override_via_ctx_vars() -> None:
    raw = ADT_A01.replace(b"SYNTHPATIENT", "SYNTHPÅTIENT".encode("latin-1"))
    message = make_message(raw, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    ctx.vars[ENCODING_VARS_KEY] = "latin-1"
    parse(message, ctx)
    hl7 = ctx.vars[VARS_KEY]
    assert hl7["PID.5.1"] == "SYNTHPÅTIENT"
