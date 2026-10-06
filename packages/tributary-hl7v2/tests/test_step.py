"""`tributary.hl7v2.step.parse`: the registered pipeline step."""

from __future__ import annotations

import pytest
from tributary.hl7v2.errors import Hl7Error, MalformedMessageError
from tributary.hl7v2.message import Hl7Message
from tributary.hl7v2.step import ENCODING_VARS_KEY, VARS_KEY, VERSION_VARS_KEY, parse
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


# -- `with: {version: ...}` (channel-spec README 3.3, W2-5) -------------------------------------------


def test_version_with_value_binds_and_matches_msh12() -> None:
    """`{"version": "2.5.1"}` is exactly what the channel-spec examples pass; ADT_A01's own MSH-12 is
    also "2.5.1", so no mismatch is logged."""
    bound = parse.bind({"version": "2.5.1"})
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    bound(message, ctx)
    assert ctx.vars[VERSION_VARS_KEY] == "2.5.1"
    assert not any(record.event == "hl7_version_mismatch" for record in ctx.log.records)


def test_version_mismatch_is_logged_without_the_version_strings_and_does_not_fail() -> None:
    bound = parse.bind({"version": "2.8"})
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")  # MSH-12 is "2.5.1"
    ctx = FakeContext()
    result = bound(message, ctx)
    assert result is message
    mismatch_records = [r for r in ctx.log.records if r.event == "hl7_version_mismatch"]
    assert len(mismatch_records) == 1
    assert mismatch_records[0].fields == {"declared_version_matches_msh12": False}
    for record in ctx.log.records:
        assert "2.8" not in repr(record.fields)
        assert "2.5.1" not in repr(record.fields)


def test_no_version_given_leaves_ctx_vars_and_logs_untouched() -> None:
    message = make_message(ADT_A01, content_type="application/hl7-v2+er7")
    ctx = FakeContext()
    parse(message, ctx)
    assert VERSION_VARS_KEY not in ctx.vars
    assert not any(record.event == "hl7_version_mismatch" for record in ctx.log.records)


def test_unknown_with_key_is_refused() -> None:
    from tributary.sdk.errors import StepConfigError

    with pytest.raises(StepConfigError):
        parse.bind({"not_a_real_option": "x"})
