"""The shared key evaluator (`contracts/channel-spec/v0` README 4.3). Synthetic data only."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.sdk import keys
from tributary.sdk.keys import (
    KeyExpressionError,
    ResolvedKey,
    UnresolvedKey,
    check_key_source,
    evaluate_key,
    parse_key_expression,
)

HL7 = "application/hl7-v2+er7"
FHIR = "application/fhir+json"


def adt(pid3: bytes = b"SYNTH-MRN-000123^^^SYNTH^MR~SYNTH-ACCT-9^^^SYNTH^AN", *, sep: bytes = b"\r") -> bytes:
    segments = [
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|TB|TB|20260101120000||ADT^A01^ADT_A01|CTRL0001|P|2.5.1",
        b"EVN|A01|20260101120000",
        b"PID|1||" + pid3 + b"||TESTPATIENT^ALPHA||19700101|M",
    ]
    return sep.join(segments)


def resolve(expression: str, payload: bytes, content_type: str = HL7, **kwargs: object) -> bytes | str:
    result = evaluate_key(parse_key_expression(expression), payload=payload, content_type=content_type, **kwargs)  # type: ignore[arg-type]
    return result.key if isinstance(result, ResolvedKey) else result.reason


# -- hl7 ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("hl7:PID.3.1", b"SYNTH-MRN-000123"),  # first repetition, first component
        ("hl7:PID.3", b"SYNTH-MRN-000123^^^SYNTH^MR"),  # the whole first repetition, delimiters included
        ("hl7:PID.3.5", b"MR"),
        ("hl7:MSH.1", b"|"),  # HL7 numbering: MSH-1 is the field separator
        ("hl7:MSH.2", b"^~\\&"),
        ("hl7:MSH.3", b"TRIBUTARY-SYNTH"),
        ("hl7:MSH.9.2", b"A01"),
        ("hl7:EVN.1", b"A01"),
    ],
)
def test_hl7_locators(expression: str, expected: bytes) -> None:
    assert resolve(expression, adt()) == expected


def test_hl7_value_is_not_normalised() -> None:
    """README 4.3: bytes exactly as they appear - no trim, no unescape, no case fold."""
    assert resolve("hl7:PID.3.1", adt(b" synth-00123 \\T\\x^^^SYNTH^MR")) == b" synth-00123 \\T\\x"


def test_hl7_newline_segment_terminators_are_accepted() -> None:
    assert resolve("hl7:PID.3.1", adt(sep=b"\n")) == b"SYNTH-MRN-000123"
    assert resolve("hl7:PID.3.1", adt(sep=b"\r\n")) == b"SYNTH-MRN-000123"


def test_hl7_custom_delimiters_come_from_msh() -> None:
    payload = b"MSH#$*\\%#TRIBUTARY-SYNTH#F\rPID#1##SYNTH-7$$$X%B$MR*SYNTH-8"
    assert resolve("hl7:MSH.3", payload) == b"TRIBUTARY-SYNTH"
    assert resolve("hl7:PID.3.1", payload) == b"SYNTH-7"  # '*' is the repetition separator here
    assert resolve("hl7:PID.3.4.2", payload) == b"B"  # '%' is the subcomponent separator


@pytest.mark.parametrize(
    ("expression", "payload", "reason"),
    [
        ("hl7:PID.3.1", adt(b""), keys.KEY_EMPTY),
        ("hl7:PID.3.1", adt(b'""'), keys.KEY_EMPTY),  # HL7 explicit null
        ("hl7:PID.3.1", adt(b"X" * 257), keys.KEY_TOO_LONG),
        ("hl7:PID.3.9", adt(), keys.KEY_FIELD_ABSENT),
        ("hl7:PID.30", adt(), keys.KEY_FIELD_ABSENT),
        ("hl7:PV1.19", adt(), keys.KEY_SEGMENT_ABSENT),
        ("hl7:PID.3.1", b'{"resourceType": "Patient"}', keys.KEY_NOT_HL7),
        ("hl7:PID.3.1", b"MSH", keys.KEY_NOT_HL7),
    ],
)
def test_hl7_unresolved(expression: str, payload: bytes, reason: str) -> None:
    assert resolve(expression, payload) == reason


def test_key_at_the_limit_resolves() -> None:
    assert resolve("hl7:PID.3.1", adt(b"X" * 256)) == b"X" * 256


def test_hl7_scheme_on_json_content_is_a_mismatch() -> None:
    assert resolve("hl7:PID.3.1", adt(), FHIR) == keys.KEY_CONTENT_TYPE_MISMATCH


field_text = st.text(
    alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E, exclude_characters='|^~\\&\r\n"'),
    min_size=1,
    max_size=40,
)


@settings(max_examples=300, deadline=None)
@given(st.lists(st.lists(field_text, min_size=1, max_size=5), min_size=1, max_size=4), st.integers(1, 5))
def test_hl7_component_of_first_repetition_is_exact(repetitions: list[list[str]], component: int) -> None:
    pid3 = "~".join("^".join(components) for components in repetitions).encode()
    expected = repetitions[0][component - 1].encode() if component <= len(repetitions[0]) else keys.KEY_FIELD_ABSENT
    assert resolve(f"hl7:PID.3.{component}", adt(pid3)) == expected


# -- json -----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pointer", "payload", "expected"),
    [
        ("/subject/reference", b'{"subject": {"reference": "Patient/SYNTH-1"}}', b"Patient/SYNTH-1"),
        ("/identifier/0/value", b'{"identifier": [{"value": "SYNTH-2"}]}', b"SYNTH-2"),
        ("/n", b'{"n": 1.50}', b"1.50"),  # a number keeps the text it was written as
        ("/n", b'{"n": 12}', b"12"),
        ("/s", b'{"s": "12\\u0033"}', b"123"),  # a string is the decoded string
        ("/b", b'{"b": true}', b"true"),
        ("/s", '{"s": "é"}'.encode(), "é".encode()),
    ],
)
def test_json_pointers(pointer: str, payload: bytes, expected: bytes) -> None:
    assert resolve(f"json:{pointer}", payload, FHIR) == expected


@pytest.mark.parametrize(
    ("pointer", "payload", "reason"),
    [
        ("/a", b'{"a": null}', keys.KEY_EMPTY),
        ("/a", b'{"a": ""}', keys.KEY_EMPTY),
        ("/a", b'{"a": {"b": 1}}', keys.KEY_NOT_SCALAR),
        ("/a", b'{"a": [1]}', keys.KEY_NOT_SCALAR),
        ("/a", b'{"b": 1}', keys.KEY_POINTER_ABSENT),
        ("/a/01", b'{"a": [1, 2]}', keys.KEY_POINTER_ABSENT),  # no leading zeros in an index
        ("/a/5", b'{"a": [1, 2]}', keys.KEY_POINTER_ABSENT),
        ("/a", b'{"a": "x", "a": "y"}', keys.KEY_JSON_AMBIGUOUS),  # parser-differential bait
        ("/a", b"{not json", keys.KEY_NOT_JSON),
        ("/a", b'{"a": NaN}', keys.KEY_JSON_AMBIGUOUS),
    ],
)
def test_json_unresolved(pointer: str, payload: bytes, reason: str) -> None:
    assert resolve(f"json:{pointer}", payload, FHIR) == reason


def test_json_nesting_bomb_is_unresolved_not_a_crash() -> None:
    assert resolve("json:/a", b"[" * 100_000, FHIR) == keys.KEY_NOT_JSON


def test_json_scheme_on_hl7_content_is_a_mismatch() -> None:
    assert resolve("json:/a", b'{"a": "x"}', HL7) == keys.KEY_CONTENT_TYPE_MISMATCH
    assert resolve("json:/a", b'{"a": "x"}', "application/json; charset=utf-8") == b"x"


# -- header and record:key --------------------------------------------------------------------------


def test_header_names_match_by_source_type() -> None:
    """README 4.3: case-insensitive for http, exact for kafka; a repeated header is ambiguous."""
    http = {"header_source": "http"}
    assert resolve("header:X-Patient", b"", headers={"x-patient": b"SYNTH-3"}, **http) == b"SYNTH-3"
    assert resolve("header:X-Patient", b"", headers={"x-patient": b"SYNTH-3"}) == keys.KEY_HEADER_ABSENT
    assert resolve("header:x-id", b"", headers=[("X-Id", b"A"), ("x-id", b"B")]) == b"B"  # kafka: distinct names
    assert resolve("header:x-id", b"", headers=[("X-Id", b"A"), ("x-id", b"B")], **http) == keys.KEY_HEADER_AMBIGUOUS
    assert resolve("header:x-id", b"", headers=[("x-id", b"A"), ("x-id", b"B")]) == keys.KEY_HEADER_AMBIGUOUS
    assert resolve("header:x-id", b"", headers={}) == keys.KEY_HEADER_ABSENT
    assert resolve("header:x-id", b"") == keys.KEY_HEADER_ABSENT
    assert resolve("header:x-id", b"", headers={"x-id": None}) == keys.KEY_EMPTY


# -- the PID-3 repetition selector ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("pid3", "expected"),
    [
        (b"SYNTH-MRN-1^^^SYNTH^MR~SYNTH-ACCT-9^^^SYNTH^AN", b"SYNTH-MRN-1"),
        (b"SYNTH-ACCT-9^^^SYNTH^AN~SYNTH-MRN-1^^^SYNTH^MR", b"SYNTH-MRN-1"),  # order does not matter
        (b"SYNTH-A^^^SYNTH^MR~SYNTH-B^^^SYNTH^MR", b"SYNTH-A"),  # the first match
        (b"SYNTH-ACCT-9^^^SYNTH^AN", keys.KEY_REPETITION_ABSENT),
        (b"SYNTH-X^^^SYNTH^mr", keys.KEY_REPETITION_ABSENT),  # exact: no case folding
        (b"SYNTH-X", keys.KEY_REPETITION_ABSENT),  # too few components to compare
    ],
)
def test_repetition_selector(pid3: bytes, expected: bytes | str) -> None:
    assert resolve("hl7:PID.3[5:MR].1", adt(pid3)) == expected


def test_selector_without_component_returns_the_whole_repetition() -> None:
    assert resolve("hl7:PID.3[5:MR]", adt(b"A^^^S^AN~B^^^S^MR")) == b"B^^^S^MR"


def test_the_bare_form_is_order_sensitive_which_is_why_the_selector_exists() -> None:
    """The evidence behind README 4.3: two feeds, one patient, two different bare keys."""
    feed_a = resolve("hl7:PID.3.1", adt(b"SYNTH-MRN-1^^^SYNTH^MR~SYNTH-ACCT-9^^^SYNTH^AN"))
    feed_b = resolve("hl7:PID.3.1", adt(b"SYNTH-ACCT-9^^^SYNTH^AN~SYNTH-MRN-1^^^SYNTH^MR"))
    assert feed_a != feed_b
    selected = "hl7:PID.3[5:MR].1"
    assert resolve(selected, adt(b"SYNTH-MRN-1^^^SYNTH^MR~SYNTH-ACCT-9^^^SYNTH^AN")) == resolve(
        selected, adt(b"SYNTH-ACCT-9^^^SYNTH^AN~SYNTH-MRN-1^^^SYNTH^MR")
    )


def test_record_key() -> None:
    assert resolve("record:key", b"", record_key=b"SYNTH-4") == b"SYNTH-4"
    assert resolve("record:key", b"") == keys.KEY_RECORD_KEY_ABSENT
    assert resolve("record:key", b"", record_key=b"") == keys.KEY_EMPTY


# -- parsing and deploy-time refusals ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("xpath://PID/3", "KEY_SCHEME_RESERVED"),
        ("hl7:PID.3[2].1", "KEY_LOCATOR_RESERVED"),
        ("hl7:PID.3[5:MR", "KEY_LOCATOR_INVALID"),
        ("hl7:PID.3[0:MR].1", "KEY_LOCATOR_INVALID"),
        ("hl7:MSH.2[1:X]", "KEY_LOCATOR_INVALID"),
        ("hl7:PID.3[5:].1", "KEY_LOCATOR_INVALID"),
        ("hl7:PID", "KEY_LOCATOR_INVALID"),
        ("hl7:pid.3", "KEY_LOCATOR_INVALID"),
        ("hl7:PID.0", "KEY_LOCATOR_INVALID"),
        ("hl7:PID.3.1.1.1", "KEY_LOCATOR_INVALID"),
        ("json:subject", "KEY_LOCATOR_INVALID"),
        ("json:/a~1b", "KEY_LOCATOR_INVALID"),
        ("header:bad name", "KEY_LOCATOR_INVALID"),
        ("record:other", "KEY_SCHEME_UNKNOWN"),
        ("sql:x", "KEY_SCHEME_UNKNOWN"),
        ("PID.3", "KEY_EXPRESSION_INVALID"),
    ],
)
def test_parse_refusals(text: str, code: str) -> None:
    with pytest.raises(KeyExpressionError) as caught:
        parse_key_expression(text)
    assert caught.value.reason_code == code


@pytest.mark.parametrize(
    ("text", "source", "allowed"),
    [
        ("hl7:PID.3.1", "mllp", True),
        ("header:x-id", "mllp", False),
        ("header:x-id", "sftp", False),
        ("header:x-id", "http", True),
        ("record:key", "kafka", True),
        ("record:key", "http", False),
        ("json:/a", "mllp", False),
    ],
)
def test_scheme_source_compatibility(text: str, source: str, allowed: bool) -> None:
    expression = parse_key_expression(text)
    if allowed:
        check_key_source(expression, source)
    else:
        with pytest.raises(KeyExpressionError):
            check_key_source(expression, source)


def test_hl7_key_on_a_json_only_http_source_is_refused() -> None:
    with pytest.raises(KeyExpressionError):
        check_key_source(parse_key_expression("hl7:PID.3.1"), "http", content_types=[FHIR])
    check_key_source(parse_key_expression("hl7:PID.3.1"), "http", content_types=[HL7, FHIR])


# -- PHI and robustness ---------------------------------------------------------------------------


def test_a_resolved_key_never_shows_its_value() -> None:
    result = evaluate_key(parse_key_expression("hl7:PID.3.1"), payload=adt(), content_type=HL7)
    assert isinstance(result, ResolvedKey)
    for rendering in (repr(result), str(result), f"{result}", repr([result])):
        assert "SYNTH-MRN" not in rendering
    unresolved = evaluate_key(parse_key_expression("hl7:PID.3.1"), payload=adt(b" "), content_type=HL7)
    assert isinstance(unresolved, ResolvedKey)  # a space is a value: no trimming


@settings(max_examples=500, deadline=None)
@given(
    st.sampled_from(["hl7:PID.3.1", "hl7:PID.3[5:MR].1", "hl7:MSH.9.2", "hl7:MSH.2", "json:/a/0/b", "header:x-id", "record:key"]),
    st.binary(max_size=400) | st.builds(lambda tail: b"MSH" + tail, st.binary(max_size=200)),
    st.sampled_from([HL7, FHIR, "text/plain"]),
)
def test_never_raises_on_anything_a_message_contains(text: str, payload: bytes, content_type: str) -> None:
    result = evaluate_key(
        parse_key_expression(text),
        payload=payload,
        content_type=content_type,
        headers=[("x-id", payload[:300])],
        record_key=payload[:300],
    )
    assert isinstance(result, ResolvedKey | UnresolvedKey)
    if isinstance(result, ResolvedKey):
        assert 0 < len(result.key) <= keys.KEY_MAX_BYTES
