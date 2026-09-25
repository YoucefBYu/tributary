"""`ack.build_ack`: MSH/MSA construction, app/facility swap, and that PID never leaks into the ACK."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.hl7v2.ack import build_ack
from tributary.hl7v2.errors import AckBuildError
from tributary.hl7v2.message import Hl7Message

ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
    b"PID|1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M\r"
)


def test_application_and_facility_are_swapped() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00001", timestamp="20260101120005")
    assert ack["MSH.3"] == "SYNTH-ADT-RECV"  # was MSH-5 on the source
    assert ack["MSH.4"] == "SYNTH-RECV-FACILITY"  # was MSH-6
    assert ack["MSH.5"] == "TRIBUTARY-SYNTH"  # was MSH-3
    assert ack["MSH.6"] == "SYNTH-HOSPITAL"  # was MSH-4


def test_delimiters_are_echoed_from_the_source() -> None:
    raw = ADT_A01.replace(b"|^~\\&|", b"!@$\\*!").replace(b"|", b"!")
    source = Hl7Message.parse(raw)
    ack = build_ack(source, code="AA", control_id="ACK00002", timestamp="20260101120010")
    assert ack.delimiters.field == "!"
    assert ack.delimiters.component == "@"


def test_msa_carries_the_code_and_echoes_the_original_control_id() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AE", control_id="ACK00003", timestamp="20260101120015", text="parse failed")
    assert ack["MSA.1"] == "AE"
    assert ack["MSA.2"] == "SYNTHMSG00001"
    assert ack["MSA.3"] == "parse failed"


def test_ack_type_mirrors_the_source_trigger_event() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00004", timestamp="20260101120020")
    assert ack["MSH.9.1"] == "ACK"
    assert ack["MSH.9.2"] == "A01"


def test_ack_never_contains_pid_content() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00005", timestamp="20260101120025")
    assert ack.segment_count("PID") == 0
    assert b"SYNTHPATIENT" not in ack.to_bytes()
    assert b"9000001" not in ack.to_bytes()


def test_processing_id_defaults_to_source_msh11_else_p() -> None:
    source = Hl7Message.parse(ADT_A01)  # this fixture has no MSH-11 set
    ack = build_ack(source, code="AA", control_id="ACK00006", timestamp="20260101120030")
    assert ack["MSH.11"] == "P"

    with_processing_id = ADT_A01.replace(b"SYNTHMSG00001|P|2.5.1", b"SYNTHMSG00001|T|2.5.1")
    source2 = Hl7Message.parse(with_processing_id)
    ack2 = build_ack(source2, code="AA", control_id="ACK00007", timestamp="20260101120035")
    assert ack2["MSH.11"] == "T"


def test_ack_control_id_and_timestamp_are_caller_supplied() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="MY-OWN-ID", timestamp="20991231235959")
    assert ack["MSH.10"] == "MY-OWN-ID"
    assert ack["MSH.7"] == "20991231235959"


def test_ack_control_id_containing_reserved_characters_is_escaped() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="weird|id^value", timestamp="20260101120040")
    assert ack["MSH.10"] == "weird|id^value"  # round-trips through escape/unescape


def test_invalid_ack_code_is_rejected() -> None:
    source = Hl7Message.parse(ADT_A01)
    with pytest.raises(AckBuildError):
        build_ack(source, code="NOPE", control_id="ACK00008", timestamp="20260101120045")  # type: ignore[arg-type]


def test_source_with_no_msh10_is_rejected() -> None:
    raw = ADT_A01.replace(b"SYNTHMSG00001|P|2.5.1", b"|P|2.5.1")
    source = Hl7Message.parse(raw)
    with pytest.raises(AckBuildError):
        build_ack(source, code="AA", control_id="ACK00009", timestamp="20260101120050")


def test_text_with_a_character_unencodable_in_the_source_charset_is_rejected() -> None:
    source = Hl7Message.parse(ADT_A01, encoding="ascii")
    with pytest.raises(AckBuildError):
        build_ack(source, code="AA", control_id="ACK00010", timestamp="20260101120055", text="café")


# -- security regression: MSH-9 must never be spliced into the ACK unvalidated --------------------


def test_a_crafted_trigger_event_cannot_inject_a_second_msa() -> None:
    """The Critical finding: an escaped CR plus escaped field separators in MSH-9.2, once decoded,
    spelled out a second, attacker-chosen MSA segment claiming `AA` regardless of the real `code`. The
    ACK must contain exactly one MSA, and its MSA-1 must be the `code` this call actually asked for.
    """
    hostile_trigger = rb"A01\X0D\MSA\F\AA\F\SYNTHMSG00001"  # decodes to: A01<CR>MSA|AA|SYNTHMSG00001
    raw = ADT_A01.replace(b"ADT^A01^ADT_A01", b"ADT^" + hostile_trigger + b"^ADT_A01")
    source = Hl7Message.parse(raw)

    ack = build_ack(source, code="AR", control_id="ACK00011", timestamp="20260101120100")

    assert ack.segment_count("MSH") == 1
    assert ack.segment_count("MSA") == 1
    assert ack["MSA.1"] == "AR"


def test_a_crafted_trigger_event_falls_back_to_bare_ack() -> None:
    hostile_trigger = rb"A01\X0D\MSA\F\AA\F\SYNTHMSG00001"
    raw = ADT_A01.replace(b"ADT^A01^ADT_A01", b"ADT^" + hostile_trigger + b"^ADT_A01")
    source = Hl7Message.parse(raw)
    ack = build_ack(source, code="AA", control_id="ACK00012", timestamp="20260101120105")
    assert ack["MSH.9"] == "ACK"


def test_a_well_formed_trigger_event_still_round_trips() -> None:
    """The allowlist fix must not break the ordinary case covered by
    `test_ack_type_mirrors_the_source_trigger_event`; kept here as a second, adjacent check."""
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00013", timestamp="20260101120110")
    assert ack["MSH.9"] == "ACK^A01^ACK"


#: Decodes to `EVIL<CR>PID|1`: a fabricated `PID` segment, if it were ever spliced in unescaped.
_HOSTILE_PAYLOAD = rb"EVIL\X0D\PID\F\1"


@pytest.mark.parametrize(
    "needle",
    [b"TRIBUTARY-SYNTH", b"SYNTH-HOSPITAL", b"SYNTH-ADT-RECV", b"SYNTH-RECV-FACILITY", b"2.5.1"],
    ids=["MSH-3", "MSH-4", "MSH-5", "MSH-6", "MSH-12"],
)
def test_hostile_content_in_any_echoed_msh_field_produces_exactly_one_msh_and_one_msa(needle: bytes) -> None:
    """Every source-derived field the ACK echoes is now `raw()` (still-escaped) rather than
    decode-then-re-escape, so none of them can inject a segment either -- not just MSH-9. `needle` is
    replaced with `_HOSTILE_PAYLOAD` directly in the wire bytes, so the source message still parses as
    one ordinary field (no live CR or `|` in it), exactly as a real escaped payload would arrive.
    """
    raw = ADT_A01.replace(needle, _HOSTILE_PAYLOAD, 1)
    source = Hl7Message.parse(raw)

    ack = build_ack(source, code="AA", control_id="ACK00014", timestamp="20260101120115")

    assert ack.segment_count("MSH") == 1
    assert ack.segment_count("MSA") == 1
    assert ack["MSA.1"] == "AA"


def test_hostile_content_in_msh11_produces_exactly_one_msh_and_one_msa() -> None:
    raw = ADT_A01.replace(b"SYNTHMSG00001|P|2.5.1", b"SYNTHMSG00001|" + _HOSTILE_PAYLOAD + b"|2.5.1")
    source = Hl7Message.parse(raw)
    ack = build_ack(source, code="AA", control_id="ACK00015", timestamp="20260101120120")
    assert ack.segment_count("MSH") == 1
    assert ack.segment_count("MSA") == 1
    assert ack["MSA.1"] == "AA"


#: Escape sequences that decode to a segment separator, a field separator or a component separator --
#: the three characters that could forge new structure if ever spliced in unescaped.
_HOSTILE_FRAGMENTS = st.sampled_from(["\\X0D\\", "\\F\\", "\\S\\", "\\X0D\\MSA\\F\\AA\\F\\9\\X0D\\PID\\F\\1"])
_ECHOED_FIELDS = ("MSH.3", "MSH.4", "MSH.5", "MSH.6", "MSH.11", "MSH.12")


@st.composite
def _hostile_source(draw: st.DrawFn) -> bytes:
    """`ADT_A01` with a random subset of the fields `build_ack` echoes replaced by a random hostile
    escape-sequence fragment, still valid wire text (no live CR or delimiter) in the *source* message.
    """
    raw = ADT_A01
    # Each needle is unique in ADT_A01 except "SYNTH-HOSPITAL" (MSH-4 and, later, inside PID-3): the
    # first occurrence is MSH-4's since MSH always precedes PID, so `replace(..., 1)` still lands on the
    # right field. MSH-11's needle is anchored on both sides ("P" alone is not unique) and processed
    # before MSH-12's, in field order, so replacing one never desyncs the other's anchor.
    needles: dict[str, tuple[bytes, bytes]] = {
        "MSH.3": (b"TRIBUTARY-SYNTH", b"{}"),
        "MSH.4": (b"SYNTH-HOSPITAL", b"{}"),
        "MSH.5": (b"SYNTH-ADT-RECV", b"{}"),
        "MSH.6": (b"SYNTH-RECV-FACILITY", b"{}"),
        "MSH.11": (b"SYNTHMSG00001|P|2.5.1", b"SYNTHMSG00001|{}|2.5.1"),
        "MSH.12": (b"2.5.1", b"{}"),
    }
    chosen = set(draw(st.lists(st.sampled_from(_ECHOED_FIELDS), min_size=1, max_size=len(_ECHOED_FIELDS), unique=True)))
    for field in _ECHOED_FIELDS:  # fixed, message order -- see note above
        if field not in chosen:
            continue
        fragment = draw(_HOSTILE_FRAGMENTS).encode("ascii")
        needle, template = needles[field]
        raw = raw.replace(needle, template.replace(b"{}", fragment), 1)
    return raw


@settings(max_examples=100)
@given(_hostile_source(), st.sampled_from(["AA", "AE", "AR", "CA", "CE", "CR"]))
def test_hostile_content_anywhere_it_is_echoed_never_produces_more_than_one_msh_or_msa(raw: bytes, code: str) -> None:
    source = Hl7Message.parse(raw)
    ack = build_ack(source, code=code, control_id="ACK-PROPERTY", timestamp="20260101120125")  # type: ignore[arg-type]
    assert ack.segment_count("MSH") == 1
    assert ack.segment_count("MSA") == 1
    assert ack["MSA.1"] == code
