"""`Hl7Message`: parsing, accessors, repetitions, mutation and the round-trip guarantee."""

from __future__ import annotations

import pytest
from tributary.hl7v2.errors import FieldPathError, MalformedMessageError, PayloadDecodeError, SegmentNotFoundError
from tributary.hl7v2.message import Hl7Message

ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
    b"EVN|A01|20260101120000\r"
    b"PID|1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M\r"
    b"NK1|1|SYNTHCONTACT^ONE|SPO\r"
    b"NK1|2|SYNTHCONTACT^TWO|CHD\r"
    b"PV1|1|I|SYNTHUNIT^101^A^SYNTH-HOSPITAL||||9001^SYNTHDOC^ATTENDING\r"
)


def test_msh_field_offset_msh1_is_the_separator_itself() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.1"] == "|"


def test_msh_field_offset_msh2_is_the_encoding_characters() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.2"] == "^~\\&"


def test_msh_field_offset_msh3_is_the_first_real_field() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.3"] == "TRIBUTARY-SYNTH"


def test_component_access() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5.1"] == "SYNTHPATIENT"
    assert msg["PID.5.2"] == "ALPHA"
    assert msg["PID.5.3"] == "Q"


def test_missing_trailing_component_is_empty_string_not_an_error() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5.4"] == ""


def test_missing_trailing_field_is_empty_string() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.29"] == ""  # far beyond what this fixture sends


def test_whole_field_with_no_component_index() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5"] == "SYNTHPATIENT^ALPHA^Q"


def test_second_segment_occurrence() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["NK1[1].2.1"] == "SYNTHCONTACT"
    assert msg["NK1[1].2.2"] == "ONE"
    assert msg["NK1[2].2.2"] == "TWO"


def test_segment_count() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.segment_count("NK1") == 2
    assert msg.segment_count("OBX") == 0


def test_segments_returns_every_occurrence_in_order() -> None:
    msg = Hl7Message.parse(ADT_A01)
    names = [seg.field(2, component=2) for seg in msg.segments("NK1")]
    assert names == ["ONE", "TWO"]


def test_segment_field_convenience_method() -> None:
    msg = Hl7Message.parse(ADT_A01)
    pid = msg.segment("PID")
    assert pid.field(5, component=1) == "SYNTHPATIENT"
    assert pid.field(5) == "SYNTHPATIENT^ALPHA^Q"


def test_get_returns_default_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.get("OBX.5") is None
    assert msg.get("OBX.5", "fallback") == "fallback"


def test_require_raises_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.require("OBX.5")


def test_getitem_raises_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        _ = msg["OBX.5"]


def test_segment_raises_for_occurrence_beyond_what_exists() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.segment("NK1", 3)


def test_message_type() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.message_type() == ("ADT", "A01", "ADT_A01")


def test_control_id_and_version() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.control_id() == "SYNTHMSG00001"
    assert msg.version() == "2.5.1"


# -- round trip -----------------------------------------------------------------------------------


def test_to_bytes_is_identical_for_an_untouched_message() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.to_bytes() == ADT_A01


def test_repr_never_contains_field_content() -> None:
    msg = Hl7Message.parse(ADT_A01)
    text = repr(msg)
    assert "SYNTHPATIENT" not in text
    assert "9000001" not in text


def test_len_counts_segments() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert len(msg) == 6  # MSH, EVN, PID, NK1, NK1, PV1


# -- repetitions ------------------------------------------------------------------------------------


def test_repetitions_of_a_repeating_field() -> None:
    raw = ADT_A01.replace(b"9001^SYNTHDOC^ATTENDING", b"9001^SYNTHDOC^ATTENDING~9002^SYNTHDOC2^COATTENDING")
    msg = Hl7Message.parse(raw)
    reps = msg.repetitions("PV1.7.2")
    assert reps == ("SYNTHDOC", "SYNTHDOC2")


def test_repetitions_needs_a_field_index() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.repetitions("PID")


# -- custom delimiters ------------------------------------------------------------------------------


def test_custom_delimiters_round_trip_and_are_used_for_field_access() -> None:
    raw = (
        b"MSH!@$\\*!TRIBUTARY-SYNTH!SYNTH-HOSPITAL!SYNTH-ADT-RECV!SYNTH-RECV-FACILITY!"
        b"20260106140000!!ADT@A01@ADT_A01!SYNTHMSG00005!P!2.5.1\r"
        b"PID!1!!9000004^^^SYNTH-HOSPITAL^MR!!SYNTHPATIENT@EPSILON!!19850707!M\r"
    )
    msg = Hl7Message.parse(raw)
    assert msg.delimiters.field == "!"
    assert msg.delimiters.component == "@"
    assert msg["PID.5.1"] == "SYNTHPATIENT"
    assert msg["PID.5.2"] == "EPSILON"
    assert msg.to_bytes() == raw


# -- escaping on read --------------------------------------------------------------------------------


def test_field_access_unescapes_reserved_characters() -> None:
    raw = ADT_A01.replace(b"SYNTHPATIENT^ALPHA^Q", rb"SYNTHPATIENT\S\ALPHA^Q")
    msg = Hl7Message.parse(raw)
    assert msg["PID.5.1"] == "SYNTHPATIENT^ALPHA"  # \S\ decoded to a literal component separator


# -- malformed input: typed errors, never a crash ------------------------------------------------


def test_empty_message_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"")


def test_non_msh_start_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"NOTHL7 this is not a message at all")


def test_truncated_msh_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"MS")


def test_bad_encoding_raises_typed_error_not_unicode_decode_error() -> None:
    # A field containing a byte invalid in the *requested* encoding raises on access, typed, not raw.
    raw = ADT_A01.replace(b"SYNTHPATIENT", b"SYNTHPATIENT\xff\xfe")
    bad = Hl7Message.parse(raw, encoding="ascii")
    with pytest.raises(PayloadDecodeError):
        _ = bad["PID.5.1"]


def test_malformed_message_error_never_contains_message_bytes() -> None:
    with pytest.raises(MalformedMessageError) as caught:
        Hl7Message.parse(b"")
    assert "TRIBUTARY-SYNTH" not in str(caught.value)


# -- mutation (copy-on-write) ------------------------------------------------------------------------


def test_with_field_replaces_only_the_addressed_component() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.2", "BETA")
    assert updated["PID.5.1"] == "SYNTHPATIENT"
    assert updated["PID.5.2"] == "BETA"
    assert updated["PID.5.3"] == "Q"
    # Every other segment, byte-identical.
    assert updated.segment("EVN").raw_bytes() == msg.segment("EVN").raw_bytes()
    assert updated.segment("NK1", 1).raw_bytes() == msg.segment("NK1", 1).raw_bytes()


def test_with_field_escapes_reserved_characters_in_the_new_value() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.2", "A^B")
    assert updated["PID.5.2"] == "A^B"  # round-trips through escape/unescape
    assert b"A\\S\\B" in updated.segment("PID").raw_bytes()


def test_with_field_does_not_mutate_the_original() -> None:
    msg = Hl7Message.parse(ADT_A01)
    msg.with_field("PID.5.2", "CHANGED")
    assert msg["PID.5.2"] == "ALPHA"
    assert msg.to_bytes() == ADT_A01


def test_with_field_on_a_new_component_pads_missing_ones() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.5", "NEWCOMP")
    assert updated["PID.5.5"] == "NEWCOMP"
    assert updated["PID.5.4"] == ""


def test_with_field_cannot_set_msh1() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.with_field("MSH.1", "#")


def test_with_field_cannot_set_msh2() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.with_field("MSH.2", "@$\\*")


def test_with_field_on_a_missing_segment_occurrence_raises() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.with_field("OBX.5", "1")


# -- raw(): still-escaped wire text, no unescape ---------------------------------------------------


def test_raw_does_not_unescape() -> None:
    raw = ADT_A01.replace(b"SYNTHPATIENT^ALPHA^Q", rb"SYNTHPATIENT\S\ALPHA^Q")
    msg = Hl7Message.parse(raw)
    assert msg.raw("PID.5.1") == "SYNTHPATIENT\\S\\ALPHA"  # unlike msg["PID.5.1"], the \S\ stays literal
    assert msg["PID.5.1"] == "SYNTHPATIENT^ALPHA"


def test_raw_on_msh1_and_msh2_matches_get() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.raw("MSH.1") == msg["MSH.1"] == "|"
    assert msg.raw("MSH.2") == msg["MSH.2"] == "^~\\&"


def test_raw_returns_default_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.raw("OBX.5") is None
    assert msg.raw("OBX.5", "fallback") == "fallback"


# -- security regression: mixed segment separators are rejected, not silently misread --------------


def test_bare_cr_separated_message_parses_every_segment() -> None:
    msg = Hl7Message.parse(ADT_A01)  # the fixture is bare-CR throughout
    assert len(msg) == 6


def test_crlf_separated_message_parses_every_segment() -> None:
    raw = ADT_A01.replace(b"\r", b"\r\n")
    msg = Hl7Message.parse(raw)
    assert len(msg) == 6
    assert msg["PID.5.1"] == "SYNTHPATIENT"


def test_mixed_cr_and_crlf_separators_are_rejected() -> None:
    """The bug a code and security review both flagged: a global "does CRLF occur anywhere" check
    misread a bare-CR message that happened to carry one CRLF byte pair, silently merging every
    following bare-CR-terminated segment into one. Now a typed reject instead of silent corruption.
    """
    raw = ADT_A01.replace(b"NK1|1|SYNTHCONTACT^ONE|SPO\r", b"NK1|1|SYNTHCONTACT^ONE|SPO\r\n", 1)
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(raw)


def test_a_single_trailing_cr_with_no_other_separator_still_parses() -> None:
    """A one-segment message's only `\\r` is the final terminator, which is ambiguous evidence either
    way and must not itself trigger the mixed-separator reject."""
    msg = Hl7Message.parse(b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|||20260101000000||ADT^A01|1|P|2.5.1\r")
    assert len(msg) == 1


# -- security regression: Segment.name is always the fixed 3-character prefix ----------------------


def test_segment_name_is_the_first_three_characters_even_with_no_field_separator() -> None:
    """`Hl7Message`'s own segment index already used the fixed 3-character prefix; the bug was in
    `Segment.name` specifically (it searched for a field separator instead), so this reads `.name`
    directly rather than only `segment_count`, which would not have caught the regression."""
    raw = b"MSH|^~\\&|TRIBUTARY-SYNTH|||||||ADT^A01|1|P|2.5.1\rZZZNOFIELDSEP\r"
    msg = Hl7Message.parse(raw)
    segment = msg.segment("ZZZ", 1)
    assert segment.name == "ZZZ"


# -- security regression: a decode error never carries the offending bytes onward ------------------


def test_payload_decode_error_does_not_chain_the_original_exception() -> None:
    raw = ADT_A01.replace(b"SYNTHPATIENT", b"SYNTHPATIENT\xff\xfe")
    bad = Hl7Message.parse(raw, encoding="ascii")
    with pytest.raises(PayloadDecodeError) as caught:
        _ = bad["PID.5.1"]
    assert caught.value.__cause__ is None


# -- defensive caps: max_bytes / max_segments -------------------------------------------------------


def test_max_bytes_default_matches_the_envelope_inline_limit() -> None:
    assert Hl7Message.INLINE_LIMIT_BYTES == 1_000_000


def test_parse_rejects_a_message_over_max_bytes() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(ADT_A01, max_bytes=10)


def test_parse_accepts_a_message_at_or_under_max_bytes() -> None:
    Hl7Message.parse(ADT_A01, max_bytes=len(ADT_A01))


def test_max_bytes_none_disables_the_check() -> None:
    Hl7Message.parse(ADT_A01, max_bytes=None)


def test_parse_rejects_a_message_over_max_segments() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(ADT_A01, max_segments=2)


def test_parse_accepts_a_message_at_or_under_max_segments() -> None:
    Hl7Message.parse(ADT_A01, max_segments=6)


# -- code review test gap: a later segment's lazy decode failure is a typed error, not a crash -----


def test_a_field_access_after_a_successful_parse_can_still_raise_on_a_later_segment() -> None:
    """`Hl7Message.parse` only reads MSH eagerly (module docstring): a message that parses cleanly can
    still fail, loudly and with a typed error, the first time something touches a segment whose bytes
    are not valid in the requested encoding -- exactly what a downstream mapping step would hit, and it
    must propagate rather than being swallowed anywhere in this package."""
    raw = ADT_A01.replace(b"SYNTHPATIENT", b"SYNTHPATIENT\xff\xfe")
    msg = Hl7Message.parse(raw, encoding="ascii")  # MSH decodes fine; parse() succeeds
    assert msg.message_type() == ("ADT", "A01", "ADT_A01")  # MSH-only access: still fine
    with pytest.raises(PayloadDecodeError):
        msg.segment("PID").field(5, component=1)  # PID's bytes are not valid ascii
