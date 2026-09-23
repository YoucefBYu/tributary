"""Hypothesis property tests: the round-trip guarantee and escape-handling, across generated messages.

`Hl7Message.parse(raw).to_bytes() == raw` is the core promise (W2-1 verify line). These tests build
synthetic-shaped HL7 v2 messages from generated segments, fields and delimiters rather than hand-typed
examples, so the guarantee is checked against a much larger space than the fixture corpus covers.
"""

from __future__ import annotations

import contextlib

from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.hl7v2.errors import MalformedMessageError
from tributary.hl7v2.message import Hl7Message

#: Printable ASCII, excluding the standard delimiters and CR/LF, safe to drop into a field verbatim.
_FIELD_TEXT = st.text(
    alphabet=st.characters(min_codepoint=0x20, max_codepoint=0x7E, exclude_characters="|^~\\&\r\n"), max_size=12
)

_SEGMENT_NAMES = st.sampled_from(["PID", "PV1", "NK1", "OBX", "OBR", "ZPD", "ZZZ"])


@st.composite
def _messages(draw: st.DrawFn) -> bytes:
    control_id = draw(
        st.text(
            alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E, exclude_characters="|^~\\&"),
            min_size=1,
            max_size=10,
        )
    )
    msh = f"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|SYNTH-RECV|SYNTH-RECV-FAC|20260101000000||ADT^A01^ADT_A01|{control_id}|P|2.5.1"
    extra_segments = draw(st.lists(st.tuples(_SEGMENT_NAMES, st.lists(_FIELD_TEXT, max_size=5)), max_size=6))
    lines = [msh]
    for name, fields in extra_segments:
        lines.append("|".join([name, *fields]))
    return ("\r".join(lines) + "\r").encode("ascii")


@settings(max_examples=200)
@given(_messages())
def test_untouched_messages_round_trip_byte_identically(raw: bytes) -> None:
    msg = Hl7Message.parse(raw)
    assert msg.to_bytes() == raw


@settings(max_examples=100)
@given(_messages())
def test_round_trip_survives_reading_every_field(raw: bytes) -> None:
    """Reading fields is supposed to be side-effect free: touching every field still round-trips."""
    msg = Hl7Message.parse(raw)
    for segment_name in ("PID", "PV1", "NK1", "OBX", "OBR", "ZPD", "ZZZ", "MSH"):
        for segment in msg.segments(segment_name):
            for field_number in range(1, 8):
                segment.field(field_number)
    assert msg.to_bytes() == raw


@settings(max_examples=100)
@given(_messages(), st.text(alphabet="ab|^&~\\", max_size=10))
def test_with_field_then_read_back_recovers_the_value(raw: bytes, value: str) -> None:
    msg = Hl7Message.parse(raw)
    if msg.segment_count("PID") == 0:
        return
    updated = msg.with_field("PID.3.1", value)
    assert updated["PID.3.1"] == value


@settings(max_examples=100)
@given(_messages())
def test_with_field_leaves_every_other_segment_byte_identical(raw: bytes) -> None:
    msg = Hl7Message.parse(raw)
    if msg.segment_count("PID") == 0:
        return
    updated = msg.with_field("PID.3.1", "X")
    for name in ("MSH", "PV1", "NK1", "OBX", "OBR", "ZPD", "ZZZ"):
        originals = msg.segments(name)
        updates = updated.segments(name)
        assert len(originals) == len(updates)
        for before, after in zip(originals, updates, strict=True):
            assert before.raw_bytes() == after.raw_bytes()


@given(st.binary(max_size=200))
def test_parse_never_raises_anything_but_hl7error_or_succeeds(raw: bytes) -> None:
    """Arbitrary bytes either parse or raise a typed `Hl7Error` subclass -- never a bare crash."""
    with contextlib.suppress(MalformedMessageError):
        Hl7Message.parse(raw)
