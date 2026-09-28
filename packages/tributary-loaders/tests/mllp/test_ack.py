"""`parse_ack`: reads `MSA-1`/`MSA-2` only, never raises, and strips HL7 separators the same way
`framing.control_id` does (invariant 6: this value only ever reaches a log as a match/mismatch bool)."""

from __future__ import annotations

from tributary.loaders.mllp.ack import parse_ack


def test_parses_aa_and_control_id() -> None:
    ack = parse_ack(b"MSH|^~\\&|RECV\rMSA|AA|CTRL-123")
    assert ack.code == "AA"
    assert ack.control_id == "CTRL-123"


def test_parses_ae() -> None:
    ack = parse_ack(b"MSH|^~\\&\rMSA|AE|CTRL-123|unexpected error")
    assert ack.code == "AE"
    assert ack.control_id == "CTRL-123"


def test_no_msa_segment_returns_empty_result() -> None:
    ack = parse_ack(b"MSH|^~\\&|RECV")
    assert ack.code == ""
    assert ack.control_id == ""


def test_never_raises_on_garbage_bytes() -> None:
    ack = parse_ack(b"\x00\x01\xff not hl7 at all")
    assert ack.code == ""


def test_msa_with_no_control_id_field() -> None:
    ack = parse_ack(b"MSA|AA")
    assert ack.code == "AA"
    assert ack.control_id == ""


def test_control_id_strips_hl7_separators() -> None:
    ack = parse_ack(b"MSA|AA|CTRL|123")
    assert "|" not in ack.control_id


def test_a_segment_merely_starting_with_msa_is_not_read_as_the_msa_segment() -> None:
    """Security review, Low: `segment[:3] != b"MSA"` also matched a look-alike segment such as
    `MSAX|...` -- only a segment whose fourth byte is the real field separator is the actual `MSA`
    segment."""
    ack = parse_ack(b"MSAX|AA|CTRL-123")
    assert ack.code == ""
    assert ack.control_id == ""


def test_the_real_msa_segment_after_a_look_alike_is_still_found() -> None:
    ack = parse_ack(b"MSAX|AA|CTRL-999\rMSA|AA|CTRL-123")
    assert ack.code == "AA"
    assert ack.control_id == "CTRL-123"
