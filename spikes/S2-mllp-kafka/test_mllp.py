"""Unit tests for MLLP framing edge cases. No sockets, no Kafka -- pure functions."""

from __future__ import annotations

from mllp import FATAL, FrameParser, Oversize, build_ack, control_id

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def frame(payload: bytes) -> bytes:
    return SB + payload + EB + CR


ADT = b"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|CTRL-1|P|2.5\rPID|1||X"


def test_single_frame_one_chunk() -> None:
    p = FrameParser(max_size=1024)
    out = p.feed(frame(ADT))
    assert out == [ADT]
    assert p.buffered() == 0


def test_frame_split_across_chunks() -> None:
    p = FrameParser(max_size=1024)
    whole = frame(ADT)
    mid = len(whole) // 2
    assert p.feed(whole[:mid]) == []
    assert p.feed(whole[mid:]) == [ADT]


def test_partial_frame_at_eof_never_returned() -> None:
    """A frame with no terminator is not a complete message: it must never surface,
    so the caller can never accidentally ACK it."""
    p = FrameParser(max_size=1024)
    out = p.feed(SB + ADT)  # no EB CR ever arrives
    assert out == []
    assert p.buffered() > 0
    assert not p.fatal


def test_multiple_frames_one_chunk_pipelined() -> None:
    p = FrameParser(max_size=1024)
    out = p.feed(frame(ADT) + frame(ADT + b"-2"))
    assert out == [ADT, ADT + b"-2"]


def test_garbage_before_start_block_discarded() -> None:
    p = FrameParser(max_size=1024)
    out = p.feed(b"\x00\x00garbage" + frame(ADT))
    assert out == [ADT]


def test_chunk_with_no_start_block_is_dropped() -> None:
    p = FrameParser(max_size=1024)
    out = p.feed(b"not mllp at all, no start byte")
    assert out == []
    assert p.buffered() == 0


def test_oversize_complete_frame_yields_control_id_not_payload() -> None:
    big = ADT + b"X" * 2000
    p = FrameParser(max_size=1024)
    out = p.feed(frame(big))
    assert len(out) == 1
    assert isinstance(out[0], Oversize)
    assert out[0].cid == "CTRL-1"


def test_unterminated_growth_beyond_max_is_fatal_once() -> None:
    p = FrameParser(max_size=16)
    out = p.feed(SB + b"x" * 100)  # never terminates, grows past max_size
    assert out == [FATAL]
    assert p.fatal is True
    assert p.buffered() == 0
    # further feeds must not raise or double-report; caller is expected to stop calling
    # once .fatal is set, but the parser itself must stay well-behaved.
    assert p.feed(b"more") == []


def test_exactly_max_size_is_not_oversize() -> None:
    payload = b"A" * 32
    p = FrameParser(max_size=32)
    out = p.feed(frame(payload))
    assert out == [payload]


def test_control_id_normal() -> None:
    assert control_id(ADT) == "CTRL-1"


def test_control_id_missing_fields_returns_empty() -> None:
    assert control_id(b"MSH|^~\\&|A|B") == ""


def test_control_id_no_cr_returns_empty_for_short_msh() -> None:
    assert control_id(b"MSH|^~\\&") == ""


def test_control_id_sanitizes_separators() -> None:
    # MSH-10 cannot contain a literal "|" (that would start a new field), but it can
    # legally contain component/sub-component separators; those must not survive into
    # the ACK's MSA-2, or a crafted control id could forge extra ACK fields.
    hostile = b"MSH|^~\\&|A|B|||ts||ADT^A01|EVIL^1~2\\3&4|P|2.5\r"
    cid = control_id(hostile)
    assert not set(cid) & set("|^~\\&")
    assert cid == "EVIL1234"


def test_control_id_truncated_to_64_chars() -> None:
    long_id = "Y" * 200
    msg = f"MSH|^~\\&|A|B|||ts||ADT^A01|{long_id}|P|2.5\r".encode()
    assert len(control_id(msg)) == 64


def test_build_ack_shape_and_msa() -> None:
    ack = build_ack("AA", "CTRL-1")
    assert ack[:1] == SB
    assert ack[-2:] == EB + CR
    assert b"\rMSA|AA|CTRL-1|\r" in ack


def test_build_ack_ar_carries_reason_text() -> None:
    ack = build_ack("AR", "CTRL-2", "too large")
    assert b"MSA|AR|CTRL-2|too large" in ack


def test_build_ack_empty_control_id_still_well_formed() -> None:
    ack = build_ack("AR", "", "too large")
    assert b"MSA|AR||too large" in ack
