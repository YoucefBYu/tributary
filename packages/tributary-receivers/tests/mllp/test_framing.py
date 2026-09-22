"""Framing edge cases: partial frames, oversized messages, unterminated growth, control id
extraction. All synthetic, MSH-3 `TRIBUTARY-SYNTH`."""

from __future__ import annotations

from tributary.receivers.mllp.framing import FATAL, Fatal, FrameParser, Oversize, control_id, looks_like_hl7

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def frame(payload: bytes) -> bytes:
    return SB + payload + EB + CR


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def test_single_complete_frame() -> None:
    out = FrameParser(1_000_000).feed(frame(adt("C1")))
    assert out == [adt("C1")]


def test_split_across_two_feeds() -> None:
    p = FrameParser(1_000_000)
    whole = frame(adt("C1"))
    assert p.feed(whole[:10]) == []
    assert p.feed(whole[10:]) == [adt("C1")]


def test_two_frames_in_one_chunk() -> None:
    out = FrameParser(1_000_000).feed(frame(adt("C1")) + frame(adt("C2")))
    assert out == [adt("C1"), adt("C2")]


def test_garbage_before_start_block_is_discarded() -> None:
    out = FrameParser(1_000_000).feed(b"\x00\x00garbage" + frame(adt("C1")))
    assert out == [adt("C1")]


def test_oversize_complete_frame_reports_control_id() -> None:
    out = FrameParser(10).feed(frame(adt("C1")))
    assert len(out) == 1
    assert isinstance(out[0], Oversize)
    assert out[0].cid == "C1"


def test_unterminated_growth_past_max_is_fatal() -> None:
    p = FrameParser(10)
    out = p.feed(SB + b"x" * 100)
    assert out == [FATAL]
    assert p.fatal is True


def test_fatal_is_returned_only_once() -> None:
    p = FrameParser(10)
    p.feed(SB + b"x" * 100)
    assert p.feed(b"more") == []


def test_partial_frame_at_eof_is_never_returned() -> None:
    p = FrameParser(1_000_000)
    assert p.feed(frame(adt("C1"))[:-1]) == []  # never sees the trailing CR
    assert p.buffered() > 0


def test_control_id_strips_hl7_encoding_characters() -> None:
    # A raw "|" cannot appear inside MSH-10 without shifting field positions (it is the field
    # separator); the encoding characters this strips are the other four: ^ ~ \ &.
    assert control_id(adt("C^~\\&1")) == "C1"


def test_control_id_missing_field_returns_empty() -> None:
    assert control_id(b"MSH|too|short\r") == ""


def test_looks_like_hl7_accepts_msh_with_enough_fields() -> None:
    assert looks_like_hl7(adt("C1")) is True


def test_looks_like_hl7_rejects_garbage() -> None:
    assert looks_like_hl7(b"not an hl7 message at all") is False


def test_looks_like_hl7_rejects_too_few_fields() -> None:
    assert looks_like_hl7(b"MSH|a|b\r") is False


def test_fatal_singleton_type() -> None:
    assert isinstance(FATAL, Fatal)
