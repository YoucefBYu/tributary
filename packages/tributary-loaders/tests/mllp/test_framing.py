"""`frame`, `control_id` and `FrameParser`: the four small, pure functions `client.py`'s module
docstring calls out as a second, independently tested copy of `tributary.receivers.mllp.framing`."""

from __future__ import annotations

import pytest
from tributary.loaders.mllp.framing import EB_CR, SB, FrameParser, control_id, frame

MSH = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|RECV|FAC2|20260101000000||ADT^A01|CTRL-123|P|2.5"


def test_frame_wraps_with_start_block_and_end_block_cr() -> None:
    wrapped = frame(b"hello")
    assert wrapped == SB + b"hello" + EB_CR


def test_frame_never_touches_the_payload_itself() -> None:
    payload = MSH + b"\rPID|1||123"
    assert frame(payload)[1:-2] == payload


def test_control_id_reads_msh_10() -> None:
    assert control_id(MSH) == "CTRL-123"


def test_control_id_of_a_message_with_too_few_fields_is_empty() -> None:
    assert control_id(b"MSH|^~\\&") == ""


def test_control_id_strips_hl7_separators_and_non_printable_characters() -> None:
    msh = b"MSH|^~\\&|A|B|C|D|E||F|CTRL|^~\\&|P|2.5"
    assert "|" not in control_id(msh)
    assert "^" not in control_id(msh)


def test_control_id_only_reads_the_first_segment() -> None:
    msg = MSH + b"\rPID|1||should-not-matter"
    assert control_id(msg) == "CTRL-123"


class TestFrameParser:
    def test_returns_none_until_a_complete_frame_arrives(self) -> None:
        parser = FrameParser(max_size=1024)
        assert parser.feed(SB) is None
        assert parser.feed(b"MSA|AA|CTRL-123") is None
        assert parser.feed(EB_CR) == b"MSA|AA|CTRL-123"

    def test_handles_one_call_delivering_the_whole_frame(self) -> None:
        parser = FrameParser(max_size=1024)
        assert parser.feed(SB + b"MSA|AA|CTRL-123" + EB_CR) == b"MSA|AA|CTRL-123"

    def test_handles_byte_at_a_time_delivery(self) -> None:
        parser = FrameParser(max_size=1024)
        whole = SB + b"MSA|AA|CTRL-123" + EB_CR
        result = None
        for i in range(len(whole)):
            result = parser.feed(whole[i : i + 1])
        assert result == b"MSA|AA|CTRL-123"

    def test_raises_value_error_once_max_size_is_exceeded_before_terminating(self) -> None:
        parser = FrameParser(max_size=8)
        with pytest.raises(ValueError):
            parser.feed(SB + b"0123456789")

    def test_ignores_bytes_before_the_start_block(self) -> None:
        parser = FrameParser(max_size=1024)
        assert parser.feed(b"garbage" + SB + b"MSA|AA|CTRL-123" + EB_CR) == b"MSA|AA|CTRL-123"
