from __future__ import annotations

from tributary.receivers.mllp.ack import build_ack
from tributary.receivers.mllp.framing import CR, EB, SB


def test_build_ack_is_correctly_framed() -> None:
    out = build_ack("AA", "C1")
    assert out.startswith(SB)
    assert out.endswith(EB + CR)


def test_build_ack_msa_fields() -> None:
    out = build_ack("AE", "C1", "parse-error")
    body = out[1:-2].decode("ascii")
    msa = next(line for line in body.split("\r") if line.startswith("MSA|"))
    fields = msa.split("|")
    assert fields[1] == "AE"
    assert fields[2] == "C1"
    assert fields[3] == "parse-error"


def test_build_ack_never_embeds_raw_control_characters_from_text() -> None:
    # text is always a fixed reason string the package chooses; this just confirms the framing
    # bytes used by MLLP itself cannot appear inside the body this function builds.
    out = build_ack("AA", "C1")
    assert out[1:-2].count(SB) == 0
    assert out[1:-2].count(EB + CR) == 0
