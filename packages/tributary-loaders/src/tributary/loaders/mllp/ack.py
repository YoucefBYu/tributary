"""Parsing the HL7 ACK reply this loader reads back over the wire. Not general HL7 v2 parsing
(`tributary-hl7v2` owns that, not built in this wave) — only enough to read `MSA-1` (the ack code) and
`MSA-2` (the control id it is replying to), which is all `loader.py` needs to classify the outcome and
validate it against the control id of the message this loader sent (README 3.4.6's own point: "the
loader does not rewrite MSH", so the sent message's own `MSH-10` is the truth to check `MSA-2` against —
`framing.control_id` reads that side).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AckResult:
    code: str  # MSA-1: "AA", "AE", "AR", "CA", "CE", "CR", or "" if no MSA segment was found at all
    control_id: str  # MSA-2, bounded and stripped the same way framing.control_id is


def parse_ack(msg: bytes) -> AckResult:
    """Finds the `MSA` segment (segments are `\\r`-terminated) and reads its first two fields. Never
    raises: an ACK this loader cannot parse is `AckResult("", "")`, which `loader.py` turns into
    `MLLP_NO_ACK` — a malformed reply is not this loader's payload to log (invariant 6), only its
    absence of an MSA."""
    for segment in msg.split(b"\r"):
        # `segment[:4] != b"MSA|"`, not `segment[:3] != b"MSA"` (security review, Low): the latter also
        # matched a segment like `MSAX|AA|...` -- any segment merely STARTING with the three bytes
        # `MSA`, not the real `MSA` segment (whose HL7 field separator makes the fourth byte `|`
        # exactly) -- letting a made-up, look-alike segment be read as the actual acknowledgement.
        if segment[:4] != b"MSA|":
            continue
        fields = segment.split(b"|")
        code = fields[1].decode("ascii", "replace").strip() if len(fields) > 1 else ""
        raw_control_id = fields[2].decode("ascii", "replace")[:64] if len(fields) > 2 else ""
        control_id = "".join(ch for ch in raw_control_id if ch.isprintable() and ch not in "|^~\\&")
        return AckResult(code, control_id)
    return AckResult("", "")
