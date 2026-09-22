"""Building the MLLP-framed HL7 ACK the receiver writes back to the sender.

This is the receiver's own reply envelope, not general HL7 v2 construction — `tributary-hl7v2`
(`healthcare-standards-engineer`, not built in this wave) owns that. `text` is a channel-spec-facing
reason (`"parse-error"`, `"queue-full"`, `"unavailable"`, ...), always a short fixed string chosen by
this package, never sender- or payload-derived, so it cannot carry PHI or forge extra ACK fields.
"""

from __future__ import annotations

import time
import uuid

from .framing import CR, EB, SB

_EB_CR = EB + CR


def build_ack(code: str, control_id: str, text: str = "") -> bytes:
    """An MLLP-framed HL7 ACK with `MSA-1` = `code` (`AA`, `AE` or `AR`) and `MSA-2` = `control_id`."""
    ts = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    body = f"MSH|^~\\&|TRIBUTARY|TRIBUTARY|||{ts}||ACK|{uuid.uuid4().hex[:12]}|P|2.5\rMSA|{code}|{control_id}|{text}\r"
    return SB + body.encode("ascii", "replace") + _EB_CR
