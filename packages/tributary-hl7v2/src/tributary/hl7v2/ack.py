"""Build an HL7 v2 ACK (`MSH` + `MSA`) in reply to a received message.

Only ever reads `MSH` fields from the source message: sending/receiving application and facility,
message type, version, processing id. Never touches `PID`, `OBX` or any other segment, so an ACK cannot
carry patient data forward even if a caller is careless with `text` -- which stays the caller's
responsibility: `MSA-3` reaches a sender-facing reply, and invariant 6 treats a sender-facing string the
same as a log line, so it must be a short static code, never interpolated from payload content.
"""

from __future__ import annotations

from typing import Literal, get_args

from .errors import AckBuildError
from .escaping import escape
from .message import Hl7Message

AckCode = Literal["AA", "AE", "AR", "CA", "CE", "CR"]
_VALID_CODES: frozenset[str] = frozenset(get_args(AckCode))


def build_ack(
    message: Hl7Message,
    *,
    code: AckCode,
    control_id: str,
    timestamp: str,
    text: str | None = None,
    processing_id: str | None = None,
) -> Hl7Message:
    """Build the ACK for `message`.

    `control_id` and `timestamp` are caller-supplied rather than generated here, the same way
    `tributary.sdk.Context.now()` and id minting are injected everywhere else in this codebase: a step
    that builds an ACK stays testable without the wall clock or a UUID source. `processing_id` defaults
    to echoing the source message's `MSH-11`, falling back to `"P"` (production) when that is absent.

    Uses the source message's own delimiters (echoed back, never hardcoded `|^~\\&`), and reverses
    `MSH-3`..`MSH-6`: the ACK's sending application/facility is the original *receiving* one, and vice
    versa, per the standard's reply convention.

    Raises `AckBuildError` for a `code` outside the standard's ack/enhanced-ack vocabulary -- `AckCode`
    is a `Literal` for static checking, but a bundle step calling this with an untyped string (from a
    channel spec's `with:` block, say) gets a typed rejection rather than a malformed MSA-1 on the wire.
    """
    if code not in _VALID_CODES:
        raise AckBuildError(f"invalid ack code {code!r}: must be one of {sorted(_VALID_CODES)}")
    delimiters = message.delimiters
    sending_app = message.get("MSH.3") or ""
    sending_facility = message.get("MSH.4") or ""
    receiving_app = message.get("MSH.5") or ""
    receiving_facility = message.get("MSH.6") or ""
    original_control_id = message.get("MSH.10") or ""
    version = message.get("MSH.12") or ""
    processing = processing_id if processing_id is not None else (message.get("MSH.11") or "P")

    _, trigger_event, structure = message.message_type()
    if trigger_event and structure:
        ack_type = f"ACK^{trigger_event}^ACK"  # v2.4+ style: mirror the original message's structure
    elif trigger_event:
        ack_type = f"ACK^{trigger_event}"
    else:
        ack_type = "ACK"  # tolerate a source MSH-9 with no trigger event at all

    encoding_chars = f"{delimiters.component}{delimiters.repetition}{delimiters.escape}{delimiters.subcomponent}"
    field_sep = delimiters.field

    msh_fields = [
        "MSH",
        encoding_chars,
        escape(receiving_app, delimiters),
        escape(receiving_facility, delimiters),
        escape(sending_app, delimiters),
        escape(sending_facility, delimiters),
        escape(timestamp, delimiters),
        "",  # MSH-8: security, not used
        ack_type,
        escape(control_id, delimiters),
        escape(processing, delimiters),
        escape(version, delimiters),
    ]
    msh_segment = field_sep.join(msh_fields)

    msa_fields = ["MSA", code, escape(original_control_id, delimiters)]
    if text is not None:
        msa_fields.append(escape(text, delimiters))
    msa_segment = field_sep.join(msa_fields)

    raw = b"\r".join(part.encode(message.encoding) for part in (msh_segment, msa_segment))
    return Hl7Message.parse(raw, encoding=message.encoding)
