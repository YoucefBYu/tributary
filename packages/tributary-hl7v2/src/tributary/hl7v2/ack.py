"""Build an HL7 v2 ACK (`MSH` + `MSA`) in reply to a received message.

Only ever reads `MSH` fields from the source message: sending/receiving application and facility,
message type, version, processing id. Never touches `PID`, `OBX` or any other segment, so an ACK cannot
carry patient data forward even if a caller is careless with `text` -- which stays the caller's
responsibility: `MSA-3` reaches a sender-facing reply, and invariant 6 treats a sender-facing string the
same as a log line, so it must be a short static code, never interpolated from payload content.

**Security note (fixed after review).** Every source-derived field is echoed with `Hl7Message.raw`
(still-escaped wire text, never decoded) rather than `get` (decoded) plus a re-`escape`: a decode-then-
re-encode round trip is only as safe as `escape` is complete, and a security review found it was not
(bare LF was unescaped). Copying the raw wire text needs no re-escaping at all -- it was already valid
wire text. `MSH-9`'s trigger event is additionally **allowlisted**, not merely escaped: a sender-supplied
message type is never spliced into the ACK's own `MSH-9` verbatim, because the reviewers' proof of
concept showed an escaped-but-unvalidated trigger event injecting a second `MSA` segment, so `MSA-1`
(the sender-visible accept/reject code) no longer necessarily matches the `code` this function was asked
to send. An allowlist closes that class of bug outright rather than relying on `escape` staying correct
forever.
"""

from __future__ import annotations

import re
from typing import Literal, get_args

from .errors import AckBuildError
from .escaping import escape
from .message import Hl7Message

AckCode = Literal["AA", "AE", "AR", "CA", "CE", "CR"]
_VALID_CODES: frozenset[str] = frozenset(get_args(AckCode))

#: A trigger event is three uppercase letters or digits (`A01`, `R01`, `O01`, ...) in every HL7 v2
#: version this package targets. Anything else is not treated as a trigger event at all -- the ACK's own
#: `MSH-9` becomes bare `"ACK"` rather than embedding a value that failed the check.
_TRIGGER_EVENT = re.compile(r"^[A-Z0-9]{3}$")


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

    Raises `AckBuildError` for: a `code` outside the standard's ack/enhanced-ack vocabulary; a source
    message with no `MSH-10` to echo into `MSA-2` (an ACK with an empty `MSA-2` looks well-formed but
    cannot be matched back to the message it acknowledges, by the sender or by anyone reading it later);
    or `control_id`/`processing_id`/`timestamp`/`text` containing a character not representable in
    `message.encoding` (an `ascii`-encoded message given a non-ASCII `text`, say) -- a typed rejection
    instead of a bare `UnicodeEncodeError` escaping this function.
    """
    if code not in _VALID_CODES:
        raise AckBuildError(f"invalid ack code {code!r}: must be one of {sorted(_VALID_CODES)}")

    delimiters = message.delimiters
    # Echoed verbatim: still-escaped wire text from the source message, never decoded then re-encoded.
    sending_app = message.raw("MSH.3") or ""
    sending_facility = message.raw("MSH.4") or ""
    receiving_app = message.raw("MSH.5") or ""
    receiving_facility = message.raw("MSH.6") or ""
    original_control_id = message.raw("MSH.10") or ""
    version = message.raw("MSH.12") or ""
    if not original_control_id:
        raise AckBuildError("source message has no MSH-10 to echo into MSA-2")

    if processing_id is not None:
        processing = escape(processing_id, delimiters)  # fresh, caller-supplied content
    else:
        processing = message.raw("MSH.11") or "P"  # echoed verbatim, or the literal default

    # MSH-9's trigger event is allowlisted, not echoed: see the module docstring.
    _, trigger_event, _structure = message.message_type()
    ack_type = f"ACK^{trigger_event}^ACK" if _TRIGGER_EVENT.match(trigger_event) else "ACK"

    encoding_chars = f"{delimiters.component}{delimiters.repetition}{delimiters.escape}{delimiters.subcomponent}"
    field_sep = delimiters.field

    msh_fields = [
        "MSH",
        encoding_chars,
        receiving_app,
        receiving_facility,
        sending_app,
        sending_facility,
        escape(timestamp, delimiters),  # fresh, caller-supplied content
        "",  # MSH-8: security, not used
        ack_type,
        escape(control_id, delimiters),  # fresh, caller-supplied content
        processing,
        version,
    ]
    msh_segment = field_sep.join(msh_fields)

    msa_fields = ["MSA", code, original_control_id]
    if text is not None:
        msa_fields.append(escape(text, delimiters))  # fresh, caller-supplied content
    msa_segment = field_sep.join(msa_fields)

    try:
        raw = b"\r".join(part.encode(message.encoding) for part in (msh_segment, msa_segment))
    except UnicodeEncodeError:
        # `from None`: a UnicodeEncodeError carries the offending character, which may be PHI-adjacent
        # caller-supplied text (invariant 6) -- see message.py's parallel fix for the same class of leak.
        raise AckBuildError(f"ack text is not encodable as {message.encoding!r}") from None
    return Hl7Message.parse(raw, encoding=message.encoding)
