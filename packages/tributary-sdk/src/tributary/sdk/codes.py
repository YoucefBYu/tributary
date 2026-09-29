"""`tb-error-code` vocabulary: the grammar, and the codes the engine reserves for itself.

`contracts/channel-spec/v0` README 3.3.2: a step may raise `Reject(code)`, but not with a code the engine
uses, "so an alert on an engine code stays meaningful". An alert on `HEADER_PARSE_ERROR` means a broken
producer; if a channel's own step could raise it too, the alert would page for a validation rule.

**The contract is the source** (`contracts/envelope/v0/error-codes.json`, README 2.3.2). This module
mirrors it and `tests/test_codes_contract.py` checks both directions: `ENGINE_CODES` equals the
registry's `codes` plus `log_only`, and `RESERVED_PREFIXES` equals `reserved_prefixes`. A new exact
code is added to the contract first, through the architect; a member of a reserved family (`TB_...`,
`HTTP_503`) needs no change. It is a deny-list for author mistakes, not a security boundary: steps
are trusted code.
"""

from __future__ import annotations

import re
from typing import Final

#: `tb-error-code` as a step may raise it (README 3.3.2). The events schema admits one character more
#: loosely (`{0,63}`); a step code is held to three characters minimum so it says something.
REJECT_CODE: Final = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")

#: Families of engine codes (error-codes.json `reserved_prefixes`). A new member needs no change.
RESERVED_PREFIXES: Final = (
    "TB_",
    "KEY_",
    "HTTP_",
    "FHIR_",
    "PG_",
    "CH_",
    "SFTP_",
    "SSH_",
    "S3_",
    "MLLP_",
    "KAFKA_",
    "REPLY_",  # set 1.2: synchronous reply path (channel-spec README 3.2.6)
)

#: A step raised on every attempt (stepRetries exhausted). Envelope 2.3.2: the contract's spelling.
TRANSFORM_ERROR: Final = "TRANSFORM_ERROR"
#: Log-only (error-codes.json `log_only`): never in a header or an event.
ENVELOPE_INVALID: Final = "ENVELOPE_INVALID"
#: Members of the engine's `TB_` family, registered by name in error-codes.json (2026-09-25).
#: A record the pipeline built fails the envelope at produce time (too large, too many headers).
TB_OUTPUT_ENVELOPE_INVALID: Final = "TB_OUTPUT_ENVELOPE_INVALID"
#: One `handle` outlasted its time budget, which is sized under `max.poll.interval.ms`.
TB_PIPELINE_TIME_BUDGET: Final = "TB_PIPELINE_TIME_BUDGET"

#: A dead letter that cannot be written (its record exceeds the envelope even with registered headers
#: only): the partition halts. A member of the reserved `TB_` family, registered by name in
#: error-codes.json (2026-09-28).
TB_DLQ_UNWRITABLE: Final = "TB_DLQ_UNWRITABLE"

# Synchronous reply mode (set 1.2, channel-spec README 3.2.6, envelope README 2.3.3). The four
# receiver codes reach the sender and metrics only, never a header or event; they are registered so a
# channel's `Reject` cannot borrow them. The two worker codes are emitted by `tributary.runtime.reply`.
REPLY_TIMEOUT: Final = "REPLY_TIMEOUT"
REPLY_ROUTE_LOST: Final = "REPLY_ROUTE_LOST"
REPLY_BUSY: Final = "REPLY_BUSY"
REPLY_UNAVAILABLE: Final = "REPLY_UNAVAILABLE"
#: `body: step` and no pipeline step called `ctx.reply` on a message that was not dropped.
REPLY_BODY_MISSING: Final = "REPLY_BODY_MISSING"
#: `tb-reply-partition` or `tb-reply-deadline` malformed on a `raw` record: the reply is disabled,
#: the message is processed normally (envelope 2.3.3). Carried by a `replied` event only.
REPLY_ROUTE_INVALID: Final = "REPLY_ROUTE_INVALID"
#: Set 1.4: a reply past `tb-reply-deadline` plus the skew allowance; not produced, message processed.
#: Carried by a `replied` event with `status: dropped` only; never a header, never sender-facing.
REPLY_EXPIRED: Final = "REPLY_EXPIRED"

# Built-in step codes. Named here so the built-ins and the reserved list cannot drift apart.
FILTER_UNRESOLVED: Final = "FILTER_UNRESOLVED"
VALIDATE_NOT_HL7: Final = "VALIDATE_NOT_HL7"
VALIDATE_SEGMENT_MISSING: Final = "VALIDATE_SEGMENT_MISSING"
VALIDATE_VERSION_MISMATCH: Final = "VALIDATE_VERSION_MISMATCH"
SPLIT_TOO_LARGE: Final = "SPLIT_TOO_LARGE"
SPLIT_INPUT_INVALID: Final = "SPLIT_INPUT_INVALID"

ENGINE_CODES: Final = frozenset(
    {
        # envelope/v0 2.3
        "HEADER_PARSE_ERROR",
        "RECORD_UNDECODABLE",
        # runtime: executor, worker, producer, transactions
        TRANSFORM_ERROR,
        "STEP_ERROR",  # reserved: the pre-fix spelling of TRANSFORM_ERROR (envelope 2.3.2)
        "PIPELINE_EMPTY_RESULT",
        "PRODUCER_QUEUE_FULL",
        "PRODUCE_REFUSED",
        "PRODUCE_TIMEOUT",
        "PRODUCE_FAILED",
        "TRANSACTION_ABORTED",
        # loaders
        "CIRCUIT_OPEN",
        "CONNECT_FAILED",
        "TLS_VERIFY_FAILED",
        "TRANSPORT_ERROR",
        "TIMEOUT",
        "PAYLOAD_SHAPE_INVALID",
        ENVELOPE_INVALID,  # log_only
        TB_OUTPUT_ENVELOPE_INVALID,
        TB_PIPELINE_TIME_BUDGET,
        TB_DLQ_UNWRITABLE,
        # synchronous reply (set 1.2)
        REPLY_TIMEOUT,
        REPLY_ROUTE_LOST,
        REPLY_BUSY,
        REPLY_UNAVAILABLE,
        REPLY_BODY_MISSING,
        REPLY_ROUTE_INVALID,
        REPLY_EXPIRED,  # set 1.4
        # set 1.3: fixed names the wave 2 loaders emit under reserved prefixes (envelope README
        # 2.3.2 rule 2), plus the receivers' mTLS client allow-list refusal (channel-spec 3.2.7)
        "HTTP_REDIRECT_REFUSED",
        "FHIR_BUNDLE_INVALID",
        "FHIR_ENTRY_FAILED",
        "FHIR_RESPONSE_INVALID",
        "CH_UNKNOWN",
        "SFTP_NAME_CONFLICT",
        "SSH_HOST_KEY_MISMATCH",
        "SSH_AUTH_FAILED",
        "S3_KEY_CONFLICT",
        "S3_KEY_VANISHED",
        "S3_NO_CREDENTIALS",
        "S3_UNKNOWN",
        "MLLP_AE",
        "MLLP_AR",
        "MLLP_TIMEOUT",
        "MLLP_NO_ACK",
        "TLS_PEER_NOT_ALLOWED",  # log line and metrics only; never a header or event
        # built-in steps (channel-spec README 3.3.2)
        FILTER_UNRESOLVED,
        VALIDATE_NOT_HL7,
        VALIDATE_SEGMENT_MISSING,
        VALIDATE_VERSION_MISMATCH,
        SPLIT_TOO_LARGE,
        SPLIT_INPUT_INVALID,
    }
)


def is_engine_code(code: str) -> bool:
    """True when `code` belongs to the engine: listed above or under a reserved prefix."""
    return code in ENGINE_CODES or code.startswith(RESERVED_PREFIXES)
