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
RESERVED_PREFIXES: Final = ("TB_", "KEY_", "HTTP_", "FHIR_", "PG_", "CH_", "SFTP_", "SSH_", "S3_", "MLLP_", "KAFKA_")

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
#: only): the partition halts. A member of the reserved `TB_` family, so usable without a registry
#: change (envelope 2.3.2 rule 2); registration by name is requested, and until then it is deliberately
#: not in `ENGINE_CODES`, which mirrors the registry's exact names.
TB_DLQ_UNWRITABLE: Final = "TB_DLQ_UNWRITABLE"

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
