"""`tb-error-code` vocabulary: the grammar, and the codes the engine reserves for itself.

`contracts/channel-spec/v0` README 3.3.2: a step may raise `Reject(code)`, but not with a code the engine
uses, "so an alert on an engine code stays meaningful". An alert on `HEADER_PARSE_ERROR` means a broken
producer; if a channel's own step could raise it too, the alert would page for a validation rule.

There is no contract-level registry of engine codes yet, so this module is the list: every code the
runtime, the built-in steps and the loaders emit today, plus three prefixes that whole families live
under (`KEY_` from the key evaluator, `HTTP_` from the HTTP loader, `TB_` for the engine's namespace).
It is a deny-list for author mistakes, not a security boundary: steps are trusted code.
"""

from __future__ import annotations

import re
from typing import Final

#: `tb-error-code` as a step may raise it (README 3.3.2). The events schema admits one character more
#: loosely (`{0,63}`); a step code is held to three characters minimum so it says something.
REJECT_CODE: Final = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")

#: Families of engine codes. A new code under one of these is reserved without editing this file.
RESERVED_PREFIXES: Final = ("KEY_", "HTTP_", "TB_")

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
        "STEP_ERROR",
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
        "TRANSFORM_ERROR",
        "ENVELOPE_INVALID",
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
