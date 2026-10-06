"""Identifier and trace-context minting: UUIDv7 message ids and W3C `traceparent`.

Python 3.12 has no `uuid.uuid7`, so the layout of RFC 9562 section 5.7 is built here. The version and
variant nibbles are what the envelope schema checks (`^...-7[0-9a-f]{3}-[89ab]...`), and a v4 id or a
hex-without-hyphens id is the exact non-conformance the Phase 0 gate found in S3 (section 6, G1).

Nothing here derives an identifier from payload content. A correlation id in particular must not be a
hash of a patient identifier, because that makes it a stable pseudonym for that patient in every reply
record and archive row (envelope contract 3.2).
"""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid

_UNIX_TS_MS_BITS = 48
_RAND_A_BITS = 12
_RAND_B_BITS = 62


def uuid7(*, unix_ts_ms: int | None = None) -> str:
    """A time-ordered UUIDv7 in canonical lowercase form.

    Ordering is at millisecond granularity: two ids minted in the same millisecond sort arbitrarily.
    That is sufficient here — `tb-msg-id` is an identity and an idempotency key, never a sequence
    number, and per-key ordering comes from the partition, not from the id.
    """
    stamp = time.time_ns() // 1_000_000 if unix_ts_ms is None else unix_ts_ms
    if not 0 <= stamp < (1 << _UNIX_TS_MS_BITS):
        raise ValueError("unix_ts_ms does not fit in 48 bits")
    value = stamp << 80
    value |= 0x7 << 76
    value |= secrets.randbits(_RAND_A_BITS) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(_RAND_B_BITS)
    return str(uuid.UUID(int=value))


#: Split children per parent (channel-spec README 3.3.2 rule 4). The index is encoded in 4 bytes.
MAX_CHILD_INDEX = (1 << 32) - 1


def derived_uuid7(parent_id: str, index: int) -> str:
    """The deterministic UUIDv7 of child `index` (0-based) of the message `parent_id`.

    Channel-spec README 3.3.2 split rule 1: the timestamp is the parent's, and the remaining 74 bits come
    from `SHA-256(parent ‖ index)`, so redelivering the parent reproduces the same children and every
    loader's `tb-msg-id` idempotency absorbs them. A random id per run would make each replay a new set of
    messages, which is invariant 4 broken.

    The exact layout is pinned in channel-spec README 3.3.3, because two implementations must agree on
    it bit for bit. Input: the parent's 16 UUID bytes (RFC 9562 network order) followed by the index as
    a 4-byte big-endian unsigned integer. `D` is the first 10 bytes of the SHA-256 digest, an 80-bit
    big-endian integer, with bits counted from the most significant. `rand_a` is `D` bits 0-11
    (`D >> 68`); **bits 12-17 are discarded**; `rand_b` is `D` bits 18-79 (`D mod 2^62`). Nothing secret
    is involved: this is a derivation from a random parent id, not a keyed construction.
    """
    parent = uuid.UUID(parent_id)
    if parent.version != 7:
        raise ValueError("a derived id needs a UUIDv7 parent")
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index <= MAX_CHILD_INDEX:
        raise ValueError("child index must be an integer in [0, 2**32)")
    digest = int.from_bytes(hashlib.sha256(parent.bytes + index.to_bytes(4, "big")).digest()[:10], "big")
    value = (parent.int >> 80) << 80  # the parent's 48-bit timestamp
    value |= 0x7 << 76
    value |= ((digest >> 68) & 0xFFF) << 64
    value |= 0b10 << 62
    value |= digest & ((1 << _RAND_B_BITS) - 1)
    return str(uuid.UUID(int=value))


def new_correlation_id() -> str:
    """A fresh engine-minted correlation id (UUIDv4, never derived from the payload; envelope 3.2)."""
    return str(uuid.uuid4())


def _nonzero_hex(byte_count: int) -> str:
    """Random lowercase hex that is not all zeros, which the `traceparent` grammar forbids."""
    while True:
        candidate = secrets.token_bytes(byte_count)
        if any(candidate):
            return candidate.hex()


def new_traceparent(*, sampled: bool = True) -> str:
    """A fresh W3C `traceparent` with a new trace id and span id."""
    return f"00-{_nonzero_hex(16)}-{_nonzero_hex(8)}-{'01' if sampled else '00'}"


def child_traceparent(parent: str) -> str:
    """The same trace id and flags with a new span id: one span per hop (envelope contract 6, rule 2).

    An unparseable parent yields a fresh `traceparent` rather than an error. Trace context is
    observability: a malformed inbound value must not fail a message that is otherwise deliverable.
    """
    parts = parent.split("-")
    if len(parts) != 4 or parts[0] != "00" or len(parts[1]) != 32 or len(parts[3]) != 2:
        return new_traceparent()
    try:
        int(parts[1], 16)
        int(parts[3], 16)
    except ValueError:
        return new_traceparent()
    if parts[1] == "0" * 32:
        return new_traceparent()
    return f"00-{parts[1]}-{_nonzero_hex(8)}-{parts[3]}"
