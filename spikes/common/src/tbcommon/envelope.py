"""The ADR-001 section 5.4 message envelope, as Kafka headers.

Spike-local approximation of the future ``contracts/`` envelope. The architect
owns the real thing; this exists so the Phase 0 spikes agree with each other.
Payload stays opaque bytes -- nothing here ever looks inside it, so no PHI can
leak into logs or test output through this module.
"""

from __future__ import annotations

import os
import secrets
import time
import uuid
from dataclasses import dataclass, field

Headers = list[tuple[str, bytes]]

# --- header names (ADR-001 5.4) ---------------------------------------------
H_MSG_ID = "tb-msg-id"
H_CHANNEL = "tb-channel"
H_CHANNEL_VERSION = "tb-channel-version"
H_SOURCE = "tb-source"
H_RECEIVED_AT = "tb-received-at"
H_CONTENT_TYPE = "tb-content-type"
H_CORRELATION_ID = "tb-correlation-id"
H_PARENT_ID = "tb-parent-id"
H_ATTEMPT = "tb-attempt"
H_TRACEPARENT = "traceparent"
H_CLAIM_CHECK = "tb-claim-check"

# Spike-only headers, used by S5 to make assertions without parsing payloads.
H_SEQ = "tb-x-seq"  # per-key monotonic sequence number, set by the generator
H_STAGE = "tb-x-stage"  # last stage that wrote this record


def uuid7() -> str:
    """UUIDv7 (time-ordered). Python 3.12's ``uuid`` module has no v7 yet."""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))


def new_traceparent() -> str:
    """W3C trace context, sampled."""
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


def child_traceparent(parent: str | None) -> str:
    """Keep the trace id, mint a new span id. Invariant: propagate traceparent."""
    if not parent:
        return new_traceparent()
    parts = parent.split("-")
    if len(parts) != 4 or len(parts[1]) != 32:
        return new_traceparent()
    return f"00-{parts[1]}-{secrets.token_hex(8)}-{parts[3]}"


@dataclass(slots=True)
class Envelope:
    """Decoded headers. Never holds the payload."""

    msg_id: str = field(default_factory=uuid7)
    channel: str = "spike"
    channel_version: int = 1
    source: str = field(default_factory=lambda: f"spike/{os.getpid()}")
    received_at: str = ""
    content_type: str = "application/hl7-v2+er7"
    correlation_id: str | None = None
    parent_id: str | None = None
    attempt: int = 0
    traceparent: str = field(default_factory=new_traceparent)
    claim_check: str | None = None
    seq: int | None = None
    stage: str | None = None

    def __post_init__(self) -> None:
        if not self.received_at:
            self.received_at = _utc_now()

    def to_headers(self) -> Headers:
        out: Headers = [
            (H_MSG_ID, self.msg_id.encode()),
            (H_CHANNEL, self.channel.encode()),
            (H_CHANNEL_VERSION, str(self.channel_version).encode()),
            (H_SOURCE, self.source.encode()),
            (H_RECEIVED_AT, self.received_at.encode()),
            (H_CONTENT_TYPE, self.content_type.encode()),
            (H_ATTEMPT, str(self.attempt).encode()),
            (H_TRACEPARENT, self.traceparent.encode()),
        ]
        for name, value in (
            (H_CORRELATION_ID, self.correlation_id),
            (H_PARENT_ID, self.parent_id),
            (H_CLAIM_CHECK, self.claim_check),
            (H_STAGE, self.stage),
        ):
            if value is not None:
                out.append((name, value.encode()))
        if self.seq is not None:
            out.append((H_SEQ, str(self.seq).encode()))
        return out

    @classmethod
    def from_headers(cls, headers: Headers | None) -> Envelope:
        h = decode(headers)
        return cls(
            msg_id=h.get(H_MSG_ID, ""),
            channel=h.get(H_CHANNEL, ""),
            channel_version=int(h.get(H_CHANNEL_VERSION, "1") or 1),
            source=h.get(H_SOURCE, ""),
            received_at=h.get(H_RECEIVED_AT, "") or _utc_now(),
            content_type=h.get(H_CONTENT_TYPE, ""),
            correlation_id=h.get(H_CORRELATION_ID),
            parent_id=h.get(H_PARENT_ID),
            attempt=int(h.get(H_ATTEMPT, "0") or 0),
            traceparent=h.get(H_TRACEPARENT) or new_traceparent(),
            claim_check=h.get(H_CLAIM_CHECK),
            seq=int(h[H_SEQ]) if H_SEQ in h else None,
            stage=h.get(H_STAGE),
        )

    def next_stage(self, stage: str) -> Envelope:
        """Envelope for the next hop: same message id, new span, stage recorded."""
        nxt = Envelope(
            msg_id=self.msg_id,
            channel=self.channel,
            channel_version=self.channel_version,
            source=self.source,
            received_at=self.received_at,
            content_type=self.content_type,
            correlation_id=self.correlation_id,
            parent_id=self.parent_id,
            attempt=self.attempt,
            traceparent=child_traceparent(self.traceparent),
            claim_check=self.claim_check,
            seq=self.seq,
            stage=stage,
        )
        return nxt

    def with_attempt(self, attempt: int) -> Envelope:
        nxt = self.next_stage(self.stage or "retry")
        nxt.attempt = attempt
        return nxt


def decode(headers: Headers | None) -> dict[str, str]:
    """Kafka headers -> dict. Last value wins, which is what librdkafka does."""
    if not headers:
        return {}
    out: dict[str, str] = {}
    for key, value in headers:
        if value is None:
            continue
        out[key] = value.decode("utf-8", errors="replace")
    return out


def _utc_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="milliseconds")
