"""`ctx.reply`: the rules a synchronous reply body must satisfy, shared by the runtime and the fake.

Channel-spec README 3.2.6 (set 1.2, W2-8). A pipeline step on a `body: step` channel supplies the
response payload with `ctx.reply(body, content_type=...)`. The rules live here, once, so `FakeContext`
enforces exactly what the pipeline worker enforces (the fakes module explains why a permissive fake is
worse than none):

1. At most once per message; a second call raises. A body set by an attempt that then raised is
   discarded by the runtime before the retry, so "once" holds per successful pipeline run.
2. **Pipeline steps only.** A destination step works on its own copy per destination, so N destinations
   could each answer; there the call raises.
3. `content_type` is a registered envelope type (envelope 3.1). For an `mllp` source it must be
   `application/hl7-v2+er7`.
4. `body` is at most 1,000,000 bytes: there is no claim-check on `reply`.

Calling it is otherwise always legal. Whether the body is used is decided later, by the worker: only
when the consumed record carries `tb-reply-partition` and the applied spec says `body: step`. A step
written for sync mode therefore keeps working across a hot reload to async.

**The body is payload** (invariant 6). `ReplyBody.__repr__` shows the media type and the size only, so a
failed assertion or a log of the object never prints it, and no error here quotes it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import headers as h
from .errors import ReplyError, safe_token

#: No claim-check on `reply`: the inline limit is the whole limit (channel-spec 3.2.6).
MAX_REPLY_BODY_BYTES = 1_000_000

#: The one type an `mllp` source may answer with: the receiver writes the body to the socket as is.
MLLP_REPLY_CONTENT_TYPE = "application/hl7-v2+er7"


@dataclass(frozen=True, slots=True)
class ReplyBody:
    """A body a step supplied through `ctx.reply`. The bytes never appear in `repr`."""

    body: bytes = field(repr=False)
    content_type: str

    def __repr__(self) -> str:
        return f"ReplyBody(content_type={self.content_type!r}, size={len(self.body)})"

    @property
    def size(self) -> int:
        return len(self.body)


def check_reply(
    body: object,
    *,
    content_type: object,
    destination: str | None,
    source_type: str | None,
) -> ReplyBody:
    """Validate one `ctx.reply` call and return the body to hold. Raises `ReplyError`.

    `destination` is the context's destination: not `None` means a destination step, where the call is
    refused. `source_type` is the channel's `spec.source.type` when the context knows it.
    """
    if destination is not None:
        raise ReplyError(
            f"ctx.reply is allowed in pipeline steps only, not in destination {safe_token(destination)}'s steps: "
            "each destination works on its own copy, so several could answer one request"
        )
    if not isinstance(body, bytes | bytearray | memoryview):
        raise ReplyError(f"ctx.reply body must be bytes, not {type(body).__name__}")
    raw = bytes(body)
    if len(raw) > MAX_REPLY_BODY_BYTES:
        raise ReplyError(
            f"ctx.reply body is {len(raw)} bytes, at most {MAX_REPLY_BODY_BYTES} allowed (no claim-check on reply)"
        )
    if not isinstance(content_type, str) or not content_type:
        raise ReplyError("ctx.reply content_type must be a non-empty string")
    media = h.media_type(content_type)
    if media not in h.CONTENT_TYPES or not h.is_well_formed(h.TB_CONTENT_TYPE, content_type):
        # The caller's value is not echoed: it is step-supplied text and could carry anything.
        raise ReplyError("ctx.reply content_type is not a registered envelope content type (envelope 3.1)")
    if source_type == "mllp" and media != MLLP_REPLY_CONTENT_TYPE:
        raise ReplyError(f"an mllp source answers with {MLLP_REPLY_CONTENT_TYPE} only")
    return ReplyBody(body=raw, content_type=content_type)


def check_once(current: ReplyBody | None) -> None:
    """Raise `ReplyError` when a body is already set for this message."""
    if current is not None:
        raise ReplyError("ctx.reply was already called for this message; it may be called at most once")
