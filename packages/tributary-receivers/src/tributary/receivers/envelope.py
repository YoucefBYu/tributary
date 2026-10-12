"""Building a `class: raw` envelope for any source type.

`tributary.receivers.mllp.kafka_publisher.build_raw_envelope` does the same thing for the MLLP
receiver but hardcodes `application/hl7-v2+er7`, the only content type an MLLP frame can ever carry.
The `http`, `sftp` and `kafka` sources accept a `contentType`/`accept` the channel spec chooses, so
this module is the same construction with `content_type` as a parameter, shared by all three so
there is exactly one place a `raw` envelope's required headers are assembled outside the MLLP
receiver (gate requirement 10: validated against `contracts/envelope/v0`, not "matches by
inspection" -- `tributary.sdk.projection.validate_for_produce`, called by
`mllp.kafka_publisher.RawPublisher.publish`, is what actually enforces that at produce time; this
function only has to agree with the schema on shape).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def build_raw_envelope(
    *,
    channel: str,
    channel_version: int,
    source: str,
    payload: bytes,
    key: bytes | None,
    content_type: str,
    reply_headers: Mapping[str, str] | None = None,
) -> Message:
    """A fresh `tb-msg-id`, `tb-attempt: 0` (this is the first hop), a fresh `traceparent`.

    `reply_headers` is synchronous reply mode's `tb-correlation-id`, `tb-reply-partition` and `tb-reply-deadline`
    (`reply/pending.py`): engine-minted ids, an integer and a timestamp. Nothing the sender supplied is a header."""
    return Message(
        headers={
            **(reply_headers or {}),
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: channel,
            h.TB_CHANNEL_VERSION: str(channel_version),
            h.TB_SOURCE: source,
            h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
            h.TB_CONTENT_TYPE: content_type,
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
        },
        payload=payload,
        key=key,
    )
