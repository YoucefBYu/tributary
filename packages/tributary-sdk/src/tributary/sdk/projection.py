"""The envelope record projection, and the producer-side checks JSON Schema cannot make.

`contracts/envelope/v0` validates a **projection** of a Kafka record rather than the record itself, and
that projection carries no key value and no payload bytes by construction: a validator embeds the
failing instance in its error message, and the record key is usually a patient identifier (contract
section 1.1). `record_projection` is the one implementation of that mapping, so the runtime, the
connectors and the conformance tests all project the same way.

`validate_for_produce` covers what the schema cannot see:

* the byte budgets (16 KiB of headers in total, per-value maxima) — JSON Schema cannot sum lengths;
* the per-class key identity rules (`dlq` keyed by `tb-msg-id`, `reply` by `tb-correlation-id`), which
  the contract says are "checked in code, not by this schema" for the same PHI reason;
* the ban on producing a `tb-x-*` header from a released package (envelope 2.1, and gate section 6:
  the spikes' `tb-x-stage` is exactly what Phase 1 must not copy).

Value *formats* are not re-validated here. Re-implementing the schema in Python would give two
descriptions of one contract that can drift; instead the schema itself runs over projections from these
emitters in the package's conformance tests (gate requirement 10).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

from . import headers as h
from .errors import EnvelopeError, UsrHeaderError, safe_token
from .guards import validate_usr
from .message import Message

TopicClass = Literal["raw", "out", "retry", "dlq", "reply"]
Ordering = Literal["per-key", "none"]

TOPIC_CLASSES: frozenset[str] = frozenset({"raw", "out", "retry", "dlq", "reply"})

_CORE = (
    h.TB_MSG_ID,
    h.TB_CHANNEL,
    h.TB_CHANNEL_VERSION,
    h.TB_SOURCE,
    h.TB_RECEIVED_AT,
    h.TB_CONTENT_TYPE,
    h.TB_ATTEMPT,
    h.TRACEPARENT,
)

#: Required and forbidden header names per topic class, mirroring the conditional rules in
#: `envelope.schema.json`. Kept as data so the two are comparable by eye in review.
REQUIRED: dict[str, tuple[str, ...]] = {
    "raw": _CORE,
    "out": (*_CORE, h.TB_DESTINATION),
    "retry": (*_CORE, h.TB_DESTINATION, h.TB_STAGE, h.TB_ERROR_CODE, h.TB_RETRY_NOT_BEFORE),
    "dlq": (*_CORE, h.TB_STAGE, h.TB_ERROR_CODE),
    "reply": (
        h.TB_MSG_ID,
        h.TB_CHANNEL,
        h.TB_CHANNEL_VERSION,
        h.TB_RECEIVED_AT,
        h.TB_CONTENT_TYPE,
        h.TB_CORRELATION_ID,
        h.TRACEPARENT,
    ),
}

_REPLY_HEADERS = (h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE, h.TB_REPLY_STATUS)

FORBIDDEN: dict[str, tuple[str, ...]] = {
    "raw": (h.TB_DESTINATION, h.TB_STAGE, h.TB_ERROR_CODE, h.TB_RETRY_NOT_BEFORE, h.TB_REPLY_STATUS),
    "out": (h.TB_ERROR_CODE, h.TB_RETRY_NOT_BEFORE, *_REPLY_HEADERS),
    "retry": _REPLY_HEADERS,
    "dlq": (h.TB_RETRY_NOT_BEFORE, *_REPLY_HEADERS),
    # tb-error-code on reply is conditional since set 1.2: see `_validate_conditionals`.
    "reply": (h.TB_RETRY_NOT_BEFORE, h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE),
}

#: `reply` statuses that require `tb-error-code`; every other reply (including one with no status, the
#: set 1.0 shape) must not carry one (envelope 3, schema `if`/`then`/`else`).
_FAILED_REPLY_STATUSES = frozenset({"rejected", "failed"})

MAX_INLINE_PAYLOAD_BYTES = 1_000_000
MAX_KEY_BYTES = 256


def _check_class(topic_class: str) -> None:
    if topic_class not in TOPIC_CLASSES:
        raise EnvelopeError(f"unknown topic class {safe_token(topic_class)}")


def record_projection(message: Message, *, topic_class: TopicClass, topic: str | None = None) -> dict[str, object]:
    """Project a `Message` into the JSON object `contracts/envelope/v0/envelope.schema.json` validates.

    The projection deliberately contains **no key value and no payload bytes**, only their presence and
    length: everything the schema ever checked about them, and nothing a validation error can leak.
    """
    _check_class(topic_class)
    projection: dict[str, object] = {
        "class": topic_class,
        "key_present": message.key is not None,
        "key_size": message.key_size,
        "payload_present": message.payload is not None,
        "payload_size": message.payload_size,
        "headers": dict(message.headers),
    }
    if topic is not None:
        projection["topic"] = topic
    return projection


def validate_for_produce(
    message: Message,
    *,
    topic_class: TopicClass,
    ordering: Ordering = "per-key",
) -> None:
    """Refuse to produce a record that breaks the envelope contract. Raises `EnvelopeError`.

    PHI-safe by construction: every message names a header, a class or a size, never a value.
    """
    _check_class(topic_class)
    headers = message.headers

    for name in REQUIRED[topic_class]:
        value = headers.get(name)
        if not value:
            raise EnvelopeError(f"class {topic_class}: header {safe_token(name)} is required")
    for name in FORBIDDEN[topic_class]:
        if name in headers:
            raise EnvelopeError(f"class {topic_class}: header {safe_token(name)} must not be set")

    received_at = headers.get(h.TB_RECEIVED_AT)
    if received_at is not None and not h.is_well_formed(h.TB_RECEIVED_AT, received_at):
        # The same pattern `decode_record` enforces on consume (envelope/v0 3): a record this layer let
        # through would only be rejected one hop later, by every reader. The message never quotes the value.
        raise EnvelopeError(
            f"header {h.TB_RECEIVED_AT} must be RFC 3339 UTC with 3 to 9 fractional digits and a Z or +00:00 offset"
        )

    _validate_conditionals(headers, topic_class=topic_class)

    if len(headers) > h.MAX_HEADERS:
        raise EnvelopeError(f"{len(headers)} headers, at most {h.MAX_HEADERS} allowed")

    total = 0
    for name, value in headers.items():
        name_bytes = len(name.encode("utf-8"))
        value_bytes = len(value.encode("utf-8"))
        total += name_bytes + value_bytes
        if name_bytes > h.MAX_NAME_BYTES:
            raise EnvelopeError(f"header name is {name_bytes} bytes, at most {h.MAX_NAME_BYTES} allowed")
        if h.is_experimental(name):
            raise EnvelopeError(
                f"header {safe_token(name)}: tb-x-* is spike-local and must not be produced by a released "
                "package (envelope contract 2.1)"
            )
        if name.startswith(h.USR_PREFIX):
            # The full tb-usr-* rule, not just the byte budget. This is the layer every produce path
            # goes through, so a header set with `with_header` directly — bypassing `with_usr` and
            # `ctx.set_usr` — is caught here rather than reaching a topic (code-reviewer High, wave 1).
            try:
                validate_usr(name[len(h.USR_PREFIX) :], value)
            except UsrHeaderError as exc:
                raise EnvelopeError(str(exc)) from exc
        elif name.startswith("tb-") and name not in h.REGISTERED:
            raise EnvelopeError(f"header {safe_token(name)} is not registered; tb- is reserved (envelope contract 3)")
        elif value_bytes > h.MAX_REGISTERED_VALUE_BYTES:
            raise EnvelopeError(
                f"header {safe_token(name)} is {value_bytes} bytes, at most {h.MAX_REGISTERED_VALUE_BYTES} allowed"
            )
    if total > h.MAX_ALL_HEADERS_BYTES:
        raise EnvelopeError(f"headers total {total} bytes, at most {h.MAX_ALL_HEADERS_BYTES} allowed")

    if message.claim_check is not None:
        if message.payload is not None:
            raise EnvelopeError("tb-claim-check is set, so the record value must be null, not empty bytes")
    elif message.payload_size > MAX_INLINE_PAYLOAD_BYTES:
        raise EnvelopeError(
            f"payload is {message.payload_size} bytes, over the {MAX_INLINE_PAYLOAD_BYTES}-byte inline limit: "
            "use a claim-check"
        )

    _validate_key(message, topic_class=topic_class, ordering=ordering)


def _validate_conditionals(headers: Mapping[str, str], *, topic_class: str) -> None:
    """The set 1.2 presence rules the schema holds with `dependentRequired` and `if`/`then`/`else`."""
    if topic_class == "raw":
        # dependentRequired: a sync request names its partition, its deadline and its correlation id
        # together, and a deadline never travels alone.
        if h.TB_REPLY_PARTITION in headers:
            for name in (h.TB_CORRELATION_ID, h.TB_REPLY_DEADLINE):
                if not headers.get(name):
                    raise EnvelopeError(f"class raw: {h.TB_REPLY_PARTITION} requires header {safe_token(name)}")
        if h.TB_REPLY_DEADLINE in headers and h.TB_REPLY_PARTITION not in headers:
            raise EnvelopeError(f"class raw: {h.TB_REPLY_DEADLINE} requires header {h.TB_REPLY_PARTITION}")
    elif topic_class == "reply":
        failed = headers.get(h.TB_REPLY_STATUS) in _FAILED_REPLY_STATUSES
        if failed and not headers.get(h.TB_ERROR_CODE):
            raise EnvelopeError(f"class reply: a rejected or failed reply requires {h.TB_ERROR_CODE}")
        if not failed and h.TB_ERROR_CODE in headers:
            raise EnvelopeError(f"class reply: {h.TB_ERROR_CODE} is allowed only on a rejected or failed reply")


def _validate_key(message: Message, *, topic_class: str, ordering: Ordering) -> None:
    """The per-class key identity rules the schema cannot express (envelope contract 1.1 and 4)."""
    if message.key_size > MAX_KEY_BYTES:
        raise EnvelopeError(f"record key is {message.key_size} bytes, at most {MAX_KEY_BYTES} allowed")

    if topic_class == "dlq":
        expected = message.require(h.TB_MSG_ID).encode("utf-8")
        if message.key != expected:
            raise EnvelopeError("class dlq: the record key must be tb-msg-id")
        return
    if topic_class == "reply":
        correlation = message.get(h.TB_CORRELATION_ID)
        if correlation is None or message.key != correlation.encode("utf-8"):
            raise EnvelopeError("class reply: the record key must be tb-correlation-id")
        return
    if message.key is None and ordering == "per-key":
        raise EnvelopeError(
            f"class {topic_class}: a null key is allowed only when the channel declares ordering: none "
            "(it round-robins and destroys per-key order, invariant 5)"
        )
