"""`Message`: one Tributary message as a step sees it.

Headers carry every routing, retry and ordering decision; the payload is opaque bytes (invariant 8), so
a format the engine has never seen still flows end to end. A `Message` is immutable: every mutator
returns a new one, which is what makes a step's effect on a message something a test can compare.

The record key is deliberately awkward. It is usually a direct patient identifier — ADR-001's worked
example is `hl7:PID.3.1` — so `__repr__` and `__str__` publish sizes and ids only. A `Message` that
printed itself would put PHI into pytest output, log lines and exception context, which is exactly what
invariant 6 forbids and what `contracts/envelope/v0/README.md` section 4 calls a Critical finding.

Kafka coordinates (topic, partition, offset) are not here on purpose: they belong to the record the
runtime consumed, not to the message, and keeping them out is half of what makes every step unit
testable with no Kafka (invariant 7).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from types import MappingProxyType

from . import headers as h
from .errors import HeaderValueError, PayloadError, safe_token

_Bytes = bytes | bytearray | memoryview


def _as_bytes(name: str, value: _Bytes | None) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise TypeError(f"{name} must be bytes or None, not {type(value).__name__}")


class Message:
    """An immutable Tributary message: `tb-*` headers, an opaque payload, and the ordering key."""

    __slots__ = ("_headers", "_key", "_payload")

    def __init__(
        self,
        *,
        headers: Mapping[str, str] | Iterable[tuple[str, str]] | None = None,
        payload: _Bytes | None = None,
        key: _Bytes | None = None,
    ) -> None:
        collected: dict[str, str] = {}
        for name, value in dict(headers or {}).items():
            if not isinstance(name, str):
                raise HeaderValueError("<non-text-name>", "header name must be text")
            if not isinstance(value, str):
                raise HeaderValueError(name, "header value must be text, not bytes or a number")
            collected[name] = value
        self._headers: Mapping[str, str] = MappingProxyType(collected)
        self._payload = _as_bytes("payload", payload)
        self._key = _as_bytes("key", key)

    # -- record parts ----------------------------------------------------------------------------

    @property
    def headers(self) -> Mapping[str, str]:
        """All headers, decoded to text. Read-only; use `with_header` to change one."""
        return self._headers

    @property
    def payload(self) -> bytes | None:
        """The payload bytes, or `None` when the record is claim-checked (envelope contract 5)."""
        return self._payload

    @property
    def key(self) -> bytes | None:
        """The Kafka record key: the channel's `partitionKey` result, or `None` for `ordering: none`.

        **This is usually PHI.** Never log it, never put it in a metric label, a trace attribute, a URL,
        an exception message or a sender-facing error (envelope contract section 4).
        """
        return self._key

    @property
    def payload_size(self) -> int:
        return 0 if self._payload is None else len(self._payload)

    @property
    def key_size(self) -> int:
        return 0 if self._key is None else len(self._key)

    def text(self, *, encoding: str | None = None) -> str:
        """The payload decoded to text.

        Default encoding is UTF-8, except `application/hl7-v2+er7`, where the envelope contract leaves
        the charset to the channel spec because HL7 v2 in the field is frequently ISO-8859-1 — so a
        step must pass `encoding=` explicitly rather than have the SDK guess and corrupt a name.
        """
        if self._payload is None:
            raise PayloadError("payload is None (claim-checked record): fetch the content through ctx")
        if encoding is None:
            content_type = self.get(h.TB_CONTENT_TYPE) or ""
            if content_type.split(";")[0].strip() == "application/hl7-v2+er7":
                raise PayloadError("hl7-v2+er7 payloads have no default charset: pass encoding= from the channel spec")
            encoding = "utf-8"
        try:
            return self._payload.decode(encoding)
        except (UnicodeDecodeError, LookupError) as exc:
            # Neither the bytes nor the offending character reach the message: they may be PHI.
            raise PayloadError(f"payload is not decodable as {safe_token(encoding)}") from exc

    # -- header access ---------------------------------------------------------------------------

    def get(self, name: str) -> str | None:
        """One header value, or `None`. Unknown headers are returned, never rejected (envelope 1.2)."""
        return self._headers.get(name)

    def has(self, name: str) -> bool:
        return name in self._headers

    def require(self, name: str) -> str:
        """One header value, or `MissingHeaderError`."""
        return h.require(self._headers, name)

    def int_header(self, name: str, *, default: int | None = None) -> int:
        """An integer header, parsed totally: an int, or a typed reject. Never a crash."""
        value = self._headers.get(name)
        if value is None and default is not None:
            return default
        return h.parse_int(name, value)

    @property
    def msg_id(self) -> str:
        """`tb-msg-id`: UUIDv7, minted once at receive, stable across every hop. The idempotency key."""
        return h.parse_uuid(h.TB_MSG_ID, self.require(h.TB_MSG_ID), version7=True)

    @property
    def channel(self) -> str:
        return self.require(h.TB_CHANNEL)

    @property
    def channel_version(self) -> int:
        return h.parse_int(h.TB_CHANNEL_VERSION, self.require(h.TB_CHANNEL_VERSION))

    @property
    def source(self) -> str | None:
        return self.get(h.TB_SOURCE)

    @property
    def received_at(self) -> str:
        """`tb-received-at` as contract text. End-to-end latency is `now - tb-received-at`."""
        return self.require(h.TB_RECEIVED_AT)

    @property
    def received_at_time(self) -> datetime:
        return h.parse_timestamp(h.TB_RECEIVED_AT, self.require(h.TB_RECEIVED_AT))

    @property
    def content_type(self) -> str:
        return self.require(h.TB_CONTENT_TYPE)

    @property
    def attempt(self) -> int:
        """`tb-attempt`: delivery attempt within the current stage, `0` at receive."""
        return h.parse_int(h.TB_ATTEMPT, self.require(h.TB_ATTEMPT))

    @property
    def correlation_id(self) -> str | None:
        value = self.get(h.TB_CORRELATION_ID)
        return None if value is None else h.parse_uuid(h.TB_CORRELATION_ID, value)

    @property
    def parent_id(self) -> str | None:
        value = self.get(h.TB_PARENT_ID)
        return None if value is None else h.parse_uuid(h.TB_PARENT_ID, value)

    @property
    def claim_check(self) -> str | None:
        return self.get(h.TB_CLAIM_CHECK)

    @property
    def destination(self) -> str | None:
        return self.get(h.TB_DESTINATION)

    @property
    def stage(self) -> str | None:
        return self.get(h.TB_STAGE)

    @property
    def error_code(self) -> str | None:
        return self.get(h.TB_ERROR_CODE)

    @property
    def retry_not_before(self) -> datetime | None:
        value = self.get(h.TB_RETRY_NOT_BEFORE)
        return None if value is None else h.parse_timestamp(h.TB_RETRY_NOT_BEFORE, value)

    @property
    def traceparent(self) -> str | None:
        return self.get(h.TRACEPARENT)

    def usr_headers(self) -> Mapping[str, str]:
        """The channel-defined `tb-usr-*` headers, without the prefix. Propagated, never interpreted."""
        return MappingProxyType(
            {name[len(h.USR_PREFIX) :]: value for name, value in self._headers.items() if name.startswith(h.USR_PREFIX)}
        )

    # -- copy-on-write ---------------------------------------------------------------------------

    def with_payload(self, payload: _Bytes | None, *, content_type: str | None = None) -> Message:
        """A copy with a new payload, and `tb-content-type` rewritten when the format changed.

        Envelope contract 6 rule 3 puts that rewrite on the step that changes the format. Passing the
        payload without the content type is legal (a payload edit that keeps the format) and is the one
        place a stale content type can come from, so a step that transforms must pass both.
        """
        message = Message(headers=self._headers, payload=payload, key=self._key)
        if content_type is not None:
            return message.with_header(h.TB_CONTENT_TYPE, content_type)
        return message

    def with_header(self, name: str, value: str) -> Message:
        """A copy with one header set. Rejects an empty value: omit the header to mean absent.

        A `tb-usr-*` name is routed through the same guard `with_usr` uses. Without that, the deny-list
        is only enforced on the convenience method and `with_header("tb-usr-mrn", ...)` walks straight
        past it — a guard with a documented bypass is not a guard.
        """
        if not isinstance(name, str) or not name:
            raise HeaderValueError("<non-text-name>", "header name must be non-empty text")
        if not isinstance(value, str):
            raise HeaderValueError(name, "header value must be text")
        if not value:
            raise HeaderValueError(name, "empty value; omit the header to mean absent (envelope contract 2)")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
            raise HeaderValueError(name, "control characters are not allowed in a header value")
        if name.startswith(h.USR_PREFIX):
            from .guards import validate_usr  # local import: guards imports nothing from message

            validate_usr(name[len(h.USR_PREFIX) :], value)
        return Message(headers={**self._headers, name: value}, payload=self._payload, key=self._key)

    def with_headers(self, values: Mapping[str, str]) -> Message:
        message = self
        for name, value in values.items():
            message = message.with_header(name, value)
        return message

    def without_header(self, name: str) -> Message:
        """A copy with one header removed.

        A step must not remove a registered header it does not understand (envelope contract 6 rule 6);
        that rule is on the step author, because dropping `tb-destination` is legal for the runtime when
        a record moves between classes.
        """
        remaining = {key: value for key, value in self._headers.items() if key != name}
        return Message(headers=remaining, payload=self._payload, key=self._key)

    def with_key(self, key: _Bytes | None) -> Message:
        """A copy with a different ordering key. Changing it changes the partition, so it changes order."""
        return Message(headers=self._headers, payload=self._payload, key=key)

    def with_attempt(self, attempt: int) -> Message:
        """A copy with `tb-attempt` set, formatted as the contract requires."""
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
            raise HeaderValueError(h.TB_ATTEMPT, "attempt must be a non-negative integer")
        return self.with_header(h.TB_ATTEMPT, str(attempt))

    def with_usr(self, name: str, value: str) -> Message:
        """A copy with a channel-defined `tb-usr-*` header set, validated against the envelope rules."""
        return self.with_header(h.usr_name(name), value)

    # -- dunder ----------------------------------------------------------------------------------

    def __repr__(self) -> str:
        """Ids and sizes only. The key and the payload never appear (invariant 6)."""
        msg_id = self._headers.get(h.TB_MSG_ID, "-")
        channel = self._headers.get(h.TB_CHANNEL, "-")
        return (
            f"Message(msg_id={msg_id}, channel={channel}, headers={len(self._headers)}, "
            f"payload={self.payload_size}B, key={self.key_size}B)"
        )

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return NotImplemented
        return self._headers == other._headers and self._payload == other._payload and self._key == other._key

    __hash__ = None  # type: ignore[assignment]  # mutable mapping inside; equality is for tests
