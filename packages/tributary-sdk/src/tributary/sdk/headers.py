"""The `tb-*` header registry and total parsers for it.

"Total" is the point (gate requirement 13, SR-10): every function here either returns a value or raises
a typed `HeaderError`. None of them lets a `ValueError`, `TypeError`, `UnicodeDecodeError` or
`OverflowError` escape, because a malformed header arrives from the network and a crash in the consumer
loop is an availability bug where a reject is a DLQ record.

`int()` is deliberately not used on header text: it accepts `"1_0"` (10), `"١٢"` (12),
leading `+` and surrounding whitespace, none of which the envelope contract permits ("decimal ASCII, no
sign, no padding" — section 2). The regex is the parser; `int()` only runs on text it has cleared.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

from .errors import HeaderError, HeaderValueError, MissingHeaderError

TB_MSG_ID = "tb-msg-id"
TB_CHANNEL = "tb-channel"
TB_CHANNEL_VERSION = "tb-channel-version"
TB_SOURCE = "tb-source"
TB_RECEIVED_AT = "tb-received-at"
TB_CONTENT_TYPE = "tb-content-type"
TB_ATTEMPT = "tb-attempt"
TB_CORRELATION_ID = "tb-correlation-id"
TB_PARENT_ID = "tb-parent-id"
TB_CLAIM_CHECK = "tb-claim-check"
TB_DESTINATION = "tb-destination"
TB_STAGE = "tb-stage"
TB_ERROR_CODE = "tb-error-code"
TB_RETRY_NOT_BEFORE = "tb-retry-not-before"
TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"

USR_PREFIX = "tb-usr-"
EXPERIMENTAL_PREFIX = "tb-x-"

#: Every name the envelope contract registers. Producers stay inside this set plus the two prefixes;
#: consumers must not reject a record for an unregistered header (envelope contract 1.2).
REGISTERED: frozenset[str] = frozenset(
    {
        TB_MSG_ID,
        TB_CHANNEL,
        TB_CHANNEL_VERSION,
        TB_SOURCE,
        TB_RECEIVED_AT,
        TB_CONTENT_TYPE,
        TB_ATTEMPT,
        TB_CORRELATION_ID,
        TB_PARENT_ID,
        TB_CLAIM_CHECK,
        TB_DESTINATION,
        TB_STAGE,
        TB_ERROR_CODE,
        TB_RETRY_NOT_BEFORE,
        TRACEPARENT,
        TRACESTATE,
    }
)

#: Headers whose value is an integer. Gate requirement 13 asks for total parsing across all of them.
INTEGER_HEADERS: Mapping[str, int] = {TB_ATTEMPT: 9999, TB_CHANNEL_VERSION: 9_999_999_999}

#: Headers whose value is an RFC 3339 UTC timestamp with 3 to 9 fractional digits.
TIMESTAMP_HEADERS: frozenset[str] = frozenset({TB_RECEIVED_AT, TB_RETRY_NOT_BEFORE})

MAX_HEADERS = 32
MAX_NAME_BYTES = 64
MAX_REGISTERED_VALUE_BYTES = 1024
MAX_PREFIXED_VALUE_BYTES = 256
MAX_ALL_HEADERS_BYTES = 16 * 1024

_INTEGER = re.compile(r"^(?:0|[1-9][0-9]*)$")
_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3,9})(Z|\+00:00)$")
_UUID_ANY = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$")
_UUID7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_TRACEPARENT = re.compile(r"^00-(?![0]{32})[0-9a-f]{32}-(?![0]{16})[0-9a-f]{16}-[0-9a-f]{2}$")
_CLAIM_CHECK = re.compile(r"^(s3|gs|az|https)://[A-Za-z0-9._-]{1,253}(:[0-9]{2,5})?(/[A-Za-z0-9._-]{1,128}){1,16}$")
_CHANNEL_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_DESTINATION_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_SOURCE = re.compile(r"^[A-Za-z0-9._:@/=\[\]-]{1,253}$")
_CONTENT_TYPE = re.compile(r"^[a-z]+/[a-zA-Z0-9.+_-]+( *; *[a-zA-Z0-9._-]+=[^;]{1,64})*$")
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")

#: The `tb-stage` enum from the envelope contract's section 3 stage definition.
STAGES: frozenset[str] = frozenset(
    {
        "received",
        "queued",
        "filtered",
        "transformed",
        "routed",
        "pending",
        "sent",
        "retrying",
        "replied",
        "error",
        "dlq",
        "quarantined",
    }
)


def decode_headers(raw: Iterable[tuple[str, bytes | str | None]]) -> dict[str, str]:
    """Decode Kafka headers to text, last value wins.

    Raises `HeaderValueError` for a value that is not UTF-8: that is a reject, not a crash, and not a
    silent `errors="replace"` either, because a replacement character would then flow through the
    envelope as if it had been sent.
    """
    decoded: dict[str, str] = {}
    for name, value in raw:
        if not isinstance(name, str):
            raise HeaderValueError("<non-text-name>", "header name must be text")
        if value is None:
            raise HeaderValueError(name, "null header value; omit the header instead")
        if isinstance(value, str):
            decoded[name] = value
            continue
        try:
            decoded[name] = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            raise HeaderValueError(name, "value is not valid UTF-8", value_length=len(value)) from None
    return decoded


def require(headers: Mapping[str, str], name: str) -> str:
    """The value of a required header, or `MissingHeaderError`."""
    try:
        value = headers[name]
    except (KeyError, TypeError):
        raise MissingHeaderError(name) from None
    if not isinstance(value, str):
        raise HeaderValueError(name, "value must be text")
    return value


def parse_int(name: str, value: str | None, *, max_value: int | None = None) -> int:
    """Parse an integer header. Total: returns an `int` or raises `HeaderValueError`."""
    if value is None:
        raise MissingHeaderError(name)
    if not isinstance(value, str):
        raise HeaderValueError(name, "value must be text")
    limit = max_value if max_value is not None else INTEGER_HEADERS.get(name)
    if not _INTEGER.match(value):
        raise HeaderValueError(
            name,
            "value must be decimal ASCII digits with no sign, padding, separators or whitespace",
            value_length=len(value),
        )
    if len(value) > 19:
        # Bounded before int() so a megabyte of digits cannot become a multi-second conversion.
        raise HeaderValueError(name, "value has too many digits", value_length=len(value))
    parsed = int(value)
    if limit is not None and parsed > limit:
        raise HeaderValueError(name, f"value exceeds the contract maximum of {limit}", value_length=len(value))
    return parsed


def parse_uuid(name: str, value: str | None, *, version7: bool = False) -> str:
    """Validate a UUID header and return it unchanged (canonical lowercase hyphenated form)."""
    if value is None:
        raise MissingHeaderError(name)
    if not isinstance(value, str):
        raise HeaderValueError(name, "value must be text")
    pattern = _UUID7 if version7 else _UUID_ANY
    if not pattern.match(value):
        expected = "a canonical lowercase UUIDv7" if version7 else "a canonical lowercase UUID"
        raise HeaderValueError(name, f"value must be {expected}", value_length=len(value))
    return value


def parse_timestamp(name: str, value: str | None) -> datetime:
    """Parse an RFC 3339 UTC timestamp header with 3 to 9 fractional digits.

    `datetime.fromisoformat` accepts at most 6 fractional digits, so a contract-legal 9-digit value
    would raise `ValueError` if handed over directly. The extra digits are truncated for the returned
    `datetime` (microsecond resolution) while the header text keeps whatever precision it arrived with.
    """
    if value is None:
        raise MissingHeaderError(name)
    if not isinstance(value, str):
        raise HeaderValueError(name, "value must be text")
    match = _TIMESTAMP.match(value)
    if match is None:
        raise HeaderValueError(
            name,
            "value must be RFC 3339 UTC with 3 to 9 fractional digits and a Z or +00:00 offset",
            value_length=len(value),
        )
    base, fraction, _offset = match.groups()
    micros = fraction[:6].ljust(6, "0")  # ljust, not an f-string width: str formatting pads with spaces
    try:
        parsed = datetime.fromisoformat(f"{base}.{micros}+00:00")
    except ValueError:
        raise HeaderValueError(name, "value is not a real date and time", value_length=len(value)) from None
    return parsed


def parse_claim_check(name: str, value: str | None) -> str:
    """Validate a `tb-claim-check` locator and return it unchanged.

    The pattern is the contract's: scheme, host or bucket, optional port, path segments — and **no query
    string, no fragment, no userinfo**. A presigned URL carries its signature in the query string and an
    embedded `user:password@host` carries credentials in userinfo, and this value is persisted to Kafka,
    copied into `_tb.events`, written to ClickHouse and rendered in the browser, so either one would
    become a stored, replayable credential that outlives its own expiry in backups (envelope 5.1).
    """
    if value is None:
        raise MissingHeaderError(name)
    if not isinstance(value, str) or not _CLAIM_CHECK.match(value):
        length = len(value) if isinstance(value, str) else None
        raise HeaderValueError(
            name,
            "value must be an opaque object locator with no query string, fragment or userinfo",
            value_length=length,
        )
    return value


def parse_traceparent(name: str, value: str | None) -> str:
    """Validate a W3C `traceparent` (version 00, non-zero trace and span ids) and return it unchanged."""
    if value is None:
        raise MissingHeaderError(name)
    if not isinstance(value, str) or not _TRACEPARENT.match(value):
        length = len(value) if isinstance(value, str) else None
        raise HeaderValueError(
            name, "value must be a W3C traceparent with non-zero trace and span ids", value_length=length
        )
    return value


def format_timestamp(moment: datetime) -> str:
    """Format a `datetime` as the contract's RFC 3339 UTC value with millisecond precision.

    A naive `datetime` is rejected: guessing that it meant UTC is how a local-time timestamp ends up on
    a topic, where it silently shifts every end-to-end latency measurement by the host's offset.
    """
    if moment.tzinfo is None:
        raise HeaderValueError("<timestamp>", "datetime must be timezone-aware")
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def is_well_formed(name: str, value: str) -> bool:
    """True when a registered header's value satisfies the contract's pattern for it.

    Total and never raising: a caller deciding whether to reject a record needs a verdict per header,
    not an exception per header. Unregistered names return `True` — a consumer must not reject a record
    for a header it does not recognise (envelope contract 1.2).
    """
    try:
        if name in INTEGER_HEADERS:
            parse_int(name, value)
        elif name in TIMESTAMP_HEADERS:
            parse_timestamp(name, value)
        elif name == TB_MSG_ID:
            parse_uuid(name, value, version7=True)
        elif name in (TB_CORRELATION_ID, TB_PARENT_ID):
            parse_uuid(name, value)
        elif name == TRACEPARENT:
            parse_traceparent(name, value)
        elif name == TB_CLAIM_CHECK:
            parse_claim_check(name, value)
        elif name == TB_CHANNEL:
            return bool(_CHANNEL_NAME.match(value))
        elif name == TB_DESTINATION:
            return bool(_DESTINATION_NAME.match(value))
        elif name == TB_SOURCE:
            return bool(_SOURCE.match(value))
        elif name == TB_CONTENT_TYPE:
            return len(value) <= 255 and bool(_CONTENT_TYPE.match(value))
        elif name == TB_ERROR_CODE:
            return bool(_ERROR_CODE.match(value))
        elif name == TB_STAGE:
            return value in STAGES
    except HeaderError:
        return False
    return True


def usr_name(name: str) -> str:
    """The full header name for a channel-defined `tb-usr-*` header."""
    return f"{USR_PREFIX}{name}"


def is_experimental(name: str) -> bool:
    """True for a `tb-x-*` header. Never load-bearing, and must not be produced by a released package."""
    return name.startswith(EXPERIMENTAL_PREFIX)
