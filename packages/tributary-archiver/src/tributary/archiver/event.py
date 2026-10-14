"""One `_tb.<env>.events` record to one `message_events` row.

Validation is a hand-written mirror of the parts of `contracts/events/v0/event.schema.json` the archive
depends on (the data plane has no `jsonschema`). Consumers ignore unknown fields and higher `v` values
(contract README 2.2), so an unknown field is dropped here and a higher `v` is rejected, not failed.

PHI (invariant 6): nothing here reads a payload (events carry none). `attrs` is stored only as the
contract's low-cardinality short codes. This module never creates an attribute and never derives one from
an identifier. An `*_hmac` entry, or a name on the schema's identifier deny-list, is DROPPED rather than
stored: how identifier-derived attributes are built is human-reserved (events README 3.1, ADR-004), so
the archive does not become the place where one is persisted before that decision exists.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import orjson

from .errors import EventRejected
from .tswindow import in_window

SUPPORTED_VERSION: Final = 0
MAX_ATTRS: Final = 16
_SHORT_CODE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}")
_ATTR_NAME: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
_TS: Final = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(?:Z|\+00:00)")
_NAME: Final = re.compile(r"[a-z][a-z0-9-]{0,63}")
_ERROR_CODE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
_TRACEPARENT: Final = re.compile(r"00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}")
_CLAIM_CHECK: Final = re.compile(
    r"(s3|gs|az|https)://[A-Za-z0-9._-]{1,253}(:[0-9]{2,5})?(/[A-Za-z0-9._-]{1,128}){1,16}"
)
STAGES: Final = frozenset(
    {"received", "queued", "filtered", "transformed", "routed", "pending", "sent", "retrying", "replied",
     "error", "dlq", "partition_paused", "partition_resumed", "partition_halted", "quarantined"}
)  # fmt: skip
STATUSES: Final = frozenset({"ok", "error", "timeout", "dropped"})
_NIL_UUID: Final = uuid.UUID(int=0)
KINDS: Final = frozenset({"message", "partition", "record"})
#: The schema's identifier deny-list (names that denote a direct identifier). Kept in step with
#: `event.schema.json` `attrs.propertyNames`; a test compares the two.
IDENTIFIER_NAMES: Final = frozenset(
    {
        "mrn", "pid", "patient", "patient_id", "patientid", "patient_name", "name", "family_name",
        "given_name", "surname", "first_name", "last_name", "dob", "birth_date", "birthdate", "ssn",
        "nhs_number", "insurance_number", "account", "account_number", "visit_number", "encounter_id",
        "accession", "accession_number", "address", "street", "postcode", "zip", "phone", "telephone",
        "email", "identifier", "key", "message_key", "partition_key",
    }
)  # fmt: skip

COLUMNS: Final[tuple[str, ...]] = (
    "ts",
    "kind",
    "channel",
    "channel_version",
    "stage",
    "status",
    "instance",
    "message_id",
    "parent_id",
    "correlation_id",
    "destination",
    "attempt",
    "content_type",
    "size_bytes",
    "claim_check",
    "topic",
    "partition",
    "offset",
    "error_code",
    "error_message",
    "duration_ms",
    "traceparent",
    "attrs",
    "scope",
)


@dataclass(frozen=True, slots=True)
class EventRow:
    """One row in `COLUMNS` order. `scope` is the dedup discriminator for events with no message identity."""

    values: tuple[Any, ...]
    ts: datetime
    attrs_dropped: int
    fields_blanked: int = 0
    #: Of `attrs_dropped`, the entries whose name ends `_hmac` (`tb_archiver_attrs_dropped_total{reason="hmac"}`).
    attrs_dropped_hmac: int = 0


def _uuid(value: Any) -> uuid.UUID:
    if not isinstance(value, str):
        raise EventRejected("EVENT_FIELD_TYPE")
    try:
        return uuid.UUID(value)
    except ValueError:
        raise EventRejected("EVENT_FIELD_TYPE") from None


def parse_ts(value: Any) -> datetime:
    """RFC 3339 UTC with 1-9 fractional digits; stored at millisecond precision (the column's)."""
    match = _TS.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise EventRejected("EVENT_TS_INVALID")
    whole, fraction = match.group(1), (match.group(2) or "0")
    micros = int(fraction.ljust(6, "0")[:6])
    try:
        return datetime.fromisoformat(whole).replace(tzinfo=UTC, microsecond=micros)
    except ValueError:
        raise EventRejected("EVENT_TS_INVALID") from None


def _str(raw: dict[str, Any], name: str, *, required: bool = False, limit: int = 1024) -> str:
    value = raw.get(name)
    if value is None:
        if required:
            raise EventRejected("EVENT_FIELD_MISSING")
        return ""
    if not isinstance(value, str) or len(value) > limit or (required and not value):
        raise EventRejected("EVENT_FIELD_TYPE")
    return value


def _matching(raw: dict[str, Any], name: str, pattern: re.Pattern[str], *, limit: int) -> tuple[str, bool]:
    """A schema-patterned optional field: (value, blanked). A failing value is blanked, never stored."""
    value = _str(raw, name, limit=limit)
    if value and pattern.fullmatch(value) is None:
        return "", True
    return value, False


def _int(raw: dict[str, Any], name: str, *, maximum: int, required: bool = False) -> int | None:
    value = raw.get(name)
    if value is None:
        if required:
            raise EventRejected("EVENT_FIELD_MISSING")
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise EventRejected("EVENT_FIELD_TYPE")
    return value


def clean_attrs(value: Any) -> tuple[dict[str, str], int]:
    """The storable entries of `attrs` and how many were dropped. Never raises on content."""
    if value is None:
        return {}, 0
    if not isinstance(value, dict):
        raise EventRejected("EVENT_FIELD_TYPE")
    kept: dict[str, str] = {}
    dropped = 0
    for name, entry in value.items():
        storable = (
            len(kept) < MAX_ATTRS
            and isinstance(name, str)
            and _ATTR_NAME.fullmatch(name) is not None
            and not name.endswith("_hmac")
            and name not in IDENTIFIER_NAMES
            and isinstance(entry, str)
            and _SHORT_CODE.fullmatch(entry) is not None
        )
        if storable:
            kept[name] = entry
        else:
            dropped += 1
    return kept, dropped


def parse_event(record_value: bytes, *, anchor: datetime | None = None, now: datetime | None = None) -> EventRow:
    """Decode one record value. Raises `EventRejected` (a code, never content) for anything unstorable."""
    try:
        raw = orjson.loads(record_value)
    except orjson.JSONDecodeError:
        raise EventRejected("EVENT_NOT_JSON") from None
    if not isinstance(raw, dict):
        raise EventRejected("EVENT_NOT_OBJECT")
    version = raw.get("v")
    if isinstance(version, bool) or not isinstance(version, int) or version != SUPPORTED_VERSION:
        raise EventRejected("EVENT_VERSION_UNSUPPORTED")
    kind = _str(raw, "kind", required=True, limit=16)
    if kind not in KINDS:
        raise EventRejected("EVENT_KIND_UNKNOWN")
    ts = parse_ts(raw.get("ts"))
    if not in_window(ts, anchor, now=now):  # the table is partitioned by month: an absurd ts must not reach an insert
        raise EventRejected("EVENT_TS_OUT_OF_RANGE")
    channel = _str(raw, "channel", required=True, limit=64)
    stage = _str(raw, "stage", required=True, limit=32)
    status = _str(raw, "status", required=True, limit=16)
    if _NAME.fullmatch(channel) is None:
        raise EventRejected("EVENT_CHANNEL_INVALID")
    if stage not in STAGES:
        raise EventRejected("EVENT_STAGE_UNKNOWN")
    if status not in STATUSES:
        raise EventRejected("EVENT_STATUS_UNKNOWN")
    destination = _str(raw, "destination", limit=64)
    if destination and _NAME.fullmatch(destination) is None:
        raise EventRejected("EVENT_DESTINATION_INVALID")
    instance = _str(raw, "instance", required=True, limit=253)
    channel_version = _int(raw, "channel_version", maximum=2**32 - 1, required=True)
    raw_message_id = raw.get("message_id")
    if raw_message_id is None and kind == "message":
        raise EventRejected("EVENT_FIELD_MISSING")
    message_id = _NIL_UUID if raw_message_id is None else _uuid(raw_message_id)
    topic = _str(raw, "topic", limit=249)
    partition = _int(raw, "partition", maximum=2**31 - 1)
    offset = _int(raw, "offset", maximum=2**63 - 1)
    if kind != "message" and (not topic or partition is None):
        raise EventRejected("EVENT_FIELD_MISSING")
    duration = raw.get("duration_ms")
    if duration is not None and (isinstance(duration, bool) or not isinstance(duration, int | float) or duration < 0):
        raise EventRejected("EVENT_FIELD_TYPE")
    attrs, dropped = clean_attrs(raw.get("attrs"))
    error_code, b1 = _matching(raw, "error_code", _ERROR_CODE, limit=64)
    traceparent, b2 = _matching(raw, "traceparent", _TRACEPARENT, limit=64)
    claim_check, b3 = _matching(raw, "claim_check", _CLAIM_CHECK, limit=1024)
    parent = raw.get("parent_id")
    correlation = raw.get("correlation_id")
    # Idempotency key (README 2.2): (message_id, stage, destination, attempt). Events that carry no
    # message identity (partition events) are distinguished by where and when they happened instead.
    scope = "" if kind == "message" else f"{topic}:{partition}:{offset if offset is not None else ''}:{ts.isoformat()}"
    values = (
        ts,
        kind,
        channel,
        channel_version,
        stage,
        status,
        instance,
        message_id,
        None if parent is None else _uuid(parent),
        None if correlation is None else _uuid(correlation),
        destination,
        _int(raw, "attempt", maximum=9999) or 0,
        _str(raw, "content_type", limit=255),
        _int(raw, "size_bytes", maximum=2**63 - 1) or 0,
        claim_check,
        topic,
        partition,
        offset,
        error_code,
        _str(raw, "error_message", limit=512),
        None if duration is None else float(duration),
        traceparent,
        attrs,
        scope,
    )
    raw_attrs = raw.get("attrs")
    hmac_dropped = (
        sum(1 for name in raw_attrs if isinstance(name, str) and name.endswith("_hmac"))
        if isinstance(raw_attrs, dict)
        else 0
    )
    return EventRow(
        values=values, ts=ts, attrs_dropped=dropped, fields_blanked=b1 + b2 + b3, attrs_dropped_hmac=hmac_dropped
    )
