"""`_tb.<env>.audit` records into `audit_log` rows (`contracts/audit/v0`; ADR-004 R7 rev 3).

The events archiver consumes the audit topic in the same loop as `_tb.<env>.events` (same group, same principal,
READ LITERAL on the topic: no grant beyond `topics/v0`). Off the critical path (invariant 9): the API produced
and acknowledged the record before it acted; nothing waits on this consumer.

**Validation is the schema's, by hand** (no JSON Schema dependency in the archiver runtime): `v` exactly 0, every
required field, the closed enums, the patterns, and the per-action conditionals (what each action requires, the
`allow_reorder` rule of invariant 5, `error_code` on `failed`, no `version` on undeploy). **A record that does
not parse or fails the schema is not stored** (README 4): it is rejected with a fixed code, counted, and logged
with topic, partition and offset only, so an operator can inspect it with Kafka tools inside the 30 d buffer.
That includes a different `environment` than the topic's.

**Lenient consumer (audit/v0 README 4, ruled 2026-10-13).** A top-level field the contract does not list is
dropped and the known fields are stored (`unknown_fields` counts them; the loop logs
`audit_unknown_fields_ignored` with the count and topic/partition/offset, never a field name or value), so an
additive audit field is not lost records before every archiver upgrades. A record with `v` other than 0 is not
stored at all (`UnsupportedVersion`): counted as `unsupported_version`, logged by offset, committed past,
because a breaking change could alter what the known fields mean.

**Append-only, dedup at read.** Each accepted record is one new row in a plain `MergeTree`. A redelivery
is a second identical row; reads collapse on `audit_id` keeping the earliest-archived copy
(`schema.audit_dedup_sql`). Two copies of one `audit_id` whose content differs are the tamper signal
(`schema.audit_tamper_sql`), not a redelivery. `inserted_at` is server time (MATERIALIZED) and is never sent.

Invariant 6: the row holds the contract's fields only: ids, actor (staff identity, not PHI), roles, codes,
counts, timestamps. Never payload bytes, a key or a header value.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import orjson

from .errors import AuditRejected, UnsupportedVersion
from .tswindow import EARLIEST, LATEST_RANGE, LOOKBACK, in_window

TABLE: Final = "audit_log"
MAX_RECORD_BYTES: Final = 8192

COLUMNS: Final = (
    "audit_id",
    "ts",
    "environment",
    "action",
    "phase",
    "actor",
    "actor_roles",
    "request_id",
    "operation_id",
    "message_id",
    "deployment_id",
    "channel",
    "destination",
    "target_topic",
    "stage",
    "error_code",
    "version",
    "previous_version",
    "allow_reorder",
    "range_start",
    "range_end",
    "count",
    "source_partition",
    "source_offset",
)

_UUID: Final = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$")
_UUID7: Final = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_TIMESTAMP: Final = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$")
_NAME: Final = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_ENV: Final = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_ACTOR: Final = re.compile(r"^[\x21-\x7e]{1,255}$")
_TARGET: Final = re.compile(r"^[a-z][a-z0-9-]{0,15}\.[a-z][a-z0-9-]{0,63}\.raw$")
_ERROR_CODE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")

ACTIONS: Final = frozenset(
    {
        "message.content.view",
        "message.reprocess",
        "channel.replay",
        "channel.version.create",
        "deployment.deploy",
        "deployment.rollback",
        "deployment.undeploy",
    }
)
PHASES: Final = frozenset({"requested", "completed", "failed"})
ROLES: Final = frozenset({"viewer", "developer", "operator", "auditor", "admin"})
STAGES: Final = frozenset({"received", "routed", "dlq"})
_OPTIONAL: Final = frozenset(
    {
        "request_id",
        "operation_id",
        "channel",
        "message_id",
        "stage",
        "destination",
        "deployment_id",
        "version",
        "previous_version",
        "allow_reorder",
        "target_topic",
        "range_start",
        "range_end",
        "count",
        "error_code",
    }
)
_REQUIRED: Final = frozenset({"v", "audit_id", "ts", "environment", "action", "phase", "actor", "actor_roles"})
_KNOWN: Final = _REQUIRED | _OPTIONAL
_MAX_VERSION: Final = 2_147_483_647
_MAX_COUNT: Final = 2**64 - 1  # UInt64: a larger value would make the whole insert fail


@dataclass(frozen=True, slots=True)
class AuditRow:
    """One `audit_log` row in `COLUMNS` order. `table` routes it to its sink in the archive loop."""

    values: tuple[Any, ...]
    ts: datetime
    table: str = TABLE
    attrs_dropped: int = 0
    attrs_dropped_hmac: int = 0
    fields_blanked: int = 0
    #: Top-level fields the contract does not list: dropped, never stored; the loop logs the count only.
    unknown_fields: int = 0


def _bad(code: str) -> AuditRejected:
    return AuditRejected(code)


def _match(record: dict[str, Any], name: str, pattern: re.Pattern[str]) -> str | None:
    """A field that is absent is None; a field that is PRESENT must be valid (a JSON null is not absent: the
    schema has no nullable field)."""
    if name not in record:
        return None
    value = record[name]
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise _bad("AUDIT_FIELD_INVALID")  # the field name stays out of logs: the code is fixed
    return value


def _number(value: Any) -> int | None:
    """JSON Schema `integer`: an int, or a float with an integral value (`1.0`); never a bool."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _int(record: dict[str, Any], name: str, low: int, high: int | None = None) -> int | None:
    if name not in record:
        return None
    value = _number(record[name])
    if value is None or value < low or (high is not None and value > high):
        raise _bad("AUDIT_FIELD_INVALID")
    return value


def _when(text: str | None) -> datetime | None:
    if text is None:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        raise _bad("AUDIT_FIELD_INVALID") from None


def _bounded(value: datetime | None) -> datetime | None:
    """`range_start`/`range_end` are DateTime64 columns (1900-2299): a value outside makes the insert fail."""
    if value is not None and not EARLIEST <= value <= LATEST_RANGE:
        raise _bad("AUDIT_TIMESTAMP_INVALID")
    return value


def _require(present: dict[str, Any], *names: str) -> None:
    if any(present.get(n) is None for n in names):
        raise _bad("AUDIT_FIELD_MISSING")


def parse_audit_record(
    value: bytes | None, *, env: str, partition: int, offset: int, anchor: datetime | None = None
) -> AuditRow:
    """The row for one record on `_tb.<env>.audit`, or `EventRejected(code)`. Codes are fixed; the record is
    never quoted."""
    if value is None or not value:
        raise _bad("AUDIT_EMPTY")
    if len(value) > MAX_RECORD_BYTES:
        raise _bad("AUDIT_TOO_LARGE")
    try:
        record = orjson.loads(value)
    except orjson.JSONDecodeError:
        raise _bad("AUDIT_NOT_JSON") from None
    if not isinstance(record, dict):
        raise _bad("AUDIT_NOT_OBJECT")
    if "v" not in record:
        raise _bad("AUDIT_FIELD_MISSING")
    version_field = _number(record["v"])
    if version_field is None:
        raise _bad("AUDIT_FIELD_INVALID")
    if version_field != 0:
        raise UnsupportedVersion  # any other v is not stored: it could change what the known fields mean
    unknown = sum(1 for name in record if name not in _KNOWN)  # lenient consumer (README 4): drop, never store
    if any(name not in record for name in _REQUIRED):
        raise _bad("AUDIT_FIELD_MISSING")

    audit_id = _match(record, "audit_id", _UUID7)
    ts = _when(_match(record, "ts", _TIMESTAMP))
    if ts is not None and not in_window(
        ts, anchor, lookback=LOOKBACK
    ):  # `audit_log` is partitioned by month (tswindow.py)
        raise _bad("AUDIT_TIMESTAMP_INVALID")
    environment = _match(record, "environment", _ENV)
    action, phase = record["action"], record["phase"]
    if not (isinstance(action, str) and action in ACTIONS and isinstance(phase, str) and phase in PHASES):
        raise _bad("AUDIT_FIELD_INVALID")  # a list or dict is unhashable: check the type before the membership
    actor = _match(record, "actor", _ACTOR)
    roles = record["actor_roles"]
    if (
        not isinstance(roles, list)
        or not 1 <= len(roles) <= 5
        or not all(isinstance(r, str) and r in ROLES for r in roles)
        or len(set(roles)) != len(roles)
    ):
        raise _bad("AUDIT_FIELD_INVALID")
    if environment != env:
        raise _bad("AUDIT_ENVIRONMENT_MISMATCH")  # a record for another environment never lands in this one

    request_id = _match(record, "request_id", _UUID)
    operation_id = _match(record, "operation_id", _UUID7)
    channel = _match(record, "channel", _NAME)
    message_id = _match(record, "message_id", _UUID)
    stage = record.get("stage")
    if "stage" in record and not (isinstance(stage, str) and stage in STAGES):
        raise _bad("AUDIT_FIELD_INVALID")
    destination = _match(record, "destination", _NAME)
    deployment_id = _match(record, "deployment_id", _UUID)
    version = _int(record, "version", 1, _MAX_VERSION)
    previous_version = _int(record, "previous_version", 1, _MAX_VERSION)
    allow_reorder = record.get("allow_reorder")
    if "allow_reorder" in record and not isinstance(allow_reorder, bool):
        raise _bad("AUDIT_FIELD_INVALID")
    target_topic = _match(record, "target_topic", _TARGET)
    range_start = _bounded(_when(_match(record, "range_start", _TIMESTAMP)))
    range_end = _bounded(_when(_match(record, "range_end", _TIMESTAMP)))
    count = _int(record, "count", 0, _MAX_COUNT)
    error_code = _match(record, "error_code", _ERROR_CODE)

    _conditionals(
        action,
        phase,
        {
            "operation_id": operation_id,
            "channel": channel,
            "message_id": message_id,
            "stage": stage,
            "deployment_id": deployment_id,
            "version": version,
            "allow_reorder": allow_reorder,
            "target_topic": target_topic,
            "range_start": range_start,
            "range_end": range_end,
            "count": count,
            "error_code": error_code,
        },
    )
    if ts is None or audit_id is None or actor is None:  # unreachable: required fields were checked above
        raise _bad("AUDIT_FIELD_MISSING")
    values = (
        uuid.UUID(audit_id),
        ts,
        environment,
        action,
        phase,
        actor,
        list(roles),
        uuid.UUID(request_id) if request_id else None,
        uuid.UUID(operation_id) if operation_id else None,
        uuid.UUID(message_id) if message_id else None,
        uuid.UUID(deployment_id) if deployment_id else None,
        channel or "",
        destination or "",
        target_topic or "",
        stage or "",
        error_code or "",
        version,
        previous_version,
        allow_reorder,
        range_start,
        range_end,
        count,
        partition,
        offset,
    )
    return AuditRow(values=values, ts=ts, unknown_fields=unknown)


def _conditionals(action: str, phase: str, f: dict[str, Any]) -> None:
    """The schema's `allOf`: what each action requires."""
    if action == "message.content.view":
        _require(f, "channel", "message_id", "stage")
        if phase != "requested":
            raise _bad("AUDIT_FIELD_INVALID")
    elif action == "message.reprocess":
        _require(f, "channel", "message_id", "operation_id")
    elif action == "channel.replay":
        _require(f, "channel", "operation_id", "target_topic", "range_start", "range_end")
    if action in ("message.reprocess", "channel.replay"):
        if phase == "requested":
            _require(f, "allow_reorder")  # invariant 5: the operator's reorder choice is on the record first
        else:
            _require(f, "count")
    if phase == "failed":
        _require(f, "error_code")
    if action in ("channel.version.create", "deployment.deploy", "deployment.rollback", "deployment.undeploy"):
        _require(f, "channel")
        if phase != "requested":
            raise _bad("AUDIT_FIELD_INVALID")
    if action in ("deployment.deploy", "deployment.rollback", "deployment.undeploy"):
        _require(f, "deployment_id")
    if action in ("channel.version.create", "deployment.deploy", "deployment.rollback"):
        _require(f, "version")
    if action == "deployment.undeploy" and f.get("version") is not None:
        raise _bad("AUDIT_FIELD_INVALID")
