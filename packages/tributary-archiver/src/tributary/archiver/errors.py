"""Typed exceptions. Messages are fixed sentences or names: never an event field value (invariant 6)."""

from __future__ import annotations

from tributary.sdk.errors import TributaryError


class ArchiverError(TributaryError):
    """Base for every archiver failure.

    `republished`: records the broker acknowledged before a mid-run failure of replay or reprocess, `None` when
    the failure happened before any publish. At-least-once: those records stay on the topic.
    """

    republished: int | None = None


class ArchiverConfigError(ArchiverError):
    """The archiver cannot start with the configuration it was given."""


class EventRejected(ArchiverError):
    """A record on `_tb.<env>.events` is not an event this archiver stores.

    `code` is a stable token for the `events_rejected` counter. The message never carries the record.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class SinkError(ArchiverError):
    """ClickHouse did not take a batch. The batch is not committed and is retried (idempotent).

    `transient=False` means retrying the same batch cannot succeed until an operator acts (unknown table,
    bad column type, authentication or access refused). The archiver halts on it; it never drops the batch.
    """

    def __init__(self, message: str, *, transient: bool = True, code: str = "SINK_ERROR") -> None:
        super().__init__(message)
        self.transient = transient
        #: `TLS_VERIFY_FAILED` (certificate or hostname check refused) or `SINK_ERROR`. By code only: the
        #: driver's text is never kept.
        self.code = code


class RetentionDecreaseRefused(ArchiverError):
    """The migrate step will not shorten a table's TTL without `TB_ARCHIVE_ALLOW_TTL_DECREASE=true` (ADR-004 R6).

    Rows past the new TTL are deleted irreversibly at the next TTL merge, so a decrease needs the operator's
    explicit flag. Nothing was changed when this is raised. The message names tables, never row data.
    """

    code = "RETENTION_DECREASE_REFUSED"

    def __init__(self, tables: list[str]) -> None:
        super().__init__(
            "retention would shorten " + ", ".join(tables) + "; TB_ARCHIVE_ALLOW_TTL_DECREASE=true allows it"
        )
        self.tables = tables


class RetentionChanged(ArchiverError):
    """A table's TTL changed between planning and the ALTER (a concurrent migrate run). Nothing more is altered."""

    code = "RETENTION_CHANGED_CONCURRENTLY"

    def __init__(self, table: str) -> None:
        super().__init__(f"the TTL of {table} changed during the migration; run the migrate step again")
        self.table = table


class ArchiverGrantsExcessive(ArchiverConfigError):
    """The archiver's ClickHouse user holds more than INSERT and SELECT on `audit_log` (`grants.py`)."""

    code = "ARCHIVER_GRANTS_EXCESSIVE"

    def __init__(self, privilege: str, table: str) -> None:
        super().__init__(f"the archiver user holds {privilege} on {table}")
        self.privilege = privilege
        self.table = table


class ArchiverSettingsUnsafe(ArchiverConfigError):
    """The archiver's ClickHouse session can set `insert_allow_materialized_columns` (`grants.py`)."""

    code = "ARCHIVER_SETTINGS_UNSAFE"

    def __init__(self, setting: str) -> None:
        super().__init__(f"{setting} must be 0 and read-only (CONST) for the archiver user")
        self.setting = setting


class ContentConfigError(ArchiverConfigError):
    """The content archiver cannot start (W3-2b). Codes: `CONTENT_DISABLED` (`TB_ARCHIVE_CONTENT` is not true:
    nothing is stored), `CONTENT_CHANNELS_EMPTY`, `CONTENT_CHANNEL_NOT_ENABLED` (backfill of a channel not in
    `TB_ARCHIVE_CONTENT_CHANNELS`), `CONTENT_SCHEMA_MISSING` (run the migrate step first: the content archiver
    never runs DDL, ADR-004 A5)."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class AuditLogEngineInvalid(ArchiverConfigError):
    """An existing `audit_log` is not a plain `MergeTree` (a `ReplacingMergeTree` would let INSERT overwrite)."""

    code = "AUDIT_LOG_ENGINE_INVALID"


class QueryError(ArchiverError):
    """A query was refused before it ran, or ClickHouse did not answer it.

    `code` is a stable token (`QUERY_INVALID`, `CURSOR_INVALID`, `QUERY_FAILED`). The message is a fixed
    sentence or a parameter NAME, never a parameter value and never the driver's text (the driver echoes SQL).
    """

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


class MessageNotFound(ArchiverError):
    """The archive holds no event for this message id (it may not have arrived yet: the archive lags)."""

    code = "MESSAGE_NOT_FOUND"


class _CodedError(ArchiverError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ReprocessRefused(_CodedError):
    """Reprocess will not run for this message. Codes: `NO_PAYLOAD_COORDINATES` (the events carry no `raw`
    partition and offset, and content storage is not built), `DESTINATION_SCOPED` (a `dlq` record at one
    destination: a `raw` re-injection would re-deliver to every destination), `CHILD_MESSAGE` (the message
    has a `tb-parent-id`: a split child or an earlier reprocess), `INVALID_REQUEST`, `ORDERING_WOULD_BREAK` (the channel is per-key and `allow_reorder` was not passed)."""


class PayloadUnavailable(_CodedError):
    """The payload source cannot produce the original record. Codes: `RETENTION_LAPSED` (offset below the
    log start), `OFFSET_NOT_FOUND`, `RECORD_MISMATCH` (the record at that offset is a different message),
    `RECORD_UNDECODABLE`, `SOURCE_ERROR`, `SOURCE_TIMEOUT`."""


class ReplayRefused(_CodedError):
    """Replay will not run. Codes: `INVALID_REQUEST`, `TOPIC_NOT_FOUND`, `RANGE_TOO_LARGE`, `SOURCE_ERROR`,
    `SOURCE_TIMEOUT`, `ORDERING_WOULD_BREAK`."""


class PublishError(_CodedError):
    """A re-publish was not acknowledged by Kafka (`PRODUCE_FAILED`, `PRODUCE_TIMEOUT`, `PRODUCE_REFUSED`,
    `ENVELOPE_INVALID`). At-least-once: records before the failure may have been delivered."""
