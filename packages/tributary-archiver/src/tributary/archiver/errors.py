"""Typed exceptions. Messages are fixed sentences or names: never an event field value (invariant 6)."""

from __future__ import annotations

from tributary.sdk.errors import TributaryError


class ArchiverError(TributaryError):
    """Base for every archiver failure."""


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
