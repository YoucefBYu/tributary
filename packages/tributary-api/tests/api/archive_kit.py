"""Helpers for the archive endpoint tests: a real ClickHouse (testcontainers), the archiver's own DDL, and row
builders. Synthetic ids and payload bytes only (invariant 6)."""

from __future__ import annotations

import base64
import urllib.error
import urllib.request
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from tributary.api.chclient import ArchiveClient
from tributary.api.tamper import ClickHouseHttp


class Admin:
    """Full-rights HTTP access for setup only; the API under test uses its own readonly client."""

    def __init__(self, url: str, user: str, password: str) -> None:
        self.url, self.user, self.password = url, user, password
        self._auth = base64.b64encode(f"{user}:{password}".encode()).decode()

    def run(self, sql: str) -> str:
        req = urllib.request.Request(  # noqa: S310
            self.url, data=sql.encode(), method="POST", headers={"Authorization": f"Basic {self._auth}"}
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
                return str(r.read().decode())
        except urllib.error.HTTPError as exc:  # test setup only: show ClickHouse's own message
            raise RuntimeError(exc.read().decode()[:300]) from None

    def client(self) -> ArchiveClient:
        return ArchiveClient(ClickHouseHttp(self.url, self.user, self.password, single_flight=False))


class Raw(str):
    """An SQL expression, inserted as is."""


def lit(value: Any) -> str:
    """A ClickHouse literal for test inserts."""
    if isinstance(value, Raw):
        return str(value)
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, uuid.UUID):
        return f"'{value}'"
    if isinstance(value, datetime):
        return "'" + value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.") + f"{value.microsecond // 1000:03d}'"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{lit(k)}: {lit(v)}" for k, v in value.items()) + "}"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def insert(admin: Admin, database: str, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    cols = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)
    admin.run(f"INSERT INTO `{database}`.{table} ({cols}) VALUES {values}")  # noqa: S608


EVENT_COLUMNS = (
    "ts", "kind", "channel", "channel_version", "stage", "status", "instance", "message_id", "parent_id",
    "correlation_id", "destination", "attempt", "content_type", "size_bytes", "claim_check", "topic", "partition",
    "offset", "error_code", "error_message", "duration_ms", "traceparent", "attrs", "scope",
)  # fmt: skip


def event(
    ts: datetime,
    message_id: uuid.UUID,
    stage: str,
    *,
    channel: str = "adt",
    status: str = "ok",
    destination: str = "",
    attempt: int = 0,
    version: int = 1,
    instance: str = "worker-0",
    error_code: str = "",
    duration_ms: float | None = None,
    attrs: dict[str, str] | None = None,
    parent_id: uuid.UUID | None = None,
    correlation_id: uuid.UUID | None = None,
    topic: str = "",
    partition: int | None = None,
    offset: int | None = None,
) -> tuple[Any, ...]:
    return (
        ts, "message", channel, version, stage, status, instance, message_id, parent_id, correlation_id, destination,
        attempt, "application/hl7-v2", 120, "", topic, partition, offset, error_code, "", duration_ms, "",
        attrs or {}, "",
    )  # fmt: skip


CONTENT_COLUMNS = (
    "ts", "channel", "channel_version", "message_id", "stage", "destination", "content_type", "size_bytes",
    "payload", "claim_ref", "parent_id", "correlation_id", "attempt", "error_code", "topic", "partition", "offset",
)  # fmt: skip


def content(
    ts: datetime,
    message_id: uuid.UUID,
    stage: str,
    payload: bytes,
    *,
    channel: str = "adt",
    destination: str = "",
    claim_ref: str = "",
) -> tuple[Any, ...]:
    # The payload goes through `base64Decode` so arbitrary bytes survive the SQL text.
    return (
        ts, channel, 1, message_id, stage, destination, "application/hl7-v2", len(payload),
        Raw(f"base64Decode('{base64.b64encode(payload).decode()}')"), claim_ref, None, None, 0, "", "", 0, 0,
    )  # fmt: skip


def now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def msg_id(when: datetime | None = None) -> uuid.UUID:
    """A UUIDv7 whose time is `when` (so the library's time pruning sees a coherent id)."""
    from tributary.sdk.ids import uuid7

    return uuid.UUID(uuid7(unix_ts_ms=int((when or now()).timestamp() * 1000)))


def make_database(admin: Admin, *, with_content: bool = True) -> str:
    from tributary.archiver.content import content_ddl
    from tributary.archiver.schema import ddl, settings_ddl

    db = f"t_{uuid.uuid4().hex[:10]}"
    admin.run(f"CREATE DATABASE {db}")
    admin.run(ddl(database=db))
    admin.run(settings_ddl(database=db))
    if with_content:
        admin.run(content_ddl(database=db))
    return db


def minutes_ago(n: float) -> datetime:
    return now() - timedelta(minutes=n)


# -- a live server, for streaming responses (the ASGI test transport buffers a whole response) -----------


class Sse:
    """A blocking reader of one `text/event-stream` response."""

    def __init__(self, base: str, path: str, token: str, *, headers: dict[str, str] | None = None) -> None:
        import http.client
        from urllib.parse import urlsplit

        parts = urlsplit(base)
        self.conn = http.client.HTTPConnection(parts.hostname or "127.0.0.1", parts.port or 80, timeout=20)
        self.conn.request("GET", path, headers={"Authorization": f"Bearer {token}", **(headers or {})})
        self.response = self.conn.getresponse()
        self.status = self.response.status

    def body(self) -> bytes:
        return self.response.read()

    def frames(
        self, count: int, *, kinds: tuple[str, ...] | None = None
    ) -> list[tuple[str, str | None, dict[str, Any]]]:
        """The next `count` frames as `(event, id, data)`, optionally skipping other kinds."""
        import json as _json

        out: list[tuple[str, str | None, dict[str, Any]]] = []
        event, ident, data = "message", None, ""
        while len(out) < count:
            line = self.response.readline()
            if not line:
                break  # the server closed the stream
            text = line.decode().rstrip("\n")
            if text.startswith("event: "):
                event = text[7:]
            elif text.startswith("id: "):
                ident = text[4:]
            elif text.startswith("data: "):
                data = text[6:]
            elif text == "" and data:
                if kinds is None or event in kinds:
                    out.append((event, ident, _json.loads(data)))
                event, ident, data = "message", None, ""
        return out

    def close(self) -> None:
        self.conn.close()


def free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def restore_uvicorn_logging() -> None:
    """Undo what a test server's `uvicorn.Config(access_log=False)` did to the process-wide `uvicorn.access`
    logger (no propagation, and whatever pytest attached while it was off). The API refuses to start with a
    handler on that logger, so the next test's app would otherwise fail its own startup check."""
    import logging

    logger = logging.getLogger("uvicorn.access")
    logger.handlers = []
    logger.propagate = True


def set_content_flag(admin: Admin, database: str, enabled: bool) -> None:
    """The row the archiver's migrate step writes: `archive_settings` pseudo-table `content_storage`."""
    admin.run(
        f"INSERT INTO `{database}`.archive_settings (table_name, days, previous_days, release, decrease_allowed, "  # noqa: S608
        f"source, enabled) VALUES ('content_storage', 0, NULL, 'test', false, 'applied', {1 if enabled else 0})"
    )
