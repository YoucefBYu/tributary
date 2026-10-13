"""The migrate step's retention logic (ADR-004 R1, R2, R6).

Only `python -m tributary.archiver --migrate-only` (migration credential) runs this. The archivers insert and
never read or apply a TTL.

Per table, in this order:

1. Read the current TTL (days) from `system.tables`. A table that exists with no parseable TTL counts as
   unlimited, so any value is a decrease.
2. Plan: unchanged, lengthened, shortened, or created. **Every shortened table is checked before anything is
   changed.** One refusal (`RetentionDecreaseRefused`, code `RETENTION_DECREASE_REFUSED`) leaves every table as
   it was. `allow_decrease` (`TB_ARCHIVE_ALLOW_TTL_DECREASE=true`) is the only way past it, because rows past
   the new TTL are deleted irreversibly at the next TTL merge.
3. Create absent tables with the configured TTL; run `ALTER TABLE .. MODIFY TTL` for changed ones
   (`materialize_ttl_after_modify` is left at its default, so existing parts get the new TTL; the integration
   test proves a lengthened TTL on rows already stored).
4. Write one `archive_settings` row per change (table, days, previous days, release) and log
   `retention_applied`. Nothing is written for an unchanged table.

**Concurrency.** The migrate Job must run serialized (one at a time per database; Helm Job and Compose service
with concurrency 1). As a second line of defence the live TTL is re-read right before each ALTER and the run
stops with `RetentionChanged` if it moved since planning, so two runs cannot race past the decrease guard.
If an ALTER succeeds but its `archive_settings` row is not written, the next run backfills a row for the live
value (`retention_backfilled`), so a rerun never sees a silent "unchanged".

Lengthening never restores rows already dropped. `ttl_only_drop_parts = 1` means a row can outlive its TTL by
up to one month partition (R1); a prompt single-message delete is R4's lightweight delete, not a shorter TTL.

**Content (W3-2b).** `message_content` is not defined here. W3-2b passes a `TableSpec("message_content", ddl,
content_days)` in `extra`; it then gets the same create, guard, alter and settings row. `ArchiverConfig`
already carries `content_ttl_days` (None unless `TB_ARCHIVE_CONTENT=true`).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

from tributary.sdk.context import Logger

from .errors import AuditLogEngineInvalid, RetentionChanged, RetentionDecreaseRefused
from .schema import (
    AUDIT_TABLE,
    MAX_TTL_DAYS,
    SETTINGS_TABLE,
    TABLE,
    CommandClient,
    audit_ddl,
    ddl,
    qualified,
    settings_ddl,
    settings_upgrade_commands,
)

_IDENT: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_TTL_DAYS: Final = re.compile(r"TTL toDateTime\(ts\) \+ toIntervalDay\((\d+)\)")

Action = Literal["created", "unchanged", "lengthen", "shorten"]


@dataclass(frozen=True, slots=True)
class TableSpec:
    """A table whose TTL the migrate step owns. `ddl(database=..., ttl_days=...)` is its `CREATE IF NOT EXISTS`."""

    name: str
    days: int
    ddl: Callable[..., str]

    def __post_init__(self) -> None:
        if not _IDENT.fullmatch(self.name):
            raise ValueError("table name is not a plain identifier")


@dataclass(frozen=True, slots=True)
class Change:
    table: str
    days: int
    previous_days: int | None
    action: Action


def core_tables(*, events_days: int, audit_days: int) -> list[TableSpec]:
    return [TableSpec(TABLE, events_days, ddl), TableSpec(AUDIT_TABLE, audit_days, audit_ddl)]


def current_ttl_days(client: CommandClient, table: str, database: str | None) -> int | Literal["absent"] | None:
    """Days from `system.tables`, `None` if the table has no TTL we can read, `"absent"` if it does not exist."""
    shown = client.command(
        "SELECT create_table_query FROM system.tables WHERE name = {t:String} "
        "AND database = if({db:String} = '', currentDatabase(), {db:String})",
        parameters={"t": table, "db": database or ""},
    )
    text = str(shown) if shown is not None else ""
    if not text.strip():
        return "absent"
    found = _TTL_DAYS.search(text)
    return int(found.group(1)) if found else None


def check_audit_engine(client: CommandClient, database: str | None) -> None:
    """An existing `audit_log` must be a plain `MergeTree` (append-only). Absent is fine."""
    engine = str(
        client.command(
            "SELECT engine FROM system.tables WHERE name = {t:String} "
            "AND database = if({db:String} = '', currentDatabase(), {db:String})",
            parameters={"t": AUDIT_TABLE, "db": database or ""},
        )
        or ""
    ).strip()
    if engine and engine != "MergeTree":
        raise AuditLogEngineInvalid("audit_log must be a plain MergeTree")
    if not engine:
        return
    kind = str(
        client.command(
            "SELECT default_kind FROM system.columns WHERE name = 'inserted_at' AND table = {t:String} "
            "AND database = if({db:String} = '', currentDatabase(), {db:String})",
            parameters={"t": AUDIT_TABLE, "db": database or ""},
        )
        or ""
    ).strip()
    if kind != "MATERIALIZED":  # a writable inserted_at lets a forged copy win the earliest-copy dedup
        raise AuditLogEngineInvalid("audit_log.inserted_at must be MATERIALIZED")


def _upgrade_settings(client: CommandClient, database: str | None) -> None:
    has = int(
        client.command(
            "SELECT count() FROM system.columns WHERE name = 'applied_by' AND table = {t:String} "
            "AND database = if({db:String} = '', currentDatabase(), {db:String})",
            parameters={"t": SETTINGS_TABLE, "db": database or ""},
        )
        or 0
    )
    for command in settings_upgrade_commands(database=database, has_applied_by=bool(has)):
        client.command(command)


def plan(client: CommandClient, tables: Sequence[TableSpec], database: str | None) -> list[Change]:
    changes: list[Change] = []
    for spec in tables:
        if not isinstance(spec.days, int) or isinstance(spec.days, bool) or not 1 <= spec.days <= MAX_TTL_DAYS:
            raise ValueError("ttl days must be an integer from 1 to 36500")
        current = current_ttl_days(client, spec.name, database)
        action: Action
        if current == "absent":
            changes.append(Change(spec.name, spec.days, None, "created"))
            continue
        if current == spec.days:
            action = "unchanged"
        elif current is None or spec.days < current:
            action = "shorten"
        else:
            action = "lengthen"
        changes.append(Change(spec.name, spec.days, current, action))
    return changes


def apply_retention(
    client: CommandClient,
    *,
    database: str | None,
    events_days: int,
    audit_days: int,
    allow_decrease: bool,
    log: Logger,
    release: str,
    extra: Sequence[TableSpec] = (),
    content_enabled: bool = False,
) -> list[Change]:
    """Create tables and apply TTLs as described in the module doc. Returns what was changed or created."""
    check_audit_engine(client, database)
    tables = [*core_tables(events_days=events_days, audit_days=audit_days), *extra]
    changes = plan(client, tables, database)
    shortened = [c.table for c in changes if c.action == "shorten"]
    if shortened and not allow_decrease:
        raise RetentionDecreaseRefused(shortened)
    by_name = {spec.name: spec for spec in tables}
    client.command(settings_ddl(database=database))
    _upgrade_settings(client, database)
    _backfill(client, changes, database, release, log)
    done: list[Change] = []
    for change in changes:
        spec = by_name[change.table]
        if change.action == "unchanged":
            continue
        if change.action == "created":
            client.command(spec.ddl(database=database, ttl_days=spec.days))
        else:
            # Another migrate run may have changed the TTL since planning: never alter on a stale plan, so the
            # decrease guard cannot be raced. (The Job must also run serialized; see the module doc.)
            if current_ttl_days(client, spec.name, database) != change.previous_days:
                raise RetentionChanged(change.table)
            client.command(
                f"ALTER TABLE {qualified(spec.name, database)} MODIFY TTL toDateTime(ts) + INTERVAL {spec.days} DAY"
            )
        _record(client, change, database, release, allowed=change.action == "shorten")
        log.info(
            "retention_applied",
            table=change.table,
            days=change.days,
            previous_days=change.previous_days,
            action=change.action,
            release=release,
        )
        done.append(change)
    _record_content_flag(client, database, content_enabled, release, log)
    return done


def _record_content_flag(client: Any, database: str | None, enabled: bool, release: str, log: Logger) -> None:
    """Write the content on/off flag (`archive_settings`, row `content_storage`) when it differs from the last one
    recorded, after every table above was created or altered, so `true` is never recorded for a table that was not
    made. The API reads it: off answers 409 CONTENT_STORAGE_OFF, on with the table missing answers 503."""
    table = qualified(SETTINGS_TABLE, database)
    rows = int(client.command(f"SELECT count() FROM {table} WHERE table_name = 'content_storage'") or 0)  # noqa: S608
    if rows:
        last = int(
            client.command(
                f"SELECT argMax(toUInt8(ifNull(enabled, 0)), applied_at) FROM {table} "  # noqa: S608
                "WHERE table_name = 'content_storage'"
            )
            or 0
        )
        if bool(last) == enabled:
            return
    client.command(
        f"INSERT INTO {table} (table_name, days, previous_days, release, decrease_allowed, source, enabled) "  # noqa: S608
        "VALUES ('content_storage', 0, NULL, {r:String}, false, 'applied', {e:Bool})",
        parameters={"r": release, "e": enabled},
    )
    log.info("content_storage_flag", enabled=enabled, release=release)


def _record(
    client: Any,
    change: Change,
    database: str | None,
    release: str,
    *,
    allowed: bool = False,
    source: str = "applied",
) -> None:
    client.command(
        f"INSERT INTO {qualified(SETTINGS_TABLE, database)} "  # noqa: S608
        "(table_name, days, previous_days, release, decrease_allowed, source) "
        "VALUES ({t:String}, {d:UInt32}, {p:Nullable(UInt32)}, {r:String}, {a:Bool}, {s:String})",
        parameters={
            "t": change.table,
            "d": change.days,
            "p": change.previous_days,
            "r": release,
            "a": allowed,
            "s": source,
        },
    )


def _backfill(client: Any, changes: Sequence[Change], database: str | None, release: str, log: Logger) -> None:
    """If an earlier run altered a TTL but died before recording it, the live TTL differs from the last recorded
    row. Record what is live now (previous = the last recorded value), so the history has no silent gap."""
    for change in changes:
        if change.action == "created" or change.previous_days is None:
            continue
        last = int(
            client.command(
                f"SELECT argMax(days, applied_at) FROM {qualified(SETTINGS_TABLE, database)} "  # noqa: S608
                "WHERE table_name = {t:String}",
                parameters={"t": change.table},
            )
            or 0
        )
        if last == change.previous_days:
            continue
        _record(
            client,
            Change(change.table, change.previous_days, last or None, "unchanged"),
            database,
            release,
            source="backfilled",
        )
        log.warning(
            "retention_backfilled",
            table=change.table,
            days=change.previous_days,
            previous_days=last or None,
            release=release,
        )
