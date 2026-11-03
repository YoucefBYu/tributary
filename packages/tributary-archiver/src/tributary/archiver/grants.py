"""Startup check: the archiver's ClickHouse user may hold only INSERT and SELECT on `audit_log` (ADR-004 A5, R7),
and the content archiver's also on `message_content` (`check_grants`).

The audit table is the system of record and is append-only (`schema.audit_ddl`). A Secret that carries more than
INSERT and SELECT (ALTER, TRUNCATE, DROP, DELETE, `ALL`, `GRANT OPTION`) would let a compromised archiver alter or
erase it, so the archiver refuses to start (`ArchiverGrantsExcessive`, code `ARCHIVER_GRANTS_EXCESSIVE`). The log
names the privilege and the table only. **Not run on the migrate path**, which needs ALTER.

The grants come from `SHOW GRANTS` (allowed for the current user without any privilege). A privilege held through
a role is not expanded by `SHOW GRANTS`, so a role grant is refused as unverifiable (`ROLE`): give the archiver
user its INSERT and SELECT directly.
"""

from __future__ import annotations

import re
from typing import Any, Final

from .errors import ArchiverGrantsExcessive, ArchiverSettingsUnsafe
from .schema import AUDIT_TABLE, CommandClient

#: `audit_log.inserted_at` is MATERIALIZED so a writer cannot backdate a copy. ClickHouse lets an INSERT override
#: that with `SETTINGS insert_allow_materialized_columns=1` unless the user's profile pins it (README, profile DDL).
PINNED_SETTING: Final = "insert_allow_materialized_columns"
ALLOWED: Final = frozenset({"INSERT", "SELECT"})
_GRANT: Final = re.compile(r"^GRANT (?P<privs>.+?) ON (?P<scope>\S+) TO .*?(?P<option> WITH GRANT OPTION)?\s*$")
_ROLE_GRANT: Final = re.compile(r"^GRANT .+ TO ")


def _split_privileges(text: str) -> list[str]:
    parts, depth, current = [], 0, ""
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch
    parts.append(current)
    return [re.sub(r"\(.*\)", "", p).strip().upper() for p in parts if p.strip()]


def _covers(scope: str, database: str, table: str) -> bool:
    db, _, tbl = scope.replace("`", "").partition(".")
    # Fail closed: any wildcard (`*`, `d*`, `audit_*`) may reach the table, so it counts as covering.
    return ("*" in db or db == database) and ("*" in tbl or tbl == table)


def excess_on_audit_log(grants: list[str], database: str) -> list[str]:
    return excess_on(grants, database, AUDIT_TABLE)


def excess_on(grants: list[str], database: str, table: str) -> list[str]:
    """Privileges beyond INSERT and SELECT that reach `table`, in order of appearance."""
    excess: list[str] = []
    for line in grants:
        found = _GRANT.match(line.strip())
        if found is None:
            if _ROLE_GRANT.match(line.strip()):
                excess.append("ROLE")
            continue
        if not _covers(found.group("scope"), database, table):
            continue
        excess.extend(p for p in _split_privileges(found.group("privs")) if p not in ALLOWED)
        if found.group("option"):
            excess.append("GRANT OPTION")
    return list(dict.fromkeys(excess))


def check_audit_grants(client: CommandClient, *, database: str) -> None:
    check_grants(client, database=database, tables=(AUDIT_TABLE,))


def check_grants(client: CommandClient, *, database: str, tables: tuple[str, ...]) -> None:
    """Refuse when the current user holds anything beyond INSERT and SELECT on any of `tables`.

    The content archiver passes `(audit_log, message_content)`: with ALTER on `message_content` it could MODIFY TTL
    (bypassing the R6 decrease guard) or delete PHI. A wildcard scope counts as covering (fail closed)."""
    shown: Any = client.command("SHOW GRANTS")
    lines = [str(x) for x in shown] if isinstance(shown, list | tuple) else str(shown).splitlines()
    for table in tables:
        excess = excess_on(lines, database, table)
        if excess:
            raise ArchiverGrantsExcessive(excess[0], table)


def check_session_settings(client: CommandClient) -> None:
    """Refuse unless `insert_allow_materialized_columns` is 0 and read-only (a CONST settings-profile constraint)
    for the current user, as `system.settings` reports it. Startup only, like `check_audit_grants`."""
    query = "SELECT {col} FROM system.settings WHERE name = {{n:String}}"
    value = str(client.command(query.format(col="value"), parameters={"n": PINNED_SETTING})).strip()
    readonly = str(client.command(query.format(col="readonly"), parameters={"n": PINNED_SETTING})).strip()
    if value != "0" or readonly != "1":
        raise ArchiverSettingsUnsafe(PINNED_SETTING)
