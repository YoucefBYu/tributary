"""Startup check: the archiver's ClickHouse user may hold only INSERT and SELECT on `audit_log` (ADR-004 A5, R7).

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


def _covers(scope: str, database: str) -> bool:
    db, _, table = scope.replace("`", "").partition(".")
    # Fail closed: any wildcard (`*`, `d*`, `audit_*`) may reach audit_log, so it counts as covering.
    return ("*" in db or db == database) and ("*" in table or table == AUDIT_TABLE)


def excess_on_audit_log(grants: list[str], database: str) -> list[str]:
    """Privileges beyond INSERT and SELECT that reach `audit_log`, in order of appearance."""
    excess: list[str] = []
    for line in grants:
        found = _GRANT.match(line.strip())
        if found is None:
            if _ROLE_GRANT.match(line.strip()):
                excess.append("ROLE")
            continue
        if not _covers(found.group("scope"), database):
            continue
        excess.extend(p for p in _split_privileges(found.group("privs")) if p not in ALLOWED)
        if found.group("option"):
            excess.append("GRANT OPTION")
    return list(dict.fromkeys(excess))


def check_audit_grants(client: CommandClient, *, database: str) -> None:
    shown: Any = client.command("SHOW GRANTS")
    lines = [str(x) for x in shown] if isinstance(shown, list | tuple) else str(shown).splitlines()
    excess = excess_on_audit_log(lines, database)
    if excess:
        raise ArchiverGrantsExcessive(excess[0], AUDIT_TABLE)


def check_session_settings(client: CommandClient) -> None:
    """Refuse unless `insert_allow_materialized_columns` is 0 and read-only (a CONST settings-profile constraint)
    for the current user, as `system.settings` reports it. Startup only, like `check_audit_grants`."""
    query = "SELECT {col} FROM system.settings WHERE name = {{n:String}}"
    value = str(client.command(query.format(col="value"), parameters={"n": PINNED_SETTING})).strip()
    readonly = str(client.command(query.format(col="readonly"), parameters={"n": PINNED_SETTING})).strip()
    if value != "0" or readonly != "1":
        raise ArchiverSettingsUnsafe(PINNED_SETTING)
