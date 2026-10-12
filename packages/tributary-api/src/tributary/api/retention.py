"""`GET /v1/environments/{env}/retention`: the effective archive retention per store, read from `archive_settings`
(what the migrate step last applied; ADR-004 R3, R6). Read-only: the API has no write surface for retention.

The API holds SELECT on the archive (ADR-004 A5). The read is one bounded, readonly query through the same stdlib
HTTP client as the tamper check, so it needs no archive library and no new dependency. Nothing here is PHI:
store names, day counts, a timestamp and a release string.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Final

from .problems import ApiProblem

SETTINGS_TABLE: Final = "archive_settings"
#: `archive_settings.table_name` -> the contract's `store`.
STORES: Final = {"message_events": "events", "message_content": "content", "audit_log": "audit"}
_RELEASE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")
_DB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def retention_sql(*, database: str) -> str:
    if not _DB_NAME.fullmatch(database):
        raise ValueError("invalid database name")
    return (
        "SELECT table_name, argMax(days, applied_at) AS eff_days, argMax(previous_days, applied_at) AS eff_previous, "  # noqa: S608
        "max(applied_at) AS eff_applied_at, argMax(release, applied_at) AS eff_release "
        f"FROM `{database}`.{SETTINGS_TABLE} GROUP BY table_name ORDER BY table_name FORMAT JSONEachRow"
    )


def _timestamp(raw: str) -> str:
    when = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    return when.strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"


def parse(environment: str, text: str) -> dict[str, Any]:
    """The contract's `RetentionSettings` from the query's JSONEachRow output. A row the contract cannot carry
    (unknown table, out-of-range days) is dropped rather than shown wrong."""
    stores: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        store = STORES.get(row.get("table_name"))
        days = int(row["eff_days"])
        if store is None or not 1 <= days <= 36500:
            continue
        previous = row.get("eff_previous")
        entry: dict[str, Any] = {
            "store": store,
            "days": days,
            "previous_days": None if previous is None else int(previous),
            "applied_at": _timestamp(str(row["eff_applied_at"])),
        }
        if isinstance(row.get("eff_release"), str) and _RELEASE.fullmatch(row["eff_release"]):
            entry["release"] = row["eff_release"]
        stores.append(entry)
    order = {"events": 0, "content": 1, "audit": 2}
    stores.sort(key=lambda s: order[s["store"]])
    return {"environment": environment, "stores": stores}


class RetentionReader:
    def __init__(self, query: Callable[[str], str], targets: Mapping[str, str]) -> None:
        self._query = query
        self._targets = dict(targets)

    async def read(self, environment: str) -> dict[str, Any]:
        """`503 ARCHIVE_UNAVAILABLE` when the archive is not configured for the environment or cannot answer."""
        database = self._targets.get(environment)
        if database is None:
            raise ApiProblem("ARCHIVE_UNAVAILABLE")
        try:
            text = await asyncio.to_thread(self._query, retention_sql(database=database))
            return parse(environment, text)
        except Exception:  # any failure, the client's bare error included: the answer is the same
            raise ApiProblem("ARCHIVE_UNAVAILABLE") from None
