"""Retention planning and the decrease guard (ADR-004 R6) against a fake ClickHouse that tracks TTLs."""

from __future__ import annotations

import re
from typing import Any

import pytest
from tributary.archiver.errors import RetentionDecreaseRefused
from tributary.archiver.retention import TableSpec, apply_retention
from tributary.archiver.schema import audit_ddl, ddl, settings_ddl
from tributary.runtime.logs import JsonLogger


class FakeClickHouse:
    """Tables and TTL days; `SHOW CREATE`-style text comes from `system.tables`."""

    def __init__(self, **tables: int | None) -> None:
        self.tables = dict(tables)
        self.commands: list[str] = []
        # Existing tables start with their last change recorded, as after a normal migrate run.
        self.settings_rows: list[dict[str, Any]] = [
            {"t": t, "d": d, "p": None, "r": "old", "a": False} for t, d in tables.items() if d is not None
        ]
        self.audit_engine = "MergeTree"
        self.inserted_at_kind = "MATERIALIZED"
        self.has_applied_by = True
        self.on_alter: Any = None
        self.fail_record = False

    def command(self, cmd: str, parameters: dict[str, Any] | None = None, **_: Any) -> Any:
        self.commands.append(cmd)
        if cmd.startswith("SELECT create_table_query"):
            assert parameters is not None
            name = parameters["t"]
            if name not in self.tables:
                return ""
            days = self.tables[name]
            return "CREATE TABLE x" + ("" if days is None else f" TTL toDateTime(ts) + toIntervalDay({days})")
        if cmd.startswith("CREATE TABLE IF NOT EXISTS"):
            name = re.search(r"EXISTS (?:`[^`]+`\.)?(\w+)", cmd).group(1)  # type: ignore[union-attr]
            found = re.search(r"INTERVAL (\d+) DAY", cmd)
            self.tables.setdefault(name, int(found.group(1)) if found else None)
        elif cmd.startswith("SELECT argMax"):
            assert parameters is not None
            recorded = [r for r in self.settings_rows if r["t"] == parameters["t"]]
            return recorded[-1]["d"] if recorded else 0
        elif cmd.startswith("SELECT default_kind"):
            return self.inserted_at_kind
        elif cmd.startswith("SELECT engine"):
            return self.audit_engine if "audit_log" in self.tables else ""
        elif cmd.startswith("SELECT count() FROM system.columns"):
            return 1 if self.has_applied_by else 0
        elif cmd.startswith("ALTER TABLE") and "MODIFY TTL" not in cmd:
            pass
        elif cmd.startswith("ALTER TABLE"):
            if self.on_alter is not None:
                self.on_alter(self)
            name = re.search(r"ALTER TABLE (?:`[^`]+`\.)?(\w+)", cmd).group(1)  # type: ignore[union-attr]
            self.tables[name] = int(re.search(r"INTERVAL (\d+) DAY", cmd).group(1))  # type: ignore[union-attr]
        elif cmd.startswith("INSERT INTO"):
            assert parameters is not None
            if self.fail_record:
                raise OSError("simulated")
            self.settings_rows.append(parameters)
        return None


LOG = JsonLogger("retention-test")


def run(client: FakeClickHouse, *, events: int = 90, audit: int = 2190, allow: bool = False, **kw: Any) -> list[Any]:
    return apply_retention(
        client, database="d", events_days=events, audit_days=audit, allow_decrease=allow, log=LOG, release="t", **kw
    )


def test_fresh_database_creates_both_tables_with_their_own_ttl_and_records_settings() -> None:
    client = FakeClickHouse()
    done = run(client, events=45, audit=400)
    assert client.tables == {"message_events": 45, "audit_log": 400, "archive_settings": None}
    assert {(c.table, c.action) for c in done} == {("message_events", "created"), ("audit_log", "created")}
    assert [r["p"] for r in client.settings_rows] == [None, None]
    assert not any(r["a"] for r in client.settings_rows)


def test_unchanged_is_a_no_op() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    assert run(client) == []
    assert not [c for c in client.commands if c.startswith(("INSERT",)) or "MODIFY" in c]


def test_lengthening_alters_without_a_flag() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    run(client, events=120)
    assert client.tables["message_events"] == 120
    assert client.settings_rows[-1] == {"t": "message_events", "d": 120, "p": 90, "r": "t", "a": False, "s": "applied"}
    assert len(client.settings_rows) == 3


def test_a_decrease_is_refused_and_changes_nothing_even_when_another_table_would_lengthen() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    with pytest.raises(RetentionDecreaseRefused) as raised:
        run(client, events=30, audit=3000)
    assert raised.value.code == "RETENTION_DECREASE_REFUSED"
    assert raised.value.tables == ["message_events"]
    assert client.tables == {"message_events": 90, "audit_log": 2190}
    assert not [c for c in client.commands if c.startswith(("ALTER", "CREATE", "INSERT"))]


def test_a_decrease_is_applied_with_the_flag() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    run(client, events=30, allow=True)
    assert client.tables["message_events"] == 30
    assert client.settings_rows[-1]["p"] == 90 and client.settings_rows[-1]["a"] is True


def test_a_table_with_no_readable_ttl_counts_as_unlimited() -> None:
    client = FakeClickHouse(message_events=None, audit_log=2190)
    with pytest.raises(RetentionDecreaseRefused):
        run(client)


def test_extra_table_is_wired_for_content() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    content = TableSpec(
        "message_content",
        30,
        lambda **kw: f"CREATE TABLE IF NOT EXISTS message_content TTL INTERVAL {kw['ttl_days']} DAY",
    )
    run(client, extra=[content])
    assert client.tables["message_content"] == 30
    with pytest.raises(RetentionDecreaseRefused):
        apply_retention(
            client,
            database="d",
            events_days=90,
            audit_days=2190,
            allow_decrease=False,
            log=LOG,
            release="t",
            extra=[TableSpec("message_content", 7, content.ddl)],
        )


@pytest.mark.parametrize("bad", [0, 36501, True])
def test_out_of_range_days_are_refused(bad: Any) -> None:
    with pytest.raises(ValueError):
        run(FakeClickHouse(), events=bad)


def test_ddl_shapes() -> None:
    audit = audit_ddl(ttl_days=2190)
    assert "ENGINE = MergeTree" in audit and "ReplacingMergeTree" not in audit and "PARTITION BY toYYYYMM(ts)" in audit
    assert "INTERVAL 2190 DAY" in audit and "ttl_only_drop_parts = 1" in audit
    assert "payload" not in audit
    assert "TTL" not in settings_ddl()
    assert "INTERVAL 5 DAY" in ddl(ttl_days=5)


def test_a_concurrent_change_between_plan_and_alter_stops_the_run() -> None:
    """Two migrate runs must not race past the decrease guard: the live TTL is re-read before each ALTER."""
    from tributary.archiver.errors import RetentionChanged

    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.on_alter = lambda c: None
    original = client.command

    def racing(cmd: str, parameters: dict[str, Any] | None = None, **kw: Any) -> Any:
        if cmd.startswith("ALTER TABLE") and "ADD COLUMN" not in cmd:  # pragma: no cover - must not be reached
            raise AssertionError("altered on a stale plan")
        return original(cmd, parameters=parameters, **kw)

    # Simulate the other run lengthening events to 200 right after our plan, before our ALTER.
    state = {"planned": False}

    def command(cmd: str, parameters: dict[str, Any] | None = None, **kw: Any) -> Any:
        if cmd.startswith("CREATE TABLE IF NOT EXISTS archive_settings") or (
            "archive_settings" in cmd and not state["planned"]
        ):
            state["planned"] = True
            client.tables["message_events"] = 200
        return racing(cmd, parameters, **kw)

    client.command = command  # type: ignore[method-assign]
    with pytest.raises(RetentionChanged):
        run(client, events=120)
    assert client.tables["message_events"] == 200


def test_a_lost_settings_row_is_backfilled_on_the_next_run() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.settings_rows = [{"t": "message_events", "d": 60, "p": None, "r": "old", "a": False}]
    run(client)  # live 90 differs from the last recorded 60: the gap is recorded, nothing else changes
    assert {"t": "message_events", "d": 90, "p": 60, "r": "t", "a": False, "s": "backfilled"} in client.settings_rows
    assert not [c for c in client.commands if c.startswith("ALTER") and "MODIFY" in c]


def test_audit_log_with_another_engine_is_refused_before_anything_changes() -> None:
    from tributary.archiver.errors import AuditLogEngineInvalid

    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.audit_engine = "ReplacingMergeTree"
    with pytest.raises(AuditLogEngineInvalid) as raised:
        run(client, events=120)
    assert raised.value.code == "AUDIT_LOG_ENGINE_INVALID"
    assert not [c for c in client.commands if c.startswith(("ALTER", "CREATE", "INSERT"))]


def test_applied_by_is_added_with_an_empty_default_materialised_then_switched_to_current_user() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.has_applied_by = False
    run(client)
    steps = [c for c in client.commands if c.startswith("ALTER TABLE") and "applied_by" in c]
    assert [("DEFAULT ''" in c, "MATERIALIZE" in c, "currentUser()" in c) for c in steps] == [
        (True, False, False),
        (False, True, False),
        (False, False, True),
    ]
    client.commands.clear()
    client.has_applied_by = True
    run(client)
    assert not [c for c in client.commands if "applied_by" in c and c.startswith("ALTER")]


def test_a_backfilled_row_says_so() -> None:
    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.settings_rows = []
    run(client)
    assert {r["s"] for r in client.settings_rows} == {"backfilled"}
    lengthen = FakeClickHouse(message_events=90, audit_log=2190)
    run(lengthen, events=120)
    assert lengthen.settings_rows[-1]["s"] == "applied"


def test_table_name_must_be_a_plain_identifier() -> None:
    with pytest.raises(ValueError):
        TableSpec("x; DROP TABLE y", 5, lambda **_: "")


def test_audit_log_with_a_writable_inserted_at_is_refused() -> None:
    from tributary.archiver.errors import AuditLogEngineInvalid

    client = FakeClickHouse(message_events=90, audit_log=2190)
    client.inserted_at_kind = "DEFAULT"
    with pytest.raises(AuditLogEngineInvalid):
        run(client)
