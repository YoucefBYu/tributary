"""Startup grant check, audit_log DDL and read rule, and the audit library gaps (contracts/audit/v0)."""

from __future__ import annotations

import pytest
from tributary.archiver.audit import ACTOR, AuditEvent, error_code_of
from tributary.archiver.errors import ArchiverGrantsExcessive, ArchiverSettingsUnsafe, PublishError, ReprocessRefused
from tributary.archiver.grants import check_audit_grants, check_session_settings, excess_on_audit_log
from tributary.archiver.schema import AUDIT_DEDUP_ORDER, audit_ddl, audit_dedup_sql, audit_tamper_sql


class Shown:
    def __init__(self, *lines: str) -> None:
        self.lines = lines

    def command(self, cmd: str, **_: object) -> str:
        assert cmd == "SHOW GRANTS"
        return "\n".join(self.lines)


def test_insert_and_select_on_audit_log_pass() -> None:
    check_audit_grants(
        Shown(
            "GRANT INSERT, SELECT ON d.audit_log TO archiver",
            "GRANT INSERT ON d.message_events TO archiver",
            "GRANT SELECT(ts) ON d.* TO archiver",
        ),
        database="d",
    )


@pytest.mark.parametrize(
    ("line", "privilege"),
    [
        ("GRANT INSERT, ALTER UPDATE ON d.audit_log TO a", "ALTER UPDATE"),
        ("GRANT ALL ON d.* TO a", "ALL"),
        ("GRANT TRUNCATE ON `d`.`audit_log` TO a", "TRUNCATE"),
        ("GRANT DROP TABLE ON *.* TO a", "DROP TABLE"),
        ("GRANT ALTER DELETE ON d.* TO a", "ALTER DELETE"),
        ("GRANT INSERT ON d.audit_log TO a WITH GRANT OPTION", "GRANT OPTION"),
        ("GRANT some_role TO a", "ROLE"),
    ],
)
def test_more_than_insert_and_select_is_refused_by_name(line: str, privilege: str) -> None:
    with pytest.raises(ArchiverGrantsExcessive) as raised:
        check_audit_grants(Shown("GRANT INSERT ON d.message_events TO a", line), database="d")
    assert raised.value.code == "ARCHIVER_GRANTS_EXCESSIVE"
    assert (raised.value.privilege, raised.value.table) == (privilege, "audit_log")


def test_privileges_on_other_tables_or_databases_do_not_count() -> None:
    assert excess_on_audit_log(["GRANT ALTER ON d.message_events TO a", "GRANT ALL ON other.* TO a"], "d") == []


@pytest.mark.parametrize("scope", ["d*.audit_log", "d.audit_*", "*.audit_log", "d.*", "*.*", "`d`.`*`"])
def test_any_wildcard_scope_counts_as_covering(scope: str) -> None:
    assert excess_on_audit_log([f"GRANT ALTER ON {scope} TO a"], "d") == ["ALTER"]


def test_column_lists_with_commas_are_split_correctly() -> None:
    assert excess_on_audit_log(["GRANT INSERT(a, b), SELECT(c) ON d.audit_log TO a"], "d") == []


def test_audit_ddl_follows_the_contract() -> None:
    sql = audit_ddl(ttl_days=2190)
    assert "ENGINE = MergeTree" in sql and "Replacing" not in sql
    assert "ORDER BY (actor, ts, audit_id)" in sql and "PARTITION BY toYYYYMM(ts)" in sql
    for column in ("audit_id         UUID", "actor_roles", "source_partition", "source_offset", "inserted_at"):
        assert column in sql
    assert "INTERVAL 2190 DAY" in sql


def test_read_rule_is_audit_id_earliest_archived_and_never_final() -> None:
    assert AUDIT_DEDUP_ORDER == "ORDER BY inserted_at, source_partition, source_offset LIMIT 1 BY audit_id"
    assert "FINAL" not in audit_dedup_sql(database="d") and AUDIT_DEDUP_ORDER in audit_dedup_sql(database="d")
    tamper = audit_tamper_sql()
    assert "GROUP BY audit_id" in tamper and "uniqExact" in tamper and "inserted_at" not in tamper


@pytest.mark.parametrize("actor", ["idp|1234", "user:synthetic-operator", "a" * 255, "https://idp.example/sub!1"])
def test_actor_accepts_the_oidc_subject_grammar(actor: str) -> None:
    assert ACTOR.fullmatch(actor)


@pytest.mark.parametrize("actor", ["", "a" * 256, "has space", "tab\t", "é"])
def test_actor_still_refuses_non_printable_or_oversized(actor: str) -> None:
    assert ACTOR.fullmatch(actor) is None


def test_audit_event_carries_an_error_code_and_it_is_the_errors_code_only() -> None:
    assert AuditEvent("message.reprocess", "failed", "a", error_code="X").error_code == "X"
    assert error_code_of(ReprocessRefused("NO_PAYLOAD_COORDINATES")) == "NO_PAYLOAD_COORDINATES"
    assert error_code_of(PublishError("PRODUCE_FAILED")) == "PRODUCE_FAILED"
    assert error_code_of(OSError("secret detail")) == "INTERNAL_ERROR"  # not a code: never the message
    assert error_code_of(type("E", (Exception,), {"code": "lower-case"})()) == "INTERNAL_ERROR"


class Settings:
    def __init__(self, value: object, readonly: object) -> None:
        self.answers = {"value": value, "readonly": readonly}

    def command(self, cmd: str, **_: object) -> object:
        return self.answers["value" if cmd.startswith("SELECT value") else "readonly"]


def test_session_settings_pass_only_when_zero_and_read_only() -> None:
    check_session_settings(Settings("0", 1))


@pytest.mark.parametrize(("value", "readonly"), [("1", 1), ("0", 0), ("1", 0), ("", ""), (None, None)])
def test_session_settings_unpinned_or_enabled_are_refused(value: object, readonly: object) -> None:
    with pytest.raises(ArchiverSettingsUnsafe) as raised:
        check_session_settings(Settings(value, readonly))
    assert (
        raised.value.code == "ARCHIVER_SETTINGS_UNSAFE" and raised.value.setting == "insert_allow_materialized_columns"
    )
