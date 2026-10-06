"""`ClickhouseLoader` against a real ClickHouse server (`testcontainers`), proving invariant 4 and
README 3.4.3 for real: a duplicate delivery of the same `tb-msg-id` produces one row once
deduplication has run (a plain `count()` is not the proof -- `FINAL` is, README 3.4.3's own words), and
a table that is not a `ReplacingMergeTree` family sorted by `idColumn` refuses to start rather than
silently duplicate.

`uv run pytest -m integration packages/tributary-loaders/tests/clickhouse`.

Matches `tests/postgresql/test_integration_postgresql_loader.py`'s shape: this is the package-owned
integration proof (both halves of README 3.4.3, against a real server); a `ConsumerLoop`-driven
genuine-redelivery-from-an-uncommitted-offset harness (the W2-9 style already built for the HTTP and
Kafka loaders) is tracked the same way `contracts/channel-spec/v0/README.md` 3.0 already tracks it as
still pending for `postgresql`.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest
from testcontainers.community.clickhouse import ClickHouseContainer
from tributary.loaders.clickhouse.client import transport_from_config
from tributary.loaders.clickhouse.config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig
from tributary.loaders.clickhouse.loader import ClickhouseLoader
from tributary.loaders.errors import IdempotencyConstraintError
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

pytestmark = pytest.mark.integration


class _FakeResolver:
    def __init__(self, user: str, password: str) -> None:
        self._user = user
        self._password = password

    def resolve_text(self, ref: str, key: str) -> str:
        return self._user if key == "user" else self._password

    def resolve_file(self, ref: str, key: str) -> Any:  # pragma: no cover - password auth only in this suite
        raise FileNotFoundError("no files needed for password auth")


def make_message(row: dict[str, Any] | None = None, *, msg_id: str | None = None) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "adt-analytics",
        },
        payload=json.dumps(row if row is not None else {"value": 1}).encode("utf-8"),
    )


def make_config(
    container: ClickHouseContainer, *, table: str = "adt_events", **overrides: object
) -> ClickhouseDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "adt-analytics",
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(8123)),
        "database": container.dbname,
        "table": table,
        "id_column": "tb_msg_id",
        "auth": ChAuthSpec("password", "unused"),
        "tls": ChTlsSpec("disable", None),
        "timeout_s": 10.0,
        "circuit_breaker": None,
        "idempotency_technique": "sink-native",
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return ClickhouseDestinationConfig(**base)  # type: ignore[arg-type]


def _execute(container: ClickHouseContainer, sql: str) -> bytes:
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(8123))
    url = f"http://{host}:{port}/?database={container.dbname}"
    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST",
        headers={"X-ClickHouse-User": container.username, "X-ClickHouse-Key": container.password},
    )
    with urllib.request.urlopen(request, timeout=10) as resp:  # noqa: S310
        return resp.read()


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container


def _loader(container: ClickHouseContainer, *, table: str = "adt_events") -> ClickhouseLoader:
    config = make_config(container, table=table)
    resolver = _FakeResolver(container.username, container.password)
    transport = transport_from_config(config, resolver)
    return ClickhouseLoader(config, transport)


def test_duplicate_delivery_produces_one_row_after_final(clickhouse: ClickHouseContainer) -> None:
    _execute(
        clickhouse,
        "CREATE TABLE dup_events (tb_msg_id String, value Int32) ENGINE=ReplacingMergeTree ORDER BY tb_msg_id",
    )
    loader = _loader(clickhouse, table="dup_events")
    loader.verify_idempotency_constraint()  # must not raise: ReplacingMergeTree sorted by tb_msg_id
    message = make_message({"value": 42})

    first = loader.deliver(message)
    second = loader.deliver(message)  # simulates a redelivery of the identical record

    assert first.success is True
    assert second.success is True

    plain_count = int(_execute(clickhouse, "SELECT count() FROM dup_events").strip())
    final_count = int(_execute(clickhouse, "SELECT count() FROM dup_events FINAL").strip())
    # A plain count can still show 2 immediately after two inserts (merges are eventual, README
    # 3.4.3) -- FINAL is the only count this loader's duplicate-delivery claim is proven against.
    assert plain_count >= 1
    assert final_count == 1


def test_two_different_messages_produce_two_rows_after_final(clickhouse: ClickHouseContainer) -> None:
    _execute(
        clickhouse,
        "CREATE TABLE two_events (tb_msg_id String, value Int32) ENGINE=ReplacingMergeTree ORDER BY tb_msg_id",
    )
    loader = _loader(clickhouse, table="two_events")
    loader.deliver(make_message({"value": 1}))
    loader.deliver(make_message({"value": 2}))
    final_count = int(_execute(clickhouse, "SELECT count() FROM two_events FINAL").strip())
    assert final_count == 2


def test_verify_idempotency_constraint_refuses_a_plain_merge_tree(clickhouse: ClickHouseContainer) -> None:
    _execute(
        clickhouse, "CREATE TABLE plain_events (tb_msg_id String, value Int32) ENGINE=MergeTree ORDER BY tb_msg_id"
    )
    loader = _loader(clickhouse, table="plain_events")
    with pytest.raises(IdempotencyConstraintError):
        loader.verify_idempotency_constraint()


def test_verify_idempotency_constraint_refuses_a_replacing_merge_tree_sorted_by_something_else(
    clickhouse: ClickHouseContainer,
) -> None:
    _execute(
        clickhouse,
        "CREATE TABLE wrong_sort (tb_msg_id String, value Int32) ENGINE=ReplacingMergeTree ORDER BY value",
    )
    loader = _loader(clickhouse, table="wrong_sort")
    with pytest.raises(IdempotencyConstraintError):
        loader.verify_idempotency_constraint()


def test_verify_idempotency_constraint_refuses_a_missing_table(clickhouse: ClickHouseContainer) -> None:
    loader = _loader(clickhouse, table="does_not_exist")
    with pytest.raises(IdempotencyConstraintError):
        loader.verify_idempotency_constraint()


def test_unknown_table_delivery_is_a_ch_60_and_not_retryable(clickhouse: ClickHouseContainer) -> None:
    """Real `X-ClickHouse-Exception-Code: 60` (`UNKNOWN_TABLE`), not a hand-picked fixture: the
    classification in `loader.py` matches what the server actually sends."""
    loader = _loader(clickhouse, table="does_not_exist_either")
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "CH_60"
    assert outcome.retryable is False


def test_row_columns_land_in_the_right_table_columns(clickhouse: ClickHouseContainer) -> None:
    _execute(
        clickhouse,
        "CREATE TABLE cols_events (tb_msg_id String, patient_ref String, value Int32) "
        "ENGINE=ReplacingMergeTree ORDER BY tb_msg_id",
    )
    loader = _loader(clickhouse, table="cols_events")
    message = make_message({"patient_ref": "SYNTH-1", "value": 99})
    outcome = loader.deliver(message)
    assert outcome.success is True
    body = _execute(
        clickhouse,
        f"SELECT patient_ref, value FROM cols_events FINAL WHERE tb_msg_id = '{message.msg_id}' FORMAT TabSeparated",  # noqa: S608
    )
    patient_ref, value = body.decode("utf-8").strip().split("\t")
    assert patient_ref == "SYNTH-1"
    assert int(value) == 99
