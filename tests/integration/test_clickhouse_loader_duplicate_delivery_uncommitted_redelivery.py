"""W2-9 duplicate-delivery harness, `clickhouse` destination: proves invariant 4 for the ClickHouse
loader against `contracts/channel-spec/v0/README.md` 5.1's exact bar for wave 2 exit criterion 3 --
*"deliver the same `out` record twice -- the second as a genuine redelivery from an uncommitted offset,
not a second call in the same process -- and assert what the sink holds."*

`packages/tributary-loaders/tests/clickhouse/test_integration_clickhouse_loader.py::
test_duplicate_delivery_produces_one_row_after_final` (package-owned, connector-engineer, `a31f91a`)
calls `ClickhouseLoader.deliver(message)` twice in the same process against a real server. Its own module
docstring already names the gap this test fills: "a `ConsumerLoop`-driven genuine-redelivery harness ...
is left as a follow-up, the same way README 3.0 already tracks it as still pending for `postgresql`."
This test is that follow-up: one record, published once to `out.<dest>`, delivered through the real
`python -m tributary.loaders.clickhouse` entry point (`tests.support.processes.spawn_clickhouse_loader`)
against a **real** `testcontainers` ClickHouse server.

**The fault, and why it is genuine.** ClickHouse's own default-expression evaluation is the fault, not a
proxy or a stub: the table carries an extra column, `slow_gate Int8 DEFAULT sleepEachRow(2)`, excluded
from the loader's own explicit `INSERT INTO ... (tb_msg_id, ...)` column list (`loader.py`'s
`_insert_statement` never names it), so the server computes that column's default -- a genuine two-second
`sleepEachRow` -- for every row this loader inserts. With `timeoutMs` set below that, the loader's first
delivery attempt genuinely reaches the server, genuinely blocks past its own client-side deadline, and
`client.py`'s `send` genuinely raises (confirmed empirically -- see the note below -- always as
`TransportError`/`CONNECT_FAILED`, never as the bare `TimeoutError`/`TIMEOUT` `loader.py`'s own module
docstring says a timeout produces: `client.py` catches `TimeoutError` and re-raises it as `TransportError`
unconditionally, so `loader.py`'s `except TimeoutError:` branch immediately above `except TransportError:`
is unreachable dead code for a real timeout. Flagged for connector-engineer; this test asserts the
classification actually observed, `CONNECT_FAILED`, not the one the docstring claims). Either way the
failure is retryable, so the real `ConsumerLoop._pause_and_rewind`/`_resume_due_partitions` blocking-retry
cycle fires (`partition_paused`, `assert_fault_fired`) -- not a second in-process call. This test then
removes the slow default (`ALTER TABLE ... MODIFY COLUMN slow_gate ... DEFAULT 0`) so the SAME record,
redelivered from the SAME still-uncommitted offset, lands quickly.

**Why `FINAL`, and why a plain count is not evidence of anything.** Because the first attempt's INSERT
was never aborted server-side (only this loader's client gave up waiting for the response -- README
3.4.3's "deduplication is eventual"), it can still land in the background after the client already
reported `CONNECT_FAILED` and after the redelivered second attempt also lands: `SELECT count()` (no
`FINAL`) over this table can show more than one physical row for a while. `SELECT count() FROM t FINAL`
is what this test's invariant-4 assertion is built on, exactly as README 3.4.3 requires and exactly as
`test_duplicate_delivery_produces_one_row_after_final`'s own module docstring does.

**Broken-implementation control.** `test_a_plain_merge_tree_would_not_collapse_the_same_redelivery` is
the same shape by hand against a plain `MergeTree` (not `ReplacingMergeTree`) table -- the engine
`ClickhouseLoader.verify_idempotency_constraint` refuses to start against. It shows that `FINAL` is not,
by itself, what makes the main test's "exactly one row" true: `MergeTree` does not even accept the
keyword (a real `ILLEGAL_FINAL` from the server), so a duplicate delivery lands as two rows with no
query available to hide it behind, unlike `ReplacingMergeTree`, where `FINAL` is a real, engine-specific
behaviour, not a universal count modifier.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired, duplicate_rate
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import spawn_clickhouse_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-clickhouse-duplicate-delivery"
DESTINATION = "adt-analytics"
TABLE = "dup_events"
PARTITIONS = 3
SECRET_REF = "ch-analytics-creds"  # noqa: S105 - a channel-spec secretRef name, not a credential

#: The slow gate sleeps 2s; `DESTINATION_TIMEOUT_MS` must be comfortably below that so the first attempt
#: reliably times out client-side, and `maxAttempts` must be generous enough that the test has time to
#: run its `ALTER TABLE` before the retry budget is exhausted (module docstring).
SLOW_GATE_SLEEP_SECONDS = 2
DESTINATION_TIMEOUT_MS = 800
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 100,
    "maxDelayMs": 100,
    "maxAttempts": 30,
    "onExhaust": "halt-partition",
}


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "dup-delivery-clickhouse")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def topics(kafka: str) -> str:
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for _topic, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # pragma: no cover - already exists on a re-run
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return kafka


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container


@pytest.fixture(scope="module")
def secrets_dir(tmp_path_factory: pytest.TempPathFactory, clickhouse: ClickHouseContainer) -> str:
    """`<base_dir>/<ref>/<key>` -- `tributary.loaders.secrets.DirSecretResolver`'s convention."""
    base = tmp_path_factory.mktemp("ch-secrets")
    ref_dir = base / SECRET_REF
    ref_dir.mkdir()
    (ref_dir / "user").write_text(clickhouse.username, encoding="utf-8")
    (ref_dir / "password").write_text(clickhouse.password, encoding="utf-8")
    return str(base)


@pytest.fixture(scope="module")
def deployed_spec(topics: str, clickhouse: ClickHouseContainer) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "clickhouse",
        "with": {
            "host": clickhouse.get_container_host_ip(),
            "port": int(clickhouse.get_exposed_port(8123)),
            "database": clickhouse.dbname,
            "table": TABLE,
            "idColumn": "tb_msg_id",
            "auth": {"type": "password", "secretRef": SECRET_REF},
            "tls": {"mode": "disable"},
            "timeoutMs": DESTINATION_TIMEOUT_MS,
        },
        "idempotency": {"technique": "sink-native"},
        "retry": dict(RETRY_OVERRIDE),
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def _execute(container: ClickHouseContainer, sql: str, *, timeout: float = 30.0) -> bytes:
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(8123))
    url = f"http://{host}:{port}/?database={container.dbname}&wait_end_of_query=1"
    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST",
        headers={"X-ClickHouse-User": container.username, "X-ClickHouse-Key": container.password},
    )
    with urllib.request.urlopen(request, timeout=timeout) as resp:  # noqa: S310
        return resp.read()


def test_a_blocking_retry_redelivers_from_the_uncommitted_offset_and_final_shows_exactly_one_row(
    topics: str, clickhouse: ClickHouseContainer, secrets_dir: str, deployed_spec: dict[str, Any]
) -> None:
    _execute(
        clickhouse,
        f"CREATE TABLE {TABLE} (tb_msg_id String, patient_ref String, value Int32, "
        f"slow_gate Int8 DEFAULT sleepEachRow({SLOW_GATE_SLEEP_SECONDS})) "
        "ENGINE=ReplacingMergeTree ORDER BY tb_msg_id",
    )

    message = make_message(
        json.dumps({"patient_ref": "SYNTH-1", "value": 42}).encode(),
        key=None,
        channel=CHANNEL,
        destination=DESTINATION,
        content_type="application/json",
    )
    published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
    assert len(published_ids) == 1

    loader = spawn_clickhouse_loader(
        channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics, secrets_dir=secrets_dir
    )
    try:
        # The fault, confirmed rather than assumed: the loader must actually observe a real,
        # server-genuine delay past its own client-side deadline and enter blocking retry before a
        # later successful insert can be trusted as a genuine redelivery of the same offset. The
        # observed code is CONNECT_FAILED, not the TIMEOUT the loader's own module docstring claims a
        # timeout produces -- see this module's docstring; asserted here as what the loader actually
        # does today, not what it says it does.
        paused = loader.wait_for_event("partition_paused", timeout_s=30.0)
        assert_fault_fired(
            paused.get("error_code") == "CONNECT_FAILED" and bool(paused.get("retryable", True)),
            description=f"expected a retryable transport failure while the slow gate was armed, got {paused!r}",
        )

        # Disarm the fault: the next attempt (and only the next attempt) must actually complete inside
        # `DESTINATION_TIMEOUT_MS`.
        _execute(clickhouse, f"ALTER TABLE {TABLE} MODIFY COLUMN slow_gate Int8 DEFAULT 0")

        deadline = time.monotonic() + 60.0
        final_count = 0
        while time.monotonic() < deadline:
            final_count = int(
                _execute(
                    clickhouse,
                    f"SELECT count() FROM {TABLE} FINAL WHERE tb_msg_id = '{message.msg_id}'",  # noqa: S608
                )
                .decode()
                .strip()
            )
            if final_count >= 1:
                break
            time.sleep(0.5)
    finally:
        loader.terminate_and_wait()

    assert_fault_fired(final_count >= 1, description=f"expected the redelivered row to land, saw {final_count}")

    plain_count = int(
        _execute(clickhouse, f"SELECT count() FROM {TABLE} WHERE tb_msg_id = '{message.msg_id}'").decode().strip()  # noqa: S608
    )
    # README 3.4.3 / module docstring: a plain count can still show more than one physical row (the
    # first, abandoned-by-the-client attempt can still land in the background) -- the FINAL count is
    # this test's actual invariant-4 assertion.
    assert final_count == 1, (
        "FINAL must collapse the genuine redelivery to exactly one logical row, invariant 4 "
        f"(plain count was {plain_count})"
    )

    row = (
        _execute(
            clickhouse,
            f"SELECT patient_ref, value FROM {TABLE} FINAL WHERE tb_msg_id = '{message.msg_id}' "  # noqa: S608
            "FORMAT TabSeparated",
        )
        .decode()
        .strip()
    )
    patient_ref, value = row.split("\t")
    assert patient_ref == "SYNTH-1"
    assert int(value) == 42

    rate = duplicate_rate(published_count=1, delivered_count=plain_count)
    print(
        f"clickhouse loader genuine-redelivery duplicate rate: {rate:.1%} "
        f"({plain_count} physical row(s), {final_count} logical row(s) FINAL, for 1 published)"
    )


def test_a_plain_merge_tree_would_not_collapse_the_same_redelivery(clickhouse: ClickHouseContainer) -> None:
    """Broken-implementation control for the test above: the identical "insert, insert again with the
    same tb-msg-id" shape, by hand, against a plain `MergeTree` -- the engine
    `ClickhouseLoader.verify_idempotency_constraint` refuses to start against (README 3.4.3). `FINAL` is
    a `ReplacingMergeTree`-family behaviour, not a universal count modifier: a plain `MergeTree` does not
    even accept the keyword (a real, server-returned `ILLEGAL_FINAL`, found empirically against
    `clickhouse/clickhouse-server:24.8`, not assumed) -- so there is no query this test's main assertion
    style could even run to hide the duplicate behind. The plain count still shows both rows.
    """
    table = "plain_events_no_dedup"
    _execute(clickhouse, f"CREATE TABLE {table} (tb_msg_id String, value Int32) ENGINE=MergeTree ORDER BY tb_msg_id")
    msg_id = "broken-control-message-id"
    row_line = json.dumps({"tb_msg_id": msg_id, "value": 1})
    _execute(clickhouse, f"INSERT INTO {table} (tb_msg_id, value) FORMAT JSONEachRow\n{row_line}\n")
    _execute(clickhouse, f"INSERT INTO {table} (tb_msg_id, value) FORMAT JSONEachRow\n{row_line}\n")

    plain_count = int(
        _execute(clickhouse, f"SELECT count() FROM {table} WHERE tb_msg_id = '{msg_id}'").decode().strip()  # noqa: S608
    )
    assert plain_count == 2  # the duplicate is still there: no ON CONFLICT, no dedup, nothing absorbed it

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _execute(clickhouse, f"SELECT count() FROM {table} FINAL WHERE tb_msg_id = '{msg_id}'")  # noqa: S608
    assert exc_info.value.code == 500
    body = exc_info.value.read()
    assert b"ILLEGAL_FINAL" in body, f"expected a real ILLEGAL_FINAL from the server, got {body!r}"
