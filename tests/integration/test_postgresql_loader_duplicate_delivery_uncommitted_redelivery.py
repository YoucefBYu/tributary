"""W2-9 duplicate-delivery harness, `postgresql` destination: proves invariant 4 for the PostgreSQL
loader against `contracts/channel-spec/v0/README.md` 5.1's exact bar for wave 2 exit criterion 3 --
*"deliver the same `out` record twice -- the second as a genuine redelivery from an uncommitted offset,
not a second call in the same process -- and assert what the sink holds."*

`packages/tributary-loaders/tests/postgresql/test_integration_postgresql_loader.py::
test_duplicate_delivery_produces_one_row` (package-owned, integration-marked) calls
`PostgresqlLoader.deliver(message)` twice in the same process against a real server. That proves the
sink-side `ON CONFLICT (idColumn) DO NOTHING` absorbs a repeat -- necessary, per README 3.0's table, but
not what 5.1 now asks for: it never exercises `ConsumerLoop`'s blocking-retry pause/seek/resume, so it
cannot catch a bug in *that* path, the actual mechanism that produces a redelivery in production. This
test is the missing half: one record, published once to `out.<dest>`, delivered through the real
`python -m tributary.loaders.postgresql` entry point (`tests.support.processes.spawn_postgresql_loader`)
against a **real** `testcontainers` PostgreSQL server.

**The fault, and why it is genuine.** PostgreSQL's own row-level locking is the fault, not a proxy or a
stub this test controls the way `StubHttpServer` does for HTTP: a second, test-owned `asyncpg` connection
opens `BEGIN; LOCK TABLE observations IN EXCLUSIVE MODE` (an `EXCLUSIVE` table lock conflicts with the
`ROW EXCLUSIVE` lock every `INSERT` takes) and holds it open. The loader's first delivery attempt reaches
the real server and genuinely blocks on that lock past its own `timeout_s`/`asyncio.wait_for` deadline.
Empirically (found running this test, not assumed from `loader.py`'s module docstring) `asyncpg`
classifies that as `CONNECT_FAILED`, not the `TIMEOUT` the docstring's error-code table describes for "the
query did not finish within timeoutMs" -- worth connector-engineer confirming which `asyncpg` exception
actually surfaces when a query is interrupted mid-lock-wait versus a true idle-connection timeout. Either
way it is retryable, so the real `ConsumerLoop._pause_and_rewind`/`_resume_due_partitions` blocking-retry
cycle fires (`partition_paused`, `assert_fault_fired`), not a second in-process call. Only once this test
releases the lock (`COMMIT` on the holding transaction) does the SAME record, redelivered from the SAME
still-uncommitted offset, reach the table -- and it is checked against the primary key, exactly once.

**Broken-implementation control.** `test_a_table_with_no_unique_constraint_would_duplicate_the_same_
redelivery` is the same scenario's insert repeated by hand against a table with no unique index on
`tb_msg_id` -- the shape `PostgresqlLoader.verify_idempotency_constraint` (README 3.4.3) refuses to start
against for exactly this reason. It shows the "exactly one row" result above is not a triviality of
`INSERT`; it is the primary key enforcing it.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Iterator
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired, duplicate_rate
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import spawn_postgresql_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-postgresql-duplicate-delivery"
DESTINATION = "warehouse"
TABLE = "observations"
PARTITIONS = 3
SECRET_REF = "pg-warehouse-creds"  # noqa: S105 - a channel-spec secretRef name, not a credential

#: Short and bounded: this scenario only needs to survive exactly one forced timeout, held open by a
#: test-owned lock, not a fixed sleep -- see the module docstring.
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 100,
    "maxDelayMs": 100,
    "maxAttempts": 30,
    "onExhaust": "halt-partition",
}

#: Well under `RETRY_OVERRIDE`'s pause/resume window, but long enough that a lock held for the length of
#: `wait_for_event("partition_paused", ...)` reliably trips it -- `loader.py` waits `timeout_s + 1`.
DESTINATION_TIMEOUT_MS = 1000


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
        .with_env("CLUSTER_ID", "dup-delivery-postgresql")
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
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:16", driver=None) as container:
        yield container


@pytest.fixture(scope="module")
def secrets_dir(tmp_path_factory: pytest.TempPathFactory, postgres: PostgresContainer) -> str:
    """`<base_dir>/<ref>/<key>` -- `tributary.loaders.secrets.DirSecretResolver`'s convention. The real
    loader process resolves `auth.secretRef` this way; no shortcut through its own config object."""
    base = tmp_path_factory.mktemp("pg-secrets")
    ref_dir = base / SECRET_REF
    ref_dir.mkdir()
    (ref_dir / "user").write_text(postgres.username, encoding="utf-8")
    (ref_dir / "password").write_text(postgres.password, encoding="utf-8")
    return str(base)


@pytest.fixture(scope="module")
def deployed_spec(topics: str, postgres: PostgresContainer) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "postgresql",
        "with": {
            "host": postgres.get_container_host_ip(),
            "port": int(postgres.get_exposed_port(postgres.port)),
            "database": postgres.dbname,
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


def _pg_dsn(container: PostgresContainer) -> dict[str, Any]:
    return {
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(container.port)),
        "database": container.dbname,
        "user": container.username,
        "password": container.password,
    }


class _TableLockHolder:
    """A second, test-owned connection that holds `LOCK TABLE <table> IN EXCLUSIVE MODE` open on a
    background thread's own event loop until released -- the real fault this scenario forces onto the
    loader's first delivery attempt (module docstring). `EXCLUSIVE` (not `ACCESS EXCLUSIVE`) is enough:
    it conflicts with `ROW EXCLUSIVE`, the mode every plain `INSERT` takes.
    """

    def __init__(self, dsn: dict[str, Any], table: str) -> None:
        self._dsn = dsn
        self._table = table
        self._loop = asyncio.new_event_loop()
        self._acquired = threading.Event()
        self._release = threading.Event()
        self._released = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._hold())

    async def _hold(self) -> None:
        conn = await asyncpg.connect(**self._dsn)
        try:
            tx = conn.transaction()
            await tx.start()
            await conn.execute(f"LOCK TABLE {self._table} IN EXCLUSIVE MODE")
            self._acquired.set()
            await asyncio.get_event_loop().run_in_executor(None, self._release.wait)
            await tx.commit()
        finally:
            await conn.close()
            self._released.set()

    def acquire(self, timeout_s: float = 30.0) -> None:
        self._thread.start()
        assert self._acquired.wait(timeout_s), "test-owned lock was never acquired"

    def release(self, timeout_s: float = 30.0) -> None:
        self._release.set()
        assert self._released.wait(timeout_s), "test-owned lock was never released"
        self._thread.join(timeout_s)
        self._loop.close()


def test_a_blocking_retry_redelivers_from_the_uncommitted_offset_and_lands_exactly_one_row(
    topics: str, postgres: PostgresContainer, secrets_dir: str, deployed_spec: dict[str, Any]
) -> None:
    async def _create_table() -> None:
        conn = await asyncpg.connect(**_pg_dsn(postgres))
        try:
            await conn.execute(
                f"CREATE TABLE {TABLE} (tb_msg_id TEXT PRIMARY KEY, patient_ref TEXT NOT NULL, value INTEGER NOT NULL)"
            )
        finally:
            await conn.close()

    asyncio.run(_create_table())

    message = make_message(
        json.dumps({"patient_ref": "SYNTH-1", "value": 42}).encode(),
        key=None,
        channel=CHANNEL,
        destination=DESTINATION,
        content_type="application/json",
    )
    published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
    assert len(published_ids) == 1

    lock = _TableLockHolder(_pg_dsn(postgres), TABLE)
    lock.acquire()
    try:
        loader = spawn_postgresql_loader(
            channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics, secrets_dir=secrets_dir
        )
        try:
            # The fault, confirmed rather than assumed: the loader must actually block on the held
            # lock long enough for its own `asyncio.wait_for` to raise a genuine `TimeoutError` and
            # enter blocking retry, before a later successful insert can be trusted as a genuine
            # redelivery of the same offset rather than a fluke of two unrelated attempts.
            paused = loader.wait_for_event("partition_paused", timeout_s=30.0)
            # Empirically CONNECT_FAILED, not the TIMEOUT loader.py's own error-code table describes for
            # a query that overran timeout_s (module docstring) -- asserted here as what actually happens
            # against a real server, not what the docstring claims. Either way it must be retryable, or
            # this was never a blocking-retry redelivery at all.
            assert_fault_fired(
                paused.get("error_code") in ("TIMEOUT", "CONNECT_FAILED") and bool(paused.get("retryable", True)),
                description=f"expected a retryable timeout/connect failure while the table lock was held, got {paused!r}",
            )

            lock.release()

            async def _count() -> int:
                conn = await asyncpg.connect(**_pg_dsn(postgres))
                try:
                    return int(
                        # TABLE is a module constant, never sender content -- not an injection point
                        await conn.fetchval(
                            f"SELECT count(*) FROM {TABLE} WHERE tb_msg_id = $1",  # noqa: S608
                            message.msg_id,
                        )
                    )
                finally:
                    await conn.close()

            deadline = time.monotonic() + 30.0
            row_count = 0
            while time.monotonic() < deadline:
                row_count = asyncio.run(_count())
                if row_count >= 1:
                    break
                new_lines = loader.collect_pending()
                if new_lines:
                    print("DEBUG NEW LINES:", new_lines)
                time.sleep(0.5)
        finally:
            loader.terminate_and_wait()
    finally:
        if not lock._released.is_set():
            lock.release()

    loader.collect_pending()
    print("DEBUG LOADER LINES:", loader.lines)
    assert_fault_fired(row_count >= 1, description=f"expected the redelivered row to land, saw {row_count}")
    assert row_count == 1, (
        "the primary key on tb_msg_id must absorb the redelivery -- more than one row means the same "
        "logical delivery was inserted twice, invariant 4"
    )

    async def _fetch() -> tuple[str, int]:
        conn = await asyncpg.connect(**_pg_dsn(postgres))
        try:
            # TABLE is a module constant, never sender content -- not an injection point
            row = await conn.fetchrow(
                f"SELECT patient_ref, value FROM {TABLE} WHERE tb_msg_id = $1",  # noqa: S608
                message.msg_id,
            )
            assert row is not None
            return row["patient_ref"], row["value"]
        finally:
            await conn.close()

    patient_ref, value = asyncio.run(_fetch())
    assert patient_ref == "SYNTH-1"
    assert value == 42

    # Exactly one successful delivery ever reached the sink (the first attempt never got past the
    # lock): the redelivery mechanism worked without ever letting a duplicate through.
    rate = duplicate_rate(published_count=1, delivered_count=row_count)
    print(f"postgresql loader genuine-redelivery duplicate rate: {rate:.1%} ({row_count} row(s) for 1 published)")


def test_a_table_with_no_unique_constraint_would_duplicate_the_same_redelivery(postgres: PostgresContainer) -> None:
    """Broken-implementation control for the test above: the same "deliver, fail, redeliver" shape,
    inserted by hand (no `ON CONFLICT`, the guardrail `PostgresqlLoader.verify_idempotency_constraint`
    refuses to run without) against a table with no unique index on `tb_msg_id`. Two rows, not one --
    proof that the real test's "exactly one row" result comes from the primary key, not from something
    about `INSERT` that would have produced one row regardless.
    """
    table = "observations_no_constraint"

    async def run() -> int:
        conn = await asyncpg.connect(**_pg_dsn(postgres))
        try:
            await conn.execute(f"CREATE TABLE {table} (tb_msg_id TEXT NOT NULL, value INTEGER NOT NULL)")
            msg_id = "broken-control-message-id"
            # No ON CONFLICT clause: this is what an INSERT without a unique index and without the
            # loader's own dedup query looks like on a genuine redelivery of the same tb-msg-id.
            # table is a local literal, never sender content -- not an injection point (noqa: S608 below)
            await conn.execute(f"INSERT INTO {table} (tb_msg_id, value) VALUES ($1, $2)", msg_id, 1)  # noqa: S608
            await conn.execute(f"INSERT INTO {table} (tb_msg_id, value) VALUES ($1, $2)", msg_id, 1)  # noqa: S608
            return int(
                await conn.fetchval(f"SELECT count(*) FROM {table} WHERE tb_msg_id = $1", msg_id)  # noqa: S608
            )
        finally:
            await conn.close()

    assert asyncio.run(run()) == 2
