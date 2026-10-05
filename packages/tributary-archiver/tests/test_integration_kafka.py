"""The archiver against real Kafka and real ClickHouse (own containers), and invariant 9 with a real worker.

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_kafka.py`

1. Genuine redelivery: a first archiver stores the batch but its commit is refused, so the SAME records
   come back from the uncommitted offset to a second archiver on the same group. Plain `count()` can show
   both; `FINAL` shows each event once.
2. Real worker events parse: a real `PipelineWorker` process emits real lifecycle events; none is rejected.
3. Invariant 9: the real subprocess `python -m tributary.archiver` is run, then killed, and a worker keeps
   moving `raw` to `out` at the same rate with the archiver gone. A control with the archiver STOPPED
   (SIGSTOP, group alive but not consuming) shows the produce path is never blocked either.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import orjson
import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.core.container import DockerContainer
from tributary.archiver.archiver import Archiver, ArchiverSettings
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event as base
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import await_broker, drain_topic, free_port, publish_messages
from tests.support.processes import ManagedProcess, spawn_pipeline_worker

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "archive-probe"
DESTINATION = "http-stub"
PARTITIONS = 3
SECRET_REF = "ch-archive-creds"  # noqa: S105 - a secretRef name, not a credential
BATCH = 300


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
        .with_env("CLUSTER_ID", "archiver-it")
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
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container


def make_topics(bootstrap: str, names: list[tuple[str, int]], *, compact: str | None = None) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = [NewTopic(n, p, 1, config={"cleanup.policy": "compact"} if n == compact else {}) for n, p in names]
    for future in admin.create_topics(wanted).values():
        try:
            future.result(timeout=30)
        except Exception as exc:  # re-run
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def target(ch: ClickHouseContainer, database: str) -> ClickhouseTarget:
    return ClickhouseTarget(
        host=ch.get_container_host_ip(),
        port=int(ch.get_exposed_port(8123)),
        database=database,
        username=ch.username,
        password=ch.password,
        secure=False,
    )


def fresh_db(ch: ClickHouseContainer) -> tuple[Any, str]:
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = connect(target(ch, ch.dbname))
    admin.command(f"CREATE DATABASE {name}")
    admin.close()
    client = connect(target(ch, name))
    ensure_schema(client, database=name)
    return client, name


def recent(offset_s: float = 0.0) -> str:
    from datetime import UTC, datetime, timedelta

    m = datetime.now(UTC) - timedelta(minutes=2) + timedelta(seconds=offset_s)
    return m.strftime("%Y-%m-%dT%H:%M:%S.") + f"{m.microsecond // 1000:03d}Z"


class NoCommit:
    """A real consumer whose commit is refused: the records stay uncommitted, so they are redelivered."""

    def __init__(self, inner: Consumer) -> None:
        self._inner = inner

    def commit(self, *, offsets: Any, asynchronous: bool) -> Any:
        raise KafkaException(KafkaException(None).args[0] if False else _transport_error())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _transport_error() -> Any:
    from confluent_kafka import KafkaError

    return KafkaError(KafkaError._TRANSPORT)


def make_consumer(bootstrap: str, group: str, instance: str) -> Consumer:
    config = consumer_config(
        KafkaSettings(bootstrap_servers=bootstrap), load_topic_defaults(), group_id=group, group_instance_id=instance
    )
    consumer = Consumer(config)
    consumer.subscribe([events_topic(ENV)])
    return consumer


def run_until(archiver: Archiver, predicate: Any, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        archiver.run_once()
        if predicate():
            return
    raise AssertionError("archiver did not reach the expected state in time")


def test_redelivery_from_an_uncommitted_offset_counts_once_under_final(
    kafka: str, clickhouse: ClickHouseContainer
) -> None:
    make_topics(kafka, [(events_topic(ENV), 1)])
    client, _ = fresh_db(clickhouse)
    sink = ClickhouseSink(client)
    # 20 distinct events, plus one worker-restart style re-emit of the first with a later ts.
    events = [base(ts=recent(i / 10), message_id=str(uuid.uuid4())) for i in range(20)]
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    for event in [*events, {**events[0], "ts": recent(30)}]:
        producer.produce(events_topic(ENV), key=event["message_id"].encode(), value=orjson.dumps(event))
    assert producer.flush(30) == 0
    group = f"tb.{ENV}.archiver"
    settings = ArchiverSettings(flush_interval_s=0.5)
    log = JsonLogger("archiver-it")

    first_consumer = make_consumer(kafka, group, "a-1")
    first = Archiver(NoCommit(first_consumer), sink, log=log, settings=settings)  # type: ignore[arg-type]
    run_until(first, lambda: first.metrics.rows_written >= 21)
    assert first.metrics.commit_failures >= 1
    first_consumer.close()

    second_consumer = make_consumer(kafka, group, "a-2")
    second = Archiver(second_consumer, sink, log=log, settings=settings)
    run_until(second, lambda: second.metrics.rows_written >= 21)  # the same 21 records, again
    second_consumer.close()

    plain = int(client.command("SELECT count() FROM message_events"))
    final = int(client.command("SELECT count() FROM message_events FINAL"))
    assert plain >= 20
    assert final == 20, f"42 deliveries of 20 distinct events must be 20 rows under FINAL (plain count {plain})"


def archiver_env(kafka: str, ch: ClickHouseContainer, database: str, secrets: Path) -> dict[str, str]:
    env = engine_kafka_env(kafka)
    env.update(
        {
            "TB_ARCHIVE_CLICKHOUSE_HOST": ch.get_container_host_ip(),
            "TB_ARCHIVE_CLICKHOUSE_PORT": str(ch.get_exposed_port(8123)),
            "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
            "TB_ARCHIVE_CLICKHOUSE_DATABASE": database,
            "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": SECRET_REF,
            "TB_SECRETS_DIR": str(secrets),
        }
    )
    return env


def spawn_archiver(env: dict[str, str], instance: str) -> ManagedProcess:
    proc = ManagedProcess(
        [
            sys.executable,
            "-m",
            "tributary.archiver",
            "--env",
            ENV,
            "--instance-id",
            instance,
            "--flush-interval-s",
            "0.5",
        ],
        env=env,
    )
    proc.wait_for_event("archiver_started", timeout_s=60.0)
    return proc


def wait_count(ch: ClickHouseContainer, database: str, sql: str, at_least: int, timeout_s: float = 60.0) -> int:
    client = connect(target(ch, database))
    deadline = time.monotonic() + timeout_s
    value = 0
    try:
        while time.monotonic() < deadline:
            value = int(client.command(sql))
            if value >= at_least:
                return value
            time.sleep(0.5)
    finally:
        client.close()
    return value


def push_raw(kafka: str, count: int) -> list[str]:
    messages = [make_message(b'{"synthetic": true}', key=f"SYNTH-{i}".encode(), channel=CHANNEL) for i in range(count)]
    return publish_messages(kafka, raw_topic(ENV, CHANNEL), messages)


def seconds_to_out(kafka: str, total_expected: int) -> float:
    """Wall time until `out` holds `total_expected` records, polled by high watermark."""
    consumer = Consumer({"bootstrap.servers": kafka, "group.id": f"wm-{uuid.uuid4()}"})
    start = time.monotonic()
    try:
        while time.monotonic() - start < 120:
            from confluent_kafka import TopicPartition

            total = 0
            for p in range(PARTITIONS):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(out_topic(ENV, CHANNEL, DESTINATION), p), timeout=5
                )
                total += high - low
            if total >= total_expected:
                return time.monotonic() - start
            time.sleep(0.05)
    finally:
        consumer.close()
    raise AssertionError("worker did not reach the expected out count")


def test_archiver_stopped_leaves_the_worker_unblocked_and_real_events_parse(
    kafka: str, clickhouse: ClickHouseContainer, tmp_path: Path
) -> None:
    make_topics(
        kafka,
        [
            (raw_topic(ENV, CHANNEL), PARTITIONS),
            (out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS),
            (dlq_topic(ENV, CHANNEL), 1),
            (events_topic(ENV), 1),
            (channels_topic(ENV), 1),
        ],
        compact=channels_topic(ENV),
    )
    ref = tmp_path / SECRET_REF
    ref.mkdir()
    (ref / "user").write_text(clickhouse.username)
    (ref / "password").write_text(clickhouse.password)
    for f in ref.iterdir():
        f.chmod(0o600)
    client, database = fresh_db(clickhouse)
    client.close()
    spec = walking_skeleton_document(
        mllp_port=free_port(), http_url="http://127.0.0.1:1/unused", channel=CHANNEL, destination=DESTINATION
    )
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(spec).encode())
    assert producer.flush(30) == 0

    env = archiver_env(kafka, clickhouse, database, tmp_path)
    archiver = spawn_archiver(env, "archiver-live")
    worker = spawn_pipeline_worker(env=ENV, channel=CHANNEL, bootstrap=kafka, instance_id="worker-1", timeout_s=60.0)
    try:
        # Phase A: archiver consuming.
        push_raw(kafka, BATCH)
        with_archiver_s = seconds_to_out(kafka, BATCH)
        stored = wait_count(
            clickhouse, database, "SELECT count() FROM message_events FINAL WHERE kind = 'message'", BATCH
        )
        assert stored >= BATCH, "real worker events must land in ClickHouse (and parse)"
        rejected = [line for line in archiver.collect_pending() if line.get("event") == "event_rejected"]
        assert rejected == [], "a real worker event was rejected by the archiver"

        # Phase B: archiver SIGSTOPped (process alive, group member, not consuming) then killed.
        os.killpg(archiver.process.pid, signal.SIGSTOP)
        push_raw(kafka, BATCH)
        stopped_s = seconds_to_out(kafka, 2 * BATCH)
        os.killpg(archiver.process.pid, signal.SIGCONT)
        archiver.kill_and_wait()
        push_raw(kafka, BATCH)
        killed_s = seconds_to_out(kafka, 3 * BATCH)

        # Nothing was dropped on the worker side while the archive was away.
        out = drain_topic(kafka, out_topic(ENV, CHANNEL, DESTINATION), partitions=PARTITIONS, expected=3 * BATCH)
        assert len({bytes(dict(r.headers())["tb-msg-id"]) for r in out}) == 3 * BATCH
        worker_lines = worker.collect_pending()
        assert not [x for x in worker_lines if x.get("event") in ("event_dropped", "event_emit_failed")]
        print(
            f"worker {BATCH} msgs: archiver up {with_archiver_s:.2f}s, stopped {stopped_s:.2f}s, killed {killed_s:.2f}s"
        )
        # Not blocked: generous bound against noise, tight enough to catch a produce that waits on the archive.
        assert stopped_s < max(10.0, 5 * with_archiver_s)
        assert killed_s < max(10.0, 5 * with_archiver_s)

        # Restart: the backlog the archiver missed is archived from Kafka (invariant 9, restartable).
        archiver = spawn_archiver(env, "archiver-restarted")
        grown = wait_count(
            clickhouse,
            database,
            "SELECT uniqExact(message_id) FROM message_events FINAL WHERE kind = 'message'",
            3 * BATCH,
        )
        assert grown >= 3 * BATCH, "messages processed while the archive was away must be archived from Kafka"
    finally:
        worker.terminate_and_wait()
        archiver.terminate_and_wait()
