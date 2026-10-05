"""`_tb.<env>.channels` through a real broker outage: the cache says it may be stale and keeps serving.

The unit tests (`test_channels.py`) script `list_topics`; this one stops and restarts a real Kafka 4.0
broker under a real `confluent_kafka.Consumer` built by the runtime's own `consumer_config`, because the
behaviour it relies on is librdkafka's and was found by measurement, not documentation: during an outage
`poll()` returns no error event at all, and after one it returns nothing either (module docstring of
`tributary.runtime.channels`). If a librdkafka upgrade changes that, this is the test that notices.

**How long recovery may take, and why the test measures it from readiness, not from `docker start`.**
After the broker is back, the cache recovers on the first probe that runs once the consumer has
reconnected. librdkafka reconnects on its own backoff, which doubles up to `reconnect.backoff.max.ms`
(10 s by default) with -25%..+50% jitter, and a probe sends no reconnect of its own: it fails with
`_TRANSPORT` until the backoff expires. Measured on 2026-10-05 (four cores, 0-16 busy processes, live
Docker contention, clean and SIGKILL stops): the broker took 14.9-19.9 s to serve the topic again, and
the cache recovered 0-7.8 s after that. Until then the test gave both halves one 90 s window starting
at `docker start`, so time Docker and the JVM spent restarting counted against the cache. It failed
once with "never recovered" under contention, in a run where no client logged a disconnect at all.
That is a Docker-level event, not slow recovery after a normal stop. Now the restart has its own
environment budget and its own failure message, and the cache's recovery is held to a bound derived
from the consumer's backoff and the probe cadence (`recovery_bound_s`).

`uv run pytest -m integration packages/tributary-runtime/tests/test_integration_channels_outage.py`
"""

from __future__ import annotations

import io
import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.channels import LiveSpecSource, open_spec_source
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.errors import ChannelsUnavailableError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import channels_topic

from tests.support.kafka_helpers import await_broker, create_topics_with_retry, free_port

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough"
PROBE_INTERVAL_S = 1.0
PROBE_TIMEOUT_S = 0.5
POLL_S = 0.1
#: How long one readiness check by a fresh client may block the loop, so also how late recovery can be seen.
READY_CHECK_S = 1.0
#: librdkafka's default `reconnect.backoff.max.ms` (CONFIGURATION.md), used when the config sets none.
LIBRDKAFKA_RECONNECT_BACKOFF_MAX_MS = 10_000
#: An environment budget, not a property of the cache: the time Docker and the JVM may take to serve the
#: topic again after `docker start`. The same budget the fixture gives the cold start (`await_broker`).
RESTART_BUDGET_S = 90.0
EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "channel-spec"
    / "v0"
    / "examples"
    / "valid"
    / "walking-skeleton-loopback.json"
)


@pytest.fixture(scope="module")
def broker() -> Iterator[tuple[DockerContainer, str]]:
    """Single-node KRaft, lite profile. A fixed host port, so the restarted broker is at the same address."""
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
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield container, bootstrap
    finally:
        container.stop()


def publish_spec(bootstrap: str, *, version: int) -> None:
    document = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "name": CHANNEL, "version": version}
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0


def poll_until(source: LiveSpecSource, *, raising: bool, timeout_s: float) -> float:
    """Poll the cache like the MLLP receiver's feed does until it does (or stops) raising. Returns seconds."""
    started = time.monotonic()
    while time.monotonic() - started < timeout_s:
        try:
            source.cache.poll_once(timeout_s=0.1)
        except ChannelsUnavailableError:
            if raising:
                return time.monotonic() - started
        else:
            if not raising:
                return time.monotonic() - started
    raise AssertionError(f"cache {'never reported' if raising else 'never recovered from'} the outage")


def recovery_bound_s(config: dict[str, Any]) -> float:
    """The longest the cache may stay unavailable once the broker serves the topic again.

    * the consumer's next reconnect is at most one backoff away: the cap times 1.5, librdkafka's upper
      jitter, which is the worst case whether or not the cap is applied after the jitter;
    * then one handshake round trip, given the probe's own timeout;
    * the probe that proves recovery may have just run, so up to one `probe_interval_s` until the next,
      which itself may take up to `probe_timeout_s`;
    * plus the loop's own blind spots: one poll timeout and one readiness check.
    """
    backoff_max_s = int(config.get("reconnect.backoff.max.ms", LIBRDKAFKA_RECONNECT_BACKOFF_MAX_MS)) / 1000
    return backoff_max_s * 1.5 + PROBE_TIMEOUT_S + PROBE_INTERVAL_S + PROBE_TIMEOUT_S + POLL_S + READY_CHECK_S


def topic_served(bootstrap: str) -> bool:
    """A fresh client (no reconnect backoff of its own) is served this topic with a leader."""
    try:
        metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(channels_topic(ENV), timeout=READY_CHECK_S)
    except Exception:  # the broker is not serving yet
        return False
    topic = metadata.topics.get(channels_topic(ENV))
    return topic is not None and topic.error is None and all(p.leader >= 0 for p in topic.partitions.values())


def restart_and_await_recovery(
    container: DockerContainer, bootstrap: str, source: LiveSpecSource, *, bound_s: float
) -> tuple[float, float]:
    """Start the broker, poll the cache as a receiver does, and time both halves separately.

    Returns `(restart_s, lag_s)`: seconds from `docker start` until a fresh client is served the topic,
    and seconds from then until the cache recovers (negative if the cache saw it first).
    """
    container.get_wrapped_container().start()
    started = time.monotonic()
    ready_at: float | None = None
    recovered_at: float | None = None
    next_check = started
    while ready_at is None or recovered_at is None:
        now = time.monotonic()
        if ready_at is None:
            if now - started > RESTART_BUDGET_S:
                raise AssertionError(
                    f"environment, not the cache: the restarted broker did not serve the topic within "
                    f"{RESTART_BUDGET_S:.0f}s of docker start (cache recovered: {recovered_at is not None})"
                )
            if now >= next_check:
                next_check = now + READY_CHECK_S
                if topic_served(bootstrap):
                    ready_at = time.monotonic()
        elif recovered_at is None and now - ready_at > bound_s:
            outage = source.cache.outage
            raise AssertionError(
                f"cache did not recover within {bound_s:.1f}s of the broker serving the topic again "
                f"(restart took {ready_at - started:.1f}s; still {outage.error_code if outage else 'unknown'})"
            )
        if recovered_at is None:
            try:
                source.cache.poll_once(timeout_s=POLL_S)
            except ChannelsUnavailableError:
                continue
            recovered_at = time.monotonic()
    return ready_at - started, recovered_at - ready_at


def logged(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    return [line for line in map(json.loads, stream.getvalue().splitlines()) if line["event"] == event]


def test_a_broker_outage_is_reported_and_the_last_spec_keeps_serving(broker: tuple[DockerContainer, str]) -> None:
    container, bootstrap = broker
    create_topics_with_retry(bootstrap, [NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"})])
    publish_spec(bootstrap, version=1)
    stream = io.StringIO()
    config = consumer_config(
        KafkaSettings(bootstrap_servers=bootstrap),
        load_topic_defaults(),
        group_id="tb.dev.spec-reader.outage-test",
        enable_partition_eof=True,
    )
    consumer: Any = Consumer(config)
    source = open_spec_source(
        channel=CHANNEL,
        consumer=consumer,
        topic=channels_topic(ENV),
        log=JsonLogger("test", stream=stream),
        probe_interval_s=PROBE_INTERVAL_S,
        probe_timeout_s=PROBE_TIMEOUT_S,
    )
    try:
        assert source.current().version == 1 and source.available

        container.get_wrapped_container().stop(timeout=5)
        detected_after_s = poll_until(source, raising=True, timeout_s=20.0)
        assert source.available is False
        assert source.current().version == 1, "invariant 3: the applied spec is served through the outage"
        assert len(logged(stream, "channels_unavailable")) == 1

        restart_s, lag_s = restart_and_await_recovery(container, bootstrap, source, bound_s=recovery_bound_s(config))
        assert source.available is True
        assert len(logged(stream, "channels_recovered")) == 1

        # Hot reload works again after the outage, on the same consumer.
        await_broker(bootstrap)
        publish_spec(bootstrap, version=2)
        deadline = time.monotonic() + 30.0
        while source.current().version != 2:
            assert time.monotonic() < deadline, "the republished spec never arrived after recovery"
            time.sleep(0.1)
        print(
            f"outage detected after {detected_after_s:.1f}s; broker served again {restart_s:.1f}s after start; "
            f"cache recovered {lag_s:.1f}s after that (bound {recovery_bound_s(config):.1f}s)"
        )
    finally:
        source.close()
