"""W3-23 remainder: the platform Kafka roles `lag_reader` and `api` (and the `reprocessor` set) under their exact
contract grants on the deny-by-default SASL_SSL broker (`tests/support/secure_broker.py`: `StandardAuthorizer`,
`allow.everyone.if.no.acl.found=false`).

Grants are parsed from `contracts/topics/v0/topic-defaults.json` `security.acls.<role>` through the lite stack's
own parser (`contract_grants`), never written by hand here, so a contract change that breaks a role fails here.
Both roles were marked UNVERIFIED in the contract until this test.

* `lag_reader` (KEDA, the lag exporter): DESCRIBE only. Positive: list topic metadata, read the end offsets of a
  channel's `raw` and the committed offset of a worker group; the broker's authorizer log holds no denial for the
  principal. Negative: READ on the same `raw` (a consumer that assigns it) and WRITE (a produce to it) are refused
  with `TOPIC_AUTHORIZATION_FAILED`, the broker's answer and not a timeout, and each refusal is in the
  authorizer log. The role can describe and read offsets, never payload.
* `api` (control plane): READ and WRITE `_tb.<env>.channels`, WRITE `_tb.<env>.audit`, cluster IDEMPOTENT_WRITE,
  DESCRIBE on group `tb.<env>.reconciler`. Positive: the reconciler's read (assign from the beginning under the
  reconciler group id, no commit) returns the seeded spec, an idempotent `acks=all` publish to `channels` and to
  `audit` is delivered, and the authorizer log has no denial for the principal so far. Negative: READ on `audit`
  and READ and WRITE on an `out` topic are refused and logged.
* `reprocessor` (the Kafka principal behind reprocess and replay): reads and publishes only on its own channel's `raw`
  (assign, DESCRIBE on its LITERAL group); every other read and write, group membership and commits are refused, and a
  widened principal passes the same probes (control).
* `reprocessor`, library side: the archiver's deny-by-default test
  (`packages/tributary-archiver/tests/test_integration_reprocessor_acls.py`) runs reprocess and replay under a hand
  written grant set; `test_reprocessor_contract_grants_are_the_set_the_archiver_test_runs_under` pins that the
  contract parses to exactly that set, so the archiver test is a test of the contract's role.

Synthetic ids only; no payload is logged.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from tributary.runtime.clients import consumer_config, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, platform_topic, raw_topic

from tests.support.secure_broker import Grant, SecureBroker, contract_grants, secure_broker

pytestmark = pytest.mark.integration

ENV = "plat"
CHANNEL = "plat-adt"
DESTINATION = "stub"
SEEDED = 5
COMMITTED = 3
AUTH = KafkaError.TOPIC_AUTHORIZATION_FAILED
RAW = raw_topic(ENV, CHANNEL)
OUT = out_topic(ENV, CHANNEL, DESTINATION)
OUT_WRITE = out_topic(ENV, CHANNEL, "stub-w")  # a topic only the WRITE negative touches (no earlier denial on it)
SENTINEL = "tb-plat-sentinel"
DLQ = dlq_topic(ENV, CHANNEL)
OTHER_RAW = raw_topic(ENV, "other-adt")  # another channel's raw: not the reprocessor's to touch
EVENTS = events_topic(ENV)
REPROCESSOR_GROUP = f"tb.{ENV}.reprocessor"
CHANNELS = channels_topic(ENV)
AUDIT = platform_topic(ENV, "audit")
WORKER_GROUP = f"tb.{ENV}.{CHANNEL}.worker"
RECONCILER_GROUP = f"tb.{ENV}.reconciler"


@pytest.fixture(scope="module")
def broker(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SecureBroker]:
    with secure_broker(tmp_path_factory.mktemp("platform-acl-broker")) as running:
        running.create_topics(
            [
                NewTopic(CHANNELS, 1, 1, config={"cleanup.policy": "compact"}),
                NewTopic(AUDIT, 1, 1),
                NewTopic(events_topic(ENV), 1, 1),
                NewTopic(RAW, 1, 1),
                NewTopic(OUT, 1, 1),
                NewTopic(OUT_WRITE, 1, 1),
                NewTopic(DLQ, 1, 1),
                NewTopic(OTHER_RAW, 1, 1),
            ]
        )
        seed = Producer({"bootstrap.servers": running.admin, "acks": "all"})
        for index in range(SEEDED):
            seed.produce(RAW, key=b"SYNTH-K", value=f"synthetic-{index}".encode())
        seed.produce(CHANNELS, key=CHANNEL.encode(), value=b'{"synthetic": "spec"}')
        seed.produce(AUDIT, key=b"SYNTH-A", value=b"synthetic-audit")
        for topic in (OUT, DLQ, EVENTS, OTHER_RAW):  # one record each, so a read that is allowed returns something
            seed.produce(topic, key=b"SYNTH-K", value=b"synthetic")
        assert seed.flush(30) == 0
        # A worker group with a committed offset, written by the set-up super user.
        reader = Consumer(
            {
                "bootstrap.servers": running.admin,
                "group.id": WORKER_GROUP,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        reader.assign([TopicPartition(RAW, 0, 0)])
        taken = 0
        deadline = time.monotonic() + 30
        while taken < COMMITTED and time.monotonic() < deadline:
            if (message := reader.poll(1.0)) is not None and message.error() is None:
                taken += 1
        assert taken == COMMITTED
        reader.commit(offsets=[TopicPartition(RAW, 0, COMMITTED)], asynchronous=False)
        reader.close()
        yield running


def _settings(broker: SecureBroker, tmp_path: Path, principal: str) -> Any:
    env = broker.process_env(tmp_path, principal)
    return load_engine_kafka(
        {"TB_ENGINE_KAFKA_FILE": env["TB_ENGINE_KAFKA_FILE"], "TB_SECRETS_DIR": env["TB_SECRETS_DIR"]}
    )


def _plain_producer(broker: SecureBroker, tmp_path: Path, principal: str) -> Producer:
    """The contract profile minus idempotence: a role without cluster IDEMPOTENT_WRITE (lag_reader) is refused by the
    idempotent producer at PID acquisition (`CLUSTER_AUTHORIZATION_FAILED`) before any topic is tried; this probe
    isolates the topic-level WRITE check."""
    config = producer_config(
        _settings(broker, tmp_path, principal), load_topic_defaults(), client_id=f"{principal}-plain"
    )
    config.update({"enable.idempotence": False, "acks": 1})
    return Producer(config)


def _consumer(broker: SecureBroker, tmp_path: Path, principal: str, group: str) -> Consumer:
    config = consumer_config(
        _settings(broker, tmp_path, principal), load_topic_defaults(), group_id=group, enable_partition_eof=True
    )
    config["enable.auto.commit"] = False
    return Consumer(config)


def _producer(broker: SecureBroker, tmp_path: Path, principal: str) -> Producer:
    return Producer(
        producer_config(_settings(broker, tmp_path, principal), load_topic_defaults(), client_id=f"{principal}-probe")
    )


def _first_error(consumer: Consumer, topic: str, *, timeout_s: float = 30.0) -> KafkaError | None:
    """Assign `topic`/0 from the beginning and return the first error the broker raises (None if a record came)."""
    consumer.assign([TopicPartition(topic, 0, 0)])
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error() is not None and message.error().code() != KafkaError._PARTITION_EOF:
            return message.error()
        if message.error() is None:
            return None
    raise AssertionError(f"neither a record nor an error from {topic} within {timeout_s}s (a timeout, not a refusal)")


def _produce_outcome(producer: Producer, topic: str) -> KafkaError | None:
    outcome: list[KafkaError | None] = []
    producer.produce(topic, key=b"SYNTH-K", value=b"synthetic", on_delivery=lambda err, _msg: outcome.append(err))
    producer.flush(30)
    assert outcome, f"no delivery report for {topic} within 30s (a timeout, not a refusal)"
    return outcome[0]


def _denied(broker: SecureBroker, principal: str, operation: str, resource: str) -> bool:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if broker.denials_for(principal, operation=operation, resource=resource):
            return True
        time.sleep(0.5)
    return False


def _denied_any(broker: SecureBroker, principal: str, operations: tuple[str, ...], resource: str) -> bool:
    return any(_denied(broker, principal, operation, resource) for operation in operations)


@pytest.fixture(scope="module")
def sentinel(broker: SecureBroker) -> str:
    broker.add_principal(SENTINEL)  # no grants at all: every request it makes is denied and logged
    return SENTINEL


def _assert_no_denials(broker: SecureBroker, tmp_path: Path, sentinel: str, principal: str, why: str) -> None:
    """The authorizer log is written asynchronously, so an empty list right after an action proves little. Make the
    sentinel principal cause a fresh denial, wait until it shows (the log is ordered), then read `principal`'s."""
    before = len(broker.denials_for(sentinel))
    probe = _plain_producer(broker, tmp_path, sentinel)
    probe.produce(RAW, value=b"synthetic")
    probe.flush(10)
    deadline = time.monotonic() + 20
    while len(broker.denials_for(sentinel)) <= before:
        assert time.monotonic() < deadline, "the authorizer log did not catch up (sentinel denial never appeared)"
        time.sleep(0.5)
    assert broker.denials_for(principal) == [], why


# ---------------------------------------------------------------------------------------------------
# lag_reader


def test_lag_reader_can_describe_and_read_offsets_but_not_read_or_write_data(
    broker: SecureBroker, tmp_path: Path, sentinel: str
) -> None:
    principal = "tb-plat-lag-reader"
    grants = contract_grants("lag_reader", env=ENV, channel=CHANNEL)
    assert {g.operation for g in grants} == {"DESCRIBE"}, "the role must hold DESCRIBE and nothing else"
    broker.provision(principal, grants)

    # Positive: topic metadata, end offsets and a committed offset, with no denial recorded.
    probe = _consumer(broker, tmp_path, principal, WORKER_GROUP)
    try:
        metadata = AdminClient(_admin_conf(broker, tmp_path, principal)).list_topics(RAW, timeout=30)
        assert metadata.topics[RAW].error is None and len(metadata.topics[RAW].partitions) == 1
        low, high = probe.get_watermark_offsets(TopicPartition(RAW, 0), timeout=30, cached=False)
        assert (low, high) == (0, SEEDED)
        committed = probe.committed([TopicPartition(RAW, 0)], timeout=30)
        assert committed[0].error is None and committed[0].offset == COMMITTED
        assert high - committed[0].offset == SEEDED - COMMITTED  # the lag KEDA scales on
    finally:
        probe.close()
    _assert_no_denials(broker, tmp_path, sentinel, principal, "the lag reader needed a grant the contract withholds")

    # Negative: READ and WRITE on the data topic are refused by the broker.
    reader = _consumer(broker, tmp_path, principal, WORKER_GROUP)
    try:
        error = _first_error(reader, RAW)
    finally:
        reader.close()
    assert error is not None and error.code() == AUTH, f"expected TOPIC_AUTHORIZATION_FAILED, got {error}"
    writer = _plain_producer(broker, tmp_path, principal)
    refused = _produce_outcome(writer, RAW)
    assert refused is not None and refused.code() == AUTH, f"expected TOPIC_AUTHORIZATION_FAILED, got {refused}"
    assert _denied(broker, principal, "READ", f"Topic:LITERAL:{RAW}")
    assert _denied(broker, principal, "WRITE", f"Topic:LITERAL:{RAW}")
    del writer


def _admin_conf(broker: SecureBroker, tmp_path: Path, principal: str) -> dict[str, Any]:
    config = consumer_config(
        _settings(broker, tmp_path, principal), load_topic_defaults(), group_id=WORKER_GROUP, enable_partition_eof=True
    )
    return {k: v for k, v in config.items() if k.startswith(("bootstrap", "security", "sasl", "ssl"))}


# ---------------------------------------------------------------------------------------------------
# api


def test_api_role_reconciler_read_and_publishes_work_and_audit_read_and_out_topic_are_refused(
    broker: SecureBroker, tmp_path: Path, sentinel: str
) -> None:
    principal = "tb-plat-api"
    broker.provision(principal, contract_grants("api", env=ENV, channel=CHANNEL))

    # Positive 1: the reconciler's table read: assign from the beginning under the reconciler group, never commit.
    reconciler = _consumer(broker, tmp_path, principal, RECONCILER_GROUP)
    try:
        reconciler.assign([TopicPartition(CHANNELS, 0, 0)])
        record = None
        deadline = time.monotonic() + 30
        while record is None and time.monotonic() < deadline:
            message = reconciler.poll(1.0)
            if message is not None and message.error() is None:
                record = message
        assert record is not None and record.key() == CHANNEL.encode(), "the reconciler read of channels failed"
    finally:
        reconciler.close()

    # Positive 2: idempotent acks=all publish to channels (deploy) and to audit (AuditHook).
    producer = _producer(broker, tmp_path, principal)
    assert _produce_outcome(producer, CHANNELS) is None
    assert _produce_outcome(producer, AUDIT) is None
    _assert_no_denials(broker, tmp_path, sentinel, principal, "the api role needed a grant the contract withholds")

    # Negative 1: READ on audit. The role holds WRITE there, which implies DESCRIBE, so the broker answers the fetch
    # itself with TOPIC_AUTHORIZATION_FAILED.
    reader = _consumer(broker, tmp_path, principal, RECONCILER_GROUP)
    try:
        error = _first_error(reader, AUDIT)
    finally:
        reader.close()
    assert error is not None and error.code() == AUTH, f"READ {AUDIT}: expected authorization failure, got {error}"
    assert _denied(broker, principal, "READ", f"Topic:LITERAL:{AUDIT}")

    # Negative 2: READ on an out topic. The role holds nothing there, not even DESCRIBE, so librdkafka's metadata
    # request is the first thing refused: the client may surface an error or silence, and the broker's authorizer
    # log is the evidence. No record may arrive.
    reader = _consumer(broker, tmp_path, principal, RECONCILER_GROUP)
    try:
        reader.assign([TopicPartition(OUT, 0, 0)])
        arrived = [m for m in (reader.poll(1.0) for _ in range(15)) if m is not None and m.error() is None]
    finally:
        reader.close()
    assert arrived == []
    assert _denied_any(broker, principal, ("DESCRIBE", "READ"), f"Topic:LITERAL:{OUT}")

    # Negative 3: WRITE on a different out topic nothing else touched, so its denial can only be this produce's.
    refused = _produce_outcome(producer, OUT_WRITE)
    assert refused is not None, "a produce to an out topic succeeded under the api role"
    assert refused.code() == AUTH, f"WRITE {OUT_WRITE}: expected TOPIC_AUTHORIZATION_FAILED, got {refused}"
    assert _denied_any(broker, principal, ("DESCRIBE", "WRITE"), f"Topic:LITERAL:{OUT_WRITE}")
    del producer


def test_controls_each_over_wide_grant_is_seen_by_the_same_probes(
    broker: SecureBroker, tmp_path: Path, sentinel: str
) -> None:
    """The negative probes above must go red when a role is too wide. Each widened principal below makes the same
    probe succeed, and must itself record no denial (so the success is the grant's doing, not a retry)."""
    base_lag = contract_grants("lag_reader", env=ENV, channel=CHANNEL)
    lag = "tb-plat-lag-reader-wide"
    broker.provision(lag, [*base_lag, Grant("TOPIC", RAW, "LITERAL", "READ"), Grant("TOPIC", RAW, "LITERAL", "WRITE")])
    reader = _consumer(broker, tmp_path, lag, WORKER_GROUP)
    try:
        assert _first_error(reader, RAW) is None, "a lag reader with READ should have read a record"
    finally:
        reader.close()
    assert _produce_outcome(_plain_producer(broker, tmp_path, lag), RAW) is None, "WRITE should have been accepted"
    _assert_no_denials(broker, tmp_path, sentinel, lag, "the widened lag reader was denied something")

    base_api = contract_grants("api", env=ENV, channel=CHANNEL)
    api = "tb-plat-api-wide"
    broker.provision(
        api,
        [
            *base_api,
            Grant("TOPIC", AUDIT, "LITERAL", "READ"),
            Grant("TOPIC", OUT, "LITERAL", "READ"),
            Grant("TOPIC", OUT, "LITERAL", "WRITE"),
        ],
    )
    assert _produce_outcome(_producer(broker, tmp_path, api), OUT) is None, "WRITE on out should have been accepted"
    for topic in (AUDIT, OUT):
        reader = _consumer(broker, tmp_path, api, RECONCILER_GROUP)
        try:
            assert _first_error(reader, topic) is None, f"READ on {topic} should have returned a record"
        finally:
            reader.close()
    _assert_no_denials(broker, tmp_path, sentinel, api, "the widened api principal was denied something")


def _no_record_arrives(consumer: Consumer, topic: str, *, polls: int = 5) -> bool:
    consumer.assign([TopicPartition(topic, 0, 0)])
    return not any(m is not None and m.error() is None for m in (consumer.poll(1.0) for _ in range(polls)))


def test_reprocessor_does_exactly_what_it_is_granted_and_nothing_else(
    broker: SecureBroker, tmp_path: Path, sentinel: str
) -> None:
    """Replay and reprocess fetch from the channel's `raw` (assign, never join or commit) and publish back to it, and
    that is all: no read of any other topic, no write anywhere else, no group membership."""
    principal = "tb-plat-reprocessor"
    grants = contract_grants("reprocessor", env=ENV, channel=CHANNEL)
    broker.provision(principal, grants)

    # Positive: what the library does. Assign-read of raw under the reprocessor group id (DESCRIBE on the group only),
    # watermarks and offsetsForTimes (DESCRIBE on the topic), an idempotent acks=all publish to raw.
    reader = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
    try:
        reader.assign([TopicPartition(RAW, 0, 0)])
        records = []
        deadline = time.monotonic() + 30
        while len(records) < SEEDED and time.monotonic() < deadline:
            message = reader.poll(1.0)
            if message is not None and message.error() is None:
                records.append(message)
        assert len(records) >= SEEDED, f"read {len(records)} of {SEEDED} records from raw"
        low, high = reader.get_watermark_offsets(TopicPartition(RAW, 0), timeout=30, cached=False)
        assert low == 0 and high >= SEEDED
        times = reader.offsets_for_times([TopicPartition(RAW, 0, 0)], timeout=30)
        assert times[0].error is None
    finally:
        reader.close()
    producer = _producer(broker, tmp_path, principal)
    assert _produce_outcome(producer, RAW) is None
    _assert_no_denials(broker, tmp_path, sentinel, principal, "the reprocessor needed a grant the contract withholds")

    # Negative: no read of any other topic (out, dlq, events, channels, audit, another channel's raw).
    for topic in (OUT, DLQ, EVENTS, CHANNELS, AUDIT, OTHER_RAW):
        probe = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
        try:
            assert _no_record_arrives(probe, topic), f"the reprocessor read a record from {topic}"
        finally:
            probe.close()
        assert _denied_any(broker, principal, ("DESCRIBE", "READ"), f"Topic:LITERAL:{topic}"), f"no denial for {topic}"

    # Negative: no write anywhere but the channel's raw. The delivery error is the broker's authorization refusal.
    for topic in (OUT, DLQ, EVENTS, CHANNELS, AUDIT, OTHER_RAW):
        refused = _produce_outcome(producer, topic)
        assert refused is not None and refused.code() == AUTH, (
            f"WRITE {topic}: expected authorization failure, got {refused}"
        )
        assert _denied_any(broker, principal, ("DESCRIBE", "WRITE"), f"Topic:LITERAL:{topic}")

    # Negative: DESCRIBE on its group is not membership. It cannot commit, and cannot join (subscribe).
    committer = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
    try:
        committer.assign([TopicPartition(RAW, 0, 0)])
        with pytest.raises(KafkaException) as refused_commit:
            committer.commit(offsets=[TopicPartition(RAW, 0, 1)], asynchronous=False)
        assert refused_commit.value.args[0].code() == KafkaError.GROUP_AUTHORIZATION_FAILED
    finally:
        committer.close()
    joiner = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
    try:
        joiner.subscribe([RAW])
        errors = [m.error().code() for m in (joiner.poll(1.0) for _ in range(10)) if m is not None and m.error()]
        assert KafkaError.GROUP_AUTHORIZATION_FAILED in errors, f"joining the group was not refused: {errors}"
    finally:
        joiner.close()
    assert _denied(broker, principal, "READ", f"Group:LITERAL:{REPROCESSOR_GROUP}")


def test_controls_a_wider_reprocessor_is_seen_by_the_same_probes(
    broker: SecureBroker, tmp_path: Path, sentinel: str
) -> None:
    """One extra READ (out), one extra WRITE (dlq) and group READ: the reads, writes and commit that the test above
    expects to be refused now succeed, and the widened principal records no denial."""
    principal = "tb-plat-reprocessor-wide"
    broker.provision(
        principal,
        [
            *contract_grants("reprocessor", env=ENV, channel=CHANNEL),
            Grant("TOPIC", OUT, "LITERAL", "READ"),
            Grant("TOPIC", DLQ, "LITERAL", "WRITE"),
            Grant("GROUP", REPROCESSOR_GROUP, "LITERAL", "READ"),
        ],
    )
    probe = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
    try:
        assert not _no_record_arrives(probe, OUT), "READ on out should have returned the seeded record"
    finally:
        probe.close()
    assert _produce_outcome(_producer(broker, tmp_path, principal), DLQ) is None, "WRITE on dlq should be accepted"
    committer = _consumer(broker, tmp_path, principal, REPROCESSOR_GROUP)
    try:
        committer.assign([TopicPartition(RAW, 0, 0)])
        committer.commit(offsets=[TopicPartition(RAW, 0, 1)], asynchronous=False)
    finally:
        committer.close()
    _assert_no_denials(broker, tmp_path, sentinel, principal, "the widened reprocessor was denied something")


# ---------------------------------------------------------------------------------------------------
# reprocessor


def test_reprocessor_contract_grants_are_the_set_the_archiver_test_runs_under() -> None:
    """No broker needed: the archiver's deny-by-default test hand-writes this exact set; the contract must parse to it."""
    topic = raw_topic(ENV, CHANNEL)
    assert set(contract_grants("reprocessor", env=ENV, channel=CHANNEL)) == {
        Grant("TOPIC", topic, "LITERAL", "READ"),
        Grant("TOPIC", topic, "LITERAL", "WRITE"),
        Grant("TOPIC", topic, "LITERAL", "DESCRIBE"),
        Grant("CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
        Grant("GROUP", f"tb.{ENV}.reprocessor", "LITERAL", "DESCRIBE"),
    }
