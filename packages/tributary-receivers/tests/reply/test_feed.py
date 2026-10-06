"""The reply feed over a fake consumer: when a partition may be advertised, what a revoke does, that an
unreadable record never crashes it, and what it will and will not log. No Kafka, no thread (`tick` is driven
by the test)."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator

import pytest
from tributary.receivers.metrics import Metrics
from tributary.receivers.reply.feed import parse_reply_record
from tributary.runtime.sync_answer import Resolution
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_ROUTE_LOST
from tributary.sdk.ids import new_correlation_id, uuid7

from .support import (
    CONTROL_ID_MARKER,
    KEY_MARKER,
    FakeConsumer,
    FakeKafkaError,
    FakeMessage,
    RecordingLog,
    new_feed,
    new_map,
    open_entry,
    reply_message,
)


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    event_loop = asyncio.new_event_loop()
    yield event_loop
    event_loop.close()


def test_a_partition_is_advertised_only_once_assigned_at_an_explicit_offset() -> None:
    consumer = FakeConsumer(watermarks={0: 7, 1: 0, 2: 41})
    feed, _, _ = new_feed(consumer)
    assert feed.choose_partition() is None and not feed.ready, "nothing is held before the first assignment"
    consumer.rebalance_assign(0, 2)
    assert sorted((p.partition, p.offset) for p in consumer.assigned) == [(0, 7), (2, 41)], (
        "assigned at the high watermark, so every reply produced after the advertisement is at or above it"
    )
    assert sorted(feed.held) == [0, 2] and feed.ready
    assert {feed.choose_partition() for _ in range(4)} == {0, 2}, "round-robin across held partitions"


def test_a_partition_whose_watermark_cannot_be_read_is_not_advertised_until_it_can() -> None:
    consumer = FakeConsumer(watermarks={0: 3, 1: 9})
    consumer.watermark_failures = {1}
    feed, _, _ = new_feed(consumer)
    consumer.rebalance_assign(0, 1)
    assert feed.held == (0,), "partition 1 has no established position: not advertised"
    feed._retry_at = 0.0
    feed.tick()
    assert feed.held == (0,) and consumer.seeks == []
    consumer.watermark_failures = set()
    feed._retry_at = 0.0
    feed.tick()
    assert consumer.seeks == [(1, 9)] and sorted(feed.held) == [0, 1]


def test_revoke_and_loss_remove_the_partition_and_resolve_its_entries(loop: asyncio.AbstractEventLoop) -> None:
    consumer = FakeConsumer(watermarks={0: 0, 1: 0})
    feed, metrics, _ = new_feed(consumer)
    consumer.rebalance_assign(0, 1)
    pending, _, _, _ = new_map(loop, partitions=feed, metrics=metrics)  # type: ignore[arg-type]
    feed.attach(pending)
    on_one = open_entry(pending)
    while on_one.partition != 1:  # round-robin: open until one lands on partition 1
        pending.commit(on_one, ok=False)
        on_one = open_entry(pending)
    pending.commit(on_one, ok=True)
    consumer.rebalance_revoke(1)
    assert on_one.outcome.result().resolution == Resolution("AE", REPLY_ROUTE_LOST)
    assert feed.held == (0,) and {feed.choose_partition() for _ in range(3)} == {0}
    assert [p.partition for p in consumer.assigned] == [0]
    consumer.rebalance_revoke(0, lost=True)
    assert feed.choose_partition() is None and not feed.ready, "no partition: sync traffic is refused"
    assert metrics.reply_ready == 0


def test_a_reply_record_resolves_its_entry_and_an_unknown_one_is_counted(loop: asyncio.AbstractEventLoop) -> None:
    consumer = FakeConsumer()
    feed, metrics, _ = new_feed(consumer)
    consumer.rebalance_assign(0)
    pending, _, _, _ = new_map(loop, partitions=feed, metrics=metrics)  # type: ignore[arg-type]
    feed.attach(pending)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    consumer.messages.append(reply_message(new_correlation_id(), str(uuid7())))  # nobody's
    consumer.messages.append(reply_message(entry.correlation_id, entry.msg_id or ""))
    feed.tick()
    feed.tick()
    assert metrics.replies_unmatched == 1
    assert entry.outcome.result().resolution == Resolution("AA")


@pytest.mark.parametrize(
    "msg",
    [
        FakeMessage(headers=None),
        FakeMessage(headers=[]),
        FakeMessage(headers=[(h.TB_CORRELATION_ID, b"not-a-uuid"), (h.TB_MSG_ID, str(uuid7()).encode())]),
        FakeMessage(headers=[(h.TB_CORRELATION_ID, new_correlation_id().encode())]),
        FakeMessage(headers=[(h.TB_CORRELATION_ID, b"\xff\xfe"), (h.TB_MSG_ID, str(uuid7()).encode())]),
        FakeMessage(headers=[(h.TB_CORRELATION_ID, None), (h.TB_MSG_ID, None)]),
        FakeMessage(headers=[("tb-correlation-id", new_correlation_id().encode()), ("tb-msg-id", b"x" * 100_000)]),
    ],
)
def test_an_unreadable_reply_is_dropped_never_fatal_and_logged_by_coordinates_only(msg: FakeMessage) -> None:
    assert parse_reply_record(msg) is None
    consumer = FakeConsumer()
    feed, metrics, log = new_feed(consumer)
    consumer.messages.append(msg)
    feed.tick()  # must not raise
    assert metrics.replies_unreadable == 1
    (_, event, fields) = log.lines[-1]
    assert event == "reply_record_dropped"
    assert set(fields) == {"reason", "topic", "partition", "offset"}
    assert KEY_MARKER not in log.text()


def test_a_poison_record_does_not_stop_the_records_behind_it(loop: asyncio.AbstractEventLoop) -> None:
    consumer = FakeConsumer()
    feed, metrics, _ = new_feed(consumer)
    consumer.rebalance_assign(0)
    pending, _, _, _ = new_map(loop, partitions=feed, metrics=metrics)  # type: ignore[arg-type]
    feed.attach(pending)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    consumer.messages += [FakeMessage(headers=None), reply_message(entry.correlation_id, entry.msg_id or "")]
    feed.tick()
    feed.tick()
    assert entry.outcome.result().resolution == Resolution("AA")


def test_drop_logging_is_bounded_under_a_poison_flood() -> None:
    consumer = FakeConsumer()
    feed, metrics, log = new_feed(consumer)
    for _ in range(50):
        consumer.messages.append(FakeMessage(headers=None))
        feed.tick()
    assert metrics.replies_unreadable == 50
    assert len([e for e in log.events() if e == "reply_record_dropped"]) < 10


def test_status_and_code_are_read_as_sent_and_bounded() -> None:
    corr, msg_id = new_correlation_id(), str(uuid7())
    record = parse_reply_record(reply_message(corr, msg_id, status="failed", code="TRANSFORM_ERROR", value=b"body"))
    assert record is not None
    assert (record.status, record.code, record.body) == ("failed", "TRANSFORM_ERROR", b"body")
    absent = parse_reply_record(reply_message(corr, msg_id, status=None))
    assert absent is not None and absent.status is None
    long = parse_reply_record(reply_message(corr, msg_id, status="s" * 500, code="C" * 500))
    assert long is not None and len(long.status or "") == 64 and len(long.code or "") == 64


def test_connection_down_makes_the_feed_not_ready_and_a_metadata_answer_restores_it() -> None:
    consumer = FakeConsumer()
    feed, metrics, _ = new_feed(consumer)
    consumer.rebalance_assign(0)
    assert feed.ready and metrics.reply_ready == 1
    consumer.messages.append(FakeMessage(error=FakeKafkaError("_ALL_BROKERS_DOWN")))
    feed.tick()
    assert not feed.ready and feed.choose_partition() is None and metrics.reply_ready == 0
    consumer.list_topics_ok = False
    feed._down_probe_at = 0.001
    feed.tick()
    assert not feed.ready
    consumer.list_topics_ok = True
    feed._down_probe_at = 0.001
    feed.tick()
    assert feed.ready and metrics.reply_ready == 1


def test_other_kafka_errors_do_not_change_readiness() -> None:
    consumer = FakeConsumer()
    feed, _, _ = new_feed(consumer)
    consumer.rebalance_assign(0)
    consumer.messages.append(FakeMessage(error=FakeKafkaError("_PARTITION_EOF")))
    feed.tick()
    assert feed.ready


def test_a_loop_that_falls_behind_pauses_the_poll_instead_of_queueing() -> None:
    """At most `slots` records are handed to the loop at once; the consumer is not polled past that."""
    consumer = FakeConsumer()
    posted: list[object] = []
    feed, _, _ = new_feed(consumer, slots=2)
    feed._custom_post = lambda fn, *a: posted.append((fn, a))  # type: ignore[assignment]
    for _ in range(5):
        consumer.messages.append(reply_message(new_correlation_id(), str(uuid7())))
    for _ in range(5):
        feed.tick()
    assert len(posted) == 2 and len(consumer.messages) == 3, "two slots in flight, three records left unread"
    fn, args = posted[0]  # type: ignore[misc]
    fn(*args)  # the loop catches up: one slot frees
    feed.tick()
    assert len(posted) == 3


def test_the_consumer_thread_reaches_the_loop_only_through_call_soon_threadsafe() -> None:
    """A real thread and a real loop: the reply is delivered on the loop's thread, not the consumer's."""
    loop = asyncio.new_event_loop()
    seen: list[str] = []
    consumer = FakeConsumer()
    feed, metrics, _ = new_feed(consumer)
    feed._custom_post = None

    class Sink:
        retired = False

        def __len__(self) -> int:
            return 1

        def deliver_reply(self, record: object) -> tuple[bool, Resolution | None]:
            seen.append(threading.current_thread().name)
            return True, None

        def partitions_revoked(self, partitions: frozenset[int]) -> list[Resolution]:
            return []

    feed.attach(Sink())
    consumer.messages.append(reply_message(new_correlation_id(), str(uuid7())))
    feed._loop = loop
    try:
        poll = threading.Thread(target=feed.tick, name="consumer-thread")
        poll.start()
        poll.join()
        loop.call_soon(loop.stop)
        loop.run_forever()
    finally:
        loop.close()
    assert seen == [threading.main_thread().name]
    assert metrics.replies_unmatched == 0


def test_close_leaves_the_group_and_drops_the_partitions() -> None:
    consumer = FakeConsumer()
    feed, metrics, _ = new_feed(consumer)
    consumer.rebalance_assign(0)
    feed.close()
    feed.close()  # idempotent
    assert consumer.closed and feed.choose_partition() is None and metrics.reply_ready == 0


def test_nothing_the_feed_logs_names_a_key_a_control_id_or_a_correlation_id(loop: asyncio.AbstractEventLoop) -> None:
    consumer = FakeConsumer(watermarks={0: 0})
    feed, metrics, log = new_feed(consumer)
    consumer.rebalance_assign(0)
    pending, _, _, _ = new_map(loop, partitions=feed, metrics=metrics)  # type: ignore[arg-type]
    feed.attach(pending)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    consumer.messages += [FakeMessage(headers=None), reply_message(entry.correlation_id, entry.msg_id or "")]
    feed.tick()
    feed.tick()
    consumer.rebalance_revoke(0)
    text = log.text() + repr(metrics.snapshot())
    for secret in (KEY_MARKER, CONTROL_ID_MARKER, entry.correlation_id, entry.msg_id or "?"):
        assert secret not in text


def test_a_fatal_consumer_error_stops_advertising_answers_the_entries_and_asks_for_a_restart(
    loop: asyncio.AbstractEventLoop,
) -> None:
    """A second member with the same `group.instance.id` makes librdkafka return `_FATAL` from `poll`, with no
    revoke or lost callback (measured): the fenced process must refuse before publishing, not keep advertising."""
    consumer = FakeConsumer()
    feed, metrics, log = new_feed(consumer)
    consumer.rebalance_assign(0)
    pending, _, _, _ = new_map(loop, partitions=feed, metrics=metrics)  # type: ignore[arg-type]
    feed.attach(pending)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    failures: list[BaseException] = []
    feed.fatal_handler = failures.append

    consumer.messages.append(FakeMessage(error=FakeKafkaError("_FATAL")))
    feed.tick()

    assert feed.choose_partition() is None and not feed.ready and metrics.reply_ready == 0
    assert entry.outcome.result().resolution == Resolution("AE", REPLY_ROUTE_LOST)
    assert len(failures) == 1 and "reply_consumer_fatal" in log.events()
    consumer.list_topics_ok = True
    feed._down_probe_at = 0.001
    feed.tick()
    assert not feed.ready, "a metadata answer does not revive a fatally broken consumer"
    consumer.messages.append(FakeMessage(error=FakeKafkaError("_FATAL")))
    feed.tick()
    assert len(failures) == 1, "reported once"


def test_open_reply_feed_refuses_without_an_explicit_instance() -> None:
    from tributary.receivers.errors import ConfigError
    from tributary.receivers.reply.feed import open_reply_feed
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.config import load_topic_defaults

    with pytest.raises(ConfigError, match="--instance"):
        open_reply_feed(
            KafkaSettings(bootstrap_servers="localhost:9092"),
            load_topic_defaults(),
            env="dev",
            channel="lab",
            instance=None,
            pid=1,
            log=RecordingLog(),
            metrics=Metrics(),
        )


def test_the_replier_effective_config_equals_the_topics_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """`topics/v0` `clients.reply_consumer`: every `must_not_hardcode` key reaches the real consumer with the
    contract's value, and nothing is borrowed from the worker profile (the acceptance line of the contract)."""
    import json
    from pathlib import Path

    import confluent_kafka
    from tributary.receivers.reply.feed import open_reply_feed
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.config import load_topic_defaults

    seen: dict[str, object] = {}

    class Capture:
        def __init__(self, config: dict[str, object]) -> None:
            seen.update(config)

    monkeypatch.setattr(confluent_kafka, "Consumer", Capture)
    contract = json.loads(
        (Path(__file__).resolve().parents[4] / "contracts" / "topics" / "v0" / "topic-defaults.json").read_text()
    )
    profile = contract["clients"]["reply_consumer"]
    must_not_hardcode = set(contract["must_not_hardcode"]["values"])
    open_reply_feed(
        KafkaSettings(bootstrap_servers="localhost:9092"),
        load_topic_defaults(),
        env="dev",
        channel="lab",
        instance="rx-1",
        pid=7,
        log=RecordingLog(),
        metrics=Metrics(),
    )
    expected = {
        key: profile[key]
        for key in (
            "enable.auto.commit",
            "auto.offset.reset",
            "isolation.level",
            "partition.assignment.strategy",
            "session.timeout.ms",
            "heartbeat.interval.ms",
        )
    }
    for key, value in expected.items():
        assert seen[key] == value, key
    # Each must_not_hardcode key of the replier is asserted against the effective config above.
    for key in (k for k in must_not_hardcode if k.startswith("clients.reply_consumer.")):
        assert seen[key.removeprefix("clients.reply_consumer.")] == profile[key.removeprefix("clients.reply_consumer.")]
    assert seen["group.id"] == "tb.dev.lab.replier" and seen["group.instance.id"] == "rx-1-p7"
    assert seen["isolation.level"] == "read_uncommitted"


# -- readiness reasons (contracts/observability/v0 2.3, `reply_consumer`) and authorization failures ------


def test_readiness_reason_walks_the_closed_list() -> None:
    consumer = FakeConsumer(watermarks={0: 5})
    consumer.watermark_failures = {1}
    feed, _, _ = new_feed(consumer)
    assert feed.readiness_reason() == "no_partition"  # nothing assigned yet
    consumer.rebalance_assign(1)  # assigned, but the watermark cannot be read: no fetch position
    assert feed.readiness_reason() == "not_established"
    consumer.rebalance_assign(0)
    assert feed.readiness_reason() is None
    consumer.messages.append(FakeMessage(error=FakeKafkaError("_ALL_BROKERS_DOWN")))
    feed.tick()
    assert feed.readiness_reason() == "disconnected"
    consumer.messages.append(FakeMessage(error=FakeKafkaError("_FATAL")))
    feed.tick()
    assert feed.readiness_reason() == "fatal"


@pytest.mark.parametrize("name", ["TOPIC_AUTHORIZATION_FAILED", "GROUP_AUTHORIZATION_FAILED", "_AUTHENTICATION"])
def test_an_authorization_error_makes_the_feed_not_ready_and_logs_a_code_once(name: str) -> None:
    consumer = FakeConsumer()
    feed, metrics, log = new_feed(consumer)
    consumer.rebalance_assign(0)
    assert feed.ready
    for _ in range(3):
        consumer.messages.append(FakeMessage(error=FakeKafkaError(name)))
        feed.tick()
    assert not feed.ready and feed.choose_partition() is None and metrics.reply_ready == 0
    assert feed.readiness_reason() == "unauthorized"
    [(level, _event, fields)] = [line for line in log.lines if line[1] == "reply_consumer_unauthorized"]
    assert level == "error" and fields["error_code"] == "REPLY_CONSUMER_UNAUTHORIZED"
    assert fields["kafka_error"] == name.lstrip("_") and "error_code" in fields


def test_authorization_recovers_when_a_record_arrives_or_the_error_stops_repeating() -> None:
    consumer = FakeConsumer()
    feed, _, log = new_feed(consumer)
    consumer.rebalance_assign(0)
    consumer.messages.append(FakeMessage(error=FakeKafkaError("TOPIC_AUTHORIZATION_FAILED")))
    feed.tick()
    assert not feed.ready
    consumer.messages.append(reply_message(new_correlation_id(), str(uuid7())))
    feed.tick()
    assert feed.ready and "reply_consumer_authorized" in log.events()
    consumer.messages.append(FakeMessage(error=FakeKafkaError("TOPIC_AUTHORIZATION_FAILED")))
    feed.tick()
    assert not feed.ready
    feed._auth_until = 0.001  # the decay window has passed with no repeat
    assert feed.ready
    assert [e for e in log.events() if e == "reply_consumer_unauthorized"] == ["reply_consumer_unauthorized"] * 2
