"""The channel-spec cache and `LiveSpecSource`: invariant 3 and hot reload, with no Kafka.

`_tb.<env>.channels` is a worker's only source of truth for a channel, so the things tested here are
the ones that would take the data plane down if they were wrong: a bad record must not replace a good
spec, a tombstone must stop the channel, catch-up must end on partition EOF rather than a watermark,
and a republished version must take effect without a restart.
"""

from __future__ import annotations

import io
import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from tributary.runtime.channels import ChannelCache, LiveSpecSource, open_spec_source
from tributary.runtime.consumer import PARTITION_EOF
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.logs import JsonLogger

from .fakes import FakeError

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
TOPIC = "_tb.dev.channels"
CHANNEL = "adt-passthrough"


def spec_bytes(*, version: int = 1, name: str = CHANNEL) -> bytes:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "name": name, "version": version}
    return json.dumps(document).encode("utf-8")


@dataclass(frozen=True, slots=True)
class FakeChannelsRecord:
    _key: bytes | None
    _value: bytes | None
    _offset: int = 0
    _partition: int = 0
    _error: FakeError | None = None

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return self._partition

    def error(self) -> FakeError | None:
        return self._error


class FakeTopicMetadata:
    def __init__(self, partitions: int) -> None:
        self.partitions = dict.fromkeys(range(partitions))
        self.error = None


class FakeMetadata:
    def __init__(self, topic: str, partitions: int) -> None:
        self.topics = {topic: FakeTopicMetadata(partitions)}


@dataclass
class FakeChannelsConsumer:
    """A compacted-topic reader: `assign` from the beginning, then a scripted sequence of records."""

    records: list[FakeChannelsRecord] = field(default_factory=list)
    partitions: int = 1
    assigned: list[Any] = field(default_factory=list)
    closed: bool = False
    index: int = 0

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = list(partitions)

    def poll(self, timeout: float) -> FakeChannelsRecord | None:
        if self.index >= len(self.records):
            return None
        record = self.records[self.index]
        self.index += 1
        return record

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> FakeMetadata:
        return FakeMetadata(topic or TOPIC, self.partitions)

    def close(self) -> None:
        self.closed = True


def eof(partition: int = 0) -> FakeChannelsRecord:
    return FakeChannelsRecord(_key=None, _value=None, _partition=partition, _error=FakeError(PARTITION_EOF))


def cache(consumer: FakeChannelsConsumer, stream: io.StringIO | None = None) -> ChannelCache:
    return ChannelCache(consumer, TOPIC, log=JsonLogger("test", stream=stream or io.StringIO(), level="debug"))


def test_the_cache_assigns_every_partition_from_the_beginning() -> None:
    """`assign`, not `subscribe`: every worker needs every spec, so group sharing would give each a
    fraction of the channels."""
    consumer = FakeChannelsConsumer(partitions=3)
    subject = cache(consumer)
    subject.start()
    assert len(consumer.assigned) == 3


def test_a_published_spec_is_applied() -> None:
    consumer = FakeChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=4))])
    subject = cache(consumer)
    subject.start()
    assert subject.poll_once() is True

    spec = subject.require(CHANNEL)
    assert spec.name == CHANNEL and spec.version == 4
    assert subject.names() == [CHANNEL]
    assert subject.applied == 1


def test_a_tombstone_undeploys_the_channel() -> None:
    """A null value means undeployed: the runtime stops the channel and releases its partitions."""
    consumer = FakeChannelsConsumer(
        records=[
            FakeChannelsRecord(CHANNEL.encode(), spec_bytes()),
            FakeChannelsRecord(CHANNEL.encode(), None, _offset=1),
        ]
    )
    subject = cache(consumer)
    subject.start()
    subject.poll_once()
    subject.poll_once()

    assert subject.get(CHANNEL) is None
    assert subject.removed == 1
    with pytest.raises(ChannelSpecError):
        subject.require(CHANNEL)


@pytest.mark.parametrize(
    ("value", "why"),
    [
        (b"{not json", "unparseable JSON"),
        (json.dumps({"kind": "Channel", "metadata": {"name": CHANNEL}, "spec": {}}).encode(), "no metadata.version"),
        (spec_bytes(name="other-channel"), "the record key and metadata.name disagree"),
    ],
)
def test_a_bad_record_is_rejected_and_the_previous_spec_keeps_running(value: bytes, why: str) -> None:
    """Channel-spec contract 6 rule 1. A worker that dropped its spec over a bad publish would stop
    processing a channel that was working a moment earlier."""
    consumer = FakeChannelsConsumer(
        records=[
            FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=2)),
            FakeChannelsRecord(CHANNEL.encode(), value, _offset=1),
        ]
    )
    stream = io.StringIO()
    subject = cache(consumer, stream)
    subject.start()
    subject.poll_once()
    subject.poll_once()

    assert subject.require(CHANNEL).version == 2, f"rejected because: {why}"
    assert subject.rejected == 1
    assert "channel_spec" in stream.getvalue()


def test_a_rejected_record_never_logs_the_spec_body() -> None:
    """A spec is tenant-supplied text. It is not PHI by design, but it is not ours to echo either."""
    poisoned = json.dumps(
        {"kind": "Channel", "metadata": {"name": CHANNEL, "description": "SYNTH-SPEC-MARKER"}, "spec": {}}
    ).encode()
    consumer = FakeChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), poisoned)])
    stream = io.StringIO()
    subject = cache(consumer, stream)
    subject.start()
    subject.poll_once()

    assert subject.rejected == 1
    assert "SYNTH-SPEC-MARKER" not in stream.getvalue()


def test_catch_up_ends_on_partition_eof() -> None:
    """EOF, not the high watermark: the watermark counts control records the application never sees,
    so a watermark loop can simply never terminate."""
    consumer = FakeChannelsConsumer(
        records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0), eof(1)],
        partitions=2,
    )
    subject = cache(consumer)
    subject.start()
    subject.catch_up(timeout_s=5.0)

    assert subject.require(CHANNEL).name == CHANNEL


def test_catch_up_refuses_to_start_a_worker_without_its_specs() -> None:
    consumer = FakeChannelsConsumer(records=[], partitions=1)
    subject = cache(consumer)
    subject.start()
    with pytest.raises(ChannelSpecError, match="timed out"):
        subject.catch_up(timeout_s=0.2)


def test_a_republished_version_takes_effect_without_a_restart() -> None:
    """Hot reload, which is what `LiveSpecSource.current()` exists for (invariant 3)."""
    consumer = FakeChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=1)), eof(0)])
    source = open_spec_source(
        channel=CHANNEL, consumer=consumer, topic=TOPIC, log=JsonLogger("test", stream=io.StringIO())
    )
    assert source.current().version == 1

    # The control plane republishes while the worker is running.
    consumer.records.append(FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=9), _offset=1))

    assert source.current().version == 9, "the next call picks up the republished spec; no restart"


def test_the_spec_source_refuses_to_open_for_an_undeployed_channel() -> None:
    """Starting with no spec means having nothing to apply; guessing a default would be worse."""
    consumer = FakeChannelsConsumer(records=[eof(0)])
    with pytest.raises(ChannelSpecError):
        open_spec_source(
            channel="not-deployed", consumer=consumer, topic=TOPIC, log=JsonLogger("test", stream=io.StringIO())
        )


def test_closing_the_source_closes_the_consumer() -> None:
    consumer = FakeChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    source = open_spec_source(
        channel=CHANNEL, consumer=consumer, topic=TOPIC, log=JsonLogger("test", stream=io.StringIO())
    )
    source.close()
    assert consumer.closed is True


def test_the_source_is_the_only_hot_reload_path() -> None:
    """A component holding a spec it read once would drift from the deployed channel until restarted."""
    consumer = FakeChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes())])
    subject = cache(consumer)
    subject.start()
    subject.poll_once()
    source = LiveSpecSource(subject, CHANNEL)
    assert source.cache is subject
    assert source.current().name == CHANNEL


# -- broker outage: the cache says it may be stale, and keeps serving (invariant 3) -------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


@dataclass
class FlakyChannelsConsumer(FakeChannelsConsumer):
    """`list_topics` fails while `down`, the way librdkafka does with every broker unreachable."""

    down: bool = False
    probes: int = 0
    topic_error: Any = None
    leader: int = 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        self.probes += 1
        if self.down and self.probes > 1:  # the first call is `start()`'s own metadata read
            from confluent_kafka import KafkaError, KafkaException

            raise KafkaException(KafkaError(KafkaError._TRANSPORT, "broker SYNTHPATIENT^ALPHA:9092 unreachable"))
        metadata = FakeMetadata(topic or TOPIC, self.partitions)
        detail = type("PartitionMetadata", (), {"error": None, "leader": self.leader})()
        metadata.topics[topic or TOPIC].partitions = dict.fromkeys(range(self.partitions), detail)
        metadata.topics[topic or TOPIC].error = self.topic_error
        return metadata


def inline(target: Callable[[], None]) -> None:
    """Run the probe on the caller's thread, so a test sees its result in the same `poll_once`."""
    target()


def started(
    consumer: FlakyChannelsConsumer,
    clock: FakeClock,
    stream: io.StringIO,
    *,
    spawn: Callable[[Callable[[], None]], None] | None = inline,
) -> ChannelCache:
    subject = ChannelCache(
        consumer,
        TOPIC,
        log=JsonLogger("test", stream=stream, level="debug"),
        probe_interval_s=5.0,
        clock=clock,
        spawn=spawn,
    )
    subject.start()
    subject.catch_up(timeout_s=1.0)
    return subject


def events(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_an_unreachable_broker_raises_a_typed_error_and_the_spec_keeps_serving() -> None:
    """The security finding: callers could not tell they were serving stale specs. Now `poll_once`
    raises `ChannelsUnavailableError` while the topic cannot be shown readable, and the cache and
    `LiveSpecSource.current()` keep answering from the last applied spec."""
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=2)), eof(0)])
    clock, stream = FakeClock(), io.StringIO()
    subject = started(consumer, clock, stream)
    source = LiveSpecSource(subject, CHANNEL)

    consumer.down = True
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError) as raised:
        subject.poll_once(timeout_s=0.0)
    assert raised.value.error_code == "_TRANSPORT"
    assert raised.value.code == "CHANNELS_UNAVAILABLE"
    assert "SYNTHPATIENT" not in str(raised.value), "broker text never reaches the message"
    assert subject.available is False and source.available is False
    assert subject.require(CHANNEL).version == 2, "invariant 3: the applied spec is still served"
    assert source.current().version == 2, "current() never fails on an outage"

    clock.now += 1.0
    with pytest.raises(ChannelsUnavailableError):
        subject.poll_once(timeout_s=0.0)  # a level, not an edge: raised for as long as it lasts

    logged = [line for line in events(stream) if line["event"].startswith("channels_")]
    assert [line["event"] for line in logged] == ["channels_unavailable"], "logged once per streak"
    assert logged[0]["error_code"] == "_TRANSPORT"
    assert "SYNTHPATIENT" not in stream.getvalue()


def test_recovery_is_proven_by_the_next_probe_and_logged_once() -> None:
    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock, stream = FakeClock(), io.StringIO()
    subject = started(consumer, clock, stream)
    consumer.down = True
    clock.now += 5.0
    source = LiveSpecSource(subject, CHANNEL)
    source.current()
    assert subject.available is False

    consumer.down = False
    clock.now += 2.0
    source.current()
    assert subject.available is False, "the probe is rate-limited: not re-run within the interval"
    clock.now += 3.0
    assert subject.poll_once(timeout_s=0.0) is False  # no raise

    assert subject.available is True and subject.outages == 1
    recovered = [line for line in events(stream) if line["event"] == "channels_recovered"]
    assert len(recovered) == 1 and recovered[0]["outage_s"] == 5.0


def test_a_long_outage_does_not_delay_recovery_beyond_one_probe_interval() -> None:
    """Pins the code half of the integration test's recovery bound (`recovery_bound_s`): however many
    probes failed, the cache keeps probing at the same cadence, caches no error from them, and recovers
    on the first probe after the broker answers. So any longer delay is librdkafka's reconnect backoff
    or the environment, never the cache."""
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock, stream = FakeClock(), io.StringIO()
    subject = started(consumer, clock, stream)
    consumer.down = True
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError):
        subject.poll_once(timeout_s=0.0)  # detected by the first probe due after `start()`'s
    for _ in range(720):  # an hour more of failed probes at a 5 s interval, polled ten times per interval
        for _tick in range(10):
            clock.now += 0.5
            with pytest.raises(ChannelsUnavailableError):
                subject.poll_once(timeout_s=0.0)
    failed_probes = consumer.probes - 1  # less `start()`'s own metadata read
    assert failed_probes == 721, "one probe per interval for the whole outage, no back-off of its own"

    consumer.down = False
    back_at = clock.now
    while True:
        clock.now += 0.5
        try:
            subject.poll_once(timeout_s=0.0)
        except ChannelsUnavailableError:
            continue
        break
    assert clock.now - back_at <= 5.0, "recovered on the first probe after the broker answered"
    assert subject.available is True and subject.outages == 1
    assert [line["event"] for line in events(stream) if line["event"].startswith("channels_")] == [
        "channels_unavailable",
        "channels_recovered",
    ]


def test_the_probe_is_rate_limited_while_healthy() -> None:
    """One metadata request per interval, however often a worker asks for its spec."""
    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock = FakeClock()
    subject = started(consumer, clock, io.StringIO())
    after_start, started_at = consumer.probes, clock.now
    for tick in range(60):  # 100 ms apart for 6 s: the probe is due once, at 5 s
        clock.now = started_at + tick / 10
        subject.poll_once(timeout_s=0.0)
    assert consumer.probes - after_start == 1


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"topic_error": FakeError(29)}, "29"),  # TOPIC_AUTHORIZATION_FAILED, as a fake without name()
        ({"leader": -1}, "LEADER_NOT_AVAILABLE"),
    ],
)
def test_a_readable_cluster_with_an_unreadable_topic_is_an_outage(change: dict[str, Any], code: str) -> None:
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock = FakeClock()
    subject = started(consumer, clock, io.StringIO())
    for name, value in change.items():
        setattr(consumer, name, value)
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError) as raised:
        subject.poll_once(timeout_s=0.0)
    assert raised.value.error_code == code


def test_a_consumer_error_event_is_an_outage_until_a_fetch_succeeds() -> None:
    """A fetch error outranks a passing metadata probe in the same interval (READ can be denied while
    DESCRIBE is allowed); a delivered record or an EOF is proof of readability and ends it at once."""
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock, stream = FakeClock(), io.StringIO()
    subject = started(consumer, clock, stream)
    consumer.records.append(FakeChannelsRecord(None, None, _error=FakeError(29)))
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError):
        subject.poll_once(timeout_s=0.0)
    assert [line["event"] for line in events(stream)][-2:] == ["channels_consumer_error", "channels_unavailable"]

    consumer.records.append(FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=5), _offset=1))
    assert subject.poll_once(timeout_s=0.0) is True, "the record is applied, and nothing is raised"
    assert subject.available is True and subject.require(CHANNEL).version == 5


def test_catch_up_is_not_interrupted_by_the_outage_signal() -> None:
    """Startup keeps its own rule (refuse after `timeout_s`), rather than failing on the first error event."""
    consumer = FlakyChannelsConsumer(
        records=[
            FakeChannelsRecord(None, None, _error=FakeError(29)),
            FakeChannelsRecord(CHANNEL.encode(), spec_bytes()),
            eof(0),
        ]
    )
    subject = started(consumer, FakeClock(), io.StringIO())
    assert subject.require(CHANNEL).name == CHANNEL
    assert subject.available is True, "the EOF after the error proved the topic readable"


def test_the_mllp_receivers_feed_now_sees_the_outage() -> None:
    """`LiveSpecFeed.poll` is what the MLLP runner wraps for `spec_poll_failed`/`spec_poll_recovered`;
    before this change it could never raise, so neither line could fire."""
    from tributary.receivers.mllp.runner import LiveSpecFeed
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes(version=3)), eof(0)])
    clock = FakeClock()
    feed = LiveSpecFeed(LiveSpecSource(started(consumer, clock, io.StringIO()), CHANNEL), CHANNEL)
    consumer.down = True
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError):
        feed.poll()
    spec = feed.get()
    assert spec is not None and spec.version == 3, "the runner reconciles against the last good spec"
    consumer.down = False
    clock.now += 5.0
    feed.poll()


# -- the probe runs off the caller's thread (sync-reply p99 regression, 2026-10-05) -------------------


@dataclass
class SlowChannelsConsumer(FlakyChannelsConsumer):
    """`list_topics` blocks until released, as a MetadataRequest queued behind a Fetch long-poll does."""

    release: threading.Event = field(default_factory=threading.Event)
    entered: threading.Event = field(default_factory=threading.Event)
    fail: bool = False
    #: Probes that have entered `list_topics` (the base class counts them only once they return).
    entries: int = 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        if self.probes == 0:  # `start()`'s own metadata read is synchronous by design
            return super().list_topics(topic, timeout)
        self.entries += 1
        self.entered.set()
        self.release.wait(5.0)
        if self.fail:
            self.down = True
        return super().list_topics(topic, timeout)


def test_a_slow_probe_never_blocks_the_caller() -> None:
    """Regression: the probe ran inline and took 470-490 ms on a healthy broker (its request waits
    behind the consumer's own Fetch long-poll), stalling the worker thread and the MLLP event loop every
    5 s: the sync-reply AA p99 of 440-500 ms. Now `poll_once` returns while the probe is in flight."""
    consumer = SlowChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock = FakeClock()
    subject = started(consumer, clock, io.StringIO(), spawn=None)  # the production daemon thread
    source = LiveSpecSource(subject, CHANNEL)
    clock.now += 5.0
    began = time.monotonic()
    assert source.current().name == CHANNEL
    assert consumer.entered.wait(2.0), "the probe started"
    for _ in range(20):  # however often the worker asks, while the probe is out
        clock.now += 1.0
        source.current()
    assert time.monotonic() - began < 0.5, "no call waited for the probe"
    assert consumer.entries == 1, "at most one probe in flight"
    consumer.release.set()
    assert subject._probe_idle.wait(2.0)
    assert subject.available is True


def test_a_failed_off_thread_probe_is_applied_and_logged_on_the_callers_thread() -> None:
    from tributary.runtime.errors import ChannelsUnavailableError

    consumer = SlowChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)], fail=True)
    clock, stream = FakeClock(), io.StringIO()
    subject = started(consumer, clock, stream, spawn=None)
    clock.now += 5.0
    subject.poll_once(timeout_s=0.0)  # starts the probe; nothing is known yet, so nothing is raised
    assert subject.available is True
    consumer.release.set()
    assert subject._probe_idle.wait(2.0)
    with pytest.raises(ChannelsUnavailableError) as raised:
        subject.poll_once(timeout_s=0.0)
    assert raised.value.error_code == "_TRANSPORT"
    assert [line["event"] for line in events(stream) if line["event"].startswith("channels_")] == [
        "channels_unavailable"
    ]


def test_a_probe_that_cannot_start_is_an_outage_not_a_crash() -> None:
    from tributary.runtime.errors import ChannelsUnavailableError

    def refuse(_target: Callable[[], None]) -> None:
        raise RuntimeError("can't start new thread")

    consumer = FlakyChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock = FakeClock()
    subject = started(consumer, clock, io.StringIO(), spawn=refuse)
    clock.now += 5.0
    with pytest.raises(ChannelsUnavailableError) as raised:
        subject.poll_once(timeout_s=0.0)
    assert raised.value.error_code == "RuntimeError"
    assert LiveSpecSource(subject, CHANNEL).current().name == CHANNEL, "invariant 3: still served"


def test_close_waits_for_an_in_flight_probe_before_closing_the_consumer() -> None:
    """Closing a librdkafka handle under a thread inside `list_topics` is a use-after-free."""
    consumer = SlowChannelsConsumer(records=[FakeChannelsRecord(CHANNEL.encode(), spec_bytes()), eof(0)])
    clock = FakeClock()
    subject = started(consumer, clock, io.StringIO(), spawn=None)
    clock.now += 5.0
    subject.poll_once(timeout_s=0.0)
    assert consumer.entered.wait(2.0)
    threading.Timer(0.2, consumer.release.set).start()
    began = time.monotonic()
    subject.close()
    assert time.monotonic() - began >= 0.15, "close waited for the probe"
    assert consumer.closed
