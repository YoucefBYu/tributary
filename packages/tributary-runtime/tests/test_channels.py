"""The channel-spec cache and `LiveSpecSource`: invariant 3 and hot reload, with no Kafka.

`_tb.<env>.channels` is a worker's only source of truth for a channel, so the things tested here are
the ones that would take the data plane down if they were wrong: a bad record must not replace a good
spec, a tombstone must stop the channel, catch-up must end on partition EOF rather than a watermark,
and a republished version must take effect without a restart.
"""

from __future__ import annotations

import io
import json
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
