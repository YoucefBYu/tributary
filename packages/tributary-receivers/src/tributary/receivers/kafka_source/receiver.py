"""The Kafka source receiver: consume a topic on an external cluster, publish each record to
`<env>.<channel>.raw`, and commit the **source** offset only after the raw produce is acknowledged
(invariant 1; README 3.2.2's `kafka` row).

**Ordering and throughput.** Records are handled one at a time and each waits for its own delivery
report before the next is published. That is deliberate: publishing a batch concurrently would let a
later record of a partition reach `raw` while an earlier one is still failing and being re-read, which
reorders a per-key channel silently (invariant 5). The cost is one ack round trip per record per
receiver process; scale by partitions (consumer-group members), not by pipelining. Offsets are committed
in batches (`commit_every`, the topics contract's value, and at most `commit_flush_interval_s` after the
first uncommitted record), which is this source's replay window: after an unclean death, up to that many
already-published records are re-published under new `tb-msg-id`s (README 3.2.2 "Duplicates at ingest").

**Publish timeouts can duplicate.** A produce that outlives `ack.timeoutMs` is reported as refused, but it
is not cancelled in librdkafka and may still be delivered later (`delivery.timeout.ms` is longer). The
source record is then re-read and re-published under a new `tb-msg-id`, so `raw` can hold two records for
one source record. At-least-once, never loss; loader idempotency on `tb-msg-id` does not absorb it
(README 3.2.2 "Duplicates at ingest").

**Kafka unavailable** (the engine's `raw` cluster refuses): nothing is committed; every assigned
partition is paused, the failed record's partition is sought back to it, and consumption resumes after
an exponential backoff. The source consumer keeps polling while paused, so the group does not
lose its membership.

**A refused record halts its partition** (ADR-007 D16). A Kafka source cannot NACK and cannot write to
`dlq` (the receiver's ACL is WRITE on `raw` only), so a record that is oversize, empty, a tombstone,
malformed JSON when `parse-error` is declared, or whose per-key partition key does not resolve pauses
its source partition: no offset at or past it is committed, nothing is dropped, and order is kept. The
log line and metric carry topic, partition and offset and a reason code, never the key, headers or
payload. The halt lasts until the process restarts or the partition is reassigned, and an operator
must act (skip the offset on the source cluster, or fix the producer).

**Live spec (invariant 3).** `apply_settings` replaces the settings; each record reads them once when it is
handled, so one record is never checked under one spec and stamped with another. `pause` (a tombstone) stops
after the record in flight: nothing fetched afterwards is published, `run` then commits **only the offsets
that were acked** and closes the consumer. A record fetched but not published is not committed and is re-read
after a redeploy. `kafka_source/reload.py` is the live-versus-restart table.

**Inbound headers are never trusted or copied**: the envelope is minted fresh; the inbound key is used
only by a `record:key` partitionKey; inbound headers are read only by a `header:` partitionKey.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import Callable
from typing import Any, Literal, Protocol

from confluent_kafka import KafkaException, TopicPartition
from tributary.runtime import load_engine_kafka_or_exit
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.sdk import Message

from ..envelope import build_raw_envelope
from ..errors import PermanentPublishError, ReceiverError
from ..keys import resolve_key
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher, topic_name
from ..payload import well_formed
from ..secrets import DirSecretResolver, SecretResolver
from .config import KafkaSourceSettings
from .security import security_properties

__all__ = ["KafkaSourceReceiver", "build_consumer"]

_log = logging.getLogger("tributary.receivers.kafka_source")
_PARTITION_EOF = -191  # confluent_kafka.KafkaError._PARTITION_EOF


class _Publisher(Protocol):
    async def publish(self, message: Message, *, timeout_s: float) -> bool: ...


class SourceConsumer(Protocol):
    """The slice of `confluent_kafka.Consumer` this receiver uses (fakeable without a broker)."""

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[[Any, list[TopicPartition]], None] | None = ...,
        on_revoke: Callable[[Any, list[TopicPartition]], None] | None = ...,
        on_lost: Callable[[Any, list[TopicPartition]], None] | None = ...,
    ) -> None: ...
    def poll(self, timeout: float) -> Any: ...
    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[False]) -> Any: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    def assignment(self) -> list[TopicPartition]: ...
    def close(self) -> None: ...


def _event(event: str, **fields: object) -> None:
    _log.info(json.dumps({"event": event, **fields}))


class KafkaSourceReceiver:
    def __init__(
        self,
        cfg: KafkaSourceSettings,
        publisher: _Publisher,
        consumer: SourceConsumer,
        *,
        metrics: Metrics | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        self.publisher = publisher
        self.consumer = consumer
        self.metrics = metrics if metrics is not None else Metrics()
        self._clock = clock
        self._pending: dict[int, int] = {}  # partition -> next offset to commit (last acked + 1)
        self._pending_count = 0
        self._first_pending_at = 0.0
        self.halted: dict[int, int] = {}  # partition -> offset of the record that halted it
        self._resume_at: float | None = None
        self._backoff_s = cfg.unavailable_backoff_initial_s
        self._source = f"kafka:{cfg.instance}@{cfg.source_bootstrap[0]}"
        self._paused = False

    def apply_settings(self, new: KafkaSourceSettings) -> None:
        """Replace the settings (a live spec update; `kafka_source/reload.py`). One assignment: a record that
        already took its snapshot finishes under the old one. The caller has refused restart-only changes."""
        self.cfg = new

    def pause(self) -> None:
        """Stop after the record in flight (a tombstone): nothing more is published, `run` commits what was
        acked and closes. Idempotent; a redeploy builds a new receiver."""
        self._paused = True

    @property
    def paused(self) -> bool:
        return self._paused

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self.consumer.subscribe([self.cfg.topic], on_revoke=self._on_revoke, on_lost=self._on_lost)

    def _on_revoke(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        self._commit()  # everything acked so far, before another member takes the partitions over
        for tp in partitions:
            self._pending.pop(tp.partition, None)
            self.halted.pop(tp.partition, None)

    def _on_lost(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        for tp in partitions:  # membership is gone: a commit would be refused; the new owner re-reads
            self._pending.pop(tp.partition, None)
            self.halted.pop(tp.partition, None)

    def close(self) -> None:
        self._commit()
        self.consumer.close()

    async def run(self, stop: asyncio.Event) -> None:
        self.start()
        try:
            while not stop.is_set() and not self._paused:
                await self.step()
        finally:
            await asyncio.to_thread(self.close)

    async def step(self) -> None:
        """One poll and, if it yielded a record, its handling. Public so tests can drive it."""
        self._maybe_resume()
        msg = await asyncio.to_thread(self.consumer.poll, self.cfg.poll_timeout_s)
        if msg is None:
            self._commit_if_due()
            return
        error = msg.error()
        if error is not None:
            self._on_poll_error(error)
            return
        await self._handle(msg)
        self._commit_if_due()

    def _on_poll_error(self, error: Any) -> None:
        if error.code() == _PARTITION_EOF:
            return
        if error.fatal():
            raise ReceiverError(f"fatal source consumer error ({error.name()})")
        self.metrics.poll_errors += 1
        _event("kafka_source_error", channel=self.cfg.channel, code=error.name())

    # ------------------------------------------------------------------ one record

    @staticmethod
    def _refusal(
        cfg: KafkaSourceSettings, value: bytes | None, key: bytes | None, headers: Any
    ) -> tuple[str | None, bytes | None]:
        """`(reason, None)` if the record must be refused, else `(None, partition key)`."""
        if value is None:
            return "tombstone", None
        if len(value) > cfg.max_message_bytes:
            return "oversize", None
        if not value:
            return "empty", None
        if "parse-error" in cfg.nack_on and not well_formed(cfg.content_type, value):
            return "parse-error", None
        outcome = resolve_key(
            cfg.key_expression,
            payload=value,
            content_type=cfg.content_type,
            headers=[(name, val) for name, val in (headers or [])],
            record_key=key,
            header_source="kafka",
        )
        if outcome.reason is not None:
            return outcome.reason, None
        return None, outcome.key

    async def _handle(self, msg: Any) -> None:
        cfg = self.cfg  # one snapshot for this whole record
        partition: int = msg.partition()
        offset: int = msg.offset()
        if partition in self.halted:
            return  # in-flight leftover from a halted partition: never processed, never committed past
        if self._paused:
            return  # tombstoned after this record was fetched: not published, not committed, re-read on redeploy
        self.metrics.messages_received += 1
        value: bytes | None = msg.value()
        reason, key = self._refusal(cfg, value, msg.key(), msg.headers())
        if reason is not None or value is None:
            self._halt(partition, offset, reason or "tombstone")
            return
        message = build_raw_envelope(
            channel=cfg.channel,
            channel_version=cfg.channel_version,
            source=self._source,
            payload=value,
            key=key,
            content_type=cfg.content_type,
        )
        t0 = time.monotonic()
        try:
            ok = await self.publisher.publish(message, timeout_s=cfg.ack_timeout_s)
        except PermanentPublishError:
            self.metrics.rejected_envelope += 1
            self._halt(partition, offset, "envelope-invalid")  # D16: retrying cannot help
            return
        except Exception:  # any failure is "not committed"; its text is never surfaced
            ok = False
        self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
        if not ok:
            self._back_off(partition, offset)
            return
        # The raw record is committed: only now may the source offset advance.
        self.metrics.acked += 1
        self._backoff_s = self.cfg.unavailable_backoff_initial_s
        if not self._pending:
            self._first_pending_at = self._clock()
        self._pending[partition] = offset + 1
        self._pending_count += 1

    def _halt(self, partition: int, offset: int, reason: str) -> None:
        """D16: pause the source partition; no offset at or past `offset` is ever committed."""
        self._commit()  # what was acked before the bad record is still committed
        self.halted[partition] = offset
        self.consumer.pause([TopicPartition(self.cfg.topic, partition)])
        self.metrics.partitions_halted += 1
        if reason == "oversize":
            self.metrics.nacked_oversize += 1
        else:
            self.metrics.nacked_parse_error += 1
        _event(
            "kafka_source_partition_halted",
            channel=self.cfg.channel,
            topic=self.cfg.topic,
            partition=partition,
            offset=offset,
            reason=reason,
        )

    def _back_off(self, partition: int, offset: int) -> None:
        """The engine's `raw` cluster refused: commit nothing for this record, pause, re-read it later."""
        self.metrics.publish_failures += 1
        self.metrics.nacked_unavailable += 1
        assigned = [tp for tp in self.consumer.assignment() if tp.partition not in self.halted]
        if assigned:
            self.consumer.pause(assigned)
        self.consumer.seek(TopicPartition(self.cfg.topic, partition, offset))
        self._resume_at = self._clock() + self._backoff_s
        _event(
            "kafka_source_publish_refused",
            channel=self.cfg.channel,
            topic=self.cfg.topic,
            partition=partition,
            offset=offset,
            retry_in_s=self._backoff_s,
        )
        self._backoff_s = min(self._backoff_s * 2, self.cfg.unavailable_backoff_max_s)

    def _maybe_resume(self) -> None:
        if self._resume_at is None or self._clock() < self._resume_at:
            return
        self._resume_at = None
        resumable = [tp for tp in self.consumer.assignment() if tp.partition not in self.halted]
        if resumable:
            self.consumer.resume(resumable)

    # ------------------------------------------------------------------ offsets

    def _commit_if_due(self) -> None:
        if not self._pending:
            return
        if self._pending_count >= self.cfg.commit_every or (
            self._clock() - self._first_pending_at >= self.cfg.commit_flush_interval_s
        ):
            self._commit()

    def _commit(self) -> None:
        if not self._pending:
            return
        offsets = [TopicPartition(self.cfg.topic, p, o) for p, o in sorted(self._pending.items())]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            # Not fatal: the offsets stay pending and are retried; at worst the window is replayed.
            self.metrics.commit_failures += 1
            _event("kafka_source_commit_failed", channel=self.cfg.channel, code=exc.args[0].name())
            return
        self.metrics.offset_commits += 1
        self._pending.clear()
        self._pending_count = 0


def build_consumer(cfg: KafkaSourceSettings, resolver: SecretResolver) -> Any:
    """The source consumer, on the source's own connection and credentials (ADR-007 D11). Manual commit,
    cooperative-sticky, static membership (`group.instance.id` is the instance id)."""
    from confluent_kafka import Consumer

    config = consumer_config(
        KafkaSettings(",".join(cfg.source_bootstrap), security_properties(cfg.security, resolver)),
        load_topic_defaults(),
        group_id=cfg.group_id,
        group_instance_id=cfg.instance,
        auto_offset_reset=cfg.start_from,
    )
    config["client.id"] = f"kafka-source-{cfg.channel}-{cfg.instance}"
    config["queued.max.messages.kbytes"] = 8192  # bounds what librdkafka prefetches ahead of us
    config["socket.keepalive.enable"] = True
    return Consumer(config)


def main() -> None:
    """`python -m tributary.receivers.kafka_source --channel <name> --env <env>`.

    The channel spec is read from the compacted `_tb.<env>.channels` topic over the engine Kafka link
    (invariant 3); there is no file input. Consumption starts only after a valid spec for `--channel` has been
    read. Exit codes: 0 after SIGTERM/SIGINT or after a spec change that needs a restart (the supervisor
    restarts on 0), 2 for a refused startup configuration."""
    from tributary.runtime.errors import RuntimeConfigurationError

    from ..cli import QuietArgumentParser, run_until_stopped, spec_feed_opener
    from ..startup import refuse, refuse_retired_channel_spec_flag, spec_reader_group_or_exit
    from .runner import KafkaSourceRunner, RunnerOptions
    from .service import KafkaSourceService

    argv = sys.argv[1:]
    log = JsonLogger("receiver-kafka-source")
    # Before parse_args: argparse echoes an unknown flag's value to stderr, so a retired
    # --bootstrap / --engine-security / --channel-spec must reach its refusal first.
    kafka = load_engine_kafka_or_exit(argv=argv, log=log)
    refuse_retired_channel_spec_flag(argv, log)
    p = QuietArgumentParser(log, description=__doc__)
    p.add_argument("--channel", required=True, help="the deployed channel name (spec.metadata.name)")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--instance", default="kafka-0", help="this process's identity: static group member and tb-source")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
    p.add_argument("--metrics-file")
    a = p.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    # Operator input is validated (and refused without being echoed) before it becomes a log field.
    spec_group = spec_reader_group_or_exit(a.env, a.channel, a.instance, log)
    log = JsonLogger("receiver-kafka-source", base={"channel": a.channel, "instance": a.instance})
    try:
        defaults = load_topic_defaults(a.contracts_dir)
    except RuntimeConfigurationError as exc:
        refuse(log, error=type(exc).__name__, code=exc.code)
    try:
        topic = topic_name(a.env, a.channel)
    except ValueError:
        refuse(log, error="UsageError", code="ARGUMENTS_INVALID", reason="invalid --env or --channel")
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()

    def make_publisher(cfg: KafkaSourceSettings) -> RawPublisher:
        publisher = RawPublisher(
            kafka,
            topic,
            topic_defaults=defaults,
            ordering="per-key" if cfg.key_expression is not None else "none",
            client_id_prefix="kafka-source-receiver",
        )
        publisher.start(asyncio.get_running_loop())
        return publisher

    def make_service(cfg: KafkaSourceSettings, publisher: RawPublisher, metrics: Metrics) -> KafkaSourceService:
        # The topic, the foreign connection and its credentials are restart-only, so a running service never
        # changes them in place. A service is built per start, not per process: a tombstone followed by a
        # redeploy starts a new one, and this builds it a new consumer from the redeployed spec's settings (the
        # old one was committed and closed on the pause).
        return KafkaSourceService(cfg, publisher, build_consumer(cfg, resolver), metrics=metrics)

    runner = KafkaSourceRunner(
        open_feed=spec_feed_opener(
            kafka=kafka, defaults=defaults, env=a.env, channel=a.channel, group=spec_group, log=log
        ),
        make_publisher=make_publisher,
        make_service=make_service,
        options=RunnerOptions(env=a.env, instance=a.instance, topic=topic),
        commit_every=defaults.consumer.commit_every,
        log=log,
        metrics_file=a.metrics_file,
    )
    run_until_stopped(runner)


if __name__ == "__main__":
    main()
