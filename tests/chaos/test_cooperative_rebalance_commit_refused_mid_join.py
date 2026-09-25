"""W2-9 chaos scenario 2b: members join a channel's consumer group **while traffic is flowing**, so the
existing members' commits land inside a cooperative rebalance and the broker refuses some of them.

This is the live counterpart of `d2ead8d` ("a commit refused mid-rebalance no longer crashes the consumer
loop"). The S7 spike loader crashed on `ILLEGAL_GENERATION` during a planned handover and then hung on
exit; `ConsumerLoop` had the same unguarded commit. The unit tests in
`packages/tributary-runtime/tests/test_consumer_loop.py` fake the refusal; this scenario makes a real
broker refuse a real commit and checks what a refusal is allowed to cost and what it is not:

* **Allowed:** duplicates. A refused commit leaves its offsets pending, and a revoke whose commit is
  refused hands the partition over at the last *committed* offset, so the new owner replays. That is
  at-least-once (invariant 4), and the duplicate rate is printed, not asserted to be zero -- unlike
  `test_cooperative_rebalance_stable_join.py`, which sends traffic only between settled rebalances and
  therefore can demand zero.
* **Not allowed:** a lost message, a per-key reordering (first-delivery order, S5's rule), or a founder
  process that died on the refusal.

**The fault must be shown to fire** (PHASE-0-DECISION section 4). "A joiner arrived under load" is not
enough: a run in which every commit happened to land outside the rebalance window proves nothing about
the refusal path. So the scenario adds joiners one at a time, each while the publisher is running, until
at least one member has logged `commit_refused` (the `ConsumerLoop` warning emitted only on the three
rebalance error codes), and fails if `MAX_JOINERS` joins never produce one. Every joiner must also settle
with more than zero partitions, for the same reason the stable-join scenario checks it.

Joiners are never removed: members use static membership (`group.instance.id`), so a leaving member does
not send `LeaveGroup` and its partitions would sit unowned until `session.timeout.ms` expired, which is a
different scenario (`test_worker_kill_mid_commit_window.py`).

Real subprocesses (`tests.support.processes.spawn_pipeline_worker`), the shared checks from
`tests.support.chaos_assertions` (mutation-controlled in `test_chaos_assertion_mutation_controls.py`), the
settle logic from `tests.support.rebalance_helpers`, and `ordering: per-key` published straight to `raw`.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk import headers as h
from tributary.sdk.testing import make_message

from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import (
    assert_fault_fired,
    assert_no_acknowledged_loss,
    assert_per_key_order_preserved,
    duplicate_rate,
)
from tests.support.kafka_helpers import (
    await_broker,
    drain_topic,
    first_delivery_order,
    free_port,
    header_map,
    wait_for_at_least,
)
from tests.support.processes import ManagedProcess, spawn_pipeline_worker
from tests.support.rebalance_helpers import wait_until_settled

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough-ordered-join-under-load"
DESTINATION = "http-stub"
PARTITIONS = 6
KEYS = [f"SYNTH-JL-{index}".encode() for index in range(12)]
SEQ_HEADER = "tb-usr-seq"
#: One round is one message per key. At 20 rounds/s that is 240 msg/s: enough that a founder commits
#: (every `commit_every` = 50 records, or on idle) several times inside each rebalance window, and well
#: under the ~700 msg/s S5 measured for one worker, so the backlog never hides a stall.
ROUND_INTERVAL_S = 0.05
#: Joins attempted before the scenario gives up on seeing a refusal. Each adds a member; with 6
#: partitions, 4 joiners (6 members) is the most that can each still own at least one partition.
MAX_JOINERS = 4


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
        .with_env("CLUSTER_ID", "chaos-join-under-load")
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
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
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
def deployed_spec(topics: str) -> dict[str, Any]:
    document = ordered_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/ingest",  # never dialled: no loader runs in this scenario
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


class ContinuousPublisher:
    """Publishes one sequenced message per key every `ROUND_INTERVAL_S` on a background thread until
    stopped, so traffic is in flight for the whole of every join. `acks=all` and the idempotent
    producer, the same floor `tests.support.kafka_helpers.publish_messages` uses; an id counts as
    published only once its delivery report succeeded, so a send the broker never acknowledged can
    never be reported as "lost"."""

    def __init__(self, bootstrap: str) -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._acked: list[str] = []
        self._failed = 0
        self._rounds = 0
        self._thread = threading.Thread(target=self._run, name="continuous-publisher", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _on_delivery(self, msg_id: str) -> Any:
        def callback(error: Any, _record: Any) -> None:
            with self._lock:
                if error is None:
                    self._acked.append(msg_id)
                else:
                    self._failed += 1

        return callback

    def _run(self) -> None:
        sequence = 0
        while not self._stop.is_set():
            for key in KEYS:
                message = make_message(
                    b'{"synthetic": true}', key=key, channel=CHANNEL, headers={SEQ_HEADER: str(sequence)}
                )
                msg_id = message.require(h.TB_MSG_ID)
                self._producer.produce(
                    raw_topic(ENV, CHANNEL),
                    key=message.key,
                    value=message.payload,
                    headers=[(name, value.encode()) for name, value in message.headers.items()],
                    on_delivery=self._on_delivery(msg_id),
                )
            self._producer.poll(0)
            sequence += 1
            with self._lock:
                self._rounds = sequence
            self._stop.wait(ROUND_INTERVAL_S)

    def published_count(self) -> int:
        with self._lock:
            return len(self._acked)

    def abort(self) -> None:
        """Stop without `stop()`'s assertions, for cleanup after a failure elsewhere."""
        self._stop.set()
        self._thread.join(timeout=30)

    def stop(self) -> list[str]:
        self._stop.set()
        self._thread.join(timeout=30)
        assert not self._thread.is_alive(), "publisher thread did not stop"
        assert self._producer.flush(30) == 0, "producer.flush timed out or left records undelivered"
        with self._lock:
            assert self._failed == 0, f"{self._failed} publish(es) failed: the broker never acknowledged them"
            return list(self._acked)


def _refusals(processes: dict[str, ManagedProcess]) -> int:
    for process in processes.values():
        process.collect_pending()
    return sum(1 for process in processes.values() for line in process.lines if line.get("event") == "commit_refused")


def _assert_alive(processes: dict[str, ManagedProcess]) -> None:
    dead = {
        name: process.process.returncode for name, process in processes.items() if process.process.poll() is not None
    }
    assert not dead, f"member(s) exited during a rebalance under load -- the refusal path crashed them: {dead}"


def test_joins_under_load_survive_refused_commits_without_loss_or_reordering(
    topics: str, deployed_spec: dict[str, Any]
) -> None:
    out = out_topic(ENV, CHANNEL, DESTINATION)
    processes: dict[str, ManagedProcess] = {}
    publisher = ContinuousPublisher(topics)
    published: list[str] = []
    try:
        for name in ("founder1", "founder2"):
            processes[name] = spawn_pipeline_worker(
                env=ENV, channel=CHANNEL, bootstrap=topics, instance_id=f"pipeline-{name}"
            )
        founders = wait_until_settled(processes, expected_total=PARTITIONS)
        assert_fault_fired(
            min(founders.values()) > 0,
            description="a founder settled with zero partitions -- the group never actually split",
        )

        publisher.start()
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=len(KEYS) * 20, timeout_s=60.0)

        joiners = 0
        while _refusals(processes) == 0 and joiners < MAX_JOINERS:
            joiners += 1
            name = f"joiner{joiners}"
            before = publisher.published_count()
            processes[name] = spawn_pipeline_worker(
                env=ENV, channel=CHANNEL, bootstrap=topics, instance_id=f"pipeline-{name}", timeout_s=60.0
            )
            settled = wait_until_settled(processes, expected_total=PARTITIONS)
            assert_fault_fired(
                settled[name] > 0,
                description=f"{name} settled with zero partitions -- this join moved nothing",
            )
            assert_fault_fired(
                publisher.published_count() > before,
                description=f"no traffic was published while {name} joined -- the join was not under load",
            )
            _assert_alive(processes)

        refusals = _refusals(processes)
        assert_fault_fired(
            refusals > 0,
            description=f"{joiners} join(s) under load and no member logged commit_refused -- "
            "no commit landed inside a rebalance, so the refusal path was never exercised",
        )

        published = publisher.stop()
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=len(published), timeout_s=120.0)
        _assert_alive(processes)
    finally:
        if not published:
            publisher.abort()  # a failure above: stop the thread without masking the original error
        for process in reversed(list(processes.values())):
            process.terminate_and_wait()

    delivered = drain_topic(topics, out, partitions=PARTITIONS, expected=len(published), timeout_s=30.0)
    delivered_ids = [header_map(record)[h.TB_MSG_ID] for record in delivered]

    assert_no_acknowledged_loss(set(published), set(delivered_ids))

    duplicates = len(delivered_ids) - len(set(delivered_ids))
    rate = duplicate_rate(published_count=len(published), delivered_count=len(delivered_ids))
    print(
        f"join-under-load: {joiners} joiner(s), {refusals} refused commit(s), duplicate rate {rate:.2%} "
        f"({duplicates} of {len(published)} published)"
    )

    order = first_delivery_order(delivered, sequence_header=SEQ_HEADER)
    assert set(order) == set(KEYS), "every key must appear in what was delivered"
    assert_per_key_order_preserved(order)
