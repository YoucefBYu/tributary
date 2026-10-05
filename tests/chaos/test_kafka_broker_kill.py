"""W1-8's first chaos test: kill a Kafka broker mid-stream and prove invariant 1 held — no message the
real MLLP receiver ACKed is ever lost, even though the broker that goes down was actually leading part
of the traffic at the moment it died.

**W3-22: the scenario is keyed.** The channel spec sets `ordering: per-key` and a receiver-side
`partitionKey` (`hl7:PID.3.1`), so the record keys under chaos are the ones the real receiver derived from
each message, not keys a test harness invented (this closed "receiver-originated keys are not under chaos").
The sender interleaves 8 synthetic patients; every message carries its per-patient sequence number in
MSH-10. The test additionally asserts invariant 5 across the leader change: per key, the first delivery of
each sequence number is increasing (counted, not only asserted: `order_violations` in the report row); the
record key is the patient id the message carries; each key lives on exactly one partition; and the fault
touched keyed traffic: at least one key sits on a partition the killed broker led and has ACKed messages
both before and after the kill. The helpers that make those checks have mutation controls in
`test_chaos_assertion_mutation_controls.py`.

Marked `chaos` and `integration`: it needs Docker and injects a real `docker kill` against one of three
broker containers. Follows PHASE-0-DECISION section 4's rule for fault-injection tests: **assert the
fault happened rather than assume it, and keep a mutation control for the headline claim.** Two checks
do that here, not one:

1. Before killing, `_kill_a_leader` picks the target from *live metadata*, requiring it to actually be
   the leader of at least one `raw` partition right then — killing an idle bystander broker would make
   "no message lost" trivially true, the exact failure mode PHASE-0-DECISION names ("a kill scenario
   with `commit_every=1` replays nothing ... appearing to prove replay ordering").
2. After killing, `_kill_and_confirm` waits for the container to actually reach Docker's `exited`
   state and the test asserts a non-zero exit code: `docker kill` sends SIGKILL, which a JVM does not
   trap, so a graceful shutdown (exit 0) would mean the fault never really fired.

The remaining sends only continue once (2) is confirmed, so every send after the kill call is
guaranteed to run against a cluster that is genuinely down one broker, not one still mid-shutdown.

This is a 3-broker, RF=3/min-insync=2 cluster — the production-representative topology from
`spikes/common/docker-compose.yml` (ADR-001 2.2) — not the lite (RF=1) profile used elsewhere in this
wave: RF=1 cannot survive losing its only broker by design (ADR-002 D14, the lite-profile warning every
data-plane component logs), so a broker-kill test against it would prove nothing about durability, only
that the durability floor is exactly what the lite profile admits it is. It is a private, ephemeral
cluster for this test module only — never the shared Phase 0 spike cluster
(`spikes/common/docker-compose.yml`'s "DO NOT start a second cluster" is about *that* one, not about a
test bringing its own).
"""

from __future__ import annotations

import contextlib
import socket
import time
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from tributary.runtime.topics import raw_topic

from tests.support.channel_spec import ordered_document, write_channel_spec_file
from tests.support.chaos_assertions import (
    assert_each_key_on_one_partition,
    assert_fault_fired,
    assert_per_key_order_preserved,
    count_order_violations,
    keys_hit_by_fault,
    record_chaos_row,
)
from tests.support.kafka_helpers import create_topics_with_retry, drain_topic
from tests.support.mllp_client import is_application_accept, send_mllp
from tests.support.processes import ManagedProcess, spawn_mllp_receiver

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough"
DESTINATION = "http-stub"  # unused here (no loader in this test) but required by the spec shape
PARTITIONS = 3
MESSAGE_COUNT = 120
KILL_AFTER = MESSAGE_COUNT // 2
KEYS = 8  # synthetic patients, interleaved: message i belongs to patient i % KEYS, sequence i // KEYS
PARTITION_KEY = "hl7:PID.3.1"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _await_cluster(bootstrap: str, *, expected_brokers: int, timeout_s: float = 120.0) -> None:
    """Wait for metadata showing every broker, not just one: a 1-of-3 quorum can answer
    `list_topics` before the other two have finished joining, which would let a later assertion about
    "the leader is one of our three brokers" fail for a reason that has nothing to do with the fault
    this test injects."""
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            if len(metadata.brokers) >= expected_brokers:
                return
        except Exception as exc:  # any failure here means the cluster is not ready yet
            last = exc
        time.sleep(1.0)
    raise RuntimeError(
        f"Kafka cluster did not reach {expected_brokers} brokers: {type(last).__name__ if last else 'timeout'}"
    )


class Broker(NamedTuple):
    node_id: int
    container: DockerContainer
    bootstrap: str  # this broker's own EXTERNAL address, host-reachable


@pytest.fixture(scope="module")
def kafka_cluster() -> Iterator[tuple[str, dict[int, Broker]]]:
    """Three brokers, RF=3/min-insync=2, on their own Docker network — configuration matches
    `spikes/common/docker-compose.yml`'s `x-kafka-env`, parameterised for random ports/hostnames/cluster
    id so this test never touches that shared cluster."""
    ports = {1: _free_port(), 2: _free_port(), 3: _free_port()}
    network = Network().create()
    brokers: dict[int, Broker] = {}
    try:
        for node_id, port in ports.items():
            hostname = f"kafka{node_id}"
            container = (
                DockerContainer("apache/kafka:4.0.0")
                .with_network(network)
                .with_network_aliases(hostname)
                .with_env("CLUSTER_ID", "qa-chaos-3-broker")
                .with_env("KAFKA_NODE_ID", str(node_id))
                .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
                .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka1:9093,2@kafka2:9093,3@kafka3:9093")
                .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .with_env(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
                )
                .with_env(
                    "KAFKA_LISTENERS", "INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:19092"
                )
                .with_env("KAFKA_ADVERTISED_LISTENERS", f"INTERNAL://{hostname}:9092,EXTERNAL://127.0.0.1:{port}")
                # ADR-001 2.2 durability defaults -- the real floor, not the lite profile (see module
                # docstring for why this test needs RF=3/min-insync=2 rather than RF=1).
                .with_env("KAFKA_DEFAULT_REPLICATION_FACTOR", "3")
                .with_env("KAFKA_MIN_INSYNC_REPLICAS", "2")
                .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
                .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
                .with_env("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false")
                .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .with_bind_ports(19092, port)
            )
            container.start()
            brokers[node_id] = Broker(node_id, container, f"127.0.0.1:{port}")

        bootstrap = ",".join(broker.bootstrap for broker in brokers.values())
        _await_cluster(bootstrap, expected_brokers=len(brokers))
        yield bootstrap, brokers
    finally:
        for broker in brokers.values():
            # best-effort cleanup: the killed broker is already gone, stopping it again would just raise
            with contextlib.suppress(Exception):
                broker.container.stop()
        network.remove()


@pytest.fixture(scope="module")
def topics(kafka_cluster: tuple[str, dict[int, Broker]]) -> tuple[str, dict[int, Broker]]:
    """Create `raw` with a bounded retry: `_await_cluster` proves metadata lists all 3 brokers, not that
    the controller quorum has finished settling enough to serve `CreateTopics` -- a single attempt right
    after that gate can race a controller-quorum leader election and see a broker connection reset
    (`_DESTROY_BROKER`) instead of an answer. See `create_topics_with_retry`."""
    bootstrap, brokers = kafka_cluster
    wanted = [NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 3)]
    create_topics_with_retry(bootstrap, wanted)
    return bootstrap, brokers


@pytest.fixture(scope="module")
def mllp_port() -> int:
    return _free_port()


@pytest.fixture(scope="module")
def channel_spec_file(tmp_path_factory: pytest.TempPathFactory, mllp_port: int) -> Path:
    document = ordered_document(
        partition_key=PARTITION_KEY,
        mllp_port=mllp_port,
        http_url="http://127.0.0.1:1/ingest",  # never dialled: this test has no loader
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    path = tmp_path_factory.mktemp("chaos-channel-spec") / "channel.json"
    return write_channel_spec_file(path, document)


@pytest.fixture(scope="module")
def receiver(topics: tuple[str, dict[int, Broker]], channel_spec_file: Path) -> Iterator[ManagedProcess]:
    bootstrap, _brokers = topics
    proc = spawn_mllp_receiver(
        channel_spec=channel_spec_file, env=ENV, bootstrap=bootstrap, instance="mllp-chaos", timeout_s=60.0
    )
    try:
        yield proc
    finally:
        proc.terminate_and_wait()


def _kill_and_confirm(broker: Broker, *, timeout_s: float = 30.0) -> int:
    """`docker kill` (SIGKILL), then block until Docker itself reports the container `exited` — the
    guarantee every send issued after this returns is a send against a cluster genuinely down one
    broker, not one still mid-shutdown."""
    container = broker.container.get_wrapped_container()
    container.kill()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        container.reload()
        if container.status == "exited":
            return int(container.attrs["State"]["ExitCode"])
        time.sleep(0.2)
    raise AssertionError(f"broker {broker.node_id} did not reach Docker's 'exited' state within {timeout_s}s of kill()")


def _kill_a_leader(bootstrap: str, brokers: dict[int, Broker], *, topic: str) -> tuple[Broker, int, frozenset[int]]:
    """Pick the broker to kill from live metadata rather than a fixed node id: it must actually be
    leading at least one partition of `topic` right now, or killing it would prove nothing (module
    docstring, point 1)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(topic=topic, timeout=15)
    raw_meta = metadata.topics[topic]
    leaders = {partition.leader for partition in raw_meta.partitions.values()}
    assert leaders, f"{topic} has no partition leaders yet -- the cluster is not ready to be tested"
    target_id = next(iter(leaders))
    assert target_id in brokers, f"leader {target_id} is not one of the brokers this test started: {sorted(brokers)}"
    target = brokers[target_id]
    led = frozenset(p.id for p in raw_meta.partitions.values() if p.leader == target_id)
    exit_code = _kill_and_confirm(target)
    return target, exit_code, led


def _control_id(message: str) -> str:
    return message.split("\r", 1)[0].split("|")[9]


def _patient(index: int) -> str:
    return f"SYNTH{index % KEYS:03d}"


def _sequence(index: int) -> int:
    return index // KEYS


def _keyed_message(index: int) -> str:
    """ADT with MSH-3 TRIBUTARY-SYNTH. PID-3.1 is the partition key; MSH-10 is `P<patient>-<sequence>`."""
    control = f"{_patient(index)}-{_sequence(index):04d}"
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|TRIBUTARY-DST|DST-FAC|20260101000000||ADT^A08|{control}|P|2.5.1\r"
        f"PID|1||{_patient(index)}^^^SYNTH||TESTPATIENT^SYNTHETIC||19700101|U\r"
    )


def _sequence_of(control_id: str) -> int:
    return int(control_id.rsplit("-", 1)[1])


def test_broker_kill_mid_stream_loses_no_acknowledged_message_and_keeps_per_key_order(
    mllp_port: int, topics: tuple[str, dict[int, Broker]], receiver: ManagedProcess
) -> None:
    bootstrap, brokers = topics
    acked: dict[str, bool] = {}
    index_of: dict[str, int] = {}
    killed: Broker | None = None
    exit_code: int | None = None
    led: frozenset[int] = frozenset()

    for index in range(MESSAGE_COUNT):
        message = _keyed_message(index)
        control_id = _control_id(message)
        assert control_id and control_id not in acked, "control ids must be unique"
        ack = send_mllp("127.0.0.1", mllp_port, message.encode("ascii"), timeout_s=20.0)
        acked[control_id] = is_application_accept(ack)
        index_of[control_id] = index
        if index + 1 == KILL_AFTER:
            killed, exit_code, led = _kill_a_leader(bootstrap, brokers, topic=raw_topic(ENV, CHANNEL))

    assert killed is not None, "the kill never ran -- MESSAGE_COUNT is too small for KILL_AFTER to fire"
    assert exit_code is not None and exit_code != 0, (
        f"broker {killed.node_id} exited with code {exit_code}: a graceful stop exits 0, "
        "so this would mean the fault never really fired"
    )
    assert led, "the killed broker led no raw partition: the kill would prove nothing"

    acked_ids = {control_id for control_id, ok in acked.items() if ok}
    print(
        f"acked: {len(acked_ids)}/{MESSAGE_COUNT}; broker {killed.node_id} (led partitions {sorted(led)}) "
        f"killed after send #{KILL_AFTER}"
    )
    assert acked_ids, "nothing was acknowledged at all -- this run tested nothing"
    # A meaningful run must have kept the cluster available across the kill: RF=3/min-insync=2
    # surviving on 2 brokers is the entire point of this topology (module docstring). If most sends
    # after the kill failed, this cluster was not exercising the durability floor this test claims to.
    assert len(acked_ids) >= MESSAGE_COUNT * 0.9, (
        f"only {len(acked_ids)}/{MESSAGE_COUNT} were acknowledged -- RF=3/min-insync=2 should tolerate "
        "one broker down without this much disruption"
    )

    records = drain_topic(
        bootstrap, raw_topic(ENV, CHANNEL), partitions=PARTITIONS, expected=len(acked_ids), timeout_s=60
    )
    found_ids = {_control_id(r.value().decode("ascii")) for r in records}
    missing = acked_ids - found_ids

    # Invariant 5, from the receiver-derived keys: first deliveries per key, in `raw` order per partition.
    first: dict[bytes, list[int]] = {}
    partitions_of: dict[bytes, set[int]] = {}
    wrong_key = 0
    for record in sorted(records, key=lambda r: (r.partition(), r.offset())):
        control_id = _control_id(record.value().decode("ascii"))
        key = record.key()
        assert key is not None, "a per-key channel produced a record with no key (invariant 5)"
        if key != control_id.rsplit("-", 1)[0].encode("ascii"):
            wrong_key += 1
        partitions_of.setdefault(key, set()).add(record.partition())
        seen = first.setdefault(key, [])
        sequence = _sequence_of(control_id)
        if sequence not in seen:
            seen.append(sequence)
    violations = count_order_violations(first)
    duplicates = len(records) - len(found_ids)

    # The fault touched keyed traffic: a key on a partition the killed broker led, ACKed on both sides of it.
    def acked_indexes(key: bytes) -> list[int]:
        return [index_of[c] for c in acked_ids if c.rsplit("-", 1)[0].encode("ascii") == key]

    hit = keys_hit_by_fault(
        partitions_of, led, {key: acked_indexes(key) for key in partitions_of}, kill_index=KILL_AFTER
    )
    record_chaos_row(
        scenario="broker-kill-mid-stream-keyed",
        sent=MESSAGE_COUNT,
        delivered_total=len(records),
        delivered_distinct=len(found_ids),
        order_violations=violations,
        fault_fired=exit_code != 0 and bool(hit),
    )
    print(
        f"keys={len(first)} partitions_by_key={ {k.decode(): sorted(v) for k, v in partitions_of.items()} } "
        f"keys_hit_by_fault={sorted(k.decode() for k in hit)} order_violations={violations} duplicates={duplicates}"
    )
    assert not missing, f"invariant 1 violated: {len(missing)} acknowledged message(s) missing from raw: {missing}"
    assert wrong_key == 0, f"{wrong_key} record(s) whose key is not the PID-3.1 the message carries"
    assert len(first) == KEYS, f"expected {KEYS} distinct keys on raw, saw {len(first)}"
    assert_each_key_on_one_partition(partitions_of)
    assert_fault_fired(
        bool(hit),
        description="no key with ACKed traffic on both sides of the kill sat on a partition the killed broker led",
    )
    assert_per_key_order_preserved(first)
    assert violations == 0
