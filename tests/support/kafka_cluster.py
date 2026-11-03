"""A free RF=3 Kafka cluster for chaos tests (W3-33b): three KRaft brokers in Docker via testcontainers.

Two topologies, both on a private Docker network with unique container names, so a run never touches the
shared Phase 0 spike cluster, the lite Compose stack or another agent's containers, and teardown only
stops containers this object started:

* `combined`: three nodes, each `broker,controller`. Losing two of them also loses the controller quorum.
* `split`: three brokers plus one dedicated controller. Stopping two brokers leaves the quorum (and the
  metadata path) alive, which is what "two of three brokers down" needs to mean for a durability test.

Every broker and topic value comes from `contracts/topics/v0/topic-defaults.json` (the `broker` block and
the `raw` class), not from a literal here: `min.insync.replicas` is the value invariant 1 depends on. Only
settings that exist to make a laptop-sized test cluster behave (heap, fast ISR shrink, no rebalance delay)
are local, and each says why.

The module is test-owned plumbing. It does not stand in for anything in `packages/*/src`.
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from tests.support.kafka_helpers import await_broker, create_topics_with_retry, free_port

Topology = Literal["combined", "split"]

KAFKA_IMAGE = "apache/kafka:4.0.0"
TOPIC_DEFAULTS = Path(__file__).resolve().parents[2] / "contracts" / "topics" / "v0" / "topic-defaults.json"
CONTROLLER_NODE_ID = 4  # `split` only
#: Test-cluster tuning, not contract values. Heap keeps four JVMs inside a 4 vCPU / 15 GiB host; the short
#: ISR lag makes "ISR shrank" observable in seconds instead of the 30 s default.
HEAP_OPTS = "-Xms256m -Xmx512m"
REPLICA_LAG_TIME_MAX_MS = "6000"


def load_defaults() -> dict[str, Any]:
    document = json.loads(TOPIC_DEFAULTS.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _env_name(key: str) -> str:
    return "KAFKA_" + key.upper().replace(".", "_")


@dataclass
class ClusterBroker:
    node_id: int
    container: DockerContainer
    external: str  # host-reachable `127.0.0.1:<port>`
    name: str


@dataclass
class Rf3Cluster:
    topology: Topology
    network: Network
    brokers: dict[int, ClusterBroker]
    controller: DockerContainer | None
    defaults: dict[str, Any]
    run_id: str
    _stopped: set[int] = field(default_factory=set)

    # -- addresses --------------------------------------------------------------------------------

    @property
    def bootstrap(self) -> str:
        return ",".join(b.external for b in self.brokers.values())

    def bootstrap_for(self, ids: list[int]) -> str:
        return ",".join(self.brokers[i].external for i in ids)

    def alive_ids(self) -> list[int]:
        return [i for i in self.brokers if i not in self._stopped]

    @property
    def alive_bootstrap(self) -> str:
        return self.bootstrap_for(self.alive_ids())

    # -- contract-derived values ------------------------------------------------------------------

    @property
    def replication_factor(self) -> int:
        return int(self.defaults["broker"]["default.replication.factor"])

    @property
    def min_insync(self) -> int:
        return int(self.defaults["broker"]["min.insync.replicas"])

    @property
    def raw_partitions(self) -> int:
        return int(self.defaults["classes"]["raw"]["partitions"])

    def raw_topic_configs(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.defaults["classes"]["raw"]["configs"].items()}

    def create_topic(
        self, name: str, *, replication_factor: int | None = None, overrides: Mapping[str, str] | None = None
    ) -> None:
        """A `raw`-class topic per the contract. `overrides` is for controls (`min.insync.replicas=1`)."""
        config = {**self.raw_topic_configs(), **(overrides or {})}
        rf = self.replication_factor if replication_factor is None else replication_factor
        create_topics_with_retry(self.alive_bootstrap, [NewTopic(name, self.raw_partitions, rf, config=config)])

    # -- metadata ---------------------------------------------------------------------------------

    def partition_state(self, topic: str, *, timeout_s: float = 10.0) -> dict[int, tuple[int, list[int]]]:
        """`{partition: (leader, isr)}` read from the brokers that are up."""
        metadata = AdminClient({"bootstrap.servers": self.alive_bootstrap}).list_topics(topic=topic, timeout=timeout_s)
        info = metadata.topics[topic]
        return {pid: (p.leader, sorted(p.isrs)) for pid, p in info.partitions.items()}

    def leaders(self, topic: str) -> dict[int, int]:
        return {pid: leader for pid, (leader, _isr) in self.partition_state(topic).items()}

    def await_isr(self, topic: str, predicate: Any, *, what: str, timeout_s: float = 120.0) -> dict[int, Any]:
        """Poll until `predicate(leader, isr)` holds for every partition; the state is returned as evidence."""
        deadline = time.monotonic() + timeout_s
        last: dict[int, tuple[int, list[int]]] = {}
        while time.monotonic() < deadline:
            with contextlib.suppress(Exception):
                last = self.partition_state(topic)
                if last and all(predicate(leader, isr) for leader, isr in last.values()):
                    return last
            time.sleep(1.0)
        raise AssertionError(f"{topic}: {what} not reached within {timeout_s}s; last state {last}")

    def await_full_isr(self, topic: str, *, timeout_s: float = 180.0) -> dict[int, Any]:
        rf = self.replication_factor
        return self.await_isr(
            topic, lambda leader, isr: leader >= 0 and len(isr) == rf, what=f"full ISR ({rf})", timeout_s=timeout_s
        )

    # -- faults (own containers only) -------------------------------------------------------------

    def _wait_exited(self, broker: ClusterBroker, *, timeout_s: float = 60.0) -> int:
        wrapped = broker.container.get_wrapped_container()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            wrapped.reload()
            if wrapped.status == "exited":
                return int(wrapped.attrs["State"]["ExitCode"])
            time.sleep(0.2)
        raise AssertionError(f"broker {broker.node_id} did not reach 'exited' within {timeout_s}s")

    def kill(self, node_id: int) -> int:
        """SIGKILL (`docker kill`); returns the exit code, non-zero when the fault really fired."""
        broker = self.brokers[node_id]
        broker.container.get_wrapped_container().kill()
        code = self._wait_exited(broker)
        self._stopped.add(node_id)
        return code

    def stop(self, node_id: int, *, timeout_s: int = 60) -> int:
        """Graceful stop (SIGTERM, controlled shutdown); exit code 143 or 0 are both normal for the JVM."""
        broker = self.brokers[node_id]
        broker.container.get_wrapped_container().stop(timeout=timeout_s)
        code = self._wait_exited(broker)
        self._stopped.add(node_id)
        return code

    def is_stopped(self, node_id: int) -> bool:
        return node_id in self._stopped

    def assert_durability_floor(self, topic: str) -> None:
        """Guard: the brokers and `topic` really carry min ISR, RF and unclean election from the contract, read
        back with `describe_configs`, so a mis-set cluster cannot make the chaos results look meaningful."""
        admin = AdminClient({"bootstrap.servers": self.alive_bootstrap})
        want_isr, want_rf = str(self.min_insync), self.replication_factor
        resources = [ConfigResource(ConfigResource.Type.BROKER, str(i)) for i in self.alive_ids()]
        resources.append(ConfigResource(ConfigResource.Type.TOPIC, topic))
        described = {}
        for resource in resources:  # one BROKER resource per call is all the admin API allows
            described.update(admin.describe_configs([resource]))
        for resource, future in described.items():
            entries = {name: entry.value for name, entry in future.result(timeout=30).items()}
            is_broker = resource.restype == ConfigResource.Type.BROKER
            label = f"{'broker' if is_broker else 'topic'}:{resource.name}"
            assert entries.get("min.insync.replicas") == want_isr, f"{label} min.insync.replicas != {want_isr}"
            assert entries.get("unclean.leader.election.enable") == "false", f"{label} unclean election not off"
            if is_broker:
                assert entries.get("default.replication.factor") == str(want_rf), f"{label} default RF != {want_rf}"
        metadata = admin.list_topics(topic=topic, timeout=15).topics[topic]
        short = {p: len(m.replicas) for p, m in metadata.partitions.items() if len(m.replicas) != want_rf}
        assert not short, f"{topic}: partitions with replica count != {want_rf}: {short}"

    def start(self, node_id: int) -> None:
        broker = self.brokers[node_id]
        broker.container.get_wrapped_container().start()
        self._stopped.discard(node_id)

    def close(self) -> None:
        for broker in self.brokers.values():
            with contextlib.suppress(Exception):
                broker.container.stop()
        if self.controller is not None:
            with contextlib.suppress(Exception):
                self.controller.stop()
        with contextlib.suppress(Exception):
            self.network.remove()


def _common_env(container: DockerContainer, defaults: Mapping[str, Any], cluster_id: str) -> DockerContainer:
    broker = defaults["broker"]
    rf = str(int(broker["default.replication.factor"]))
    min_isr = str(int(broker["min.insync.replicas"]))
    container = (
        container.with_env("CLUSTER_ID", cluster_id)
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_HEAP_OPTS", HEAP_OPTS)
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_REPLICA_LAG_TIME_MAX_MS", REPLICA_LAG_TIME_MAX_MS)
        # internal topics follow the same floor as the contract's data topics
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", rf)
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", rf)
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", min_isr)
    )
    for key, value in broker.items():  # replication factor, min ISR, unclean election, auto-create, max bytes
        container = container.with_env(_env_name(key), str(value).lower() if isinstance(value, bool) else str(value))
    return container


def start_cluster(topology: Topology = "split") -> Rf3Cluster:
    """Start the cluster and block until metadata lists all three brokers. Raises with the containers
    already stopped if startup fails."""
    defaults = load_defaults()
    run_id = uuid.uuid4().hex[:8]
    cluster_id = f"tbrf3{run_id}".ljust(22, "x")[:22]
    network = Network().create()
    ports = {1: free_port(), 2: free_port(), 3: free_port()}
    if topology == "split":
        voters = f"{CONTROLLER_NODE_ID}@tb-ctrl:9093"
    else:
        voters = ",".join(f"{i}@tb-k{i}:9093" for i in ports)
    brokers: dict[int, ClusterBroker] = {}
    controller: DockerContainer | None = None
    cluster = Rf3Cluster(topology, network, brokers, None, defaults, run_id)
    try:
        if topology == "split":
            controller = _common_env(
                DockerContainer(KAFKA_IMAGE)
                .with_name(f"tb-rf3-{run_id}-ctrl")
                .with_network(network)
                .with_network_aliases("tb-ctrl")
                .with_env("KAFKA_NODE_ID", str(CONTROLLER_NODE_ID))
                .with_env("KAFKA_PROCESS_ROLES", "controller")
                .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", voters)
                .with_env("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:9093")
                .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT"),
                defaults,
                cluster_id,
            )
            cluster.controller = controller  # registered before start(): close() reaches it if start() raises
            controller.start()
        for node_id, port in ports.items():
            name = f"tb-rf3-{run_id}-k{node_id}"
            host = f"tb-k{node_id}"
            roles = "broker" if topology == "split" else "broker,controller"
            listeners = "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092"
            if topology == "combined":
                listeners += ",CONTROLLER://0.0.0.0:9093"
            container = _common_env(
                DockerContainer(KAFKA_IMAGE)
                .with_name(name)
                .with_network(network)
                .with_network_aliases(host)
                .with_env("KAFKA_NODE_ID", str(node_id))
                .with_env("KAFKA_PROCESS_ROLES", roles)
                .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", voters)
                .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .with_env(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
                )
                .with_env("KAFKA_LISTENERS", listeners)
                .with_env("KAFKA_ADVERTISED_LISTENERS", f"INTERNAL://{host}:9092,EXTERNAL://127.0.0.1:{port}")
                .with_bind_ports(19092, port),
                defaults,
                cluster_id,
            )
            brokers[node_id] = ClusterBroker(node_id, container, f"127.0.0.1:{port}", name)
            container.start()
        await_broker(cluster.bootstrap, expected_brokers=len(brokers), timeout_s=180.0)
    except BaseException:
        cluster.close()
        raise
    return cluster


@contextlib.contextmanager
def rf3_cluster(topology: Topology = "split") -> Iterator[Rf3Cluster]:
    cluster = start_cluster(topology)
    try:
        yield cluster
    finally:
        cluster.close()
