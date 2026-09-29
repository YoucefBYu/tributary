"""W2-9 duplicate-delivery harness, `fhir` destination: invariant 4 for the FHIR loader against
`contracts/channel-spec/v0/README.md` 5.1's bar for wave 2 exit criterion 3 -- *"deliver the same `out`
record twice -- the second as a genuine redelivery from an uncommitted offset, not a second call in the
same process -- and assert what the sink holds."*

**The fault, and why it is genuine.** One step-built ADT transaction Bundle (the golden output in
`tests/fixtures/fhir/`) is published once to `out.<dest>`. The real `python -m tributary.loaders.fhir`
process delivers it to a **real HAPI FHIR server** (`hapiproject/hapi`, testcontainers), through a small
transparent forwarding proxy that only records what passes (the loader refuses plaintext `http://` to a
non-loopback host, and the container may not be on loopback). HAPI applies the transaction. The test then
**SIGKILLs the loader before it commits its Kafka offset** (`commit_every` is 50, so a single record is
only committed at a graceful stop): the sink applied the write, the source offset did not move, which is
the at-least-once crash window in its purest form. A second loader process, same static instance id,
resumes from the uncommitted offset and receives the *same record* again. This replaces an earlier
version that dropped the first response and relied on the loader's in-process blocking retry, which
is a second call in the same process and does not meet README 5.1.

The assertions: HAPI held exactly one Patient and one Encounter and the group's committed offset was still
0 when the first loader died (the fault fired); the replayed delivery reached HAPI as an identical second
request and the second loader absorbed it as a duplicate (its own `delivery_duplicate` counter, which is
"the server created nothing", not an inference from the resource counts); HAPI still holds exactly one
of each; and the offset is committed only after that confirmed delivery.

**Broken-implementation control.** `test_the_same_replay_with_unconditional_posts_would_duplicate`
replays the same shape with a Bundle whose entries are plain `POST`s (what README 3.4.2 forbids) and shows
HAPI ends up with two Patients, so "exactly one" above is not something HAPI does on its own.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import ManagedProcess, spawn_fhir_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-fhir-duplicate-delivery"
DESTINATION = "fhir-server"
PARTITIONS = 3
GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "fhir" / "adt_a01_admit.bundle.json"
HAPI_IMAGE = "hapiproject/hapi:v7.4.0"
INSTANCE = "loader-fhir-replay"  # static membership: the second process resumes the first one's partitions
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 500,
    "maxDelayMs": 500,
    "maxAttempts": 10,
    "onExhaust": "halt-partition",
}
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class ForwardingProxy:
    """Forwards every POST to `upstream` and relays the answer unchanged, recording the request bodies.
    Bounded everywhere: every upstream call has a timeout, and it serves on a daemon thread."""

    def __init__(self, upstream: str) -> None:
        self.bodies: list[bytes] = []
        self.passed = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                pass

            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                outer.bodies.append(body)
                request = urllib.request.Request(  # noqa: S310
                    upstream, data=body, method="POST", headers={"Content-Type": "application/fhir+json"}
                )
                try:
                    with _NO_PROXY.open(request, timeout=30) as resp:
                        status, payload = resp.status, resp.read()
                except urllib.error.HTTPError as exc:
                    status, payload = exc.code, exc.read()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/fhir+json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except OSError:  # the loader was SIGKILLed while HAPI was answering
                    return
                outer.passed += 1

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self.server.server_address[1])
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> ForwardingProxy:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()


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
        .with_env("CLUSTER_ID", "dup-delivery-fhir")
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
def hapi() -> Iterator[str]:
    container = (
        DockerContainer(HAPI_IMAGE)
        .with_exposed_ports(8080)
        .waiting_for(LogMessageWaitStrategy("Started Application in").with_startup_timeout(240))
    )
    with container:
        yield f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8080)}/fhir"


def _deploy(topics: str, proxy_port: int, *, version: int) -> None:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run
        channel=CHANNEL,
        destination=DESTINATION,
        version=version,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "fhir",
        "with": {
            "url": f"http://127.0.0.1:{proxy_port}/fhir",
            "fhirVersion": "4.0.1",
            "interaction": "transaction",
            "timeoutMs": 10000,
        },
        "idempotency": {"technique": "conditional-update"},
        "retry": dict(RETRY_OVERRIDE),
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0


def _count(base: str, query: str) -> int:
    url = f"{base}/{urllib.parse.quote(query, safe='?=&:/')}&_summary=count"
    # HAPI reuses a cached search result for 60 s by default, so a poll would keep reading the first (empty)
    # answer; `no-cache` makes every count a fresh search.
    request = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})  # noqa: S310
    with _NO_PROXY.open(request, timeout=30) as resp:
        return int(json.loads(resp.read())["total"])


def _golden(mrn: str) -> bytes:
    text = GOLDEN.read_text(encoding="utf-8").replace("9000001", mrn)
    return text.replace("0199036e-7a3e-7f00-8c21-abc123456789", f"synth-{mrn}").encode()


def _committed_total(bootstrap: str) -> int:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": consumer_group(ENV, CHANNEL, "loader", DESTINATION)}
    )
    try:
        partitions = [TopicPartition(out_topic(ENV, CHANNEL, DESTINATION), p) for p in range(PARTITIONS)]
        return sum(max(tp.offset, 0) for tp in consumer.committed(partitions, timeout=10))
    finally:
        consumer.close()


def _spawn(topics: str) -> ManagedProcess:
    return spawn_fhir_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics, instance_id=INSTANCE)


def test_a_loader_killed_after_hapi_applied_the_bundle_and_before_the_commit_replays_into_one_of_each(
    topics: str, hapi: str
) -> None:
    mrn = f"RDL{uuid.uuid4().hex[:10]}"
    patient_q = f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}"
    encounter_q = f"Encounter?identifier=https://tributary.dev/fhir/identifier/tb-msg-id|synth-{mrn}:encounter"

    with ForwardingProxy(hapi) as proxy:
        _deploy(topics, proxy.port, version=1)
        message = make_message(
            _golden(mrn),
            key=None,
            channel=CHANNEL,
            destination=DESTINATION,
            content_type="application/fhir+json",
        )
        assert len(publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])) == 1

        first = _spawn(topics)
        try:
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline and _count(hapi, patient_q) < 1:
                time.sleep(0.2)
        finally:
            first.kill_and_wait()  # SIGKILL: no graceful commit
        applied_at_kill = (_count(hapi, patient_q), _count(hapi, encounter_q))
        committed_after_kill = _committed_total(topics)
        assert_fault_fired(
            applied_at_kill == (1, 1) and committed_after_kill == 0 and len(proxy.bodies) >= 1,
            description=(
                "expected HAPI to hold the resources and the offset to be uncommitted when the loader died, "
                f"got applied={applied_at_kill} committed={committed_after_kill}"
            ),
        )
        requests_before_replay = len(proxy.bodies)

        second = _spawn(topics)
        try:
            second.wait_for_event("partitions_assigned", timeout_s=60.0)
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline and len(proxy.bodies) <= requests_before_replay:
                time.sleep(0.2)
            while time.monotonic() < deadline and proxy.passed < requests_before_replay + 1:
                time.sleep(0.2)  # HAPI's answer relayed: the loader now has the confirmation to commit on
            assert_fault_fired(len(proxy.bodies) > requests_before_replay, description="the replay never reached HAPI")
        finally:
            second.terminate_and_wait()  # commit_every is 50: the record is committed on the graceful stop
        second.collect_pending()
        exits = [line for line in second.lines if line.get("event") == "loader_exit"]
        metrics = exits[-1]["delivery_metrics"] if exits else {}
        committed = _committed_total(topics)

    assert_fault_fired(
        metrics.get("delivery_duplicate") == 1,
        description=f"the replay was not absorbed as a duplicate by the second loader: {metrics!r}",
    )
    assert proxy.bodies[0] == proxy.bodies[-1] == message.payload, "the replay carries the identical original Bundle"
    patients, encounters = _count(hapi, patient_q), _count(hapi, encounter_q)
    assert (patients, encounters) == (1, 1), "a redelivered Bundle must converge on one Patient and one Encounter"
    assert committed == 1, "the source offset must be committed only after HAPI's confirmed answer"
    print(f"fhir loader genuine-redelivery: 1 published, {len(proxy.bodies)} requests, {patients} Patient(s) held")


def test_the_same_replay_with_unconditional_posts_would_duplicate(hapi: str) -> None:
    """Broken-implementation control (module docstring): plain `POST` entries, delivered twice, leave two
    Patients on the same real server."""
    mrn = f"CTL{uuid.uuid4().hex[:10]}"
    document = json.loads(_golden(mrn))
    document["entry"] = [
        {"request": {"method": "POST", "url": "Patient"}, "resource": document["entry"][0]["resource"]}
    ]
    for _ in range(2):
        request = urllib.request.Request(  # noqa: S310
            hapi, data=json.dumps(document).encode(), method="POST", headers={"Content-Type": "application/fhir+json"}
        )
        with _NO_PROXY.open(request, timeout=30):
            pass
    assert _count(hapi, f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}") == 2
