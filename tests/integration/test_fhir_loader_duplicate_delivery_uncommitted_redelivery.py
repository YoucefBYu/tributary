"""W2-9 duplicate-delivery harness, `fhir` destination: invariant 4 for the FHIR loader against
`contracts/channel-spec/v0/README.md` 5.1's bar for wave 2 exit criterion 3 -- *"deliver the same `out`
record twice -- the second as a genuine redelivery from an uncommitted offset, not a second call in the
same process -- and assert what the sink holds."*

One step-built ADT transaction Bundle (the golden output in `tests/fixtures/fhir/`) is published once to
`out.<dest>` and delivered through the real `python -m tributary.loaders.fhir` entry point to a **real
HAPI FHIR server** (`hapiproject/hapi`, testcontainers).

**The fault, and why it is genuine and hard.** The loader talks to HAPI through a small forwarding proxy
in this test. For the first attempt the proxy forwards the request to HAPI, lets HAPI apply the
transaction, then throws the response away and cuts the connection. HAPI has therefore *committed the
Patient and the Encounter* while the loader saw only a dropped connection (retryable `CONNECT_FAILED`),
did not commit its Kafka offset, and enters `ConsumerLoop`'s blocking retry. This is the worst duplicate
case there is (the sink applied it, the sender never learned). The retry delay is long enough that the
test observes HAPI holding the resources *before* the redelivery, then the proxy passes the second attempt
through. The assertion is that HAPI still holds exactly one Patient and one Encounter, and that the source
offset is committed only after that confirmed delivery.

**Broken-implementation control.** `test_the_same_replay_with_unconditional_posts_would_duplicate`
replays the same shape with a Bundle whose entries are plain `POST`s (what README 3.4.2 forbids) and shows
HAPI ends up with two Patients, so "exactly one" above is not something HAPI does on its own.
"""

from __future__ import annotations

import contextlib
import json
import socket
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
from tests.support.processes import spawn_fhir_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-fhir-duplicate-delivery"
DESTINATION = "fhir-server"
PARTITIONS = 3
GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "fhir" / "adt_a01_admit.bundle.json"
HAPI_IMAGE = "hapiproject/hapi:v7.4.0"
RETRY_DELAY_MS = 4000
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": RETRY_DELAY_MS,
    "maxDelayMs": RETRY_DELAY_MS,
    "maxAttempts": 10,
    "onExhaust": "halt-partition",
}
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class DroppingProxy:
    """Forwards to `upstream`; for the first `drop_first_n` requests it forwards, waits for the answer,
    then cuts the client connection without relaying it (the sink applied the write, the sender never
    learned). Bounded everywhere: every upstream call has a timeout, and it serves on a daemon thread."""

    def __init__(self, upstream: str, *, drop_first_n: int) -> None:
        self.bodies: list[bytes] = []
        self.dropped = 0
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
                if outer.dropped < drop_first_n:
                    outer.dropped += 1
                    self.close_connection = True
                    with contextlib.suppress(OSError):
                        self.request.shutdown(socket.SHUT_RDWR)
                    self.request.close()
                    return
                outer.passed += 1
                self.send_response(status)
                self.send_header("Content-Type", "application/fhir+json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self.server.server_address[1])
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> DroppingProxy:
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
    with _NO_PROXY.open(url, timeout=30) as resp:
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


def test_a_dropped_response_after_hapi_applied_the_write_redelivers_and_leaves_exactly_one_of_each(
    topics: str, hapi: str
) -> None:
    mrn = f"RDL{uuid.uuid4().hex[:10]}"
    patient_q = f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}"
    encounter_q = f"Encounter?identifier=https://tributary.dev/fhir/identifier/tb-msg-id|synth-{mrn}:encounter"

    with DroppingProxy(hapi, drop_first_n=1) as proxy:
        _deploy(topics, proxy.port, version=1)
        message = make_message(
            _golden(mrn),
            key=None,
            channel=CHANNEL,
            destination=DESTINATION,
            content_type="application/fhir+json",
        )
        published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
        assert len(published_ids) == 1

        loader = spawn_fhir_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics)
        try:
            paused = loader.wait_for_event("partition_paused", timeout_s=60.0)
            applied_before_redelivery = (_count(hapi, patient_q), _count(hapi, encounter_q))
            passed_before_redelivery = proxy.passed
            committed_during_fault = _committed_total(topics)
            assert_fault_fired(
                proxy.dropped == 1
                and paused.get("error_code") == "CONNECT_FAILED"
                and paused.get("retryable", True) is not False
                and applied_before_redelivery == (1, 1)
                and passed_before_redelivery == 0
                and committed_during_fault == 0,
                description=(
                    "expected HAPI to hold the resources while the loader saw only a dropped connection, "
                    f"got dropped={proxy.dropped} paused={paused!r} applied={applied_before_redelivery}"
                ),
            )

            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline and proxy.passed < 1:
                time.sleep(0.2)
            assert_fault_fired(proxy.passed == 1, description="the redelivery never reached HAPI")

        finally:
            loader.terminate_and_wait()  # commit_every is 50: the single record is committed on the graceful stop
        committed = _committed_total(topics)

    assert len(proxy.bodies) == 2 and proxy.bodies[0] == proxy.bodies[1] == message.payload, (
        "both attempts must carry the identical original Bundle: a replay of the same offset"
    )
    patients, encounters = _count(hapi, patient_q), _count(hapi, encounter_q)
    assert (patients, encounters) == (1, 1), "a redelivered Bundle must converge on one Patient and one Encounter"
    assert committed == 1, "the source offset must be committed only after HAPI's confirmed answer"
    print(
        f"fhir loader genuine-redelivery: 1 published, {proxy.dropped + proxy.passed} attempts, {patients} Patient(s) held"
    )


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
