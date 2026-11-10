"""W3-25 verify line against a real broker: scrape a running worker's `/metrics`, force a partition halt, redeploy.

Marked `integration`: it needs Docker. `uv run pytest -m integration packages/tributary-runtime -k ops`.

1. **The scrape of a real worker agrees with `metrics.json`** in both directions, after real traffic (the same
   `contract_diff` the unit test uses), and `/livez` is 200 while `/readyz` is 404 (`observability/v0` 1).
2. **A forced halt scrapes `tb_partitions_halted{...} 1`.** The fault is a 1,000,001-byte `raw` record: the broker
   accepts it, no `out` or `dlq` projection fits, and the worker halts that partition (`TB_DLQ_UNWRITABLE`). The
   test proves the fault fired (a `partition_halted` event on `_tb.<env>.events`) and that the other partition
   still flows, so the gauge is read on a worker that is alive, not one that died.
3. **A redeploy moves `tb_spec_version`** with no restart and no traffic (the loop's idle tick applies it).
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.ops import OpsSettings
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import Message, step
from tributary.sdk.ids import new_traceparent, uuid7
from tributary.sdk.projection import MAX_INLINE_PAYLOAD_BYTES
from tributary.sdk.testing import make_message

from .ops_support import Exposition, parse_exposition
from .test_integration_kafka import CONTRACTS, ENV, drain, header_map, kafka  # noqa: F401 - kafka is a fixture
from .test_worker_metrics import DOCUMENT, contract_diff

pytestmark = pytest.mark.integration

CHANNEL = "ops-probe"
DESTINATION = "http-stub"
PARTITIONS = 2
BASE = {"component": "worker", "channel": CHANNEL}


@step
def passthrough(message: Message) -> None:
    return None


def spec_document(version: int) -> dict[str, Any]:
    document = json.loads(
        (CONTRACTS / "channel-spec" / "v0" / "examples" / "valid" / "walking-skeleton-loopback.json").read_text(
            encoding="utf-8"
        )
    )
    document["metadata"] = {**document["metadata"], "name": CHANNEL, "version": version}
    return document


def deploy(bootstrap: str, version: int) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(spec_document(version)).encode())
    assert producer.flush(30) == 0


def publish(bootstrap: str, payload: bytes, *, partition: int) -> str:
    # librdkafka's own `message.max.bytes` default (1,000,000) would refuse the oversize record client-side.
    producer = Producer(
        {"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True, "message.max.bytes": 1_048_576}
    )
    message = make_message(payload, key=b"SYNTH-KEY-1", channel=CHANNEL, msg_id=uuid7(), traceparent=new_traceparent())
    producer.produce(
        raw_topic(ENV, CHANNEL),
        key=message.key,
        value=message.payload,
        partition=partition,
        headers=[(name, value.encode()) for name, value in message.headers.items()],
    )
    assert producer.flush(30) == 0
    return message.msg_id


@pytest.fixture(scope="module")
def topics(kafka: str) -> str:  # noqa: F811 - the imported fixture
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
    deploy(kafka, 1)
    return kafka


@pytest.fixture(scope="module")
def running(topics: str) -> Iterator[tuple[PipelineWorker, int]]:
    """A real worker, in-process on a thread, with its ops listener on an ephemeral loopback port."""
    worker = PipelineWorker(
        settings=WorkerSettings(
            env=ENV,
            channel=CHANNEL,
            kafka=KafkaSettings(bootstrap_servers=topics),
            instance_id="ops-probe-0",
            poll_timeout_s=0.1,
            ops=OpsSettings("127.0.0.1", 0),
        ),
        steps_override=(passthrough,),
        defaults=load_topic_defaults(),
    )
    worker.build()
    assert worker.ops is not None
    port = worker.ops.server.port
    thread = threading.Thread(target=worker.run, name="worker", daemon=True)
    thread.start()
    try:
        yield worker, port
    finally:
        worker.stop()
        thread.join(60)
    assert not thread.is_alive(), "the worker drained and stopped"


def get(port: int, path: str) -> tuple[int, bytes, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
            return response.status, response.read(), response.headers.get("Content-Type", "")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), exc.headers.get("Content-Type", "")


def scrape(port: int) -> Exposition:
    status, body, content_type = get(port, "/metrics")
    assert status == 200 and content_type.startswith("text/plain; version=0.0.4")
    return parse_exposition(body.decode("utf-8"))


def wait_for(port: int, check: Callable[[Exposition], bool], *, timeout_s: float = 60.0) -> Exposition:
    deadline = time.monotonic() + timeout_s
    exposition = scrape(port)
    while not check(exposition):
        if time.monotonic() > deadline:
            raise AssertionError("the scrape never showed the expected value")
        time.sleep(0.25)
        exposition = scrape(port)
    return exposition


def handled(exposition: Exposition) -> float:
    return exposition.value("tb_worker_records_total", channel=CHANNEL, outcome="handled") or 0.0


def test_a_real_worker_s_scrape_agrees_with_the_registry_in_both_directions(
    running: tuple[PipelineWorker, int], topics: str
) -> None:
    _worker, port = running
    for partition in range(PARTITIONS):
        publish(topics, b'{"synthetic": true}', partition=partition)
    exposition = wait_for(port, lambda e: handled(e) >= PARTITIONS)
    assert contract_diff(exposition, DOCUMENT, "worker") == []
    assert exposition.value("tb_messages_total", **BASE, stage="routed", status="ok", destination=DESTINATION) == 2
    assert exposition.value("tb_stage_duration_seconds_count", **BASE, stage="transformed", destination="") == 2
    assert exposition.value("tb_partitions_halted", **BASE, destination="") == 0
    assert get(port, "/livez")[:2] == (200, b'{"v":0,"status":"alive","component":"worker"}')
    assert get(port, "/readyz")[:2] == (404, b""), "no worker /readyz in observability/v0"
    assert "SYNTH" not in get(port, "/metrics")[1].decode()


def test_a_forced_halt_scrapes_tb_partitions_halted_1_and_the_other_partition_still_flows(
    running: tuple[PipelineWorker, int], topics: str
) -> None:
    _worker, port = running
    before = handled(scrape(port))
    halted_id = publish(topics, b"x" * (MAX_INLINE_PAYLOAD_BYTES + 1), partition=0)
    exposition = wait_for(port, lambda e: e.value("tb_partitions_halted", **BASE, destination="") == 1)
    assert exposition.value("tb_messages_total", **BASE, stage="partition_halted", status="error", destination="") == 1

    # The fault fired: the worker said so on the events topic, with the registered code.
    events = [json.loads(raw.value()) for raw in drain(topics, events_topic(ENV), expected=1, timeout_s=20)]
    halts = [e for e in events if e.get("stage") == "partition_halted" and e.get("channel") == CHANNEL]
    assert [(e["partition"], e["error_code"], e.get("message_id")) for e in halts] == [
        (0, "TB_DLQ_UNWRITABLE", halted_id)
    ]
    # Alive, not dead: partition 1 is still consumed while partition 0 is held.
    publish(topics, b'{"synthetic": true}', partition=1)
    after = wait_for(port, lambda e: handled(e) >= before + 1)
    assert after.value("tb_partitions_halted", **BASE, destination="") == 1
    assert get(port, "/livez")[0] == 200
    out = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=1, timeout_s=10)
    assert all(header_map(raw).get("tb-msg-id") != halted_id for raw in out), "the halted record was not produced"


def test_a_redeploy_moves_tb_spec_version_with_no_restart_and_no_traffic(
    running: tuple[PipelineWorker, int], topics: str
) -> None:
    worker, port = running
    loop = worker.loop
    assert loop is not None
    start = scrape(port).value("tb_spec_version", **BASE)
    assert start is not None
    deploy(topics, int(start) + 1)
    exposition = wait_for(port, lambda e: e.value("tb_spec_version", **BASE) == start + 1, timeout_s=30)
    assert exposition.value("tb_spec_source_unproven", **BASE) == 0
    assert worker.loop is loop, "same loop: no restart"
