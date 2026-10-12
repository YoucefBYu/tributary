"""W3-5 verify: a channel change reaches a RUNNING worker with no restart, through the real chain.

Real PostgreSQL (TLS), real Kafka, a real worker process, and the API built by `build_app` with its
reconciler loop on. The lost record is made by recording a deploy as `published` through a control plane whose
publisher drops the record, which is exactly the state a crash or an unknown-outcome timeout leaves behind.
The worker's PID is the same before and after, and its log shows `metadata.version` 1, then 2.

Also: the Kafka topic reader against a real compacted topic.
"""

from __future__ import annotations

import asyncio
import copy
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from api_kit import Contract, Idp, TlsPg, bearer, run
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from sqlalchemy import text
from test_deploy_kafka_integration import api_environ
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import EnvironmentRepository
from tributary.api.main import build_app
from tributary.api.reconciler import KafkaTopicReader, TopicReadError
from tributary.api.service import ControlPlane
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import create_topics_with_retry, free_port
from tests.support.processes import spawn_pipeline_worker

pytestmark = pytest.mark.integration

ENV = "hr"
CHANNEL = "hr-adt"
DESTINATION = "http-stub"


class DroppingPublisher:
    """Acknowledges and loses the record: PostgreSQL then says `published` while the topic never got it."""

    degraded = False

    async def publish(self, env: str, channel: str, value: bytes | None) -> None:
        return None


def seed(pg_url: str, mode: str = "ui-managed") -> None:
    upgrade(pg_url)

    async def go() -> None:
        engine = make_engine(pg_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE deployments, channel_versions, channels, environments RESTART IDENTITY CASCADE")
                )
            async with make_session_factory(engine)() as s, s.begin():
                await EnvironmentRepository(s).create(ENV, mode)
        finally:
            await engine.dispose()

    asyncio.run(go())


def applied_versions(worker: Any) -> list[int]:
    worker.collect_pending()
    return [
        line["channel_version"]
        for line in worker.lines
        if line.get("event") == "channel_spec_applied" and line.get("channel") == CHANNEL
    ]


def test_a_reconciled_channel_change_reaches_a_running_worker_without_a_restart(
    tls_pg: TlsPg, kafka: str, idp: Idp, contract: Contract, tmp_path: Path
) -> None:
    for topic, config in (
        (raw_topic(ENV, CHANNEL), {}),
        (out_topic(ENV, CHANNEL, DESTINATION), {}),
        (dlq_topic(ENV, CHANNEL), {}),
        (events_topic(ENV), {}),
        (channels_topic(ENV), {"cleanup.policy": "compact"}),
    ):
        create_topics_with_retry(kafka, [NewTopic(topic, 1, 1, config=config)])
    seed(tls_pg.plain_url)
    document = walking_skeleton_document(
        mllp_port=free_port(), http_url="http://127.0.0.1:18080/ingest", channel=CHANNEL, destination=DESTINATION
    )
    del document["metadata"]["version"]  # the server assigns it
    changed = copy.deepcopy(document)
    changed["spec"]["pipeline"]["stepRetries"] = 4  # a real spec change: the worker must apply version 2

    app = build_app(
        api_environ(
            tls_pg,
            tls_pg.secrets_dir(tmp_path),
            idp,
            kafka,
            idp.jwks_file(tmp_path / "jwks.json"),
            TB_RECONCILE_INTERVAL_S="1",
        )
    )
    worker = None

    async def go() -> None:
        nonlocal worker
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            headers = bearer(idp.mint(roles=["tb-admin"]))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                created = await c.post(
                    "/v1/channels", json={"document": document, "origin": {"kind": "api"}}, headers=headers
                )
                assert created.status_code == 201, created.text
                added = await c.post(
                    f"/v1/channels/{CHANNEL}/versions",
                    json={"document": changed, "origin": {"kind": "api"}},
                    headers=headers,
                )
                assert added.status_code == 201, added.text
                v1 = await c.post(
                    f"/v1/environments/{ENV}/channels/{CHANNEL}/deploy", json={"version": 1}, headers=headers
                )
                assert v1.status_code == 202 and v1.json()["status"] == "published", v1.text

                worker = await asyncio.to_thread(
                    spawn_pipeline_worker, env=ENV, channel=CHANNEL, bootstrap=kafka, instance_id="hr-worker"
                )
                pid = worker.process.pid
                assert applied_versions(worker) == [1]

                # PostgreSQL records v2 as published; the topic never receives it (the lost record).
                engine = make_engine(tls_pg.plain_url)
                try:
                    lost = ControlPlane(
                        make_session_factory(engine), DroppingPublisher(), JsonLogger("t", level="error")
                    )
                    d = await lost.deploy(ENV, CHANNEL, 2, 1, "hr-test")
                    assert d["status"] == "published"
                finally:
                    await engine.dispose()

                # The API's own reconciler loop (1 s) repairs the topic; the worker applies it.
                await asyncio.to_thread(wait_for_version, worker, 2)
                assert worker.process.pid == pid and worker.process.poll() is None  # no restart
                assert applied_versions(worker) == [1, 2]

    try:
        run(go())
    finally:
        if worker is not None:
            worker.terminate_and_wait()


def wait_for_version(worker: Any, version: int, timeout_s: float = 60.0) -> None:
    """Block on the worker's own log events until it reports applying `version`."""
    deadline = time.monotonic() + timeout_s
    while version not in applied_versions(worker):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"worker did not apply version {version} within {timeout_s} s")
        worker.wait_for_event("channel_spec_applied", timeout_s=remaining)


def test_the_topic_reader_returns_the_last_record_per_key_and_tombstones(kafka: str) -> None:
    env = f"rd{uuid.uuid4().hex[:6]}"
    topic = channels_topic(env)
    create_topics_with_retry(kafka, [NewTopic(topic, 2, 1, config={"cleanup.policy": "compact"})])
    producer = Producer({"bootstrap.servers": kafka, "enable.idempotence": True})
    for key, value in (("a", b"1"), ("a", b"2"), ("b", b"x"), ("b", None), ("c", b"3")):
        producer.produce(topic, key=key.encode(), value=value)
    assert producer.flush(30) == 0
    from confluent_kafka import Consumer

    reader = KafkaTopicReader(
        lambda: Consumer({"bootstrap.servers": kafka, "group.id": f"rd-{uuid.uuid4()}", "enable.auto.commit": False})
    )
    assert reader.snapshot(env) == {"a": b"2", "b": None, "c": b"3"}
    empty = f"em{uuid.uuid4().hex[:6]}"
    create_topics_with_retry(kafka, [NewTopic(channels_topic(empty), 1, 1, config={"cleanup.policy": "compact"})])
    assert reader.snapshot(empty) == {}
    with pytest.raises(TopicReadError):
        reader.snapshot("no-such-env")


def test_stop_ends_a_snapshot_in_progress_promptly(kafka: str) -> None:
    """A reader polling a topic that never reaches its end offset (a record the consumer cannot see because
    the watermark is ahead) returns within a poll slice once stopped, not after `timeout_s`."""
    import threading

    from confluent_kafka import Consumer

    env = f"st{uuid.uuid4().hex[:6]}"
    topic = channels_topic(env)
    create_topics_with_retry(kafka, [NewTopic(topic, 1, 1, config={"cleanup.policy": "compact"})])
    producer = Producer({"bootstrap.servers": kafka, "enable.idempotence": True})
    producer.produce(topic, key=b"a", value=b"1")
    assert producer.flush(30) == 0
    reader = KafkaTopicReader(
        lambda: Consumer({"bootstrap.servers": kafka, "group.id": f"st-{uuid.uuid4()}", "enable.auto.commit": False}),
        timeout_s=60.0,
    )
    reader.stop()  # before the read: refuses at once
    with pytest.raises(TopicReadError):
        reader.snapshot(env)

    # Mid-read: a consumer whose polls never deliver, so only the stop flag can end the loop.
    class Silent:
        def list_topics(self, *_a: Any, **_k: Any) -> Any:
            return Consumer({"bootstrap.servers": kafka, "group.id": "x"}).list_topics(topic, timeout=10)

        def get_watermark_offsets(self, *_a: Any, **_k: Any) -> tuple[int, int]:
            return 0, 5

        def assign(self, *_a: Any) -> None: ...

        def poll(self, _t: float) -> None:
            polling.set()
            return None

        def close(self) -> None: ...

    polling = threading.Event()
    live = KafkaTopicReader(Silent, timeout_s=60.0)
    outcome: list[float] = []

    def read() -> None:
        started = time.monotonic()
        with pytest.raises(TopicReadError):
            live.snapshot(env)
        outcome.append(time.monotonic() - started)

    t = threading.Thread(target=read)
    t.start()
    assert polling.wait(30)  # it is inside the poll loop
    live.stop()
    t.join(10)
    assert outcome and outcome[0] < 5.0
