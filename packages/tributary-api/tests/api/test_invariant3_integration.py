"""Invariant 3: nothing in the data plane depends on the control plane. A real worker, started from the spec
the API deployed, keeps processing after the API process is stopped AND its PostgreSQL is stopped.

Real processes: the API (`python -m tributary.api`, JWKS from a static file under profile lite, ADR-009 D9 b),
a pipeline worker (`tests.support.processes.spawn_pipeline_worker`), and real Kafka and PostgreSQL containers.
The fault is shown to have fired (API process exited, PostgreSQL unreachable) before the second batch is sent,
and the worker is shown to be the same process throughout.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
from pathlib import Path

import httpx
import pytest
from api_kit import AUDIENCE, ROLE_MAP, Idp, TlsPg, bearer
from confluent_kafka.admin import NewTopic
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import EnvironmentRepository
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired, assert_no_acknowledged_loss
from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import (
    create_topics_with_retry,
    free_port,
    header_map,
    publish_messages,
    wait_for_at_least,
)
from tests.support.processes import ManagedProcess, spawn_pipeline_worker

pytestmark = pytest.mark.integration

ENV = "inv3"
CHANNEL = "inv3-adt"
DESTINATION = "http-stub"
BATCH = 20


def _batch(count: int) -> list[object]:
    return [make_message(b'{"synthetic": true}', key=f"SYNTH-{i}".encode(), channel=CHANNEL) for i in range(count)]


def test_worker_keeps_processing_with_the_api_and_postgresql_stopped(kafka: str, idp: Idp, tmp_path: Path) -> None:
    for topic, config in (
        (raw_topic(ENV, CHANNEL), {}),
        (out_topic(ENV, CHANNEL, DESTINATION), {}),
        (dlq_topic(ENV, CHANNEL), {}),
        (events_topic(ENV), {}),
        (channels_topic(ENV), {"cleanup.policy": "compact"}),
    ):
        create_topics_with_retry(kafka, [NewTopic(topic, 1, 1, config=config)])

    api: ManagedProcess | None = None
    worker: ManagedProcess | None = None
    pg = TlsPg(tmp_path)  # this test's own: it is stopped mid-test
    try:
        pg_url = pg.plain_url  # seeding only; the API connects with verify-full
        upgrade(pg_url)

        async def seed() -> None:
            engine = make_engine(pg_url)
            try:
                async with make_session_factory(engine)() as s, s.begin():
                    await EnvironmentRepository(s).create(ENV, "ui-managed")
            finally:
                await engine.dispose()

        asyncio.run(seed())

        port = free_port()
        env = engine_kafka_env(
            kafka,
            {
                **pg.env(pg.secrets_dir(tmp_path)),
                "TB_OIDC_ISSUER": idp.issuer,
                "TB_OIDC_AUDIENCE": AUDIENCE,
                "TB_OIDC_ROLE_MAP": json.dumps(ROLE_MAP),
                "TB_OIDC_JWKS_FILE": str(idp.jwks_file(tmp_path / "jwks.json")),
                "TB_PROFILE": "lite",
                "TB_API_BIND": "127.0.0.1",
                "TB_API_PORT": str(port),
                "PATH": "/usr/bin:/bin",
            },
        )
        api = ManagedProcess([sys.executable, "-m", "tributary.api"], env=env)
        api.wait_for_event("api_started", timeout_s=60)
        base = f"http://127.0.0.1:{port}"

        document = walking_skeleton_document(
            mllp_port=free_port(),
            http_url="http://127.0.0.1:18080/ingest",  # never dialled: no loader runs here
            channel=CHANNEL,
            destination=DESTINATION,
        )
        del document["metadata"]["version"]  # the server assigns it
        token = bearer(idp.mint(roles=["tb-admin"]))
        with httpx.Client(base_url=base, headers=token, timeout=30) as http:
            deadline = time.monotonic() + 30
            while http.get("/readyz").status_code != 200 and time.monotonic() < deadline:
                time.sleep(0.2)
            created = http.post("/v1/channels", json={"document": document, "origin": {"kind": "api"}})
            assert created.status_code == 201, created.text
            deployed = http.post(f"/v1/environments/{ENV}/channels/{CHANNEL}/deploy", json={"version": 1})
            assert deployed.status_code == 202 and deployed.json()["status"] == "published", deployed.text

        worker = spawn_pipeline_worker(
            env=ENV, channel=CHANNEL, bootstrap=kafka, instance_id="inv3-worker", timeout_s=60
        )
        worker_pid = worker.process.pid
        out = out_topic(ENV, CHANNEL, DESTINATION)

        first = _batch(BATCH)
        ids = publish_messages(kafka, raw_topic(ENV, CHANNEL), first)  # type: ignore[arg-type]
        wait_for_at_least(kafka, out, partitions=1, count=BATCH, timeout_s=90)

        # The fault: stop the API process, then PostgreSQL. Show both are really down.
        assert api.terminate_and_wait() is not None
        pg.stop()
        assert_fault_fired(api.process.poll() is not None, description="the API process is still running")
        with pytest.raises(OSError):
            import socket

            host_port = pg_url.split("@")[1].split("/")[0]
            socket.create_connection((host_port.split(":")[0], int(host_port.split(":")[1])), timeout=2).close()
        with pytest.raises(httpx.TransportError):
            httpx.get(f"{base}/livez", timeout=2)

        second = _batch(BATCH)
        ids += publish_messages(kafka, raw_topic(ENV, CHANNEL), second)  # type: ignore[arg-type]
        delivered = wait_for_at_least(kafka, out, partitions=1, count=2 * BATCH, timeout_s=90)
        assert_no_acknowledged_loss(set(ids), {header_map(r)["tb-msg-id"] for r in delivered})
        assert worker.process.pid == worker_pid and worker.process.poll() is None  # the same, still running
        assert not any(line.get("event") == "startup_refused" for line in worker.collect_pending())
    finally:
        for proc in (worker, api):
            if proc is not None:
                proc.terminate_and_wait()
        with contextlib.suppress(Exception):  # already stopped by the test body
            pg.stop()
