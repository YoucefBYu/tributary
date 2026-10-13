"""Wave 3 exit criterion 3: deploy v2, then roll back, and a RUNNING worker applies version 2 and then version 1
under the same PID. Real PostgreSQL (TLS), real Kafka, a real worker process, and the API built by `build_app`
(the production wiring: real channels publisher, real audit producer). The reconciler is off, so what the worker
sees is what the deploy and rollback paths themselves published to `_tb.<env>.channels`.

Two variants: through the HTTP API, and through the CLI (`tributary deploy` / `rollback`) against the same API
served over loopback.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from api_kit import Contract, Idp, TlsPg, bearer, run
from archive_kit import free_port, restore_uvicorn_logging
from confluent_kafka.admin import NewTopic
from test_deploy_kafka_integration import api_environ, seed
from tributary.api.main import build_app
from tributary.cli import main as cli
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, platform_topic, raw_topic
from typer.testing import CliRunner

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import create_topics_with_retry
from tests.support.kafka_helpers import free_port as kafka_free_port
from tests.support.processes import ManagedProcess, spawn_pipeline_worker

pytestmark = pytest.mark.integration

DESTINATION = "http-stub"


def fresh() -> tuple[str, str]:
    tag = uuid.uuid4().hex[:6]
    return f"hr{tag}", f"hr-{tag}"


def make_topics(kafka: str, env: str, channel: str) -> None:
    for topic, config in (
        (raw_topic(env, channel), {}),
        (out_topic(env, channel, DESTINATION), {}),
        (dlq_topic(env, channel), {}),
        (events_topic(env), {}),
        (channels_topic(env), {"cleanup.policy": "compact"}),
        (platform_topic(env, "audit"), {}),
    ):
        create_topics_with_retry(kafka, [NewTopic(topic, 1, 1, config=config)])


def documents(channel: str) -> tuple[dict[str, Any], dict[str, Any]]:
    v1 = walking_skeleton_document(
        mllp_port=kafka_free_port(), http_url="http://127.0.0.1:18080/ingest", channel=channel, destination=DESTINATION
    )
    del v1["metadata"]["version"]  # the server assigns it
    v2 = copy.deepcopy(v1)
    v2["spec"]["pipeline"]["stepRetries"] = 4  # a real spec change
    return v1, v2


def applied(worker: ManagedProcess, channel: str) -> list[int]:
    worker.collect_pending()
    return [
        line["channel_version"]
        for line in worker.lines
        if line.get("event") == "channel_spec_applied" and line.get("channel") == channel
    ]


def wait_for_sequence(worker: ManagedProcess, channel: str, sequence: list[int], timeout_s: float = 60.0) -> None:
    """Block on the worker's own log until it has applied exactly `sequence` (version 1 appears twice: after the
    rollback it is a new application, not the first one)."""
    deadline = time.monotonic() + timeout_s
    while applied(worker, channel) != sequence:
        if time.monotonic() > deadline or len(applied(worker, channel)) > len(sequence):
            raise AssertionError(f"worker applied {applied(worker, channel)}, wanted {sequence}")
        time.sleep(0.2)


def build(tls_pg: TlsPg, kafka: str, idp: Idp, tmp_path: Path) -> Any:
    return build_app(
        api_environ(
            tls_pg,
            tls_pg.secrets_dir(tmp_path),
            idp,
            kafka,
            idp.jwks_file(tmp_path / "jwks.json"),
            TB_RECONCILE_INTERVAL_S="0",  # off: only the deploy and rollback paths publish
        )
    )


def test_deploy_v2_then_rollback_reaches_a_running_worker_through_the_api(
    tls_pg: TlsPg, kafka: str, idp: Idp, contract: Contract, tmp_path: Path
) -> None:
    env, channel = fresh()
    make_topics(kafka, env, channel)
    seed(tls_pg.plain_url, env)
    v1, v2 = documents(channel)
    app = build(tls_pg, kafka, idp, tmp_path)
    worker: ManagedProcess | None = None

    async def go() -> None:
        nonlocal worker
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            headers = bearer(idp.mint(roles=["tb-admin"]))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                for doc in (v1, v2):
                    r = await c.post("/v1/channels" if doc is v1 else f"/v1/channels/{channel}/versions",
                                     json={"document": doc, "origin": {"kind": "api"}}, headers=headers)  # fmt: skip
                    assert r.status_code == 201, r.text
                base = f"/v1/environments/{env}/channels/{channel}"
                first = await c.post(f"{base}/deploy", json={"version": 1}, headers=headers)
                assert first.status_code == 202 and first.json()["status"] == "published", first.text

                worker = await asyncio.to_thread(
                    spawn_pipeline_worker, env=env, channel=channel, bootstrap=kafka, instance_id=f"{env}-worker"
                )
                pid = worker.process.pid
                await asyncio.to_thread(wait_for_sequence, worker, channel, [1])

                second = await c.post(
                    f"{base}/deploy", json={"version": 2, "expected_current_version": 1}, headers=headers
                )
                assert second.status_code == 202 and second.json()["status"] == "published", second.text
                await asyncio.to_thread(wait_for_sequence, worker, channel, [1, 2])
                assert worker.process.pid == pid and worker.process.poll() is None  # applied v2, not restarted

                back = await c.post(f"{base}/rollback", json={"expected_current_version": 2}, headers=headers)
                assert back.status_code == 202 and back.json()["status"] == "published", back.text
                assert back.json()["version"] == 1 and back.json()["previous_version"] == 2
                await asyncio.to_thread(wait_for_sequence, worker, channel, [1, 2, 1])
                assert worker.process.pid == pid and worker.process.poll() is None  # applied v1 again, same process

    try:
        run(go())
    finally:
        if worker is not None:
            worker.terminate_and_wait()


def test_deploy_v2_then_rollback_reaches_a_running_worker_through_the_cli(
    tls_pg: TlsPg, kafka: str, idp: Idp, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env, channel = fresh()
    make_topics(kafka, env, channel)
    seed(tls_pg.plain_url, env)
    v1, v2 = documents(channel)
    files = []
    for n, doc in enumerate((v1, v2), start=1):
        path = tmp_path / f"v{n}.json"
        path.write_text(json.dumps(doc))
        files.append(path)

    app = build(tls_pg, kafka, idp, tmp_path)
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, access_log=False, log_config=None))
    thread = threading.Thread(target=server.run, daemon=True)
    base = f"http://127.0.0.1:{port}"
    monkeypatch.setenv("TRIBUTARY_API_URL", base)
    runner = CliRunner()
    worker: ManagedProcess | None = None

    def tributary(*args: str) -> str:
        monkeypatch.setenv("TRIBUTARY_TOKEN", idp.mint(roles=["tb-admin"]))  # fresh for each call (exp is 300 s)
        result = runner.invoke(cli.app, list(args))
        assert result.exit_code == 0, (args, result.exit_code, result.output)
        return result.output

    thread.start()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with contextlib.suppress(httpx.HTTPError):
                if httpx.get(f"{base}/readyz", timeout=2).status_code == 200:
                    break
            time.sleep(0.2)
        else:
            raise RuntimeError("the API under test did not become ready")

        assert "v1 (created)" in tributary("channels", "push", str(files[0]))
        assert "v2 (added)" in tributary("channels", "push", str(files[1]))
        assert "v1 published" in tributary("deploy", env, channel, "1", "--expect", "none")

        worker = spawn_pipeline_worker(env=env, channel=channel, bootstrap=kafka, instance_id=f"{env}-worker")
        pid = worker.process.pid
        wait_for_sequence(worker, channel, [1])

        assert "v2 published" in tributary("deploy", env, channel, "2", "--expect", "1")
        wait_for_sequence(worker, channel, [1, 2])
        assert worker.process.pid == pid and worker.process.poll() is None

        assert "v1 published" in tributary("rollback", env, channel, "--expect", "2")
        wait_for_sequence(worker, channel, [1, 2, 1])
        assert worker.process.pid == pid and worker.process.poll() is None
    finally:
        if worker is not None:
            worker.terminate_and_wait()
        server.should_exit = True
        thread.join(15)
        assert not thread.is_alive()
        restore_uvicorn_logging()
