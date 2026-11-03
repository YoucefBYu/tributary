"""W3-17 against a real broker, one real receiver process per type (MLLP, HTTP, SFTP, Kafka source): the ops
listener answers `/livez` at once, `/readyz` is 503 before the first spec, stays 503 `raw_producer: not_warmed`
while the first `raw` metadata round trip cannot succeed (the raw topic does not exist yet) with the ingest side
not started, turns 200 once it can, goes 503 `undeployed` on a tombstone, and a bind conflict on the ops port
exits 2. Synthetic data only.

`uv run pytest -m integration packages/tributary-receivers -k readiness` (Docker).
"""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import raw_topic

from tests.support.kafka_helpers import create_topics_with_retry, free_port
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import http_document, kafka_source_document, sftp_document

from .readiness_rig import await_ready, get, ops_port, readyz, reasons, scrape

pytestmark = pytest.mark.integration

ENV = "rdytest"
LOG_SCHEMA = json.loads(
    (Path(__file__).resolve().parents[4] / "contracts" / "observability" / "v0" / "log-line.schema.json").read_text(
        encoding="utf-8"
    )
)


def mllp_document(channel: str, port: int, version: int = 1) -> dict[str, Any]:
    from tests.support.channel_spec import walking_skeleton_document

    doc = walking_skeleton_document(
        mllp_port=port, http_url="http://127.0.0.1:1/x", channel=channel, destination="stub", version=version
    )
    doc["spec"]["source"]["bind"] = "127.0.0.1"
    return doc


def build(
    kind: str, channel: str, tmp_path: Path, kafka: str
) -> tuple[Callable[[int], dict[str, Any]], int, Path | None]:
    """`(document for a version, ingest port or 0, secrets dir or None)`."""
    if kind == "mllp":
        port = free_port()
        return (lambda v: mllp_document(channel, port, v)), port, None
    if kind == "http":
        port = free_port()
        return (lambda v: http_document(channel, version=v, port=port)), port, None
    if kind == "sftp":
        cred = tmp_path / "secrets" / "sftp-cred"
        cred.mkdir(parents=True)
        (cred / "username").write_text("synth", encoding="utf-8")
        (cred / "password").write_text("synth-pass", encoding="utf-8")  # tb-secret-scan: allow
        return (
            (lambda v: sftp_document(channel, version=v, port=1, host="127.0.0.1", poll_interval_s=1.0)),
            0,
            tmp_path / "secrets",
        )
    source = f"upstream.{channel}"
    create_topics_with_retry(kafka, [NewTopic(source, 1, 1)])
    return (lambda v: kafka_source_document(channel, version=v, bootstrap=kafka, topic=source)), 0, None


MODULES = {
    "mllp": "tributary.receivers.mllp",
    "http": "tributary.receivers.http",
    "sftp": "tributary.receivers.sftp",
    "kafka": "tributary.receivers.kafka_source",
}


@pytest.fixture
def procs() -> Iterator[list[ManagedProcess]]:
    started: list[ManagedProcess] = []
    yield started
    for proc in started:
        proc.terminate_and_wait()


def refused(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return False
    except OSError:
        return True


@pytest.mark.parametrize("kind", ["mllp", "http", "sftp", "kafka"])
def test_not_ready_before_the_first_spec_and_the_warm_up_then_ready_then_undeployed(
    kind: str, kafka: str, procs: list[ManagedProcess], tmp_path: Path
) -> None:
    ensure_channels_topic(kafka, ENV)
    channel = f"rdy-{kind}"
    document, ingest_port, secrets = build(kind, channel, tmp_path, kafka)
    proc = start_receiver(
        MODULES[kind], channel=channel, env=ENV, bootstrap=kafka, instance=f"{channel}-0", secrets_dir=secrets
    )
    procs.append(proc)
    port = ops_port(proc)

    # 1. No spec: alive, not ready. The fault fired: the reason says there is no spec.
    assert get(port, "/livez") == (200, {"v": 0, "status": "alive", "component": "receiver"})
    status, doc = readyz(port)
    assert status == 503 and reasons(doc)["spec"] == "no_spec" and doc["receiver_type"] == kind
    assert doc["channel"] == channel and doc["status"] == "not_ready"

    # 2. A spec, but `raw` does not exist: the metadata round trip cannot name a leader, so the producer is not
    #    warm, the receiver is not ready, and nothing was started (no listener, no `listening` event).
    publish_channel_spec(kafka, ENV, channel, document(1))
    doc = await_ready(port, lambda s, d: reasons(d)["spec"] is None, "the spec to be applied")
    status, doc = readyz(port)
    assert status == 503
    assert reasons(doc) == {"spec": None, "component": "starting", "raw_producer": "not_warmed"}
    assert proc.process.poll() is None
    assert not any(line.get("event") == "listening" for line in proc.collect_pending())
    deadline = time.monotonic() + 30
    while not any(line.get("event") == "producer_warm_failed" for line in proc.lines):
        assert time.monotonic() < deadline, "the warm-up fault never fired"
        proc.collect_pending()
        time.sleep(0.1)
    assert not any(line.get("event") == "listening" for line in proc.lines)
    if ingest_port:
        assert refused(ingest_port), "the ingest listener must not bind before the producer is warm"

    # 3. The topic appears: the next warm-up succeeds, the listener binds, readiness turns 200.
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1)])
    listening = proc.wait_for_event("listening", timeout_s=60)
    assert listening["ops_port"] == port
    doc = await_ready(port, lambda s, d: s == 200, "ready")
    assert doc["status"] == "ready" and doc["channel_version"] == 1
    assert all(check["ok"] for check in doc["checks"])
    assert "reason" not in str(doc["checks"])
    if ingest_port:
        assert not refused(ingest_port)
    series = scrape(port)
    assert series[f'tb_ready{{component="receiver",channel="{channel}"}}'] == 1
    assert series[f'tb_spec_version{{component="receiver",channel="{channel}"}}'] == 1
    assert get(port, "/readyz", method="POST") == (405, None)
    assert get(port, "/nothing-here") == (404, None)

    # 4. Tombstone: not ready, `undeployed`; the process stays up and alive.
    publish_channel_spec(kafka, ENV, channel, None)
    doc = await_ready(port, lambda s, d: s == 503, "undeployed")
    assert reasons(doc)["spec"] == "undeployed"
    assert get(port, "/livez")[0] == 200

    # 5. Redeploy: ready again with the new version.
    publish_channel_spec(kafka, ENV, channel, document(2))
    doc = await_ready(port, lambda s, d: s == 200, "ready after redeploy")
    assert doc["channel_version"] == 2

    # 6. SIGTERM exits 0, and every structured line the process wrote validates against the log-line schema
    #    (the `listening` event included, now written by `JsonLogger`).
    assert proc.terminate_and_wait() == 0
    proc.collect_pending()
    validator = jsonschema.Draft202012Validator(LOG_SCHEMA)
    structured = [json.loads(raw) for raw in proc.raw if raw.startswith(b"{")]
    assert {"ops_listening", "listening", "channel_undeployed", "receiver_stopped"} <= {r["event"] for r in structured}
    for record in structured:
        validator.validate(record)


@pytest.mark.parametrize("kind", ["mllp", "http", "sftp", "kafka"])
def test_an_ops_port_that_cannot_bind_refuses_the_start_with_exit_2(
    kind: str, kafka: str, procs: list[ManagedProcess], tmp_path: Path
) -> None:
    channel = f"rdy-bind-{kind}"
    _document, _ingest, secrets = build(kind, channel, tmp_path, kafka)
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        taken = blocker.getsockname()[1]
        proc = start_receiver(
            MODULES[kind],
            channel=channel,
            env=ENV,
            bootstrap=kafka,
            instance=f"{channel}-0",
            secrets_dir=secrets,
            extra_args=["--ops-port", str(taken)],
        )
        procs.append(proc)
        refusal = proc.wait_for_event("startup_refused", timeout_s=60)
        assert proc.process.wait(timeout=30) == 2
    assert refusal["code"] == "OPS_LISTENER_BIND_FAILED" and refusal["level"] == "error"
    assert str(taken) not in "".join(raw.decode() for raw in proc.raw)


def test_a_non_loopback_ops_bind_is_explicit_and_warned(kafka: str, procs: list[ManagedProcess]) -> None:
    channel = "rdy-nonloop"
    proc = start_receiver(
        MODULES["http"],
        channel=channel,
        env=ENV,
        bootstrap=kafka,
        instance=f"{channel}-0",
        extra_args=["--ops-bind", "0.0.0.0", "--ops-port", "0"],  # noqa: S104
    )
    procs.append(proc)
    warning = proc.wait_for_event("ops_listener_non_loopback", timeout_s=60)
    assert warning["level"] == "warning" and "ops_port" in warning
    assert get(int(warning["ops_port"]), "/livez")[0] == 200


def test_the_default_ops_listener_is_loopback_only(kafka: str, procs: list[ManagedProcess]) -> None:
    proc = start_receiver(MODULES["http"], channel="rdy-loop", env=ENV, bootstrap=kafka, instance="rdy-loop-0")
    procs.append(proc)
    port = ops_port(proc)
    external = [
        a[4][0]
        for a in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
        if not a[4][0].startswith("127.")
    ]
    for address in external[:1]:
        with pytest.raises(OSError), socket.create_connection((address, port), timeout=1):
            pass
