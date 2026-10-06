"""The SFTP receiver's spec lifecycle against a real broker (invariant 3): the spec comes only from
`_tb.<env>.channels`, polling starts once it has been read, a new version applies live, a tombstone pauses
polling, and a redeploy resumes it in the same process. Real receiver process (`python -m
tributary.receivers.sftp`), real Kafka, an in-process SFTP server on loopback, synthetic data only."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from pathlib import Path

import paramiko  # type: ignore[import-untyped]
import pytest
from confluent_kafka.admin import NewTopic
from tributary.receivers.sftp.session import fingerprint
from tributary.runtime.topics import raw_topic

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import sftp_document

from ..sftp.fake_server import PASSWORD, USER, FakeSftpServer

pytestmark = pytest.mark.integration

ENV = "itest"
MODULE = "tributary.receivers.sftp"


def hl7(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|F|20260101000000||ORU^R01|{cid}|P|2.5\rPID|1||SYNTH0001^^^F||TEST^SYNTHETIC\r"
    ).encode()


@pytest.fixture
def receivers() -> Iterator[list[ManagedProcess]]:
    procs: list[ManagedProcess] = []
    yield procs
    for proc in procs:
        proc.terminate_and_wait()


@pytest.fixture
def world(tmp_path: Path) -> tuple[Path, Path, paramiko.PKey]:
    root = tmp_path / "srv"
    for d in ("in", "in/rejected", "done"):
        (root / d).mkdir(parents=True)
    cred = tmp_path / "secrets" / "cred"
    cred.mkdir(parents=True)
    (cred / "username").write_text(USER, encoding="utf-8")
    (cred / "password").write_text(PASSWORD, encoding="utf-8")
    return root, tmp_path / "secrets", paramiko.RSAKey.generate(2048)


def wait_until(predicate: Callable[[], bool], what: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.1)


def test_publish_change_live_tombstone_redeploy(
    kafka: str, receivers: list[ManagedProcess], world: tuple[Path, Path, paramiko.PKey]
) -> None:
    root, secrets, key = world
    ensure_channels_topic(kafka, ENV)
    channel = "sftp-spec-live"
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1)])
    proc = start_receiver(MODULE, channel=channel, env=ENV, bootstrap=kafka, instance="sftp-live", secrets_dir=secrets)
    receivers.append(proc)

    with FakeSftpServer(root, [key]) as srv:

        def doc(version: int, **kw: object) -> dict[str, object]:
            return sftp_document(
                channel,
                version=version,
                host="127.0.0.1",
                port=srv.port,
                host_key=fingerprint(key.asbytes()),
                secret_ref="cred",  # noqa: S106
                **kw,
            )

        # No spec yet: not ready. A file dropped now is left alone.
        (root / "in" / "a.hl7").write_bytes(hl7("SFTP-1"))
        proc.wait_for_event("spec_unavailable", timeout_s=60)
        time.sleep(0.5)
        assert (root / "in" / "a.hl7").exists()

        publish_channel_spec(kafka, ENV, channel, doc(1))
        listening = proc.wait_for_event("listening", timeout_s=60)
        assert listening["channel_version"] == 1
        wait_until(lambda: not (root / "in" / "a.hl7").exists(), "a.hl7 committed and removed")

        # Live: afterCommit becomes `move`, in the same process.
        pid = proc.process.pid
        publish_channel_spec(kafka, ENV, channel, doc(2, afterCommit="move", archiveDirectory="/done"))
        applied = proc.wait_for_event("spec_applied", timeout_s=60)
        assert applied["changed"] == "channel_version,after_commit,archive_directory"
        (root / "in" / "b.hl7").write_bytes(hl7("SFTP-2"))
        wait_until(lambda: (root / "done" / "b.hl7").exists(), "b.hl7 archived")
        assert proc.process.pid == pid and proc.process.poll() is None

        records = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=2)
        assert [r.value().split(b"|")[9].decode() for r in records] == ["SFTP-1", "SFTP-2"]
        assert {dict(r.headers())["tb-channel-version"] for r in records} == {b"1", b"2"}

        # Tombstone: polling pauses; a file dropped now stays where it is.
        publish_channel_spec(kafka, ENV, channel, None)
        proc.wait_for_event("channel_undeployed", timeout_s=60)
        (root / "in" / "c.hl7").write_bytes(hl7("SFTP-3"))
        time.sleep(1.0)
        assert (root / "in" / "c.hl7").exists() and proc.process.poll() is None

        # Redeploy: the same process polls again and takes the waiting file.
        publish_channel_spec(kafka, ENV, channel, doc(3, afterCommit="move", archiveDirectory="/done"))
        proc.wait_for_event("listening", timeout_s=60)
        wait_until(lambda: (root / "done" / "c.hl7").exists(), "c.hl7 archived after the redeploy")
        assert len(drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=3)) == 3
        assert proc.process.pid == pid

        # Restart-only: a different pinned host key exits 0 for the supervisor.
        changed = doc(5, afterCommit="move", archiveDirectory="/done")
        changed["spec"]["source"]["sftp"]["hostKeys"] = ["SHA256:" + "B" * 43]  # type: ignore[index]
        publish_channel_spec(kafka, ENV, channel, changed)
        assert proc.wait_for_event("spec_restart_required", timeout_s=60)["fields"] == "host_keys"
        assert proc.process.wait(timeout=30) == 0
