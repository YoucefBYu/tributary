"""SFTP poller against a real Kafka broker and a real (in-process) SFTP server: a file leaves the source
directory only once its record is readable from `<env>.<channel>.raw`, and an unreachable Kafka leaves
the file where it was."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import paramiko  # type: ignore[import-untyped]
import pytest
from confluent_kafka.admin import NewTopic
from tributary.receivers.mllp.kafka_publisher import RawPublisher, topic_name
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.sftp.poller import SftpPoller
from tributary.receivers.sftp.session import connect, fingerprint

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, free_port, header_map

from ..sftp.fake_server import PASSWORD, USER, FakeSftpServer
from ..sftp.test_poller import HL7, settings

pytestmark = pytest.mark.integration


@pytest.fixture
def world(tmp_path: Path) -> tuple[Path, DirSecretResolver, paramiko.PKey]:
    root = tmp_path / "srv"
    for d in ("in", "done", "rejected"):
        (root / d).mkdir(parents=True)
    ref = tmp_path / "secrets" / "cred"
    ref.mkdir(parents=True)
    (ref / "username").write_text(USER, encoding="utf-8")
    (ref / "password").write_text(PASSWORD, encoding="utf-8")
    return root, DirSecretResolver(tmp_path / "secrets"), paramiko.RSAKey.generate(2048)


def make_cfg(server: FakeSftpServer, key: paramiko.PKey, bootstrap: str, channel: str, **kw: object):  # type: ignore[no-untyped-def]
    return replace(
        settings(directory="/in", reject_directory="/rejected", archive_directory="/done", after_commit="move"),
        channel=channel,
        host="127.0.0.1",
        port=server.port,
        host_keys=frozenset({fingerprint(key.asbytes())}),
        auth_secret_ref="cred",  # noqa: S106
        bootstrap_servers=bootstrap,
        ack_timeout_s=kw.pop("ack_timeout_s", 15.0),  # type: ignore[arg-type]
        connect_timeout_s=5.0,
        op_timeout_s=5.0,
    )


def test_file_is_archived_only_after_its_record_is_committed(
    kafka: str, world: tuple[Path, DirSecretResolver, paramiko.PKey]
) -> None:
    root, secrets, key = world
    channel = "sftp-commit"
    create_topics_with_retry(kafka, [NewTopic(topic_name("dev", channel), 1, 1)])
    (root / "in" / "a.hl7").write_bytes(HL7)

    async def go() -> None:
        with FakeSftpServer(root, [key]) as srv:
            cfg = make_cfg(srv, key, kafka, channel)
            publisher = RawPublisher(kafka, topic_name("dev", channel), client_id_prefix="sftp-receiver-test")
            publisher.start(asyncio.get_running_loop())
            try:
                poller = SftpPoller(cfg, publisher, lambda: connect(cfg, secrets))
                assert await poller.poll_once() == 1
            finally:
                publisher.close()

    asyncio.run(go())
    # The file is gone from the source directory, so the record must already be committed.
    assert [p.name for p in (root / "done").iterdir()] == ["a.hl7"]
    records = drain_topic(kafka, topic_name("dev", channel), partitions=1, expected=1, timeout_s=30)
    assert len(records) == 1 and records[0].value() == HL7
    headers = header_map(records[0])
    assert headers["tb-content-type"] == "application/hl7-v2+er7" and headers["tb-channel"] == channel


def test_unreachable_kafka_leaves_the_file_in_place(world: tuple[Path, DirSecretResolver, paramiko.PKey]) -> None:
    root, secrets, key = world
    (root / "in" / "a.hl7").write_bytes(HL7)
    dead = f"127.0.0.1:{free_port()}"

    async def go() -> None:
        with FakeSftpServer(root, [key]) as srv:
            cfg = make_cfg(srv, key, dead, "sftp-down", ack_timeout_s=2.0)
            publisher = RawPublisher(dead, topic_name("dev", "sftp-down"), client_id_prefix="sftp-receiver-test")
            publisher.start(asyncio.get_running_loop())
            try:
                poller = SftpPoller(cfg, publisher, lambda: connect(cfg, secrets))
                assert await poller.poll_once() == 0
                assert poller.metrics.acked == 0 and poller.metrics.nacked_unavailable == 1
            finally:
                publisher.close(timeout_s=0.5)

    asyncio.run(go())
    assert [p.name for p in (root / "in").iterdir()] == ["a.hl7"]  # unclaimed, not archived, not deleted
    assert list((root / "done").iterdir()) == [] and list((root / "rejected").iterdir()) == []
