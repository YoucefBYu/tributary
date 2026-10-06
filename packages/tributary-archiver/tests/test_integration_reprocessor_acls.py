"""Reprocess and replay under ONLY the reprocessor grants, on a deny-by-default SASL_SSL broker.

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_reprocessor_acls.py`

The grants below are this package's proposal for the `reprocessor` role (W3-29 will put them in the contract; then
replace `REPROCESSOR` with `contract_grants("reprocessor", ...)`): on the channel's `raw` topic READ, WRITE and
DESCRIBE, plus cluster IDEMPOTENT_WRITE, plus the group DESCRIBE below. only GROUP DESCRIBE on the literal group `tb.<env>.reprocessor`:
librdkafka sends FindCoordinator for a configured `group.id` even when the consumer only `assign`s (denied without
that grant, logged by the broker). The consumers never join, commit or read the group (no group READ).
"""

from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from tributary.archiver.audit import AuditEvent
from tributary.archiver.errors import ArchiverError
from tributary.archiver.event import parse_event
from tributary.archiver.ordering import ChannelTraits
from tributary.archiver.payload_source import KafkaRawPayloadSource
from tributary.archiver.query import ArchiveQueries
from tributary.archiver.replay import ReplayService
from tributary.archiver.reprocess import ReprocessService
from tributary.archiver.republish import make_publisher
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import raw_topic
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.ids import new_traceparent, uuid7

from tests.support.secure_broker import Grant, SecureBroker, secure_broker

pytestmark = pytest.mark.integration

ENV = "aclr"
CANARY = "CANARY-PHI-8812"
ACTOR = "user:synthetic-operator"


def reprocessor_grants(channel: str) -> list[Grant]:
    topic = raw_topic(ENV, channel)
    return [
        Grant("TOPIC", topic, "LITERAL", "READ"),
        Grant("TOPIC", topic, "LITERAL", "WRITE"),
        Grant("TOPIC", topic, "LITERAL", "DESCRIBE"),
        Grant("CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
        Grant("GROUP", f"tb.{ENV}.reprocessor", "LITERAL", "DESCRIBE"),
    ]


@pytest.fixture(scope="module")
def broker(tmp_path_factory: pytest.TempPathFactory) -> Any:
    with secure_broker(tmp_path_factory.mktemp("acl-broker")) as running:
        yield running


def seed(broker: SecureBroker, channel: str) -> tuple[str, int, int]:
    """One raw record, written by the broker's set-up (super user) listener."""
    broker.create_topics([NewTopic(raw_topic(ENV, channel), 1, 1)])
    mid = uuid7()
    message = Message(
        headers={
            h.TB_MSG_ID: mid,
            h.TB_CHANNEL: channel,
            h.TB_CHANNEL_VERSION: "1",
            h.TB_SOURCE: "r:1@127.0.0.1:1",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "text/plain",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: new_traceparent(),
        },
        payload=f"MSH|TRIBUTARY-SYNTH|{CANARY}".encode(),
        key=f"{CANARY}-KEY".encode(),
    )
    got: list[tuple[int, int]] = []
    producer = Producer({"bootstrap.servers": broker.admin, "acks": "all"})
    producer.produce(
        raw_topic(ENV, channel),
        value=message.payload,
        key=message.key,
        headers=[(k, v.encode()) for k, v in message.headers.items()],
        on_delivery=lambda err, m: got.append((m.partition(), m.offset())),
    )
    assert producer.flush(30) == 0 and got
    return mid, got[0][0], got[0][1]


def archive(ch: Any, channel: str, mid: str, partition: int, offset: int) -> ArchiveQueries:
    def target(db: str) -> ClickhouseTarget:
        return ClickhouseTarget(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            database=db,
            username=ch.username,
            password=ch.password,
            secure=False,
        )

    name = f"a_{uuid.uuid4().hex[:12]}"
    admin = connect(target(ch.dbname))
    admin.command(f"CREATE DATABASE {name}")
    admin.close()
    client = connect(target(name))
    ensure_schema(client, database=name, ttl_days=3650)
    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    event = sample_event(
        ts=stamp,
        channel=channel,
        message_id=mid,
        stage="received",
        topic=raw_topic(ENV, channel),
        partition=partition,
        offset=offset,
    )
    ClickhouseSink(client).insert([parse_event(orjson.dumps(event)).values])
    return ArchiveQueries(client, database=name)


def services(
    broker: SecureBroker, tmp: Path, principal: str, queries: ArchiveQueries, audit: list[AuditEvent], logs: io.StringIO
) -> tuple[ReprocessService, ReplayService]:
    settings = load_engine_kafka(environ=broker.process_env(tmp, principal))
    defaults = load_topic_defaults()
    log = JsonLogger("acl-test", stream=logs, strict=True)
    traits = lambda channel: ChannelTraits("none")  # noqa: E731
    reprocess = ReprocessService(
        queries,
        KafkaRawPayloadSource(settings, defaults, env=ENV, timeout_s=20),
        make_publisher(settings, defaults, client_id=principal),
        env=ENV,
        audit=audit.append,
        log=log,
        traits=traits,
    )
    replay = ReplayService(
        settings,
        defaults,
        make_publisher(settings, defaults, client_id=principal),
        env=ENV,
        audit=audit.append,
        log=log,
        traits=traits,
        idle_timeout_s=20,
    )
    return reprocess, replay


def test_reprocess_and_replay_work_with_only_the_reprocessor_grants_and_no_group_grant(
    broker: SecureBroker, ch_container: Any, tmp_path: Path
) -> None:
    channel = f"acl-{uuid.uuid4().hex[:8]}"
    mid, partition, offset = seed(broker, channel)
    principal = f"tb-reprocessor-{uuid.uuid4().hex[:6]}"
    broker.provision(principal, reprocessor_grants(channel))
    audit: list[AuditEvent] = []
    logs = io.StringIO()
    reprocess, replay = services(
        broker, tmp_path, principal, archive(ch_container, channel, mid, partition, offset), audit, logs
    )

    result = reprocess.reprocess(mid, channel=channel, request_id=uuid7(), actor=ACTOR)
    assert result.new_message_id != mid and result.partition == partition

    done = replay.replay(
        channel,
        start=datetime.fromtimestamp(0, UTC),
        end=datetime.now(UTC),
        replay_id=uuid7(),
        actor=ACTOR,
    )
    assert done.republished == 2, "the original and the reprocessed record are both in the span"
    assert broker.denials_for(principal) == [], "no ACL was exercised beyond the reprocessor grants"
    assert CANARY not in logs.getvalue() + repr(audit)


def test_without_read_on_raw_the_broker_denies_and_the_error_is_coded(
    broker: SecureBroker, ch_container: Any, tmp_path: Path
) -> None:
    channel = f"acl-{uuid.uuid4().hex[:8]}"
    mid, partition, offset = seed(broker, channel)
    principal = f"tb-noread-{uuid.uuid4().hex[:6]}"
    broker.provision(principal, [g for g in reprocessor_grants(channel) if g.operation != "READ"])
    audit: list[AuditEvent] = []
    logs = io.StringIO()
    reprocess, _replay = services(
        broker, tmp_path, principal, archive(ch_container, channel, mid, partition, offset), audit, logs
    )

    with pytest.raises(ArchiverError) as raised:
        reprocess.reprocess(mid, channel=channel, request_id=uuid7(), actor=ACTOR)
    assert getattr(raised.value, "code", "") in {"SOURCE_ERROR", "SOURCE_TIMEOUT"}
    assert broker.denials_for(principal, operation="READ", resource=f"Topic:LITERAL:{raw_topic(ENV, channel)}")
    assert [e.phase for e in audit] == ["requested", "failed"]
    assert CANARY not in logs.getvalue() + repr(audit) + str(raised.value)
