"""HTTP receiver against a real Kafka broker: a 202 means the record is already readable from
`<env>.<channel>.raw` (invariant 1), and an unreachable Kafka is a 503, never a 202."""

from __future__ import annotations

import asyncio
import json

import pytest
from confluent_kafka.admin import NewTopic
from tributary.receivers.http.config import AuthSpec, HttpSettings
from tributary.receivers.http.server import HttpServer
from tributary.receivers.mllp.kafka_publisher import RawPublisher, topic_name
from tributary.runtime.clients import KafkaSettings
from tributary.sdk.keys import parse_key_expression

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, free_port, header_map

pytestmark = pytest.mark.integration

ENV = "dev"
BODY = json.dumps({"subject": {"reference": "Patient/SYNTH42"}, "meta": "TRIBUTARY-SYNTH"}).encode()


def cfg(channel: str, **kw: object) -> HttpSettings:
    base = HttpSettings(
        channel=channel,
        channel_version=1,
        bind="127.0.0.1",
        port=0,
        tls=None,
        path="/ingest",
        accept=frozenset({"application/fhir+json"}),
        auth=AuthSpec("none"),
        key_expression=parse_key_expression("json:/subject/reference"),
        nack_on=frozenset({"parse-error"}),
        ack_timeout_s=kw.pop("ack_timeout_s", 15.0),  # type: ignore[arg-type]
        max_message_bytes=1_000_000,
        queue_max_bytes=64 * 1024 * 1024,
        queue_max_messages=None,
        env=ENV,
        ordering="per-key",
    )
    return base


async def post(port: int, body: bytes) -> tuple[int, dict[str, str], bytes]:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(
        b"POST /ingest HTTP/1.1\r\nContent-Type: application/fhir+json\r\nContent-Length: "
        + str(len(body)).encode()
        + b"\r\n\r\n"
        + body
    )
    await w.drain()
    raw = await asyncio.wait_for(r.read(65536), 30)
    w.close()
    head, _, payload = raw.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    return int(lines[0].split(" ")[1]), {k.lower(): v for k, v in (x.split(": ", 1) for x in lines[1:])}, payload


async def serve(config: HttpSettings, bootstrap: str) -> tuple[HttpServer, asyncio.base_events.Server, RawPublisher]:
    publisher = RawPublisher(
        KafkaSettings(bootstrap),
        topic_name(config.env, config.channel),
        ordering="per-key",
        client_id_prefix="http-receiver-test",
    )
    publisher.start(asyncio.get_running_loop())
    srv = HttpServer(config, publisher)
    return srv, await srv.start(), publisher


def test_202_means_the_record_is_committed_to_raw_with_the_evaluated_key(kafka: str) -> None:
    channel = "http-commit"
    create_topics_with_retry(kafka, [NewTopic(topic_name(ENV, channel), 3, 1)])

    async def go() -> None:
        srv, server, publisher = await serve(cfg(channel), kafka)
        try:
            port = server.sockets[0].getsockname()[1]
            status, _, body = await post(port, BODY)
            assert status == 202
            msg_id = json.loads(body)["tb-msg-id"]
            # Read straight after the 202: it must already be there.
            records = await asyncio.to_thread(
                drain_topic, kafka, topic_name(ENV, channel), partitions=3, expected=1, timeout_s=30
            )
            assert len(records) == 1
            record = records[0]
            assert header_map(record)["tb-msg-id"] == msg_id
            assert record.value() == BODY
            assert record.key() == b"Patient/SYNTH42"
            assert header_map(record)["tb-content-type"] == "application/fhir+json"
            assert srv.metrics.acked == 1
        finally:
            server.close()
            await server.wait_closed()
            publisher.close()

    asyncio.run(go())


def test_unreachable_kafka_is_503_with_retry_after_never_202() -> None:
    dead = f"127.0.0.1:{free_port()}"  # nothing listens: the produce can never be acknowledged

    async def go() -> None:
        srv, server, publisher = await serve(cfg("http-down", ack_timeout_s=2.0), dead)
        try:
            port = server.sockets[0].getsockname()[1]
            status, headers, body = await post(port, BODY)
            assert status == 503 and int(headers["retry-after"]) >= 1
            assert b"tb-msg-id" not in body
            assert srv.metrics.acked == 0 and srv.metrics.nacked_unavailable == 1
            assert srv.budget.used_bytes == 0
        finally:
            server.close()
            await server.wait_closed()
            publisher.close(timeout_s=0.5)

    asyncio.run(go())
