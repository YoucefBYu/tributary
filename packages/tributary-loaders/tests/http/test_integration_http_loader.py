"""Real Kafka, real HTTP loopback server: publish to `dev.itest.out.dest`, run `OutConsumer` for a
few seconds, confirm delivery, commit, and (with the destination made to fail first) retry-then-
success. Needs `TB_TEST_BOOTSTRAP` (e.g. `localhost:19092`) pointing at a reachable cluster —
skipped otherwise, since testcontainers is not yet a dependency of this workspace (see the W1-5
handoff to the lead: devops-engineer owns the root pyproject.toml dev group this needs).

NOT run in this wave — the shared cluster was in use for other agents' work while this was written.
`tests/http/test_consumer.py` covers the same state machine against a fake consumer.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from tributary.loaders.http.config import HttpDestinationConfig
from tributary.loaders.http.consumer import OutConsumer
from tributary.loaders.http.loader import HttpLoader
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("TB_TEST_BOOTSTRAP")


def _require_cluster() -> str:
    if not BOOTSTRAP:
        pytest.skip("TB_TEST_BOOTSTRAP not set; no Kafka cluster available for this integration test")
    return BOOTSTRAP


class FlakyThenOkHandler(BaseHTTPRequestHandler):
    fail_first_n = 0
    seen = 0

    def log_message(self, *_args: object) -> None:
        pass

    def do_POST(self) -> None:
        type(self).seen += 1
        if type(self).seen <= type(self).fail_first_n:
            self.send_response(500)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        self.send_response(200)
        self.end_headers()


@pytest.fixture
def stub_server() -> tuple[str, type[FlakyThenOkHandler]]:
    FlakyThenOkHandler.seen = 0
    FlakyThenOkHandler.fail_first_n = 0
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), FlakyThenOkHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}/ingest", FlakyThenOkHandler
    finally:
        httpd.shutdown()
        thread.join(2)


def _produce_one(bootstrap: str, topic: str) -> str:
    from confluent_kafka import Producer

    message = Message(
        headers={
            h.TB_MSG_ID: (msg_id := ids.uuid7()),
            h.TB_CHANNEL: "itest",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "test:0@x",
            h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
            h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "dest",
        },
        payload=b"MSH|x\r",
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    headers = [(k, v.encode()) for k, v in message.headers.items()]
    producer.produce(topic, value=message.payload, headers=headers)
    producer.flush(10)
    return msg_id


def test_retry_then_success(stub_server: tuple[str, type[FlakyThenOkHandler]]) -> None:
    bootstrap = _require_cluster()
    url, handler_cls = stub_server
    handler_cls.fail_first_n = 2  # first two attempts fail, third succeeds
    topic = f"dev.itest{int(time.time())}.out.dest"
    _produce_one(bootstrap, topic)

    config = HttpDestinationConfig(
        channel="itest",
        destination="dest",
        url=url,
        method="POST",
        timeout_s=2.0,
        headers={},
        auth=None,
        circuit_breaker=None,
        idempotency_technique="none",
        idempotency_header=None,
        backoff="fixed",
        initial_delay_ms=200,
        max_delay_ms=200,
        max_attempts=5,
        on_exhaust="halt-partition",
    )
    loader = HttpLoader(config)
    consumer = OutConsumer(config, loader, bootstrap_servers=bootstrap, group_id=f"itest-{time.time()}", topic=topic)

    async def run_briefly() -> None:
        task = asyncio.ensure_future(consumer.run())
        for _ in range(50):
            if handler_cls.seen >= 3:
                break
            await asyncio.sleep(0.2)
        consumer.stop()
        await asyncio.wait_for(task, 5)

    asyncio.run(run_briefly())
    assert handler_cls.seen == 3  # two failures, then a success: blocking retry worked
