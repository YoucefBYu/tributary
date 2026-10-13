# ruff: noqa: S101, S608  # a measurement script: asserts are its checks, SQL text is built from constants and ints
"""W3-9b benchmark: archive search latency at about 100k archived rows, and replay throughput.

    uv run python -m benchmarks.w3_archive_search_replay [--rows 100000] [--requests 200] [--replay 5000 20000]

**What runs** (real processes and containers, one host): a single-node Kafka, ClickHouse 24.8 with the archiver's own
DDL (`--migrate-only`), TLS PostgreSQL and the real API process (`python -m tributary.api`, SELECT-only ClickHouse
user, static-JWKS bearer auth). No worker, receiver or archiver runs during the measurement: the archive is filled
directly and the `raw` topic is filled by a producer, so the numbers are the API plus ClickHouse plus Kafka, nothing
else.

**Search.** `--rows` synthetic `message_events` rows (half `transformed`, half `routed`, 1% `status=error` with one
error code) are inserted with one `INSERT ... SELECT FROM numbers()`, spread over the last ~`rows * 30 ms`. Four
queries go through `POST .../messages/search` over HTTP, sequentially, after 10 discarded warm-up requests, `--requests`
timed requests each: `page1` (one stage, limit 50, whole window), `page1_narrow` (one stage, 5-minute window),
`error_filter` (status and error code), `paginate` (20 consecutive cursor pages, the time of each page), and a baseline of `GET /v1/environments` (authenticated, PostgreSQL only) for the API's fixed cost. Latency is
the client-observed time with `perf_counter`.

**Replay.** For each `--replay` size, that many records (keyed over 16 keys, 3 partitions, valid envelopes) are produced
to `raw`, then `POST .../replays` over exactly their time window with `allow_reorder` true; the throughput is
`republished / (time from the POST to the first poll that reads status completed)`, polled every 50 ms. The replay
re-publishes into the same `raw` under the reprocessor link (here the engine link; PLAINTEXT loopback).

Results are written to `benchmarks/results/w3-9b-archive-search-replay.json` with `environment()` and image tags.
Single host, shared with the load generator: absolute numbers are not sizing numbers.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from confluent_kafka import Producer  # noqa: E402
from confluent_kafka.admin import NewTopic  # noqa: E402
from testcontainers.core.container import DockerContainer  # noqa: E402
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic  # noqa: E402
from tributary.sdk.ids import uuid7  # noqa: E402
from tributary.sdk.testing import make_message  # noqa: E402

from benchmarks.harness import environment, percentile  # noqa: E402
from tests.support.archive_e2e import DATABASE, ArchiveStack  # noqa: E402
from tests.support.kafka_helpers import await_broker, create_topics_with_retry, free_port  # noqa: E402

ENV = "w9bbench"
CHANNEL = "w9b-bench"
PARTITIONS = 3
RESULTS = ROOT / "benchmarks" / "results" / "w3-9b-archive-search-replay.json"
KAFKA_IMAGE = "apache/kafka:4.0.0"
BASE = f"/v1/environments/{ENV}/channels/{CHANNEL}"


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def summarize(samples_ms: list[float]) -> dict[str, float]:
    ordered = sorted(samples_ms)
    return {
        "n": float(len(ordered)),
        "p50_ms": round(percentile(ordered, 0.50), 2),
        "p95_ms": round(percentile(ordered, 0.95), 2),
        "p99_ms": round(percentile(ordered, 0.99), 2),
        "max_ms": round(ordered[-1], 2),
        "mean_ms": round(statistics.fmean(ordered), 2),
    }


def seed_events(stack: ArchiveStack, rows: int) -> None:
    assert stack.clickhouse is not None
    stack.clickhouse.query(
        f"""INSERT INTO {DATABASE}.message_events
(ts, kind, channel, channel_version, stage, status, instance, message_id, parent_id, correlation_id, destination,
 attempt, content_type, size_bytes, claim_check, topic, `partition`, `offset`, error_code, error_message, duration_ms,
 traceparent, attrs, scope)
SELECT
  now64(3) - toIntervalMillisecond(intDiv(number, 2) * 30),
  'message', '{CHANNEL}', 1, if(number % 2 = 0, 'transformed', 'routed'),
  if(intDiv(number, 2) % 100 = 0, 'error', 'ok'), 'worker-0',
  reinterpretAsUUID(unhex(lpad(hex(intDiv(number, 2)), 32, '0'))), NULL, NULL, if(number % 2 = 0, '', 'stub'),
  0, 'x-application/hl7-v2+er7', 120, '', '{raw_topic(ENV, CHANNEL)}', number % {PARTITIONS}, number,
  if(intDiv(number, 2) % 100 = 0, 'DEST_REFUSED', ''), '', 1.5, '', map('msg_type', 'ADT-A01'), ''
FROM numbers({rows})""",
        timeout_s=600,
    )


def bench_search(stack: ArchiveStack, rows: int, requests: int) -> dict[str, Any]:
    since = stamp(datetime.now(UTC) - timedelta(milliseconds=(rows // 2) * 30 + 60_000))
    narrow = stamp(datetime.now(UTC) - timedelta(minutes=5))
    cases: dict[str, dict[str, Any]] = {
        "page1": {"since": since, "stages": ["routed"], "limit": 50},
        "page1_narrow": {"since": narrow, "stages": ["routed"], "limit": 50},
        "error_filter": {
            "since": since,
            "stages": ["routed"],
            "status": "error",
            "error_code": "DEST_REFUSED",
            "limit": 50,
        },
    }
    out: dict[str, Any] = {}
    with stack.client() as http:
        for name, body in cases.items():
            samples: list[float] = []
            returned = 0
            for i in range(10 + requests):
                started = time.perf_counter()
                response = http.post(f"{BASE}/messages/search", json=body)
                elapsed = (time.perf_counter() - started) * 1000
                assert response.status_code == 200, (name, response.status_code)
                if i >= 10:
                    samples.append(elapsed)
                    returned = len(response.json()["items"])
            out[name] = {**summarize(samples), "items_per_page": returned}
        base: list[float] = []  # an authenticated request that reads PostgreSQL only: the API's own fixed cost
        for i in range(10 + requests):
            started = time.perf_counter()
            assert http.get("/v1/environments").status_code == 200
            if i >= 10:
                base.append((time.perf_counter() - started) * 1000)
        out["baseline_authed_no_archive"] = summarize(base)
        pages: list[float] = []
        cursor: str | None = None
        for _ in range(20):
            body = {**cases["page1"], **({"cursor": cursor} if cursor else {})}
            started = time.perf_counter()
            response = http.post(f"{BASE}/messages/search", json=body)
            pages.append((time.perf_counter() - started) * 1000)
            assert response.status_code == 200
            cursor = response.json().get("next_cursor")
            if not cursor:
                break
        out["paginate"] = {**summarize(pages), "pages": len(pages)}
    return out


def bench_replay(stack: ArchiveStack, kafka: str, count: int) -> dict[str, Any]:
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True, "linger.ms": 20})
    start = datetime.now(UTC) - timedelta(milliseconds=50)
    for i in range(count):
        message = make_message(b'{"synthetic": true}', key=f"SYNTH-{i % 16}".encode(), channel=CHANNEL)
        while True:
            try:
                producer.produce(
                    raw_topic(ENV, CHANNEL),
                    key=message.key,
                    value=message.payload,
                    headers=[(k, v.encode()) for k, v in message.headers.items()],
                )
                break
            except BufferError:
                producer.poll(0.1)
    assert producer.flush(120) == 0
    end = datetime.now(UTC) + timedelta(milliseconds=50)
    replay_id = uuid7()
    with stack.client() as http:
        began = time.perf_counter()
        started = http.post(
            f"{BASE}/replays",
            json={
                "replay_id": replay_id,
                "start": stamp(start),
                "end": stamp(end),
                "allow_reorder": True,
                "max_records": 1_000_000,
                "deadline_s": 900,
            },
        )
        assert started.status_code == 202, started.status_code
        status: dict[str, Any] = {}
        while True:
            status = http.get(f"{BASE}/replays/{replay_id}").json()
            if status["status"] != "running":
                break
            time.sleep(0.05)
        elapsed = time.perf_counter() - began
    assert status["status"] == "completed", status.get("failure_code")
    republished = int(status["republished"])
    return {
        "records_produced": count,
        "republished": republished,
        "elapsed_s": round(elapsed, 3),
        "records_per_s": round(republished / elapsed, 1),
        "skipped_undecodable": status.get("skipped_undecodable"),
        "skipped_out_of_range": status.get("skipped_out_of_range"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--replay", type=int, nargs="+", default=[5_000, 20_000])
    args = parser.parse_args(argv)

    port = free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_name(f"tb-qa-w39b-bench-kafka-{os.getpid()}")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "qa-w39b-bench")
        .with_bind_ports(port, port)
    )
    container.start()
    kafka = f"127.0.0.1:{port}"
    workdir = Path(tempfile.mkdtemp(prefix="tb-w39b-bench-"))
    stack = ArchiveStack(workdir, kafka, env=ENV, channel=CHANNEL, run=f"bench-{uuid.uuid4().hex[:6]}", content=False)
    try:
        await_broker(kafka)
        create_topics_with_retry(
            kafka,
            [
                NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
                NewTopic(out_topic(ENV, CHANNEL, "stub"), PARTITIONS, 1),
                NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
                NewTopic(events_topic(ENV), 1, 1),
                NewTopic(f"_tb.{ENV}.audit", 1, 1),
                NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
            ],
        )
        stack.start_stores()
        stack.start_api()
        stack.deploy_channel()
        seed_events(stack, args.rows)
        assert stack.clickhouse is not None
        stored = int(stack.clickhouse.query(f"SELECT count() FROM {DATABASE}.message_events"))
        search = bench_search(stack, args.rows, args.requests)
        replay = [bench_replay(stack, kafka, count) for count in args.replay]
    finally:
        stack.teardown()
        container.stop()
    document = {
        "benchmark": "w3-9b-archive-search-replay",
        "at": datetime.now(UTC).isoformat(),
        "environment": environment(),
        "images": {
            "kafka": KAFKA_IMAGE,
            "clickhouse": "clickhouse/clickhouse-server:24.8",
            "postgres": "postgres:16-alpine",
        },
        "rows_requested": args.rows,
        "rows_stored": stored,
        "requests_per_case": args.requests,
        "search": search,
        "replay": replay,
    }
    RESULTS.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(document, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
