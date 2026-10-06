"""S2 for Phase 1, against the real runtime: MLLP ACK-after-commit latency through the real receiver
(`tributary.receivers.mllp`) and a real Kafka broker, replacing Phase 0's stand-in `spikes/S2-mllp-kafka`
server (`docs/spikes/S2-mllp-kafka.md`'s own finding 3: "a stand-in worker", carried in
PHASE-0-DECISION.md section 6 item 6 as unmeasured against the real component).

    uv run python -m benchmarks.s2_mllp_ack_latency [--count 2000] [--conns 10] [--seed 1]

**Methodology, and how it differs from Phase 0's S2** (state the difference, not just the number,
PHASE-0-DECISION.md section 2): this script drives `--conns` persistent MLLP connections, each in a
*closed* loop -- send one message, wait for its `MSA|AA`, record the round-trip, send the next -- rather
than Phase 0's open-loop fixed-rate generator (`spikes/S2-mllp-kafka/loadgen.py`). Closed-loop cannot
show what happens to latency under a fixed offered load the receiver cannot keep up with; it can only
show latency at whatever rate `--conns` connections in lockstep actually sustain. It is the simpler,
honest baseline for "does the real receiver's ACK-after-commit path still land near Phase 0's numbers",
not a replacement for an open-loop rate study.

**Topology**: single-node Kafka (the `lite` profile, RF=1, matching this script's own private
testcontainers broker) -- the *production* durability floor is RF=3, and PHASE-0-DECISION.md's own S2
number (33-125 ms p99 at 1k msg/s) was measured against a 3-broker cluster. A lite-profile number here
is therefore a *different, easier* topology, reported as such, never substituted for the RF=3 threshold.

Every result is written with `environment()` and this run's Kafka image tag, so a reader does not have
to guess what "the ACK p99" was measured against.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import statistics
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402
from testcontainers.core.container import DockerContainer  # noqa: E402
from tributary.runtime.topics import raw_topic  # noqa: E402

from benchmarks.gen import generate  # noqa: E402
from benchmarks.harness import environment, percentile  # noqa: E402
from tests.support.channel_spec import walking_skeleton_document, write_channel_spec_file  # noqa: E402
from tests.support.kafka_helpers import await_broker, free_port  # noqa: E402
from tests.support.processes import ManagedProcess, spawn_mllp_receiver  # noqa: E402

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
KAFKA_IMAGE = "apache/kafka:4.0.0"
ENV = "dev"
CHANNEL = "bench-s2-ack-latency"
DESTINATION = "unused"  # this benchmark never runs a loader; the destination is never dialled


def _kafka_container(port: int) -> DockerContainer:
    return (
        DockerContainer(KAFKA_IMAGE)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
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
        .with_env("CLUSTER_ID", "bench-s2")
        .with_bind_ports(9092, port)
    )


def _split(corpus: list[str], conns: int) -> Iterator[list[str]]:
    """`conns` roughly-equal, contiguous slices -- each connection gets its own messages, so control
    ids never collide across connections."""
    per = -(-len(corpus) // conns)  # ceil division
    for i in range(0, len(corpus), per):
        yield corpus[i : i + per]


class ConnResult:
    """One connection's raw record, kept in send order so the first ACK is never lost to sorting."""

    def __init__(self) -> None:
        self.connect_ms = 0.0
        self.latencies: list[float] = []
        self.first_ack_done_ms = 0.0  # when the first ACK arrived, relative to the run's start


async def _run_connection(host: str, port: int, messages: list[str], timeout_s: float, run_start: float) -> ConnResult:
    res = ConnResult()
    t_conn = time.perf_counter()
    reader, writer = await asyncio.open_connection(host, port)
    res.connect_ms = (time.perf_counter() - t_conn) * 1000
    latencies = res.latencies
    try:
        for message in messages:
            frame = SB + message.encode("ascii") + EB + CR
            start = time.perf_counter()
            writer.write(frame)
            await writer.drain()
            buf = b""
            try:
                async with asyncio.timeout(timeout_s):
                    while not buf.endswith(EB + CR):
                        chunk = await reader.read(4096)
                        if not chunk:
                            break
                        buf += chunk
            except TimeoutError:
                pass
            done = time.perf_counter()
            latencies.append((done - start) * 1000)  # ms
            if len(latencies) == 1:
                res.first_ack_done_ms = (done - run_start) * 1000
            if b"MSA|AA" not in buf:
                raise RuntimeError(f"expected MSA|AA, got {buf!r}")
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
    return res


async def _run_all(
    host: str, port: int, slices: list[list[str]], timeout_s: float, prewarm: list[str]
) -> list[ConnResult]:
    if prewarm:  # one throwaway message on its own connection, before the clock starts
        await _run_connection(host, port, prewarm, timeout_s, time.perf_counter())
    run_start = time.perf_counter()
    return list(await asyncio.gather(*(_run_connection(host, port, s, timeout_s, run_start) for s in slices)))


def _stats(sorted_ms: list[float]) -> dict[str, float]:
    return {
        "n": float(len(sorted_ms)),
        "p50": percentile(sorted_ms, 0.5),
        "p90": percentile(sorted_ms, 0.9),
        "p99": percentile(sorted_ms, 0.99),
        "mean": statistics.fmean(sorted_ms),
        "max": sorted_ms[-1],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--count", type=int, default=2000)
    ap.add_argument("--conns", type=int, default=10)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--prewarm", action="store_true", help="send one message on a separate connection first")
    ap.add_argument("--timeout-s", type=float, default=20.0)
    ap.add_argument("--out", type=Path, default=ROOT / "benchmarks" / "results" / "s2-mllp-ack-latency.json")
    a = ap.parse_args(argv)

    corpus = list(generate(a.count + (1 if a.prewarm else 0), seed=a.seed))
    prewarm = [corpus.pop()] if a.prewarm else []
    slices = list(_split(corpus, a.conns))

    kafka_port = free_port()
    mllp_port = free_port()
    container = _kafka_container(kafka_port)
    container.start()
    receiver: ManagedProcess | None = None
    try:
        bootstrap = f"127.0.0.1:{kafka_port}"
        await_broker(bootstrap)

        admin = AdminClient({"bootstrap.servers": bootstrap})
        for _topic, future in admin.create_topics([NewTopic(raw_topic(ENV, CHANNEL), 3, 1)]).items():
            try:
                future.result(timeout=30)
            except Exception as exc:  # pragma: no cover - already exists on a re-run
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

        document = walking_skeleton_document(
            mllp_port=mllp_port,
            http_url="http://127.0.0.1:1/ingest",  # never dialled: no loader in this benchmark
            channel=CHANNEL,
            destination=DESTINATION,
            version=1,
        )
        spec_path = write_channel_spec_file(ROOT / "benchmarks" / "results" / "s2-channel-spec.json", document)

        receiver = spawn_mllp_receiver(channel_spec=spec_path, env=ENV, bootstrap=bootstrap, instance="bench-s2")

        started = time.perf_counter()
        conn_results = asyncio.run(_run_all("127.0.0.1", mllp_port, slices, a.timeout_s, prewarm))
        wall_s = time.perf_counter() - started
    finally:
        if receiver is not None:
            receiver.terminate_and_wait()
        container.stop()

    latencies_ms = sorted(x for r in conn_results for x in r.latencies)
    first_acks = sorted(r.latencies[0] for r in conn_results if r.latencies)
    rest = sorted(x for r in conn_results for x in r.latencies[1:])
    done_at = sorted(r.first_ack_done_ms for r in conn_results if r.latencies)
    connect = sorted(r.connect_ms for r in conn_results)
    out: dict[str, Any] = {
        "spike": "S2 (Phase 1 re-run against the real tributary.receivers.mllp)",
        "environment": environment(),
        "kafka_image": KAFKA_IMAGE,
        "topology": "single-node lite (RF=1) -- NOT the RF=3 production floor PHASE-0-DECISION.md's S2 used",
        "methodology": "closed-loop per connection (send, await ACK, repeat) -- see module docstring for "
        "how this differs from Phase 0's open-loop fixed-rate loadgen.py",
        "count": a.count,
        "conns": a.conns,
        "prewarm": a.prewarm,
        "wall_s": wall_s,
        "acks_per_s": len(latencies_ms) / wall_s if wall_s > 0 else 0.0,
        # every ACK, warm-up included: this is the number compared against the threshold
        "ack_latency_ms": _stats(latencies_ms),
        # diagnostics: the first ACK on each connection reported separately, and the rest without it
        "first_ack_per_conn_ms": _stats(first_acks),
        "excluding_first_ack_per_conn_ms": _stats(rest) if rest else None,
        "first_ack_completed_at_ms_since_start": {"min": done_at[0], "max": done_at[-1]},
        "connect_ms": {"p50": percentile(connect, 0.5), "max": connect[-1]},
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(out, indent=2, default=str))
    print(
        f"{len(latencies_ms)} ACKs in {wall_s:.1f}s ({out['acks_per_s']:.0f}/s); "
        f"p50 {out['ack_latency_ms']['p50']:.1f}ms  p99 {out['ack_latency_ms']['p99']:.1f}ms"
    )
    print(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
