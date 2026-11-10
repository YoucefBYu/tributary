"""W3-24: the synchronous-reply tail, measured with at least 3,000 requests per run and the evidence needed to
find a cause kept for every slow request.

STATUS rank 9 reported one run in four with an MLLP sync-reply p99 of about 270 ms inside a full integration
session; `docs/spikes/wave-2-sync-reply-latency.md` could not reproduce it with 300 requests per run in
isolation. This test runs the same exchange (a real MLLP receiver process, a real pipeline worker process, the
real `raw`, `reply` topics and `replier` group on a single-broker Kafka) with 5 senders x 650 requests at
about 50 msg/s, and for every request over `SLOW_MS` (200) it keeps, from one session clock:

* the request's control id, wall-clock start and latency;
* the receiver's and the worker's structured log lines in the window around it;
* the broker's container log lines in the window (`docker logs --timestamps`, Kafka's own timestamps);
* host CPU state sampled every 250 ms (busy, iowait, steal, load average); the file also carries every request
  and every host sample, so the tail can be correlated with the host offline
  (`benchmarks/analyze_sync_reply_tail.py`).

It is a measurement, not a gate: it asserts only that every request was answered `AA`, that at least 3,000
were made, and that the evidence file was written. Control ids and timestamps only (invariant 6). Output goes to
`$TB_TAIL_REPORT_DIR/sync-reply-tail-<run>.json` (default: the test's tmp directory).

Marked `bench`, not `integration`: it is a measurement of about 85 s with no gate, so it must not run in every
integration push. `pytest -m bench tests/integration/test_sync_reply_tail.py` runs it alone (the "isolated" arm of
the report); `pytest -m "integration or bench"` runs it at the end of the full integration session, the context in
which the original outlier was seen.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer

from tests.support.kafka_helpers import await_broker, free_port
from tests.support.processes import ManagedProcess

pytestmark = pytest.mark.bench

SENDERS = 5
PER_SENDER = int(os.environ.get("TB_TAIL_PER_SENDER", "650"))  # 5 x 650 = 3,250 requests
#: The plan asks for at least 3,000 requests per run; a smaller `TB_TAIL_PER_SENDER` is a smoke run and asserts its own size.
MIN_REQUESTS = 3000 if "TB_TAIL_PER_SENDER" not in os.environ else SENDERS * PER_SENDER
THINK_S = 0.08  # as the wave 2 test: about 50 msg/s in total
SLOW_MS = 200.0
WINDOW_BEFORE_S = 3.0
WINDOW_AFTER_S = 1.0
RIG_PATH = Path(__file__).resolve().parents[2] / "packages/tributary-receivers/tests/integration/sync_reply_rig.py"


def _load_rig() -> ModuleType:
    spec = importlib.util.spec_from_file_location("qa_sync_reply_rig", RIG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HostSampler:
    """CPU busy / iowait / steal percentages from /proc/stat and the 1-minute load, every 250 ms."""

    def __init__(self) -> None:
        self.samples: list[dict[str, float]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _stat() -> list[int]:
        return [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]

    def _run(self) -> None:
        previous = self._stat()
        while not self._stop.wait(0.25):
            current = self._stat()
            delta = [c - p for c, p in zip(current, previous, strict=False)]
            total = sum(delta[:8]) or 1
            idle, iowait, steal = delta[3], delta[4], delta[7]
            self.samples.append(
                {
                    "t": time.time(),
                    "busy_pct": round(100.0 * (total - idle - iowait) / total, 1),
                    "iowait_pct": round(100.0 * iowait / total, 1),
                    "steal_pct": round(100.0 * steal / total, 1),
                    "load1": float(Path("/proc/loadavg").read_text().split()[0]),
                }
            )
            previous = current

    def __enter__(self) -> HostSampler:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2)


@pytest.fixture(scope="module")
def broker() -> Iterator[tuple[str, DockerContainer]]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_name(f"tb-qa-w324-kafka-{os.getpid()}")
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
        .with_env("CLUSTER_ID", "qa-w324-sync-reply-tail")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap, container
    finally:
        container.stop()


def _epoch(stamp: str) -> float | None:
    """Epoch seconds of an ISO-8601 stamp (`2026-10-12T09:01:02.345Z`, nanosecond tails tolerated)."""
    try:
        text = stamp.strip().rstrip("Z")
        head, _, frac = text.partition(".")
        moment = dt.datetime.fromisoformat(head).replace(tzinfo=dt.UTC)
        return moment.timestamp() + (float("0." + "".join(c for c in frac if c.isdigit())) if frac else 0.0)
    except ValueError:
        return None


def _json_lines(proc: ManagedProcess) -> list[tuple[float, str]]:
    proc.collect_pending()
    out: list[tuple[float, str]] = []
    for raw in proc.raw:
        try:
            record = json.loads(raw)
        except ValueError:
            continue
        stamp = _epoch(str(record.get("ts", ""))) if isinstance(record, dict) else None
        if stamp is not None:
            out.append((stamp, raw.decode("utf-8", "replace").strip()))
    return out


def _broker_lines(container: DockerContainer, since: float) -> list[tuple[float, str]]:
    wrapped = container.get_wrapped_container()
    blob = wrapped.logs(timestamps=True, since=dt.datetime.fromtimestamp(since - 5, dt.UTC)).decode("utf-8", "replace")
    out: list[tuple[float, str]] = []
    for line in blob.splitlines():
        stamp_text, _, rest = line.partition(" ")
        stamp = _epoch(stamp_text)
        if stamp is not None:
            out.append((stamp, rest))
    return out


def _percentile(ordered: list[float], q: float) -> float:
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


def test_sync_reply_tail_over_3000_requests_with_logs(
    broker: tuple[str, DockerContainer], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TB_OPS_PORT", "0")
    kafka, container = broker
    rig = _load_rig()
    channel = f"sr-tail-{int(time.time()) % 100000}"
    rig.deploy(kafka, channel, reply={"timeoutMs": 20000})
    receiver = rig.start_receiver_process(kafka, channel, f"{channel}-rx")
    worker: ManagedProcess | None = None
    try:
        rig.await_partitions_held(receiver)
        worker = rig.start_worker(kafka, channel, f"{channel}-wk")
        port = rig.listening_port(receiver)
        warm = rig.Sender(port)
        for index in range(10):
            assert warm.exchange(f"SYNTH-WARM-{index}").code == "AA"
        warm.close()

        requests: list[dict[str, Any]] = []
        lock = threading.Lock()
        failures: list[str] = []

        def run(sender_index: int) -> None:
            sender = rig.Sender(port)
            for index in range(PER_SENDER):
                control_id = f"SYNTH-TAIL-{sender_index}-{index}"
                wall = time.time()
                started = time.perf_counter()
                ack = sender.exchange(control_id)
                elapsed = (time.perf_counter() - started) * 1000
                with lock:
                    requests.append({"id": control_id, "start": wall, "ms": elapsed})
                    if ack.code != "AA":
                        failures.append(ack.code)
                time.sleep(THINK_S)
            sender.close()

        window_start = time.time()
        with HostSampler() as host:
            threads = [threading.Thread(target=run, args=(i,)) for i in range(SENDERS)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        window_end = time.time()

        assert failures == []
        assert len(requests) >= MIN_REQUESTS
        ordered = sorted(r["ms"] for r in requests)
        slow = sorted((r for r in requests if r["ms"] > SLOW_MS), key=lambda r: r["start"])
        receiver_lines = _json_lines(receiver)
        worker_lines = _json_lines(worker)
        broker_lines = _broker_lines(container, window_start)
        evidence = []
        for request in slow:
            low, high = request["start"] - WINDOW_BEFORE_S, request["start"] + request["ms"] / 1000 + WINDOW_AFTER_S
            evidence.append(
                {
                    **request,
                    "receiver_log": [line for t, line in receiver_lines if low <= t <= high],
                    "worker_log": [line for t, line in worker_lines if low <= t <= high],
                    "broker_log": [line for t, line in broker_lines if low <= t <= high],
                    "host": [s for s in host.samples if low <= s["t"] <= high],
                }
            )
        busy = sorted(s["busy_pct"] for s in host.samples) or [0.0]
        summary = {
            "n": len(ordered),
            "p50_ms": round(_percentile(ordered, 0.50), 1),
            "p90_ms": round(_percentile(ordered, 0.90), 1),
            "p99_ms": round(_percentile(ordered, 0.99), 1),
            "p999_ms": round(_percentile(ordered, 0.999), 1),
            "max_ms": round(ordered[-1], 1),
            "over_100ms": sum(1 for v in ordered if v > 100),
            "over_200ms": len(slow),
            "wall_s": round(window_end - window_start, 1),
            "host_busy_pct_median": busy[len(busy) // 2],
            "host_busy_pct_max": busy[-1],
            "host_steal_pct_max": max((s["steal_pct"] for s in host.samples), default=0.0),
            "host_iowait_pct_max": max((s["iowait_pct"] for s in host.samples), default=0.0),
            "receiver_log_lines": len(receiver_lines),
            "worker_log_lines": len(worker_lines),
            "broker_log_lines": len(broker_lines),
        }
        print(f"SYNC_REPLY_TAIL {json.dumps(summary, sort_keys=True)}")
        directory = Path(os.environ.get("TB_TAIL_REPORT_DIR", str(tmp_path)))
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"sync-reply-tail-{int(window_start)}.json"
        target.write_text(
            json.dumps(
                {"summary": summary, "slow": evidence, "requests": requests, "host_samples": host.samples}, indent=1
            ),
            encoding="utf-8",
        )
        assert target.exists()
    finally:
        for proc in (worker, receiver):
            if proc is not None:
                proc.terminate_and_wait()
