"""W3-33b B1: receiver ACK latency, RF=3 vs RF=1, as a ratio, on one host and one cluster.

Both topics live on the same 3-broker cluster and are served by their own real MLLP receiver (same code, same
contract producer profile). The only difference is the topic: RF=3 with `min.insync.replicas=2` (contract
values) against RF=1 with `min.insync.replicas=1`. Phases alternate RF1, RF3, RF1, RF3, ... so host drift hits
both arms equally; the first phase pair is a discarded warm-up.

Load: 8 persistent MLLP connections, each sending at a fixed schedule (offered about 1000 msg/s in total),
latency measured per message from its scheduled send time to `MSA|AA` (coordinated omission avoided: a stalled
ACK is charged to the messages that were due meanwhile). When the host cannot keep the schedule the achieved rate
is below the offered one and the row says so. The result is a ratio (p50 and p99); absolute latencies are
printed as raw rows only, because they are single-host numbers and prove nothing about dedicated capacity.

Marked `bench`: it reports, it asserts only that every phase ACKed and that the ratio is finite.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from benchmarks.harness import environment, percentile
from tests.support.kafka_cluster import Rf3Cluster, rf3_cluster
from tests.support.rf3_harness import Rig, keyed_message, start_rig

pytestmark = [pytest.mark.chaos, pytest.mark.bench, pytest.mark.rf3]

CONNS = 8
OFFERED_TOTAL = int(os.environ.get("TB_RF3_B1_RATE", "1000"))  # msg/s across all connections
OFFERED_PER_CONN = OFFERED_TOTAL / CONNS
PHASE_S = 8.0
PAIRS = 3  # measured RF1/RF3 pairs, after one discarded warm-up pair
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


@pytest.fixture(scope="module")
def cluster() -> Iterator[Rf3Cluster]:
    with rf3_cluster("combined") as c:
        yield c


def _conn_loop(port: int, conn: int, deadline: float, out: list[float], failures: list[int]) -> None:
    interval = 1.0 / OFFERED_PER_CONN
    patient = conn % 8
    with socket.create_connection(("127.0.0.1", port), timeout=30) as sock:
        sequence = 0
        end = time.perf_counter() + (deadline - time.monotonic())
        next_at = time.perf_counter()  # the SCHEDULED send time; latency is measured from it, never from the
        while next_at < end:  # actual send, so a stalled ACK charges the messages queued behind it
            delay = next_at - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            frame = SB + keyed_message(patient, sequence).encode("ascii") + EB + CR
            sequence += 1
            started = next_at
            sock.sendall(frame)
            buf = b""
            while not buf.endswith(EB + CR):
                chunk = sock.recv(4096)
                if not chunk:
                    failures.append(1)
                    return
                buf += chunk
            if b"MSA|AA" in buf:
                out.append((time.perf_counter() - started) * 1000.0)
            else:
                failures.append(1)
            next_at += interval


def _phase(rig: Rig) -> dict[str, float]:
    latencies: list[list[float]] = [[] for _ in range(CONNS)]
    failures: list[int] = []
    deadline = time.monotonic() + PHASE_S
    threads = [
        threading.Thread(target=_conn_loop, args=(rig.port, i, deadline, latencies[i], failures)) for i in range(CONNS)
    ]
    started = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started
    flat = sorted(v for lane in latencies for v in lane)
    assert flat and not failures, f"phase had {len(failures)} non-ACKed message(s) and {len(flat)} ACKs"
    return {
        "n": float(len(flat)),
        "achieved_msg_s": len(flat) / elapsed,
        "p50_ms": percentile(flat, 0.50),
        "p99_ms": percentile(flat, 0.99),
    }


def test_rf3_vs_rf1_ack_latency_ratio(cluster: Rf3Cluster) -> None:
    rf1 = start_rig(cluster, "b1-rf1", replication_factor=1, topic_overrides={"min.insync.replicas": "1"})
    rf3 = start_rig(cluster, "b1-rf3")
    rows: list[dict[str, object]] = []
    try:
        for pair in range(PAIRS + 1):
            for label, rig in (("rf1", rf1), ("rf3", rf3)):
                result = _phase(rig)
                rows.append({"pair": pair, "arm": label, "warmup": pair == 0, **result})
    finally:
        rf1.close()
        rf3.close()
    measured = [r for r in rows if not r["warmup"]]

    def arm(label: str, key: str) -> list[float]:
        return [float(r[key]) for r in measured if r["arm"] == label]  # type: ignore[arg-type]

    summary: dict[str, object] = {"offered_msg_s": OFFERED_TOTAL, "phase_s": PHASE_S}
    for label in ("rf1", "rf3"):
        summary[f"{label}_achieved_msg_s"] = [round(v) for v in arm(label, "achieved_msg_s")]
    for key in ("p50_ms", "p99_ms"):
        ratios = [b / a for a, b in zip(arm("rf1", key), arm("rf3", key), strict=True)]
        summary[f"{key[:-3]}_ratio_per_pair"] = [round(r, 2) for r in ratios]
        summary[f"{key[:-3]}_ratio_median"] = round(sorted(ratios)[len(ratios) // 2], 2)
    row = {"scenario": "rf3-b1-ack-latency-ratio", "env": environment(), "summary": summary, "rows": rows}
    line = json.dumps(row, sort_keys=True, default=str)
    print(f"RF3_B1_ROW {line}")
    path = os.environ.get("TB_CHAOS_ROWS_FILE")
    if path:
        with Path(path).open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    assert all(float(r["p50_ms"]) > 0 for r in rows)  # type: ignore[arg-type]
    assert float(summary["p50_ratio_median"]) > 0  # type: ignore[arg-type]
