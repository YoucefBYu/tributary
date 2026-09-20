"""S3 spike (throwaway): synchronous reply receiver.

Extends the S2 MLLP server. Per message: register a pending future under a fresh correlation
id, publish to raw (acks=all, idempotent) with header tb-correlation-id, and write the
worker's reply to the sender when it arrives on the reply topic. Bounded everywhere: the
pending map is capped (over the cap -> immediate AR, nothing published), each entry has a hard
deadline, and a commit failure removes the entry and returns AR.

Invariant 1 still holds: the message is committed before any reply wait starts. A timeout NACK
means "committed but no answer"; the sender's retry is a duplicate downstream (invariant 4).

Reply consumption modes (--reply-mode):
  broadcast  ADR-001 6.5 literally: the reply topic is keyed by correlation id, so this instance
             reads ALL partitions (own assign(), no group, no commits) and drops replies that
             are not in its pending map. Cost grows with (replies x receiver instances).
  targeted   this instance owns one reply partition, advertised on raw in header
             tb-x-reply-partition; the worker produces the reply to exactly that partition.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass

from mllp import build_ack
from server import KafkaPublisher, Server, Settings

H_CORR, H_REPLY_PART = "tb-correlation-id", "tb-x-reply-partition"


@dataclass
class SyncStats:
    replied: int = 0
    timeouts: int = 0
    busy: int = 0
    commit_fail: int = 0
    late_replies: int = 0  # reply for an unknown corr id: after timeout, duplicate, or not ours
    pending: int = 0
    pending_hwm: int = 0
    replies_seen: int = 0  # every reply record read, matching or not (broadcast cost)
    rss_kb: int = 0


def rss_kb() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


class ReplyConsumer:
    """One consumer per receiver instance, polled on a thread; deliveries hop to the loop."""

    def __init__(self, bootstrap: str, topic: str, partitions: list[int] | None) -> None:
        from confluent_kafka import Consumer, TopicPartition

        self.c = Consumer({
            "bootstrap.servers": bootstrap, "group.id": f"s3-reply-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False, "isolation.level": "read_committed",
            "socket.keepalive.enable": True, "fetch.wait.max.ms": 5,
        })
        if partitions is None:
            md = self.c.list_topics(topic, timeout=15).topics[topic]
            partitions = sorted(md.partitions)
        self.parts = partitions
        # Concrete end offsets (not OFFSET_END): position stays unresolved on an idle topic, and a
        # reply produced right after the first publish must never be skipped.
        ends = [self.c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1]
                for p in partitions]
        self.c.assign([TopicPartition(topic, p, o) for p, o in zip(partitions, ends, strict=True)])
        self._stop = threading.Event()
        self.seen = 0

    def start(self, loop: asyncio.AbstractEventLoop, deliver: Callable[[str, bytes], None]) -> None:
        self.loop, self.deliver = loop, deliver
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while not self._stop.is_set():
            m = self.c.poll(0.05)
            if m is None or m.error():
                continue
            self.seen += 1
            corr = next((v for k, v in (m.headers() or []) if k == H_CORR), None)
            if corr is not None:
                self.loop.call_soon_threadsafe(self.deliver, corr.decode(), m.value())

    def close(self) -> None:
        self._stop.set()
        self.c.close()


class SyncServer(Server):
    def __init__(self, cfg: Settings, kp: KafkaPublisher, reply_timeout_s: float,
                 max_pending: int, timeout_code: str, instance_part: int | None) -> None:
        super().__init__(cfg, self._publish_and_wait)
        self.kp, self.reply_timeout_s, self.max_pending = kp, reply_timeout_s, max_pending
        self.timeout_code, self.instance_part = timeout_code, instance_part
        self.pending: dict[str, asyncio.Future[bytes]] = {}
        self.sstats = SyncStats()

    def deliver(self, corr: str, payload: bytes) -> None:
        fut = self.pending.get(corr)
        if fut is None or fut.done():
            self.sstats.late_replies += 1
        else:
            fut.set_result(payload)

    async def _publish_and_wait(self, frame: bytes, cid: str) -> bytes | bool:
        st = self.sstats
        if len(self.pending) >= self.max_pending:
            st.busy += 1
            return False
        corr = uuid.uuid4().hex
        fut: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self.pending[corr] = fut
        st.pending = len(self.pending)
        st.pending_hwm = max(st.pending_hwm, st.pending)
        t0 = time.monotonic()
        try:
            headers = [("tb-msg-id", uuid.uuid4().bytes), (H_CORR, corr.encode())]
            if self.instance_part is not None:
                headers.append((H_REPLY_PART, str(self.instance_part).encode()))
            if not await self.kp.publish(frame, corr, headers):
                st.commit_fail += 1
                return False
            left = self.reply_timeout_s - (time.monotonic() - t0)
            try:
                reply = await asyncio.wait_for(fut, max(left, 0.001))
            except TimeoutError:
                st.timeouts += 1
                return b""  # render() turns this into the configured NACK
            st.replied += 1
            return reply
        finally:
            self.pending.pop(corr, None)
            st.pending = len(self.pending)

    def render(self, cid: str, result: object) -> tuple[bytes, bool]:
        if isinstance(result, bytes) and result:
            return b"\x0b" + result + b"\x1c\x0d", True  # worker's response goes to the sender
        if result == b"":
            return build_ack(self.timeout_code, cid, "reply timeout"), False
        return build_ack("AR", cid, "unavailable"), False


async def amain(a: argparse.Namespace) -> None:
    loop = asyncio.get_running_loop()
    cfg = Settings(port=a.port, max_inflight=a.max_pending + 100, max_conns=1024,
                   conn_pipeline=a.conn_pipeline, ack_timeout_s=a.reply_timeout + 5)
    kp = KafkaPublisher(a.bootstrap, a.topic, a.linger_ms, 5000, (a.max_pending + 100) * 2)
    kp.start(loop)
    part = a.reply_partition if a.reply_mode == "targeted" else None
    rc = ReplyConsumer(a.bootstrap, a.reply_topic, [part] if part is not None else None)
    srv = SyncServer(cfg, kp, a.reply_timeout, a.max_pending, a.timeout_code, part)
    rc.start(loop, srv.deliver)
    s = await srv.start()
    print(json.dumps({"event": "listening", "reply_mode": a.reply_mode,
                      "reply_partitions": rc.parts}), flush=True)
    async with s:
        while True:
            await asyncio.sleep(1)
            srv.sstats.rss_kb, srv.sstats.replies_seen = rss_kb(), rc.seen
            if a.metrics_file:
                with open(a.metrics_file, "w") as f:
                    json.dump({**asdict(srv.stats), **asdict(srv.sstats)}, f)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka1:9092,kafka2:9092,kafka3:9092")
    p.add_argument("--topic", default="spike.s3.raw")
    p.add_argument("--reply-topic", default="spike.s3.reply")
    p.add_argument("--reply-mode", choices=["broadcast", "targeted"], default="broadcast")
    p.add_argument("--reply-partition", type=int, default=0)
    p.add_argument("--reply-timeout", type=float, default=10.0)
    p.add_argument("--timeout-code", choices=["AE", "AR"], default="AE")
    p.add_argument("--max-pending", type=int, default=20000)
    p.add_argument("--conn-pipeline", type=int, default=256)
    p.add_argument("--port", type=int, default=2575)
    p.add_argument("--linger-ms", type=int, default=2)
    p.add_argument("--metrics-file")
    a = p.parse_args()
    import uvloop

    uvloop.run(amain(a))


if __name__ == "__main__":
    main()
