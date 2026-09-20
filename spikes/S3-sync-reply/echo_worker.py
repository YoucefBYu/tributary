"""S3 STAND-IN worker (throwaway). platform-engineer owns the real worker; this one exists so the
receiver side can be measured now. Contract it implements (share with the real worker):

  in : <raw> record with header tb-correlation-id (and optional tb-x-reply-partition)
  out: <reply> record, header tb-correlation-id copied, payload = HL7 ACK whose MSA-2 is the
       request's MSH-10, key = correlation id. If tb-x-reply-partition is present the record is
       produced to exactly that partition (targeted mode).
  Offsets are committed only after the reply is delivered (at-least-once): a worker restart
  re-processes uncommitted requests, so the receiver may see duplicate or late replies.

Options simulate processing time and crashes; nothing here reads message content beyond MSH-10.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from mllp import build_ack, control_id


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka1:9092,kafka2:9092,kafka3:9092")
    p.add_argument("--raw", default="spike.s3.raw")
    p.add_argument("--reply", default="spike.s3.reply")
    p.add_argument("--group", default="s3-worker")
    p.add_argument("--delay-ms", type=float, default=0.0)
    a = p.parse_args()
    c = Consumer({
        "bootstrap.servers": a.bootstrap, "group.id": a.group, "enable.auto.commit": False,
        "auto.offset.reset": "earliest", "isolation.level": "read_committed",
        "partition.assignment.strategy": "cooperative-sticky", "fetch.wait.max.ms": 5,
        "session.timeout.ms": 10000,
    })
    pr = Producer({"bootstrap.servers": a.bootstrap, "enable.idempotence": True, "acks": "all",
                   "linger.ms": 2})
    c.subscribe([a.raw])
    stop = False

    def _term(*_: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _term)
    print('{"event": "worker-up"}', flush=True)
    while not stop:
        msgs = c.consume(200, 0.05)
        if not msgs:
            continue
        pending = 0
        last: dict[int, int] = {}

        def done(err: object, _m: object) -> None:
            nonlocal pending
            pending -= 1
            if err is not None:
                print('{"event": "reply-delivery-failed"}', flush=True)
                os._exit(3)  # crash rather than commit an undelivered reply

        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    print('{"event": "consume-error"}', flush=True)
                continue
            h = dict(m.headers() or [])
            corr = h.get("tb-correlation-id")
            if corr is None:
                continue
            if a.delay_ms:
                time.sleep(a.delay_ms / 1000)
            part = int(h["tb-x-reply-partition"]) if "tb-x-reply-partition" in h else -1
            ack = build_ack("AA", control_id(m.value()))
            pending += 1
            pr.produce(a.reply, ack[1:-2], key=corr, headers=[("tb-correlation-id", corr)],
                       partition=part, on_delivery=done)
            last[m.partition()] = m.offset()
        while pr.flush(5) > 0 and not stop:
            pass
        if last:
            c.commit(offsets=[TopicPartition(msgs[0].topic(), pt, off + 1)
                              for pt, off in last.items()], asynchronous=False)
    c.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
