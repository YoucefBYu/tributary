"""P0-2 verification: produce/consume smoke test against the spike cluster.

Run:  uv run --project spikes/common python -m tbcommon.smoke

Checks, in order:
  1. three brokers visible;
  2. topic creates with RF=3 / min.insync=2 and gets leaders;
  3. N keyed synthetic messages produce with acks=all + idempotence;
  4. the same N come back with envelope headers intact;
  5. per-key order is preserved and nothing is lost;
  6. manual offset commit after consumption succeeds.

Prints one JSON line. Exit 0 = healthy cluster. No payload bytes are printed.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

import orjson
from confluent_kafka import TopicPartition

from . import admin, checks, config, envelope, payloads
from .clients import flush_or_raise, make_consumer, make_producer

log = logging.getLogger("tbcommon.smoke")


def run(count: int, keys: int, timeout: float) -> dict[str, object]:
    topic = f"{config.topic_prefix()}.smoke"
    group = f"{config.topic_prefix()}-smoke-{int(time.time())}"
    result: dict[str, object] = {"topic": topic, "group": group}

    t0 = time.monotonic()
    brokers = admin.wait_for_cluster(expected_brokers=3, timeout=timeout)
    result["brokers"] = brokers
    result["cluster_ready_s"] = round(time.monotonic() - t0, 3)

    admin.ensure_topics(
        [
            admin.TopicSpec(
                topic,
                partitions=6,
                replication_factor=config.replication_factor(),
                configs={"retention.ms": "600000"},
            )
        ]
    )
    result["topic_config"] = {
        k: v
        for k, v in admin.topic_config(topic).items()
        if k in {"min.insync.replicas", "retention.ms"}
    }
    result["leaders"] = admin.partition_leaders(topic)

    # --- produce -----------------------------------------------------------
    producer = make_producer()
    expected: list[checks.Observation] = []
    failures: list[str] = []

    def on_delivery(err: object, msg: object) -> None:
        if err is not None:
            failures.append(str(err))

    t0 = time.monotonic()
    for key, seq, payload in payloads.stream(count, keys, seed=1):
        env = envelope.Envelope(channel="smoke", seq=seq, stage="produced")
        expected.append(checks.Observation(key=key, seq=seq, msg_id=env.msg_id))
        producer.produce(
            topic,
            key=key.encode(),
            value=payload,
            headers=env.to_headers(),
            on_delivery=on_delivery,
        )
        producer.poll(0)
    flush_or_raise(producer, timeout=timeout)
    result["produce_s"] = round(time.monotonic() - t0, 3)
    result["produce_errors"] = failures
    if failures:
        raise RuntimeError(f"{len(failures)} delivery failure(s); first: {failures[0]}")

    # --- consume -----------------------------------------------------------
    consumer = make_consumer(group, [topic])
    observed: list[checks.Observation] = []
    missing_headers: list[str] = []
    deadline = time.monotonic() + timeout
    t0 = time.monotonic()
    try:
        while len(observed) < count and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                raise RuntimeError(f"consume error: {msg.error()}")
            env = envelope.Envelope.from_headers(msg.headers())
            if not env.msg_id or not env.traceparent or env.seq is None:
                missing_headers.append(f"{msg.partition()}:{msg.offset()}")
                continue
            observed.append(
                checks.Observation(
                    key=(msg.key() or b"").decode(), seq=env.seq, msg_id=env.msg_id
                )
            )
            consumer.store_offsets(msg)
        # Invariant: commit only after everything above succeeded.
        committed = consumer.commit(asynchronous=False) or []
        result["committed"] = [
            {"partition": tp.partition, "offset": tp.offset}
            for tp in committed
            if isinstance(tp, TopicPartition)
        ]
    finally:
        consumer.close()

    result["consume_s"] = round(time.monotonic() - t0, 3)
    result["missing_headers"] = missing_headers

    report = checks.check(observed, expected=expected)
    result["delivery"] = report.summary()

    ok = (
        len(observed) == count
        and report.no_loss
        and report.order_preserved
        and not missing_headers
        and report.duplicates == 0
    )
    result["ok"] = ok

    admin.delete_topics([topic])
    return result


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Tributary spike cluster smoke test")
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--keys", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args(argv)

    try:
        result = run(args.count, args.keys, args.timeout)
    except Exception as exc:  # noqa: BLE001 - top-level CLI boundary
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 1

    # OPT_NON_STR_KEYS: `leaders` is partition:int -> leader:int. Without it
    # orjson raises "Dict key must be str" and the whole run reports nothing.
    sys.stdout.write(orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS).decode() + "\n")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
