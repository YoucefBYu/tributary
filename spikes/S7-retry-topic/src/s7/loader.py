"""A `retry-topic` loader for one destination, as its own OS process (so it can really be killed).

It consumes `out.<dest>` and the retry topic(s) in one consumer group, attempts delivery against a
fake destination driven by `s7.plan.FailurePlan`, and on failure writes the record to the retry
topic with `tb-attempt` incremented and `tb-retry-not-before` set - or to the DLQ once
`max_attempts` is reached. The input offset is committed only after every retry/DLQ record it
produced has a delivery report (flush-before-commit), which is the invariant-1/4 rule the runtime
already applies to `out.<dest>`.

Mechanisms (see `s7.__init__`): `pause`, `buffer`, `tiers`.

Everything the runner needs is written to disk as it happens, because a killed process reports
nothing: one JSONL journal line per delivery attempt, per DLQ write and per state sample, and a
stats file written **only** on a clean exit - its absence is the proof of a kill.

Nothing from a payload or a key is ever written: ids, a key ordinal and a sequence number only.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, TextIO

import orjson
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from tbcommon import clients

from .plan import (
    H_ATTEMPT,
    H_DESTINATION,
    H_ERROR_CODE,
    H_KEY_ORDINAL,
    H_MSG_ID,
    H_NOT_BEFORE,
    H_SEQ,
    H_STAGE,
    FailurePlan,
    Tiers,
    format_not_before,
    parse_not_before,
)

TP = tuple[str, int]


@dataclass(order=True, slots=True)
class Held:
    """A retry record read before it was due (`buffer` mechanism)."""

    not_before: float
    order: int
    tp: TP = field(compare=False)
    offset: int = field(compare=False)
    generation: int = field(compare=False)
    key: bytes | None = field(compare=False)
    value: bytes | None = field(compare=False)
    headers: list[tuple[str, bytes]] = field(compare=False)


@dataclass(slots=True)
class Stats:
    attempts: int = 0
    delivered: int = 0
    retried: int = 0
    dead_lettered: int = 0
    pauses: int = 0
    commits: int = 0
    reenqueued: int = 0
    compactions: int = 0
    max_buffered: int = 0


def _header(headers: list[tuple[str, bytes]], name: str) -> str | None:
    for key, value in headers:
        if key == name and value is not None:
            return value.decode("utf-8")
    return None


def _with(headers: list[tuple[str, bytes]], **updates: str) -> list[tuple[str, bytes]]:
    names = {name.replace("_", "-") for name in updates}
    kept = [(key, value) for key, value in headers if key not in names]
    return kept + [(name.replace("_", "-"), value.encode("utf-8")) for name, value in updates.items()]


class Loader:
    def __init__(self, args: argparse.Namespace, plan: FailurePlan, journal: TextIO) -> None:
        self.args = args
        self.plan = plan
        self.tiers = Tiers(tuple(float(value) for value in args.delays.split(",")))
        self.journal = journal
        self.worker = args.instance_id
        self.mechanism: str = args.mechanism
        self.retry_topics: list[str] = args.retry_topic
        self.stats = Stats()
        overrides: dict[str, Any] = {
            "session.timeout.ms": 15_000,
            "heartbeat.interval.ms": 5_000,
            "enable.partition.eof": False,
            # Windows: `localhost` resolves to ::1 first and each failed IPv6 connect costs ~2 s, which
            # is enough to make a group join outlast a short idle timer.
            "broker.address.family": "v4",
        }
        if args.fetch_wait_ms is not None:
            overrides["fetch.wait.max.ms"] = args.fetch_wait_ms
        self.consumer = Consumer(
            clients.consumer_config(args.group, instance_id=args.instance_id, read_committed=False, **overrides)
        )
        self.producer = Producer(clients.producer_config(**{"broker.address.family": "v4"}))
        self.produce_errors: list[str] = []
        # Commit bookkeeping. `inline` partitions are processed strictly in offset order; `buffer`
        # retry partitions commit a watermark: the lowest offset still held in memory.
        self.inline_next: dict[TP, int] = {}
        self.read_next: dict[TP, int] = {}
        self.outstanding: dict[TP, set[int]] = {}
        self.committed: dict[TP, int] = {}
        self.generation: dict[TP, int] = {}
        self.held: list[Held] = []
        self.order = itertools.count()
        self.paused_until: dict[TP, float] = {}
        self.buffer_paused: set[TP] = set()
        self.since_commit = 0
        self.last_commit = time.monotonic()
        self.last_state = 0.0
        self.assigned_once = False
        self.swapped: dict[TP, tuple[float, bytes | None, bytes | None, list[tuple[str, bytes]], int]] = {}

    # -- journal -------------------------------------------------------------------------------

    def _write(self, record: dict[str, Any]) -> None:
        self.journal.write(orjson.dumps(record).decode("utf-8") + "\n")
        self.journal.flush()

    # -- rebalance -----------------------------------------------------------------------------

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        consumer.incremental_assign(partitions)
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self.generation[key] = self.generation.get(key, 0) + 1
            for table in (self.inline_next, self.read_next, self.outstanding, self.committed):
                table.pop(key, None)  # type: ignore[attr-defined]
            self.paused_until.pop(key, None)
            self.buffer_paused.discard(key)
        self.assigned_once = True
        self._write({"ts": time.time(), "w": self.worker, "assign": len(partitions)})

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        keys = [(tp.topic, tp.partition) for tp in partitions]
        if self.mechanism == "buffer" and not self.args.no_reenqueue:
            self._reenqueue(keys)
        self._commit(force=True)
        self._forget(keys)
        consumer.incremental_unassign(partitions)
        self._write({"ts": time.time(), "w": self.worker, "revoke": len(partitions)})

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        self._forget([(tp.topic, tp.partition) for tp in partitions])
        consumer.incremental_unassign(partitions)
        self._write({"ts": time.time(), "w": self.worker, "lost": len(partitions)})

    def _forget(self, keys: list[TP]) -> None:
        for key in keys:
            self.generation[key] = self.generation.get(key, 0) + 1  # invalidates held entries
            for table in (self.inline_next, self.read_next, self.outstanding, self.committed):
                table.pop(key, None)  # type: ignore[attr-defined]
            self.paused_until.pop(key, None)
            self.buffer_paused.discard(key)

    def _reenqueue(self, keys: list[TP]) -> None:
        """Hand the held records back to the log instead of replaying everything above the watermark.

        Each held record is re-produced unchanged (same attempt, same not-before) to the retry topic,
        flushed, and only then is the partition committed at its read position. A crash between the
        two leaves both copies, which is one duplicate retry chain for that record - not a loss.
        """
        wanted = set(keys)
        moved = 0
        for entry in self.held:
            if entry.tp in wanted and entry.generation == self.generation.get(entry.tp):
                self._produce(self.retry_topics[0], entry.key, entry.value, entry.headers)
                self._write(
                    {
                        "ts": time.time(),
                        "w": self.worker,
                        "moved": _header(entry.headers, H_MSG_ID),
                        "n": int(_header(entry.headers, H_ATTEMPT) or "0"),
                        "t": entry.tp[0],
                        "p": entry.tp[1],
                        "o": entry.offset,
                    }
                )
                entry.generation = -1
                moved += 1
        self._flush()
        for key in keys:
            if key in self.outstanding:
                self.outstanding[key].clear()
        self.stats.reenqueued += moved

    # -- producing -----------------------------------------------------------------------------

    def _on_delivery(self, msg_id: str | None, attempt: int, err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            self.produce_errors.append(str(err))
            return
        # The broker acknowledged a retry or DLQ record. The commit-order check needs the time of this,
        # not of produce(): a commit is only safe once the record it depends on is durable.
        # The id and attempt are bound at produce time because a delivery report's `headers()` is None
        # (confluent-kafka 2.15): reading them from `msg` journals every ack as anonymous.
        self._write(
            {
                "ts": time.time(),
                "w": self.worker,
                "acked": msg_id,
                "n": attempt,
                "dlq": msg.topic() == self.args.dlq_topic,
            }
        )

    def _produce(self, topic: str, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]]) -> None:
        msg_id = _header(headers, H_MSG_ID)
        attempt = int(_header(headers, H_ATTEMPT) or "0")
        self.producer.produce(
            topic,
            value=value,
            key=key,
            headers=headers,
            on_delivery=lambda err, msg: self._on_delivery(msg_id, attempt, err, msg),
        )
        self.producer.poll(0)

    def _flush(self) -> None:
        remaining = self.producer.flush(30)
        if remaining or self.produce_errors:
            raise RuntimeError(f"produce failed: {remaining} queued, {len(self.produce_errors)} errors")

    # -- commit --------------------------------------------------------------------------------

    def _committable(self) -> dict[TP, int]:
        offsets = dict(self.inline_next)
        for key, read in self.read_next.items():
            held = self.outstanding.get(key)
            if self.args.mutate == "commit-high" or not held:
                offsets[key] = read
            else:
                offsets[key] = min(held)
        return offsets

    def _commit(self, *, force: bool = False) -> None:
        due = self.since_commit >= self.args.commit_every or time.monotonic() - self.last_commit >= 1.0
        if not (force or due):
            return
        changed = {key: offset for key, offset in self._committable().items() if self.committed.get(key) != offset}
        self.last_commit = time.monotonic()
        if not changed:
            return
        # Flush before commit: a retry or DLQ record still in a buffer would make this commit a loss.
        if self.args.mutate != "commit-unflushed":
            self._flush()
        self.consumer.commit(
            offsets=[TopicPartition(topic, part, offset) for (topic, part), offset in changed.items()],
            asynchronous=False,
        )
        self.committed.update(changed)
        self._write(
            {
                "ts": time.time(),
                "w": self.worker,
                "commit": [[topic, part, offset] for (topic, part), offset in changed.items()],
            }
        )
        self.stats.commits += 1
        self.since_commit = 0

    # -- delivery ------------------------------------------------------------------------------

    def _attempt(
        self, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]], src: str, tp: TP, offset: int
    ) -> None:
        msg_id = _header(headers, H_MSG_ID) or ""
        retry_number = int(_header(headers, H_ATTEMPT) or "0")
        number = retry_number + 1
        not_before_text = _header(headers, H_NOT_BEFORE)
        now = time.time()
        failed = self.plan.fails(msg_id, number, now)
        self.stats.attempts += 1
        self._write(
            {
                "ts": now,
                "w": self.worker,
                "id": msg_id,
                "k": int(_header(headers, H_KEY_ORDINAL) or "-1"),
                "s": int(_header(headers, H_SEQ) or "-1"),
                "n": number,
                "nb": parse_not_before(not_before_text) if not_before_text else None,
                "ok": not failed,
                "src": src,
                "t": tp[0],
                "p": tp[1],
                "o": offset,
            }
        )
        if not failed:
            self.stats.delivered += 1
            return
        exhausted_at = self.args.max_attempts - 1 if self.args.mutate == "dlq-early" else self.args.max_attempts
        if number >= exhausted_at:
            dlq_headers = _with(headers, **{H_STAGE: "pending", H_ERROR_CODE: "DESTINATION_UNAVAILABLE"})
            dlq_headers = [(k, v) for k, v in dlq_headers if k != H_NOT_BEFORE]
            self._produce(self.args.dlq_topic, msg_id.encode("utf-8"), value, dlq_headers)
            self.stats.dead_lettered += 1
            self._write({"ts": now, "w": self.worker, "id": msg_id, "dlq": 1})
            return
        if self.args.mutate == "drop-retry":
            return  # the mutation: a failed record is committed with no retry record behind it
        next_retry = number
        not_before = now + self.tiers.delay(next_retry)
        topic = self.retry_topics[self.tiers.tier(next_retry)] if self.mechanism == "tiers" else self.retry_topics[0]
        retry_headers = _with(
            headers,
            **{H_ATTEMPT: str(next_retry), H_NOT_BEFORE: format_not_before(not_before), H_DESTINATION: "dest"},
        )
        self._produce(topic, key, value, retry_headers)
        self.stats.retried += 1

    # -- the loop ------------------------------------------------------------------------------

    def _handle(self, msg: Any) -> None:
        tp: TP = (msg.topic(), msg.partition())
        offset = msg.offset()
        headers = list(msg.headers() or [])
        if tp[0] == self.args.out_topic:
            if self.args.mutate == "swap-out":
                self._swap_out(msg, tp, offset, headers)
                return
            self._attempt(msg.key(), msg.value(), headers, "out", tp, offset)
            self.inline_next[tp] = offset + 1
            self.since_commit += 1
            return

        not_before_text = _header(headers, H_NOT_BEFORE)
        not_before = parse_not_before(not_before_text) if not_before_text else 0.0
        now = time.time()
        if self.mechanism == "buffer":
            self.read_next[tp] = offset + 1
            if not_before <= now:
                self._attempt(msg.key(), msg.value(), headers, "retry", tp, offset)
                self.since_commit += 1
                return
            self.outstanding.setdefault(tp, set()).add(offset)
            heapq.heappush(
                self.held,
                Held(
                    not_before,
                    next(self.order),
                    tp,
                    offset,
                    self.generation.get(tp, 0),
                    msg.key(),
                    msg.value(),
                    headers,
                ),
            )
            live = sum(len(held) for held in self.outstanding.values())
            self.stats.max_buffered = max(self.stats.max_buffered, live)
            if live >= self.args.buffer_limit and tp not in self.buffer_paused:
                partition = TopicPartition(tp[0], tp[1], offset + 1)
                self.consumer.pause([partition])
                self.consumer.seek(partition)
                self.buffer_paused.add(tp)
                self.stats.pauses += 1
            return

        # pause / tiers: a record that is not due stops its partition until it is.
        if not_before > now:
            partition = TopicPartition(tp[0], tp[1], offset)
            self.consumer.pause([partition])
            self.consumer.seek(partition)  # while paused: pause() purged the prefetch (S5)
            self.paused_until[tp] = not_before
            self.stats.pauses += 1
            return
        self._attempt(msg.key(), msg.value(), headers, "retry", tp, offset)
        self.inline_next[tp] = offset + 1
        self.since_commit += 1

    def _run_due(self) -> float | None:
        """Deliver or resume whatever is due. Returns the next due time, if any."""
        now = time.time()
        self._release_swapped()
        if self.mechanism == "buffer":
            while self.held:
                head = self.held[0]
                stale = head.generation != self.generation.get(head.tp)  # revoked, lost or re-enqueued
                if not stale and head.not_before > now:
                    break
                entry = heapq.heappop(self.held)
                if stale:
                    continue
                self._attempt(entry.key, entry.value, entry.headers, "retry", entry.tp, entry.offset)
                self.outstanding[entry.tp].discard(entry.offset)
                self.since_commit += 1
            live = sum(len(held) for held in self.outstanding.values())
            if self.buffer_paused and live < self.args.buffer_limit // 2:
                self.consumer.resume([TopicPartition(t, p) for t, p in self.buffer_paused])
                self.buffer_paused.clear()
            if self.args.compact_at and self._exposure() > self.args.compact_at:
                self._compact()
            return self.held[0].not_before if self.held else None

        for tp, resume_at in list(self.paused_until.items()):
            if resume_at <= now:
                self.consumer.resume([TopicPartition(tp[0], tp[1])])
                del self.paused_until[tp]
                self._write({"ts": now, "w": self.worker, "resume": 1, "t": tp[0], "p": tp[1], "at": resume_at})
        return min(self.paused_until.values()) if self.paused_until else None

    def _exposure(self) -> int:
        """Records read above the lowest held offset: roughly what a crash now would replay."""
        total = 0
        for key, held in self.outstanding.items():
            if held:
                total += self.read_next[key] - min(held) - len(held)
        return total

    def _compact(self) -> None:
        keys = [key for key, held in self.outstanding.items() if held]
        self._reenqueue(keys)
        self._commit(force=True)
        self.stats.compactions += 1

    def _swap_out(self, msg: Any, tp: TP, offset: int, headers: list[tuple[str, bytes]]) -> None:
        """Mutation: deliver every tenth `out` record after the one behind it. Never-failed order breaks."""
        held = self.swapped.pop(tp, None)
        if held is None and offset % 10 == 0:
            self.swapped[tp] = (time.monotonic(), msg.key(), msg.value(), headers, offset)
            return
        self._attempt(msg.key(), msg.value(), headers, "out", tp, offset)
        if held is not None:
            _at, key, value, held_headers, held_offset = held
            self._attempt(key, value, held_headers, "out", tp, held_offset)
        self.inline_next[tp] = offset + 1
        self.since_commit += 1

    def _release_swapped(self) -> None:
        for tp, (at, key, value, headers, offset) in list(self.swapped.items()):
            if time.monotonic() - at > 0.5:
                del self.swapped[tp]
                self._attempt(key, value, headers, "out", tp, offset)
                self.inline_next[tp] = offset + 1

    def run(self) -> Stats:
        topics = [self.args.out_topic, *self.retry_topics]
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_lost)
        started = time.monotonic()
        last_message = time.monotonic()
        while time.monotonic() - started < self.args.max_runtime_s:
            next_due = self._run_due()
            now = time.time()
            if self.args.naive_poll or next_due is None:
                timeout = self.args.poll_timeout_s
            else:
                timeout = max(0.0, min(self.args.poll_timeout_s, next_due - now))
            if now - self.last_state >= 0.5:
                self.last_state = now
                self._write(
                    {
                        "ts": now,
                        "w": self.worker,
                        "state": 1,
                        "held": sum(len(held) for held in self.outstanding.values()),
                        "paused": len(self.paused_until) + len(self.buffer_paused),
                        "attempts": self.stats.attempts,
                    }
                )
            msg = self.consumer.poll(timeout)
            if msg is not None:
                if msg.error() is not None:
                    if msg.error().fatal():
                        raise RuntimeError("fatal consumer error")
                else:
                    last_message = time.monotonic()
                    self._handle(msg)
            self._commit()
            if not self.assigned_once:
                last_message = time.monotonic()  # the idle clock starts at the first assignment
            idle = time.monotonic() - last_message >= self.args.idle_exit_s
            if idle and not self.held and not self.paused_until and not self.buffer_paused and not self.swapped:
                break
        self._commit(force=True)
        self.consumer.close()
        return self.stats


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="s7.loader")
    p.add_argument("--out-topic", required=True)
    p.add_argument("--retry-topic", action="append", required=True, help="one, or one per tier for `tiers`")
    p.add_argument("--dlq-topic", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--instance-id", required=True)
    p.add_argument("--mechanism", choices=["pause", "buffer", "tiers"], required=True)
    p.add_argument("--delays", default="1,4,12", help="seconds per retry tier")
    p.add_argument("--max-attempts", type=int, default=5)
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--journal", required=True, type=Path)
    p.add_argument("--stats", required=True, type=Path, help="written on clean exit only")
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--poll-timeout-s", type=float, default=0.2)
    p.add_argument("--naive-poll", action="store_true", help="control: resume only at poll-timeout granularity")
    p.add_argument("--fetch-wait-ms", type=int, default=None)
    p.add_argument("--buffer-limit", type=int, default=100_000)
    p.add_argument("--compact-at", type=int, default=0, help="buffer: re-enqueue held records past this exposure")
    p.add_argument("--no-reenqueue", action="store_true", help="buffer: on revoke, drop held records instead")
    p.add_argument(
        "--mutate",
        choices=["none", "drop-retry", "commit-high", "commit-unflushed", "dlq-early", "swap-out"],
        default="none",
        help="deliberately broken behaviour, one per headline check, so each check is shown able to fail",
    )
    p.add_argument("--idle-exit-s", type=float, default=8.0)
    p.add_argument("--max-runtime-s", type=float, default=600.0)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    plan = FailurePlan.from_json(orjson.loads(args.plan.read_bytes()))
    args.journal.parent.mkdir(parents=True, exist_ok=True)
    with args.journal.open("a", encoding="utf-8") as journal:
        stats = Loader(args, plan, journal).run()
    args.stats.write_bytes(orjson.dumps({"pid": os.getpid(), "stats": asdict(stats)}))
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
