"""The headline checks, as pure functions over a run's journal.

Pure so that each one is unit-tested against a hand-built journal that must pass and one that must
fail (`tests/test_checks.py`); a check that has never been seen to fail proves nothing
(PHASE-0-DECISION section 4). The cluster runs then add a deliberately broken loader per check.

Journal line shapes (all carry `ts` and `w`, the worker):

* attempt  `{id, k, s, n, nb, ok, src, t, p, o}` - one delivery attempt of record `(t, p, o)`;
* ack      `{acked, n, dlq}` - the broker acknowledged a retry record (`tb-attempt` = n) or a DLQ one;
* moved    `{moved, n, t, p, o}` - a held record was re-enqueued (same `tb-attempt`) instead of attempted;
* commit   `{commit: [[t, p, offset], ...]}` - a synchronous offset commit this loader made;
* resume   `{resume, t, p, at}` - a paused partition was resumed; `at` is when it was due.
"""

from __future__ import annotations

import bisect
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .model import percentile

Coord = tuple[str, int, int]


@dataclass(slots=True)
class Evidence:
    attempts: list[dict[str, Any]] = field(default_factory=list)
    acks: list[dict[str, Any]] = field(default_factory=list)
    moves: list[dict[str, Any]] = field(default_factory=list)
    commits: list[tuple[float, str, int, int]] = field(default_factory=list)
    resumes: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_lines(cls, lines: Iterable[Mapping[str, Any]]) -> Evidence:
        evidence = cls()
        for line in lines:
            if "acked" in line:
                evidence.acks.append(dict(line))
            elif "moved" in line:
                evidence.moves.append(dict(line))
            elif "commit" in line:
                evidence.commits.extend((line["ts"], t, p, offset) for t, p, offset in line["commit"])
            elif "resume" in line:
                evidence.resumes.append(dict(line))
            elif "n" in line and "id" in line:
                evidence.attempts.append(dict(line))
        return evidence


def resolution_times(evidence: Evidence) -> dict[Coord, float]:
    """When each consumed record became safe to commit past.

    A successful attempt resolves its record at once. A failed attempt resolves it only when the
    record it caused - the next retry (`tb-attempt` = n) or the DLQ record - is acknowledged by the
    broker. A re-enqueued record resolves when its copy is acknowledged. Replays mean one coordinate
    can have several attempts; the earliest resolution counts.
    """
    acks: dict[tuple[str, int | str], list[float]] = defaultdict(list)
    for ack in evidence.acks:
        acks[(ack["acked"], "dlq" if ack["dlq"] else ack["n"])].append(ack["ts"])
    for times in acks.values():
        times.sort()

    def first_after(key: tuple[str, int | str], since: float) -> float:
        times = acks.get(key, [])
        index = bisect.bisect_left(times, since)
        return times[index] if index < len(times) else math.inf

    resolved: dict[Coord, float] = {}
    for attempt in evidence.attempts:
        coord = (attempt["t"], attempt["p"], attempt["o"])
        if attempt["ok"]:
            at = attempt["ts"]
        else:
            at = min(
                first_after((attempt["id"], attempt["n"]), attempt["ts"]),
                first_after((attempt["id"], "dlq"), attempt["ts"]),
            )
        resolved[coord] = min(resolved.get(coord, math.inf), at)
    for move in evidence.moves:
        coord = (move["t"], move["p"], move["o"])
        at = first_after((move["moved"], move["n"]), move["ts"])
        resolved[coord] = min(resolved.get(coord, math.inf), at)
    return resolved


def commit_violations(
    resolved: Mapping[Coord, float], commits: Iterable[tuple[float, str, int, int]]
) -> list[tuple[float, str, int, int, int]]:
    """Commits that moved past a record not yet resolved at commit time.

    Returns `(commit_ts, topic, partition, committed_offset, first_unresolved_offset)`. An offset with
    no evidence at all counts as never resolved, which is the conservative reading.
    """
    by_partition: dict[tuple[str, int], dict[int, float]] = defaultdict(dict)
    for (topic, part, offset), at in resolved.items():
        by_partition[(topic, part)][offset] = at
    prefix: dict[tuple[str, int], list[float]] = {}
    for key, offsets in by_partition.items():
        running, highest = [], -math.inf
        for offset in range(max(offsets) + 1):
            highest = max(highest, offsets.get(offset, math.inf))
            running.append(highest)
        prefix[key] = running

    violations = []
    for ts, topic, part, committed in commits:
        if committed <= 0:
            continue
        running = prefix.get((topic, part), [])
        latest = running[committed - 1] if committed - 1 < len(running) else math.inf
        if latest > ts:
            offsets = by_partition.get((topic, part), {})
            first_bad = next((o for o in range(committed) if offsets.get(o, math.inf) > ts), committed - 1)
            violations.append((ts, topic, part, committed, first_bad))
    return violations


def first_ok(evidence: Evidence) -> dict[str, float]:
    times: dict[str, float] = {}
    for attempt in evidence.attempts:
        if attempt["ok"]:
            times[attempt["id"]] = min(times.get(attempt["id"], math.inf), attempt["ts"])
    return times


def never_failed_inversions(evidence: Evidence, produced: Mapping[str, tuple[int, int]]) -> int:
    """Messages that never failed, first delivered after a later message of their key that never failed.

    `retry-topic` reorders retried messages by design; it must not reorder anything else. Must be 0.
    """
    failed = {attempt["id"] for attempt in evidence.attempts if not attempt["ok"]}
    delivered = first_ok(evidence)
    by_key: dict[int, list[tuple[float, int]]] = defaultdict(list)
    for msg_id, at in delivered.items():
        if msg_id in failed or msg_id not in produced:
            continue
        key, seq = produced[msg_id]
        by_key[key].append((at, seq))
    inverted = 0
    for deliveries in by_key.values():
        highest = -1
        for _at, seq in sorted(deliveries):
            if seq < highest:
                inverted += 1
            highest = max(highest, seq)
    return inverted


def displacement(evidence: Evidence, produced: Mapping[str, tuple[int, int]]) -> dict[str, Any]:
    """How far out of order a retried message lands: overtaken-by count and time behind, per message."""
    failed = {attempt["id"] for attempt in evidence.attempts if not attempt["ok"]}
    delivered = first_ok(evidence)
    by_key: dict[int, list[tuple[int, float, str]]] = defaultdict(list)
    for msg_id, at in delivered.items():
        if msg_id in produced:
            key, seq = produced[msg_id]
            by_key[key].append((seq, at, msg_id))
    overtaken: list[float] = []
    behind_s: list[float] = []
    for deliveries in by_key.values():
        for seq, at, msg_id in deliveries:
            if msg_id not in failed:
                continue
            later = [other_at for other_seq, other_at, _ in deliveries if other_seq > seq and other_at < at]
            overtaken.append(len(later))
            if later:
                behind_s.append(at - min(later))
    retried = len(overtaken)
    return {
        "retried_delivered": retried,
        "displaced": sum(1 for count in overtaken if count),
        "displaced_fraction": round(sum(1 for count in overtaken if count) / retried, 4) if retried else 0.0,
        "overtaken_by_p50": percentile(overtaken, 0.5) if overtaken else 0,
        "overtaken_by_p99": percentile(overtaken, 0.99) if overtaken else 0,
        "overtaken_by_max": max(overtaken) if overtaken else 0,
        "behind_s_p50": round(percentile(behind_s, 0.5), 3) if behind_s else 0.0,
        "behind_s_p99": round(percentile(behind_s, 0.99), 3) if behind_s else 0.0,
        "behind_s_max": round(max(behind_s), 3) if behind_s else 0.0,
    }


def lateness(evidence: Evidence) -> dict[int, list[float]]:
    """Attempt time minus `tb-retry-not-before`, per retry number."""
    grouped: dict[int, list[float]] = defaultdict(list)
    for attempt in evidence.attempts:
        if attempt["n"] > 1 and attempt.get("nb") is not None:
            grouped[attempt["n"] - 1].append(attempt["ts"] - attempt["nb"])
    return dict(grouped)


def resume_lag(evidence: Evidence) -> list[float]:
    """For a paused partition: first attempt on it after the resume, minus when it was due.

    This is the number S5 measured at ~169 ms with ~155 ms of it the poll timeout. It includes the
    refetch after `resume()`, which the `buffer` mechanism never pays because the record is in memory.
    """
    by_partition: dict[tuple[str, int], list[float]] = defaultdict(list)
    for attempt in evidence.attempts:
        by_partition[(attempt["t"], attempt["p"])].append(attempt["ts"])
    for times in by_partition.values():
        times.sort()
    lags = []
    for resume in evidence.resumes:
        times = by_partition.get((resume["t"], resume["p"]), [])
        index = bisect.bisect_left(times, resume["ts"])
        if index < len(times):
            lags.append(times[index] - resume["at"])
    return lags


def scheduling_lag(evidence: Evidence, mechanism: str) -> list[float]:
    """The delay the retry consumer itself adds once a record is due, excluding head-of-line waits.

    `buffer` holds records in memory, so it is the lateness of every retry attempt. `pause` and
    `tiers` pay head-of-line waits by design (measured separately as lateness); what they add
    themselves is the resume lag.
    """
    if mechanism == "buffer":
        return [value for values in lateness(evidence).values() for value in values]
    return resume_lag(evidence)
