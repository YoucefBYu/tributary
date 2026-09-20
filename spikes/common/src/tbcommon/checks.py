"""Delivery-property analysis shared by S2 and S5.

Everything here works on ``(key, seq, msg_id)`` observations. It never touches
payloads, so reports cannot leak PHI (invariant 6).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Observation:
    """One delivered record, as seen by the sink."""

    key: str
    seq: int
    msg_id: str


@dataclass(slots=True)
class OrderingReport:
    """Result of checking per-key ordering, loss and duplication."""

    delivered: int = 0
    unique_msg_ids: int = 0
    duplicates: int = 0
    duplicate_rate: float = 0.0
    lost: list[str] = field(default_factory=list)
    reordered_keys: dict[str, list[tuple[int, int]]] = field(default_factory=dict)
    max_seq_per_key: dict[str, int] = field(default_factory=dict)

    @property
    def order_preserved(self) -> bool:
        return not self.reordered_keys

    @property
    def no_loss(self) -> bool:
        return not self.lost

    def summary(self) -> dict[str, object]:
        return {
            "delivered": self.delivered,
            "unique": self.unique_msg_ids,
            "duplicates": self.duplicates,
            "duplicate_rate": round(self.duplicate_rate, 6),
            "lost": len(self.lost),
            "order_preserved": self.order_preserved,
            "reordered_key_count": len(self.reordered_keys),
        }


def check(
    observed: Sequence[Observation],
    expected: Iterable[Observation] | None = None,
) -> OrderingReport:
    """Analyse a delivery log.

    Ordering rule: per key, the *first* delivery of each ``seq`` must be
    non-decreasing. A re-delivery of an already-seen seq is a duplicate, not a
    reordering -- at-least-once allows it (invariant 4) and ordered sinks are
    idempotent by ``tb-msg-id``.
    """
    report = OrderingReport(delivered=len(observed))

    ids = Counter(o.msg_id for o in observed)
    report.unique_msg_ids = len(ids)
    report.duplicates = sum(c - 1 for c in ids.values() if c > 1)
    report.duplicate_rate = report.duplicates / len(observed) if observed else 0.0

    seen_first: dict[str, set[int]] = defaultdict(set)
    high_water: dict[str, int] = {}
    reordered: dict[str, list[tuple[int, int]]] = defaultdict(list)

    for obs in observed:
        if obs.seq in seen_first[obs.key]:
            continue  # duplicate replay, already accounted for
        seen_first[obs.key].add(obs.seq)
        prev = high_water.get(obs.key)
        if prev is not None and obs.seq < prev:
            reordered[obs.key].append((prev, obs.seq))
        high_water[obs.key] = max(prev or -1, obs.seq)

    report.reordered_keys = dict(reordered)
    report.max_seq_per_key = dict(high_water)

    if expected is not None:
        expected_ids = {o.msg_id for o in expected}
        report.lost = sorted(expected_ids - set(ids))

    return report


def gaps_per_key(observed: Sequence[Observation]) -> dict[str, list[int]]:
    """Missing sequence numbers per key -- evidence of loss inside a key."""
    by_key: dict[str, set[int]] = defaultdict(set)
    for obs in observed:
        by_key[obs.key].add(obs.seq)
    out: dict[str, list[int]] = {}
    for key, seqs in by_key.items():
        missing = sorted(set(range(max(seqs) + 1)) - seqs)
        if missing:
            out[key] = missing
    return out
