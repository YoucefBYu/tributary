"""The failure plan and the delay tiers, shared by the loader process, the runner and the model.

**The plan is a pure function of `tb-msg-id`.** A loader that is killed and replaced must make the
same decision about the same attempt of the same message, or a kill test measures the plan changing
rather than the loader recovering. So "does attempt n of message m fail" is a hash, not an RNG
stream, and the runner can compute the expected outcome of every message without running anything.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

#: A message that fails every attempt: it must end up in the DLQ after `max_attempts`.
POISON = 1_000_000

H_MSG_ID = "tb-msg-id"
H_ATTEMPT = "tb-attempt"
H_NOT_BEFORE = "tb-retry-not-before"
H_DESTINATION = "tb-destination"
H_STAGE = "tb-stage"
H_ERROR_CODE = "tb-error-code"
#: Spike-only: a per-key sequence number and a key ordinal, so the analysis can judge ordering
#: without ever reading or printing a key (keys are patient identifiers in production).
H_SEQ = "tb-x-seq"
H_KEY_ORDINAL = "tb-x-kord"


@dataclass(frozen=True, slots=True)
class Tiers:
    """Retry delay per retry number. Retry n (1-based) waits `delays[min(n, len) - 1]` seconds."""

    delays: tuple[float, ...]

    def delay(self, retry_number: int) -> float:
        if retry_number < 1:
            raise ValueError("retry numbers start at 1")
        return self.delays[min(retry_number, len(self.delays)) - 1]

    def tier(self, retry_number: int) -> int:
        """0-based tier index, which is also the topic index under the `tiers` mechanism."""
        return min(retry_number, len(self.delays)) - 1


@dataclass(frozen=True, slots=True)
class FailurePlan:
    """Which delivery attempts fail.

    `fail_first[k]` is the probability that a message fails exactly its first k+1 attempts and then
    succeeds; `poison` is the probability that it fails every attempt. `outages` are wall-clock
    windows (epoch seconds) during which every attempt fails, which is what mixes tiers inside one
    retry partition and so creates head-of-line blocking.
    """

    seed: int = 1
    fail_first: tuple[float, ...] = (0.10, 0.05, 0.03)
    poison: float = 0.01
    outages: tuple[tuple[float, float], ...] = ()

    def leading_failures(self, msg_id: str) -> int:
        digest = hashlib.sha256(f"{self.seed}:{msg_id}".encode()).digest()
        draw = int.from_bytes(digest[:8], "big") / 2**64
        if draw < self.poison:
            return POISON
        cumulative = self.poison
        for index, probability in enumerate(self.fail_first):
            cumulative += probability
            if draw < cumulative:
                return index + 1
        return 0

    def fails(self, msg_id: str, attempt_number: int, now: float) -> bool:
        """`attempt_number` is 1 for the first delivery from `out`, 2 for the first retry, ..."""
        if attempt_number <= self.leading_failures(msg_id):
            return True
        return any(start <= now < end for start, end in self.outages)

    def is_poison(self, msg_id: str) -> bool:
        return self.leading_failures(msg_id) >= POISON

    def to_json(self) -> dict[str, object]:
        return {
            "seed": self.seed,
            "fail_first": list(self.fail_first),
            "poison": self.poison,
            "outages": [list(window) for window in self.outages],
        }

    @classmethod
    def from_json(cls, document: dict[str, object]) -> FailurePlan:
        outages = document.get("outages") or []
        return cls(
            seed=int(document["seed"]),  # type: ignore[call-overload]
            fail_first=tuple(float(value) for value in document["fail_first"]),  # type: ignore[attr-defined]
            poison=float(document["poison"]),  # type: ignore[arg-type]
            outages=tuple((float(start), float(end)) for start, end in outages),  # type: ignore[attr-defined]
        )


def format_not_before(epoch_s: float) -> str:
    """`tb-retry-not-before` in the envelope's RFC 3339 UTC millisecond form."""
    moment = datetime.fromtimestamp(epoch_s, UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def parse_not_before(text: str) -> float:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC).timestamp()
