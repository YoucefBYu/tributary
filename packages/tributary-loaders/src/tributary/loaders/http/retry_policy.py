"""What to do after one failed delivery attempt: a pure decision, no Kafka, no clock, no sleep —
`consumer.py`'s `OutConsumer` is the only caller and the only place any of those happen.

`retry.policy: blocking` is the only value the channel-spec schema allows (`retry-topic` fails
validation, ADR-003 D1/D2), so there is exactly one shape to decide for: keep the partition on this
message (`"retry"`), or stop trying and dispose of it per `onExhaust` (`"dlq"` or `"halt"`).
"""

from __future__ import annotations

from typing import Literal

Decision = Literal["retry", "dlq", "halt"]


def decide(*, retryable: bool, attempt: int, max_attempts: int, on_exhaust: str) -> Decision:
    """`attempt` is the attempt just made (1 for the first delivery try). A non-retryable outcome
    (`DeliveryOutcome.retryable is False`, i.e. a terminal 4xx) goes straight to `onExhaust` — there
    is no reason to spend the remaining attempt budget resending something the destination has
    already permanently rejected."""
    if retryable and attempt < max_attempts:
        return "retry"
    return "dlq" if on_exhaust == "dlq-and-continue" else "halt"
