"""Keyed synthetic traffic for the S5 scenarios.

Synthetic data only, MSH-3 = ``TRIBUTARY-SYNTH`` (`tbcommon.payloads`; swap for
`qa-engineer`'s P0-1 corpus when it lands). The generator's contract with the
analysis is the `tb-x-seq` header: a monotonic per-key sequence number, which
is what lets the checker say "this key was reordered" without ever looking at
a payload.
"""

from __future__ import annotations

from dataclasses import dataclass

from tbcommon import checks, payloads
from tbcommon.envelope import Envelope

from .driver import ProducerLike


@dataclass(frozen=True, slots=True)
class Plan:
    """What was produced, for the analysis to compare against."""

    expected: list[checks.Observation]
    #: (key, seq) pairs chosen to be poison, i.e. must end up in the DLQ.
    poison: frozenset[tuple[str, int]]

    @property
    def deliverable(self) -> list[checks.Observation]:
        """Everything that must reach the sink: all of it except the poison."""
        return [o for o in self.expected if (o.key, o.seq) not in self.poison]

    def poison_ids(self) -> set[str]:
        return {o.msg_id for o in self.expected if (o.key, o.seq) in self.poison}


def publish(
    producer: ProducerLike,
    topic: str,
    *,
    count: int,
    keys: int,
    seed: int = 1,
    channel: str = "s5",
    stage: str = "received",
    poison_every: int | None = None,
    flush_timeout: float = 60.0,
) -> Plan:
    """Produce ``count`` messages over ``keys`` ordering keys.

    ``poison_every``: mark every Nth message poison (the sink will reject it
    permanently). Poison is chosen here, not by the loader, so the analysis
    knows exactly which ids are supposed to end up in the DLQ.
    """
    expected: list[checks.Observation] = []
    poison: set[tuple[str, int]] = set()
    errors: list[str] = []

    def on_delivery(err: object, _msg: object) -> None:
        if err is not None:
            errors.append(str(err))

    for index, (key, seq, payload) in enumerate(payloads.stream(count, keys, seed=seed)):
        env = Envelope(channel=channel, seq=seq, stage=stage)
        expected.append(checks.Observation(key=key, seq=seq, msg_id=env.msg_id))
        if poison_every and index % poison_every == poison_every - 1:
            poison.add((key, seq))
        producer.produce(
            topic,
            key=key.encode(),
            value=payload,
            headers=env.to_headers(),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(flush_timeout)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) unflushed; refusing to claim they were sent")
    if errors:
        raise RuntimeError(f"{len(errors)} delivery failure(s); first: {errors[0]}")
    return Plan(expected=expected, poison=frozenset(poison))
