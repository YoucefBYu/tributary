"""Topic layout for the S5 scenario (mirrors ADR-001 section 5.3)."""

from __future__ import annotations

from dataclasses import dataclass

from tbcommon import admin, config

CHANNEL = "s5"


@dataclass(frozen=True, slots=True)
class Layout:
    raw: str
    out: str
    retry: str
    dlq: str
    events: str
    partitions: int

    def all_topics(self) -> list[str]:
        return [self.raw, self.out, self.retry, self.dlq, self.events]

    def specs(self) -> list[admin.TopicSpec]:
        rf = config.replication_factor()
        return [
            admin.TopicSpec(self.raw, self.partitions, rf),
            admin.TopicSpec(self.out, self.partitions, rf),
            admin.TopicSpec(self.retry, self.partitions, rf),
            # DLQ is keyed by message id and low volume: fewer partitions.
            admin.TopicSpec(self.dlq, max(1, self.partitions // 4), rf),
            admin.TopicSpec(self.events, self.partitions, rf, {"retention.ms": "259200000"}),
        ]


def layout(run_id: str, partitions: int = 6, destination: str = "fhir") -> Layout:
    # `spike.s5.<run>` -- every S5 topic shares the `s5.` component so one
    # prefix match finds them all for cleanup, and a stray topic is traceable.
    prefix = f"{config.topic_prefix()}.{CHANNEL}.{run_id}"
    return Layout(
        raw=f"{prefix}.raw",
        out=f"{prefix}.out.{destination}",
        retry=f"{prefix}.retry",
        dlq=f"{prefix}.dlq",
        # _tb.events is global in production; namespaced here so parallel runs
        # do not read each other's events.
        events=f"{prefix}._tb.events",
        partitions=partitions,
    )
