"""The channel spec as the runtime sees it: a typed, lenient parse of `contracts/channel-spec/v0`.

Two rules from the contract shape this module.

**Lenient consumer (section 1.1).** Unknown fields are ignored, never rejected. A worker that refuses a
spec written by a newer control plane takes the data plane down over an additive change, which is
invariant 3 inverted. The CLI and control plane validate strictly against the JSON Schema; the runtime
only needs what it executes.

**`metadata.version` is mandatory here even though the schema makes it optional (section 6, rule 1),**
because one schema validates both the authored file and the published record, and only the published
form carries the version the engine stamps into `tb-channel-version`.

Two things are refused outright rather than parsed: `retry.policy: retry-topic`, which the schema also
forbids (ADR-003 D1: defined, never measured), and a `per-key` channel with no `partitionKey`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from .errors import ChannelSpecError, safe_token
from .topics import CHANNEL, DESTINATION

Ordering = Literal["per-key", "none"]
OnExhaust = Literal["halt-partition", "dlq-and-continue"]

#: ADR-003 D4: a step exception is retried in place a small number of times, then dead-lettered.
#: Default 3 is the contract's, and is a judgement — S5 exercised the DLQ path, not the retry count.
DEFAULT_STEP_RETRIES = 3

#: Blocking-retry backoff. JUDGEMENT, not measured: S5's pauses lasted seconds and the 30 s cap is the
#: arithmetic that keeps a long destination outage inside `max.poll.interval.ms` (ADR-003 follow-up 7).
DEFAULT_INITIAL_DELAY_MS = 100
DEFAULT_MAX_DELAY_MS = 30_000


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Per-destination retry behaviour. `blocking` is the only policy v0 may select."""

    policy: Literal["blocking"]
    max_attempts: int
    on_exhaust: OnExhaust
    backoff: Literal["exponential", "fixed"] = "exponential"
    initial_delay_ms: int = DEFAULT_INITIAL_DELAY_MS
    max_delay_ms: int = DEFAULT_MAX_DELAY_MS


@dataclass(frozen=True, slots=True)
class Idempotency:
    """Invariant 4: how this destination absorbs the duplicates at-least-once delivery will produce."""

    technique: str
    header: str | None = None
    note: str | None = None


@dataclass(frozen=True, slots=True)
class StepRef:
    """A step to load: exactly one of `uses` (entry point) or `ref` (dotted path in the bundle)."""

    name: str
    uses: str | None = None
    ref: str | None = None
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Destination:
    name: str
    type: str
    params: Mapping[str, Any]
    idempotency: Idempotency
    retry: RetryPolicy
    #: Set 1.1 (README 3.5): steps run for this destination only, in the pipeline worker, before the
    #: record is produced to `out.<dest>`. Empty means the destination gets the pipeline output as is.
    steps: tuple[StepRef, ...] = ()


@dataclass(frozen=True, slots=True)
class Pipeline:
    bundle: str
    steps: tuple[StepRef, ...]
    step_retries: int = DEFAULT_STEP_RETRIES
    transactional: bool = False


@dataclass(frozen=True, slots=True)
class ChannelSpec:
    """One deployed channel."""

    name: str
    version: int
    ordering: Ordering
    pipeline: Pipeline
    destinations: tuple[Destination, ...]
    partition_key: str | None = None
    partitions: int | None = None
    source: Mapping[str, Any] = field(default_factory=dict)

    def destination(self, name: str) -> Destination:
        for destination in self.destinations:
            if destination.name == name:
                return destination
        raise ChannelSpecError(f"channel {safe_token(self.name)} has no destination {safe_token(name)}")

    @property
    def ordered(self) -> bool:
        return self.ordering == "per-key"

    @classmethod
    def from_document(cls, document: Any) -> ChannelSpec:
        """Parse a published `_tb.<env>.channels` record value. Raises `ChannelSpecError`."""
        if not isinstance(document, dict):
            raise ChannelSpecError("channel spec is not a JSON object")
        kind = document.get("kind")
        if kind != "Channel":
            raise ChannelSpecError(f"unsupported kind {safe_token(str(kind))}, expected Channel")

        metadata = _object(document, "metadata")
        name = _text(metadata, "name", "metadata")
        if not CHANNEL.match(name):
            raise ChannelSpecError(f"metadata.name {safe_token(name)} is not topic-safe")
        version = metadata.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            # Contract section 6 rule 1: reject the record and keep the previously applied spec.
            raise ChannelSpecError(f"channel {safe_token(name)}: metadata.version is absent or not an integer")

        spec = _object(document, "spec")
        ordering = spec.get("ordering")
        if ordering not in ("per-key", "none"):
            raise ChannelSpecError(f"channel {safe_token(name)}: spec.ordering must be per-key or none")
        partition_key = spec.get("partitionKey")
        if ordering == "per-key" and not isinstance(partition_key, str):
            raise ChannelSpecError(f"channel {safe_token(name)}: ordering per-key requires spec.partitionKey")
        if ordering == "none" and partition_key is not None:
            raise ChannelSpecError(f"channel {safe_token(name)}: ordering none must not carry a partitionKey")

        partitions = spec.get("partitions")
        if partitions is not None and (not isinstance(partitions, int) or isinstance(partitions, bool)):
            raise ChannelSpecError(f"channel {safe_token(name)}: spec.partitions must be an integer")

        destinations = tuple(_destination(name, item) for item in _array(spec, "destinations", name))
        if not destinations:
            raise ChannelSpecError(f"channel {safe_token(name)}: at least one destination is required")
        seen = {destination.name for destination in destinations}
        if len(seen) != len(destinations):
            raise ChannelSpecError(f"channel {safe_token(name)}: destination names must be unique")

        source = spec.get("source")
        return cls(
            name=name,
            version=version,
            ordering=ordering,
            pipeline=_pipeline(name, _object(spec, "pipeline")),
            destinations=destinations,
            partition_key=partition_key if isinstance(partition_key, str) else None,
            partitions=partitions if isinstance(partitions, int) else None,
            source=source if isinstance(source, dict) else {},
        )


def _object(parent: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ChannelSpecError(f"{safe_token(key)} is missing or not an object")
    return value


def _array(parent: Mapping[str, Any], key: str, channel: str) -> Sequence[Any]:
    value = parent.get(key)
    if not isinstance(value, list):
        raise ChannelSpecError(f"channel {safe_token(channel)}: spec.{safe_token(key)} must be an array")
    return value


def _text(parent: Mapping[str, Any], key: str, where: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise ChannelSpecError(f"{safe_token(where)}.{safe_token(key)} is missing or not a string")
    return value


def _pipeline(channel: str, block: Mapping[str, Any]) -> Pipeline:
    steps = tuple(_step(channel, item) for item in block.get("steps", []) if isinstance(item, dict))
    if not steps:
        raise ChannelSpecError(f"channel {safe_token(channel)}: spec.pipeline.steps must have at least one step")
    step_retries = block.get("stepRetries")
    return Pipeline(
        bundle=_text(block, "bundle", "pipeline"),
        steps=steps,
        step_retries=step_retries
        if isinstance(step_retries, int) and not isinstance(step_retries, bool)
        else DEFAULT_STEP_RETRIES,
        transactional=bool(block.get("transactional", False)),
    )


def _step(channel: str, block: Mapping[str, Any]) -> StepRef:
    name = _text(block, "name", "step")
    uses, ref = block.get("uses"), block.get("ref")
    if (uses is None) == (ref is None):
        raise ChannelSpecError(
            f"channel {safe_token(channel)}: step {safe_token(name)} needs exactly one of uses or ref"
        )
    params = block.get("with")
    return StepRef(
        name=name,
        uses=uses if isinstance(uses, str) else None,
        ref=ref if isinstance(ref, str) else None,
        params=params if isinstance(params, dict) else {},
    )


def _destination(channel: str, block: Any) -> Destination:
    if not isinstance(block, dict):
        raise ChannelSpecError(f"channel {safe_token(channel)}: a destination is not an object")
    name = _text(block, "name", "destination")
    if not DESTINATION.match(name):
        raise ChannelSpecError(f"channel {safe_token(channel)}: destination name {safe_token(name)} is not topic-safe")
    params = block.get("with")
    steps_block = block.get("steps")
    if steps_block is not None and not isinstance(steps_block, list):
        raise ChannelSpecError(f"{safe_token(channel)}/{safe_token(name)}: destination steps must be an array")
    steps = tuple(_step(channel, item) for item in steps_block or [] if isinstance(item, dict))
    if steps_block is not None and len(steps) != len(steps_block):
        # A non-object entry would otherwise vanish, and the destination would run fewer steps than the
        # author wrote. Lenient means ignoring unknown *fields*, not dropping known ones.
        raise ChannelSpecError(f"{safe_token(channel)}/{safe_token(name)}: every destination step must be an object")
    return Destination(
        name=name,
        type=_text(block, "type", "destination"),
        params=params if isinstance(params, dict) else {},
        idempotency=_idempotency(channel, name, _object(block, "idempotency")),
        retry=_retry(channel, name, _object(block, "retry")),
        steps=steps,
    )


def _idempotency(channel: str, destination: str, block: Mapping[str, Any]) -> Idempotency:
    technique = _text(block, "technique", "idempotency")
    header = block.get("header")
    note = block.get("note")
    if technique == "idempotency-key-header" and not isinstance(header, str):
        raise ChannelSpecError(
            f"{safe_token(channel)}/{safe_token(destination)}: idempotency-key-header needs a header"
        )
    if technique == "none" and not isinstance(note, str):
        raise ChannelSpecError(
            f"{safe_token(channel)}/{safe_token(destination)}: idempotency none needs a note "
            "(invariant 4: duplicates will be visible at this destination)"
        )
    return Idempotency(
        technique=technique,
        header=header if isinstance(header, str) else None,
        note=note if isinstance(note, str) else None,
    )


def _retry(channel: str, destination: str, block: Mapping[str, Any]) -> RetryPolicy:
    policy = block.get("policy")
    if policy == "retry-topic":
        raise ChannelSpecError(
            f"{safe_token(channel)}/{safe_token(destination)}: retry-topic was never implemented or measured "
            "and may not be selected until it is spiked (ADR-003 D1/D2)"
        )
    if policy != "blocking":
        raise ChannelSpecError(f"{safe_token(channel)}/{safe_token(destination)}: retry.policy must be blocking")

    max_attempts = block.get("maxAttempts")
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ChannelSpecError(f"{safe_token(channel)}/{safe_token(destination)}: retry.maxAttempts must be >= 1")
    on_exhaust = block.get("onExhaust")
    if on_exhaust not in ("halt-partition", "dlq-and-continue"):
        raise ChannelSpecError(
            f"{safe_token(channel)}/{safe_token(destination)}: retry.onExhaust must be halt-partition or dlq-and-continue"
        )
    backoff = block.get("backoff")
    initial = block.get("initialDelayMs")
    maximum = block.get("maxDelayMs")
    return RetryPolicy(
        policy="blocking",
        max_attempts=max_attempts,
        on_exhaust=on_exhaust,
        backoff=backoff if backoff in ("exponential", "fixed") else "exponential",
        initial_delay_ms=initial
        if isinstance(initial, int) and not isinstance(initial, bool)
        else DEFAULT_INITIAL_DELAY_MS,
        max_delay_ms=maximum if isinstance(maximum, int) and not isinstance(maximum, bool) else DEFAULT_MAX_DELAY_MS,
    )
