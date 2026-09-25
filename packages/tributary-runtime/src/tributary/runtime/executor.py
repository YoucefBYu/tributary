"""The pipeline executor: run a channel's steps in process, in order, against one message.

Invariant 2 — Kafka hops only at durability boundaries — is why this is a loop and not a topic per
step. A step returning `Drop` filters the message; a step raising is retried in place a small number of
times and then dead-lettered (ADR-003 D4), because a step exception is usually deterministic and
retrying it many times only delays the DLQ. A step raising `Reject(code)` has said retrying cannot
help, so it is dead-lettered at once with its own code (channel-spec README 3.3.2).

Steps are trusted code (ADR-001 section 8): nothing here sandboxes them. What it does do is bound their
failure — an exception cannot escape into the consumer loop, and the exception's text never reaches a
log line, an event or the DLQ record, because HL7 and FHIR parsers routinely quote the offending field
value, which is PHI (ADR-003 D4, events contract section 3).

**Per-step timing** is kept in `StepStats`: counts and durations per `(destination, step)`, both of
which are spec-defined names and never message data. It is exposed as data and as a periodic structured
log line; exporting it as metrics waits on `contracts/observability/v0` and on `security-reviewer`'s
review of a `/metrics` listener, so nothing here opens a socket.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from tributary.sdk import Context, Drop, Message, Reject, SplitResult, Splitter, Step
from tributary.sdk.context import Logger
from tributary.sdk.errors import TributaryError, safe_token

STEP_ERROR_CODE = "STEP_ERROR"

StepOutcome = Literal["ok", "dropped", "failed", "rejected"]


class StepFailure(TributaryError):
    """A step raised on every attempt, or raised `Reject`. The record is destined for the DLQ.

    `error_code` is stable and machine-readable: `STEP_ERROR` after exhausted retries, or the step's own
    `Reject` code. `exception_type` is a class name, which is code, not data. The exception's own message
    is deliberately dropped.
    """

    def __init__(
        self,
        step_name: str,
        attempts: int,
        exception_type: str,
        *,
        error_code: str = STEP_ERROR_CODE,
        rejected: bool = False,
    ) -> None:
        super().__init__(
            f"step {safe_token(step_name)} failed {attempts} time(s) with {safe_token(exception_type)} "
            f"({safe_token(error_code)})"
        )
        self.step_name = step_name
        self.attempts = attempts
        self.exception_type = exception_type
        self.error_code = error_code
        #: True when the step raised the SDK's `Reject`: non-retryable by the step's own decision.
        self.rejected = rejected


@dataclass(slots=True)
class StepStat:
    """Counters and timing for one step in one scope. Durations are wall-clock milliseconds."""

    calls: int = 0
    ok: int = 0
    dropped: int = 0
    failed: int = 0
    rejected: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0

    @property
    def mean_ms(self) -> float:
        return self.total_ms / self.calls if self.calls else 0.0


PIPELINE_SCOPE = "-"


@dataclass(slots=True)
class StepStats:
    """Per-step timing for one worker, keyed by `(scope, step name)`.

    `scope` is a destination name for destination steps and `-` for `pipeline.steps`: both are
    spec-defined and low-cardinality (README 3.5 "Give up": per-destination timing needs the destination
    name as a label, which is not PHI). Retries count as calls, so `calls - ok - dropped` shows the cost
    of a failing step rather than hiding it.
    """

    entries: dict[tuple[str, str], StepStat] = field(default_factory=dict)

    def record(self, scope: str | None, step: str, outcome: StepOutcome, duration_ms: float) -> None:
        stat = self.entries.setdefault((scope or PIPELINE_SCOPE, step), StepStat())
        stat.calls += 1
        stat.total_ms += duration_ms
        stat.max_ms = max(stat.max_ms, duration_ms)
        if outcome == "ok":
            stat.ok += 1
        elif outcome == "dropped":
            stat.dropped += 1
        elif outcome == "rejected":
            stat.rejected += 1
        else:
            stat.failed += 1

    def get(self, step: str, scope: str | None = None) -> StepStat | None:
        return self.entries.get((scope or PIPELINE_SCOPE, step))

    def log_summary(self, log: Logger, *, channel: str, channel_version: int) -> None:
        """One `step_stats` line per step. Names and numbers only."""
        for (scope, step), stat in sorted(self.entries.items()):
            log.info(
                "step_stats",
                channel=channel,
                channel_version=channel_version,
                scope=scope,
                step=step,
                calls=stat.calls,
                ok=stat.ok,
                dropped=stat.dropped,
                failed=stat.failed,
                rejected=stat.rejected,
                mean_ms=round(stat.mean_ms, 3),
                max_ms=round(stat.max_ms, 3),
            )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """What the pipeline did with one message."""

    message: Message | None
    drop: Drop | None
    duration_ms: float
    steps_run: int

    @property
    def dropped(self) -> bool:
        return self.drop is not None


def _attempts(
    call: Callable[[], object],
    *,
    name: str,
    retries: int,
    stats: StepStats | None,
    scope: str | None,
) -> object:
    """Run one node with in-place retries. `Reject` fails at once; anything else after `retries + 1`."""
    attempts = retries + 1
    last_type = "Exception"
    for attempt in range(attempts):
        started = time.perf_counter()
        try:
            outcome = call()
        except Reject as exc:
            if stats is not None:
                stats.record(scope, name, "rejected", (time.perf_counter() - started) * 1000)
            raise StepFailure(name, attempt + 1, "Reject", error_code=exc.code, rejected=True) from None
        except Exception as exc:
            if stats is not None:
                stats.record(scope, name, "failed", (time.perf_counter() - started) * 1000)
            last_type = type(exc).__name__
            continue
        if stats is not None:
            stats.record(
                scope, name, "dropped" if isinstance(outcome, Drop) else "ok", (time.perf_counter() - started) * 1000
            )
        return outcome
    raise StepFailure(name, attempts, last_type)


class PipelineExecutor:
    """Runs one linear list of steps. Stateless between messages apart from the steps themselves."""

    __slots__ = ("_retries", "_scope", "_stats", "_steps")

    def __init__(
        self,
        steps: Sequence[Step],
        *,
        step_retries: int = 3,
        stats: StepStats | None = None,
        scope: str | None = None,
    ) -> None:
        if step_retries < 0:
            raise ValueError("step_retries must be >= 0")
        self._steps = tuple(steps)
        self._retries = step_retries
        self._stats = stats
        self._scope = scope

    @property
    def step_names(self) -> tuple[str, ...]:
        return tuple(step.name for step in self._steps)

    def __len__(self) -> int:
        return len(self._steps)

    def run(self, message: Message, ctx: Context) -> ExecutionResult:
        """Run every step in order. Raises `StepFailure` when one keeps raising or rejects."""
        started = time.perf_counter()
        current = message
        for index, step in enumerate(self._steps):
            outcome = self._run_one(step, current, ctx)
            if isinstance(outcome, Drop):
                return ExecutionResult(
                    message=None,
                    drop=outcome,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    steps_run=index + 1,
                )
            current = outcome
        return ExecutionResult(
            message=current,
            drop=None,
            duration_ms=(time.perf_counter() - started) * 1000,
            steps_run=len(self._steps),
        )

    def _run_one(self, step: Step, message: Message, ctx: Context) -> Message | Drop:
        outcome = _attempts(
            lambda: step(message, ctx), name=step.name, retries=self._retries, stats=self._stats, scope=self._scope
        )
        if not isinstance(outcome, Message | Drop):  # pragma: no cover - Step.__call__ normalises or raises
            raise StepFailure(step.name, 1, type(outcome).__name__)
        return outcome


def run_splitter(
    node: Splitter,
    message: Message,
    ctx: Context,
    *,
    step_retries: int,
    stats: StepStats | None = None,
) -> SplitResult:
    """Run a splitter with the same retry and `Reject` rules as a step. Raises `StepFailure`."""
    outcome = _attempts(lambda: node(message, ctx), name=node.name, retries=step_retries, stats=stats, scope=None)
    if not isinstance(outcome, tuple | Drop):  # pragma: no cover - Splitter.__call__ normalises or raises
        raise StepFailure(node.name, 1, type(outcome).__name__)
    return outcome


@dataclass(frozen=True, slots=True)
class ChannelPlan:
    """The executor tree for one channel version (README 3.5: "the executor becomes a small tree").

    `head` runs on the consumed message; `splitter`, when present, fans it out; `tail` runs on each child;
    then each destination's own executor runs on a copy of the result. Without a splitter, `tail` is
    empty and the message itself is the one branch.
    """

    channel: str
    version: int
    head: PipelineExecutor
    splitter: Splitter | None
    tail: PipelineExecutor
    destinations: Mapping[str, PipelineExecutor]
    step_retries: int
    stats: StepStats
