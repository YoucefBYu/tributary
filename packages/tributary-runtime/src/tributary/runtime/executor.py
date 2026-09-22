"""The pipeline executor: run a channel's steps in process, in order, against one message.

Invariant 2 — Kafka hops only at durability boundaries — is why this is a loop and not a topic per
step. A step returning `Drop` filters the message; a step raising is retried in place a small number of
times and then dead-lettered (ADR-003 D4), because a step exception is usually deterministic and
retrying it many times only delays the DLQ.

Steps are trusted code (ADR-001 section 8): nothing here sandboxes them. What it does do is bound their
failure — an exception cannot escape into the consumer loop, and the exception's text never reaches a
log line, an event or the DLQ record, because HL7 and FHIR parsers routinely quote the offending field
value, which is PHI (ADR-003 D4, events contract section 3).
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass

from tributary.sdk import Context, Drop, Message, Step
from tributary.sdk.errors import TributaryError, safe_token

STEP_ERROR_CODE = "STEP_ERROR"


class StepFailure(TributaryError):
    """A step raised on every attempt. The record is destined for the DLQ.

    `error_code` is stable and machine-readable; `exception_type` is a class name, which is code, not
    data. The exception's own message is deliberately dropped.
    """

    def __init__(self, step_name: str, attempts: int, exception_type: str) -> None:
        super().__init__(f"step {safe_token(step_name)} failed {attempts} time(s) with {safe_token(exception_type)}")
        self.step_name = step_name
        self.attempts = attempts
        self.exception_type = exception_type
        self.error_code = STEP_ERROR_CODE


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


class PipelineExecutor:
    """Runs one channel's steps. Stateless between messages apart from the steps themselves."""

    __slots__ = ("_retries", "_steps")

    def __init__(self, steps: Sequence[Step], *, step_retries: int = 3) -> None:
        if step_retries < 0:
            raise ValueError("step_retries must be >= 0")
        self._steps = tuple(steps)
        self._retries = step_retries

    @property
    def step_names(self) -> tuple[str, ...]:
        return tuple(step.name for step in self._steps)

    def run(self, message: Message, ctx: Context) -> ExecutionResult:
        """Run every step in order. Raises `StepFailure` when one keeps raising."""
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
        attempts = self._retries + 1
        last_type = "Exception"
        for _attempt in range(attempts):
            try:
                return step(message, ctx)
            except Exception as exc:
                last_type = type(exc).__name__
        raise StepFailure(step.name, attempts, last_type)
