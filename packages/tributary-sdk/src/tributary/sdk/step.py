"""`@step`: the decorator that turns a plain function into a pipeline stage.

A step is a function of `(message)` or `(message, ctx)` that returns:

* a `Message` — the message as it continues down the pipeline;
* a `Drop` — filter it out, with a reason;
* `None` — pass the input through unchanged.

**Anything else is a `StepReturnError`**, which is a deliberate v0 narrowing of the illustrative API in
ADR-001 6.2, where a step returns an `HL7Message` or a pydantic `Bundle`. Accepting an arbitrary object
means the SDK has to serialise it and guess the new `tb-content-type`, and a wrong guess leaves a
record labelled `application/hl7-v2+er7` carrying FHIR JSON — a silent envelope violation that every
downstream consumer inherits. Until the content packages exist (W2-1, W2-2), a step that changes the
format says so: `msg.with_payload(body, content_type="application/fhir+json")`.

Steps are trusted code (ADR-001 section 8): the decorator does not sandbox anything. What it does is
make the contract of a step explicit and checkable at decoration time rather than at 3am.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, overload

from .context import Context
from .errors import StepDefinitionError, StepReturnError, safe_token
from .message import Message
from .outcome import Drop

StepResult = Message | Drop
StepFunction = Callable[..., Any]


def _takes_context(func: StepFunction, name: str) -> bool:
    """True when the function wants `ctx`. Raises `StepDefinitionError` for any other shape."""
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError) as exc:  # builtins and C functions have no signature
        raise StepDefinitionError(
            f"step {safe_token(name)}: cannot inspect the signature; use a plain function"
        ) from exc

    positional = []
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            raise StepDefinitionError(f"step {safe_token(name)}: *args is not a step signature")
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            positional.append(parameter)
        elif parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is inspect.Parameter.empty:
            raise StepDefinitionError(
                f"step {safe_token(name)}: keyword-only parameter {safe_token(parameter.name)} has no default, "
                "and the runtime passes only (message, ctx)"
            )

    if len(positional) == 1:
        return False
    if len(positional) == 2:
        return True
    raise StepDefinitionError(
        f"step {safe_token(name)}: a step takes (message) or (message, ctx), not {len(positional)} positional parameters"
    )


class Step:
    """A callable pipeline stage with a checked signature and a checked return value."""

    def __init__(self, func: StepFunction, *, name: str | None = None) -> None:
        if not callable(func):
            raise StepDefinitionError("@step must decorate a callable")
        self.name = name or getattr(func, "__name__", None) or "anonymous"
        self.takes_ctx = _takes_context(func, self.name)
        self.func = func
        functools.update_wrapper(self, func)

    def __call__(self, message: Message, ctx: Context | None = None) -> StepResult:
        """Run the step and normalise its outcome to `Message` or `Drop`."""
        if not isinstance(message, Message):
            raise StepDefinitionError(
                f"step {safe_token(self.name)}: first argument must be a Message, not {type(message).__name__}"
            )
        if self.takes_ctx:
            if ctx is None:
                raise StepDefinitionError(
                    f"step {safe_token(self.name)} takes a ctx: pass one, or use run_step, which supplies a fake"
                )
            result = self.func(message, ctx)
        else:
            result = self.func(message)

        if result is None:
            return message
        if isinstance(result, Message | Drop):
            return result
        raise StepReturnError(
            f"step {safe_token(self.name)} returned {type(result).__name__}: a step returns Message, Drop or None. "
            "To change the payload use message.with_payload(body, content_type=...)"
        )

    def __repr__(self) -> str:
        return f"Step(name={safe_token(self.name)}, takes_ctx={self.takes_ctx})"


@overload
def step(func: StepFunction, /) -> Step: ...


@overload
def step(*, name: str | None = ...) -> Callable[[StepFunction], Step]: ...


def step(func: StepFunction | None = None, /, *, name: str | None = None) -> Step | Callable[[StepFunction], Step]:
    """Declare a pipeline step. Usable bare (`@step`) or with a name (`@step(name="to-fhir")`)."""
    if func is not None:
        return Step(func)

    def decorate(inner: StepFunction) -> Step:
        return Step(inner, name=name)

    return decorate
