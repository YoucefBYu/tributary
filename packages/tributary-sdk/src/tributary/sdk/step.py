"""`@step` and `@splitter`: the decorators that turn a plain function into a pipeline node.

A step is a function of `(message)` or `(message, ctx)` that returns:

* a `Message` — the message as it continues down the pipeline;
* a `Drop` — filter it out, with a reason;
* `None` — pass the input through unchanged;

or raises `Reject(code)` to fail the message non-retryably. **Anything else returned is a
`StepReturnError`**, which is a deliberate v0 narrowing of the illustrative API in ADR-001 6.2, where a
step returns an `HL7Message` or a pydantic `Bundle`. Accepting an arbitrary object means the SDK has to
serialise it and guess the new `tb-content-type`, and a wrong guess leaves a record labelled
`application/hl7-v2+er7` carrying FHIR JSON — a silent envelope violation that every downstream consumer
inherits. A step that changes the format says so: `msg.with_payload(body, content_type=...)`.

**Configuration (`with`) binds to keyword-only parameters by name** (channel-spec README 3.3):

    @step
    def parse(message, ctx, *, version: str = "2.5.1") -> None: ...

A keyword-only parameter **without** a default is required from `with`. Binding happens once, when the
runtime loads the step (or when a test calls `run_step(..., with_={...})`), and refuses an unknown key, a
key that is not a Python identifier, a value of the wrong type and a missing required parameter with a
`StepConfigError`. A `with` key that is a Python keyword (`in`) binds to the PEP 8 spelling with a
trailing underscore (`in_`), because `def f(*, in: ...)` is not valid Python.

A **splitter** is a separate node type (README 3.3.2), not a widened return type for every step: fan-out
changes identity, commit and ordering, so it is visible in the code and refused at load on a `per-key`
channel. It returns an iterable of `Message` (or a `Drop`), and the SDK stamps each child's identity —
a deterministic `tb-msg-id`, `tb-parent-id`, and the parent's key — so a unit test sees exactly the
children the runtime would produce.

Steps are trusted code (ADR-001 section 8): the decorators do not sandbox anything. What they do is make
the contract of a node explicit and checkable at decoration time rather than at 3am.
"""

from __future__ import annotations

import collections.abc
import copy
import functools
import inspect
import keyword
import types
import typing
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, overload

from . import headers as h
from .codes import SPLIT_TOO_LARGE
from .context import Context
from .errors import StepConfigError, StepDefinitionError, StepReturnError, safe_token
from .ids import child_traceparent, derived_uuid7
from .message import Message
from .outcome import Drop, Reject

StepResult = Message | Drop
SplitResult = tuple[Message, ...] | Drop
StepFunction = Callable[..., Any]

#: README 3.3.2 split rule 4.
MAX_SPLIT_CHILDREN = 10_000
#: README 3.3.2 split rule 4: an empty split is a filter decision, not a failure.
EMPTY_SPLIT_REASON = "EMPTY_SPLIT"
#: The shape of a `with` key in the channel-spec schema (`withParams.propertyNames`).
_MAX_WITH_KEY = 64


@dataclass(frozen=True, slots=True)
class Option:
    """One keyword-only parameter a `with` key can bind to."""

    key: str
    parameter: str
    required: bool
    annotation: Any = inspect.Parameter.empty


def _with_key(parameter: str) -> str:
    """The `with` key for a parameter name: `in_` is spelled `in` in the spec (PEP 8 keyword rule)."""
    if parameter.endswith("_") and keyword.iskeyword(parameter[:-1]):
        return parameter[:-1]
    return parameter


def _signature(func: StepFunction, name: str) -> inspect.Signature:
    try:
        try:
            # Resolve string annotations (`from __future__ import annotations`) so `with` values can be
            # type-checked at bind. A name only importable under TYPE_CHECKING falls back to strings,
            # and those parameters are then bound without a type check.
            return inspect.signature(func, eval_str=True)
        except Exception:  # any evaluation failure just means "no resolved annotations"
            return inspect.signature(func)
    except (TypeError, ValueError) as exc:  # builtins and C functions have no signature
        raise StepDefinitionError(
            f"step {safe_token(name)}: cannot inspect the signature; use a plain function"
        ) from exc


def _inspect(func: StepFunction, name: str, kind: str) -> tuple[bool, Mapping[str, Option]]:
    """`(takes_ctx, options)` for a node function. Raises `StepDefinitionError` for any other shape."""
    signature = _signature(func, name)
    positional = []
    options: dict[str, Option] = {}
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            raise StepDefinitionError(f"{kind} {safe_token(name)}: *args is not a {kind} signature")
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            raise StepDefinitionError(
                f"{kind} {safe_token(name)}: **kwargs would accept any `with` key, and an unknown key must be "
                "refused (channel-spec README 3.3); declare each option as a keyword-only parameter"
            )
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            positional.append(parameter)
            continue
        key = _with_key(parameter.name)
        required = parameter.default is inspect.Parameter.empty
        if required and (not key[:1].isalpha() or len(key) > _MAX_WITH_KEY):
            raise StepDefinitionError(
                f"{kind} {safe_token(name)}: required keyword-only parameter {safe_token(parameter.name)} "
                "can never be bound, because no `with` key can name it"
            )
        options[key] = Option(key=key, parameter=parameter.name, required=required, annotation=parameter.annotation)

    if len(positional) not in (1, 2):
        raise StepDefinitionError(
            f"{kind} {safe_token(name)}: a {kind} takes (message) or (message, ctx), "
            f"not {len(positional)} positional parameters"
        )
    return len(positional) == 2, MappingProxyType(options)


# -- binding `with` -----------------------------------------------------------------------------------


def _conforms(annotation: Any, value: object) -> bool | None:
    """Whether `value` fits `annotation`. `None` when the annotation is not one the binder understands."""
    if annotation is inspect.Parameter.empty or annotation is Any or annotation is object:
        return True
    if isinstance(annotation, str):
        return None
    if annotation is bool:
        return isinstance(value, bool)
    if annotation is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if annotation is float:
        return isinstance(value, int | float) and not isinstance(value, bool)
    if annotation is str:
        return isinstance(value, str)
    if annotation is type(None):
        return value is None
    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)
    if origin is Literal:
        return any(value == choice and type(value) is type(choice) for choice in arguments)
    if origin is typing.Union or origin is types.UnionType:
        verdicts = [_conforms(member, value) for member in arguments]
        if any(verdict is True for verdict in verdicts):
            return True
        return None if any(verdict is None for verdict in verdicts) else False
    if origin in (list, tuple, collections.abc.Sequence, collections.abc.Collection, collections.abc.Iterable):
        if not isinstance(value, list | tuple):
            return False
        if origin is tuple and not (len(arguments) == 2 and arguments[1] is Ellipsis):
            return None  # fixed-length tuples are not a `with` shape
        item = arguments[0] if arguments else Any
        verdicts = [_conforms(item, element) for element in value]
        if any(verdict is False for verdict in verdicts):
            return False
        return None if any(verdict is None for verdict in verdicts) else True
    return None


def _frozen(annotation: Any, value: object) -> object:
    """Arrays become tuples, so one message cannot mutate the configuration the next one sees.

    A parameter annotated `list[...]` gets a list, because handing it a tuple would make its own
    annotation a lie; it is a copy taken at bind time.
    """
    if not isinstance(value, list | tuple):
        return value
    if typing.get_origin(annotation) is list:
        return list(value)
    return tuple(value)


def bind_options(name: str, options: Mapping[str, Option], values: Mapping[str, object] | None) -> dict[str, object]:
    """Map a step's `with` values to keyword arguments. Raises `StepConfigError`; never quotes a value."""
    values = {} if values is None else values
    if not isinstance(values, Mapping):
        raise StepConfigError(f"step {safe_token(name)}: `with` must be a mapping")
    bound: dict[str, object] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key.isidentifier():
            raise StepConfigError(
                f"step {safe_token(name)}: `with` key {safe_token(key)} is not a Python identifier, so it "
                "cannot name a keyword-only parameter"
            )
        if key.endswith("_") and keyword.iskeyword(key[:-1]):
            raise StepConfigError(
                f"step {safe_token(name)}: write `with` key {safe_token(key[:-1])}, not {safe_token(key)}"
            )
        option = options.get(key)
        if option is None:
            accepted = ", ".join(sorted(options)) or "none"
            raise StepConfigError(
                f"step {safe_token(name)}: unknown `with` key {safe_token(key)} (accepted: {accepted})"
            )
        if _conforms(option.annotation, value) is False:
            raise StepConfigError(
                f"step {safe_token(name)}: `with` key {safe_token(key)} has the wrong type ({type(value).__name__})"
            )
        bound[option.parameter] = _frozen(option.annotation, value)
    missing = sorted(option.key for option in options.values() if option.required and option.parameter not in bound)
    if missing:
        raise StepConfigError(f"step {safe_token(name)}: required `with` key(s) missing: {', '.join(missing)}")
    return bound


class _Node:
    """What `Step` and `Splitter` share: a checked signature, and `with` values bound by name."""

    kind = "step"

    def __init__(self, func: StepFunction, *, name: str | None = None) -> None:
        if not callable(func):
            raise StepDefinitionError(f"@{self.kind} must decorate a callable")
        self.name: str = name or getattr(func, "__name__", None) or "anonymous"
        self.takes_ctx, self.options = _inspect(func, self.name, self.kind)
        self.func = func
        self._bound: Mapping[str, object] = MappingProxyType({})
        self._unbound_required = tuple(sorted(option.key for option in self.options.values() if option.required))
        functools.update_wrapper(self, func)  # type: ignore[arg-type]  # _Node itself defines no __call__

    @property
    def bound(self) -> Mapping[str, object]:
        """The keyword arguments this node is called with, by parameter name."""
        return self._bound

    def bind(self, values: Mapping[str, object] | None) -> typing.Self:
        """A copy of this node with `with` values bound. Raises `StepConfigError`.

        Binding is by replacement, not merge: `bind({})` on a bound node clears it, which is what a
        hot reload that removed a key from `with` must do.
        """
        bound = bind_options(self.name, self.options, values)
        clone = copy.copy(self)
        clone._bound = MappingProxyType(bound)
        clone._unbound_required = ()
        return clone

    def _invoke(self, message: Message, ctx: Context | None) -> object:
        if not isinstance(message, Message):
            raise StepDefinitionError(
                f"{self.kind} {safe_token(self.name)}: first argument must be a Message, not {type(message).__name__}"
            )
        if self._unbound_required:
            raise StepDefinitionError(
                f"{self.kind} {safe_token(self.name)} needs `with` values ({', '.join(self._unbound_required)}): "
                "bind them with .bind({...}) or run_step(..., with_={...})"
            )
        if self.takes_ctx:
            if ctx is None:
                raise StepDefinitionError(
                    f"{self.kind} {safe_token(self.name)} takes a ctx: pass one, or use run_step, which supplies a fake"
                )
            return self.func(message, ctx, **self._bound)
        return self.func(message, **self._bound)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={safe_token(self.name)}, takes_ctx={self.takes_ctx})"


class Step(_Node):
    """A callable pipeline stage with a checked signature and a checked return value."""

    kind = "step"

    def __call__(self, message: Message, ctx: Context | None = None) -> StepResult:
        """Run the step and normalise its outcome to `Message` or `Drop`. `Reject` propagates."""
        result = self._invoke(message, ctx)
        if result is None:
            return message
        if isinstance(result, Message | Drop):
            return result
        raise StepReturnError(
            f"step {safe_token(self.name)} returned {type(result).__name__}: a step returns Message, Drop or None. "
            "To change the payload use message.with_payload(body, content_type=...)"
        )


class Splitter(_Node):
    """A fan-out node: one message in, up to `MAX_SPLIT_CHILDREN` children out (README 3.3.2).

    Children are returned with their identity already stamped (`stamp_children`), so the runtime and a
    unit test see the same ids.
    """

    kind = "splitter"

    def __call__(self, message: Message, ctx: Context | None = None) -> SplitResult:
        result = self._invoke(message, ctx)
        if isinstance(result, Drop):
            return result
        if result is None or isinstance(result, Message | bytes | bytearray | str | Mapping):
            raise StepReturnError(
                f"splitter {safe_token(self.name)} returned {type(result).__name__}: a splitter returns an "
                "iterable of Message, or a Drop. A one-to-one change is a @step"
            )
        if not isinstance(result, Iterable):
            raise StepReturnError(
                f"splitter {safe_token(self.name)} returned {type(result).__name__}, which is not iterable"
            )
        children: list[Message] = []
        for child in result:
            if not isinstance(child, Message):
                raise StepReturnError(
                    f"splitter {safe_token(self.name)} yielded {type(child).__name__}: every child is a Message "
                    "(message.with_payload(part, content_type=...))"
                )
            if len(children) == MAX_SPLIT_CHILDREN:
                # Stop consuming: a generator that yields a million children never gets to allocate them.
                raise Reject.engine(SPLIT_TOO_LARGE)
            children.append(child)
        if not children:
            return Drop(reason=EMPTY_SPLIT_REASON)
        return stamp_children(message, children)


def stamp_children(parent: Message, children: Iterable[Message]) -> tuple[Message, ...]:
    """Give each child its engine-owned identity (README 3.3.2 split rules 1 and 2).

    * `tb-msg-id`: `derived_uuid7(parent, index)`, so a redelivered parent reproduces the same children;
    * `tb-parent-id`: the parent's id;
    * the parent's record key, and the parent's channel, version, source, receive time and correlation
      id — a child was received when, where and how its parent was;
    * `tb-attempt` 0 and a child span of the parent's trace.

    Every other header the splitter set on a child is kept; a header the child lacks is inherited from
    the parent, so a child built from scratch is still a contract-shaped record.
    """
    parent_id = parent.msg_id
    inherited = {
        name: value
        for name in (h.TB_CHANNEL, h.TB_CHANNEL_VERSION, h.TB_SOURCE, h.TB_RECEIVED_AT, h.TB_CORRELATION_ID)
        if (value := parent.get(name)) is not None
    }
    stamped = []
    for index, child in enumerate(children):
        headers = {**parent.headers, **child.headers, **inherited}
        headers[h.TB_MSG_ID] = derived_uuid7(parent_id, index)
        headers[h.TB_PARENT_ID] = parent_id
        headers[h.TB_ATTEMPT] = "0"
        if parent.traceparent is not None:
            headers[h.TRACEPARENT] = child_traceparent(parent.traceparent)
        stamped.append(Message(headers=headers, payload=child.payload, key=parent.key))
    return tuple(stamped)


Node = Step | Splitter


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


@overload
def splitter(func: StepFunction, /) -> Splitter: ...


@overload
def splitter(*, name: str | None = ...) -> Callable[[StepFunction], Splitter]: ...


def splitter(
    func: StepFunction | None = None, /, *, name: str | None = None
) -> Splitter | Callable[[StepFunction], Splitter]:
    """Declare a fan-out node. Refused at load on a `per-key` channel (README 3.3.2 split rule 3)."""
    if func is not None:
        return Splitter(func)

    def decorate(inner: StepFunction) -> Splitter:
        return Splitter(inner, name=name)

    return decorate
