"""`@step` and `Drop`: the signature and return contract, checked at decoration time where possible."""

from __future__ import annotations

import pytest
from tributary.sdk import Drop, Message, Step, step
from tributary.sdk.errors import DropReasonError, StepDefinitionError, StepReturnError
from tributary.sdk.fakes import FakeContext
from tributary.sdk.testing import make_message


@step
def passthrough(msg: Message) -> None:
    return None


@step
def tag(msg: Message, ctx: object) -> Message:
    return msg.with_header("tb-destination", "fhir-server")


@step(name="only-json")
def only_json(msg: Message) -> Message | Drop:
    if msg.content_type != "application/json":
        return Drop(reason="unsupported content type")
    return msg


def test_returning_none_passes_the_message_through_unchanged() -> None:
    message = make_message()
    assert passthrough(message) is message


def test_step_without_ctx_is_detected_and_called_with_one_argument() -> None:
    assert passthrough.takes_ctx is False
    assert tag.takes_ctx is True


def test_a_ctx_step_called_without_one_says_so() -> None:
    with pytest.raises(StepDefinitionError) as caught:
        tag(make_message())
    assert "run_step" in str(caught.value)


def test_drop_is_returned_as_is() -> None:
    outcome = only_json(make_message(content_type="text/csv"))
    assert isinstance(outcome, Drop)
    assert outcome.reason == "unsupported content type"


def test_explicit_name_is_kept() -> None:
    assert only_json.name == "only-json"
    assert passthrough.name == "passthrough"


def test_an_unexpected_return_type_is_a_typed_error_that_names_the_fix() -> None:
    @step
    def returns_bytes(msg: Message) -> bytes:
        return b"transformed"

    with pytest.raises(StepReturnError) as caught:
        returns_bytes(make_message())
    assert "with_payload" in str(caught.value)


def test_a_returned_object_is_not_rendered_into_the_error() -> None:
    """A step's return value can be a parsed message: its repr is PHI, so only the type name is used."""

    class ParsedPatient:
        def __repr__(self) -> str:
            return "ParsedPatient(name='SYNTH^JANE')"

    @step
    def returns_object(msg: Message) -> ParsedPatient:
        return ParsedPatient()

    with pytest.raises(StepReturnError) as caught:
        returns_object(make_message())
    assert "ParsedPatient" in str(caught.value)
    assert "SYNTH" not in str(caught.value)


@pytest.mark.parametrize(
    "func",
    [
        lambda: None,
        lambda a, b, c: None,
        lambda *args: None,
    ],
)
def test_wrong_signatures_are_refused_at_decoration_time(func: object) -> None:
    with pytest.raises(StepDefinitionError):
        Step(func)  # type: ignore[arg-type]


def test_keyword_only_parameters_need_defaults() -> None:
    def with_required_kwarg(msg: Message, *, threshold: int) -> None:
        return None

    with pytest.raises(StepDefinitionError):
        Step(with_required_kwarg)

    def with_defaulted_kwarg(msg: Message, *, threshold: int = 1) -> None:
        return None

    assert Step(with_defaulted_kwarg).takes_ctx is False


def test_first_argument_must_be_a_message() -> None:
    with pytest.raises(StepDefinitionError):
        passthrough("not a message")  # type: ignore[arg-type]


def test_steps_compose_through_a_shared_ctx() -> None:
    ctx = FakeContext()
    outcome = tag(passthrough(make_message()), ctx)
    assert isinstance(outcome, Message)
    assert outcome.destination == "fhir-server"


@pytest.mark.parametrize("reason", ["", "   ", "x" * 513, "two\nlines"])
def test_drop_reasons_are_bounded_and_single_line(reason: str) -> None:
    """The reason lands in `_tb.events.error_message`: capped at 512 and never a forged second line."""
    with pytest.raises(DropReasonError):
        Drop(reason=reason)


def test_drop_is_frozen() -> None:
    drop = Drop(reason="unsupported trigger event")
    with pytest.raises(Exception):  # noqa: B017 - FrozenInstanceError is a dataclasses detail
        drop.reason = "other"  # type: ignore[misc]
