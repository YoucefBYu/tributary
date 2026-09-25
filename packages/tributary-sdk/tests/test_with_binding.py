"""`with` binds to keyword-only parameters by name, and is refused loudly when wrong (README 3.3).

Every refusal named in the contract gets a test: an unknown key, a key that is not a Python identifier,
and a required keyword-only parameter with no value. Type mismatches and the keyword spelling (`in`
binds to `in_`) are the binder's own rules and are tested the same way.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from tributary.sdk import Message, Step, StepConfigError, StepDefinitionError, step
from tributary.sdk.fakes import FakeContext
from tributary.sdk.testing import make_message, run_step


@step
def configured(
    msg: Message,
    ctx: FakeContext,
    *,
    version: str = "2.5.1",
    threshold: int,
    codes: Sequence[str] = (),
    in_: Sequence[str] = (),
    strict: bool = False,
) -> None:
    ctx.vars["seen"] = {"version": version, "threshold": threshold, "codes": codes, "in": in_, "strict": strict}


def seen(with_: dict[str, object]) -> dict[str, object]:
    ctx = FakeContext()
    run_step(configured, make_message(), ctx=ctx, with_=with_)
    value = ctx.vars["seen"]
    assert isinstance(value, dict)
    return value


def test_values_bind_by_exact_name_and_defaults_fill_the_rest() -> None:
    assert seen({"threshold": 3}) == {"version": "2.5.1", "threshold": 3, "codes": (), "in": (), "strict": False}
    assert seen({"threshold": 3, "version": "2.3"})["version"] == "2.3"


def test_a_python_keyword_key_binds_to_the_trailing_underscore_parameter() -> None:
    """`in` is the filter step's contract key and not a legal parameter name (PEP 8: `in_`)."""
    assert configured.options["in"].parameter == "in_"
    assert seen({"threshold": 1, "in": ["A01", "A04"]})["in"] == ("A01", "A04")


def test_the_underscore_spelling_is_refused_so_the_spec_has_one_spelling() -> None:
    with pytest.raises(StepConfigError, match="write `with` key in"):
        configured.bind({"threshold": 1, "in_": ["A01"]})


def test_an_unknown_key_is_refused_and_names_the_accepted_keys() -> None:
    with pytest.raises(StepConfigError) as caught:
        configured.bind({"threshold": 1, "verison": "2.5"})
    assert "verison" in str(caught.value)
    assert "version" in str(caught.value)


@pytest.mark.parametrize("key", ["on-unresolved", "2fast", ""])
def test_a_key_that_is_not_a_python_identifier_is_refused(key: str) -> None:
    """The schema admits `-` in a `with` key; the binder does not (README 3.3)."""
    with pytest.raises(StepConfigError, match="not a Python identifier"):
        configured.bind({"threshold": 1, key: "x"})


def test_a_missing_required_key_is_refused_at_bind_not_at_first_message() -> None:
    with pytest.raises(StepConfigError, match="threshold"):
        configured.bind({})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("threshold", "3"),
        ("threshold", True),  # bool is an int subclass; a flag is not a count
        ("version", 2.5),  # YAML turns 2.5 into a float: the author meant "2.5"
        ("codes", "A01"),  # a scalar where a list is expected
        ("codes", ["A01", 7]),
        ("strict", "yes"),
    ],
)
def test_a_value_of_the_wrong_type_is_refused_without_quoting_it(key: str, value: object) -> None:
    with pytest.raises(StepConfigError) as caught:
        configured.bind({"threshold": 1, key: value})
    assert key in str(caught.value)
    assert repr(value) not in str(caught.value) or isinstance(value, bool)


def test_arrays_are_frozen_so_one_message_cannot_change_the_next_one_s_config() -> None:
    values: dict[str, object] = {"threshold": 1, "codes": ["A01"]}
    bound = configured.bind(values)
    values["codes"].append("A99")  # type: ignore[attr-defined]
    assert bound.bound["codes"] == ("A01",)


def test_binding_returns_a_copy_and_leaves_the_decorated_step_unbound() -> None:
    bound = configured.bind({"threshold": 5})
    assert isinstance(bound, Step)
    assert bound is not configured
    assert configured.bound == {}
    with pytest.raises(StepDefinitionError, match="threshold"):
        configured(make_message(), FakeContext())


def test_a_step_with_no_options_refuses_any_key() -> None:
    @step
    def plain(msg: Message) -> None:
        return None

    assert plain.bind(None)(make_message()) is not None
    with pytest.raises(StepConfigError, match="accepted: none"):
        plain.bind({"version": "2.5.1"})


def test_kwargs_is_refused_at_decoration_because_it_would_swallow_unknown_keys() -> None:
    def swallow(msg: Message, **options: object) -> None:
        return None

    with pytest.raises(StepDefinitionError, match="kwargs"):
        Step(swallow)


def test_a_required_parameter_no_key_can_name_is_refused_at_decoration() -> None:
    def private(msg: Message, *, _secret_knob: int) -> None:
        return None

    with pytest.raises(StepDefinitionError, match="never be bound"):
        Step(private)


def test_an_unresolvable_annotation_binds_without_a_type_check() -> None:
    """A name imported under TYPE_CHECKING leaves a string annotation; the binder cannot check it and
    must not guess, so the value binds as given."""

    def loose(msg: Message, *, shape: NotImportedAnywhere = None) -> None:  # type: ignore[name-defined]  # noqa: F821
        return None

    node = Step(loose)
    assert node.bind({"shape": "anything"}).bound == {"shape": "anything"}
