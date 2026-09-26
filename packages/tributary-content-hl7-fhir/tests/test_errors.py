"""`errors.safe_build`: no PHI reaches a caller, anywhere in the exception chain.

Direct unit tests of the mechanism itself, not just the mapping call sites that happen to use it --
`fhir.resources`' own `ValidationError` embeds the rejected value verbatim (`input_value=...`), and this
is the one place in the package responsible for making sure that text never survives past this function.
"""

from __future__ import annotations

import pytest
from tributary.content_hl7_fhir.errors import InvalidFieldError, safe_build

SECRET = "SYNTHVERYSECRETVALUE"  # noqa: S105 -- a marker string, not a real credential


def test_safe_build_returns_the_factory_result_on_success() -> None:
    assert safe_build(lambda: 42, path="X") == 42


def test_safe_build_raises_invalid_field_error_on_any_exception() -> None:
    def factory() -> None:
        raise ValueError(SECRET)

    with pytest.raises(InvalidFieldError) as excinfo:
        safe_build(factory, path="PID.7")
    assert excinfo.value.path == "PID.7"


def test_safe_build_never_includes_the_original_message_text() -> None:
    def factory() -> None:
        raise ValueError(f"input_value={SECRET!r}")

    with pytest.raises(InvalidFieldError) as excinfo:
        safe_build(factory, path="PID.7")
    assert SECRET not in str(excinfo.value)


def test_safe_build_clears_cause_and_context() -> None:
    """The core claim of the docstring, checked directly: raising after the `try`/`except` has already
    finished (a sentinel, not `raise ... from None` inside the `except` suite) means the original
    exception is not reachable through `__context__` either, not just through `__cause__`."""

    def factory() -> None:
        raise ValueError(SECRET)

    with pytest.raises(InvalidFieldError) as excinfo:
        safe_build(factory, path="PID.7")
    exc = excinfo.value
    assert exc.__cause__ is None
    assert exc.__context__ is None
    assert SECRET not in repr(exc.__traceback__)


def test_safe_build_a_falsy_result_is_not_mistaken_for_failure() -> None:
    """`None`, `False` and `0` are all legitimate factory results -- the sentinel must not confuse any
    of them with "the factory raised"."""
    assert safe_build(lambda: None, path="X") is None
    assert safe_build(lambda: False, path="X") is False
    assert safe_build(lambda: 0, path="X") == 0
