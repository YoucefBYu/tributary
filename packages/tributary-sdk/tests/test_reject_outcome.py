"""`Reject(code)`: the non-retryable failure a step may raise (channel-spec README 3.3.2)."""

from __future__ import annotations

import pytest
from tributary.sdk import Message, Reject, RejectCodeError, step
from tributary.sdk.codes import ENGINE_CODES, SPLIT_TOO_LARGE, is_engine_code
from tributary.sdk.testing import make_message, run_step


def test_a_channel_code_is_carried_as_is() -> None:
    reject = Reject("ORDER_WITHOUT_VISIT")
    assert reject.code == "ORDER_WITHOUT_VISIT"
    assert "ORDER_WITHOUT_VISIT" in str(reject)


@pytest.mark.parametrize("code", ["", "AB", "lower_case", "1LEADING", "HAS SPACE", "X" * 65, "DASH-ED", None, 42])
def test_a_code_outside_the_grammar_is_refused(code: object) -> None:
    with pytest.raises(RejectCodeError):
        Reject(code)  # type: ignore[arg-type]


@pytest.mark.parametrize("code", [*sorted(ENGINE_CODES), "KEY_EMPTY", "HTTP_5XX", "TB_ANYTHING"])
def test_every_engine_code_is_refused_to_a_step(code: str) -> None:
    """So an alert on `HEADER_PARSE_ERROR` (or any engine code) keeps meaning what it says."""
    assert is_engine_code(code)
    with pytest.raises(RejectCodeError, match="reserved"):
        Reject(code)


def test_the_engine_may_raise_its_own_codes_and_only_those() -> None:
    assert Reject.engine(SPLIT_TOO_LARGE).code == SPLIT_TOO_LARGE
    with pytest.raises(RejectCodeError):
        Reject.engine("ORDER_WITHOUT_VISIT")


def test_a_step_raising_reject_propagates_through_run_step() -> None:
    @step
    def refuse(msg: Message) -> None:
        raise Reject("NOT_FOR_THIS_CHANNEL")

    with pytest.raises(Reject) as caught:
        run_step(refuse, make_message())
    assert caught.value.code == "NOT_FOR_THIS_CHANNEL"


def test_a_bad_code_fails_at_the_raise_site_as_an_ordinary_error() -> None:
    """The mistake is a red unit test, not a dead letter labelled with a code nobody alerts on."""

    @step
    def mislabelled(msg: Message) -> None:
        raise Reject("STEP_ERROR")

    with pytest.raises(RejectCodeError):
        run_step(mislabelled, make_message())
