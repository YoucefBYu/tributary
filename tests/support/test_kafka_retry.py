"""`kafka_call_with_retry`: coordinator races are retried within the deadline; anything else is raised at once."""

from __future__ import annotations

import pytest
from confluent_kafka import KafkaError, KafkaException

from tests.support.kafka_helpers import kafka_call_with_retry


def _failing(codes: list[int], result: str = "ok") -> tuple[list[float], object]:
    seen: list[float] = []

    def call(left: float) -> str:
        seen.append(left)
        if codes:
            raise KafkaException(KafkaError(codes.pop(0)))
        return result

    return seen, call


@pytest.mark.parametrize(
    "code", [KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError.COORDINATOR_NOT_AVAILABLE]
)
def test_a_coordinator_race_is_retried_until_it_settles(code: int) -> None:
    seen, call = _failing([code, code])
    assert kafka_call_with_retry(call, timeout_s=5, pause_s=0.01) == "ok"  # type: ignore[arg-type]
    assert len(seen) == 3 and all(0 < left <= 5 for left in seen)


def test_a_non_retriable_error_is_raised_at_once() -> None:
    seen, call = _failing([KafkaError.TOPIC_AUTHORIZATION_FAILED])
    with pytest.raises(KafkaException) as raised:
        kafka_call_with_retry(call, timeout_s=5, pause_s=0.01)  # type: ignore[arg-type]
    assert raised.value.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED and len(seen) == 1


def test_a_race_that_outlasts_the_deadline_is_raised() -> None:
    seen, call = _failing([KafkaError.NOT_COORDINATOR] * 1000)
    with pytest.raises(KafkaException) as raised:
        kafka_call_with_retry(call, timeout_s=0.3, pause_s=0.05)  # type: ignore[arg-type]
    assert raised.value.args[0].code() == KafkaError.NOT_COORDINATOR and 1 < len(seen) < 20
