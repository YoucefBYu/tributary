"""The test helper that rides out group-coordinator races on a fresh broker (no Kafka needed)."""

from __future__ import annotations

import time
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException


class Flaky:
    def __init__(self, *errors: KafkaError, result: Any = (0, 5)) -> None:
        self.errors, self.result, self.calls = list(errors), result, 0

    def __call__(self) -> Any:
        self.calls += 1
        if self.errors:
            raise KafkaException(self.errors.pop(0))
        return self.result


@pytest.mark.parametrize("code", [KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS])
def test_a_coordinator_error_once_then_success_is_retried(kafka_retry: Any, code: int) -> None:
    call = Flaky(KafkaError(code))
    assert kafka_retry(call, deadline=time.monotonic() + 5, pause_s=0.01) == (0, 5)
    assert call.calls == 2


def test_a_retriable_error_is_retried(kafka_retry: Any) -> None:
    call = Flaky(KafkaError(KafkaError._TIMED_OUT, retriable=True))
    assert kafka_retry(call, deadline=time.monotonic() + 5, pause_s=0.01) == (0, 5) and call.calls == 2


def test_a_non_retriable_error_is_raised_at_once(kafka_retry: Any) -> None:
    call = Flaky(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))
    with pytest.raises(KafkaException):
        kafka_retry(call, deadline=time.monotonic() + 5, pause_s=0.01)
    assert call.calls == 1


def test_the_deadline_bounds_the_retries(kafka_retry: Any) -> None:
    call = Flaky(*[KafkaError(KafkaError.NOT_COORDINATOR)] * 1000)
    started = time.monotonic()
    with pytest.raises(KafkaException):
        kafka_retry(call, deadline=started + 0.2, pause_s=0.05)
    assert time.monotonic() - started < 1.0
