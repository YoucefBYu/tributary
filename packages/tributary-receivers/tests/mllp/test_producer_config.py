"""`RawPublisher`'s effective producer config equals `contracts/topics/v0/topic-defaults.json`'s
`clients.receiver_producer` profile — the acceptance criterion the contract's `must_not_hardcode`
list asks each component to keep a test for (README.md 3.7): "devops-engineer and platform-engineer
each keep a test asserting the component's effective configuration equals the value here." This is
this component's copy of that test, for the three values named explicitly (`acks`,
`enable.idempotence`, `delivery.timeout.ms`).

Monkeypatches `confluent_kafka.Producer` to capture the config dict rather than connecting to a
broker: constructing a `Producer` does not itself connect, but nothing in this test needs a real
client, only the dict `RawPublisher` builds.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.mllp.kafka_publisher import RawPublisher
from tributary.runtime.config import load_topic_defaults

TOPIC_DEFAULTS_PATH = Path(__file__).resolve().parents[4] / "contracts" / "topics" / "v0" / "topic-defaults.json"


class _CapturingProducer:
    last_config: dict[str, Any] | None = None

    def __init__(self, config: dict[str, Any]) -> None:
        type(self).last_config = config

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


@pytest.fixture(autouse=True)
def _capture_producer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _CapturingProducer)


def _contract_receiver_producer() -> dict[str, Any]:
    document = json.loads(TOPIC_DEFAULTS_PATH.read_text(encoding="utf-8"))
    return document["clients"]["receiver_producer"]  # type: ignore[no-any-return]


def test_load_topic_defaults_reads_the_same_file_this_test_pins() -> None:
    """A sanity check that `load_topic_defaults`'s search order actually finds the contract file
    this test reads directly, before trusting any comparison against it."""
    assert load_topic_defaults().source == TOPIC_DEFAULTS_PATH


@pytest.mark.parametrize("key", ["acks", "enable.idempotence", "delivery.timeout.ms"])
def test_effective_producer_config_matches_the_contract(key: str) -> None:
    contract = _contract_receiver_producer()
    RawPublisher("localhost:9092", "dev.c.raw")
    config = _CapturingProducer.last_config
    assert config is not None
    assert config[key] == contract[key]


def test_effective_config_cannot_be_downgraded_by_a_caller() -> None:
    """`RawPublisher` accepts a `linger_ms` override (not in `must_not_hardcode`) but no parameter
    at all for `acks`, `enable.idempotence` or `delivery.timeout.ms` — there is no argument to pass
    that would ACK on less than `acks=all`."""
    import inspect

    params = inspect.signature(RawPublisher.__init__).parameters
    assert "acks" not in params
    assert "enable_idempotence" not in params
    assert "delivery_timeout_ms" not in params


def test_linger_ms_override_does_not_touch_the_durability_settings() -> None:
    contract = _contract_receiver_producer()
    RawPublisher("localhost:9092", "dev.c.raw", linger_ms=999)
    config = _CapturingProducer.last_config
    assert config is not None
    assert config["linger.ms"] == 999
    assert config["acks"] == contract["acks"]
    assert config["enable.idempotence"] == contract["enable.idempotence"]
