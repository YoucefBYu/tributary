"""`topic_name` and the raw envelope this receiver actually emits, validated against the real
`contracts/envelope/v0` schema (gate requirement 10: "validated against contracts/envelope/v0",
not asserted by inspection). Uses the SDK's own `record_projection`/`validate_for_produce`
(`tributary.sdk.projection`) rather than a second, hand-rolled copy of the projection mapping —
`jsonschema` is a root workspace dev dependency (`tests/conformance` uses the same library)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.validators import validator_for
from tributary.receivers.mllp.kafka_publisher import CONTENT_TYPE, build_raw_envelope, topic_name
from tributary.sdk.errors import EnvelopeError
from tributary.sdk.projection import record_projection, validate_for_produce

SCHEMA_PATH = Path(__file__).resolve().parents[4] / "contracts" / "envelope" / "v0" / "envelope.schema.json"


def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    cls = validator_for(schema, default=Draft202012Validator)
    return cls(schema, format_checker=FormatChecker())


def test_raw_envelope_validates_against_the_envelope_schema() -> None:
    message = build_raw_envelope(
        channel="adt-passthrough",
        channel_version=0,
        source="mllp:mllp-0@10.0.0.1:5555",
        payload=b"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|C1|P|2.5\r",
        key=None,
    )
    projection = record_projection(message, topic_class="raw", topic="dev.adt-passthrough.raw")
    errors = sorted(_validator().iter_errors(projection))
    assert errors == [], [f"{e.json_path} [{e.validator}]" for e in errors]


def test_raw_envelope_passes_validate_for_produce_with_ordering_none() -> None:
    message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)
    validate_for_produce(message, topic_class="raw", ordering="none")  # must not raise


def test_validate_for_produce_refuses_a_null_key_under_per_key_ordering() -> None:
    # this receiver never asks for ordering: per-key (config.py rejects it at load time), but the
    # SDK check itself is exercised directly here since it is the thing kafka_publisher relies on.
    message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)
    with pytest.raises(EnvelopeError, match="ordering"):
        validate_for_produce(message, topic_class="raw", ordering="per-key")


def test_raw_envelope_content_type_is_hl7_er7() -> None:
    message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)
    assert message.content_type == CONTENT_TYPE == "application/hl7-v2+er7"


def test_raw_envelope_msg_id_is_uuid7_and_attempt_is_zero() -> None:
    message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)
    assert message.msg_id  # parse_uuid(version7=True) inside the property already asserts the shape
    assert message.attempt == 0


@pytest.mark.parametrize(("env", "channel"), [("dev", "adt"), ("prod-1", "lab-results")])
def test_topic_name_shape(env: str, channel: str) -> None:
    assert topic_name(env, channel) == f"{env}.{channel}.raw"


@pytest.mark.parametrize("env", ["Dev", "1dev", "dev.sub", ""])
def test_topic_name_rejects_bad_env(env: str) -> None:
    with pytest.raises(ValueError, match="env"):
        topic_name(env, "adt")


def test_topic_name_rejects_bad_channel() -> None:
    with pytest.raises(ValueError, match="channel"):
        topic_name("dev", "ADT")


def _no_producer(*_a: object, **_k: object) -> object:
    raise AssertionError("a Producer was built for a refused SSL configuration")


@pytest.mark.parametrize(
    "extra",
    [{}, {"ssl.certificate.location": "/run/secrets/tls.crt"}, {"ssl.key.location": "/run/secrets/tls.key"}],
)
def test_raw_publisher_refuses_server_auth_only_ssl(extra: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.receivers.mllp.kafka_publisher import RawPublisher
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.errors import KafkaSecurityError
    from tributary.runtime.kafka_security import PINNED_CIPHER_SUITES

    monkeypatch.setattr("confluent_kafka.Producer", _no_producer)
    security = {
        "security.protocol": "SSL",
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        "ssl.cipher.suites": PINNED_CIPHER_SUITES,
        **extra,
    }
    with pytest.raises(KafkaSecurityError) as exc:
        RawPublisher(KafkaSettings("broker.example:9093", security=security), "dev.c.raw")
    assert "ssl.certificate.location" in str(exc.value)


def test_a_closed_publisher_refuses_later_publishes_and_never_reports_a_commit() -> None:
    import asyncio

    from tributary.receivers.mllp.kafka_publisher import RawPublisher
    from tributary.runtime.clients import KafkaSettings

    publisher = RawPublisher(
        KafkaSettings(bootstrap_servers="127.0.0.1:19"),
        "dev.c.raw",
        topic_defaults=None,
    )

    async def go() -> list[bool]:
        publisher.start(asyncio.get_running_loop())
        publisher.close(timeout_s=0.1)
        publisher.close(timeout_s=0.1)  # idempotent
        message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)
        return [await publisher.publish(message, timeout_s=0.5)]

    assert asyncio.run(go()) == [False]


def test_publish_result_tells_a_delivery_timeout_from_a_failure() -> None:
    """After a timeout librdkafka may still complete the write, so only that `AR` can duplicate on a resend:
    sync reply mode answers it `PRODUCE_TIMEOUT`, distinct from `PRODUCE_FAILED` (README 3.2.6)."""
    import asyncio

    from tributary.receivers.mllp.kafka_publisher import RawPublisher
    from tributary.runtime.clients import KafkaSettings

    publisher = RawPublisher(KafkaSettings(bootstrap_servers="127.0.0.1:19"), "dev.c.raw", topic_defaults=None)
    message = build_raw_envelope(channel="c", channel_version=0, source="mllp:i@p", payload=b"x", key=None)

    async def go() -> tuple[str, bool, str]:
        publisher.start(asyncio.get_running_loop())
        try:
            timed_out = await publisher.publish_result(message, timeout_s=0.3)  # no broker answers: no report
            as_bool = await publisher.publish(message, timeout_s=0.3)
        finally:
            publisher.close(timeout_s=0.1)
        return timed_out, as_bool, await publisher.publish_result(message, timeout_s=0.3)

    assert asyncio.run(go()) == ("timeout", False, "failed")  # a closed producer is a failure, not a timeout
