"""Content rows: stage from the topic, claim-check, oversize, channel switch, PHI hygiene (synthetic data only)."""

from __future__ import annotations

import re
from typing import Any

import pytest
from tributary.archiver.config import content_group, load_config
from tributary.archiver.content import COLUMNS, MAX_INLINE_BYTES, content_ddl, parse_content_record
from tributary.archiver.content_archiver import topic_pattern
from tributary.archiver.errors import ArchiverConfigError, EventRejected
from tributary.sdk.ids import uuid7

# MSH-3 TRIBUTARY-SYNTH: the synthetic marker every fixture carries.
SYNTH = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|DST|FAC|20260101000000||ADT^A01|C1|P|2.5\rPID|1||SYNTH-0001\r"
ENABLED = frozenset({"alpha"})


def headers(channel: str = "alpha", **extra: str) -> list[tuple[str, bytes]]:
    base = {"tb-msg-id": uuid7(), "tb-channel": channel, **extra}
    return [(k, v.encode()) for k, v in base.items()]


def parse(topic: str, raw: list[tuple[str, bytes]], value: bytes | None = SYNTH, **kw: Any) -> Any:
    return parse_content_record(
        topic=topic,
        raw_headers=raw,
        value=value,
        timestamp_ms=1_780_000_000_000,
        partition=2,
        offset=7,
        enabled_channels=kw.get("enabled", ENABLED),
    )


def row(topic: str, raw: list[tuple[str, bytes]], value: bytes | None = SYNTH) -> dict[str, Any]:
    return dict(zip(COLUMNS, parse(topic, raw, value).values, strict=True))


@pytest.mark.parametrize(
    ("topic", "stage", "destination", "extra"),
    [
        ("dev.alpha.raw", "received", "", {}),
        ("dev.alpha.out.sink", "routed", "sink", {"tb-destination": "sink"}),
        ("dev.alpha.dlq", "dlq", "sink", {"tb-destination": "sink", "tb-error-code": "DEST_FAILED"}),
        ("dev.alpha.dlq", "dlq", "", {}),
    ],
)
def test_stage_and_destination_come_from_the_topic(
    topic: str, stage: str, destination: str, extra: dict[str, str]
) -> None:
    found = row(topic, headers(**extra))
    assert (found["stage"], found["destination"], found["payload"], found["size_bytes"]) == (
        stage,
        destination,
        SYNTH,
        len(SYNTH),
    )
    assert (found["topic"], found["partition"], found["offset"]) == (topic, 2, 7)


def test_a_header_cannot_change_the_stage() -> None:
    assert row("dev.alpha.raw", headers(**{"tb-stage": "dlq"}))["stage"] == "received"


@pytest.mark.parametrize(
    ("topic", "raw", "code"),
    [
        ("dev.alpha.retry", headers(), "CONTENT_TOPIC_UNSUPPORTED"),
        ("dev.alpha.reply", headers(), "CONTENT_TOPIC_UNSUPPORTED"),
        ("not-a-topic", headers(), "CONTENT_TOPIC_INVALID"),
        ("dev.beta.raw", headers("beta"), "CONTENT_CHANNEL_DISABLED"),
        ("dev.alpha.raw", headers("beta"), "CONTENT_CHANNEL_MISMATCH"),
        ("dev.alpha.raw", [("tb-channel", b"alpha")], "CONTENT_MESSAGE_ID_INVALID"),
        ("dev.alpha.raw", [("tb-msg-id", b"nope"), ("tb-channel", b"alpha")], "CONTENT_MESSAGE_ID_INVALID"),
        ("dev.alpha.out.sink", headers(**{"tb-destination": "other"}), "CONTENT_DESTINATION_MISMATCH"),
        ("dev.alpha.raw", [("tb-msg-id", b"\xff")], "CONTENT_HEADERS_INVALID"),
    ],
)
def test_records_that_do_not_claim_what_they_are_are_rejected_with_a_fixed_code(
    topic: str, raw: list[tuple[str, bytes]], code: str
) -> None:
    with pytest.raises(EventRejected) as raised:
        parse(topic, raw)
    assert raised.value.code == code
    assert "SYNTH" not in str(raised.value)


def test_claim_checked_record_stores_the_locator_not_bytes() -> None:
    found = row("dev.alpha.raw", headers(**{"tb-claim-check": "s3://synth-bucket/obj/key-1"}), b"x" * 10)
    assert found["claim_ref"] == "s3://synth-bucket/obj/key-1" and found["payload"] == b"" and found["size_bytes"] == 10


def test_inline_payload_over_one_mib_without_a_claim_check_is_rejected() -> None:
    row("dev.alpha.raw", headers(), b"x" * MAX_INLINE_BYTES)  # exactly 1 MiB is still inline
    with pytest.raises(EventRejected) as raised:
        parse("dev.alpha.raw", headers(), b"x" * (MAX_INLINE_BYTES + 1))
    assert raised.value.code == "CONTENT_OVERSIZE_WITHOUT_CLAIM_CHECK"


def test_user_headers_and_unknown_headers_are_never_stored() -> None:
    parsed = parse("dev.alpha.raw", headers(**{"tb-usr-note": "SYNTH-SECRET-NOTE", "x-other": "SYNTH-OTHER"}))
    assert "SYNTH-SECRET-NOTE" not in repr(parsed.values) and "SYNTH-OTHER" not in repr(parsed.values)


def test_malformed_optional_headers_are_blanked_and_counted_not_fatal() -> None:
    parsed = parse(
        "dev.alpha.raw", headers(**{"tb-attempt": "many", "tb-parent-id": "x", "tb-content-type": "text/hl7"})
    )
    values = dict(zip(COLUMNS, parsed.values, strict=True))
    assert (values["attempt"], values["parent_id"], values["content_type"]) == (0, None, "text/hl7")
    assert parsed.fields_blanked == 2


def test_empty_value_is_an_empty_payload_but_a_tombstone_is_rejected() -> None:
    assert row("dev.alpha.raw", headers(), b"")["size_bytes"] == 0
    with pytest.raises(EventRejected) as raised:
        parse("dev.alpha.raw", headers(), None)
    assert raised.value.code == "CONTENT_EMPTY"


@pytest.mark.parametrize("version", ["4294967295", "0", "7"])
def test_channel_version_that_fits_uint32_is_kept(version: str) -> None:
    assert row("dev.alpha.raw", headers(**{"tb-channel-version": version}))["channel_version"] == int(version)


@pytest.mark.parametrize("version", ["4294967296", "9999999999"])
def test_channel_version_over_uint32_is_blanked_and_counted_not_fatal(version: str) -> None:
    """Regression: 9_999_999_999 is a valid header but overflows UInt32; the driver raises and the batch halts."""
    parsed = parse("dev.alpha.raw", headers(**{"tb-channel-version": version}))
    assert dict(zip(COLUMNS, parsed.values, strict=True))["channel_version"] == 0 and parsed.fields_blanked == 1


@pytest.mark.parametrize("timestamp_ms", [None, 0, -5, 946_684_799_000, 4_102_444_800_000])
def test_missing_or_out_of_bounds_timestamps_are_rejected_without_a_now_fallback(timestamp_ms: int | None) -> None:
    with pytest.raises(EventRejected) as raised:
        parse_content_record(
            topic="dev.alpha.raw", raw_headers=headers(), value=SYNTH, timestamp_ms=timestamp_ms,
            partition=0, offset=0, enabled_channels=ENABLED,
        )  # fmt: skip
    assert raised.value.code == "CONTENT_TIMESTAMP_INVALID"


def test_ddl_has_the_replacing_key_ttl_and_no_user_header_column() -> None:
    sql = content_ddl(ttl_days=14)
    assert "ReplacingMergeTree(ts)" in sql and "INTERVAL 14 DAY" in sql and "ttl_only_drop_parts = 1" in sql
    assert "ORDER BY (channel, message_id, stage, destination)" in sql
    assert "usr" not in sql
    with pytest.raises(ValueError):
        content_ddl(ttl_days=0)


def test_subscription_pattern_covers_only_enabled_channels_and_data_topics() -> None:
    pattern = re.compile(topic_pattern("dev", frozenset({"alpha", "gamma"})))
    for good in ("dev.alpha.raw", "dev.gamma.dlq", "dev.alpha.out.sink"):
        assert pattern.match(good)
    for bad in (
        "dev.beta.raw",
        "dev.alpha.retry",
        "dev.alpha.reply",
        "dev.alpha.out",
        "prod.alpha.raw",
        "dev.alphax.raw",
    ):
        assert not pattern.match(bad)


class Resolver:
    def resolve_text(self, ref: str, key: str) -> str:
        return {"user": "u", "password": "SYNTH-PW"}[key]


ENVIRON = {"TB_ARCHIVE_CLICKHOUSE_HOST": "ch.internal", "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "ch-creds"}


def test_content_is_off_by_default_and_enabled_per_channel() -> None:
    config = load_config("dev", ENVIRON, Resolver())
    assert (config.content_enabled, config.content_channels, config.content_ttl_days) == (False, frozenset(), None)
    on = load_config(
        "dev", {**ENVIRON, "TB_ARCHIVE_CONTENT": "true", "TB_ARCHIVE_CONTENT_CHANNELS": "alpha, beta"}, Resolver()
    )
    assert (on.content_enabled, on.content_channels, on.content_ttl_days) == (True, frozenset({"alpha", "beta"}), 30)
    assert content_group("dev") == "tb.dev.archiver.content"


def test_bad_channel_names_are_refused() -> None:
    with pytest.raises(ArchiverConfigError):
        load_config("dev", {**ENVIRON, "TB_ARCHIVE_CONTENT_CHANNELS": "Alpha"}, Resolver())
