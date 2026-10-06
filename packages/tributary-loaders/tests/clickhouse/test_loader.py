"""`ClickhouseLoader.deliver`: the row-shape check, classification, the circuit breaker, and the
startup idempotency-constraint proof. `FakeTransport` stands in for `ClickhouseTransport`;
`tests/clickhouse/test_integration_clickhouse_loader.py` proves the same behaviour against a real
server."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from tributary.loaders.clickhouse.client import Response, TransportError
from tributary.loaders.clickhouse.config import ChAuthSpec, ChTlsSpec, CircuitBreakerSpec, ClickhouseDestinationConfig
from tributary.loaders.clickhouse.loader import ClickhouseLoader, parse_row
from tributary.loaders.errors import IdempotencyConstraintError
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_message(
    row: dict[str, Any] | None = None, *, payload: bytes | None = None, msg_id: str | None = None
) -> Message:
    if payload is None:
        payload = json.dumps(row if row is not None else {"patient_ref": "SYNTH-1", "value": 98.6}).encode("utf-8")
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "adt-analytics",
        },
        payload=payload,
    )


def make_config(**overrides: object) -> ClickhouseDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "adt-analytics",
        "host": "clickhouse.example.org",
        "port": 8443,
        "database": "analytics",
        "table": "adt_events",
        "id_column": "tb_msg_id",
        "auth": ChAuthSpec("password", "ch-creds"),
        "tls": ChTlsSpec("verify-full", None),
        "timeout_s": 2.0,
        "circuit_breaker": None,
        "idempotency_technique": "sink-native",
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return ClickhouseDestinationConfig(**base)  # type: ignore[arg-type]


class FakeTransport:
    """`respond` decides the `Response` (or raises) for every `send()`; every call is recorded."""

    def __init__(self, respond: Callable[[bytes], Response]) -> None:
        self._respond = respond
        self.calls: list[bytes] = []

    def send(self, sql: bytes, *, timeout_s: float) -> Response:
        self.calls.append(sql)
        return self._respond(sql)


def ok(body: bytes = b"") -> Response:
    return Response(200, {}, body)


def ch_error(code: int, name: str = "SOME_ERROR") -> Response:
    return Response(
        404, {"X-ClickHouse-Exception-Code": str(code)}, f"Code: {code}. DB::Exception: boom ({name})".encode()
    )


# -- row shape (README 3.4.3) ---------------------------------------------------------------------


def test_parse_row_accepts_scalars_and_null() -> None:
    row = parse_row(json.dumps({"a": "x", "b": 1, "c": 1.5, "d": True, "e": None}).encode(), "tb_msg_id")
    assert row == {"a": "x", "b": 1, "c": 1.5, "d": True, "e": None}


def test_parse_row_rejects_non_object_payload() -> None:
    with pytest.raises(Exception, match="not a JSON object"):
        parse_row(json.dumps([1, 2, 3]).encode(), "tb_msg_id")


def test_parse_row_rejects_invalid_json() -> None:
    with pytest.raises(Exception, match="not valid JSON"):
        parse_row(b"not json{", "tb_msg_id")


def test_parse_row_rejects_a_payload_that_sets_id_column_itself() -> None:
    with pytest.raises(Exception, match="tb_msg_id"):
        parse_row(json.dumps({"tb_msg_id": "attacker-supplied"}).encode(), "tb_msg_id")


def test_parse_row_rejects_a_bad_column_name() -> None:
    with pytest.raises(Exception, match="column name"):
        parse_row(json.dumps({"Bad-Column": "x"}).encode(), "tb_msg_id")


def test_parse_row_never_logs_a_sender_supplied_key_verbatim() -> None:
    """Code review, Medium: a payload's JSON keys are sender-controlled and can themselves carry PHI
    (unlike `id_column`, this loader's own config) -- the error message must say only a fixed phrase
    and the key's length, never the key text itself."""
    secret_looking_key = "PATIENT-JOHN-DOE-1980-01-01"  # noqa: S105 - a synthetic PHI-shaped JSON key, not a credential
    with pytest.raises(Exception) as exc_info:
        parse_row(json.dumps({secret_looking_key: "x"}).encode(), "tb_msg_id")
    assert secret_looking_key not in str(exc_info.value)
    assert str(len(secret_looking_key)) in str(exc_info.value)


def test_parse_row_rejects_a_trailing_newline_column_name() -> None:
    """Regression for the `^...$`/`.match()` gotcha (code review, Low): Python's `$` matches just
    before a trailing newline even without `re.MULTILINE`, so `"tb_msg_id\\n"` used to pass. `fullmatch`
    with no anchors closes it."""
    with pytest.raises(Exception, match="column name"):
        parse_row(json.dumps({"value\n": 1}).encode(), "tb_msg_id")


def test_deliver_with_malformed_payload_is_payload_shape_invalid_and_not_retryable() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ok()))
    outcome = loader.deliver(make_message(payload=b"not json"))
    assert outcome.success is False
    assert outcome.reason == "PAYLOAD_SHAPE_INVALID"
    assert outcome.retryable is False


# -- delivery and classification -------------------------------------------------------------------


def test_insert_success_returns_success() -> None:
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(), transport)
    outcome = loader.deliver(make_message({"value": 1}))
    assert outcome.success is True
    assert outcome.inserted is True


def test_insert_statement_uses_json_each_row_and_sorted_columns() -> None:
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(table="analytics.adt_events"), transport)
    message = make_message({"value": 98.6, "patient_ref": "SYNTH-1"})
    loader.deliver(message)
    sent = transport.calls[0].decode("utf-8")
    assert sent.startswith(
        "INSERT INTO `analytics`.`adt_events` (`tb_msg_id`, `patient_ref`, `value`) FORMAT JSONEachRow\n"
    )
    row_line = sent.splitlines()[1]
    assert json.loads(row_line) == {"tb_msg_id": message.msg_id, "patient_ref": "SYNTH-1", "value": 98.6}


def test_timeout_is_failure_and_retryable() -> None:
    def raiser(sql: bytes) -> Response:
        raise TimeoutError("query timed out")

    loader = ClickhouseLoader(make_config(), FakeTransport(raiser))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True


def test_transport_error_is_connect_failed_and_retryable() -> None:
    def raiser(sql: bytes) -> Response:
        raise TransportError("connection refused")

    loader = ClickhouseLoader(make_config(), FakeTransport(raiser))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "CONNECT_FAILED"
    assert outcome.retryable is True


def test_tls_verification_failure_is_tls_verify_failed_and_not_retryable() -> None:
    """`TlsVerificationError`, not a bare `ssl.SSLError` (code review, Low): `client.py`'s real `send`
    never lets `ssl.SSLError` escape -- it always wraps TLS handshake failures into
    `TlsVerificationError` -- so a fake that raised `ssl.SSLError` directly here would pass even if
    `loader.py`'s `except` clause named the wrong exception type and this classification were
    unreachable against the real transport."""
    from tributary.loaders.clickhouse.client import TlsVerificationError

    def raiser(sql: bytes) -> Response:
        raise TlsVerificationError("certificate verify failed")

    loader = ClickhouseLoader(make_config(), FakeTransport(raiser))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "TLS_VERIFY_FAILED"
    assert outcome.retryable is False


def test_timeout_exceeded_exception_code_is_retryable() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ch_error(159, "TIMEOUT_EXCEEDED")))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "CH_159"
    assert outcome.retryable is True


def test_unknown_table_exception_code_is_not_retryable() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ch_error(60, "UNKNOWN_TABLE")))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "CH_60"
    assert outcome.retryable is False


def test_a_200_with_an_embedded_code_prefix_is_still_a_failure() -> None:
    """Code review, Critical: ClickHouse can send the `200` status line and headers before the query
    has actually finished, so a row that was never inserted must not be reported as a success just
    because the HTTP status was 2xx. `client.py`'s `wait_end_of_query=1` asks the server not to do
    this; this test proves `loader.py` does not rely on that alone."""
    response = Response(200, {}, b"Code: 241. DB::Exception: Memory limit exceeded (MEMORY_LIMIT_EXCEEDED)")
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: response))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "CH_241"
    assert outcome.retryable is False


def test_a_200_with_an_embedded_retryable_code_prefix_is_retryable() -> None:
    response = Response(200, {}, b"Code: 159. DB::Exception: Timeout exceeded (TIMEOUT_EXCEEDED)")
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: response))
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "CH_159"
    assert outcome.retryable is True


def test_a_plain_200_with_no_code_prefix_is_success() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ok(b"")))
    outcome = loader.deliver(make_message())
    assert outcome.success is True


def test_verify_idempotency_constraint_refuses_a_200_with_an_embedded_code_prefix() -> None:
    """Same Critical fix, the startup half: `verify_idempotency_constraint` must not treat a `200`
    carrying an embedded ClickHouse exception as a readable `system.tables` row."""
    response = Response(200, {}, b"Code: 60. DB::Exception: Table default.t does not exist (UNKNOWN_TABLE)")
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: response))
    with pytest.raises(IdempotencyConstraintError):
        loader.verify_idempotency_constraint()


def test_error_with_no_exception_code_header_falls_back_to_body_prefix() -> None:
    response = Response(500, {}, b"Code: 241. DB::Exception: Memory limit exceeded (MEMORY_LIMIT_EXCEEDED)")
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: response))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "CH_241"
    assert outcome.retryable is False  # 241 is not in the retryable set


def test_error_with_no_code_at_all_is_ch_unknown_and_not_retryable() -> None:
    response = Response(502, {}, b"<html>Bad Gateway</html>")
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: response))
    outcome = loader.deliver(make_message())
    assert outcome.reason == "CH_UNKNOWN"
    assert outcome.retryable is False


def test_circuit_breaker_opens_after_repeated_failures_and_skips_the_transport() -> None:
    def raiser(sql: bytes) -> Response:
        raise TransportError("down")

    transport = FakeTransport(raiser)
    loader = ClickhouseLoader(make_config(circuit_breaker=CircuitBreakerSpec(2, 60.0)), transport)
    loader.deliver(make_message())
    loader.deliver(make_message())
    outcome = loader.deliver(make_message())
    assert outcome.reason == "circuit-open"
    assert len(transport.calls) == 2


# -- idempotency -------------------------------------------------------------------------------------


def test_duplicate_delivery_sends_the_same_row_both_times() -> None:
    """Unlike `postgresql`'s `ON CONFLICT`, this loader cannot tell a duplicate insert from a fresh one
    (ClickHouse's own deduplication is eventual, at merge time -- README 3.4.3). What THIS loader must
    prove is that a redelivery sends byte-identical `tb_msg_id` content both times, which is what makes
    `ReplacingMergeTree` able to collapse them later; the FINAL-count proof itself needs a real server
    (`test_integration_clickhouse_loader.py`)."""
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(), transport)
    message = make_message({"value": 1})

    first = loader.deliver(message)
    second = loader.deliver(message)  # simulates a redelivery of the identical record

    assert first.success is True
    assert second.success is True
    assert transport.calls[0] == transport.calls[1]


def test_verify_idempotency_constraint_passes_for_a_replacing_merge_tree_sorted_by_id_column() -> None:
    transport = FakeTransport(lambda sql: ok(b"ReplacingMergeTree\ttb_msg_id\n"))
    loader = ClickhouseLoader(make_config(), transport)
    loader.verify_idempotency_constraint()  # must not raise


def test_verify_idempotency_constraint_accepts_a_replicated_variant() -> None:
    transport = FakeTransport(lambda sql: ok(b"ReplicatedReplacingMergeTree\ttb_msg_id, other\n"))
    loader = ClickhouseLoader(make_config(), transport)
    loader.verify_idempotency_constraint()  # must not raise


def test_verify_idempotency_constraint_refuses_a_plain_merge_tree() -> None:
    transport = FakeTransport(lambda sql: ok(b"MergeTree\ttb_msg_id\n"))
    loader = ClickhouseLoader(make_config(), transport)
    with pytest.raises(IdempotencyConstraintError, match="ReplacingMergeTree"):
        loader.verify_idempotency_constraint()


def test_verify_idempotency_constraint_refuses_a_replacing_merge_tree_sorted_by_something_else() -> None:
    transport = FakeTransport(lambda sql: ok(b"ReplacingMergeTree\tother_column\n"))
    loader = ClickhouseLoader(make_config(), transport)
    with pytest.raises(IdempotencyConstraintError, match="sorted"):
        loader.verify_idempotency_constraint()


def test_verify_idempotency_constraint_refuses_a_missing_table() -> None:
    transport = FakeTransport(lambda sql: ok(b""))
    loader = ClickhouseLoader(make_config(), transport)
    with pytest.raises(IdempotencyConstraintError, match="does not exist"):
        loader.verify_idempotency_constraint()


def test_verify_idempotency_constraint_splits_schema_qualified_tables() -> None:
    seen: dict[str, bytes] = {}

    def respond(sql: bytes) -> Response:
        seen["sql"] = sql
        return ok(b"ReplacingMergeTree\ttb_msg_id\n")

    transport = FakeTransport(respond)
    loader = ClickhouseLoader(make_config(table="clinical.adt_events"), transport)
    loader.verify_idempotency_constraint()
    assert b"'clinical'" in seen["sql"]
    assert b"'adt_events'" in seen["sql"]


# -- metrics -------------------------------------------------------------------------------------


def test_metrics_snapshot_counts_by_reason() -> None:
    responses = iter([ok(), ch_error(60, "UNKNOWN_TABLE")])
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: next(responses)))
    loader.deliver(make_message({"value": 1}))
    outcome = loader.deliver(make_message({"value": 2}))
    assert outcome.reason == "CH_60"
    snapshot = loader.metrics.snapshot()
    assert snapshot["delivery_success"] == 1
    assert snapshot["delivery_CH_60"] == 1
