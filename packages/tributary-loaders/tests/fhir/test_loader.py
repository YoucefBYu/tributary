"""`FhirLoader` with an injected `send`: outcome classification, the Bundle checks, credentials in
headers only, and PHI-free logs. Real-server behaviour is `test_integration_fhir_loader.py`."""

from __future__ import annotations

import json
from typing import Any

import pytest
from tributary.loaders.fhir.config import FhirDestinationConfig
from tributary.loaders.fhir.loader import FhirLoader, check_transaction_bundle, classify_status
from tributary.loaders.http.client import Request, Response, TlsVerificationError, TransportError, TransportTimeout
from tributary.loaders.http.config import AuthSpec, CircuitBreakerSpec
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.fakes import RecordingLogger

CRED_MARKER = "synthetic-cred-marker-0001"
PHI_MARKER = "SYNTH-PATIENT-ZZZ"


def bundle(entries: list[dict[str, Any]] | None = None, *, type_: str = "transaction") -> dict[str, Any]:
    if entries is None:
        entries = [
            {
                "request": {"method": "PUT", "url": "Patient?identifier=urn:x|1"},
                "resource": {"resourceType": "Patient", "name": [{"family": PHI_MARKER}]},
            },
            {
                "request": {"method": "POST", "url": "Observation", "ifNoneExist": "identifier=urn:y|1"},
                "resource": {"resourceType": "Observation"},
            },
        ]
    return {"resourceType": "Bundle", "type": type_, "entry": entries}


def make_message(payload: object | bytes | None = None) -> Message:
    if payload is None:
        payload = bundle()
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return Message(
        headers={
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/fhir+json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "fhir-server",
        },
        payload=body,
    )


def response_bundle(statuses: list[str]) -> bytes:
    return json.dumps(
        {
            "resourceType": "Bundle",
            "type": "transaction-response",
            "entry": [{"response": {"status": s}} for s in statuses],
        }
    ).encode()


def make_config(**overrides: object) -> FhirDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "fhir-server",
        "url": "https://fhir.example.org/fhir",
        "fhir_version": "4.0.1",
        "timeout_s": 2.0,
        "headers": {},
        "auth": None,
        "circuit_breaker": CircuitBreakerSpec(3, 30.0),
        "idempotency_technique": "conditional-update",
        "idempotency_header": None,
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return FhirDestinationConfig(**base)  # type: ignore[arg-type]


class FakeSend:
    def __init__(self, *results: Response | Exception) -> None:
        self.results = list(results)
        self.requests: list[Request] = []

    def __call__(self, request: Request) -> Response:
        self.requests.append(request)
        result = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        if isinstance(result, Exception):
            raise result
        return result


def ok() -> Response:
    return Response(200, {}, response_bundle(["201 Created", "200 OK"]))


class FakeResolver:
    def resolve_text(self, ref: str, key: str) -> str:
        return CRED_MARKER

    def resolve_file(self, ref: str, key: str) -> Any:  # pragma: no cover
        raise FileNotFoundError


def test_success_posts_the_payload_byte_for_byte() -> None:
    send = FakeSend(ok())
    message = make_message()
    outcome = FhirLoader(make_config(), send=send).deliver(message)
    assert outcome.success is True
    request = send.requests[0]
    assert request.method == "POST"
    assert request.url == "https://fhir.example.org/fhir"
    assert request.body == message.payload
    assert request.headers["Content-Type"] == "application/fhir+json"
    assert request.timeout_s == 2.0


def test_auth_goes_in_a_header_only_and_never_in_repr_or_logs() -> None:
    log = RecordingLogger()
    config = make_config(auth=AuthSpec("bearer", "fhir-token"))
    send = FakeSend(Response(500))
    loader = FhirLoader(config, secret_resolver=FakeResolver(), send=send, log=log)
    loader.deliver(make_message())
    assert send.requests[0].headers["Authorization"] == f"Bearer {CRED_MARKER}"
    assert CRED_MARKER not in repr(loader)
    assert CRED_MARKER not in repr(log.records)
    assert CRED_MARKER not in send.requests[0].url


def test_idempotency_key_header_carries_the_msg_id() -> None:
    send = FakeSend(ok())
    config = make_config(idempotency_technique="idempotency-key-header", idempotency_header="Idempotency-Key")
    message = make_message()
    FhirLoader(config, send=send).deliver(message)
    assert send.requests[0].headers["Idempotency-Key"] == message.msg_id


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (302, "HTTP_REDIRECT_REFUSED", False),
        (400, "HTTP_400", False),
        (401, "HTTP_401", False),
        (403, "HTTP_403", False),
        (404, "HTTP_404", False),
        (408, "HTTP_408", True),
        (409, "HTTP_409", True),
        (412, "HTTP_412", False),
        (422, "HTTP_422", False),
        (429, "HTTP_429", True),
        (500, "HTTP_500", True),
        (503, "HTTP_503", True),
        (100, "HTTP_100", True),
    ],
)
def test_non_2xx_statuses_are_classified_explicitly(status: int, code: str, retryable: bool) -> None:
    outcome = FhirLoader(make_config(), send=FakeSend(Response(status))).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, code, retryable)


def test_classify_status_2xx_is_none() -> None:
    assert classify_status(201) is None


def test_a_2xx_with_a_failed_entry_is_a_failure() -> None:
    body = response_bundle(["201 Created", "422 Unprocessable Entity"])
    outcome = FhirLoader(make_config(), send=FakeSend(Response(200, {}, body))).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "FHIR_ENTRY_FAILED", False)


def test_a_2xx_with_a_transient_failed_entry_is_retryable() -> None:
    body = response_bundle(["201 Created", "503 Service Unavailable"])
    outcome = FhirLoader(make_config(), send=FakeSend(Response(200, {}, body))).deliver(make_message())
    assert (outcome.reason, outcome.retryable) == ("FHIR_ENTRY_FAILED", True)


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"<html>login</html>",
        b"[]",
        json.dumps({"resourceType": "Bundle", "type": "batch-response", "entry": []}).encode(),
        response_bundle(["201 Created"]),  # one entry for a two-entry request
        response_bundle(["201 Created", "200 OK", "200 OK"]),
        response_bundle(["201 Created", "garbage"]),
        json.dumps({"resourceType": "OperationOutcome"}).encode(),
    ],
)
def test_a_2xx_that_is_not_a_matching_transaction_response_is_a_retryable_failure(body: bytes) -> None:
    outcome = FhirLoader(make_config(), send=FakeSend(Response(200, {}, body))).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "FHIR_RESPONSE_INVALID", True)


def test_a_truncated_2xx_body_is_never_success() -> None:
    truncated = Response(200, {}, response_bundle(["201 Created", "200 OK"]), truncated=True)
    outcome = FhirLoader(make_config(), send=FakeSend(truncated)).deliver(make_message())
    assert outcome.reason == "FHIR_RESPONSE_INVALID"


def test_the_response_read_is_capped_in_the_request() -> None:
    send = FakeSend(ok())
    FhirLoader(make_config(), send=send).deliver(make_message())
    assert 0 < send.requests[0].max_response_bytes <= 16 * 1024 * 1024


@pytest.mark.parametrize(
    ("exc", "code", "retryable"),
    [
        (TransportTimeout("t"), "TIMEOUT", True),
        (TransportError("x"), "CONNECT_FAILED", True),
        (TlsVerificationError("bad"), "TLS_VERIFY_FAILED", False),
    ],
)
def test_transport_failures(exc: Exception, code: str, retryable: bool) -> None:
    outcome = FhirLoader(make_config(), send=FakeSend(exc)).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, code, retryable)


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"[]",
        {"resourceType": "Patient"},
        bundle(type_="batch"),
        bundle(type_="collection"),
        {"resourceType": "Bundle", "type": "transaction"},
        {"resourceType": "Bundle", "type": "transaction", "entry": []},
        bundle([{"resource": {"resourceType": "Patient"}}]),
    ],
)
def test_a_non_transaction_bundle_is_refused_before_anything_is_sent(payload: object) -> None:
    send = FakeSend(ok())
    outcome = FhirLoader(make_config(), send=send).deliver(make_message(payload))
    assert (outcome.reason, outcome.retryable) == ("FHIR_BUNDLE_INVALID", False)
    assert send.requests == []


def test_absent_payload_is_refused() -> None:
    message = make_message()
    message = Message(headers=message.headers, payload=None)
    outcome = FhirLoader(make_config(), send=FakeSend(ok())).deliver(message)
    assert outcome.reason == "FHIR_BUNDLE_INVALID"


@pytest.mark.parametrize(
    "entry",
    [
        {"request": {"method": "POST", "url": "Patient"}},
        {"request": {"method": "POST", "url": "Patient", "ifNoneExist": ""}},
        {"request": {"method": "PATCH", "url": "Patient/1"}},
    ],
)
def test_conditional_update_refuses_an_entry_that_is_not_idempotent(entry: dict[str, Any]) -> None:
    send = FakeSend(ok())
    outcome = FhirLoader(make_config(), send=send).deliver(make_message(bundle([entry])))
    assert outcome.reason == "FHIR_BUNDLE_INVALID"
    assert send.requests == []


def test_a_declared_none_does_not_run_the_entry_idempotency_check() -> None:
    entries = [{"request": {"method": "POST", "url": "Patient"}, "resource": {"resourceType": "Patient"}}]
    body = response_bundle(["201 Created"])
    outcome = FhirLoader(make_config(idempotency_technique="none"), send=FakeSend(Response(200, {}, body))).deliver(
        make_message(bundle(entries))
    )
    assert outcome.success is True


def test_check_transaction_bundle_returns_the_entry_count() -> None:
    assert check_transaction_bundle(json.dumps(bundle()).encode(), require_idempotent_entries=True) == 2


def test_circuit_opens_after_consecutive_retryable_failures_and_a_412_does_not_count() -> None:
    send = FakeSend(Response(412), Response(412), Response(412), Response(500))
    loader = FhirLoader(make_config(), send=send)
    for _ in range(3):
        assert loader.deliver(make_message()).reason == "HTTP_412"
    assert loader.breaker.allow()  # message-level failures are not an outage
    send2 = FakeSend(Response(503))
    loader2 = FhirLoader(make_config(), send=send2)
    for _ in range(3):
        loader2.deliver(make_message())
    outcome = loader2.deliver(make_message())
    assert (outcome.reason, outcome.retryable) == ("circuit-open", True)
    assert len(send2.requests) == 3  # the fourth was never sent
    assert loader2.metrics.circuit_opens == 1


def test_logs_carry_no_payload_or_response_text() -> None:
    log = RecordingLogger()
    hostile = json.dumps(
        {
            "resourceType": "OperationOutcome",
            "issue": [{"code": "conflict", "diagnostics": f"duplicate for {PHI_MARKER}"}],
        }
    ).encode()
    loader = FhirLoader(make_config(), send=FakeSend(Response(409, {}, hostile)), log=log)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "HTTP_409"
    dumped = repr(log.records)
    assert PHI_MARKER not in dumped
    assert "conflict" in dumped  # the allow-listed IssueType code is fine to log
    assert PHI_MARKER not in repr(outcome)


def test_an_issue_code_outside_the_fhir_vocabulary_is_not_logged() -> None:
    log = RecordingLogger()
    body = json.dumps({"resourceType": "OperationOutcome", "issue": [{"code": PHI_MARKER}]}).encode()
    FhirLoader(make_config(), send=FakeSend(Response(400, {}, body)), log=log).deliver(make_message())
    assert PHI_MARKER not in repr(log.records)


# -- duplicate observation, bounded retry, secret hygiene (code and security review) -------------------


def test_a_2xx_where_no_entry_was_created_is_counted_as_a_duplicate_and_is_a_success() -> None:
    send = FakeSend(Response(200, {}, response_bundle(["200 OK", "200 OK"])))
    loader = FhirLoader(make_config(), send=send)
    outcome = loader.deliver(make_message())
    assert outcome.success is True and outcome.reason == "duplicate"
    assert loader.metrics.snapshot()["delivery_duplicate"] == 1


def test_a_2xx_where_an_entry_was_created_is_a_plain_success() -> None:
    loader = FhirLoader(make_config(), send=FakeSend(ok()))
    assert loader.deliver(make_message()).reason == "success"
    assert "delivery_duplicate" not in loader.metrics.snapshot()


def _cfg_no_conditional(technique: str) -> FhirDestinationConfig:
    header = "Idempotency-Key" if technique == "idempotency-key-header" else None
    return make_config(
        idempotency_technique=technique, idempotency_header=header, circuit_breaker=CircuitBreakerSpec(100, 30.0)
    )


@pytest.mark.parametrize("technique", ["none", "idempotency-key-header"])
def test_retry_stops_after_the_cap_when_entries_are_not_proven_idempotent(technique: str) -> None:
    from tributary.loaders.fhir.loader import MAX_UNPROTECTED_ATTEMPTS

    plain_post = bundle([{"request": {"method": "POST", "url": "Patient"}, "resource": {"resourceType": "Patient"}}])
    send = FakeSend(TransportTimeout("timed out"))
    loader = FhirLoader(_cfg_no_conditional(technique), send=send)
    message = make_message(plain_post)
    retryable = [loader.deliver(message).retryable for _ in range(MAX_UNPROTECTED_ATTEMPTS + 2)]
    assert retryable == [True] * (MAX_UNPROTECTED_ATTEMPTS - 1) + [False] * 3
    assert loader.deliver(message).reason == "TIMEOUT"  # the code is unchanged; only retryability is


def test_the_cap_does_not_apply_to_conditional_update_entries() -> None:
    send = FakeSend(TransportTimeout("timed out"))
    loader = FhirLoader(make_config(circuit_breaker=CircuitBreakerSpec(100, 30.0)), send=send)
    message = make_message()
    assert all(loader.deliver(message).retryable for _ in range(10))


def test_the_cap_counts_per_message_and_resets_on_success() -> None:
    plain_post = bundle([{"request": {"method": "POST", "url": "Patient"}, "resource": {"resourceType": "Patient"}}])
    created = Response(200, {}, response_bundle(["201 Created"]))
    loader = FhirLoader(
        _cfg_no_conditional("none"), send=FakeSend(TransportTimeout("t"), TransportTimeout("t"), created)
    )
    first, second = make_message(plain_post), make_message(plain_post)
    assert loader.deliver(first).retryable and loader.deliver(first).retryable
    assert loader.deliver(first).success  # a success resets the count
    loader2 = FhirLoader(_cfg_no_conditional("none"), send=FakeSend(TransportTimeout("t")))
    for _ in range(2):
        loader2.deliver(first)
    assert loader2.deliver(second).retryable is True  # a different message starts at zero


@pytest.mark.parametrize("auth_type", ["bearer", "basic"])
def test_a_secret_with_a_control_character_is_refused_at_startup_without_echoing_it(auth_type: str) -> None:
    from tributary.loaders.secrets import SecretFormatError

    class NewlineResolver(FakeResolver):
        def resolve_text(self, ref: str, key: str) -> str:
            return "user:pass-" + CRED_MARKER + "\nX-Injected: 1"

    with pytest.raises(SecretFormatError) as exc_info:
        FhirLoader(make_config(auth=AuthSpec(auth_type, "ref")), secret_resolver=NewlineResolver(), send=FakeSend(ok()))
    assert CRED_MARKER not in str(exc_info.value)
