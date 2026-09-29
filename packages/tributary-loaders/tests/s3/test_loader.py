"""`S3Loader` against a fake client: the deterministic key, conditional put, duplicate handling, the
NotImplemented fallback, error classification, the bounded call and PHI-free logs. A real store is
`test_integration_s3_loader.py`."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from botocore import exceptions as bex
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.loaders.s3.config import S3DestinationConfig
from tributary.loaders.s3.loader import S3Loader, object_key, s3_error_code
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.fakes import RecordingLogger

PHI_MARKER = "SYNTH-PATIENT-ZZZ"


def make_message(
    payload: bytes | None = b"MSH|^~\\&|TRIBUTARY-SYNTH\r", *, received_at: str = "2026-03-09T23:59:59.500Z"
) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: received_at,
            h.TB_CONTENT_TYPE: "application/hl7-v2",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "s3-out",
        },
        payload=payload,
    )


def make_config(**overrides: object) -> S3DestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "s3-out",
        "bucket": "bkt",
        "region": "eu-west-1",
        "prefix": "adt",
        "endpoint": None,
        "sse": "AES256",
        "kms_key_id": None,
        "auth_type": "ambient",
        "auth_secret_ref": None,
        "file_extension": ".hl7",
        "timeout_s": 0.3,
        "circuit_breaker": CircuitBreakerSpec(3, 30.0),
        "idempotency_technique": "deterministic-name",
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return S3DestinationConfig(**base)  # type: ignore[arg-type]


def client_error(code: str, status: int, op: str = "PutObject") -> bex.ClientError:
    return bex.ClientError(
        {"Error": {"Code": code, "Message": f"leaks {PHI_MARKER}"}, "ResponseMetadata": {"HTTPStatusCode": status}}, op
    )


class FakeClient:
    """A bucket in a dict, with S3's conditional-put behaviour. `raises` maps an op to an exception."""

    def __init__(self, *, conditional: bool = True) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.raises: dict[str, list[BaseException]] = {}
        self.conditional = conditional

    def _maybe_raise(self, op: str) -> None:
        queue = self.raises.get(op)
        if queue:
            raise queue.pop(0)

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("put", kwargs))
        self._maybe_raise("put")
        if "IfNoneMatch" in kwargs:
            if not self.conditional:
                raise client_error("NotImplemented", 501)
            if kwargs["Key"] in self.objects:
                raise client_error("PreconditionFailed", 412)
        self.objects[kwargs["Key"]] = bytes(kwargs["Body"])
        return {}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("head", kwargs))
        self._maybe_raise("head")
        if kwargs["Key"] not in self.objects:
            raise client_error("404", 404, "HeadObject")
        return {"ContentLength": len(self.objects[kwargs["Key"]])}


def loader_for(client: FakeClient, **cfg: object) -> S3Loader:
    return S3Loader(make_config(**cfg), client)


def test_key_is_prefix_utc_date_msg_id_and_extension() -> None:
    message = make_message(received_at="2026-03-09T23:59:59.500Z")
    assert object_key(make_config(), message) == f"adt/2026/03/09/{message.msg_id}.hl7"


def test_the_date_is_the_utc_date_at_a_day_boundary() -> None:
    """The SDK only admits UTC timestamps, so the key date is the UTC date, never a local one."""
    message = make_message(received_at="2026-03-10T00:00:00.000Z")
    assert object_key(make_config(), message).startswith("adt/2026/03/10/")
    message = make_message(received_at="2026-03-09T23:59:59.999+00:00")
    assert object_key(make_config(), message).startswith("adt/2026/03/09/")


def test_no_prefix_means_the_date_leads_the_key() -> None:
    message = make_message()
    assert object_key(make_config(prefix=""), message) == f"2026/03/09/{message.msg_id}.hl7"


def test_the_same_message_always_maps_to_the_same_key() -> None:
    message = make_message()
    assert object_key(make_config(), message) == object_key(make_config(), message)


def test_delivery_is_a_conditional_encrypted_put_with_a_non_phi_key() -> None:
    client = FakeClient()
    message = make_message()
    outcome = loader_for(client, sse="aws:kms", kms_key_id="alias/x").deliver(message)
    assert (outcome.success, outcome.reason) == (True, "success")
    ((_, args),) = client.calls
    assert args["IfNoneMatch"] == "*"
    assert args["Bucket"] == "bkt"
    assert args["Key"] == f"adt/2026/03/09/{message.msg_id}.hl7"
    assert args["Body"] == message.payload and args["ContentLength"] == len(message.payload or b"")
    assert args["ServerSideEncryption"] == "aws:kms" and args["SSEKMSKeyId"] == "alias/x"
    assert args["Metadata"] == {"tb-msg-id": message.msg_id}


def test_a_redelivery_is_a_duplicate_success_that_never_overwrites() -> None:
    client = FakeClient()
    loader = loader_for(client)
    message = make_message()
    assert loader.deliver(message).reason == "success"
    original = dict(client.objects)
    second = loader.deliver(message)
    assert (second.success, second.reason) == (True, "duplicate")
    assert client.objects == original and len(client.objects) == 1


def test_a_key_holding_something_of_another_size_is_a_non_retryable_conflict() -> None:
    client = FakeClient()
    message = make_message()
    client.objects[object_key(make_config(), message)] = b"not this message at all, different length"
    outcome = loader_for(client).deliver(message)
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "S3_KEY_CONFLICT", False)


def test_an_object_deleted_between_the_412_and_the_head_is_retryable() -> None:
    client = FakeClient()
    message = make_message()
    client.raises["put"] = [client_error("PreconditionFailed", 412)]
    outcome = loader_for(client).deliver(message)
    assert (outcome.reason, outcome.retryable) == ("S3_KEY_VANISHED", True)


def test_a_store_without_conditional_put_falls_back_to_an_idempotent_overwrite_once() -> None:
    client = FakeClient(conditional=False)
    loader = loader_for(client)
    message = make_message()
    first = loader.deliver(message)
    assert (first.success, first.reason) == (True, "success")
    assert loader.conditional_put_supported is False
    second = loader.deliver(message)  # HEAD finds the same size: a duplicate, nothing rewritten
    assert (second.success, second.reason) == (True, "duplicate")
    assert len(client.objects) == 1
    assert next(iter(client.objects.values())) == message.payload
    conditional_attempts = [a for op, a in client.calls if op == "put" and "IfNoneMatch" in a]
    assert len(conditional_attempts) == 1  # asked once, then remembered


@pytest.mark.parametrize(
    ("code", "status", "reason", "retryable"),
    [
        ("AccessDenied", 403, "S3_ACCESS_DENIED", False),
        ("NoSuchBucket", 404, "S3_NO_SUCH_BUCKET", False),
        ("InvalidAccessKeyId", 403, "S3_INVALID_ACCESS_KEY_ID", False),
        ("SlowDown", 503, "S3_SLOW_DOWN", True),
        ("InternalError", 500, "S3_INTERNAL_ERROR", True),
        ("ServiceUnavailable", 503, "S3_SERVICE_UNAVAILABLE", True),
        ("RequestTimeout", 400, "S3_REQUEST_TIMEOUT", True),
        ("ConditionalRequestConflict", 409, "S3_CONDITIONAL_REQUEST_CONFLICT", True),
        ("EntityTooLarge", 400, "S3_ENTITY_TOO_LARGE", False),
        ("OperationAborted", 409, "S3_OPERATION_ABORTED", True),
        ("Whatever", 502, "S3_UNKNOWN", True),
    ],
)
def test_store_errors_map_to_codes_and_retryability(code: str, status: int, reason: str, retryable: bool) -> None:
    client = FakeClient()
    client.raises["put"] = [client_error(code, status)]
    outcome = loader_for(client).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, reason, retryable)


@pytest.mark.parametrize("raw", [None, 5, "", "has space", "x" * 60, "a;b", "Access Denied", "</html>"])
def test_an_error_code_that_is_not_a_plain_token_is_never_passed_through(raw: object) -> None:
    assert s3_error_code(raw) == "S3_UNKNOWN"


@pytest.mark.parametrize(
    ("exc", "reason", "retryable"),
    [
        (bex.ConnectTimeoutError(endpoint_url="http://x"), "TIMEOUT", True),
        (bex.ReadTimeoutError(endpoint_url="http://x"), "TIMEOUT", True),
        (bex.EndpointConnectionError(endpoint_url="http://x"), "CONNECT_FAILED", True),
        (bex.ConnectionClosedError(endpoint_url="http://x"), "CONNECT_FAILED", True),
        (bex.NoCredentialsError(), "S3_NO_CREDENTIALS", False),
        (bex.SSLError(endpoint_url="http://x", error="reset"), "CONNECT_FAILED", True),
        (RuntimeError("boom"), "TRANSPORT_ERROR", True),
    ],
)
def test_transport_failures_are_classified(exc: BaseException, reason: str, retryable: bool) -> None:
    client = FakeClient()
    client.raises["put"] = [exc]
    outcome = loader_for(client).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, reason, retryable)


def test_a_certificate_verification_failure_is_not_retryable() -> None:
    import ssl

    error = bex.SSLError(endpoint_url="https://x", error=ssl.SSLCertVerificationError("verify failed"))
    error.__cause__ = ssl.SSLCertVerificationError("verify failed")
    client = FakeClient()
    client.raises["put"] = [error]
    outcome = loader_for(client).deliver(make_message())
    assert (outcome.reason, outcome.retryable) == ("TLS_VERIFY_FAILED", False)


def test_a_hung_call_is_abandoned_at_the_deadline_and_reported_timeout() -> None:
    release = threading.Event()

    class Hung(FakeClient):
        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            release.wait(30)
            return {}

    loader = S3Loader(make_config(timeout_s=0.2), Hung())
    started = time.monotonic()
    try:
        outcome = loader.deliver(make_message())
        elapsed = time.monotonic() - started
        assert (outcome.reason, outcome.retryable) == ("TIMEOUT", True)
        assert elapsed < 3.0
    finally:
        release.set()
        loader.close()


def test_when_every_worker_is_wedged_further_deliveries_fail_fast_without_queueing() -> None:
    release = threading.Event()

    class Hung(FakeClient):
        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            release.wait(30)
            return {}

    loader = S3Loader(make_config(timeout_s=0.1, circuit_breaker=CircuitBreakerSpec(1000, 30.0)), Hung())
    try:
        for _ in range(2):  # both workers now wedged
            assert loader.deliver(make_message()).reason == "TIMEOUT"
        started = time.monotonic()
        outcome = loader.deliver(make_message())
        assert outcome.reason == "TIMEOUT"
        assert time.monotonic() - started < 0.5  # no wait: no free slot
    finally:
        release.set()
        loader.close()


def test_an_absent_payload_is_refused() -> None:
    client = FakeClient()
    outcome = loader_for(client).deliver(make_message(payload=None))
    assert (outcome.reason, outcome.retryable) == ("PAYLOAD_SHAPE_INVALID", False)
    assert client.calls == []


def test_circuit_opens_after_consecutive_retryable_failures_and_403_does_not_count() -> None:
    denied = FakeClient()
    denied.raises["put"] = [client_error("AccessDenied", 403)] * 5
    loader = loader_for(denied)
    for _ in range(5):
        assert loader.deliver(make_message()).reason == "S3_ACCESS_DENIED"
    assert loader.breaker.allow()

    down = FakeClient()
    down.raises["put"] = [client_error("ServiceUnavailable", 503)] * 3
    loader2 = loader_for(down)
    for _ in range(3):
        loader2.deliver(make_message())
    outcome = loader2.deliver(make_message())
    assert (outcome.reason, outcome.retryable) == ("circuit-open", True)
    assert loader2.metrics.circuit_opens == 1


def test_logs_and_outcomes_carry_no_payload_key_or_store_message() -> None:
    log = RecordingLogger()
    client = FakeClient()
    client.raises["put"] = [client_error("AccessDenied", 403)]
    payload = f"PID|1||{PHI_MARKER}".encode()
    outcome = S3Loader(make_config(), client, log=log).deliver(make_message(payload))
    dumped = repr(log.records) + repr(outcome)
    assert PHI_MARKER not in dumped
    assert "adt/2026" not in dumped and ".hl7" not in dumped and "bkt" not in dumped


# -- the NotImplemented fallback, ExpectedBucketOwner, bounded error codes (code and security review) -----


def test_the_fallback_never_overwrites_an_object_of_another_size() -> None:
    client = FakeClient(conditional=False)
    message = make_message()
    foreign = b"someone else's object, different length"
    client.objects[object_key(make_config(), message)] = foreign
    outcome = loader_for(client).deliver(message)
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "S3_KEY_CONFLICT", False)
    assert client.objects[object_key(make_config(), message)] == foreign
    assert [op for op, args in client.calls if op == "put" and "IfNoneMatch" not in args] == []


def test_concurrent_redelivery_on_a_store_without_conditional_put_converges_on_one_object() -> None:
    """Two loaders both see 'absent' at HEAD and both write (the documented HEAD-then-PUT window): the
    same bytes to the same key, so the bucket holds one object and both deliveries succeed."""
    client = FakeClient(conditional=False)
    barrier = threading.Barrier(2, timeout=5)
    real_head = client.head_object

    def head_then_wait(**kwargs: Any) -> dict[str, Any]:
        try:
            return real_head(**kwargs)
        finally:
            barrier.wait()  # both HEADs return before either PUT starts

    client.head_object = head_then_wait  # type: ignore[method-assign]
    message = make_message()
    outcomes: list[Any] = []
    threads = [
        threading.Thread(target=lambda: outcomes.append(loader_for(client, timeout_s=5.0).deliver(message)))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert [o.success for o in outcomes] == [True, True]
    assert list(client.objects.values()) == [message.payload]


def test_the_documented_limit_a_foreign_object_created_inside_the_head_put_window_is_overwritten() -> None:
    """Stated, not hidden (module docstring): without a conditional put nothing can prevent this."""
    client = FakeClient(conditional=False)
    message = make_message()
    key = object_key(make_config(), message)
    real_head = client.head_object

    def head_then_foreign_write(**kwargs: Any) -> dict[str, Any]:
        try:
            return real_head(**kwargs)
        finally:
            client.objects[key] = b"foreign"

    client.head_object = head_then_foreign_write  # type: ignore[method-assign]
    assert loader_for(client).deliver(message).success is True
    assert client.objects[key] == message.payload


def test_the_conditional_put_is_tried_again_after_the_reprobe_interval() -> None:
    from tributary.loaders.s3.loader import REPROBE_AFTER_S

    now = [1000.0]
    client = FakeClient(conditional=False)
    loader = S3Loader(make_config(), client, clock=lambda: now[0])
    loader.deliver(make_message())
    assert loader.conditional_put_supported is False
    loader.deliver(make_message())
    assert len([1 for op, a in client.calls if op == "put" and "IfNoneMatch" in a]) == 1
    now[0] += REPROBE_AFTER_S + 1
    assert loader.conditional_put_supported is True
    client.conditional = True  # the gateway's 501 was transient
    loader.deliver(make_message())
    assert len([1 for op, a in client.calls if op == "put" and "IfNoneMatch" in a]) == 2
    assert loader.conditional_put_supported is True


def test_expected_bucket_owner_is_sent_on_put_and_head_when_configured() -> None:
    client = FakeClient()
    loader = loader_for(client, expected_bucket_owner="123456789012")
    message = make_message()
    loader.deliver(message)
    loader.deliver(message)  # the duplicate path HEADs
    assert {op for op, _ in client.calls} == {"put", "head"}
    assert all(args["ExpectedBucketOwner"] == "123456789012" for _, args in client.calls)


def test_expected_bucket_owner_is_absent_when_not_configured() -> None:
    client = FakeClient()
    loader_for(client).deliver(make_message())
    assert all("ExpectedBucketOwner" not in args for _, args in client.calls)


def test_a_store_cannot_mint_unbounded_metric_keys_through_error_codes() -> None:
    client = FakeClient()
    loader = loader_for(client, circuit_breaker=CircuitBreakerSpec(10_000, 30.0))
    for i in range(50):
        client.raises["put"] = [client_error(f"Evil{i}Code", 400)]
        loader.deliver(make_message())
    keys = {k for k in loader.metrics.snapshot() if k.startswith("delivery_")}
    assert keys == {"delivery_S3_UNKNOWN"}
    assert loader.metrics.snapshot()["delivery_S3_UNKNOWN"] == 50


def test_known_error_codes_are_all_plain_tokens() -> None:
    from tributary.loaders.s3.loader import KNOWN_ERROR_CODES

    assert all(s3_error_code(code) != "S3_UNKNOWN" for code in KNOWN_ERROR_CODES)
