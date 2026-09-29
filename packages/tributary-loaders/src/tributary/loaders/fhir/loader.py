"""`FhirLoader`: POSTs one step-built FHIR transaction Bundle to one `fhir` destination
(`contracts/channel-spec/v0/README.md` section 3.4.2, ADR-007 D12). Synchronous, like the `clickhouse`
and `kafka` loaders: one blocking, time-bounded HTTP call per delivery, called from `ConsumerLoop`'s
single thread.

## Transport

`tributary.loaders.http.client.stdlib_send` is shared, not copied. It carries the wave 1 and 2
hardening: redirects refused, no environment proxy, TLS verification against the OS trust store,
certificate failures split out as non-retryable, an overall read deadline, a response-size cap, and
exceptions that name only an exception class (never the peer or the URL).

## The payload is never rewritten

This loader parses the Bundle to check it (below) and POSTs `message.payload` byte for byte. Entries
are conditional already (`PUT`, or `POST` with `ifNoneExist`), built by a step (`tributary-content-
hl7-fhir`); nothing here touches `entry.request`.

## Idempotency

Keyed by `tb-msg-id`, not configurable (invariant 4). The technique is `conditional-update` (the usual
case), `idempotency-key-header`, or `none`:

* `conditional-update`: every entry is idempotent on its own, so a redelivered Bundle converges on the
  same resources instead of creating twice. **The loader does not trust the declaration**: before
  sending, it refuses (`FHIR_BUNDLE_INVALID`, non-retryable) a Bundle with an entry that is not
  idempotent by construction: `POST` with no `ifNoneExist`, or a method other than `PUT`, `DELETE` and
  conditional `POST`. That is the per-message form of the startup proof the SQL loaders do.
* `idempotency-key-header`: the configured header carries `tb-msg-id`, for a server that honours it on
  top of the entries' own conditions.
* `none`: no loader-side behaviour; the schema requires a `note`.

**Bounded retry without conditional entries (code review, Medium).** Under `idempotency-key-header` and
`none` the loader cannot know a retry is harmless: a Bundle of plain `POST`s that timed out may already
have been applied, and every further blocking retry (`maxAttempts` allows 1000) can add another copy.
So for these two techniques the loader stops asking for a retry after `MAX_UNPROTECTED_ATTEMPTS` (3)
requests for the same `tb-msg-id`: the outcome keeps its error code but becomes non-retryable, and the
runtime then applies `onExhaust` (dead-letter, or halt the partition). The worst case is three copies
of an ambiguous write, visible and operator-handled, instead of hundreds. `conditional-update` is not
capped: its entries are proven idempotent per message. The counter is per process and per record (a
restart, or the next record, starts again).

**`duplicate` is an observation, never a decision.** A `2xx` in which no entry answered `201 Created`
(every entry `200`, which is what a server says for a conditional update or `ifNoneExist` that matched an
existing resource) is counted as `reason="duplicate"` (metric `delivery_duplicate`) and is a success either
way. A first delivery that only updates resources which already existed reads the same, so the counter is
an upper bound on redeliveries, not a proof of one.

`tests/fhir/test_integration_fhir_loader.py` proves it against a real HAPI FHIR server: the same Bundle
delivered twice leaves exactly one Patient and one Encounter, and
`tests/integration/test_fhir_loader_duplicate_delivery_uncommitted_redelivery.py` proves it through a
real `ConsumerLoop` replay: the loader process is SIGKILLed after HAPI applied the Bundle and before the
offset was committed, and a second process redelivers it.

## Classifying the outcome (README 3.4.2, and rule 1 of 3.4: nothing is success by default)

| Outcome | `tb-error-code` | Retryable |
|---|---|---|
| Payload is not a `transaction` Bundle, or has a non-idempotent entry | `FHIR_BUNDLE_INVALID` | no |
| `3xx` | `HTTP_REDIRECT_REFUSED` | no |
| `408`, `429`, any `5xx` | `HTTP_<status>` | yes |
| `409` | `HTTP_409` | yes: a version or concurrent-create conflict; the retry re-runs the conditions and converges |
| `412` | `HTTP_412` | no: a condition matched more than one resource; retrying repeats the ambiguity forever |
| any other `4xx` (`400`, `401`, `403`, `404`, `422`) | `HTTP_<status>` | no |
| `2xx`, response is a `transaction-response` with the request's entry count, every entry `2xx` | success | |
| `2xx`, but an entry status is `>= 400` | `FHIR_ENTRY_FAILED` | only if some failed entry is `408/409/429/5xx` |
| `2xx`, but no parseable `transaction-response` of the right size (a gateway's `200` page, a truncated body) | `FHIR_RESPONSE_INVALID` | yes: the entries are idempotent, so a resend is safe, and bounded by `maxAttempts` |
| timeout | `TIMEOUT` | yes |
| connect, reset, other transport failure | `CONNECT_FAILED` | yes |
| certificate verification | `TLS_VERIFY_FAILED` | no |

The code is `HTTP_<status>` for every non-2xx, per the README family table; the server's
`OperationOutcome` is **not** copied into the code, a header or a log line (rule 2: it routinely quotes
the payload). Only its `issue[0].code`, checked against the fixed FHIR `IssueType` vocabulary, is logged
as `issue_code`, so an arbitrary server string can never reach a log.
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from ..http.client import Request, Response, TlsVerificationError, TransportError, TransportTimeout, stdlib_send
from ..http.config import AuthSpec
from ..secrets import SecretResolver, reject_control_characters
from .config import FhirDestinationConfig

#: A transaction-response carries one small `response` object per entry (the request Bundle is at most
#: 1,000,000 bytes, envelope 8); even with representations it is the same order of size. Anything above
#: this is not a well-behaved answer and is `FHIR_RESPONSE_INVALID`, never buffered further.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024

#: The FHIR `IssueType` value set (`http://hl7.org/fhir/issue-type`). `issue_code` is logged only if it
#: is one of these, so no server-authored string reaches a log.
ISSUE_TYPE_CODES = frozenset(
    {
        "invalid", "structure", "required", "value", "invariant", "security", "login", "unknown", "expired",
        "forbidden", "suppressed", "processing", "not-supported", "duplicate", "multiple-matches", "not-found",
        "deleted", "too-long", "code-invalid", "extension", "too-costly", "business-rule", "conflict",
        "transient", "lock-error", "no-store", "exception", "timeout", "incomplete", "throttled", "informational",
    }
)  # fmt: skip

_RETRYABLE_STATUSES = frozenset({408, 409, 429})

#: Requests sent for one `tb-msg-id` before a retryable failure stops being retryable, for the techniques
#: that give the loader no per-message proof of idempotency (module docstring).
MAX_UNPROTECTED_ATTEMPTS = 3

__all__ = [
    "MAX_UNPROTECTED_ATTEMPTS",
    "BundleShapeError",
    "FhirLoader",
    "check_transaction_bundle",
    "classify_status",
    "judge_transaction_response",
]


class BundleShapeError(Exception):
    """The outbound payload is not an acceptable FHIR transaction Bundle. `str(exc)` is a fixed phrase,
    never a payload value (which may be PHI)."""


def check_transaction_bundle(payload: bytes | None, *, require_idempotent_entries: bool) -> int:
    """README 3.4.2: `Bundle.type` is checked before anything is sent. Returns the entry count. Raises
    `BundleShapeError`. Never repairs or rewrites the payload (README 3.4 rule 3)."""
    if payload is None:
        raise BundleShapeError("payload is absent (a claim-checked record has no Bundle to send)")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise BundleShapeError("payload is not valid JSON") from exc
    if not isinstance(document, dict):
        raise BundleShapeError("payload is not a JSON object")
    if document.get("resourceType") != "Bundle":
        raise BundleShapeError("payload is not a FHIR Bundle")
    if document.get("type") != "transaction":
        raise BundleShapeError("Bundle.type is not 'transaction' (ADR-007 D12: transaction only)")
    entries = document.get("entry")
    if not isinstance(entries, list) or not entries:
        raise BundleShapeError("Bundle has no entries")
    for entry in entries:
        request = entry.get("request") if isinstance(entry, dict) else None
        if not isinstance(request, dict) or not isinstance(request.get("method"), str):
            raise BundleShapeError("a Bundle entry has no request.method")
        if not require_idempotent_entries:
            continue
        method = request["method"]
        if method in ("PUT", "DELETE"):
            continue
        if method == "POST" and isinstance(request.get("ifNoneExist"), str) and request["ifNoneExist"]:
            continue
        raise BundleShapeError(
            "a Bundle entry is not idempotent (README 3.4.2 allows PUT, DELETE, and POST with ifNoneExist)"
        )
    return len(entries)


def classify_status(status_code: int) -> tuple[str, bool] | None:
    """`(error_code, retryable)` for a non-2xx status, `None` for 2xx."""
    if 200 <= status_code < 300:
        return None
    if 300 <= status_code < 400:
        return "HTTP_REDIRECT_REFUSED", False
    retryable = status_code in _RETRYABLE_STATUSES or 500 <= status_code < 600
    if status_code < 200 or status_code >= 600:
        retryable = True  # not a status this loader recognises: retryable, never success (rule 1)
    return f"HTTP_{status_code}", retryable


@dataclass(frozen=True, slots=True)
class _EntryVerdict:
    ok: bool
    retryable: bool = False
    index: int | None = None
    status: int | None = None
    #: On success: some entry answered `201` (a resource was created), i.e. this was not a redelivery.
    created: bool = False


def check_transaction_response(body: bytes, *, expected_entries: int) -> _EntryVerdict | None:
    """Return `None` if `body` is a valid, fully successful `transaction-response`; otherwise a verdict
    (`ok=False`) for a failed entry, or `_EntryVerdict(ok=False, index=None)` when the body is not a
    usable response at all."""
    verdict = judge_transaction_response(body, expected_entries=expected_entries)
    return None if verdict.ok else verdict


def judge_transaction_response(body: bytes, *, expected_entries: int) -> _EntryVerdict:
    """As `check_transaction_response`, but a success is `_EntryVerdict(ok=True, created=...)`."""
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return _EntryVerdict(ok=False, retryable=True)
    if (
        not isinstance(document, dict)
        or document.get("resourceType") != "Bundle"
        or document.get("type") != "transaction-response"
    ):
        return _EntryVerdict(ok=False, retryable=True)
    entries = document.get("entry")
    if not isinstance(entries, list) or len(entries) != expected_entries:
        return _EntryVerdict(ok=False, retryable=True)
    created = False
    for index, entry in enumerate(entries):
        response = entry.get("response") if isinstance(entry, dict) else None
        status = response.get("status") if isinstance(response, dict) else None
        code_text = status.split(" ", 1)[0] if isinstance(status, str) else ""
        if not code_text.isdigit():
            return _EntryVerdict(ok=False, retryable=True)
        code = int(code_text)
        if code >= 400 or code < 200:
            retryable = code in _RETRYABLE_STATUSES or 500 <= code < 600
            return _EntryVerdict(ok=False, retryable=retryable, index=index, status=code)
        created = created or code == 201
    return _EntryVerdict(ok=True, created=created)


def _issue_code(body: bytes) -> str | None:
    """`issue[0].code` of an `OperationOutcome`, only if it is in the fixed `IssueType` vocabulary."""
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return None
    if not isinstance(document, dict) or document.get("resourceType") != "OperationOutcome":
        return None
    issues = document.get("issue")
    if not isinstance(issues, list) or not issues or not isinstance(issues[0], dict):
        return None
    code = issues[0].get("code")
    return code if isinstance(code, str) and code in ISSUE_TYPE_CODES else None


def _auth_header(auth: AuthSpec, resolver: SecretResolver) -> tuple[str, str]:
    if auth.type == "basic":
        creds = reject_control_characters(resolver.resolve_text(auth.secret_ref, "credentials"))  # "user:pass"
        return "Authorization", "Basic " + base64.b64encode(creds.encode("utf-8")).decode("ascii")
    if auth.type == "bearer":
        return "Authorization", "Bearer " + reject_control_characters(resolver.resolve_text(auth.secret_ref, "token"))
    raise ValueError(f"unsupported auth type {auth.type!r}")  # config.py refuses this at load time


class FhirLoader:
    def __init__(
        self,
        config: FhirDestinationConfig,
        *,
        secret_resolver: SecretResolver | None = None,
        send: Callable[[Request], Response] = stdlib_send,
        log: Logger | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._send = send
        self._log: Logger = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        self.breaker = CircuitBreaker(
            config.circuit_breaker.failure_threshold, config.circuit_breaker.open_seconds, clock=clock
        )
        self._sent_for: str | None = None  # the msg id `_sent_count` counts requests for
        self._sent_count = 0
        # Resolved once, at startup (README 3.4 rule 4); never logged, never in an exception.
        self._auth: tuple[str, str] | None = None
        if config.auth is not None:
            if secret_resolver is None:
                raise ValueError("destination has auth configured but no secret_resolver was given")
            self._auth = _auth_header(config.auth, secret_resolver)

    def __repr__(self) -> str:  # the resolved credential is an attribute: keep it out of any repr
        return f"FhirLoader(destination={self.config.destination!r})"

    def _headers(self, message: Message) -> dict[str, str]:
        headers = dict(self.config.headers)
        headers["Content-Type"] = "application/fhir+json"
        headers.setdefault("Accept", "application/fhir+json")
        if self.config.idempotency_technique == "idempotency-key-header" and self.config.idempotency_header:
            headers[self.config.idempotency_header] = message.msg_id
        if self._auth is not None:
            headers[self._auth[0]] = self._auth[1]
        return headers

    def deliver(self, message: Message) -> DeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return DeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        try:
            expected = check_transaction_bundle(
                message.payload,
                require_idempotent_entries=self.config.idempotency_technique == "conditional-update",
            )
        except BundleShapeError as exc:
            # A bad payload says nothing about the destination's health: the breaker is not touched.
            self.metrics.record("FHIR_BUNDLE_INVALID")
            self._log.warning(
                "delivery_failed",
                destination=self.config.destination,
                message_id=msg_id,
                reason="FHIR_BUNDLE_INVALID",
                detail=str(exc),
            )
            return DeliveryOutcome(success=False, reason="FHIR_BUNDLE_INVALID", retryable=False)

        request = Request(
            method="POST",
            url=self.config.url,
            headers=self._headers(message),
            body=message.payload or b"",
            timeout_s=self.config.timeout_s,
            max_response_bytes=MAX_RESPONSE_BYTES,
        )
        if self._sent_for != msg_id:
            self._sent_for, self._sent_count = msg_id, 0
        self._sent_count += 1
        try:
            response = self._send(request)
        except TransportTimeout:
            return self._failure(msg_id, "TIMEOUT", retryable=True)
        except TlsVerificationError:
            return self._failure(msg_id, "TLS_VERIFY_FAILED", retryable=False)
        except TransportError:
            return self._failure(msg_id, "CONNECT_FAILED", retryable=True)

        failure = classify_status(response.status_code)
        if failure is not None:
            code, retryable = failure
            return self._failure(
                msg_id,
                code,
                retryable=retryable,
                status_code=response.status_code,
                issue_code=_issue_code(response.body),
            )

        if response.truncated:
            return self._failure(msg_id, "FHIR_RESPONSE_INVALID", retryable=True, status_code=response.status_code)
        verdict = judge_transaction_response(response.body, expected_entries=expected)
        if not verdict.ok:
            if verdict.index is None:
                return self._failure(msg_id, "FHIR_RESPONSE_INVALID", retryable=True, status_code=response.status_code)
            return self._failure(
                msg_id,
                "FHIR_ENTRY_FAILED",
                retryable=verdict.retryable,
                status_code=response.status_code,
                entry_index=verdict.index,
                entry_status=verdict.status,
            )

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self._sent_for = None
        reason = "success" if verdict.created else "duplicate"
        self.metrics.record(reason)
        return DeliveryOutcome(success=True, reason=reason, retryable=False, detail=str(response.status_code))

    def _failure(self, msg_id: str, reason: str, *, retryable: bool, **fields: Any) -> DeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        # Only failures that say something about the destination move the breaker: a `412` or `422`
        # is this message's problem, not an outage.
        if retryable or reason == "TLS_VERIFY_FAILED":
            self.breaker.record_failure()
        if (
            retryable
            and self.config.idempotency_technique != "conditional-update"
            and self._sent_for == msg_id
            and self._sent_count >= MAX_UNPROTECTED_ATTEMPTS
        ):
            # Module docstring: without per-entry idempotency every retry may add a copy.
            retryable = False
            fields["retry_cap"] = MAX_UNPROTECTED_ATTEMPTS
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning(
            "delivery_failed",
            destination=self.config.destination,
            message_id=msg_id,
            reason=reason,
            retryable=retryable,
            **{k: v for k, v in fields.items() if v is not None},
        )
        return DeliveryOutcome(success=False, reason=reason, retryable=retryable)

    def close(self) -> None:
        pass  # no persistent connection: every request is its own urllib call
