"""`HttpLoader`: one delivery attempt of one `Message` to one HTTP destination.

## Idempotency

Keyed by `tb-msg-id`, never configurable (`contracts/channel-spec/v0` README section 5; invariant
4). `idempotency.technique`:

* `idempotency-key-header` — this loader sets `idempotency.header` to `message.msg_id` on every
  attempt for the same message, so a retried delivery (blocking retry redelivers the identical
  record; a sender's own resend goes through a *new* `tb-msg-id`, which is a different logical
  delivery on purpose) carries the *same* header value every time. A destination that deduplicates
  on that header therefore sees one logical write no matter how many attempts this loader makes.
  Nothing here can prove a third party's server actually deduplicates; it can and does prove this
  loader sends the same key for the same message every time — the half of the contract that is this
  package's to keep. `tests/http/test_loader.py::test_duplicate_delivery_is_harmless` demonstrates
  it against a small stub destination that applies the same rule the real technique assumes.
* `conditional-update`, `deterministic-name`, `sink-native` — no loader-side header. The channel
  spec's `url` has no templating (`contracts/channel-spec/v0` README 3.4/7: "no query string, no
  templating"), so a `PUT` to a fixed URL is naturally idempotent by HTTP semantics, and a
  destination that dedupes on its own is unaffected by what this loader sends. Nothing to test here
  beyond "the loader does not add an idempotency header it was not asked for."
* `none` — no loader-side behaviour; the schema requires a `note` justifying it.

## Timeout, backoff, circuit breaker

One `deliver()` call is one attempt with `with.timeoutMs` as its hard deadline. It does not retry:
`retry.policy: blocking` (the only value the schema allows) means the *caller* — `consumer.py`'s
`OutConsumer` in this wave — holds the partition, waits `backoff.next_delay_ms`, and calls `deliver`
again with the same message. `with.circuitBreaker`, when configured, is consulted first: an open
breaker returns immediately without a network call, which is what keeps a dead destination from
adding its own timeout to every attempt while it is down.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from tributary.sdk import Message

from ..secrets import SecretResolver
from .circuit_breaker import CircuitBreaker
from .client import Request, Response, TransportError, stdlib_send
from .config import AuthSpec, HttpDestinationConfig

Reason = Literal["success", "circuit-open", "timeout", "transport-error", "http-5xx", "http-429", "http-4xx"]


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    success: bool
    reason: Reason
    status_code: int | None = None

    @property
    def retryable(self) -> bool:
        return not self.success and self.reason != "http-4xx"


def classify_status(status_code: int) -> Reason:
    if 200 <= status_code < 300:
        return "success"
    if status_code == 429:
        return "http-429"
    if 500 <= status_code < 600:
        return "http-5xx"
    return "http-4xx"


def _auth_header(auth: AuthSpec, resolver: SecretResolver) -> tuple[str, str]:
    if auth.type == "basic":
        import base64

        creds = resolver.resolve_text(auth.secret_ref, "credentials")  # "user:pass"
        return "Authorization", "Basic " + base64.b64encode(creds.encode("utf-8")).decode("ascii")
    if auth.type == "bearer":
        token = resolver.resolve_text(auth.secret_ref, "token")
        return "Authorization", f"Bearer {token}"
    raise ValueError(f"unsupported auth type {auth.type!r}")  # config.py already refuses this at load time


class HttpLoader:
    def __init__(
        self,
        config: HttpDestinationConfig,
        *,
        secret_resolver: SecretResolver | None = None,
        send: Callable[[Request], Response] = stdlib_send,
    ) -> None:
        self.config = config
        self._secret_resolver = secret_resolver
        self._send = send
        self.breaker = (
            CircuitBreaker(config.circuit_breaker.failure_threshold, config.circuit_breaker.open_seconds)
            if config.circuit_breaker
            else CircuitBreaker.disabled()
        )

    def _headers(self, message: Message) -> dict[str, str]:
        headers = dict(self.config.headers)
        headers.setdefault("Content-Type", message.content_type)
        if self.config.idempotency_technique == "idempotency-key-header" and self.config.idempotency_header:
            headers[self.config.idempotency_header] = message.msg_id
        if self.config.auth is not None:
            if self._secret_resolver is None:
                raise ValueError("destination has auth configured but no secret_resolver was given")
            name, value = _auth_header(self.config.auth, self._secret_resolver)
            headers[name] = value
        return headers

    async def deliver(self, message: Message) -> DeliveryOutcome:
        if not self.breaker.allow():
            return DeliveryOutcome(success=False, reason="circuit-open")
        payload = message.payload
        request = Request(
            method=self.config.method,
            url=self.config.url,
            headers=self._headers(message),
            body=payload if payload is not None else b"",
            timeout_s=self.config.timeout_s,
        )
        try:
            response = await asyncio.wait_for(asyncio.to_thread(self._send, request), self.config.timeout_s + 1)
        except TimeoutError:
            self.breaker.record_failure()
            return DeliveryOutcome(success=False, reason="timeout")
        except TransportError:
            self.breaker.record_failure()
            return DeliveryOutcome(success=False, reason="transport-error")
        reason = classify_status(response.status_code)
        if reason == "success":
            self.breaker.record_success()
            return DeliveryOutcome(success=True, reason=reason, status_code=response.status_code)
        self.breaker.record_failure()
        return DeliveryOutcome(success=False, reason=reason, status_code=response.status_code)
