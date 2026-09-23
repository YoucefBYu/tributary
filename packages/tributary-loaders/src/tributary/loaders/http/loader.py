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
`retry.policy: blocking` (the only value the schema allows) means the *caller* —
`tributary.runtime.consumer.ConsumerLoop`, driven through `handler.HttpRecordHandler` — holds the
partition, waits out a backoff, and calls `deliver` again with the same message. `with.circuitBreaker`,
when configured, is consulted first: an open breaker returns immediately without a network call, which
is what keeps a dead destination from adding its own timeout to every attempt while it is down.

## The thread behind a blocked request never really stops

`client.stdlib_send` is a blocking call run off the event loop. `asyncio.wait_for` gives up waiting
after `timeout_s`, but it cannot make the underlying OS thread return early — a peer that accepts a
connection and then never answers (or answers one byte at a time, resetting `urllib`'s own socket
timeout each time) keeps that thread blocked for as long as the peer wants, timeout or no timeout
(code review, W1-6 second round, High 2). Two things bound the damage rather than eliminate it,
because eliminating it needs a client that can actually interrupt a blocking read, which the standard
library does not offer without a second dependency this module already declined to add (module
docstring, `client.py`):

* A **dedicated, bounded thread pool per `HttpLoader`** (`_executor`, sized by
  `max_concurrent_requests`), not the process-wide default pool `asyncio.to_thread` shares with every
  other coroutine in the process. A destination that wedges every one of its own threads cannot starve
  an unrelated destination's, or the receiver's, thread pool.
* The circuit breaker opens on a timeout (`record_failure` below) exactly as it does on a 5xx, so once
  `failure_threshold` attempts have timed out, no further attempt is submitted to the pool at all —
  bounding how many of those threads a single wedged destination can ever occupy at once to roughly
  `failure_threshold`, not "one per retry forever."
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..secrets import SecretResolver
from .circuit_breaker import CircuitBreaker, State
from .client import Request, Response, TransportError, stdlib_send
from .config import AuthSpec, HttpDestinationConfig
from .metrics import LoaderMetrics

Reason = Literal["success", "circuit-open", "timeout", "transport-error", "http-5xx", "http-429", "http-4xx"]

DEFAULT_MAX_CONCURRENT_REQUESTS = 8


class _NullLogger:
    """The default when no `log=` is given: tests and library callers that do not care to see log
    output get quiet ones, and `main.py` passes a real `tributary.runtime.logs.JsonLogger`."""

    def debug(self, event: str, /, **fields: object) -> None:
        pass

    def info(self, event: str, /, **fields: object) -> None:
        pass

    def warning(self, event: str, /, **fields: object) -> None:
        pass

    def error(self, event: str, /, **fields: object) -> None:
        pass


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
        log: Logger | None = None,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    ) -> None:
        self.config = config
        self._secret_resolver = secret_resolver
        self._send = send
        self._log = log if log is not None else _NullLogger()
        self.metrics = LoaderMetrics()
        self.breaker = (
            CircuitBreaker(config.circuit_breaker.failure_threshold, config.circuit_breaker.open_seconds)
            if config.circuit_breaker
            else CircuitBreaker.disabled()
        )
        # A dedicated pool, not asyncio.to_thread's process-wide default one: see the module
        # docstring. Sized small on purpose — this bounds how many requests to ONE destination can
        # be in flight (and, worst case, wedged) at once, not overall loader throughput.
        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_requests, thread_name_prefix=f"http-loader-{config.destination}"
        )

    def close(self) -> None:
        """Stop accepting new work. Does not (cannot) wait for an already-wedged thread; see the
        module docstring's "never really stops" section. `wait=False`: shutting down must not hang
        behind exactly the kind of stuck request this class exists to bound the damage from."""
        self._executor.shutdown(wait=False, cancel_futures=True)

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

    def _record_failure(self, msg_id: str, reason: Reason, *, status_code: int | None = None) -> DeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        self.breaker.record_failure()
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning(
            "delivery_failed",
            destination=self.config.destination,
            message_id=msg_id,
            reason=reason,
            status_code=status_code,
        )
        return DeliveryOutcome(success=False, reason=reason, status_code=status_code)

    async def deliver(self, message: Message) -> DeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return DeliveryOutcome(success=False, reason="circuit-open")
        payload = message.payload
        request = Request(
            method=self.config.method,
            url=self.config.url,
            headers=self._headers(message),
            body=payload if payload is not None else b"",
            timeout_s=self.config.timeout_s,
        )
        loop = asyncio.get_running_loop()
        # asyncio.shield keeps wait_for's cancellation (on timeout) from touching the inner future:
        # it would not stop the OS thread anyway (not cooperative), and cancelling the inner future
        # would just discard whatever result eventually arrives instead of letting it be logged.
        inner = loop.run_in_executor(self._executor, self._send, request)
        try:
            response = await asyncio.wait_for(asyncio.shield(inner), self.config.timeout_s + 1)
        except TimeoutError:
            # Only attach the late-completion callback once we know we are giving up early: adding
            # it unconditionally would fire it on every ordinary, on-time completion too, which is
            # not "late" and not worth a log line.
            inner.add_done_callback(self._log_late_completion)
            return self._record_failure(msg_id, "timeout")
        except TransportError:
            return self._record_failure(msg_id, "transport-error")
        reason = classify_status(response.status_code)
        if reason == "success":
            was_half_open = self.breaker.state is State.HALF_OPEN
            self.breaker.record_success()
            if was_half_open:
                self.metrics.circuit_closes += 1
                self._log.info("circuit_closed", destination=self.config.destination)
            self.metrics.record(reason)
            return DeliveryOutcome(success=True, reason=reason, status_code=response.status_code)
        return self._record_failure(msg_id, reason, status_code=response.status_code)

    def _log_late_completion(self, future: asyncio.Future[Response]) -> None:
        """Fires when the shielded executor call finishes — possibly long after `deliver` already
        gave up on it. Confirms, for an operator, that a timeout was a truly wedged thread and not a
        transient blip, and prevents an "exception was never retrieved" warning for a future nobody
        else is holding onto by the time it resolves."""
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            self._log.warning("delayed_request_failed", destination=self.config.destination, error=repr(exc))
        else:
            self._log.info("delayed_request_completed", destination=self.config.destination)
