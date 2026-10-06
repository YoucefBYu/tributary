"""`S3Loader`: writes one `Message`'s payload as one S3 object (`contracts/channel-spec/v0/README.md`
3.4.5). One bounded `PutObject` per delivery, called from `ConsumerLoop`'s single thread.

## The object key

`<prefix>/<yyyy>/<mm>/<dd>/<tb-msg-id><fileExtension>`, the date from `tb-received-at` in UTC, so a retry
(or a redelivery days later) computes the **same key**: nothing from the payload, because an object key is
listed, logged and put in access logs (README 3.4.5, 10.1; the claim-check key rule of envelope 5).

## Idempotency: `deterministic-name` (the only technique this loader declares)

The key is a pure function of `tb-msg-id` and `tb-received-at`, so a redelivery targets the same object.

* **Conditional put where the store supports it.** `PutObject` carries `If-None-Match: *`: the store
  creates the object only if the key is absent, and answers `412 PreconditionFailed` if it exists. A `412`
  is a duplicate: the loader `HEAD`s the object and, if its size is the payload's, counts the delivery
  as success (`reason="duplicate"`); a different size means the key holds something that is not this
  message, which is `S3_KEY_CONFLICT`, non-retryable, never a silent success. A concurrent conditional
  write that the store cannot order answers `409`, retryable.
* **Where the store does not implement it** (`501 NotImplemented`, e.g. an older S3-compatible server)
  the loader remembers that for `REPROBE_AFTER_S` (5 minutes, then it tries the conditional put again, so
  one transient `501` from a gateway does not degrade the destination for the life of the process) and
  falls back to **`HEAD`, then an unconditional `PutObject`** of the same bytes to the same key:
  an existing object of the payload's size is a duplicate (`reason="duplicate"`, no write), one of another
  size is `S3_KEY_CONFLICT` (non-retryable, never overwritten); only an absent key is written.
  **What this does not give (code review, Medium).** `HEAD` then `PUT` is not atomic. Two deliveries of the
  same message racing (a redelivery while the first attempt is still in flight, or two loader instances
  after a rebalance) can both see "absent" and both write. That is safe for the sink: both write the
  same bytes to the same key, so the object converges and the bucket holds one object for the message id.
  It is *not* safe against a *foreign* object created at the key inside that window, which the second
  writer overwrites (with versioning on, the earlier version is kept). Stores that silently ignore
  `If-None-Match` while answering `200` behave like the conditional path but never report the duplicate.
  All of this is stated rather than hidden; the duplicate tests prove one object and identical bytes.

`tests/s3/test_integration_s3_loader.py` proves it against MinIO, and
`tests/integration/test_s3_loader_duplicate_delivery_uncommitted_redelivery.py` through a real
`ConsumerLoop` replay: the bucket holds exactly one object for the message id.

## Outcomes (README 3.4 table: `S3_<ERROR_CODE>`)

`S3_<ERROR_CODE>` is the store's error *code* (`AccessDenied` becomes `S3_ACCESS_DENIED`), taken only if it
is one of the codes in `KNOWN_ERROR_CODES` (S3's documented error list, plus the bare status codes a `HEAD`
carries), upper-snake-cased; **anything else is `S3_UNKNOWN`**, so a store cannot mint an unbounded set of
metric keys (architect ruling). The response *message* is never read. Retryability is decided from the
status and the raw code, not from the reported name. Retryable: `5xx`,
`408`, `429`, `409`, and the throttling codes (`SlowDown`, `RequestTimeout`, `InternalError`,
`ServiceUnavailable`). Everything else (`AccessDenied`, `NoSuchBucket`, `InvalidAccessKeyId`, ...) is not.

| Outcome | `tb-error-code` | Retryable |
|---|---|---|
| payload absent (claim-checked), or `tb-received-at`/`tb-msg-id` unusable | `PAYLOAD_SHAPE_INVALID` | no |
| the store's error code | `S3_<CODE>` | per the list above |
| key exists, different size | `S3_KEY_CONFLICT` | no |
| certificate verification | `TLS_VERIFY_FAILED` | no |
| connect timeout, read timeout, or the whole call exceeding its bound | `TIMEOUT` | yes |
| refused, reset, DNS | `CONNECT_FAILED` | yes |
| no credentials found (`ambient`) | `S3_NO_CREDENTIALS` | no |
| anything else botocore raises | `TRANSPORT_ERROR` | yes |

The call runs on a bounded worker pool with an overall deadline slightly above `timeoutMs`, because
botocore's own timeouts are per socket operation (and DNS is not covered by them). A call that outlives the
deadline is abandoned and reported `TIMEOUT`; if every worker is wedged, further deliveries fail fast with
`TIMEOUT` instead of queueing behind them.
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import UTC
from typing import Any, Protocol

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .config import S3DestinationConfig

_MSG_ID = re.compile(r"^[A-Za-z0-9-]{1,64}$")
_CODE = re.compile(r"^[A-Za-z0-9]{1,48}$")
#: S3's documented error codes ("Error responses" in the S3 API reference), the throttling codes botocore
#: also surfaces, and the bare HTTP status codes a `HEAD` response carries (it has no body). The only
#: strings that ever become a metric key or a `tb-error-code`; everything else is `S3_UNKNOWN`.
KNOWN_ERROR_CODES = frozenset(
    {
        "AccessDenied", "AccountProblem", "AllAccessDisabled", "AmbiguousGrantByEmailAddress",
        "AuthorizationHeaderMalformed", "BadDigest", "BucketAlreadyExists", "BucketAlreadyOwnedByYou",
        "BucketNotEmpty", "ConditionalRequestConflict", "CredentialsNotSupported", "EntityTooLarge",
        "EntityTooSmall", "ExpiredToken", "IllegalLocationConstraintException", "IncompleteBody",
        "IncorrectNumberOfFilesInPostRequest", "InlineDataTooLarge", "InternalError", "InvalidAccessKeyId",
        "InvalidArgument", "InvalidBucketName", "InvalidBucketState", "InvalidDigest",
        "InvalidEncryptionAlgorithmError", "InvalidLocationConstraint", "InvalidObjectState", "InvalidPart",
        "InvalidPartOrder", "InvalidPolicyDocument", "InvalidRange", "InvalidRequest", "InvalidSecurity",
        "InvalidToken", "InvalidURI", "KeyTooLongError", "MalformedPOSTRequest", "MalformedXML",
        "MaxMessageLengthExceeded", "MaxPostPreDataLengthExceededError", "MetadataTooLarge",
        "MethodNotAllowed", "MissingContentLength", "MissingRequestBodyError", "MissingSecurityHeader",
        "NoSuchBucket", "NoSuchBucketPolicy", "NoSuchKey", "NoSuchLifecycleConfiguration", "NoSuchUpload",
        "NoSuchVersion", "NotFound", "NotImplemented", "NotSignedUp", "OperationAborted", "PermanentRedirect",
        "PreconditionFailed", "Redirect", "RequestIsNotMultiPartContent", "RequestTimeout",
        "RequestTimeTooSkewed", "RequestTorrentOfBucketError", "RestoreAlreadyInProgress",
        "ServerSideEncryptionConfigurationNotFoundError", "ServiceUnavailable", "SignatureDoesNotMatch",
        "SlowDown", "TemporaryRedirect", "Throttling", "ThrottlingException", "TokenRefreshRequired",
        "TooManyBuckets", "UnexpectedContent", "UnresolvableGrantByEmailAddress", "UserKeyMustBeSpecified",
        "XAmzContentSHA256Mismatch",
        "400", "403", "404", "405", "409", "412", "500", "501", "503",
    }
)  # fmt: skip
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_RETRYABLE_CODES = frozenset(
    {"SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable", "Throttling", "ThrottlingException"}
)
_NOT_FOUND = frozenset({"S3_404", "S3_NOT_FOUND", "S3_NO_SUCH_KEY"})  # a HEAD 404 has no body: code "404"

#: How long a `501 NotImplemented` keeps the loader on the unconditional path before it tries the
#: conditional put again.
REPROBE_AFTER_S = 300.0

#: Slack over `timeoutMs` for the overall deadline: botocore's connect and read timeouts are each
#: `timeoutMs`, so a healthy-but-slow call can legitimately take a little longer than one of them.
DEADLINE_SLACK_S = 1.0
MAX_WORKERS = 2

__all__ = ["KNOWN_ERROR_CODES", "S3Api", "S3Loader", "object_key", "s3_error_code"]


class S3Api(Protocol):
    """The two `botocore` client calls this loader makes."""

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> Any: ...


def object_key(config: S3DestinationConfig, message: Message) -> str:
    """The deterministic key. Raises `ValueError` if `tb-msg-id` is not a plain token or the timestamp is
    unusable. Nothing here reads the payload."""
    msg_id = message.msg_id
    if not _MSG_ID.fullmatch(msg_id):
        raise ValueError("tb-msg-id is not a plain token")
    received = message.received_at_time.astimezone(UTC)
    parts = [config.prefix] if config.prefix else []
    parts += [f"{received.year:04d}", f"{received.month:02d}", f"{received.day:02d}"]
    return "/".join(parts) + f"/{msg_id}{config.file_extension}"


def s3_error_code(raw: object) -> str:
    """`S3_<UPPER_SNAKE>` from a store error code, only if it is in `KNOWN_ERROR_CODES`."""
    if not isinstance(raw, str) or not _CODE.fullmatch(raw) or raw not in KNOWN_ERROR_CODES:
        return "S3_UNKNOWN"
    return "S3_" + _CAMEL.sub("_", raw).upper()


class _Failure(Exception):
    """Internal: carries the classified outcome out of `_put`."""

    def __init__(self, reason: str, retryable: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable


class S3Loader:
    def __init__(
        self,
        config: S3DestinationConfig,
        client: S3Api,
        *,
        log: Logger | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self._client = client
        self._log: Logger = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        spec = config.circuit_breaker
        self.breaker = (
            CircuitBreaker(spec.failure_threshold, spec.open_seconds, clock=clock)
            if clock is not None
            else CircuitBreaker(spec.failure_threshold, spec.open_seconds)
        )
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._unconditional_until = 0.0  # set once the store answers NotImplemented (module docstring)
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="s3-loader")
        self._slots = threading.BoundedSemaphore(MAX_WORKERS)

    @property
    def conditional_put_supported(self) -> bool:
        return self._clock() >= self._unconditional_until

    # -- one bounded call ------------------------------------------------------------------------

    def _call(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """`fn(**kwargs)` on a worker, abandoned after the overall deadline. Never queues behind a wedged
        worker: with every slot taken it fails at once."""
        if not self._slots.acquire(blocking=False):
            raise TimeoutError
        try:
            future: Future[Any] = self._executor.submit(fn, **kwargs)
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _f: self._slots.release())
        try:
            return future.result(timeout=self.config.timeout_s + DEADLINE_SLACK_S)
        except FutureTimeout:
            raise TimeoutError from None

    def _classify(self, exc: BaseException) -> _Failure:
        """botocore and stdlib exceptions to `(code, retryable)`. Never reads an error *message*."""
        import ssl

        from botocore import exceptions as bex  # type: ignore[import-untyped]

        if isinstance(exc, bex.ClientError):
            response = exc.response
            code = response.get("Error", {}).get("Code")
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            retryable = (
                isinstance(status, int) and (status >= 500 or status in (408, 409, 429))
            ) or code in _RETRYABLE_CODES
            return _Failure(s3_error_code(code), retryable)
        if isinstance(exc, bex.NoCredentialsError | bex.PartialCredentialsError):
            return _Failure("S3_NO_CREDENTIALS", False)
        if isinstance(exc, bex.SSLError):
            chain: BaseException | None = exc
            while chain is not None:
                if isinstance(chain, ssl.SSLCertVerificationError):
                    return _Failure("TLS_VERIFY_FAILED", False)
                chain = chain.__cause__ or chain.__context__
            return _Failure("CONNECT_FAILED", True)  # a reset mid-handshake is transient
        if isinstance(exc, TimeoutError | bex.ConnectTimeoutError | bex.ReadTimeoutError):
            return _Failure("TIMEOUT", True)
        if isinstance(exc, bex.EndpointConnectionError | bex.ConnectionClosedError):
            return _Failure("CONNECT_FAILED", True)
        return _Failure("TRANSPORT_ERROR", True)  # unrecognised: retryable, never success

    # -- delivery --------------------------------------------------------------------------------

    def deliver(self, message: Message) -> DeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return DeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        payload = message.payload
        try:
            key = object_key(self.config, message)
        except ValueError:
            key = ""
        if payload is None or not key:
            self.metrics.record("PAYLOAD_SHAPE_INVALID")
            self._log.warning("delivery_failed", destination=self.config.destination, reason="PAYLOAD_SHAPE_INVALID")
            return DeliveryOutcome(success=False, reason="PAYLOAD_SHAPE_INVALID", retryable=False)

        try:
            reason = self._put(message, key, payload)
        except _Failure as failure:
            return self._failure(msg_id, failure.reason, failure.retryable)

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record(reason)
        return DeliveryOutcome(success=True, reason=reason, retryable=False)

    def _put_args(self, message: Message, key: str, payload: bytes) -> dict[str, Any]:
        args: dict[str, Any] = {
            "Bucket": self.config.bucket,
            "Key": key,
            "Body": payload,
            "ContentLength": len(payload),
            "ContentType": message.content_type,
            "ServerSideEncryption": self.config.sse,
            "Metadata": {"tb-msg-id": message.msg_id},
        }
        if self.config.kms_key_id is not None:
            args["SSEKMSKeyId"] = self.config.kms_key_id
        if self.config.expected_bucket_owner is not None:
            args["ExpectedBucketOwner"] = self.config.expected_bucket_owner
        return args

    def _head_args(self, key: str) -> dict[str, Any]:
        args: dict[str, Any] = {"Bucket": self.config.bucket, "Key": key}
        if self.config.expected_bucket_owner is not None:
            args["ExpectedBucketOwner"] = self.config.expected_bucket_owner
        return args

    def _put(self, message: Message, key: str, payload: bytes) -> str:
        args = self._put_args(message, key, payload)
        if self.conditional_put_supported:
            try:
                self._call(self._client.put_object, IfNoneMatch="*", **args)
                return "success"
            except Exception as exc:
                failure = self._classify(exc)
                if failure.reason == "S3_PRECONDITION_FAILED":
                    return self._duplicate_or_conflict(key, len(payload))
                if failure.reason == "S3_NOT_IMPLEMENTED":
                    self._unconditional_until = self._clock() + REPROBE_AFTER_S
                    self._log.info("if_none_match_unsupported", destination=self.config.destination)
                else:
                    raise failure from None
        return self._put_unconditional(key, args, len(payload))

    def _put_unconditional(self, key: str, args: dict[str, Any], payload_size: int) -> str:
        """The store has no conditional put: `HEAD` first so an existing object is a duplicate or a conflict,
        never silently overwritten; write only an absent key. Not atomic (module docstring)."""
        try:
            head = self._call(self._client.head_object, **self._head_args(key))
        except Exception as exc:
            failure = self._classify(exc)
            if failure.reason not in _NOT_FOUND:
                raise failure from None
        else:
            return self._judge_existing(head, payload_size)
        try:
            self._call(self._client.put_object, **args)
        except Exception as exc:
            raise self._classify(exc) from None
        return "success"

    def _duplicate_or_conflict(self, key: str, payload_size: int) -> str:
        try:
            head = self._call(self._client.head_object, **self._head_args(key))
        except Exception as exc:
            failure = self._classify(exc)
            if failure.reason in _NOT_FOUND:
                raise _Failure("S3_KEY_VANISHED", True) from None  # deleted between the 412 and the HEAD
            raise failure from None
        return self._judge_existing(head, payload_size)

    @staticmethod
    def _judge_existing(head: object, payload_size: int) -> str:
        size = head.get("ContentLength") if isinstance(head, dict) else None
        if size == payload_size:
            return "duplicate"
        raise _Failure("S3_KEY_CONFLICT", False)

    def _failure(self, msg_id: str, reason: str, retryable: bool) -> DeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        if retryable or reason == "TLS_VERIFY_FAILED":
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
            retryable=retryable,
        )
        return DeliveryOutcome(success=False, reason=reason, retryable=retryable)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
