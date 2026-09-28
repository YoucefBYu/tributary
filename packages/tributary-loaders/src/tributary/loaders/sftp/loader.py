"""`SftpLoader`: writes one `Message`'s payload to one remote file (`contracts/channel-spec/v0/README.md`
3.4.4). Synchronous, like the other non-asyncio loaders: every remote wait is bounded (`session.py`).

## The file name

`<fileNamePrefix><tb-msg-id><fileExtension>` and nothing else: **nothing from the payload**, because a
directory listing is a PHI surface (README 3.4.4, 10.1). A `tb-msg-id` that is not a plain token is
`PAYLOAD_SHAPE_INVALID` and never reaches the server, so a hostile header cannot build a path.

## Idempotency: `deterministic-name` (the only technique this loader declares)

The name is a pure function of `tb-msg-id`, so a redelivery targets the same file:

1. If `<name>` already exists with the payload's size, the delivery is a duplicate and counts as
   success (`deliver` returns `reason="duplicate"`, a metric of its own). A same-named file of a
   *different* size is not ours: `SFTP_NAME_CONFLICT`, non-retryable (never silently "success").
2. Otherwise write `<name>.part` (truncating a leftover from a crash), check its size, and
   **rename** it to `<name>`. A plain SFTP rename fails if the target exists, so it can never overwrite;
   if it fails and `<name>` now exists with the right size, a concurrent delivery won the race and this
   one is a duplicate too (the `.part` is removed).

`tests/sftp/test_integration_sftp_loader.py` proves it against a real OpenSSH server, and
`tests/integration/test_sftp_loader_duplicate_delivery_uncommitted_redelivery.py` proves it through a real
`ConsumerLoop` replay: the directory holds exactly one file.

## Outcomes (README 3.4 table: `SFTP_<status>`, `SSH_HOST_KEY_MISMATCH`, `SSH_AUTH_FAILED`)

| Outcome | `tb-error-code` | Retryable |
|---|---|---|
| payload absent (claim-checked), or bad `tb-msg-id` | `PAYLOAD_SHAPE_INVALID` | no |
| server host key is not pinned | `SSH_HOST_KEY_MISMATCH` | no |
| credentials refused | `SSH_AUTH_FAILED` | no |
| SFTP status 2 (no such file: the directory), 3 (permission denied) | `SFTP_2`, `SFTP_3` | no |
| any other SFTP failure (disk full, general failure) | `SFTP_4` | yes |
| name exists, different size | `SFTP_NAME_CONFLICT` | no |
| timeout | `TIMEOUT` | yes |
| refused, reset, EOF, SSH protocol error | `CONNECT_FAILED` | yes |

A dropped connection on a reused session is retried once, transparently, on a fresh connection (the whole
operation is idempotent by construction).
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Callable
from typing import Final

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .config import SftpDestinationConfig
from .session import (
    AuthFailedError,
    ConnectionLostError,
    HostKeyMismatchError,
    RemoteError,
    SessionError,
    SessionTimeoutError,
    SftpSession,
)

_MSG_ID = re.compile(r"^[A-Za-z0-9-]{1,64}$")
PART_SUFFIX: Final = ".part"

__all__ = ["PART_SUFFIX", "SftpLoader", "remote_name"]


def remote_name(config: SftpDestinationConfig, msg_id: str) -> str:
    """The deterministic remote file name. Raises `ValueError` if `msg_id` is not a plain token."""
    if not _MSG_ID.fullmatch(msg_id):
        raise ValueError("tb-msg-id is not a plain token")
    return f"{config.file_name_prefix}{msg_id}{config.file_extension}"


class SftpLoader:
    def __init__(
        self,
        config: SftpDestinationConfig,
        connect: Callable[[], SftpSession],
        *,
        log: Logger | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self._connect = connect
        self._session: SftpSession | None = None
        self._log: Logger = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        spec = config.circuit_breaker
        self.breaker = (
            CircuitBreaker(spec.failure_threshold, spec.open_seconds, clock=clock)
            if clock is not None
            else CircuitBreaker(spec.failure_threshold, spec.open_seconds)
        )

    def _path(self, name: str) -> str:
        directory = self.config.directory
        return f"{directory}{name}" if directory.endswith("/") else f"{directory}/{name}"

    def _drop_session(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            session.close()

    def deliver(self, message: Message) -> DeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return DeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        payload = message.payload
        try:
            name = remote_name(self.config, msg_id)
        except ValueError:
            name = ""
        if payload is None or not name:
            self.metrics.record("PAYLOAD_SHAPE_INVALID")
            self._log.warning("delivery_failed", destination=self.config.destination, reason="PAYLOAD_SHAPE_INVALID")
            return DeliveryOutcome(success=False, reason="PAYLOAD_SHAPE_INVALID", retryable=False)

        reused = self._session is not None
        try:
            try:
                reason = self._deliver_once(name, payload)
            except (ConnectionLostError, SessionTimeoutError):
                if not reused:
                    raise  # a fresh connection just failed: do not double the wait against a down host
                # A reused session can be half-open: retry once on a fresh connection. Safe, the whole
                # operation is idempotent by construction (module docstring).
                self._drop_session()
                reason = self._deliver_once(name, payload)
        except SessionError as exc:
            self._drop_session()
            return self._failure(msg_id, *_classify(exc))

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record(reason)
        return DeliveryOutcome(success=True, reason=reason, retryable=False)

    def _deliver_once(self, name: str, payload: bytes) -> str:
        """Returns `"success"` or `"duplicate"`; raises a `SessionError` subclass on failure."""
        if self._session is None:
            self._session = self._connect()
        session = self._session
        final = self._path(name)
        part = final + PART_SUFFIX

        existing = session.size(final)
        if existing is not None:
            return self._duplicate_or_conflict(existing, len(payload))

        session.write(part, payload)
        written = session.size(part)
        if written != len(payload):
            raise RemoteError(4)  # short or missing write: a general failure, retryable
        try:
            session.rename(part, final)
        except RemoteError:
            # A plain rename refuses an existing target: either a concurrent delivery won the race
            # (duplicate), or the rename failed for another reason (re-raised).
            raced = session.size(final)
            if raced is None:
                raise
            outcome = self._duplicate_or_conflict(raced, len(payload))
            self._best_effort_remove(session, part)
            return outcome
        return "success"

    @staticmethod
    def _duplicate_or_conflict(existing_size: int, payload_size: int) -> str:
        if existing_size == payload_size:
            return "duplicate"
        raise _NameConflictError

    @staticmethod
    def _best_effort_remove(session: SftpSession, path: str) -> None:
        with contextlib.suppress(SessionError):  # a leftover .part is overwritten by the next attempt
            session.remove(path)

    def _failure(self, msg_id: str, reason: str, retryable: bool) -> DeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        if retryable:
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
        self._drop_session()


class _NameConflictError(SessionError):
    """A file with the deterministic name exists but is not this payload (size differs)."""

    def __init__(self) -> None:
        super().__init__("remote name exists with different content")


def _classify(exc: SessionError) -> tuple[str, bool]:
    if isinstance(exc, HostKeyMismatchError):
        return "SSH_HOST_KEY_MISMATCH", False
    if isinstance(exc, AuthFailedError):
        return "SSH_AUTH_FAILED", False
    if isinstance(exc, _NameConflictError):
        return "SFTP_NAME_CONFLICT", False
    if isinstance(exc, RemoteError):
        return f"SFTP_{exc.status}", exc.status not in (2, 3)
    if isinstance(exc, SessionTimeoutError):
        return "TIMEOUT", True
    return "CONNECT_FAILED", True
