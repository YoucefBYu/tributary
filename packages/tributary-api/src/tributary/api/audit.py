"""The production `AuditHook` (W3-32; `contracts/audit/v0`).

Every audit record is produced to `_tb.<env>.audit` (`acks=all`, idempotent producer, key = actor) as one JSON
object that is validated against the contract schema BEFORE it is sent, so a malformed record is a bug that
surfaces in a test, never a bad row in `audit_log`.

* **`requested` fails closed** (`AuditHook.requested`): it is produced and acknowledged before the action runs.
  If it is not acknowledged the caller gets `503 AUDIT_UNAVAILABLE` and the action does not happen.
* **Follow-ups are best effort** (`AuditHook.followup`): `completed` and `failed` after a re-publish. A failure is
  logged by class name only (`audit_followup_failed`) and never turns a delivered re-publish into an error.
* **`audit_id` is minted once per record** (UUIDv7) and the same serialized bytes are re-sent on every retry, so a
  duplicate delivery collapses at query time in `audit_log` (README 3 rule 7). A timed-out produce is retried
  with the same id: its outcome is unknown, and the duplicate is the safe side of that doubt.
* **No PHI (invariant 6).** A record holds identifiers, codes and counts. This module never sees a payload, a
  key, a header value or a token, and it logs the error class and the action only.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from tributary.runtime.config import find_contracts_dir
from tributary.runtime.topics import platform_topic
from tributary.sdk.context import Logger
from tributary.sdk.ids import uuid7

from .problems import ApiProblem

SCHEMA_RELATIVE = Path("audit") / "v0" / "audit-event.schema.json"
MAX_RECORD_BYTES = 8192
SEND_ATTEMPTS = 2
RETRY_PAUSE_S = 0.2


class AuditSendError(Exception):
    """The record was not acknowledged. Carries no detail."""


class AuditSender(Protocol):
    """Produce one record to `_tb.<env>.audit` and return once the broker acknowledged it. Raises
    `AuditSendError` otherwise."""

    async def send(self, env: str, key: bytes, value: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class Actor:
    """Who is asking: the token's `sub`, the Tributary roles it mapped to, the HTTP request id."""

    subject: str
    roles: frozenset[str]
    request_id: str | None = None


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    path = find_contracts_dir() / SCHEMA_RELATIVE
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))


def timestamp(value: datetime) -> str:
    """`YYYY-MM-DDTHH:MM:SS.mmmZ` (README 3.1)."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


_TARGET_RAW = re.compile(r"^[a-z][a-z0-9-]{0,15}\.[a-z][a-z0-9-]{0,63}\.raw$")


def build_record(
    *,
    action: str,
    phase: str,
    environment: str,
    actor: Actor,
    audit_id: str | None = None,
    now: datetime | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """A record in the `audit/v0` shape. Fields left `None` are omitted. Raises `ValueError` (no value in the
    text) if it does not validate or if `target_topic` is not `<environment>.<channel>.raw` (README 3 rule 5)."""
    record: dict[str, Any] = {
        "v": 0,
        "audit_id": audit_id or uuid7(),
        "ts": timestamp(now or datetime.now(UTC)),
        "environment": environment,
        "action": action,
        "phase": phase,
        "actor": actor.subject,
        "actor_roles": sorted(actor.roles),
    }
    if actor.request_id is not None:
        record["request_id"] = actor.request_id
    for name, value in fields.items():
        if value is None:
            continue
        record[name] = timestamp(value) if isinstance(value, datetime) else value
    if "target_topic" in record and record["target_topic"] != f"{environment}.{record.get('channel')}.raw":
        raise ValueError("target_topic is not <environment>.<channel>.raw")
    if _validator().is_valid(record) is False:
        raise ValueError("the audit record does not satisfy audit/v0")
    return record


def encode(record: dict[str, Any]) -> bytes:
    value = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if len(value) > MAX_RECORD_BYTES:
        raise ValueError("the audit record is larger than 8192 bytes")
    return value


class AuditHook:
    def __init__(
        self,
        sender: AuditSender,
        log: Logger,
        *,
        attempts: int = SEND_ATTEMPTS,
        retry_pause_s: float = RETRY_PAUSE_S,
    ) -> None:
        self._sender = sender
        self._log = log
        self._attempts = attempts
        self._pause_s = retry_pause_s

    async def _send(self, record: dict[str, Any]) -> None:
        """Serialize once, send the same bytes every attempt (same `audit_id`)."""
        key, value = record["actor"].encode("ascii"), encode(record)
        for attempt in range(1, self._attempts + 1):
            try:
                await self._sender.send(record["environment"], key, value)
                return
            except AuditSendError:
                if attempt == self._attempts:
                    raise
                await asyncio.sleep(self._pause_s)

    async def requested(self, *, action: str, environment: str, actor: Actor, **fields: Any) -> str:
        """Fail closed. Returns the `audit_id`. Raises `ApiProblem("AUDIT_UNAVAILABLE")` if the record is not
        acknowledged; the caller must then not perform the action."""
        record = build_record(action=action, phase="requested", environment=environment, actor=actor, **fields)
        try:
            await self._send(record)
        except AuditSendError:
            self._log.error("audit_requested_failed", action=action, environment=environment)
            raise ApiProblem("AUDIT_UNAVAILABLE") from None
        return str(record["audit_id"])

    async def followup(self, *, action: str, phase: str, environment: str, actor: Actor, **fields: Any) -> bool:
        """Best effort. True when acknowledged. Never raises for a send failure."""
        record = build_record(action=action, phase=phase, environment=environment, actor=actor, **fields)
        try:
            await self._send(record)
        except AuditSendError:
            self._log.warning("audit_followup_failed", action=action, phase=phase, environment=environment)
            return False
        return True


class KafkaAuditSender:
    """`acks=all`, idempotent (the runtime's `producer_config`), one record per call, bounded by `timeout_s`
    (the producer's `delivery.timeout.ms` equals it)."""

    def __init__(self, producer: Any, *, timeout_s: float) -> None:
        self._producer = producer
        self._timeout_s = timeout_s

    async def send(self, env: str, key: bytes, value: bytes) -> None:
        await asyncio.to_thread(self._produce_and_wait, platform_topic(env, "audit"), key, value)

    def _produce_and_wait(self, topic: str, key: bytes, value: bytes) -> None:
        done = threading.Event()
        errors: list[object] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)
            done.set()

        try:
            self._producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
        except Exception:
            raise AuditSendError from None
        # A monotonic deadline, not a count of polls: `poll` returns early when another thread's callback runs,
        # which would time out early and re-send a record the broker already has.
        deadline = time.monotonic() + self._timeout_s + 2.0
        while not done.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AuditSendError
            self._producer.poll(min(0.05, remaining))
        if errors:
            raise AuditSendError

    def close(self) -> None:
        self._producer.flush(5)
