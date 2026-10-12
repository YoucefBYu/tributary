"""The reconciler (W3-5): keeps the compacted `_tb.<env>.channels` topic equal to PostgreSQL's *published*
deployments, so a lost, failed-unknown or hand-edited record heals without an operator.

Desired state, per (channel, environment): the newest deployment record whose status is `published`.
A `deploy` or `rollback` means "the topic holds this version's stored bytes"; an `undeploy` (or no
published record at all) means "the topic holds a tombstone or nothing". Observed state: the last record
per key on the topic. Where they differ the reconciler publishes the desired record.

Rules this module holds, each tested:

* **Never publish what PostgreSQL does not record as published.** Only a published record's stored bytes
  (re-hashed against `spec_sha256`, fail closed) or a tombstone ever leaves here. A `pending` record is the
  deploy path's business: while it is younger than `pending_grace_s` the channel is skipped; an older one
  was orphaned by a crash and is marked `failed` / `PUBLISH_TIMEOUT` (outcome unknown, `openapi/v0` 3.4),
  after which the repair puts the last published record back, whatever the dead publish left on the topic.
* **Idempotent.** The pass is a pure function of PostgreSQL and the topic; a second pass over a converged
  state publishes nothing. Compaction makes the rare redundant repeat harmless.
* **Survives restarts.** No state is held between passes. Writers serialise on the channel row lock
  (`DeploymentRepository.lock_channel`), the same lock the deploy path takes, so replicas and the deploy
  path cannot interleave and the topic's last record per key is the newest request.
* **Modes.** `git-managed`: PostgreSQL is a mirror of Git, so a version whose `origin.kind` is not `git`
  is refused (defence in depth; the deploy path already refuses it) and orphan keys (on the topic, no
  deployment record here) are tombstoned. `ui-managed`: any origin may be published; orphans are reported
  (`orphan_channel_record`) and left alone, because a record someone published by hand may be running.

Nothing in the data plane calls or waits for this (invariant 3). Logs carry channel names and counts only.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tributary.runtime.topics import channels_topic
from tributary.sdk.context import Logger

from .db.errors import NotFoundError, RepositoryError
from .db.models import Deployment
from .db.repository import (
    ChannelRepository,
    DeploymentRepository,
    EnvironmentRepository,
    published_bytes,
)
from .publisher import ChannelsPublisher, PublishError

LOCK_TIMEOUT = "15s"
#: A `pending` record younger than this belongs to a live deploy (two transactions around one publish that is
#: bounded by `TB_PUBLISH_TIMEOUT_S`, at most 60 s, plus the lock wait).
DEFAULT_PENDING_GRACE_S = 120.0
DEFAULT_INTERVAL_S = 30.0
#: Metadata and watermark calls block in librdkafka and cannot be interrupted, so they get a short bound.
METADATA_TIMEOUT_S = 5.0


class TopicReadError(Exception):
    """The channels topic could not be read. A pass for that environment is skipped, never guessed."""


class TopicReader(Protocol):
    def snapshot(self, env: str) -> dict[str, bytes | None]:
        """The last record per key (`None` for a tombstone). Blocking; called off the event loop."""
        ...

    def stop(self) -> None:
        """Make a snapshot in progress (and any later one) end promptly with `TopicReadError`. Shutdown only."""
        ...


class KafkaTopicReader:
    """Reads every partition of `_tb.<env>.channels` from the beginning to the end offsets seen at the start.

    `assign`, not `subscribe`: a table read with no consumer group state (nothing to commit, nothing to
    rebalance), the same way workers read it (`runtime.channels`)."""

    def __init__(self, consumer_factory: Callable[[], Any], *, timeout_s: float = 30.0) -> None:
        self._factory = consumer_factory
        self._timeout_s = timeout_s
        self._stopped = threading.Event()

    def stop(self) -> None:
        """Checked between 0.5 s polls, so shutdown never waits out `timeout_s`. Permanent: not for reuse."""
        self._stopped.set()

    def snapshot(self, env: str) -> dict[str, bytes | None]:
        import time

        from confluent_kafka import OFFSET_BEGINNING, TopicPartition

        topic = channels_topic(env)
        consumer = self._factory()
        try:
            try:
                meta = consumer.list_topics(topic, timeout=min(self._timeout_s, METADATA_TIMEOUT_S)).topics.get(topic)
                if meta is None or meta.error is not None:
                    raise TopicReadError
                partitions = sorted(meta.partitions)
                ends: dict[int, int] = {}
                for p in partitions:
                    _low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic, p), timeout=min(self._timeout_s, METADATA_TIMEOUT_S), cached=False
                    )
                    ends[p] = high
            except TopicReadError:
                raise
            except Exception:
                raise TopicReadError from None
            state: dict[str, bytes | None] = {}
            pending = {p for p, high in ends.items() if high > 0}
            if not pending:
                return state
            if self._stopped.is_set():
                raise TopicReadError
            consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
            deadline = time.monotonic() + self._timeout_s
            while pending:
                if self._stopped.is_set() or time.monotonic() > deadline:
                    raise TopicReadError
                record = consumer.poll(0.5)
                if record is None:
                    continue
                if record.error() is not None:
                    continue  # partition EOF and transient errors: the offset check below decides
                part, offset = record.partition(), record.offset()
                key = record.key()
                if key is not None:
                    with contextlib.suppress(UnicodeDecodeError):  # not a channel name; workers reject it too
                        state[key.decode("utf-8")] = record.value()
                if part in pending and offset + 1 >= ends[part]:
                    pending.discard(part)
            return state
        finally:
            with contextlib.suppress(Exception):
                consumer.close()


@dataclass(slots=True)
class ReconcileResult:
    environment: str
    mode: str
    checked: int = 0
    repaired: list[str] = field(default_factory=list)
    tombstoned: list[str] = field(default_factory=list)  # orphans removed (git-managed)
    orphans: list[str] = field(default_factory=list)  # orphans reported only (ui-managed)
    in_flight: list[str] = field(default_factory=list)
    expired_pending: list[str] = field(default_factory=list)
    refused: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def converged(self) -> bool:
        return not (self.repaired or self.tombstoned or self.expired_pending or self.refused or self.errors)


_ABSENT: Any = object()


def _matches(desired: bytes | None, observed: Any) -> bool:
    if desired is None:  # a tombstone: an absent key and a tombstone are the same state
        return observed is _ABSENT or observed is None
    return observed is not _ABSENT and observed == desired


class Reconciler:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publisher: ChannelsPublisher,
        reader: TopicReader,
        log: Logger,
        *,
        pending_grace_s: float = DEFAULT_PENDING_GRACE_S,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = sessions
        self._publisher = publisher
        self._reader = reader
        self._log = log
        self._grace_s = pending_grace_s
        self._now = now

    async def run_once(self) -> list[ReconcileResult]:
        """One pass over every environment. A failure in one environment does not stop the others."""
        async with self._sessions() as s:
            envs = list(await EnvironmentRepository(s).list())
        results = []
        for env in envs:
            results.append(await self.reconcile_environment(env.name, env.mode, env.id))
        return results

    async def run_forever(self, interval_s: float) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # the loop must outlive a bad pass; the next one starts clean
                self._log.warning("reconcile_pass_failed", error_type=type(exc).__name__)
            await asyncio.sleep(interval_s)

    async def reconcile_environment(self, env_name: str, mode: str, env_id: Any) -> ReconcileResult:
        result = ReconcileResult(environment=env_name, mode=mode)
        try:
            # Safe before any lock: PostgreSQL is the truth and is re-read under the channel lock, the lock
            # serialises writers, and a stale snapshot costs at most one redundant publish that compaction absorbs.
            snapshot = await asyncio.to_thread(self._reader.snapshot, env_name)
        except asyncio.CancelledError:
            self._reader.stop()  # the thread cannot be cancelled; make it return within one poll slice
            raise
        except TopicReadError:
            result.errors.append("*topic*")
            self._log.warning("reconcile_topic_unreadable", environment=env_name)
            return result
        try:
            async with self._sessions() as s:
                names = await DeploymentRepository(s).channel_names_in_environment(env_id)
        except (RepositoryError, DBAPIError, OSError):
            result.errors.append("*database*")
            self._log.warning("reconcile_database_unavailable", environment=env_name)
            return result
        for name in names:
            result.checked += 1
            try:
                await self._reconcile_channel(env_name, mode, name, snapshot, result)
            except (RepositoryError, DBAPIError, OSError):
                result.errors.append(name)
                self._log.warning("reconcile_channel_failed", channel=name, environment=env_name)
        known = set(names)
        for key, value in sorted(snapshot.items()):
            if value is None or key in known:
                continue
            try:
                await self._orphan(env_name, mode, env_id, key, result)
            except (RepositoryError, DBAPIError, OSError):
                result.errors.append(key)
                self._log.warning("reconcile_channel_failed", channel=key, environment=env_name)
        self._log.info(
            "reconcile_pass",
            environment=env_name,
            mode=mode,
            checked=result.checked,
            repaired=len(result.repaired),
            tombstoned=len(result.tombstoned),
            orphans=len(result.orphans),
            in_flight=len(result.in_flight),
            expired_pending=len(result.expired_pending),
            refused=len(result.refused),
            errors=len(result.errors),
        )
        return result

    # -- one channel -----------------------------------------------------------------------------

    async def _desired(
        self, s: AsyncSession, drepo: DeploymentRepository, ch_id: Any, env_id: Any, mode: str
    ) -> tuple[str, bytes | None]:
        """`("ok", bytes | None)`, or `("refused", None)` when the record must not be published."""
        dep = await drepo.latest_published(ch_id, env_id)
        if dep is None or dep.kind == "undeploy" or dep.version_id is None:
            return "ok", None
        version = await ChannelRepository(s).version_by_id(dep.version_id)
        value = published_bytes(version.spec)
        if hashlib.sha256(value).hexdigest() != version.spec_sha256:
            return "refused", None  # the stored JSON no longer hashes to its recorded digest: fail closed
        if mode == "git-managed" and version.origin.get("kind") != "git":
            return "refused", None
        return "ok", value

    async def _reconcile_channel(
        self, env_name: str, mode: str, name: str, snapshot: dict[str, bytes | None], result: ReconcileResult
    ) -> None:
        observed = snapshot.get(name, _ABSENT)

        # Unlocked look first: a converged channel costs no row lock.
        async with self._sessions() as s:
            crepo, drepo = ChannelRepository(s), DeploymentRepository(s)
            env = await EnvironmentRepository(s).get_by_name(env_name)
            ch = await crepo.get_by_name(name)
            pending = await drepo.pending(ch.id, env.id)
            state, desired = await self._desired(s, drepo, ch.id, env.id, mode)
        if pending is None and state == "ok" and _matches(desired, observed):
            return

        async with self._sessions() as s, s.begin():
            await s.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
            drepo = DeploymentRepository(s)
            ch = await drepo.lock_channel(name)
            env = await EnvironmentRepository(s).get_by_name(env_name)
            pending = await drepo.pending(ch.id, env.id)
            if pending is not None:
                if (self._now() - pending.created_at).total_seconds() < self._grace_s:
                    result.in_flight.append(name)
                    return
                await self._expire(drepo, pending)
                result.expired_pending.append(name)
                self._log.warning("reconcile_pending_expired", channel=name, environment=env_name)
            state, desired = await self._desired(s, drepo, ch.id, env.id, mode)
            if state != "ok":
                result.refused.append(name)
                self._log.error("reconcile_refused", channel=name, environment=env_name, mode=mode)
                return
            if _matches(desired, observed):
                return  # already converged (an expired record that never reached the topic leaves it so)
            try:
                await self._publisher.publish(env_name, name, desired)
            except PublishError as exc:
                result.errors.append(name)
                self._log.warning("reconcile_publish_failed", channel=name, environment=env_name, code=exc.code)
                return
            result.repaired.append(name)
            self._log.info("reconcile_repaired", channel=name, environment=env_name, tombstone=desired is None)

    async def _expire(self, drepo: DeploymentRepository, dep: Deployment) -> None:
        await drepo.mark_failed(dep, "PUBLISH_TIMEOUT")

    # -- keys with no deployment record ----------------------------------------------------------

    async def _orphan(self, env_name: str, mode: str, env_id: Any, key: str, result: ReconcileResult) -> None:
        if mode != "git-managed":
            result.orphans.append(key)
            self._log.warning("orphan_channel_record", channel=key, environment=env_name)
            return
        async with self._sessions() as s, s.begin():
            await s.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
            drepo = DeploymentRepository(s)
            try:
                ch = await ChannelRepository(s).get_by_name(key, for_update=True)
            except NotFoundError:
                ch = None  # no such channel: nothing can be deployed under this name concurrently
            if ch is not None and await drepo.has_records(ch.id, env_id):
                return  # a deploy arrived since the name list was read
            try:
                await self._publisher.publish(env_name, key, None)
            except PublishError as exc:
                result.errors.append(key)
                self._log.warning("reconcile_publish_failed", channel=key, environment=env_name, code=exc.code)
                return
            result.tombstoned.append(key)
            self._log.info("reconcile_orphan_removed", channel=key, environment=env_name)
