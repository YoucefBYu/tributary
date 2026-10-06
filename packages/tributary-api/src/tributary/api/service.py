"""Use cases behind the routes. Authorization is NOT here (it lives in `auth.request`); the transaction
boundary is: every method opens its own transaction, and deploys release it around the Kafka publish
only in the sense described on `_submit`."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tributary.sdk.context import Logger

from . import render
from .db.errors import (
    AlreadyExistsError,
    InvalidValueError,
    NotFoundError,
    RepositoryError,
)
from .db.models import ChannelVersion
from .db.repository import (
    ChannelRepository,
    DeploymentRepository,
    EnvironmentRepository,
    published_bytes,
)
from .problems import ApiProblem
from .publisher import ChannelsPublisher, PublishError

LOCK_TIMEOUT = "15s"
#: Deploys in flight at once. Each holds one pooled database connection across its Kafka publish, so the cap
#: stays below the pool size (5 + 10 overflow); a request beyond it waits `INFLIGHT_WAIT_S`, then is 503.
MAX_INFLIGHT_DEPLOYS = 4
INFLIGHT_WAIT_S = 5.0
_ABSENT = object()


def encode_cursor(value: str | int) -> str:
    return base64.urlsafe_b64encode(f"v1:{value}".encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> str | None:
    if cursor is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        raise ApiProblem("REQUEST_INVALID") from None
    if not raw.startswith("v1:") or len(raw) == 3:
        raise ApiProblem("REQUEST_INVALID")
    return raw[3:]


def _int_cursor(cursor: str | None) -> int | None:
    raw = decode_cursor(cursor)
    if raw is None:
        return None
    if not raw.isdigit() or len(raw) > 18:
        raise ApiProblem("REQUEST_INVALID")
    return int(raw)


def _problem(exc: Exception) -> ApiProblem:
    """Map repository and database errors to fixed problems; the exception text never leaves."""
    if isinstance(exc, NotFoundError):
        return ApiProblem("NOT_FOUND")
    if isinstance(exc, AlreadyExistsError):
        return ApiProblem("CHANNEL_EXISTS")
    if isinstance(exc, InvalidValueError):
        return ApiProblem("REQUEST_INVALID", status=422)
    return ApiProblem("UNAVAILABLE")


class ControlPlane:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publisher: ChannelsPublisher,
        log: Logger,
        *,
        max_inflight: int = MAX_INFLIGHT_DEPLOYS,
        inflight_wait_s: float = INFLIGHT_WAIT_S,
    ) -> None:
        self._inflight = asyncio.Semaphore(max_inflight)
        self._inflight_wait_s = inflight_wait_s
        self._sessions = sessions
        self._publisher = publisher
        self._log = log

    async def _run[T](self, work: Callable[[AsyncSession], Awaitable[T]], *, locked: bool = False) -> T:
        try:
            async with self._sessions() as s, s.begin():
                if locked:
                    await s.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
                return await work(s)
        except ApiProblem:
            raise
        except (RepositoryError, DBAPIError) as exc:
            raise _problem(exc) from None

    # -- reads -----------------------------------------------------------------------------------

    async def ping_database(self) -> None:
        async with self._sessions() as s:
            await s.execute(text("SELECT 1"))

    async def environments(self) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            return {"items": [render.environment(e) for e in await EnvironmentRepository(s).list()]}

        return await self._run(work)

    async def list_channels(self, limit: int, cursor: str | None) -> dict[str, Any]:
        after = decode_cursor(cursor)

        async def work(s: AsyncSession) -> dict[str, Any]:
            repo = ChannelRepository(s)
            rows = list(await repo.list(limit=limit + 1, after_name=after))
            page = rows[:limit]
            latest = await repo.latest_versions([c.id for c in page])
            items = [
                {"name": c.name, "latest_version": latest[c.id].version, "created_at": render.ts(c.created_at)}
                for c in page
                if c.id in latest
            ]
            more = len(rows) > limit
            return {"items": items, "next_cursor": encode_cursor(page[-1].name) if more and page else None}

        return await self._run(work)

    async def get_channel(self, name: str) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            repo = ChannelRepository(s)
            ch = await repo.get_by_name(name)
            latest = (await repo.latest_versions([ch.id])).get(ch.id)
            if latest is None:
                raise ApiProblem("NOT_FOUND")
            drepo = DeploymentRepository(s)
            states = list(await drepo.states_for_channel(ch))
            numbers = await drepo.version_numbers(d for _, d in states)
            return render.channel_detail(ch, latest, states, numbers)

        return await self._run(work)

    async def list_versions(self, name: str, limit: int, cursor: str | None) -> dict[str, Any]:
        before = _int_cursor(cursor)

        async def work(s: AsyncSession) -> dict[str, Any]:
            rows = list(await ChannelRepository(s).list_versions(name, limit=limit + 1, before_version=before))
            page = rows[:limit]
            more = len(rows) > limit
            return {
                "items": [render.version_summary(name, v) for v in page],
                "next_cursor": encode_cursor(page[-1].version) if more and page else None,
            }

        return await self._run(work)

    async def get_version(self, name: str, version: int) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            return render.version_full(name, await ChannelRepository(s).get_version(name, version))

        return await self._run(work)

    async def current_deployments(self, env_name: str, limit: int, cursor: str | None) -> dict[str, Any]:
        after = decode_cursor(cursor)

        async def work(s: AsyncSession) -> dict[str, Any]:
            env = await EnvironmentRepository(s).get_by_name(env_name)
            drepo = DeploymentRepository(s)
            rows = list(await drepo.current_in_environment(env, limit=limit + 1, after_channel=after))
            page = rows[:limit]
            numbers = await drepo.version_numbers(d for _, d in page)
            more = len(rows) > limit
            return {
                "items": [render.deployment(d, name, env_name, numbers) for name, d in page],
                "next_cursor": encode_cursor(page[-1][0]) if more and page else None,
            }

        return await self._run(work)

    async def channel_deployments(self, env_name: str, channel: str, limit: int, cursor: str | None) -> dict[str, Any]:
        before = _int_cursor(cursor)

        async def work(s: AsyncSession) -> dict[str, Any]:
            ch = await ChannelRepository(s).get_by_name(channel)
            env = await EnvironmentRepository(s).get_by_name(env_name)
            drepo = DeploymentRepository(s)
            rows = list(await drepo.history(ch, env, limit=limit + 1, before_seq=before))
            page = rows[:limit]
            numbers = await drepo.version_numbers(page)
            more = len(rows) > limit
            return {
                "items": [render.deployment(d, channel, env_name, numbers) for d in page],
                "next_cursor": encode_cursor(page[-1].seq) if more and page else None,
            }

        return await self._run(work)

    # -- channel writes --------------------------------------------------------------------------

    async def create_channel(
        self, name: str, document: dict[str, Any], origin: dict[str, Any], by: str
    ) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            repo = ChannelRepository(s)
            await repo.create(name, by)
            return render.version_full(name, await repo.add_version(name, document, by, origin))

        return await self._run(work)

    async def add_version(self, name: str, document: dict[str, Any], origin: dict[str, Any], by: str) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            return render.version_full(name, await ChannelRepository(s).add_version(name, document, by, origin))

        return await self._run(work)

    # -- deployments -----------------------------------------------------------------------------

    async def deploy(self, env: str, channel: str, version: int, expected: Any, by: str) -> dict[str, Any]:
        return await self._submit("deploy", env, channel, by, target=version, expected=expected)

    async def rollback(self, env: str, channel: str, expected: int, to_version: int | None, by: str) -> dict[str, Any]:
        return await self._submit("rollback", env, channel, by, target=to_version, expected=expected)

    async def undeploy(self, env: str, channel: str, expected: int, by: str) -> dict[str, Any]:
        return await self._submit("undeploy", env, channel, by, target=None, expected=expected)

    async def _submit(
        self, kind: str, env_name: str, channel: str, by: str, *, target: int | None, expected: Any
    ) -> dict[str, Any]:
        """Two transactions. (1) Under the channel row lock: check, then commit a `pending` record, so a crash
        after the publish leaves a visible record for the reconciler (W3-5). (2) Under the same lock, re-read
        the record: if a newer request superseded it, do not publish; otherwise publish, bounded by the
        publish timeout, and commit `published` or `failed`. The lock serialises publishes per channel across
        API replicas, so the topic's last record is the newest request. `expected` is `_ABSENT` (deploy only),
        or the caller's expected current version (None means "not deployed").

        `failed` with `PUBLISH_TIMEOUT` means UNKNOWN, possibly delivered: the broker may still have committed
        the record after the timeout. The repair is to deploy the same version again (README 3.5), which
        republishes; a compacted topic makes the repeat harmless."""
        try:
            await asyncio.wait_for(self._inflight.acquire(), timeout=self._inflight_wait_s)
        except TimeoutError:
            raise ApiProblem("UNAVAILABLE") from None
        try:
            return await self._submit_locked(kind, env_name, channel, by, target=target, expected=expected)
        finally:
            self._inflight.release()

    async def _submit_locked(
        self, kind: str, env_name: str, channel: str, by: str, *, target: int | None, expected: Any
    ) -> dict[str, Any]:

        async def request(s: AsyncSession) -> Any:
            crepo, drepo = ChannelRepository(s), DeploymentRepository(s)
            ch = await drepo.lock_channel(channel)
            env = await EnvironmentRepository(s).get_by_name(env_name)
            current = await drepo.current(ch.id, env.id)
            cur_number: int | None = None
            if current is not None and current.version_id is not None:
                cur_number = (await drepo.version_numbers([current])).get(current.version_id)
            deployed = current is not None and current.kind != "undeploy"
            version: ChannelVersion | None = None
            if expected is not _ABSENT and expected != cur_number:
                if kind != "deploy" and not deployed:
                    raise ApiProblem("NOT_DEPLOYED")
                raise ApiProblem("CURRENT_VERSION_MISMATCH")
            if kind in ("rollback", "undeploy") and not deployed:
                raise ApiProblem("NOT_DEPLOYED")
            if kind == "deploy" and target is not None:
                version = await crepo.get_version(channel, target)
            elif kind == "rollback" and current is not None:
                if target is not None:
                    if target == cur_number:
                        raise ApiProblem("REQUEST_INVALID", status=422)
                    version = await crepo.get_version(channel, target)
                else:
                    version = await drepo.previous_published_version(current)
                    if version is None:
                        raise ApiProblem("NOTHING_TO_ROLL_BACK")
            if version is not None and env.mode == "git-managed" and version.origin.get("kind") != "git":
                raise ApiProblem("MANAGEMENT_MODE")
            dep = await drepo.request(ch, env, kind, version, cur_number, by)
            return dep.id

        dep_id = await self._run(request, locked=True)

        async def publish(s: AsyncSession) -> dict[str, Any]:
            drepo = DeploymentRepository(s)
            await drepo.lock_channel(channel)
            dep = await drepo.get(dep_id, for_update=True)
            version: ChannelVersion | None = None
            if dep.version_id is not None:
                version = await ChannelRepository(s).version_by_id(dep.version_id)
            if dep.status == "pending":
                code = await self._publish(env_name, channel, dep.kind, version)
                if code is None:
                    await drepo.mark_published(dep)
                else:
                    await drepo.mark_failed(dep, code)
            numbers = await drepo.version_numbers([dep])
            return render.deployment(dep, channel, env_name, numbers)

        body = await self._run(publish, locked=True)
        self._log.info("deployment_finished", kind=kind, status=body["status"], channel=channel, environment=env_name)
        return body

    async def _publish(self, env: str, channel: str, kind: str, version: ChannelVersion | None) -> str | None:
        value: bytes | None = None
        if kind != "undeploy":
            if version is None:
                return "PUBLISH_FAILED"
            value = published_bytes(version.spec)
            if hashlib.sha256(value).hexdigest() != version.spec_sha256:
                # Stored JSON no longer hashes to the recorded digest: fail closed rather than publish bytes
                # the record does not describe.
                self._log.error("spec_digest_mismatch", channel=channel)
                return "PUBLISH_FAILED"
        try:
            await self._publisher.publish(env, channel, value)
        except PublishError as exc:
            self._log.warning("publish_failed", channel=channel, environment=env, code=exc.code)
            return exc.code
        return None
