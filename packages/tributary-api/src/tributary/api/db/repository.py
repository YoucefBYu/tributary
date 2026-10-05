"""Repository layer for channels, versions, deployments. Callers own the transaction boundary
(`async with session.begin()`); methods flush but never commit. Authorization is NOT here: it lives
in one separate layer (W3-4b). `created_by` is an opaque principal id, never a name or PHI."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from tributary.api.db.errors import (
    AlreadyExistsError,
    InUseError,
    InvalidValueError,
    NotFoundError,
    NothingToRollBackError,
    RepositoryError,
    TransientError,
)
from tributary.api.db.models import DIGEST_PATTERN, Channel, ChannelVersion, Deployment, Environment

_DIGEST_RE = re.compile(DIGEST_PATTERN)
_MAX_NAME_IN_MESSAGE = 64


def _n(name: str) -> str:
    """Bounded, repr'd name for error messages (names are user input)."""
    return repr(name[:_MAX_NAME_IN_MESSAGE])


def _classify(exc: DBAPIError, what: str) -> RepositoryError:
    """Map a constraint violation to a typed error by SQLSTATE. The raw IntegrityError is deliberately
    not chained by callers (`from None`): its text carries bound parameters and DETAIL values."""
    code = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
    if code in ("40001", "40P01") or (isinstance(code, str) and code.startswith("08")):
        return TransientError(f"{what} failed transiently; retry the transaction")
    if code == "23505":
        return AlreadyExistsError(f"{what} already exists or conflicts with a concurrent write")
    if code == "23514" or (isinstance(code, str) and code.startswith("22")):  # CHECK, or bad/oversized data
        return InvalidValueError(f"{what} rejected: a value violates a constraint")
    if code == "23503":
        return InUseError(f"{what} rejected: referenced rows exist or a reference is missing")
    return RepositoryError(f"{what} rejected by the database")


def spec_digest(spec: dict[str, Any]) -> str:
    """SHA-256 of the canonical JSON form (sorted keys, no whitespace). NaN and Infinity are refused (ValueError)."""
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _write_guarded(session: AsyncSession, *, add: object | None = None, delete: object | None = None) -> None:
    """Add or delete one row inside a SAVEPOINT so a constraint violation leaves the caller's transaction
    usable. The change must be made inside the savepoint: `begin_nested()` autoflushes on entry, so a
    pending object added before it would fail outside the savepoint and poison the outer transaction."""
    async with session.begin_nested():
        if add is not None:
            session.add(add)
        if delete is not None:
            await session.delete(delete)
        await session.flush()


class EnvironmentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, name: str, mode: str) -> Environment:
        env = Environment(name=name, mode=mode)
        try:
            await _write_guarded(self._s, add=env)
        except DBAPIError as exc:
            raise _classify(exc, f"environment {_n(name)}") from None
        return env

    async def get_by_name(self, name: str) -> Environment:
        env = (await self._s.execute(select(Environment).where(Environment.name == name))).scalar_one_or_none()
        if env is None:
            raise NotFoundError(f"environment {_n(name)} not found")
        return env

    async def list(self) -> Sequence[Environment]:
        return (await self._s.execute(select(Environment).order_by(Environment.name))).scalars().all()


class ChannelRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, name: str, created_by: str) -> Channel:
        ch = Channel(name=name, created_by=created_by)
        try:
            await _write_guarded(self._s, add=ch)
        except DBAPIError as exc:
            raise _classify(exc, f"channel {_n(name)}") from None
        return ch

    async def get_by_name(self, name: str, *, for_update: bool = False) -> Channel:
        stmt = select(Channel).where(Channel.name == name)
        if for_update:
            stmt = stmt.with_for_update()
        ch = (await self._s.execute(stmt)).scalar_one_or_none()
        if ch is None:
            raise NotFoundError(f"channel {_n(name)} not found")
        return ch

    async def list(self, *, limit: int = 100, after_name: str | None = None) -> Sequence[Channel]:
        stmt = select(Channel).order_by(Channel.name).limit(limit)
        if after_name is not None:
            stmt = stmt.where(Channel.name > after_name)
        return (await self._s.execute(stmt)).scalars().all()

    async def delete(self, name: str) -> None:
        """Delete a channel and, by cascade, its versions. Refused with InUseError while any deployment
        references the channel."""
        ch = await self.get_by_name(name)
        try:
            await _write_guarded(self._s, delete=ch)
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "23503":
                raise InUseError(f"channel {_n(name)} still has deployments") from None
            raise _classify(exc, f"channel {_n(name)} delete") from None

    async def add_version(self, name: str, spec: dict[str, Any], created_by: str) -> ChannelVersion:
        """Append the next version. Serialised per channel by a row lock, so concurrent writers get
        distinct consecutive numbers rather than a unique-violation."""
        ch = await self.get_by_name(name, for_update=True)
        current = (
            await self._s.execute(
                select(ChannelVersion.version)
                .where(ChannelVersion.channel_id == ch.id)
                .order_by(ChannelVersion.version.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        ver = ChannelVersion(
            channel_id=ch.id,
            version=(current or 0) + 1,
            spec=spec,
            spec_sha256=spec_digest(spec),
            created_by=created_by,
        )
        try:
            await _write_guarded(self._s, add=ver)
        except DBAPIError as exc:
            raise _classify(exc, f"channel {_n(name)} version") from None
        return ver

    async def get_version(self, name: str, version: int) -> ChannelVersion:
        ch = await self.get_by_name(name)
        ver = (
            await self._s.execute(
                select(ChannelVersion).where(ChannelVersion.channel_id == ch.id, ChannelVersion.version == version)
            )
        ).scalar_one_or_none()
        if ver is None:
            raise NotFoundError(f"channel {_n(name)} version {version} not found")
        return ver

    async def list_versions(self, name: str) -> Sequence[ChannelVersion]:
        ch = await self.get_by_name(name)
        stmt = select(ChannelVersion).where(ChannelVersion.channel_id == ch.id).order_by(ChannelVersion.version.desc())
        return (await self._s.execute(stmt)).scalars().all()


class DeploymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def _lock_channel(self, name: str) -> Channel:
        return await ChannelRepository(self._s).get_by_name(name, for_update=True)

    async def active(self, channel_id: uuid.UUID, environment_id: uuid.UUID) -> Deployment | None:
        stmt = select(Deployment).where(
            Deployment.channel_id == channel_id,
            Deployment.environment_id == environment_id,
            Deployment.status == "active",
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def deploy(
        self, channel_name: str, version: int, environment_name: str, image_digest: str, created_by: str
    ) -> Deployment:
        """Make `version` active in the environment; the previous active deployment becomes `superseded`.
        `image_digest` must be `sha256:<64 hex>` (a tag is refused by the CHECK constraint)."""
        ch = await self._lock_channel(channel_name)
        env = await EnvironmentRepository(self._s).get_by_name(environment_name)
        ver = await ChannelRepository(self._s).get_version(channel_name, version)
        previous = await self.active(ch.id, env.id)
        return await self._insert(ch, env, ver.id, image_digest, created_by, flip=(previous, "superseded"))

    async def rollback(self, channel_name: str, environment_name: str, created_by: str) -> Deployment:
        """Re-activate the most recent earlier deployment whose version or image differs from the current
        one (a new `active` row with `restored_from` = that deployment). The current deployment becomes
        `rolled_back`; history is append-only."""
        ch = await self._lock_channel(channel_name)
        env = await EnvironmentRepository(self._s).get_by_name(environment_name)
        current = await self.active(ch.id, env.id)
        if current is None:
            raise NothingToRollBackError(
                f"channel {_n(channel_name)} has no active deployment in {_n(environment_name)}"
            )
        # A rollback copy sits at a new seq, but the next rollback must keep walking back from the
        # deployment it restored, not bounce between the two most recent ones.
        limit = current.seq
        if current.restored_from is not None:
            limit = (await self._s.get_one(Deployment, current.restored_from)).seq
        stmt = (
            select(Deployment)
            .where(
                Deployment.channel_id == ch.id,
                Deployment.environment_id == env.id,
                Deployment.seq < limit,
                Deployment.status == "superseded",
                (Deployment.version_id != current.version_id) | (Deployment.image_digest != current.image_digest),
            )
            .order_by(Deployment.seq.desc())
            .limit(1)
        )
        target = (await self._s.execute(stmt)).scalar_one_or_none()
        if target is None:
            raise NothingToRollBackError(
                f"channel {_n(channel_name)} has no earlier deployment in {_n(environment_name)}"
            )
        return await self._insert(
            ch,
            env,
            target.version_id,
            target.image_digest,
            created_by,
            flip=(current, "rolled_back"),
            restored_from=target.id,
        )

    async def history(self, channel_name: str, environment_name: str | None = None) -> Sequence[Deployment]:
        ch = await ChannelRepository(self._s).get_by_name(channel_name)
        stmt = select(Deployment).where(Deployment.channel_id == ch.id).order_by(Deployment.seq.desc())
        if environment_name is not None:
            env = await EnvironmentRepository(self._s).get_by_name(environment_name)
            stmt = stmt.where(Deployment.environment_id == env.id)
        return (await self._s.execute(stmt)).scalars().all()

    async def _insert(
        self,
        ch: Channel,
        env: Environment,
        version_id: uuid.UUID,
        image_digest: str,
        created_by: str,
        *,
        flip: tuple[Deployment | None, str],
        restored_from: uuid.UUID | None = None,
    ) -> Deployment:
        """Retire the old active row and insert the new one in ONE savepoint: a rejected insert must not
        leave the environment with no active deployment."""
        if not _DIGEST_RE.fullmatch(image_digest):
            raise InvalidValueError("image_digest must be sha256:<64 lowercase hex>")
        dep = Deployment(
            channel_id=ch.id,
            environment_id=env.id,
            version_id=version_id,
            image_digest=image_digest,
            status="active",
            restored_from=restored_from,
            created_by=created_by,
        )
        old, new_status = flip
        try:
            async with self._s.begin_nested():
                if old is not None:
                    old.status = new_status
                    await self._s.flush()  # free the partial unique index before the insert
                self._s.add(dep)
                await self._s.flush()
        except DBAPIError as exc:
            raise _classify(exc, "deployment") from None
        return dep
