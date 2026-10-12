"""Repository layer for channels, versions, deployments. Callers own the transaction boundary
(`async with session.begin()`); methods flush but never commit. Authorization is NOT here: it lives
in one separate layer (W3-4b). `created_by` is an opaque principal id, never a name or PHI."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from tributary.api.db.errors import (
    AlreadyExistsError,
    InUseError,
    InvalidValueError,
    NotFoundError,
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


def published_bytes(spec: dict[str, Any]) -> bytes:
    """The exact bytes written to `_tb.<env>.channels` for a stored document: canonical JSON (sorted keys, no
    whitespace, UTF-8). `spec_sha256` is the SHA-256 of these bytes (`openapi/v0` README 3.2). NaN and
    Infinity are refused (ValueError)."""
    return json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def spec_digest(spec: dict[str, Any]) -> str:
    return hashlib.sha256(published_bytes(spec)).hexdigest()


def image_digest_of(spec: dict[str, Any]) -> str | None:
    """`sha256:<hex>` from `spec.pipeline.bundle` (`name@sha256:<hex>`, pinned by the channel schema)."""
    bundle = (spec.get("spec") or {}).get("pipeline", {}).get("bundle")
    if not isinstance(bundle, str) or "@" not in bundle:
        return None
    digest = bundle.rsplit("@", 1)[1]
    return digest if _DIGEST_RE.fullmatch(digest) else None


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

    async def add_version(
        self, name: str, spec: dict[str, Any], created_by: str, origin: dict[str, Any] | None = None
    ) -> ChannelVersion:
        """Append the next version. Serialised per channel by a row lock, so concurrent writers get
        distinct consecutive numbers rather than a unique-violation. The stored document is a copy of `spec`
        with `metadata.version` set to the new number, and `spec_sha256` hashes the stored document's
        published bytes (`openapi/v0` README 3.2)."""
        ch = await self.get_by_name(name, for_update=True)
        current = (
            await self._s.execute(
                select(ChannelVersion.version)
                .where(ChannelVersion.channel_id == ch.id)
                .order_by(ChannelVersion.version.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        number = (current or 0) + 1
        stored = {**spec, "metadata": {**spec.get("metadata", {}), "version": number}}
        ver = ChannelVersion(
            channel_id=ch.id,
            version=number,
            spec=stored,
            spec_sha256=spec_digest(stored),
            origin=origin if origin is not None else {"kind": "api"},
            created_by=created_by,
        )
        try:
            await _write_guarded(self._s, add=ver)
        except DBAPIError as exc:
            raise _classify(exc, f"channel {_n(name)} version") from None
        return ver

    async def next_version(self, name: str) -> int:
        """The number `add_version` would assign now (404 if the channel is missing). A read, not a reservation:
        the writer re-checks under the row lock."""
        ch = await self.get_by_name(name)
        current = (
            await self._s.execute(select(func.max(ChannelVersion.version)).where(ChannelVersion.channel_id == ch.id))
        ).scalar_one_or_none()
        return int(current or 0) + 1

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

    async def list_versions(
        self, name: str, *, limit: int | None = None, before_version: int | None = None
    ) -> Sequence[ChannelVersion]:
        """Newest first. `before_version` is the keyset cursor: only versions below it."""
        ch = await self.get_by_name(name)
        stmt = select(ChannelVersion).where(ChannelVersion.channel_id == ch.id).order_by(ChannelVersion.version.desc())
        if before_version is not None:
            stmt = stmt.where(ChannelVersion.version < before_version)
        if limit is not None:
            stmt = stmt.limit(limit)
        return (await self._s.execute(stmt)).scalars().all()

    async def latest_versions(self, channel_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, ChannelVersion]:
        """The newest version row of each channel (one query)."""
        if not channel_ids:
            return {}
        newest = (
            select(ChannelVersion.channel_id, func.max(ChannelVersion.version).label("v"))
            .where(ChannelVersion.channel_id.in_(channel_ids))
            .group_by(ChannelVersion.channel_id)
            .subquery()
        )
        stmt = select(ChannelVersion).join(
            newest, (ChannelVersion.channel_id == newest.c.channel_id) & (ChannelVersion.version == newest.c.v)
        )
        return {row.channel_id: row for row in (await self._s.execute(stmt)).scalars().all()}

    async def version_by_id(self, version_id: uuid.UUID) -> ChannelVersion:
        return await self._s.get_one(ChannelVersion, version_id)


_LIVE = ("pending", "published")


class DeploymentRepository:
    """Deployment request records. The caller holds the channel row lock (`lock_channel`) across the
    read-check-insert so two operators on one channel are serialised, which is what makes
    `expected_current_version` an honest guard."""

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def lock_channel(self, name: str) -> Channel:
        return await ChannelRepository(self._s).get_by_name(name, for_update=True)

    async def current(self, channel_id: uuid.UUID, environment_id: uuid.UUID) -> Deployment | None:
        """The effective record: the newest `pending` or `published` one (what was asked for, and what the
        channels topic holds or is about to). A newest record of kind `undeploy` means "not deployed"."""
        stmt = (
            select(Deployment)
            .where(
                Deployment.channel_id == channel_id,
                Deployment.environment_id == environment_id,
                Deployment.status.in_(_LIVE),
            )
            .order_by(Deployment.seq.desc())
            .limit(1)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def latest_published(self, channel_id: uuid.UUID, environment_id: uuid.UUID) -> Deployment | None:
        """The newest record whose publish was acknowledged: what `_tb.<env>.channels` must hold (W3-5)."""
        stmt = (
            select(Deployment)
            .where(
                Deployment.channel_id == channel_id,
                Deployment.environment_id == environment_id,
                Deployment.status == "published",
            )
            .order_by(Deployment.seq.desc())
            .limit(1)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def pending(self, channel_id: uuid.UUID, environment_id: uuid.UUID) -> Deployment | None:
        stmt = select(Deployment).where(
            Deployment.channel_id == channel_id,
            Deployment.environment_id == environment_id,
            Deployment.status == "pending",
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def channel_names_in_environment(self, environment_id: uuid.UUID) -> list[str]:
        """Every channel with at least one deployment record in the environment, by name."""
        stmt = (
            select(Channel.name)
            .join(Deployment, Deployment.channel_id == Channel.id)
            .where(Deployment.environment_id == environment_id)
            .distinct()
            .order_by(Channel.name)
        )
        return list((await self._s.execute(stmt)).scalars().all())

    async def has_records(self, channel_id: uuid.UUID, environment_id: uuid.UUID) -> bool:
        stmt = (
            select(Deployment.id)
            .where(Deployment.channel_id == channel_id, Deployment.environment_id == environment_id)
            .limit(1)
        )
        return (await self._s.execute(stmt)).first() is not None

    async def previous_published_version(self, current: Deployment) -> ChannelVersion | None:
        """The most recent version published before `current` that differs from it: the default rollback
        target (`openapi/v0` RollbackRequest.to_version)."""
        if current.version_id is None:
            return None
        stmt = (
            select(ChannelVersion)
            .join(Deployment, Deployment.version_id == ChannelVersion.id)
            .where(
                Deployment.channel_id == current.channel_id,
                Deployment.environment_id == current.environment_id,
                Deployment.status == "published",
                Deployment.seq < current.seq,
                Deployment.version_id != current.version_id,
            )
            .order_by(Deployment.seq.desc())
            .limit(1)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def request(
        self,
        ch: Channel,
        env: Environment,
        kind: str,
        version: ChannelVersion | None,
        previous_version: int | None,
        created_by: str,
        *,
        restored_from: uuid.UUID | None = None,
        dep_id: uuid.UUID | None = None,
    ) -> Deployment:
        """Insert a `pending` record and supersede any older pending one, in one savepoint. `image_digest` is
        copied from the version's `pipeline.bundle`; a deploy whose document has none is refused."""
        image: str | None = None
        if kind != "undeploy" and (version is None or (image := image_digest_of(version.spec)) is None):
            raise InvalidValueError("the document has no digest-pinned pipeline.bundle")
        dep = Deployment(
            id=dep_id or uuid.uuid4(),
            channel_id=ch.id,
            environment_id=env.id,
            kind=kind,
            version_id=None if version is None else version.id,
            image_digest=image,
            previous_version=previous_version,
            status="pending",
            restored_from=restored_from,
            created_by=created_by,
        )
        try:
            async with self._s.begin_nested():
                await self._s.execute(
                    update(Deployment)
                    .where(
                        Deployment.channel_id == ch.id,
                        Deployment.environment_id == env.id,
                        Deployment.status == "pending",
                    )
                    .values(status="superseded")
                )
                self._s.add(dep)
                await self._s.flush()
        except DBAPIError as exc:
            raise _classify(exc, "deployment") from None
        return dep

    async def get(self, deployment_id: uuid.UUID, *, for_update: bool = False) -> Deployment:
        stmt = select(Deployment).where(Deployment.id == deployment_id)
        if for_update:
            stmt = stmt.with_for_update().execution_options(populate_existing=True)
        dep = (await self._s.execute(stmt)).scalar_one_or_none()
        if dep is None:
            raise NotFoundError("deployment not found")
        return dep

    async def mark_published(self, dep: Deployment) -> None:
        """`pending` -> `published`. Anything else raises InvalidValueError (a superseded record never publishes)."""
        if dep.status != "pending":
            raise InvalidValueError(f"deployment is {dep.status}, not pending")
        dep.status = "published"
        dep.published_at = datetime.now(UTC)
        await self._s.flush()

    async def mark_failed(self, dep: Deployment, failure_code: str) -> None:
        if dep.status != "pending":
            raise InvalidValueError(f"deployment is {dep.status}, not pending")
        dep.status = "failed"
        dep.failure_code = failure_code
        await self._s.flush()

    async def history(
        self,
        ch: Channel,
        env: Environment,
        *,
        limit: int,
        before_seq: int | None = None,
    ) -> Sequence[Deployment]:
        """Newest first; `before_seq` is the keyset cursor."""
        stmt = (
            select(Deployment)
            .where(Deployment.channel_id == ch.id, Deployment.environment_id == env.id)
            .order_by(Deployment.seq.desc())
            .limit(limit)
        )
        if before_seq is not None:
            stmt = stmt.where(Deployment.seq < before_seq)
        return (await self._s.execute(stmt)).scalars().all()

    async def current_in_environment(
        self, env: Environment, *, limit: int, after_channel: str | None = None
    ) -> Sequence[tuple[str, Deployment]]:
        """The effective record of every channel currently deployed in `env`, by channel name (undeployed
        channels are left out)."""
        newest = (
            select(func.max(Deployment.seq).label("s"))
            .where(Deployment.environment_id == env.id, Deployment.status.in_(_LIVE))
            .group_by(Deployment.channel_id)
            .subquery()
        )
        stmt = (
            select(Channel.name, Deployment)
            .join(Deployment, Deployment.channel_id == Channel.id)
            .where(Deployment.seq.in_(select(newest.c.s)), Deployment.kind != "undeploy")
            .order_by(Channel.name)
            .limit(limit)
        )
        if after_channel is not None:
            stmt = stmt.where(Channel.name > after_channel)
        return [(name, dep) for name, dep in (await self._s.execute(stmt)).all()]

    async def states_for_channel(self, ch: Channel) -> Sequence[tuple[str, Deployment]]:
        """Per environment, the newest record that is not `superseded` (so a failed publish stays visible)."""
        newest = (
            select(func.max(Deployment.seq).label("s"))
            .where(Deployment.channel_id == ch.id, Deployment.status != "superseded")
            .group_by(Deployment.environment_id)
            .subquery()
        )
        stmt = (
            select(Environment.name, Deployment)
            .join(Deployment, Deployment.environment_id == Environment.id)
            .where(Deployment.seq.in_(select(newest.c.s)))
            .order_by(Environment.name)
        )
        return [(name, dep) for name, dep in (await self._s.execute(stmt)).all()]

    async def version_numbers(self, deployments: Iterable[Deployment]) -> dict[uuid.UUID, int]:
        """`version_id` -> version number, for rendering records."""
        ids = {d.version_id for d in deployments if d.version_id is not None}
        if not ids:
            return {}
        rows = await self._s.execute(
            select(ChannelVersion.id, ChannelVersion.version).where(ChannelVersion.id.in_(ids))
        )
        return {row[0]: row[1] for row in rows.all()}
