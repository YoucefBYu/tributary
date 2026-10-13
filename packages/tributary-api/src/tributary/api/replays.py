"""Replay state in PostgreSQL (`openapi/v0` README 3.14): the API's own record of each channel replay.

PostgreSQL holds this because the replay outlives the HTTP request that started it, and a second API replica can
answer `GET`. The data plane never reads it (invariant 3). Rows hold counts, codes and ids: no payload, key or
header value.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import render
from .db.errors import AlreadyExistsError, RepositoryError
from .db.models import Replay
from .db.repository import ChannelRepository, EnvironmentRepository, ReplayRepository
from .problems import ApiProblem
from .service import _int_cursor, encode_cursor


def body_hash(environment: str, channel: str, request: Any) -> str:
    """The idempotency fingerprint: the environment, the channel and every field of the request."""
    canonical = {
        "environment": environment,
        "channel": channel,
        "replay_id": request.replay_id,
        "start": request.start,
        "end": request.end,
        "allow_reorder": request.allow_reorder,
        "max_records": request.max_records,
        "deadline_s": request.deadline_s,
    }
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class ReplayStore:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = sessions
        self._clock = clock

    async def _in[T](self, work: Callable[[AsyncSession], Awaitable[T]]) -> T:
        try:
            async with self._sessions() as s, s.begin():
                return await work(s)
        except ApiProblem:
            raise
        except (RepositoryError, OSError):
            raise ApiProblem("UNAVAILABLE") from None
        except Exception as exc:  # a driver error: the database is the problem, whatever it says
            if type(exc).__module__.split(".")[0] in ("sqlalchemy", "asyncpg"):
                raise ApiProblem("UNAVAILABLE") from None
            raise

    async def begin(
        self, env: str, channel: str, request: Any, start: datetime, end: datetime, actor: str
    ) -> tuple[dict[str, Any], bool]:
        """Record the request. Returns `(record, started)`: `started` is False for an identical request whose replay
        is `running` or `completed` (the existing record, `200`); True for a new one or a `failed` one run again
        (`202`). Same id with a different body: 409 `REQUEST_ID_REUSED`."""
        replay_id = uuid.UUID(request.replay_id)
        fingerprint = body_hash(env, channel, request)

        async def work(s: AsyncSession) -> tuple[dict[str, Any], bool]:
            ch = await ChannelRepository(s).get_by_name(channel)
            environment = await EnvironmentRepository(s).get_by_name(env)
            repo = ReplayRepository(s)
            now = self._clock()
            existing = await repo.get(replay_id, for_update=True)
            if existing is not None:
                if existing.channel_id != ch.id or existing.environment_id != environment.id:
                    raise ApiProblem("NOT_FOUND")  # another channel's or environment's id: not ours to confirm
                if existing.body_sha256 != fingerprint:
                    raise ApiProblem("REQUEST_ID_REUSED")
                shown = render.replay(existing, env, channel, now=now)
                if shown["status"] != "failed":
                    return shown, False
                await repo.restart(existing, now=now, deadline_at=now + timedelta(seconds=request.deadline_s))
                return render.replay(existing, env, channel, now=now), True
            try:
                row = await repo.create(
                    ch,
                    environment,
                    replay_id=replay_id,
                    body_sha256=fingerprint,
                    range_start=start,
                    range_end=end,
                    allow_reorder=request.allow_reorder,
                    max_records=request.max_records,
                    deadline_s=request.deadline_s,
                    requested_by=actor,
                    deadline_at=now + timedelta(seconds=request.deadline_s),
                )
            except AlreadyExistsError:
                raise ApiProblem("REQUEST_ID_REUSED") from None  # a concurrent request took the id
            await s.refresh(row)
            return render.replay(row, env, channel, now=now), True

        return await self._in(work)

    async def finish_completed(self, replay_id: str, result: Any) -> None:
        async def work(s: AsyncSession) -> None:
            await ReplayRepository(s).finish(
                uuid.UUID(replay_id),
                status="completed",
                finished_at=self._clock(),
                partitions_scanned=result.partitions_scanned,
                span=result.span,
                republished=result.republished,
                skipped_undecodable=result.skipped_undecodable,
                skipped_out_of_range=result.skipped_out_of_range,
                skipped_no_timestamp=result.skipped_no_timestamp,
            )

        await self._in(work)

    async def finish_failed(self, replay_id: str, failure_code: str, republished: int) -> None:
        async def work(s: AsyncSession) -> None:
            await ReplayRepository(s).finish(
                uuid.UUID(replay_id),
                status="failed",
                finished_at=self._clock(),
                failure_code=failure_code,
                republished=republished,
            )

        await self._in(work)

    async def get(self, env: str, channel: str, replay_id: str) -> dict[str, Any]:
        async def work(s: AsyncSession) -> dict[str, Any]:
            ch = await ChannelRepository(s).get_by_name(channel)
            environment = await EnvironmentRepository(s).get_by_name(env)
            row = await ReplayRepository(s).get(uuid.UUID(replay_id))
            if row is None or row.channel_id != ch.id or row.environment_id != environment.id:
                raise ApiProblem("NOT_FOUND")
            return render.replay(row, env, channel, now=self._clock())

        return await self._in(work)

    async def list(self, env: str, channel: str, limit: int, cursor: str | None) -> dict[str, Any]:
        before = _int_cursor(cursor)

        async def work(s: AsyncSession) -> dict[str, Any]:
            ch = await ChannelRepository(s).get_by_name(channel)
            environment = await EnvironmentRepository(s).get_by_name(env)
            rows: list[Replay] = list(
                await ReplayRepository(s).list(ch, environment, limit=limit + 1, before_seq=before)
            )
            page = rows[:limit]
            now = self._clock()
            return {
                "items": [render.replay(r, env, channel, now=now) for r in page],
                "next_cursor": encode_cursor(page[-1].seq) if len(rows) > limit and page else None,
            }

        return await self._in(work)
