"""`GET /v1/environments/{env}/events` (SSE) against a real ClickHouse, through a live uvicorn server."""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import uvicorn
from api_kit import Contract, Idp, TlsPg
from archive_kit import (
    EVENT_COLUMNS,
    Admin,
    Sse,
    event,
    free_port,
    insert,
    make_database,
    minutes_ago,
    msg_id,
    restore_uvicorn_logging,
)
from test_audit_integration import Recorder, build_with
from test_deploy_kafka_integration import seed
from test_routes_integration import document
from tributary.api.archive import ArchiveBackend
from tributary.api.chclient import ArchiveClientError

pytestmark = pytest.mark.integration


class Live:
    def __init__(
        self,
        tls_pg: TlsPg,
        idp: Idp,
        contract: Contract,
        admin: Admin,
        *,
        heartbeat_s: float = 15.0,
        max_streams: int = 64,
    ) -> None:
        self.env = f"st{uuid.uuid4().hex[:6]}"
        seed(tls_pg.plain_url, self.env)
        self.admin, self.idp, self.contract = admin, idp, contract
        self.db = make_database(admin)
        self.client = admin.client()
        self.app, self.engine = build_with(
            tls_pg, idp, Recorder(), home=self.env,
            archive=ArchiveBackend(self.client, {self.env: self.db}, stream_poll_s=0.2, stream_heartbeat_s=heartbeat_s, max_streams=max_streams),
        )  # fmt: skip
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"

    def token(self, role: str = "tb-viewer", *, sub: str = "user-1", ttl: int = 300) -> str:
        t = int(time.time())
        return self.idp.mint(roles=[role], claims={"sub": sub, "exp": t + ttl})

    def url(self, *channels: str) -> str:
        return f"/v1/environments/{self.env}/events?" + "&".join(f"channel={c}" for c in channels)

    @contextlib.contextmanager
    def running(self, channels: tuple[str, ...] = ("adt",)) -> Iterator[None]:
        server = uvicorn.Server(
            uvicorn.Config(self.app, host="127.0.0.1", port=self.port, access_log=False, log_config=None)
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if httpx.get(self.base + "/readyz", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("server not ready")
        headers = {"Authorization": f"Bearer {self.idp.mint(roles=['tb-admin'])}"}
        for name in channels:
            r = httpx.post(
                f"{self.base}/v1/channels", json={"document": document(name), "origin": {"kind": "api"}},
                headers=headers, timeout=20,
            )  # fmt: skip
            assert r.status_code == 201, r.text
        try:
            yield
        finally:
            server.should_exit = True
            thread.join(15)
            restore_uvicorn_logging()

    def add(
        self, stage: str, *, channel: str = "adt", when: Any = None, mid: uuid.UUID | None = None, **kw: Any
    ) -> uuid.UUID:
        m = mid or msg_id()
        insert(
            self.admin,
            self.db,
            "message_events",
            EVENT_COLUMNS,
            [event(when or datetime.now(UTC), m, stage, channel=channel, **kw)],
        )
        return m


def test_a_stream_delivers_new_events_oldest_first_with_ids_and_heartbeats(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=0.5)
    with live.running(("adt", "oru")):
        old = live.add("received", when=minutes_ago(1))  # before the connection: not part of a live tail
        stream = Sse(live.base, live.url("adt", "oru"), live.token())
        assert stream.status == 200 and stream.response.getheader("Cache-Control") == "no-store"
        assert stream.response.getheader("Content-Type").startswith("text/event-stream")  # type: ignore[union-attr]
        time.sleep(0.5)
        first = live.add("received")
        time.sleep(0.3)
        second = live.add("routed", channel="oru", destination="ehr")
        frames = stream.frames(2, kinds=("message",))
        assert [f[2]["message_id"] for f in frames] == [str(first), str(second)]
        assert all(f[1] for f in frames) and frames[1][2]["destination"] == "ehr"
        assert str(old) not in {f[2]["message_id"] for f in frames}
        beat = stream.frames(1, kinds=("heartbeat",))
        assert beat and set(beat[0][2]) == {"ts"}
        for _, _, data in frames:  # contract shape, metadata only
            contract.check_schema("MessageEvent", data)  # type: ignore[attr-defined]
            assert "payload" not in data
        stream.close()


def test_resume_cursor_invalid_and_expired_and_the_stream_continues_live(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin)
    with live.running():
        stream = Sse(live.base, live.url("adt"), live.token())
        time.sleep(0.5)
        a = live.add("received")
        [(_, a_id, _)] = stream.frames(1, kinds=("message",))
        time.sleep(0.3)
        b = live.add("sent", destination="ehr")
        stream.close()

        resumed = Sse(live.base, live.url("adt"), live.token(), headers={"Last-Event-ID": a_id or ""})
        frames = resumed.frames(1, kinds=("message",))
        assert [f[2]["message_id"] for f in frames] == [str(b)] and str(a) != str(b)  # after `a`, not `a` again
        resumed.close()

        bad = Sse(live.base, live.url("adt"), live.token(), headers={"Last-Event-ID": "notacursor"})
        assert bad.frames(1)[0][:3:2] == ("reset", {"reason": "cursor_invalid"})
        time.sleep(0.3)
        c = live.add("received")
        assert [f[2]["message_id"] for f in bad.frames(1, kinds=("message",))] == [str(c)]  # then live
        bad.close()

        from tributary.api.stream import event_id

        stale = live.add("received", when=minutes_ago(20))
        view = next(v for v in _search(live, "adt", minutes_ago(30)) if str(v.message_id) == str(stale))
        expired = Sse(live.base, live.url("adt"), live.token(), headers={"Last-Event-ID": event_id(view)})
        assert expired.frames(1)[0][:3:2] == ("reset", {"reason": "cursor_expired"})
        expired.close()


def _search(live: Live, channel: str, since: Any) -> list[Any]:
    from tributary.archiver.query import ArchiveQueries, SearchFilter

    return list(ArchiveQueries(live.client, database=live.db).search(SearchFilter(channel=channel, since=since)).items)


def test_a_stream_ends_at_the_tokens_expiry_and_on_server_shutdown(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin)
    with live.running():
        short = Sse(live.base, live.url("adt"), live.token(ttl=3))
        end = short.frames(1, kinds=("end",))
        assert end[0][2] == {"reason": "token_expiring"}
        assert short.response.read() == b""  # and the server closed the stream
        short.close()

        long = Sse(live.base, live.url("adt"), live.token())
        time.sleep(0.5)
        live.app.state.stream_hub.closing.set()
        assert long.frames(1, kinds=("end",))[0][2] == {"reason": "server_shutdown"}
        long.close()


def test_four_streams_per_subject_then_429_and_refusals_before_the_stream_starts(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin)
    with live.running():
        mine = [Sse(live.base, live.url("adt"), live.token(sub="user-a")) for _ in range(4)]
        assert [s.status for s in mine] == [200] * 4
        fifth = Sse(live.base, live.url("adt"), live.token(sub="user-a"))
        assert fifth.status == 429 and b"TOO_MANY_STREAMS" in fifth.body()
        other = Sse(live.base, live.url("adt"), live.token(sub="user-b"))
        assert other.status == 200  # the cap is per subject
        mine[0].close()
        time.sleep(1.0)
        again = Sse(live.base, live.url("adt"), live.token(sub="user-a"))
        assert again.status == 200  # a closed stream frees its slot
        for s in [*mine[1:], other, again, fifth]:
            s.close()

        for path, want in (
            (f"/v1/environments/{live.env}/events", 400),  # no channel
            (live.url("adt", "adt"), 400),  # repeated
            (live.url(*[f"c{i}" for i in range(17)]), 400),  # more than 16
            (live.url("Bad_Name"), 400),
            (live.url("nosuch"), 404),
            ("/v1/environments/nosuch/events?channel=adt", 404),
        ):
            r = Sse(live.base, path, live.token())
            assert r.status == want, (path, r.status)
            r.close()
        anonymous = Sse(live.base, live.url("adt"), "not.a.jwt")
        assert anonymous.status == 401
        anonymous.close()


def test_with_the_archive_down_the_stream_sends_heartbeats_only_and_recovers(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=0.4)
    real = live.client.query
    down = {"on": True}

    def flaky(*a: Any, **k: Any) -> Any:
        if down["on"]:
            raise ArchiveClientError
        return real(*a, **k)

    live.client.query = flaky  # type: ignore[method-assign]
    with live.running():
        stream = Sse(live.base, live.url("adt"), live.token())
        assert stream.frames(2, kinds=("heartbeat",))  # alive, silent
        down["on"] = False
        mid = live.add("received")
        got = stream.frames(1, kinds=("message",))
        assert got[0][2]["message_id"] == str(mid)
        stream.close()


def test_every_role_may_stream_because_the_stream_is_metadata_only(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin)
    with live.running():
        for role in ("tb-viewer", "tb-developer", "tb-operator", "tb-auditor", "tb-admin"):
            s = Sse(live.base, live.url("adt"), live.token(role, sub=f"u-{role}"))
            assert s.status == 200, role
            s.close()


# -- review hardening ------------------------------------------------------------------------------------


def test_a_long_backlog_is_delivered_whole_and_in_order_nothing_skipped(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    """More new events than one poll can page through (5 pages of 200): the poll narrows its window instead of
    reading only the newest events and moving on."""
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=30)
    with live.running():
        base = datetime.now(UTC) - timedelta(minutes=2)
        anchor = live.add("received", when=base)
        ids = [msg_id() for _ in range(1200)]
        insert(
            live.admin, live.db, "message_events", EVENT_COLUMNS,
            [event(base + timedelta(milliseconds=50 * (i + 1)), mid, "received") for i, mid in enumerate(ids)],
        )  # fmt: skip
        import base64
        import json

        raw = json.dumps([int(base.timestamp() * 1000), "adt", str(anchor), "received", "", 0], separators=(",", ":"))
        cursor = base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")
        stream = Sse(live.base, live.url("adt"), live.token(), headers={"Last-Event-ID": cursor})
        frames = stream.frames(1200, kinds=("message",))
        got = [f[2]["message_id"] for f in frames]
        assert got == [str(i) for i in ids]  # every event, once, oldest first
        stream.close()


def test_a_closed_connection_frees_its_stream_slot_and_the_replica_has_a_total_cap(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=0.3, max_streams=2)
    with live.running():
        hub = live.app.state.stream_hub
        a = Sse(live.base, live.url("adt"), live.token(sub="user-a"))
        b = Sse(live.base, live.url("adt"), live.token(sub="user-b"))
        assert (a.status, b.status) == (200, 200) and hub.open_streams() == 2
        third = Sse(live.base, live.url("adt"), live.token(sub="user-c"))
        assert third.status == 429  # the replica's cap, whoever asks
        third.close()
        a.close()  # the client goes away; the next heartbeat write notices
        deadline = time.monotonic() + 15
        while hub.open_streams() > 1 and time.monotonic() < deadline:
            time.sleep(0.2)
        assert hub.open_streams() == 1
        again = Sse(live.base, live.url("adt"), live.token(sub="user-c"))
        assert again.status == 200
        again.close()
        b.close()


def test_a_slow_archive_neither_stops_heartbeats_nor_holds_the_stream_past_the_tokens_expiry(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=0.3)
    real = live.client.query

    def slow(*a: Any, **k: Any) -> Any:
        time.sleep(6)
        return real(*a, **k)

    live.client.query = slow  # type: ignore[method-assign]
    with live.running():
        started = time.monotonic()
        stream = Sse(live.base, live.url("adt"), live.token(ttl=3))
        frames = stream.frames(100)  # until the server closes the stream
        elapsed = time.monotonic() - started
        kinds = [f[0] for f in frames]
        assert kinds[-1] == "end" and frames[-1][2] == {"reason": "token_expiring"}
        assert kinds.count("heartbeat") >= 3  # still alive while the poll waited
        assert elapsed < 5.5  # ended at the token's expiry, not after the 6 s poll
        stream.close()


def test_a_window_too_dense_to_read_in_order_is_announced_with_a_reset_not_skipped(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=30)
    with live.running():
        import base64
        import json

        base = datetime.now(UTC) - timedelta(minutes=2)
        anchor = live.add("received", when=base)
        dense = base + timedelta(milliseconds=50)  # 1200 events in one millisecond: no window can split them
        insert(
            live.admin, live.db, "message_events", EVENT_COLUMNS,
            [event(dense, msg_id(), "received") for _ in range(1200)],
        )  # fmt: skip
        raw = json.dumps([int(base.timestamp() * 1000), "adt", str(anchor), "received", "", 0], separators=(",", ":"))
        cursor = base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")
        stream = Sse(live.base, live.url("adt"), live.token(), headers={"Last-Event-ID": cursor})
        first = stream.frames(1)
        assert first[0][0] == "reset" and first[0][2] == {"reason": "backlog_too_dense"}  # the gap is announced
        contract.check_schema("StreamNotice", first[0][2])  # type: ignore[attr-defined]
        time.sleep(0.5)
        fresh = live.add("received")
        got = stream.frames(1, kinds=("message",))
        assert [f[2]["message_id"] for f in got] == [str(fresh)]  # and the stream carries on, live
        stream.close()


def test_stream_polls_against_a_slow_archive_never_exceed_the_pools_threads(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    """Opening and dropping streams cannot pile polls into the pool: a poll holds a gate slot until its thread ends."""
    live = Live(tls_pg, idp, contract, clickhouse_admin, heartbeat_s=30)
    real = live.client.query
    lock = threading.Lock()
    running = {"now": 0, "max": 0}

    def slow(*a: Any, **k: Any) -> Any:
        with lock:
            running["now"] += 1
            running["max"] = max(running["max"], running["now"])
        try:
            time.sleep(1.0)
            return real(*a, **k)
        finally:
            with lock:
                running["now"] -= 1

    live.client.query = slow  # type: ignore[method-assign]
    with live.running():
        streams = [Sse(live.base, live.url("adt"), live.token(sub=f"user-{i}")) for i in range(20)]
        time.sleep(2.5)
        for s in streams:
            s.close()  # dropped while their polls are still running
        reopened = [Sse(live.base, live.url("adt"), live.token(sub=f"again-{i}")) for i in range(10)]
        time.sleep(2.5)
        for s in reopened:
            s.close()
        assert running["max"] <= 8, running  # the streams pool has 8 threads: nothing queued behind them
