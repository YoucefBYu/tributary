"""Search, message detail, content and stats against a real ClickHouse (the archiver's DDL), through the real
app, with every response validated against `openapi.json`. Content is audited before it is read."""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from datetime import timedelta
from typing import Any

import httpx
import pytest
from api_kit import Contract, Idp, TlsPg, run
from archive_kit import (
    Admin,
    content,
    event,
    insert,
    make_database,
    minutes_ago,
    msg_id,
    now,
    set_content_flag,
)
from test_audit_integration import Recorder, build_with
from test_deploy_kafka_integration import seed
from test_routes_integration import Api
from tributary.api.archive import ArchiveBackend
from tributary.api.audit import AuditSendError, _validator
from tributary.api.chclient import ArchiveClientError
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults

pytestmark = pytest.mark.integration

PAYLOAD = b"MSH|^~\\&|TRIBUTARY-SYNTH|LAB|||202610061000||ADT^A01|SYNTH-1|P|2.5\r"  # synthetic
ENV_COLS = ("ts", "kind", "channel", "channel_version", "stage", "status", "instance", "message_id", "parent_id")


class Rig:
    def __init__(
        self,
        tls_pg: TlsPg,
        idp: Idp,
        contract: Contract,
        admin: Admin,
        *,
        with_content: bool = True,
        kafka: str | None = None,
        log_level: str = "error",
        publisher_factory: Any = None,
    ) -> None:
        self.env = f"ar{uuid.uuid4().hex[:6]}"
        seed(tls_pg.plain_url, self.env)
        self.admin, self.idp, self.contract = admin, idp, contract
        self.db = make_database(admin, with_content=with_content)
        self.sender = Recorder()
        self.queries: list[str] = []
        self.read_delay = 0.0  # seconds each statement on the reads client takes (flood tests)
        self.content_delay = 0.0
        self.content_fail = False  # the content client cannot reach the archive

        def counting(delay_of: str) -> Any:
            client = admin.client()
            original = client.query

            def query(sql: str, parameters: Any = None, settings: Any = None) -> Any:
                self.queries.append(sql)
                if (d := getattr(self, delay_of)) > 0:
                    time.sleep(d)
                if delay_of == "content_delay" and self.content_fail:
                    raise ArchiveClientError
                return original(sql, parameters, settings)

            client.query = query  # type: ignore[method-assign]
            return client

        client, content_client = counting("read_delay"), counting("content_delay")
        settings = None if kafka is None else KafkaSettings(bootstrap_servers=kafka)
        self.app, self.engine = build_with(
            tls_pg,
            idp,
            self.sender,
            home=self.env,
            log_level=log_level,
            archive=ArchiveBackend(
                client,
                {self.env: self.db},
                content_client=content_client,
                kafka=settings,
                defaults=None if kafka is None else load_topic_defaults(),
                publisher_factory=publisher_factory,
            ),
        )

    async def session(self, work: Any) -> Any:
        async with self.app.router.lifespan_context(self.app):
            await self.app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test") as c:
                api = Api(c, self.idp, self.contract)
                await api.create("adt")
                try:
                    return await work(api, c)
                finally:
                    await self.engine.dispose()

    async def call(self, api: Api, method: str, tail: str, *, expect: int = 200, **kw: Any) -> Any:
        return await api.call(
            method,
            "/v1/environments/{env}/channels/{channel}" + tail,
            expect=expect,
            env=self.env,
            channel=kw.pop("channel", "adt"),
            **kw,
        )


def populate(rig: Rig) -> dict[str, Any]:
    """Two messages. A: received, routed to two destinations (one sent, one retrying then dlq), with attrs.
    B: a single received event, an hour older."""
    a, b = msg_id(minutes_ago(10)), msg_id(minutes_ago(70))
    t = minutes_ago(10)
    rows = [
        event(t, a, "received", attrs={"msgtype": "ADT"}, topic=f"{rig.env}.adt.raw", partition=0, offset=7),
        event(t + timedelta(seconds=1), a, "routed", destination="ehr", attrs={"msgtype": "ADT"}),
        event(t + timedelta(seconds=1), a, "routed", destination="lab"),
        event(t + timedelta(seconds=2), a, "sent", destination="ehr", duration_ms=40.0, version=2, instance="worker-1"),
        event(t + timedelta(seconds=3), a, "retrying", destination="lab", status="error", error_code="HTTP_503", attempt=1),
        event(t + timedelta(seconds=9), a, "dlq", destination="lab", status="error", error_code="HTTP_503", attempt=2),
        event(minutes_ago(70), b, "received"),
    ]  # fmt: skip
    from archive_kit import EVENT_COLUMNS

    insert(rig.admin, rig.db, "message_events", EVENT_COLUMNS, rows)
    insert(
        rig.admin, rig.db, "message_content",
        ("ts", "channel", "channel_version", "message_id", "stage", "destination", "content_type", "size_bytes",
         "payload", "claim_ref", "parent_id", "correlation_id", "attempt", "error_code", "topic", "partition", "offset"),
        [
            content(t, a, "received", PAYLOAD),
            content(t, a, "routed", PAYLOAD + b"|X", destination="ehr"),
            content(t, a, "routed", b"", destination="lab", claim_ref="s3://tb-claims/adt/object-1"),
        ],
    )  # fmt: skip
    return {"a": a, "b": b}


def test_search_filters_paginates_and_refuses_what_the_contract_refuses(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(rig)
    since = (now() - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        search = lambda body, **kw: rig.call(api, "POST", "/messages/search", json_body=body, **kw)  # noqa: E731
        everything = await search({"since": since, "limit": 100})
        assert [e["stage"] for e in everything["items"]][:2] == ["dlq", "retrying"]  # newest first
        assert {e["message_id"] for e in everything["items"]} == {str(ids["a"]), str(ids["b"])}
        first = await search({"since": since, "limit": 3})
        assert len(first["items"]) == 3 and first["next_cursor"]
        second = await search({"since": since, "limit": 100, "cursor": first["next_cursor"]})
        assert len(first["items"]) + len(second["items"]) == len(everything["items"])  # no overlap, no gap
        assert second["next_cursor"] is None
        only_dlq = await search({"since": since, "stages": ["dlq"], "error_code": "HTTP_503", "destination": "lab"})
        assert [e["stage"] for e in only_dlq["items"]] == ["dlq"]
        by_attr = await search({"since": since, "stages": ["routed"], "attrs": {"msgtype": "ADT"}})
        assert [e["destination"] for e in by_attr["items"]] == ["ehr"]
        # refusals
        assert (await search({"since": since, "attrs": {"mrn": "x"}}, expect=422))["code"] == "REQUEST_INVALID"
        bad_until = {"since": since, "until": since}
        assert (await search(bad_until, expect=422))["code"] == "REQUEST_INVALID"
        assert (await search({"since": since, "cursor": "AAAA"}, expect=400))["code"] == "REQUEST_INVALID"
        await search({"since": since, "bogus": 1}, expect=400)
        await rig.call(api, "POST", "/messages/search", json_body={"since": since}, expect=404, channel="nosuch")

    run(rig.session(work))


def test_message_detail_has_the_timeline_destinations_and_a_content_index_without_bytes(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(rig)

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        body = await rig.call(api, "GET", "/messages/{message_id}", message_id=str(ids["a"]), role="tb-viewer")
        assert [e["stage"] for e in body["events"]] == ["received", "routed", "routed", "sent", "retrying", "dlq"]
        assert body["destinations"] == [
            {"destination": "ehr", "state": "sent", "attempt": 0, "last_ts": body["destinations"][0]["last_ts"]},
            {"destination": "lab", "state": "dlq", "attempt": 2, "last_ts": body["destinations"][1]["last_ts"],
             "error_code": "HTTP_503"},
        ]  # fmt: skip
        assert {(c["stage"], c.get("destination"), c["storage"]) for c in body["content"]} == {
            ("received", None, "inline"), ("routed", "ehr", "inline"), ("routed", "lab", "claim_check"),
        }  # fmt: skip
        assert "payload" not in json.dumps(body) and base64.b64encode(PAYLOAD).decode() not in json.dumps(body)
        missing = await rig.call(api, "GET", "/messages/{message_id}", message_id=str(uuid.uuid4()), expect=404)
        assert missing["code"] == "MESSAGE_NOT_FOUND"

    run(rig.session(work))


def test_a_content_view_is_audited_first_returns_the_exact_bytes_and_a_locator_for_claim_checks(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(rig)
    url = "/messages/{message_id}/content"

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        mid = str(ids["a"])
        before = len(rig.sender.sent)
        inline = await rig.call(api, "GET", url, message_id=mid, params={"stage": "received"}, role="tb-operator")
        assert base64.b64decode(inline["payload_base64"]) == PAYLOAD and inline["storage"] == "inline"
        assert inline["headers"]["tb-msg-id"] == mid and "tb-usr-x" not in inline["headers"]
        raw = await c.get(
            f"/v1/environments/{rig.env}/channels/adt/messages/{mid}/content?stage=received",
            headers={"Authorization": "Bearer " + rig.idp.mint(roles=["tb-admin"])},
        )
        assert raw.headers["cache-control"] == "no-store"
        routed = await rig.call(
            api, "GET", url, message_id=mid, params={"stage": "routed", "destination": "ehr"}, role="tb-admin"
        )
        assert base64.b64decode(routed["payload_base64"]) == PAYLOAD + b"|X"
        claim = await rig.call(
            api, "GET", url, message_id=mid, params={"stage": "routed", "destination": "lab"}, role="tb-admin"
        )
        assert claim["storage"] == "claim_check" and claim["claim_check"] == "s3://tb-claims/adt/object-1"
        assert "payload_base64" not in claim  # the locator only: the object is never fetched

        # exactly one audit record per view, in the contract shape, with no payload bytes (invariant 6)
        records = [json.loads(v) for _, _, v in rig.sender.sent[before:]]
        assert len(records) == 4 and {r["action"] for r in records} == {"message.content.view"}
        assert all(_validator().is_valid(r) and r["phase"] == "requested" and r["message_id"] == mid for r in records)
        assert all(r["environment"] == rig.env and r["actor_roles"] for r in records)
        blob = json.dumps(records)
        assert base64.b64encode(PAYLOAD).decode() not in blob and "MSH" not in blob and "SYNTH-1" not in blob

        # refusals: a malformed value is 400, an impossible combination is 422, and neither writes an audit record
        # or reads anything (OpenAPI 0.2.2)
        records_before, queries_before = len(rig.sender.sent), len(rig.queries)
        await rig.call(api, "GET", url, message_id=mid, params={"stage": "weird"}, expect=400, role="tb-admin")
        routed_alone = await rig.call(
            api, "GET", url, message_id=mid, params={"stage": "routed"}, expect=422, role="tb-admin"
        )
        received_with = await rig.call(
            api, "GET", url, message_id=mid, params={"stage": "received", "destination": "ehr"}, expect=422,
            role="tb-admin",
        )  # fmt: skip
        assert routed_alone["code"] == received_with["code"] == "REQUEST_INVALID"
        assert (len(rig.sender.sent), len(rig.queries)) == (records_before, queries_before)
        gone = await rig.call(api, "GET", url, message_id=mid, params={"stage": "dlq"}, expect=404, role="tb-admin")
        assert gone["code"] == "CONTENT_NOT_FOUND"
        for role in ("tb-viewer", "tb-developer", "tb-auditor"):
            await rig.call(api, "GET", url, message_id=mid, params={"stage": "received"}, expect=403, role=role)

    run(rig.session(work))


def test_a_content_view_is_refused_and_reads_nothing_when_the_audit_record_is_not_committed(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(rig)

    async def refuse(env: str, key: bytes, value: bytes) -> None:
        raise AuditSendError

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        rig.sender.send = refuse  # type: ignore[method-assign]
        before = len(rig.queries)
        body = await rig.call(
            api, "GET", "/messages/{message_id}/content", message_id=str(ids["a"]),
            params={"stage": "received"}, expect=503, role="tb-operator",
        )  # fmt: skip
        assert body["code"] == "AUDIT_UNAVAILABLE"
        assert len(rig.queries) == before  # no archive statement ran: nothing was read

    run(rig.session(work))


def test_content_storage_off_is_a_conflict_and_the_index_is_empty(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin, with_content=False)
    a = msg_id(minutes_ago(5))
    from archive_kit import EVENT_COLUMNS

    insert(rig.admin, rig.db, "message_events", EVENT_COLUMNS, [event(minutes_ago(5), a, "received")])

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        detail = await rig.call(api, "GET", "/messages/{message_id}", message_id=str(a))
        assert detail["content"] == []
        off = await rig.call(
            api, "GET", "/messages/{message_id}/content", message_id=str(a), params={"stage": "received"},
            expect=409, role="tb-operator",
        )  # fmt: skip
        assert off["code"] == "CONTENT_STORAGE_OFF"

    run(rig.session(work))


def test_stats_count_deduplicated_events_with_percentiles_errors_and_versions(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    populate(rig)
    # a re-emitted copy of an event must not double count
    from archive_kit import EVENT_COLUMNS

    dup_id = msg_id(minutes_ago(3))
    dup = event(minutes_ago(3), dup_id, "sent", destination="ehr", duration_ms=10.0)
    insert(rig.admin, rig.db, "message_events", EVENT_COLUMNS, [dup, dup])

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        stats = await rig.call(api, "GET", "/stats", params={"window": "1h"}, role="tb-viewer")
        assert stats["counts"] == {
            "received": 1,
            "filtered": 0,
            "routed": 2,
            "sent": 2,
            "retrying": 1,
            "error": 0,
            "dlq": 1,
        }
        ehr = next(d for d in stats["destinations"] if d["destination"] == "ehr")
        assert ehr["sent"] == 2 and ehr["duration_ms_p50"] is not None
        lab = next(d for d in stats["destinations"] if d["destination"] == "lab")
        assert (lab["sent"], lab["retrying"], lab["dlq"], lab["duration_ms_p50"]) == (0, 1, 1, None)
        assert stats["errors"] == [{"error_code": "HTTP_503", "count": 2}]
        assert {v["version"] for v in stats["versions"]} == {1, 2}
        default = await rig.call(api, "GET", "/stats", role="tb-viewer")
        assert default["window"] == "15m"
        await rig.call(api, "GET", "/stats", params={"window": "3d"}, expect=400)

    run(rig.session(work))


def test_values_round_trip_through_server_side_parameters_whatever_they_contain(
    clickhouse_admin: Admin,
) -> None:
    """Nothing is spliced into SQL: quotes, backslashes, tabs, newlines and brackets come back unchanged."""
    client = clickhouse_admin.client()
    nasty = ["plain", "it's", "back\\slash", "tab\there", "new\nline", "'); DROP TABLE x; --", "[a,b]", "\u00e9"]
    for value in nasty:
        got = client.query("SELECT {s:String}, {a:Array(String)}", {"s": value, "a": [value, "z"]}, {"readonly": 1})
        assert got.result_rows == [(value, [value, "z"])], value


def test_a_stored_locator_the_grammar_refuses_is_never_shown_and_no_object_is_fetched(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    mid = msg_id(minutes_ago(5))
    presigned = "https://bucket.example/o?X-Amz-Signature=abc"  # a presigned URL is a replayable credential
    insert(
        rig.admin, rig.db, "message_content",
        ("ts", "channel", "channel_version", "message_id", "stage", "destination", "content_type", "size_bytes",
         "payload", "claim_ref", "parent_id", "correlation_id", "attempt", "error_code", "topic", "partition", "offset"),
        [content(minutes_ago(5), mid, "received", b"", claim_ref=presigned)],
    )  # fmt: skip

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        r = await c.get(
            f"/v1/environments/{rig.env}/channels/adt/messages/{mid}/content?stage=received",
            headers={"Authorization": "Bearer " + rig.idp.mint(roles=["tb-operator"])},
        )
        assert r.status_code == 503 and "X-Amz" not in r.text and "bucket.example" not in r.text

    run(rig.session(work))


def test_a_content_view_logs_no_payload_and_no_identifier_beyond_the_request_line(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, capfd: pytest.CaptureFixture[str]
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin, log_level="debug")
    ids = populate(rig)

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        capfd.readouterr()
        await rig.call(
            api, "GET", "/messages/{message_id}/content", message_id=str(ids["a"]), params={"stage": "received"},
            role="tb-operator",
        )  # fmt: skip

    run(rig.session(work))
    logged = capfd.readouterr().err
    assert "http_request" in logged  # the request line is there ...
    for secret in (base64.b64encode(PAYLOAD).decode(), "MSH", "SYNTH-1", str(ids["a"]), "stage=received"):
        assert secret not in logged, secret  # ... and nothing about the content or the message


def test_a_flood_of_reads_cannot_delay_or_pre_audit_a_content_view_and_a_full_gate_refuses_before_the_audit(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    rig = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(rig)
    since = (now() - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    viewer = {"Authorization": "Bearer " + idp.mint(roles=["tb-viewer"])}
    operator = {"Authorization": "Bearer " + idp.mint(roles=["tb-operator"])}

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        base = f"/v1/environments/{rig.env}/channels/adt"
        content_url = f"{base}/messages/{ids['a']}/content?stage=received"
        search = lambda: c.post(base + "/messages/search", json={"since": since}, headers=viewer)  # noqa: E731

        # 1. a flood of searches against a slow archive: the gate refuses the excess at once ...
        rig.read_delay = 2.0
        flood = [asyncio.create_task(search()) for _ in range(30)]
        await asyncio.sleep(0.4)
        audits_before = len(rig.sender.sent)
        view_started = time.monotonic()
        view = await c.get(content_url, headers=operator)  # ... and the content view is not queued behind them
        assert view.status_code == 200 and time.monotonic() - view_started < 1.5
        assert len(rig.sender.sent) == audits_before + 1  # exactly its own audit record
        done = await asyncio.gather(*flood)
        statuses = sorted(r.status_code for r in done)
        refused = [r for r in done if r.status_code == 503]
        assert len(refused) >= 10 and set(statuses) <= {200, 503}
        assert all(r.json()["code"] == "ARCHIVE_UNAVAILABLE" for r in refused)
        rig.read_delay = 0.0

        # 2. the content gate is full: the excess is refused BEFORE any audit record is written
        rig.content_delay = 2.0
        audits_before = len(rig.sender.sent)
        views = await asyncio.gather(*[c.get(content_url, headers=operator) for _ in range(14)])
        ok = [r for r in views if r.status_code == 200]
        full = [r for r in views if r.status_code == 503]
        assert (
            len(ok) + len(full) == 14
            and len(full) >= 4
            and all(r.json()["code"] == "ARCHIVE_UNAVAILABLE" for r in full)
        )
        assert len(rig.sender.sent) - audits_before == len(ok)  # a refused view wrote nothing

    run(rig.session(work))


def test_content_follows_the_archivers_storage_flag_off_is_409_unreadable_or_missing_while_on_is_503(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin
) -> None:
    url = "/messages/{message_id}/content"

    # flag false (storage off): 409 CONTENT_STORAGE_OFF, though the table exists
    off = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(off)
    set_content_flag(off.admin, off.db, False)

    async def storage_off(api: Api, c: httpx.AsyncClient) -> None:
        body = await off.call(
            api, "GET", url, message_id=str(ids["a"]), params={"stage": "received"}, expect=409, role="tb-operator"
        )
        assert body["code"] == "CONTENT_STORAGE_OFF"

    run(off.session(storage_off))

    # flag true, table there: served (and a later `false` flips it back, the newest row wins)
    on = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(on)
    set_content_flag(on.admin, on.db, True)

    async def storage_on(api: Api, c: httpx.AsyncClient) -> None:
        got = await on.call(api, "GET", url, message_id=str(ids["a"]), params={"stage": "received"}, role="tb-operator")
        assert base64.b64decode(got["payload_base64"]) == PAYLOAD

    run(on.session(storage_on))

    # flag true but the table is missing: a fault (503), not "off"
    broken = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(broken)
    set_content_flag(broken.admin, broken.db, True)
    broken.admin.run(f"DROP TABLE `{broken.db}`.message_content")

    async def enabled_but_missing(api: Api, c: httpx.AsyncClient) -> None:
        body = await broken.call(
            api, "GET", url, message_id=str(ids["a"]), params={"stage": "received"}, expect=503, role="tb-operator"
        )
        assert body["code"] == "ARCHIVE_UNAVAILABLE"

    run(broken.session(enabled_but_missing))

    # the flag cannot be read (the archive does not answer): 503, never a guess
    down = Rig(tls_pg, idp, contract, clickhouse_admin)
    ids = populate(down)

    async def unreadable(api: Api, c: httpx.AsyncClient) -> None:
        down.content_fail = True
        body = await down.call(
            api, "GET", url, message_id=str(ids["a"]), params={"stage": "received"}, expect=503, role="tb-operator"
        )
        assert body["code"] == "ARCHIVE_UNAVAILABLE"

    run(down.session(unreadable))
