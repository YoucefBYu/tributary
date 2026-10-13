"""Reprocess and replay on real Kafka, ClickHouse and PostgreSQL, through the real app and the archiver library.
Every response is validated against `openapi.json`; the audit records are checked against `audit/v0`."""

from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from api_kit import Contract, Idp, TlsPg, run
from archive_kit import EVENT_COLUMNS, Admin, event, insert, msg_id
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from test_archive_read_integration import Rig
from test_routes_integration import Api, document
from tributary.api import render
from tributary.api.audit import AuditSendError, _validator
from tributary.runtime.topics import raw_topic
from tributary.sdk import headers as h
from tributary.sdk.ids import uuid7
from tributary.sdk.testing import make_message

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, header_map

pytestmark = pytest.mark.integration

PAYLOAD = b"MSH|^~\\&|TRIBUTARY-SYNTH|LAB|||202610061000||ADT^A01|SYNTH-9|P|2.5\r"  # synthetic


class IngestRig(Rig):
    def __init__(self, tls_pg: TlsPg, idp: Idp, contract: Contract, admin: Admin, kafka: str, *, per_key: bool = False,
                 deployed: bool = True, publisher_factory: Any = None) -> None:  # fmt: skip
        super().__init__(tls_pg, idp, contract, admin, kafka=kafka, publisher_factory=publisher_factory)
        self.kafka, self.per_key, self.deployed = kafka, per_key, deployed
        self.topic = raw_topic(self.env, "adt")
        create_topics_with_retry(kafka, [NewTopic(self.topic, 1, 1)])

    async def session(self, work: Any) -> Any:
        async with self.app.router.lifespan_context(self.app):
            await self.app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test") as c:
                api = Api(c, self.idp, self.contract)
                doc = document("adt")
                if self.per_key:
                    doc["spec"]["ordering"] = "per-key"
                    doc["spec"]["partitionKey"] = "hl7:PID.3.1"
                await api.call(
                    "POST", "/v1/channels", expect=201, json_body={"document": doc, "origin": {"kind": "api"}}
                )
                if self.deployed:
                    await api.deploy(self.env, "adt", 1)
                try:
                    return await work(api, c)
                finally:
                    await self.engine.dispose()

    def produce_raw(self, *, timestamp_ms: int | None = None, key: bytes | None = None) -> tuple[str, int, int]:
        """One original raw record; returns (tb-msg-id, partition, offset)."""
        message = make_message(PAYLOAD, key=key, channel="adt", msg_id=uuid7())
        producer = Producer({"bootstrap.servers": self.kafka, "acks": "all", "enable.idempotence": True})
        got: list[tuple[int, int]] = []
        kw: dict[str, Any] = {} if timestamp_ms is None else {"timestamp": timestamp_ms}
        producer.produce(
            self.topic, key=message.key, value=message.payload,
            headers=[(k, v.encode()) for k, v in message.headers.items()],
            on_delivery=lambda err, m: got.append((m.partition(), m.offset())) if err is None else None, **kw,
        )  # fmt: skip
        assert producer.flush(30) == 0 and got
        return message.require(h.TB_MSG_ID), got[0][0], got[0][1]

    def archive_received(self, message_id: str, partition: int, offset: int, **extra: Any) -> None:
        mid = uuid.UUID(message_id)
        insert(
            self.admin, self.db, "message_events", EVENT_COLUMNS,
            [event(datetime.now(UTC), mid, "received", topic=self.topic, partition=partition, offset=offset, **extra)],
        )  # fmt: skip

    def audits(self, action: str) -> list[dict[str, Any]]:
        return [d for _, _, v in self.sender.sent if (d := json.loads(v))["action"] == action]


def op() -> str:
    return uuid7()


def test_reprocess_republishes_the_original_under_a_deterministic_id_and_is_audited(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    original, partition, offset = rig.produce_raw()
    rig.archive_received(original, partition, offset)
    operation = op()

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        url = "/messages/{message_id}/reprocess"
        body = await rig.call(
            api, "POST", url, message_id=original, json_body={"operation_id": operation}, role="tb-operator"
        )
        assert body["original_message_id"] == original and body["topic"] == rig.topic
        assert body["operation_id"] == operation
        again = await rig.call(
            api, "POST", url, message_id=original, json_body={"operation_id": operation}, role="tb-admin"
        )
        assert again["new_message_id"] == body["new_message_id"]  # the same operation_id: the same new id

        records = drain_topic(kafka, rig.topic, partitions=1, expected=3)
        assert len(records) == 3
        clone = header_map(records[1])
        assert clone[h.TB_MSG_ID] == body["new_message_id"] and clone[h.TB_PARENT_ID] == original
        assert records[1].value() == PAYLOAD and records[0].value() == PAYLOAD  # the original bytes, unchanged

        requested = [r for r in rig.audits("message.reprocess") if r["phase"] == "requested"]
        completed = [r for r in rig.audits("message.reprocess") if r["phase"] == "completed"]
        assert len(requested) == 2 and len(completed) == 2
        assert all(_validator().is_valid(r) for r in rig.audits("message.reprocess"))
        assert all(r["allow_reorder"] is False and r["operation_id"] == operation for r in requested)
        assert all(r["count"] == 1 and r["message_id"] == original for r in completed)
        assert b"SYNTH-9" not in b"".join(v for _, _, v in rig.sender.sent)  # no payload in any audit record

    run(rig.session(work))


def test_reprocess_refusals_follow_the_contract_and_nothing_is_published_or_audited(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    original, partition, offset = rig.produce_raw()
    rig.archive_received(original, partition, offset)
    child, cp, co = rig.produce_raw()
    rig.archive_received(child, cp, co, parent_id=uuid.UUID(original))
    scoped, sp, so = rig.produce_raw()
    rig.archive_received(scoped, sp, so)
    insert(
        rig.admin, rig.db, "message_events", EVENT_COLUMNS,
        [event(datetime.now(UTC), uuid.UUID(scoped), "dlq", destination="ehr", status="error", error_code="HTTP_500")],
    )  # fmt: skip
    nocoords = str(msg_id())
    insert(
        rig.admin, rig.db, "message_events", EVENT_COLUMNS, [event(datetime.now(UTC), uuid.UUID(nocoords), "received")]
    )

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        url = "/messages/{message_id}/reprocess"

        async def post(mid: str, *, expect: int, role: str = "tb-operator", body: Any = None) -> Any:
            return await rig.call(
                api, "POST", url, message_id=mid, json_body=body or {"operation_id": op()}, expect=expect, role=role
            )

        assert (await post(str(uuid.uuid4()), expect=404))["code"] == "MESSAGE_NOT_FOUND"
        assert (await post(child, expect=409))["code"] == "CHILD_MESSAGE"
        assert (await post(scoped, expect=409))["code"] == "DESTINATION_SCOPED"
        assert (await post(nocoords, expect=409))["code"] == "NO_PAYLOAD_COORDINATES"
        await post(original, expect=403, role="tb-viewer")
        await post(original, expect=403, role="tb-developer")
        await post(original, expect=400, body={"operation_id": str(uuid.uuid4())})  # not a UUIDv7
        await post(original, expect=400, body={"operation_id": op(), "extra": 1})
        assert rig.audits("message.reprocess") == []  # every refusal came before the audit record
        assert len(drain_topic(kafka, rig.topic, partitions=1, expected=3)) == 3  # only the three originals

    run(rig.session(work))


def test_ordering_would_break_is_refused_for_a_per_key_channel_unless_allow_reorder(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka, per_key=True)
    original, partition, offset = rig.produce_raw(key=b"SYNTH-KEY-1")
    rig.archive_received(original, partition, offset)

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        url = "/messages/{message_id}/reprocess"
        refused = await rig.call(
            api, "POST", url, message_id=original, json_body={"operation_id": op()}, expect=409, role="tb-operator"
        )
        assert refused["code"] == "ORDERING_WOULD_BREAK"
        assert (
            rig.audits("message.reprocess") == [] and len(drain_topic(kafka, rig.topic, partitions=1, expected=1)) == 1
        )
        ok = await rig.call(
            api, "POST", url, message_id=original,
            json_body={"operation_id": op(), "allow_reorder": True}, role="tb-operator",
        )  # fmt: skip
        assert ok["original_message_id"] == original
        [requested] = [r for r in rig.audits("message.reprocess") if r["phase"] == "requested"]
        assert requested["allow_reorder"] is True  # the decision to reorder is on the record

    run(rig.session(work))


def test_reprocess_needs_the_channel_deployed_and_fails_closed_without_audit(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    undeployed = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka, deployed=False)
    original, partition, offset = undeployed.produce_raw()
    undeployed.archive_received(original, partition, offset)

    async def not_deployed(api: Api, c: httpx.AsyncClient) -> None:
        refused = await undeployed.call(
            api, "POST", "/messages/{message_id}/reprocess", message_id=original,
            json_body={"operation_id": op()}, expect=409, role="tb-operator",
        )  # fmt: skip
        assert refused["code"] == "NOT_DEPLOYED"

    run(undeployed.session(not_deployed))

    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    original, partition, offset = rig.produce_raw()
    rig.archive_received(original, partition, offset)

    async def refuse(env: str, key: bytes, value: bytes) -> None:
        if json.loads(value)["action"] == "message.reprocess":
            raise AuditSendError
        rig.sender.sent.append((env, key, value))

    async def closed(api: Api, c: httpx.AsyncClient) -> None:
        rig.sender.send = refuse  # type: ignore[method-assign]
        body = await rig.call(
            api, "POST", "/messages/{message_id}/reprocess", message_id=original,
            json_body={"operation_id": op()}, expect=503, role="tb-operator",
        )  # fmt: skip
        assert body["code"] == "AUDIT_UNAVAILABLE" and "republished" not in body
        assert len(drain_topic(kafka, rig.topic, partitions=1, expected=1)) == 1  # nothing was re-published

    run(rig.session(closed))


# -- replay ----------------------------------------------------------------------------------------------


def wait_replay(rig: IngestRig, api: Api, replay_id: str, *, status: str, timeout_s: float = 60.0) -> dict[str, Any]:
    async def poll() -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while True:
            got = await rig.call(api, "GET", "/replays/{replay_id}", replay_id=replay_id, role="tb-viewer")
            if got["status"] == status:
                return got  # type: ignore[no-any-return]
            assert time.monotonic() < deadline, got
            await asyncio.sleep(0.3)

    return poll()  # type: ignore[return-value]


def ts(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"


def test_replay_runs_in_the_background_is_idempotent_and_audited(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    base = datetime.now(UTC) - timedelta(hours=1)
    for i in range(3):
        rig.produce_raw(timestamp_ms=int((base + timedelta(minutes=i)).timestamp() * 1000))
    outside = rig.produce_raw(timestamp_ms=int((base - timedelta(hours=2)).timestamp() * 1000))  # noqa: F841
    replay_id = op()
    body = {"replay_id": replay_id, "start": ts(base - timedelta(minutes=1)), "end": ts(base + timedelta(minutes=10))}

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        started = await rig.call(api, "POST", "/replays", json_body=body, expect=202, role="tb-operator")
        assert started["status"] == "running" and started["target_topic"] == rig.topic
        done = await wait_replay(rig, api, replay_id, status="completed")
        assert done["republished"] == 3 and done["skipped_undecodable"] == 0
        records = drain_topic(kafka, rig.topic, partitions=1, expected=7)
        assert len(records) == 7  # four originals + three copies
        assert sum(1 for r in records if h.TB_PARENT_ID in header_map(r)) == 3

        same = await rig.call(api, "POST", "/replays", json_body=body, expect=200, role="tb-admin")
        assert same["status"] == "completed" and same["republished"] == 3  # idempotent: no second run
        assert len(drain_topic(kafka, rig.topic, partitions=1, expected=7)) == 7
        different = {**body, "max_records": 5}
        reused = await rig.call(api, "POST", "/replays", json_body=different, expect=409, role="tb-operator")
        assert reused["code"] == "REQUEST_ID_REUSED"

        listing = await rig.call(api, "GET", "/replays", params={"limit": 10}, role="tb-viewer")
        assert [r["replay_id"] for r in listing["items"]] == [replay_id] and listing["next_cursor"] is None
        await rig.call(api, "GET", "/replays/{replay_id}", replay_id=uuid7(), expect=404, role="tb-viewer")

        requested = [r for r in rig.audits("channel.replay") if r["phase"] == "requested"]
        completed = [r for r in rig.audits("channel.replay") if r["phase"] == "completed"]
        assert len(requested) == 1 and len(completed) == 1 and completed[0]["count"] == 3
        assert requested[0]["target_topic"] == rig.topic and requested[0]["allow_reorder"] is False
        assert requested[0]["operation_id"] == replay_id
        assert all(_validator().is_valid(r) for r in rig.audits("channel.replay"))

    run(rig.session(work))


def test_replay_refusals_come_before_202_and_before_any_audit_record(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka, per_key=True)
    now = datetime.now(UTC)
    good = {"replay_id": op(), "start": ts(now - timedelta(hours=1)), "end": ts(now)}

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        async def post(body: dict[str, Any], expect: int, role: str = "tb-operator") -> Any:
            return await rig.call(api, "POST", "/replays", json_body=body, expect=expect, role=role)

        assert (await post(good, 409))["code"] == "ORDERING_WOULD_BREAK"
        await post({**good, "end": good["start"]}, 422)
        await post({**good, "allow_reorder": True, "target": "other"}, 400)  # there is no target field
        await post({**good, "replay_id": str(uuid.uuid4())}, 400)
        await post(good, 403, role="tb-viewer")
        assert rig.audits("channel.replay") == []
        listing = await rig.call(api, "GET", "/replays", role="tb-viewer")
        assert listing["items"] == []  # a refused request leaves no record

    run(rig.session(work))

    undeployed = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka, deployed=False)

    async def not_deployed(api: Api, c: httpx.AsyncClient) -> None:
        refused = await undeployed.call(api, "POST", "/replays", json_body=good, expect=409, role="tb-operator")
        assert refused["code"] == "NOT_DEPLOYED"

    run(undeployed.session(not_deployed))


def test_a_replay_whose_audit_is_refused_ends_failed_and_a_failed_one_runs_again(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    base = datetime.now(UTC) - timedelta(hours=1)
    rig.produce_raw(timestamp_ms=int(base.timestamp() * 1000))
    replay_id = op()
    body = {"replay_id": replay_id, "start": ts(base - timedelta(minutes=1)), "end": ts(base + timedelta(minutes=10))}

    async def work(api: Api, c: httpx.AsyncClient) -> None:
        healthy = rig.sender.send

        async def refuse(env: str, key: bytes, value: bytes) -> None:
            if json.loads(value)["action"] == "channel.replay":
                raise AuditSendError
            await healthy(env, key, value)

        rig.sender.send = refuse  # type: ignore[method-assign]
        await rig.call(api, "POST", "/replays", json_body=body, expect=202, role="tb-operator")
        failed = await wait_replay(rig, api, replay_id, status="failed")
        assert failed["failure_code"] == "AUDIT_UNAVAILABLE" and failed["republished"] == 0
        assert len(drain_topic(kafka, rig.topic, partitions=1, expected=1)) == 1  # nothing was read or published

        rig.sender.send = healthy  # type: ignore[method-assign]
        again = await rig.call(api, "POST", "/replays", json_body=body, expect=202, role="tb-operator")
        assert again["status"] == "running"
        done = await wait_replay(rig, api, replay_id, status="completed")
        assert done["republished"] == 1

    run(rig.session(work))


def test_a_running_replay_past_its_deadline_reads_as_interrupted() -> None:
    """The process that started it is gone: no sweeper, the read says so (openapi/v0 README 3.14)."""
    from types import SimpleNamespace

    start = datetime(2026, 10, 1, tzinfo=UTC)
    row = SimpleNamespace(
        replay_id=uuid.UUID(uuid7()), range_start=start, range_end=start + timedelta(hours=1), allow_reorder=False,
        max_records=10, deadline_s=60, requested_by="idp|1", requested_at=start, deadline_at=start + timedelta(seconds=60),
        status="running", finished_at=None, republished=None, failure_code=None,
    )  # fmt: skip
    shown = render.replay(row, "dev", "adt", now=start + timedelta(seconds=30))  # type: ignore[arg-type]
    assert shown["status"] == "running" and "failure_code" not in shown
    late = render.replay(copy.copy(row), "dev", "adt", now=start + timedelta(seconds=61))  # type: ignore[arg-type]
    assert (late["status"], late["failure_code"], late["republished"]) == ("failed", "INTERRUPTED", 0)
    assert late["finished_at"] == "2026-10-01T00:01:00.000Z"


# -- review hardening ------------------------------------------------------------------------------------


def slow_publisher_factory(delay_s: float) -> Any:
    """A real publisher that takes `delay_s` per record, so a replay is still running when the API stops."""
    from tributary.archiver.republish import RawPublisher
    from tributary.runtime.clients import producer_config

    class Slow(RawPublisher):
        def publish(self, topic: str, message: Any) -> None:
            time.sleep(delay_s)
            super().publish(topic, message)

    def make(settings: Any, defaults: Any, *, client_id: str) -> RawPublisher:
        return Slow(Producer(producer_config(settings, defaults, client_id=client_id)))

    return make


def test_stopping_the_api_during_a_replay_ends_it_before_the_publisher_closes_and_records_the_count(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    from tributary.api.db.engine import make_engine, make_session_factory
    from tributary.api.replays import ReplayStore

    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka, publisher_factory=slow_publisher_factory(0.3))
    base = datetime.now(UTC) - timedelta(hours=1)
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    originals = 300
    for i in range(originals):
        message = make_message(PAYLOAD, channel="adt", msg_id=uuid7())
        producer.produce(
            rig.topic, value=message.payload, headers=[(k, v.encode()) for k, v in message.headers.items()],
            timestamp=int((base + timedelta(seconds=i)).timestamp() * 1000),
        )  # fmt: skip
    assert producer.flush(60) == 0
    replay_id = op()
    body = {"replay_id": replay_id, "start": ts(base - timedelta(minutes=1)), "end": ts(base + timedelta(hours=1))}

    async def go() -> None:
        async with rig.app.router.lifespan_context(rig.app):
            await rig.app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=rig.app), base_url="http://test") as c:
                api = Api(c, rig.idp, rig.contract)
                await api.call(
                    "POST", "/v1/channels", expect=201,
                    json_body={"document": document("adt"), "origin": {"kind": "api"}},
                )  # fmt: skip
                await api.deploy(rig.env, "adt", 1)
                await rig.call(api, "POST", "/replays", json_body=body, expect=202, role="tb-operator")
                deadline = time.monotonic() + 60
                while len(drain_topic(kafka, rig.topic, partitions=1, expected=0, timeout_s=5)) < originals + 2:
                    assert time.monotonic() < deadline
                    await asyncio.sleep(0.3)
        # leaving the lifespan ran `Reingest.close()`: the replay thread has ended before the sinks closed

    run(go())
    engine = make_engine(tls_pg.plain_url)

    async def after() -> dict[str, Any]:
        try:
            return await ReplayStore(make_session_factory(engine)).get(rig.env, "adt", replay_id)
        finally:
            await engine.dispose()

    record = run(after())
    assert record["status"] == "failed" and record["failure_code"] == "INTERRUPTED"
    assert 0 < record["republished"] < originals
    settled = len(drain_topic(kafka, rig.topic, partitions=1, expected=0, timeout_s=5))
    time.sleep(1.5)
    assert (
        len(drain_topic(kafka, rig.topic, partitions=1, expected=0, timeout_s=5)) == settled
    )  # nothing after the stop
    assert settled - originals == record["republished"]  # and the recorded count is what the broker acknowledged


def test_a_replay_id_from_another_channel_is_not_found_and_a_rerun_keeps_who_first_asked(
    tls_pg: TlsPg, idp: Idp, contract: Contract, clickhouse_admin: Admin, kafka: str
) -> None:
    from tributary.api.db.engine import make_engine, make_session_factory
    from tributary.api.replays import ReplayStore
    from tributary.api.schemas import ReplayRequest

    rig = IngestRig(tls_pg, idp, contract, clickhouse_admin, kafka)
    now_ = datetime.now(UTC)
    request = ReplayRequest(replay_id=op(), start=ts(now_ - timedelta(hours=1)), end=ts(now_))
    start, end = now_ - timedelta(hours=1), now_

    async def go() -> None:
        async def work(api: Api, c: httpx.AsyncClient) -> None:
            await api.call(
                "POST", "/v1/channels", expect=201, json_body={"document": document("oru"), "origin": {"kind": "api"}}
            )  # fmt: skip
            engine = make_engine(tls_pg.plain_url)
            store = ReplayStore(make_session_factory(engine))
            try:
                first, started = await store.begin(rig.env, "adt", request, start, end, "idp|first")
                assert started and first["requested_by"] == "idp|first"
                with pytest.raises(Exception) as e:
                    await store.begin(rig.env, "oru", request, start, end, "idp|other")
                assert getattr(e.value, "code", None) == "NOT_FOUND"  # not 409: another channel's id is not ours
                await store.finish_failed(request.replay_id, "SOURCE_ERROR", 0)
                with pytest.raises(Exception) as e2:
                    await store.begin(rig.env, "adt", request, start, end, "idp|second")
                assert getattr(e2.value, "code", None) == "REQUEST_ID_REUSED"  # another subject cannot restart it
                again, started = await store.begin(rig.env, "adt", request, start, end, "idp|first")
                assert started and again["status"] == "running"
                assert again["requested_by"] == "idp|first"
            finally:
                await engine.dispose()

        await rig.session(work)

    run(go())
