"""The audit hook (W3-32): record shape per `contracts/audit/v0`, fail-closed `requested`, best-effort follow-ups,
the same `audit_id` and bytes on every retry. No broker: a scripted sender."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from api_kit import REPO
from tributary.api.audit import Actor, AuditHook, AuditSendError, build_record, encode
from tributary.api.problems import ApiProblem

ACTOR = Actor("idp|1234", frozenset({"operator", "admin"}), "11111111-2222-4333-8444-555555555555")
MSG = "0191a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5b"
OP = "0191a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5c"


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def __getattr__(self, level: str) -> Any:
        return lambda event, /, **f: self.events.append((event, f))


class Sender:
    def __init__(self, *outcomes: str) -> None:
        self.outcomes = list(outcomes)  # "ok", "lost" (not delivered), "unknown" (delivered, reported failed)
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send(self, env: str, key: bytes, value: bytes) -> None:
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        if outcome != "lost":
            self.sent.append((env, key, value))
        if outcome != "ok":
            raise AuditSendError


def run[T](coro: Any) -> T:
    return asyncio.run(coro)  # type: ignore[no-any-return]


def test_every_action_builds_a_valid_record_and_a_subject_with_a_pipe_is_accepted() -> None:
    cases: list[dict[str, Any]] = [
        {
            "action": "message.content.view",
            "phase": "requested",
            "channel": "adt",
            "message_id": MSG,
            "stage": "received",
        },
        {
            "action": "message.reprocess",
            "phase": "requested",
            "channel": "adt",
            "message_id": MSG,
            "operation_id": OP,
            "allow_reorder": False,
        },
        {
            "action": "message.reprocess",
            "phase": "failed",
            "channel": "adt",
            "message_id": MSG,
            "operation_id": OP,
            "count": 0,
            "error_code": "PRODUCE_FAILED",
        },
        {
            "action": "channel.replay",
            "phase": "requested",
            "channel": "adt",
            "operation_id": OP,
            "target_topic": "dev.adt.raw",
            "range_start": datetime(2026, 10, 1, tzinfo=UTC),
            "range_end": datetime(2026, 10, 2, tzinfo=UTC),
            "allow_reorder": True,
        },
        {"action": "deployment.undeploy", "phase": "requested", "channel": "adt", "deployment_id": MSG},
        {
            "action": "deployment.rollback",
            "phase": "requested",
            "channel": "adt",
            "deployment_id": MSG,
            "version": 1,
            "previous_version": 2,
        },
    ]
    for case in cases:
        record = build_record(environment="dev", actor=ACTOR, **case)
        assert record["actor"] == "idp|1234" and record["actor_roles"] == ["admin", "operator"]
        assert record["audit_id"][14] == "7"  # UUIDv7
        assert len(encode(record)) <= 8192


def test_a_replay_target_that_is_not_the_channels_raw_topic_is_refused() -> None:
    with pytest.raises(ValueError):
        build_record(
            action="channel.replay",
            phase="requested",
            environment="dev",
            actor=ACTOR,
            channel="adt",
            operation_id=OP,
            target_topic="dev.other.raw",
            range_start=datetime(2026, 1, 1, tzinfo=UTC),
            range_end=datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_a_record_with_a_missing_required_field_or_an_unknown_field_is_refused_without_echoing_values() -> None:
    with pytest.raises(ValueError) as e:
        build_record(action="message.content.view", phase="requested", environment="dev", actor=ACTOR, channel="adt")
    with pytest.raises(ValueError):
        build_record(
            action="deployment.undeploy",
            phase="requested",
            environment="dev",
            actor=ACTOR,
            channel="adt",
            deployment_id=MSG,
            payload="SECRET-BYTES",
        )
    assert "SECRET" not in str(e.value)


def test_requested_is_acknowledged_before_it_returns_and_carries_the_contract_shape() -> None:
    sender, log = Sender(), Log()
    audit_id = run(
        AuditHook(sender, log).requested(
            action="message.content.view",
            environment="dev",
            actor=ACTOR,
            channel="adt",
            message_id=MSG,
            stage="routed",
            destination="http-stub",
        )
    )
    [(env, key, value)] = sender.sent
    doc = json.loads(value)
    assert (env, key) == ("dev", b"idp|1234") and doc["audit_id"] == audit_id
    assert doc["phase"] == "requested" and doc["stage"] == "routed" and doc["destination"] == "http-stub"
    assert set(doc) <= {
        "v",
        "audit_id",
        "ts",
        "environment",
        "action",
        "phase",
        "actor",
        "actor_roles",
        "request_id",
        "channel",
        "message_id",
        "stage",
        "destination",
    }


def test_requested_fails_closed_with_audit_unavailable_and_logs_no_identifiers() -> None:
    sender, log = Sender("lost", "lost"), Log()
    with pytest.raises(ApiProblem) as e:
        run(
            AuditHook(sender, log, retry_pause_s=0).requested(
                action="message.content.view",
                environment="dev",
                actor=ACTOR,
                channel="adt",
                message_id=MSG,
                stage="received",
            )
        )
    assert (e.value.code, e.value.status) == ("AUDIT_UNAVAILABLE", 503)
    assert log.events == [("audit_requested_failed", {"action": "message.content.view", "environment": "dev"})]
    assert sender.sent == []


def test_a_retry_after_an_unknown_outcome_reuses_the_same_audit_id_and_bytes() -> None:
    sender = Sender("unknown", "ok")  # the first was delivered but reported failed: the doubt a retry resolves
    audit_id = run(
        AuditHook(sender, Log(), retry_pause_s=0).requested(
            action="deployment.deploy", environment="dev", actor=ACTOR, channel="adt", deployment_id=MSG, version=3
        )
    )
    assert len(sender.sent) == 2 and sender.sent[0][2] == sender.sent[1][2]
    assert json.loads(sender.sent[0][2])["audit_id"] == audit_id


def test_followups_are_best_effort_and_log_the_class_only() -> None:
    sender, log = Sender("lost", "lost"), Log()
    delivered = run(
        AuditHook(sender, log, retry_pause_s=0).followup(
            action="message.reprocess",
            phase="completed",
            environment="dev",
            actor=ACTOR,
            channel="adt",
            message_id=MSG,
            operation_id=OP,
            count=1,
        )
    )
    assert delivered is False
    assert log.events == [
        ("audit_followup_failed", {"action": "message.reprocess", "phase": "completed", "environment": "dev"})
    ]


def test_the_contract_examples_validate_with_the_same_schema() -> None:
    """Guards against the hook's schema path drifting from the contract's own examples."""
    from tributary.api.audit import _validator

    for path in sorted((REPO / "contracts/audit/v0/examples/valid").glob("*.json")):
        assert _validator().is_valid(json.loads(path.read_text())), path.name


def test_the_reconciler_reads_the_channels_topic_as_the_platform_group_of_its_environment() -> None:
    from tributary.api.main import reconciler_consumer_config
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.config import load_topic_defaults

    for env in ("dev", "prod"):
        conf = reconciler_consumer_config(KafkaSettings(bootstrap_servers="127.0.0.1:9092"), load_topic_defaults(), env)
        assert conf["group.id"] == f"tb.{env}.reconciler"
        assert conf["enable.auto.commit"] is False


def test_a_kafka_sender_waits_on_a_monotonic_deadline_not_a_count_of_polls() -> None:
    """`poll` returns at once when another thread's callback runs; counting polls would give up early and re-send
    a record the broker is about to acknowledge."""
    import threading

    from tributary.api.audit import KafkaAuditSender

    class Producer:
        def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: Any) -> None:
            threading.Timer(0.5, on_delivery, args=(None, None)).start()  # the broker answers after 0.5 s

        def poll(self, _t: float) -> int:
            return 0  # returns immediately, as it does when another thread is served

    asyncio.run(KafkaAuditSender(Producer(), timeout_s=1.0).send("dev", b"a", b"{}"))

    class Silent(Producer):
        def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: Any) -> None:
            return None

    import time

    started = time.monotonic()
    with pytest.raises(AuditSendError):
        asyncio.run(KafkaAuditSender(Silent(), timeout_s=0.5).send("dev", b"a", b"{}"))
    assert 2.0 <= time.monotonic() - started < 6.0  # timeout + the 2 s backstop, by the clock


def test_the_control_plane_cannot_be_built_without_an_audit_hook() -> None:
    from tributary.api.service import ControlPlane

    with pytest.raises(TypeError):
        ControlPlane(None, None, Log())  # type: ignore[call-arg,arg-type]


def test_the_reconciler_grace_must_cover_the_audit_step_too() -> None:
    from tributary.api import config
    from tributary.api.errors import ConfigurationError

    base = {
        "TB_DATABASE_HOST": "db.example",
        "TB_DATABASE_NAME": "tb",
        "TB_DATABASE_USER": "u",
        "TB_DATABASE_SECRET_REF": "r",
        "TB_OIDC_ISSUER": "https://idp.example/realms/tb",
        "TB_OIDC_AUDIENCE": "tb-api",
        "TB_OIDC_ROLE_MAP": '{"tb-admin": "admin"}',
    }
    config.from_env({**base, "TB_AUDIT_TIMEOUT_S": "20", "TB_RECONCILE_PENDING_GRACE_S": "90"})  # 2*20+10+30 = 80
    with pytest.raises(ConfigurationError) as e:
        config.from_env({**base, "TB_AUDIT_TIMEOUT_S": "20", "TB_RECONCILE_PENDING_GRACE_S": "70"})
    assert e.value.code == "RECONCILE_GRACE_OUT_OF_RANGE"
