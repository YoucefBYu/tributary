"""W3-23: all four receivers under their **exact** set 1.4 grants on a deny-by-default broker.

The ACL table has been wrong on every first deny-by-default run (set 1.4 found a missing spec READ, a
missing group, a missing transactional-id grant, unused `.retry` grants and an undelimited prefix). The
receivers' own integration tests run on a PLAINTEXT loopback broker with no authorizer, so none of them
could notice a missing grant. Here each real receiver process (`python -m tributary.receivers.<kind>`)
runs as its own SCRAM principal on a `SASL_SSL` broker with `StandardAuthorizer` and
`allow.everyone.if.no.acl.found=false`, holding the grants **parsed from the contract**
(`contracts/topics/v0/topic-defaults.json` `security.acls.receiver`, through the lite stack's own parser)
and nothing else. See `tests/support/secure_broker.py`.

What is claimed, and the control that fails for the reason named:

* **Positive, per receiver** (`mllp`, `http`, `sftp`, `kafka`): under exactly those grants it starts from
  `_tb.<env>.channels`, accepts a message, commits it to `raw`, and the broker's authorizer log holds
  **no denial at all** for that principal (a denial the receiver tolerates would be a grant the table
  lacks).
* **No spec READ**: the same principal minus READ on `_tb.<env>.channels` never listens and stays not ready
  (`spec_unavailable`; the spec is on the topic), and the broker's authorizer log shows `Denied ...
  Topic:LITERAL:_tb.<env>.channels` for that principal. Control: granting the one missing READ makes the very
  same process start, so the denial was the only cause.
* **No spec-reader group READ**: see `test_without_the_spec_reader_group_grant`, which records what the
  receiver actually does.
* **No WRITE on any non-`raw` topic**: with the receiver's own client config, a produce to `events`,
  `out.<dest>`, `dlq`, `reply`, `_tb.<env>.channels` and another channel's `raw` fails with
  `TOPIC_AUTHORIZATION_FAILED` (the broker's answer, not `_MSG_TIMED_OUT`), within seconds, and each refusal
  is in the authorizer log. Control: a produce to the receiver's own `raw` succeeds on the same producer.
* **No READ on `raw`**: a consumer in the receiver's granted spec-reader group prefix still gets
  `TOPIC_AUTHORIZATION_FAILED` on the receiver's own `raw`.
* **Sync reply** (`mllp`, set 1.2): with the extra READ on `reply` and the LITERAL replier group the reply
  consumer holds its partitions; without the group grant it does not start, and the broker denies the group;
  and WRITE on `reply` is refused (a receiver cannot forge an answer).

Why the authorizer log: the receivers do not log the broker's error (`spec_unavailable` carries no code,
by design: invariant 6), so the broker's own record is the only place "authorization, not a timeout" can
be shown. Synthetic payloads only; MSH-3 is `TRIBUTARY-SYNTH`.
"""

from __future__ import annotations

import http.client
import importlib.util
import json
import socket
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import paramiko  # type: ignore[import-untyped]
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import NewTopic
from tributary.runtime.clients import consumer_config, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic, reply_topic

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import drain_topic, free_port
from tests.support.mllp_receiver import publish_channel_spec
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import http_document, kafka_source_document, sftp_document
from tests.support.secure_broker import (
    Grant,
    SecureBroker,
    contract_grants,
    secure_broker,
    sync_reply_receiver_grants,
)

pytestmark = pytest.mark.integration

ENV = "aclt"
DESTINATION = "stub"
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
CHANNELS_READ = Grant("TOPIC", channels_topic(ENV), "LITERAL", "READ")
SFTP_USER, SFTP_PASSWORD = "synth-user", "synth-pass"  # tb-secret-scan: allow
MODULES = {
    "mllp": "tributary.receivers.mllp",
    "http": "tributary.receivers.http",
    "sftp": "tributary.receivers.sftp",
    "kafka": "tributary.receivers.kafka_source",
}


def _load_fake_sftp_server() -> Any:
    """The receivers' in-process SFTP server (a paramiko test helper), loaded by path: tests/ must not
    import another package's test tree by module name."""
    path = Path(__file__).resolve().parents[2] / "packages/tributary-receivers/tests/sftp/fake_server.py"
    spec = importlib.util.spec_from_file_location("tb_qa_fake_sftp_server", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------------------------------
# Fixtures


@pytest.fixture(scope="module")
def broker(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SecureBroker]:
    with secure_broker(tmp_path_factory.mktemp("acl-broker")) as running:
        running.create_topics([NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"})])
        # Platform topics that exist so that a refused write is the ACL's doing, not a missing topic.
        running.create_topics([NewTopic(events_topic(ENV), 1, 1)])
        yield running


@pytest.fixture
def procs() -> Iterator[list[ManagedProcess]]:
    started: list[ManagedProcess] = []
    yield started
    for proc in started:
        proc.terminate_and_wait()


@dataclass
class Deployed:
    """One channel for one receiver kind, its principal and the grants that principal was given."""

    kind: str
    channel: str
    principal: str
    grants: list[Grant]
    workdir: Path


_serial = iter(range(10_000))


def deploy(
    broker: SecureBroker,
    tmp_path: Path,
    kind: str,
    *,
    drop: tuple[Grant, ...] = (),
    sync_reply: bool = False,
    reply_topic_exists: bool = False,
    extra_grants: tuple[Grant, ...] = (),
    channel: str | None = None,
) -> Deployed:
    """Topics for a fresh channel and a principal holding the contract's receiver grants minus `drop`."""
    n = next(_serial)
    channel = channel or f"acl-{kind}-{n}"
    principal = f"tb-acl-{kind}-{n}"
    topics = [raw_topic(ENV, channel), out_topic(ENV, channel, DESTINATION), dlq_topic(ENV, channel)]
    if sync_reply or reply_topic_exists:
        topics.append(reply_topic(ENV, channel))
    broker.create_topics([NewTopic(name, 1, 1) for name in topics])
    grants = contract_grants("receiver", env=ENV, channel=channel)
    if sync_reply:
        grants += sync_reply_receiver_grants(ENV, channel)
    grants = [g for g in grants if g not in drop] + list(extra_grants)
    broker.provision(principal, grants)
    return Deployed(kind, channel, principal, grants, tmp_path)


def start(
    broker: SecureBroker, d: Deployed, procs: list[ManagedProcess], *, instance: str | None = None
) -> ManagedProcess:
    args = [
        sys.executable,
        "-m",
        MODULES[d.kind],
        "--channel",
        d.channel,
        "--env",
        ENV,
        "--instance",
        instance or f"{d.kind}-acl",
        "--secrets-dir",
        str(broker.secrets_dir),
    ]
    proc = ManagedProcess(args, env=broker.process_env(d.workdir, d.principal))
    procs.append(proc)
    return proc


def mllp_spec(channel: str, port: int, *, reply: dict[str, Any] | None = None) -> dict[str, Any]:
    doc = walking_skeleton_document(
        mllp_port=port, http_url="http://127.0.0.1:1/x", channel=channel, destination=DESTINATION, version=1
    )
    doc["spec"]["source"]["bind"] = "127.0.0.1"
    if reply is not None:
        doc["spec"]["source"]["reply"] = reply
    return doc


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def mllp_exchange(port: int, payload: bytes) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
        sock.sendall(SB + payload + EB + CR)
        sock.settimeout(15)
        buf = b""
        while not buf.endswith(EB + CR):
            chunk = sock.recv(4096)
            if not chunk:
                return ""
            buf += chunk
    return buf.split(b"\rMSA|")[1].split(b"|")[0].decode()


def http_post(port: int) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        conn.request(
            "POST",
            "/ingest",
            body=b'{"resourceType": "Patient", "id": "SYNTH1"}',
            headers={"Content-Type": "application/fhir+json"},
        )
        return conn.getresponse().status
    finally:
        conn.close()


def wait_until(predicate: Callable[[], bool], what: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.2)


def publish_via_admin(broker: SecureBroker, channel: str, document: dict[str, Any]) -> None:
    """The control plane's job. The spec is written by the test's super user, never by the receiver."""
    publish_channel_spec(broker.admin, ENV, channel, document)


def never_listened(proc: ManagedProcess) -> bool:
    proc.collect_pending()
    return not any(line.get("event") == "listening" for line in proc.lines)


def principal_client_config(
    broker: SecureBroker, d: Deployed, *, kind: str, group_id: str | None = None
) -> dict[str, Any]:
    """The receiver's own client settings (engine-kafka document, contract defaults), built the way the
    receiver builds them, for a probe that acts as the receiver's principal."""
    env = broker.process_env(d.workdir, d.principal)
    settings = load_engine_kafka(
        {"TB_ENGINE_KAFKA_FILE": env["TB_ENGINE_KAFKA_FILE"], "TB_SECRETS_DIR": env["TB_SECRETS_DIR"]}
    )
    defaults = load_topic_defaults()
    if kind == "producer":
        return producer_config(settings, defaults, client_id=f"{d.principal}-probe")
    assert group_id is not None
    return consumer_config(settings, defaults, group_id=group_id, enable_partition_eof=True)


# ---------------------------------------------------------------------------------------------------
# Positive: each receiver under its exact grants


def _run_mllp(broker: SecureBroker, d: Deployed, proc: ManagedProcess, port: int) -> None:
    proc.wait_for_event("listening", timeout_s=60)
    assert mllp_exchange(port, adt("ACL-MLLP-1")) == "AA"


def _run_http(broker: SecureBroker, d: Deployed, proc: ManagedProcess, port: int) -> None:
    proc.wait_for_event("listening", timeout_s=60)
    assert http_post(port) == 202


@pytest.mark.parametrize("kind", ["mllp", "http"])
def test_listener_receivers_run_under_their_exact_grants_with_no_denial(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess], kind: str
) -> None:
    d = deploy(broker, tmp_path, kind)
    port = free_port()
    publish_via_admin(
        broker, d.channel, mllp_spec(d.channel, port) if kind == "mllp" else http_document(d.channel, port=port)
    )
    proc = start(broker, d, procs)
    (_run_mllp if kind == "mllp" else _run_http)(broker, d, proc, port)

    records = drain_topic(broker.admin, raw_topic(ENV, d.channel), partitions=1, expected=1)
    assert len(records) == 1  # ACKed, so committed to raw under the grants
    assert broker.denials_for(d.principal) == []


def test_sftp_receiver_runs_under_its_exact_grants_with_no_denial(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess]
) -> None:
    fake = _load_fake_sftp_server()
    root = tmp_path / "srv"
    for sub in ("in", "in/rejected"):
        (root / sub).mkdir(parents=True)
    cred = broker.secrets_dir / "acl-sftp-cred"
    cred.mkdir()
    (cred / "username").write_text(SFTP_USER, encoding="utf-8")
    (cred / "password").write_text(SFTP_PASSWORD, encoding="utf-8")
    key = paramiko.RSAKey.generate(2048)
    from tributary.receivers.sftp.session import fingerprint

    d = deploy(broker, tmp_path, "sftp")
    (root / "in" / "a.hl7").write_bytes(adt("ACL-SFTP-1"))
    with fake.FakeSftpServer(root, [key]) as srv:
        publish_via_admin(
            broker,
            d.channel,
            sftp_document(
                d.channel,
                host="127.0.0.1",
                port=srv.port,
                host_key=fingerprint(key.asbytes()),
                secret_ref="acl-sftp-cred",  # noqa: S106
            ),
        )
        proc = start(broker, d, procs)
        proc.wait_for_event("listening", timeout_s=60)
        # The file leaves the source directory only once its record is committed to raw.
        wait_until(lambda: not (root / "in" / "a.hl7").exists(), "a.hl7 committed and removed")
    assert len(drain_topic(broker.admin, raw_topic(ENV, d.channel), partitions=1, expected=1)) == 1
    assert broker.denials_for(d.principal) == []


def test_kafka_source_receiver_runs_under_its_exact_grants_with_no_denial(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess]
) -> None:
    """The source cluster is the broker's PLAINTEXT loopback listener (a different principal and listener from
    the engine link, which is what "foreign" means to the receiver); the engine link is the SASL_SSL one."""
    d = deploy(broker, tmp_path, "kafka")
    source_topic = f"upstream.{d.channel}"
    broker.create_topics([NewTopic(source_topic, 1, 1)])
    producer = Producer({"bootstrap.servers": broker.admin, "acks": "all"})
    for n in range(3):
        producer.produce(source_topic, value=json.dumps({"n": n, "note": "TRIBUTARY-SYNTH"}).encode())
    assert producer.flush(30) == 0
    publish_via_admin(broker, d.channel, kafka_source_document(d.channel, bootstrap=broker.admin, topic=source_topic))
    proc = start(broker, d, procs)
    proc.wait_for_event("listening", timeout_s=60)
    records = drain_topic(broker.admin, raw_topic(ENV, d.channel), partitions=1, expected=3)
    assert sorted(json.loads(r.value())["n"] for r in records) == [0, 1, 2]
    assert broker.denials_for(d.principal) == []


# ---------------------------------------------------------------------------------------------------
# Negative: no spec READ


def _deploy_spec_for(broker: SecureBroker, d: Deployed, port: int, root: Path | None = None) -> None:
    """Publish a spec for `d` that the receiver can serve if it can read it (sftp/kafka point at nothing
    reachable: the negative cases never get that far, and the control only needs `listening`)."""
    if d.kind == "mllp":
        document = mllp_spec(d.channel, port)
    elif d.kind == "http":
        document = http_document(d.channel, port=port)
    elif d.kind == "sftp":
        document = sftp_document(d.channel, host="127.0.0.1", port=port)
    else:
        document = kafka_source_document(d.channel, bootstrap=broker.admin, topic=f"upstream.{d.channel}")
        broker.create_topics([NewTopic(f"upstream.{d.channel}", 1, 1)])
    publish_via_admin(broker, d.channel, document)


@pytest.mark.parametrize("kind", ["mllp", "http", "sftp", "kafka"])
def test_without_spec_read_the_receiver_never_listens_and_the_broker_denies(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess], kind: str
) -> None:
    d = deploy(broker, tmp_path, kind, drop=(CHANNELS_READ,))
    assert CHANNELS_READ not in d.grants
    port = free_port()
    _deploy_spec_for(broker, d, port)  # the spec IS on the topic: "not deployed" is not the explanation
    proc = start(broker, d, procs)

    proc.wait_for_event("spec_unavailable", timeout_s=60)
    # The broker's answer, not a timeout: its authorizer logs the refusal against this principal and topic.
    wait_until(
        lambda: bool(broker.denials_for(d.principal, resource=f"Topic:LITERAL:{channels_topic(ENV)}")),
        "an authorizer denial on the channels topic",
        timeout_s=30,
    )
    denial = broker.denials_for(d.principal, resource=f"Topic:LITERAL:{channels_topic(ENV)}")[0]
    assert "DefaultDeny" in denial  # denied because nothing allows it, not by an explicit DENY rule
    assert never_listened(proc) and proc.process.poll() is None  # not ready, still retrying
    if kind in ("mllp", "http"):
        with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=2):
            pass

    # Control: the one missing READ is the only cause. Grant it and the same process starts.
    broker.grant(d.principal, [CHANNELS_READ])
    proc.wait_for_event("listening", timeout_s=90)


def test_without_the_spec_reader_group_grant_the_receiver_logs_the_authorization_error_and_still_serves(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess]
) -> None:
    """READ on `_tb.<env>.channels` but not on the reader's group `tb.<env>.<ch>.channels.`. Measured: the
    coordinator lookup for the reader's `group.id` is refused (`FindCoordinator`, DESCRIBE on the group), the
    receiver logs `channels_consumer_error` with `GROUP_AUTHORIZATION_FAILED` and a sub-second
    `channels_unavailable` / `channels_recovered` pair, and then reads the spec through its `assign()` consumer
    and serves. So the group grant is not load-bearing for a receiver: its absence is loud, not fatal.
    (`topics/v0` 3.4.1 rule 1 says the lookup "needs the group grant"; it needs it to be quiet.)"""
    channel = f"acl-http-{next(_serial)}"
    group = Grant("GROUP", f"tb.{ENV}.{channel}.channels.", "PREFIXED", "READ")
    d = deploy(broker, tmp_path, "http", drop=(group,), channel=channel)
    assert group not in d.grants and CHANNELS_READ in d.grants
    port = free_port()
    publish_via_admin(broker, channel, http_document(channel, port=port))
    proc = start(broker, d, procs, instance="http-nogroup")

    proc.wait_for_event("listening", timeout_s=90)
    resource = f"Group:LITERAL:tb.{ENV}.{channel}.channels.http-nogroup"
    denials = broker.denials_for(d.principal, resource=resource)
    assert denials and "DefaultDeny" in denials[0]  # the broker refused the group
    proc.collect_pending()
    errors = [e for e in proc.lines if e.get("event") == "channels_consumer_error"]
    assert errors and errors[0]["error_code"] == "GROUP_AUTHORIZATION_FAILED"  # and the receiver said so
    assert http_post(port) == 202  # serving, so nothing but the group was refused
    assert broker.denials_for(d.principal, resource=f"Topic:LITERAL:{raw_topic(ENV, channel)}") == []


# ---------------------------------------------------------------------------------------------------
# Negative: what a receiver principal may not write or read


def _produce_results(config: dict[str, Any], topics: list[str], *, timeout_s: float = 30.0) -> dict[str, str]:
    """Produce one synthetic record to each topic with `config`; topic -> delivery error name (`""` = delivered)."""
    outcomes: dict[str, str] = {}
    config = {**config, "message.timeout.ms": int(timeout_s * 1000) - 5000}
    producer = Producer(config)
    for topic in topics:

        def done(err: Any, _msg: Any, topic: str = topic) -> None:
            outcomes[topic] = "" if err is None else str(err.name())

        producer.produce(topic, value=b'{"synthetic": true, "note": "TRIBUTARY-SYNTH"}', on_delivery=done)
    producer.flush(timeout_s)
    return outcomes


@pytest.mark.parametrize("kind", ["mllp", "http", "sftp", "kafka"])
def test_a_receiver_principal_cannot_write_any_topic_but_its_own_raw(
    broker: SecureBroker, tmp_path: Path, kind: str
) -> None:
    d = deploy(broker, tmp_path, kind, reply_topic_exists=True)
    other = f"acl-other-{next(_serial)}"
    broker.create_topics([NewTopic(raw_topic(ENV, other), 1, 1)])
    own_raw = raw_topic(ENV, d.channel)
    forbidden = [
        events_topic(ENV),
        out_topic(ENV, d.channel, DESTINATION),
        dlq_topic(ENV, d.channel),
        reply_topic(ENV, d.channel),
        channels_topic(ENV),
        raw_topic(ENV, other),
    ]
    started = time.monotonic()
    outcomes = _produce_results(principal_client_config(broker, d, kind="producer"), [own_raw, *forbidden])
    elapsed = time.monotonic() - started

    assert outcomes[own_raw] == ""  # control: the same producer delivers to the receiver's own raw
    for topic in forbidden:
        # The broker's authorization error, not a delivery timeout (`_MSG_TIMED_OUT`).
        assert outcomes[topic] == "TOPIC_AUTHORIZATION_FAILED", (topic, outcomes[topic])
    assert elapsed < 25
    for topic in forbidden:
        assert broker.denials_for(d.principal, resource=f"Topic:LITERAL:{topic}"), topic
    assert broker.denials_for(d.principal, resource=f"Topic:LITERAL:{own_raw}") == []


@pytest.mark.parametrize("kind", ["mllp", "http", "sftp", "kafka"])
def test_a_receiver_principal_cannot_read_raw(broker: SecureBroker, tmp_path: Path, kind: str) -> None:
    """The receiver holds no READ on `raw` (it produces there). A consumer in the receiver's granted
    spec-reader group prefix, subscribed to its own `raw`, is refused by the broker."""
    d = deploy(broker, tmp_path, kind)
    own_raw = raw_topic(ENV, d.channel)
    group = f"tb.{ENV}.{d.channel}.channels.probe"  # inside the granted PREFIXED group, so only the topic is at issue
    consumer = Consumer(
        {**principal_client_config(broker, d, kind="consumer", group_id=group), "group.instance.id": "probe"}
    )
    errors: list[str] = []
    try:
        consumer.subscribe([own_raw])
        deadline = time.monotonic() + 30
        while not errors and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is not None and msg.error() is not None:
                errors.append(str(msg.error().name()))
    finally:
        consumer.close()
    assert errors == ["TOPIC_AUTHORIZATION_FAILED"]
    assert broker.denials_for(d.principal, operation="READ", resource=f"Topic:LITERAL:{own_raw}")


@pytest.mark.parametrize("kind", ["mllp", "http"])
def test_without_write_on_raw_the_receiver_refuses_and_never_acks_a_dropped_message(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess], kind: str
) -> None:
    """Invariant 1 under an authorization failure on the commit itself: the principal lacks WRITE on `raw`
    (DESCRIBE kept, so the metadata is fine and the refusal is the produce). The sender must not be told
    the message was accepted, and nothing may reach `raw`."""
    channel = f"acl-{kind}-{next(_serial)}"
    write = Grant("TOPIC", raw_topic(ENV, channel), "LITERAL", "WRITE")
    d = deploy(broker, tmp_path, kind, drop=(write,), channel=channel)
    assert write not in d.grants
    port = free_port()
    publish_via_admin(
        broker, channel, mllp_spec(channel, port) if kind == "mllp" else http_document(channel, port=port)
    )
    proc = start(broker, d, procs)
    proc.wait_for_event("listening", timeout_s=60)

    started = time.monotonic()
    if kind == "mllp":
        answer = mllp_exchange(port, adt("ACL-NOWRITE-1"))
        assert answer != "AA", "ACKed a message that could not be committed"
    else:
        status = http_post(port)
        assert status != 202, "accepted a message that could not be committed"
    elapsed = time.monotonic() - started

    assert broker.denials_for(d.principal, operation="WRITE", resource=f"Topic:LITERAL:{raw_topic(ENV, channel)}")
    assert drain_topic(broker.admin, raw_topic(ENV, channel), partitions=1, expected=0) == []
    assert elapsed < 30  # refused promptly with the broker's answer, not after a timeout


# ---------------------------------------------------------------------------------------------------
# Sync reply (set 1.2): READ reply and the LITERAL replier group; never WRITE reply


def test_sync_reply_receiver_holds_its_reply_partitions_under_the_extra_grants(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess]
) -> None:
    d = deploy(broker, tmp_path, "mllp", sync_reply=True)
    port = free_port()
    publish_via_admin(broker, d.channel, mllp_spec(d.channel, port, reply={"timeoutMs": 5000}))
    proc = start(broker, d, procs)
    proc.wait_for_event("listening", timeout_s=60)
    proc.wait_for_event("reply_partitions_held", timeout_s=60)
    assert broker.denials_for(d.principal) == []
    # Still no WRITE on reply: a receiver cannot forge an answer.
    outcomes = _produce_results(principal_client_config(broker, d, kind="producer"), [reply_topic(ENV, d.channel)])
    assert outcomes[reply_topic(ENV, d.channel)] == "TOPIC_AUTHORIZATION_FAILED"


@pytest.mark.parametrize("missing", ["group", "topic"])
def test_sync_reply_receiver_without_a_reply_grant_does_not_hold_partitions(
    broker: SecureBroker, tmp_path: Path, procs: list[ManagedProcess], missing: str
) -> None:
    channel = f"acl-mllp-{next(_serial)}"
    group = Grant("GROUP", f"tb.{ENV}.{channel}.replier", "LITERAL", "READ")
    topic = Grant("TOPIC", reply_topic(ENV, channel), "LITERAL", "READ")
    dropped = group if missing == "group" else topic
    d = deploy(broker, tmp_path, "mllp", sync_reply=True, drop=(dropped,), channel=channel)
    assert dropped not in d.grants
    port = free_port()
    publish_via_admin(broker, channel, mllp_spec(channel, port, reply={"timeoutMs": 5000}))
    proc = start(broker, d, procs)
    resource = f"{'Group' if missing == 'group' else 'Topic'}:LITERAL:{dropped.name}"
    wait_until(lambda: bool(broker.denials_for(d.principal, resource=resource)), "an authorizer denial", 60)
    proc.wait_for_event("listening", timeout_s=60)
    # Measured: the reply consumer's authorization error is swallowed (`reply/feed.py` `on_kafka_error` acts
    # only on down and fatal errors), so the receiver reports `listening` and logs no cause. What the sender
    # sees is a refusal: the receiver is not ready for a synchronous reply, so nothing is committed.
    assert not any(e.get("event") == "reply_partitions_held" for e in proc.collect_pending())
    answer = mllp_exchange(port, adt("ACL-SYNC-1"))
    assert answer != "AA", "ACKed a sync-reply message whose reply route cannot work"
    assert drain_topic(broker.admin, raw_topic(ENV, channel), partitions=1, expected=0) == []
