"""Wave 2 exit criterion 1: an HL7 ADT flow to a FHIR server and to ClickHouse, from a channel spec, end to end.

    MLLP sender -> receiver -> `raw` -> worker (parse, filter, per-destination steps incl. the W2-3
    HL7-to-FHIR mapping) -> `out.fhir-server` -> FHIR loader -> real HAPI FHIR server
                                       `out.adt-analytics` -> ClickHouse loader -> real ClickHouse table
    and the MLLP `MSA|AA` back to the sender.

Observed at a real HAPI (`hapiproject/hapi:v7.4.0`, test-only image) and a real ClickHouse, not stubs.
Six processes are spawned (receiver, worker, two loaders) with `TB_ENGINE_KAFKA_FILE`; Kafka, HAPI and
ClickHouse are testcontainers. `uv run pytest -m integration tests/e2e/test_adt_mllp_to_fhir_and_clickhouse_e2e.py`.

**The channel spec** is `contracts/channel-spec/v0/examples/valid/adt-mllp-to-fhir-clickhouse-mllp.json`
(contract set 1.3+, bearer auth on the FHIR destination) with only the environment-specific values
replaced: bind address and port, TLS removed (loopback), FHIR/ClickHouse endpoints and secret refs, and
the third destination (`downstream-lis`, MLLP) dropped because the exit criterion names two sinks.
Everything else is the example's: `ordering: per-key`, `partitionKey`, the `parse` step, the per-destination
`steps`, `idempotency` and `retry`. Two step references are changed, both because they name code the repo
cannot ship: the example's `ref: adt_flow.*` are the authoring team's bundle modules, so this test provides
`tests/support/bundle/adt_flow` on the worker's `PYTHONPATH` for the filter and the ClickHouse row builder;
and `adt_flow.mapping.to_fhir_transaction_bundle` is replaced by the REAL W2-3 step
`uses: tributary.content_hl7_fhir.adt` (with `tributary.hl7v2.parse` ahead of it in the destination).

**Why PV1-19 fixtures.** The W2-3 Encounter is conditional on PV1-19 (`urn:id:SYNTH-HOSPITAL|VN...`) when
present; without it the identifier falls back to `<tb-msg-id>:encounter`, which is per message and cannot
converge on a resend. The fixture `adt_a01_admit_with_visit_number.hl7` is reused as is.

**Two kinds of "same message again".** (1) A sender resending over MLLP is a new message: the receiver
mints a new `tb-msg-id`, so HAPI converges (Patient on PID-3, Encounter on PV1-19) but ClickHouse holds one
row per `tb-msg-id`, i.e. two. (2) A genuine duplicate, the same `raw` record (same `tb-msg-id`) delivered
twice, as after a producer retry or a consumer redelivery, converges everywhere: one row after `FINAL`. Both
are asserted, and the second proves it fired by counting the two `out` records for that id.

**Control.** `test_control_a_spec_without_the_fhir_destination_fails_the_hapi_assertions` runs the same flow
with the FHIR destination removed from the spec and shows that the HAPI assertion helper the main test
uses raises, while ClickHouse still receives its row, so "HAPI holds one Patient" is not ambient.
"""

from __future__ import annotations

import copy
import ipaddress
import json
import os
import re
import time
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic

from tests.support.channel_spec import CONTRACTS_DIR
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, drain_topic, free_port, header_map
from tests.support.mllp_client import is_application_accept, send_mllp
from tests.support.processes import (
    ManagedProcess,
    ProcessDiedError,
    spawn_clickhouse_loader,
    spawn_fhir_loader,
    spawn_mllp_receiver_from_topic,
    spawn_pipeline_worker,
)

pytestmark = pytest.mark.integration

#: Exit criterion 1 (the main test) still cannot pass at HEAD: the worker's `WorkerContext` has no
#: `.fhir` / `.terminology`, so the real W2-3 step `tributary.content_hl7_fhir.adt` raises `AttributeError`
#: and the message is dead-lettered (`TRANSFORM_ERROR`). Owner: platform-engineer (`tributary-runtime`,
#: the `attach()` call site). The MLLP per-key refusal was fixed in 6ad5569. This strict xfail turns into a
#: failure the moment the worker is fixed, so it gets removed then. With `ctx.fhir` attached in a scratch
#: run every assertion below passed, so the test body itself is validated.
BLOCKED = pytest.mark.xfail(
    strict=True,
    raises=(ProcessDiedError, AssertionError),
    reason="worker has no ctx.fhir (platform-engineer)",
)

ENV = "dev"
EXAMPLE = CONTRACTS_DIR / "channel-spec" / "v0" / "examples" / "valid" / "adt-mllp-to-fhir-clickhouse-mllp.json"
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "hl7" / "adt_a01_admit_with_visit_number.hl7"
BUNDLE_DIR = Path(__file__).resolve().parents[1] / "support" / "bundle"
HAPI_IMAGE = "hapiproject/hapi:v7.4.0"
FHIR_DEST = "fhir-server"
CH_DEST = "adt-analytics"
PARTITIONS = 3
FHIR_SECRET = "fhir-token"  # noqa: S105 - a channel-spec secretRef name, not a credential
CH_SECRET = "clickhouse-adt"  # noqa: S105 - likewise
NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))

#: Markers from the synthetic fixture that identify the "patient": name parts, contact, address, phone, MRN.
#: A log line naming any of them is a PHI leak by invariant 6, whatever the data's real provenance.
PHI_MARKERS = (
    "SYNTHPATIENT",
    "ALPHA",
    "SYNTHCONTACT",
    "BETA",
    "100 SYNTH ST",
    "SYNTHVILLE",
    "555-0100",
    "555-0101",
    "19800115",
)
PHI_MRN_PATTERN = re.compile(r"(?<![0-9A-Za-z])(9000001|9000001-ACC)(?![0-9A-Za-z])")


def _loopback(host: str) -> str:
    if host == "localhost":
        return "127.0.0.1"
    assert ipaddress.ip_address(host).is_loopback, f"container host {host} is not loopback; FHIR loader refuses it"
    return host


def build_spec(
    *, channel: str, mllp_port: int, fhir_url: str, ch: ClickHouseContainer, table: str, with_fhir: bool = True
) -> dict[str, Any]:
    """The published example, retargeted (module docstring says exactly what changes and why)."""
    document = copy.deepcopy(json.loads(EXAMPLE.read_text(encoding="utf-8")))
    document["metadata"]["name"] = channel
    document["metadata"]["version"] = 1
    spec = document["spec"]
    spec["source"]["bind"] = "127.0.0.1"
    spec["source"]["port"] = mllp_port
    del spec["source"]["tls"]
    parse = {"name": "parse", "uses": "tributary.hl7v2.parse", "with": {"version": "2.5.1"}}
    fhir, clickhouse = spec["destinations"][0], spec["destinations"][1]
    assert (fhir["type"], clickhouse["type"]) == ("fhir", "clickhouse"), "the example's shape changed"
    fhir["with"]["url"] = fhir_url
    fhir["steps"] = [parse, {"name": "to-bundle", "uses": "tributary.content_hl7_fhir.adt"}]
    fhir["retry"] = {**fhir["retry"], "initialDelayMs": 200, "maxDelayMs": 1000}
    clickhouse["with"].update(
        host=ch.get_container_host_ip(),
        port=int(ch.get_exposed_port(8123)),
        database=ch.dbname,
        table=table,
        idColumn="tb_msg_id",
        tls={"mode": "disable"},
    )
    clickhouse["with"]["auth"]["secretRef"] = CH_SECRET
    clickhouse["retry"] = {**clickhouse["retry"], "backoff": "fixed", "initialDelayMs": 200, "maxDelayMs": 200}
    spec["destinations"] = [fhir, clickhouse] if with_fhir else [clickhouse]
    return document


def synthetic_message(*, mrn: str = "9000001", visit: str = "VN20260108-00001", control_id: str) -> bytes:
    text = FIXTURE.read_text(encoding="utf-8")
    text = text.replace("9000001", mrn).replace("VN20260108-00001", visit).replace("SYNTHMSG00008", control_id)
    assert text.startswith("MSH|^~\\&|TRIBUTARY-SYNTH|"), "hook-enforced: MSH-3 must be TRIBUTARY-SYNTH"
    return text.encode("utf-8")


# --------------------------------------------------------------------------------------- infrastructure


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "e2e-adt-fhir-clickhouse")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def hapi() -> Iterator[str]:
    container = (
        DockerContainer(HAPI_IMAGE)
        .with_exposed_ports(8080)
        .waiting_for(LogMessageWaitStrategy("Started Application in").with_startup_timeout(240))
    )
    with container:
        host = _loopback(container.get_container_host_ip())
        yield f"http://{host}:{container.get_exposed_port(8080)}/fhir"


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container


def ch_query(container: ClickHouseContainer, sql: str) -> str:
    url = (
        f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8123)}/"
        f"?database={container.dbname}&wait_end_of_query=1"
    )
    request = urllib.request.Request(
        url,
        data=sql.encode(),
        method="POST",
        headers={"X-ClickHouse-User": container.username, "X-ClickHouse-Key": container.password},
    )
    with NO_PROXY.open(request, timeout=30) as resp:
        return str(resp.read().decode()).strip()


def hapi_count(base: str, query: str) -> int:
    url = f"{base}/{urllib.parse.quote(query, safe='?=&:/')}&_summary=count"
    # HAPI caches identical searches for 60 s by default; no-cache makes every poll a fresh search.
    request = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})  # noqa: S310
    with NO_PROXY.open(request, timeout=30) as resp:
        return int(json.loads(resp.read())["total"])


@pytest.fixture(scope="module")
def secrets_dir(tmp_path_factory: pytest.TempPathFactory, clickhouse: ClickHouseContainer) -> str:
    base = tmp_path_factory.mktemp("adt-secrets")
    (base / FHIR_SECRET).mkdir()
    (base / FHIR_SECRET / "token").write_text("synthetic-test-bearer-token", encoding="utf-8")
    (base / CH_SECRET).mkdir()
    (base / CH_SECRET / "user").write_text(clickhouse.username, encoding="utf-8")
    (base / CH_SECRET / "password").write_text(clickhouse.password, encoding="utf-8")
    return str(base)


@dataclass
class Stack:
    channel: str
    table: str
    mllp_port: int
    bootstrap: str
    processes: dict[str, ManagedProcess] = field(default_factory=dict)

    def stop(self) -> None:
        for proc in self.processes.values():
            proc.terminate_and_wait()
            proc.collect_pending()


def start_stack(
    *,
    channel: str,
    kafka: str,
    hapi_url: str,
    clickhouse: ClickHouseContainer,
    secrets_dir: str,
    tmp_path: Path,
    with_fhir: bool,
) -> Stack:
    table = f"adt_events_{uuid.uuid4().hex[:8]}"
    mllp_port = free_port()
    ch_query(
        clickhouse,
        f"CREATE TABLE {table} (tb_msg_id String, event_type String, mrn String, visit_number String) "
        "ENGINE=ReplacingMergeTree ORDER BY tb_msg_id",
    )
    destinations = [FHIR_DEST, CH_DEST] if with_fhir else [CH_DEST]
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(raw_topic(ENV, channel), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, channel), 1, 1),
        *[NewTopic(out_topic(ENV, channel, d), PARTITIONS, 1) for d in destinations],
    ]
    wanted += [
        NewTopic(name, 1, 1, config=config)
        for name, config in ((events_topic(ENV), {}), (channels_topic(ENV), {"cleanup.policy": "compact"}))
    ]
    for _topic, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # already exists: another stack in this module made the shared topics
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    document = build_spec(
        channel=channel, mllp_port=mllp_port, fhir_url=hapi_url, ch=clickhouse, table=table, with_fhir=with_fhir
    )
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=channel.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0

    stack = Stack(channel=channel, table=table, mllp_port=mllp_port, bootstrap=kafka)
    try:
        stack.processes["worker"] = spawn_pipeline_worker(
            env=ENV,
            channel=channel,
            bootstrap=kafka,
            instance_id=f"worker-{channel}",
            extra_env={"PYTHONPATH": os.pathsep.join(filter(None, [str(BUNDLE_DIR), os.environ.get("PYTHONPATH")]))},
            timeout_s=60.0,
        )
        if with_fhir:
            stack.processes["fhir-loader"] = spawn_fhir_loader(
                channel=channel,
                destination=FHIR_DEST,
                env=ENV,
                bootstrap=kafka,
                instance_id=f"fhir-{channel}",
                secrets_dir=secrets_dir,
            )
        stack.processes["clickhouse-loader"] = spawn_clickhouse_loader(
            channel=channel,
            destination=CH_DEST,
            env=ENV,
            bootstrap=kafka,
            instance_id=f"ch-{channel}",
            secrets_dir=secrets_dir,
        )
        stack.processes["receiver"] = spawn_mllp_receiver_from_topic(
            channel=channel, env=ENV, bootstrap=kafka, instance=f"mllp-{channel}"
        )
    except BaseException:
        stack.stop()
        raise
    return stack


# --------------------------------------------------------------------------------------- assertions


def wait_until(predicate: Any, *, timeout_s: float = 60.0, interval_s: float = 0.3) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def patient_q(mrn: str) -> str:
    return f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}"


def encounter_q(visit: str) -> str:
    return f"Encounter?identifier=urn:id:SYNTH-HOSPITAL|{visit}"


def assert_hapi_holds_exactly(base: str, *, mrn: str, visit: str) -> None:
    """The HAPI assertion of the criterion: exactly one Patient on PID-3 and one Encounter on PV1-19."""
    counts = (hapi_count(base, patient_q(mrn)), hapi_count(base, encounter_q(visit)))
    assert counts == (1, 1), f"HAPI holds (Patient, Encounter) = {counts}, expected (1, 1)"


def ch_rows(ch: ClickHouseContainer, table: str, where: str = "1") -> int:
    return int(ch_query(ch, f"SELECT count() FROM {table} FINAL WHERE {where}"))  # noqa: S608


def phi_leaks(lines: list[bytes]) -> list[str]:
    """What in `lines` names a fixture patient marker. Returns marker names only, never the line."""
    leaks: list[str] = []
    for raw in lines:
        text = raw.decode("utf-8", errors="replace")
        leaks += [marker for marker in PHI_MARKERS if marker in text]
        if PHI_MRN_PATTERN.search(text):
            leaks.append("MRN")
    return leaks


def all_raw_lines(stack: Stack) -> list[bytes]:
    lines: list[bytes] = []
    for proc in stack.processes.values():
        proc.collect_pending()
        lines += proc.raw
    return lines


def worker_diagnostics(stack: Stack) -> list[str]:
    """Event names plus error/reason keys only, last dozen per process: enough to see which stage stopped."""
    out: list[str] = []
    for name, proc in stack.processes.items():
        proc.collect_pending()
        events = [
            {k: v for k, v in ln.items() if k in {"event", "error", "error_code", "reason", "code", "step"}}
            for ln in proc.lines
        ]
        out.append(f"{name}: {events[-6:]}")
    # The redacted human message lives on `_tb.<env>.events` (envelope contract: never on a header).
    reasons = []
    for record in drain_topic(stack.bootstrap, events_topic(ENV), partitions=1, timeout_s=5.0):
        try:
            body = json.loads(record.value())
        except ValueError:
            continue
        if isinstance(body, dict) and body.get("error_message"):
            reasons.append(
                {k: body.get(k) for k in ("stage", "destination", "error_code", "error_message") if k in body}
            )
    out.append(f"events with an error_message: {reasons[-4:]}")
    return out


# --------------------------------------------------------------------------------------- tests


def test_phi_scanner_would_catch_a_leak() -> None:
    """Control for the scanner: a line carrying each kind of marker is found, a message id is not."""
    assert phi_leaks([b'{"event": "x", "name": "SYNTHPATIENT^ALPHA"}']) == ["SYNTHPATIENT", "ALPHA"]
    assert phi_leaks([b"mrn 9000001 seen"]) == ["MRN"]
    assert phi_leaks([b'{"tb_msg_id": "0199036e-7a3e-7f00-8c21-9000001abcde", "control_id": "SYNTHMSG00008"}']) == []


@BLOCKED
def test_adt_flows_from_mllp_to_hapi_and_clickhouse(
    kafka: str, hapi: str, clickhouse: ClickHouseContainer, secrets_dir: str, tmp_path: Path
) -> None:
    channel = "adt-to-fhir-and-analytics"
    stack = start_stack(
        channel=channel,
        kafka=kafka,
        hapi_url=f"{hapi}",
        clickhouse=clickhouse,
        secrets_dir=secrets_dir,
        tmp_path=tmp_path,
        with_fhir=True,
    )
    try:
        mrn, visit = "9000001", "VN20260108-00001"
        assert hapi_count(hapi, patient_q(mrn)) == 0 and ch_rows(clickhouse, stack.table) == 0, "sinks start empty"

        # 1. AA only after the raw commit -----------------------------------------------------------
        first = synthetic_message(control_id="SYNTHMSG-E2E-1")
        ack = send_mllp("127.0.0.1", stack.mllp_port, first, timeout_s=20.0)
        assert is_application_accept(ack), f"expected MSA|AA, got {ack!r}"
        assert b"SYNTHMSG-E2E-1" in ack, "the ACK must reference the message it acknowledges"
        # The instant the ACK is in hand the record must already be readable from `raw` (committed, acks=all).
        raw_records = drain_topic(kafka, raw_topic(ENV, channel), partitions=PARTITIONS, expected=1, timeout_s=15.0)
        assert len(raw_records) == 1, "the sender got AA but `raw` does not hold the record"
        assert raw_records[0].value() == first, "raw holds the payload byte for byte (invariant 8)"
        first_id = header_map(raw_records[0])["tb-msg-id"]

        # 2 and 3. HAPI and ClickHouse ------------------------------------------------------------
        assert wait_until(lambda: hapi_count(hapi, patient_q(mrn)) >= 1 and ch_rows(clickhouse, stack.table) >= 1), (
            f"the flow did not reach both sinks in 60s: {worker_diagnostics(stack)}"
        )
        assert_hapi_holds_exactly(hapi, mrn=mrn, visit=visit)
        assert ch_rows(clickhouse, stack.table) == 1
        assert ch_rows(clickhouse, stack.table, f"tb_msg_id = '{first_id}'") == 1
        row = json.loads(
            ch_query(clickhouse, f"SELECT * FROM {stack.table} FINAL FORMAT JSONEachRow")  # noqa: S608
        )
        assert (row["event_type"], row["mrn"], row["visit_number"]) == ("ADT_A01", mrn, visit)

        # 4a. A sender's resend over MLLP: a new message, a new tb-msg-id ---------------------------
        resend = send_mllp("127.0.0.1", stack.mllp_port, first, timeout_s=20.0)
        assert is_application_accept(resend)
        assert wait_until(lambda: ch_rows(clickhouse, stack.table) >= 2), "the resend never reached ClickHouse"
        time.sleep(2.0)  # let HAPI apply the resend's conditional writes before counting
        assert_hapi_holds_exactly(hapi, mrn=mrn, visit=visit)
        ids = ch_query(clickhouse, f"SELECT tb_msg_id FROM {stack.table} FINAL ORDER BY tb_msg_id").split()  # noqa: S608
        assert len(ids) == len(set(ids)) == 2, "one ClickHouse row per tb-msg-id: two messages, two rows"
        assert first_id in ids

        # 4b. A genuine duplicate: the same raw record, same tb-msg-id, delivered a second time -----
        producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
        original = raw_records[0]
        producer.produce(
            raw_topic(ENV, channel), key=original.key(), value=original.value(), headers=original.headers()
        )
        assert producer.flush(30) == 0

        def out_records_for(topic: str) -> int:
            records = drain_topic(kafka, topic, partitions=PARTITIONS, timeout_s=10.0)
            return sum(1 for r in records if header_map(r).get("tb-msg-id") == first_id)

        fired = wait_until(
            lambda: (
                out_records_for(out_topic(ENV, channel, CH_DEST)) >= 2
                and out_records_for(out_topic(ENV, channel, FHIR_DEST)) >= 2
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert_fault_fired(
            fired, description="the duplicate raw record never produced a second `out` record for its tb-msg-id"
        )
        time.sleep(3.0)  # both loaders have had the second delivery; allow it to land
        assert_hapi_holds_exactly(hapi, mrn=mrn, visit=visit)
        assert ch_rows(clickhouse, stack.table, f"tb_msg_id = '{first_id}'") == 1
        assert ch_rows(clickhouse, stack.table) == 2, "still one row per tb-msg-id: two ids, two rows"
    finally:
        stack.stop()

    # 5. No PHI in any captured log line, from any of the four processes, JSON or not ----------------
    lines = all_raw_lines(stack)
    assert len(lines) > 8, "the log capture is nearly empty, so 'no PHI' would be vacuous"
    assert phi_leaks(lines) == [], "a process wrote a patient marker to its log (invariant 6)"


def test_control_a_spec_without_the_fhir_destination_fails_the_hapi_assertions(
    kafka: str, hapi: str, clickhouse: ClickHouseContainer, secrets_dir: str, tmp_path: Path
) -> None:
    """Same flow, FHIR destination dropped from the spec, distinct MRN and visit so nothing ambient can
    satisfy the check. ClickHouse still gets its row; the exact HAPI assertion the main test makes fails."""
    mrn, visit = "9000777", "VN20260108-00777"
    stack = start_stack(
        channel="adt-control-no-fhir",
        kafka=kafka,
        hapi_url=hapi,
        clickhouse=clickhouse,
        secrets_dir=secrets_dir,
        tmp_path=tmp_path,
        with_fhir=False,
    )
    try:
        ack = send_mllp(
            "127.0.0.1",
            stack.mllp_port,
            synthetic_message(mrn=mrn, visit=visit, control_id="SYNTHMSG-CTL-1"),
            timeout_s=20.0,
        )
        assert is_application_accept(ack)
        assert wait_until(lambda: ch_rows(clickhouse, stack.table) >= 1), (
            f"the control flow did not reach ClickHouse: {worker_diagnostics(stack)}"
        )
        time.sleep(3.0)
        with pytest.raises(AssertionError, match=r"HAPI holds .* expected \(1, 1\)"):
            assert_hapi_holds_exactly(hapi, mrn=mrn, visit=visit)
    finally:
        stack.stop()
