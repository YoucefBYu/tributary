"""W3-33b C1: with two of three brokers down, the real MLLP receiver never ACKs (invariant 1), on a free
RF=3 / `min.insync.replicas=2` cluster (`tests/support/kafka_cluster.py`, `split` topology so the controller
quorum, and so the metadata path, survives the outage).

Fault fired, not assumed: both killed brokers exit non-zero (SIGKILL), metadata shows ISR = [1] on every
`raw` partition, and a producer built from the receiver's own client profile (`receiver_producer` in
`topic-defaults.json`) is refused with a not-enough-replicas or timeout error. The receiver does not log
produce errors, so that producer is a probe on the same profile, not the receiver's own handle.

Mutation control: the same scenario against a receiver whose `receiver_producer` is `acks=1` ACKs during
the outage. The headline assertion (`assert_never_acked_during_outage`) must raise on that run. A control
that does not go red would mean the real run's green proves nothing.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import pytest
from confluent_kafka import KafkaError, Producer

from tests.support.chaos_assertions import record_chaos_row
from tests.support.kafka_cluster import Rf3Cluster, rf3_cluster
from tests.support.kafka_helpers import drain_topic
from tests.support.mllp_client import is_application_accept, send_mllp
from tests.support.rf3_harness import (
    ENV,
    Rig,
    control_id_of,
    keyed_message,
    mutant_contracts_dir,
    remove_mutant_contracts_dir,
    start_rig,
)

pytestmark = [pytest.mark.chaos, pytest.mark.rf3]

HEALTHY_SENDS = 12
BATCH = 6
SEND_TIMEOUT_S = 30.0
#: With ISR=[1] confirmed on the topic, an idempotent acks=all producer retries NOT_ENOUGH_REPLICAS until its
#: delivery timeout, which surfaces as `_MSG_TIMED_OUT`; both are attributable to the shrunk ISR. Generic
#: codes (`_TRANSPORT`, `LEADER_NOT_AVAILABLE`, ...) are not accepted as proof of refusal.
REFUSAL_CODES = ("NOT_ENOUGH_REPLICAS", "NOT_ENOUGH_REPLICAS_AFTER_APPEND", "_MSG_TIMED_OUT")


@dataclass
class Outcome:
    exit_codes: list[int]
    isr_during_outage: dict[int, list[int]]
    probe_error: str | None
    acked_before: set[str]
    acked_during: set[str] = field(default_factory=set)
    nack_ar_ae: int = 0  # the receiver answered MSA|AR or MSA|AE
    no_ack_within_timeout: int = 0  # no reply or closed connection within SEND_TIMEOUT_S: not ACKed
    missing_after_recovery: list[str] = field(default_factory=list)
    late_landed: int = 0
    acked_after_recovery: bool = False
    records: int = 0
    distinct: int = 0

    @property
    def not_acked_during(self) -> int:
        return self.nack_ar_ae + self.no_ack_within_timeout


def assert_never_acked_during_outage(outcome: Outcome) -> None:
    """Invariant 1 at the receiver: nothing sent while ISR < min.insync.replicas may be ACKed."""
    assert not outcome.acked_during, (
        f"invariant 1 violated: {len(outcome.acked_during)} message(s) ACKed with 2 of 3 brokers down"
    )


@pytest.fixture(scope="module")
def cluster() -> Iterator[Rf3Cluster]:
    with rf3_cluster("split") as c:
        yield c


def _classify(ack: bytes) -> str:
    if is_application_accept(ack):
        return "ack"
    if b"MSA|AR" in ack or b"MSA|AE" in ack:
        return "nack"
    return "none"


def _send_batch(rig: Rig, tag: int, n: int) -> list[tuple[str, str]]:
    """`n` concurrent single sends; returns (control_id, "ack" | "nack" | "none"). "none" is no reply, a
    refused or reset connection, or no answer within SEND_TIMEOUT_S: not ACKed, but not a receiver NACK."""
    results: list[tuple[str, str]] = []
    lock = threading.Lock()

    def one(i: int) -> None:
        message = keyed_message(i % 8, tag * 100 + i)
        try:
            kind = _classify(send_mllp("127.0.0.1", rig.port, message.encode("ascii"), timeout_s=SEND_TIMEOUT_S))
        except OSError:
            kind = "none"
        with lock:
            results.append((control_id_of(message), kind))

    workers = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return results


def _probe(cluster: Rf3Cluster, topic: str) -> str | None:
    """One produce with the receiver's profile (acks=all, idempotent), short delivery timeout; the delivery
    error name, or None if it was committed."""
    defaults = cluster.defaults["clients"]["receiver_producer"]
    config = {
        "bootstrap.servers": cluster.alive_bootstrap,
        "acks": defaults["acks"],
        "enable.idempotence": defaults["enable.idempotence"],
        "delivery.timeout.ms": 15000,
    }
    errors: list[KafkaError] = []
    producer = Producer(config)
    producer.produce(topic, value=b"probe", on_delivery=lambda err, _m: errors.append(err) if err else None)
    producer.flush(30)
    return errors[0].name() if errors else None


def run_outage(cluster: Rf3Cluster, channel: str, *, mutant: bool) -> Outcome:
    contracts = mutant_contracts_dir() if mutant else None
    rig = start_rig(cluster, channel, contracts_dir=contracts)
    probe_topic = f"{ENV}.{channel}-probe.raw"
    cluster.create_topic(probe_topic)
    victims = [2, 3]
    try:
        before = _sequential_acked(rig, HEALTHY_SENDS)
        assert len(before) == HEALTHY_SENDS, "precondition: a healthy cluster must ACK every send"
        exit_codes = [cluster.kill(v) for v in victims]
        outcome = Outcome(exit_codes, {}, None, before)
        results = _send_batch(rig, 1, BATCH)  # right after the kill, before the ISR has shrunk
        shrunk = lambda leader, isr: leader == 1 and isr == [1]  # noqa: E731
        state = cluster.await_isr(rig.topic, shrunk, what="ISR = [1]", timeout_s=90.0)
        cluster.await_isr(probe_topic, shrunk, what="probe topic ISR = [1]", timeout_s=90.0)
        outcome.isr_during_outage = {p: isr for p, (_leader, isr) in state.items()}
        results += _send_batch(rig, 2, BATCH)  # with the shrunk ISR confirmed in metadata
        if not mutant:
            outcome.probe_error = _probe(cluster, probe_topic)
        outcome.acked_during = {c for c, kind in results if kind == "ack"}
        outcome.nack_ar_ae = sum(1 for _c, kind in results if kind == "nack")
        outcome.no_ack_within_timeout = sum(1 for _c, kind in results if kind == "none")
        for v in victims:
            cluster.start(v)
        cluster.await_full_isr(rig.topic)
        after = _eventually_acked(rig, tag=3)
        outcome.acked_after_recovery = bool(after)
        sent_ok = outcome.acked_before | outcome.acked_during | after
        found, outcome.records = _found_ids(rig)
        outcome.distinct = len(found)
        outcome.missing_after_recovery = sorted(sent_ok - found)
        outcome.late_landed = len({c for c, kind in results if kind != "ack"} & found)
        return outcome
    finally:
        for v in victims:  # a failed run must not leave the shared module cluster two brokers short
            if cluster.is_stopped(v):
                with contextlib.suppress(Exception):
                    cluster.start(v)
        with contextlib.suppress(Exception):
            cluster.await_full_isr(rig.topic, timeout_s=120.0)
        rig.close()
        if contracts is not None:
            remove_mutant_contracts_dir(contracts)


def _found_ids(rig: Rig) -> tuple[set[str], int]:
    records = drain_topic(rig.cluster.alive_bootstrap, rig.topic, partitions=rig.cluster.raw_partitions, timeout_s=60)
    return {control_id_of(r.value().decode("ascii")) for r in records}, len(records)


def _sequential_acked(rig: Rig, n: int) -> set[str]:
    acked: set[str] = set()
    for i in range(n):
        message = keyed_message(i % 8, i)
        if is_application_accept(send_mllp("127.0.0.1", rig.port, message.encode("ascii"), timeout_s=SEND_TIMEOUT_S)):
            acked.add(control_id_of(message))
    return acked


def _eventually_acked(rig: Rig, *, tag: int) -> set[str]:
    """Send single messages until one is ACKed; every ACKed id is returned so the loss check covers them."""
    acked: set[str] = set()
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        for cid, kind in _send_batch(rig, tag, 1):
            if kind == "ack":
                acked.add(cid)
        if acked:
            # a few more, so the post-recovery path is exercised beyond the first success
            for cid, kind in _send_batch(rig, tag + 1, 4):
                if kind == "ack":
                    acked.add(cid)
            return acked
        time.sleep(1.0)
    return acked


def test_two_of_three_brokers_down_receiver_never_acks(cluster: Rf3Cluster) -> None:
    out = run_outage(cluster, "c1-real", mutant=False)
    print(
        f"C1 real: exit_codes={out.exit_codes} isr={out.isr_during_outage} probe={out.probe_error} "
        f"acked_before={len(out.acked_before)} acked_during={len(out.acked_during)} "
        f"nack_ar_ae={out.nack_ar_ae} no_ack_within_{int(SEND_TIMEOUT_S)}s={out.no_ack_within_timeout} "
        f"late_landed={out.late_landed} missing_after_recovery={len(out.missing_after_recovery)} "
        f"acked_after={out.acked_after_recovery}"
    )
    # fault fired
    assert all(code != 0 for code in out.exit_codes), f"brokers did not die from SIGKILL: {out.exit_codes}"
    assert out.isr_during_outage and all(isr == [1] for isr in out.isr_during_outage.values())
    assert out.probe_error in REFUSAL_CODES, f"the receiver-profile producer was not refused: {out.probe_error}"
    record_chaos_row(
        scenario="rf3-c1-two-brokers-down",
        sent=len(out.acked_before) + 2 * BATCH,
        delivered_total=out.records,
        delivered_distinct=out.distinct,
        order_violations=None,
        fault_fired=True,
    )
    # headline: nothing sent during the outage was ACKed (each was a receiver NACK or no ACK within 30 s)
    assert out.not_acked_during == 2 * BATCH
    assert_never_acked_during_outage(out)
    assert not out.missing_after_recovery, f"{len(out.missing_after_recovery)} ACKed message(s) lost"
    assert out.acked_after_recovery, "the receiver did not resume ACKing after the brokers returned"


def test_control_acks_one_receiver_acks_during_outage_and_the_assertion_goes_red(cluster: Rf3Cluster) -> None:
    out = run_outage(cluster, "c1-mutant", mutant=True)
    print(f"C1 control (acks=1): acked_during={len(out.acked_during)} not_acked_during={out.not_acked_during}")
    assert all(code != 0 for code in out.exit_codes)
    assert out.acked_during, "control did not ACK during the outage: the mutation is not exercising the claim"
    with pytest.raises(AssertionError, match="invariant 1 violated"):
        assert_never_acked_during_outage(out)
