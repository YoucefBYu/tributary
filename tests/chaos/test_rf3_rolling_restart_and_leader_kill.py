"""W3-33b C2 and C3 on a free RF=3 / `min.insync.replicas=2` cluster, keyed load through the real MLLP receiver.

C2 (`split` topology): a rolling restart of all three brokers, one at a time, under 8-key load. Per broker the
fault is confirmed (graceful stop observed, metadata shows the broker out of every ISR, ACKs kept arriving
while it was down), then the broker is started and the test waits for full ISR before moving on.

C3 (`combined` topology, so the leader is also a controller voter): SIGKILL of the broker leading the most
`raw` partitions mid-load. Beyond the C2 assertions, the number of distinct `tb-msg-id` on `raw` must equal
the record count (the idempotent producer must not duplicate across a leader change), and at least one key on
a partition the killed broker led must have ACKed sends on both sides of the kill.

Both assert: 0 ACKed messages missing from `raw` (invariant 1), 0 per-key order violations among first
deliveries (invariant 5), duplicates counted and reported, not asserted to be zero (invariant 4).
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

from tests.support.chaos_assertions import (
    assert_each_key_on_one_partition,
    assert_fault_fired,
    keys_hit_by_fault,
    record_chaos_row,
)
from tests.support.kafka_cluster import Rf3Cluster, rf3_cluster
from tests.support.rf3_harness import KEYS, Accounting, KeyedLoad, account, start_rig

pytestmark = [pytest.mark.chaos, pytest.mark.rf3]

BETWEEN_S = 4.0


@pytest.fixture(scope="module")
def split_cluster() -> Iterator[Rf3Cluster]:
    with rf3_cluster("split") as c:
        yield c


@pytest.fixture(scope="module")
def combined_cluster() -> Iterator[Rf3Cluster]:
    with rf3_cluster("combined") as c:
        yield c


def _assert_invariants(acc: Accounting) -> None:
    assert acc.acked > 0, "nothing was ACKed: this run tested nothing"
    assert not acc.missing_acked, f"invariant 1 violated: {len(acc.missing_acked)} ACKed message(s) missing from raw"
    assert acc.wrong_key == 0, f"{acc.wrong_key} record(s) keyed differently from the PID-3.1 they carry"
    assert len(acc.first_delivery) == KEYS
    assert_each_key_on_one_partition(acc.partitions_by_key)
    assert acc.order_violations == 0, f"invariant 5 violated: {acc.order_violations} per-key order violation(s)"


def test_rolling_restart_of_all_three_brokers_loses_nothing_and_keeps_order(split_cluster: Rf3Cluster) -> None:
    cluster = split_cluster
    rig = start_rig(cluster, "c2-rolling")
    load = KeyedLoad(rig)
    evidence: list[str] = []
    try:
        load.start()
        load.wait_for_acks(40)
        for node in (1, 2, 3):
            acked_at_stop = load.acked_count()
            code = cluster.stop(node)
            isr = cluster.await_isr(
                rig.topic,
                lambda leader, members, n=node: n not in members and leader not in (-1, n),
                what=f"broker {node} out of every ISR",
                timeout_s=90.0,
            )
            load.wait_for_acks(20, timeout_s=120.0)  # the cluster kept ACKing on 2 of 3
            time.sleep(BETWEEN_S)
            acked_while_down = load.acked_count() - acked_at_stop
            cluster.start(node)
            cluster.await_full_isr(rig.topic)
            load.wait_for_acks(20, timeout_s=120.0)  # and ACKs resume after the rejoin
            evidence.append(
                f"broker {node}: exit={code} isr_while_down={ {p: m for p, (_l, m) in isr.items()} } "
                f"acked_while_down={acked_while_down}"
            )
            assert acked_while_down > 0, f"no ACKs while broker {node} was down"
        time.sleep(BETWEEN_S)
    finally:
        load.stop()
        sent = list(load.sent)
        try:
            acc = account(rig, sent)
        finally:
            rig.close()
    print("C2 " + " | ".join(evidence))
    print(
        f"C2 sent={acc.sent} acked={acc.acked} records={acc.records} distinct_msg_ids={acc.distinct_msg_ids} "
        f"duplicates={acc.duplicates} order_violations={acc.order_violations} missing={len(acc.missing_acked)}"
    )
    record_chaos_row(
        scenario="rf3-c2-rolling-restart",
        sent=acc.sent,
        delivered_total=acc.records,
        delivered_distinct=acc.distinct_control_ids,
        order_violations=acc.order_violations,
        fault_fired=len(evidence) == 3,
    )
    assert_fault_fired(len(evidence) == 3, description="not every broker was restarted")
    _assert_invariants(acc)


def test_leader_sigkill_mid_load_loses_nothing_keeps_order_and_adds_no_duplicate_msg_ids(
    combined_cluster: Rf3Cluster,
) -> None:
    cluster = combined_cluster
    rig = start_rig(cluster, "c3-leader-kill")
    load = KeyedLoad(rig)
    try:
        load.start()
        load.wait_for_acks(60)
        leaders = cluster.leaders(rig.topic)
        victim = max(set(leaders.values()), key=list(leaders.values()).count)
        led = frozenset(p for p, leader in leaders.items() if leader == victim)
        kill_index = load.next_index()
        code = cluster.kill(victim)
        load.wait_for_acks(30, timeout_s=120.0)  # service continues on the two survivors
        time.sleep(BETWEEN_S)
        cluster.start(victim)
        cluster.await_full_isr(rig.topic)
        load.wait_for_acks(30, timeout_s=120.0)
    finally:
        load.stop()
        sent = list(load.sent)
        try:
            acc = account(rig, sent)
        finally:
            rig.close()
    acked_idx: dict[bytes, list[int]] = {}
    for s in sent:
        if s.acked:
            acked_idx.setdefault(f"SYNTH{s.patient:03d}".encode(), []).append(s.index)
    hit = keys_hit_by_fault(acc.partitions_by_key, led, acked_idx, kill_index=kill_index)
    print(
        f"C3 victim={victim} exit={code} led={sorted(led)} keys_hit={len(hit)} sent={acc.sent} acked={acc.acked} "
        f"records={acc.records} distinct_msg_ids={acc.distinct_msg_ids} duplicates={acc.duplicates} "
        f"order_violations={acc.order_violations} missing={len(acc.missing_acked)}"
    )
    record_chaos_row(
        scenario="rf3-c3-leader-sigkill",
        sent=acc.sent,
        delivered_total=acc.records,
        delivered_distinct=acc.distinct_control_ids,
        order_violations=acc.order_violations,
        fault_fired=code != 0 and bool(hit),
    )
    assert code != 0, "SIGKILL must not look like a graceful exit"
    assert_fault_fired(bool(hit), description="no key on a partition the killed leader led had ACKs on both sides")
    _assert_invariants(acc)
    assert acc.distinct_msg_ids == acc.records, (
        f"{acc.records - acc.distinct_msg_ids} record(s) on raw share a tb-msg-id with another record"
    )
