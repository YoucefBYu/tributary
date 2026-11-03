"""Shared pieces for the RF=3 chaos tests (W3-33b): the real MLLP receiver on a `Rf3Cluster`, a keyed
multi-threaded sender, and the end-of-run accounting (acknowledged-message loss, per-key order, duplicates).

Messages are synthetic (MSH-3 `TRIBUTARY-SYNTH`); MSH-10 carries `<patient>-<sequence>` and PID-3.1 the
patient, which the receiver turns into the record key. Nothing printed from here holds payload content, only
counts and control ids (invariant 6).
"""

from __future__ import annotations

import itertools
import json
import shutil
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tributary.runtime.topics import raw_topic

from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import count_order_violations
from tests.support.engine_kafka import engine_kafka_env, receiver_env
from tests.support.kafka_cluster import Rf3Cluster
from tests.support.kafka_helpers import drain_topic, free_port, header_map
from tests.support.mllp_client import is_application_accept, send_mllp
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, receiver_args
from tests.support.processes import ManagedProcess

ENV = "dev"
KEYS = 8
CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


def keyed_message(patient: int, sequence: int) -> str:
    control = f"SYNTH{patient:03d}-{sequence:05d}"
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|TRIBUTARY-DST|DST-FAC|20260101000000||ADT^A08|{control}|P|2.5.1\r"
        f"PID|1||SYNTH{patient:03d}^^^SYNTH||TESTPATIENT^SYNTHETIC||19700101|U\r"
    )


def control_id_of(message: str) -> str:
    return message.split("\r", 1)[0].split("|")[9]


def remove_mutant_contracts_dir(contracts: Path) -> None:
    shutil.rmtree(contracts.parent, ignore_errors=True)


def mutant_contracts_dir() -> Path:
    """A copy of `contracts/` whose `receiver_producer` ACKs on the leader alone (`acks=1`, no idempotence):
    the mutation control for invariant 1. The real receiver reads it through `TB_CONTRACTS_DIR`."""
    target = Path(tempfile.mkdtemp(prefix="tb-rf3-mutant-")) / "contracts"
    shutil.copytree(CONTRACTS, target)
    path = target / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["clients"]["receiver_producer"]["acks"] = "1"
    document["clients"]["receiver_producer"]["enable.idempotence"] = False
    path.write_text(json.dumps(document), encoding="utf-8")
    return target


@dataclass
class Rig:
    cluster: Rf3Cluster
    channel: str
    port: int
    process: ManagedProcess

    @property
    def topic(self) -> str:
        return raw_topic(ENV, self.channel)

    def close(self) -> None:
        self.process.terminate_and_wait()


def start_rig(
    cluster: Rf3Cluster,
    channel: str,
    *,
    replication_factor: int | None = None,
    topic_overrides: dict[str, str] | None = None,
    contracts_dir: Path | None = None,
) -> Rig:
    """Create the channel's `raw` topic (contract class config), publish its spec, start the real receiver."""
    cluster.create_topic(raw_topic(ENV, channel), replication_factor=replication_factor, overrides=topic_overrides)
    port = free_port()
    document = ordered_document(mllp_port=port, http_url="http://127.0.0.1:1/ingest", channel=channel)
    bootstrap = cluster.alive_bootstrap
    ensure_channels_topic(bootstrap, ENV)
    publish_channel_spec(bootstrap, ENV, channel, document)
    env = receiver_env(engine_kafka_env(cluster.bootstrap))
    if contracts_dir is not None:
        env["TB_CONTRACTS_DIR"] = str(contracts_dir)
    if replication_factor is None and not topic_overrides:
        cluster.assert_durability_floor(raw_topic(ENV, channel))
    process = ManagedProcess(receiver_args(channel=channel, env=ENV, instance=f"rf3-{channel}"), env=env)
    process.wait_for_event("listening", timeout_s=90.0)
    return Rig(cluster, channel, port, process)


@dataclass
class Sent:
    index: int  # global send order
    control_id: str
    patient: int
    sequence: int
    acked: bool
    at: float  # time.monotonic() when the reply (or refusal) came back


@dataclass
class KeyedLoad:
    """Closed-loop senders, one thread each, owning disjoint patients so each key's sequence is sent in order."""

    rig: Rig
    threads: int = 4
    sent: list[Sent] = field(default_factory=list)
    _counter: itertools.count[int] = field(default_factory=itertools.count)
    _stop: threading.Event = field(default_factory=threading.Event)
    _workers: list[threading.Thread] = field(default_factory=list)

    def next_index(self) -> int:
        return next(self._counter)

    def start(self) -> None:
        for worker in range(self.threads):
            patients = [p for p in range(KEYS) if p % self.threads == worker]
            thread = threading.Thread(target=self._run, args=(patients,), daemon=True)
            thread.start()
            self._workers.append(thread)

    def _run(self, patients: list[int]) -> None:
        sequences = dict.fromkeys(patients, 0)
        while not self._stop.is_set():
            for patient in patients:
                if self._stop.is_set():
                    return
                sequence = sequences[patient]
                sequences[patient] += 1
                message = keyed_message(patient, sequence)
                index = self.next_index()
                try:
                    ack = send_mllp("127.0.0.1", self.rig.port, message.encode("ascii"), timeout_s=30.0)
                    acked = is_application_accept(ack)
                except OSError:
                    acked = False  # refused or reset: not acknowledged
                self.sent.append(Sent(index, control_id_of(message), patient, sequence, acked, time.monotonic()))
                if not acked:
                    time.sleep(0.2)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._workers:
            thread.join(timeout=60)

    def acked_count(self) -> int:
        return sum(1 for s in list(self.sent) if s.acked)

    def wait_for_acks(self, more: int, *, timeout_s: float = 90.0) -> int:
        """Block until `more` further ACKs have arrived; proves the load is genuinely flowing."""
        target = self.acked_count() + more
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.acked_count() >= target:
                return self.acked_count()
            time.sleep(0.2)
        raise AssertionError(f"load produced fewer than {more} ACKs in {timeout_s}s")


@dataclass
class Accounting:
    sent: int
    acked: int
    records: int
    distinct_control_ids: int
    distinct_msg_ids: int
    missing_acked: list[str]
    duplicates: int
    order_violations: int
    wrong_key: int
    partitions_by_key: dict[bytes, set[int]]
    first_delivery: dict[bytes, list[int]]


def account(rig: Rig, sent: list[Sent], *, expect_floor: int | None = None) -> Accounting:
    """Drain `raw` and reconcile against what the sender saw. Loss = ACKed control ids absent from `raw`."""
    acked_ids = {s.control_id for s in sent if s.acked}
    records: list[Any] = drain_topic(
        rig.cluster.alive_bootstrap,
        rig.topic,
        partitions=rig.cluster.raw_partitions,
        expected=len(acked_ids) if expect_floor is None else expect_floor,
        timeout_s=90,
    )
    first: dict[bytes, list[int]] = {}
    parts: dict[bytes, set[int]] = {}
    found: set[str] = set()
    msg_ids: set[str] = set()
    wrong_key = 0
    for record in sorted(records, key=lambda r: (r.partition(), r.offset())):
        control = control_id_of(record.value().decode("ascii"))
        found.add(control)
        msg_ids.add(header_map(record)["tb-msg-id"])
        key = record.key()
        assert key is not None, "per-key channel produced an unkeyed record (invariant 5)"
        if key != control.rsplit("-", 1)[0].encode():
            wrong_key += 1
        parts.setdefault(key, set()).add(record.partition())
        sequence = int(control.rsplit("-", 1)[1])
        seen = first.setdefault(key, [])
        if sequence not in seen:
            seen.append(sequence)
    return Accounting(
        sent=len(sent),
        acked=len(acked_ids),
        records=len(records),
        distinct_control_ids=len(found),
        distinct_msg_ids=len(msg_ids),
        missing_acked=sorted(acked_ids - found),
        duplicates=len(records) - len(found),
        order_violations=count_order_violations(first),
        wrong_key=wrong_key,
        partitions_by_key=parts,
        first_delivery=first,
    )


def wait_until(predicate: Callable[[], bool], *, what: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")
