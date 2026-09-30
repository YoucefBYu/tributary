"""`SftpPoller` against an in-memory `SftpSession` and a scripted publisher: no server, no Kafka.
Invariant 1 for files (a file leaves the source directory only after its record is committed), the
crash and duplicate paths, the atomic claim, refusals, bounds and PHI-free logging. Payloads are
synthetic (`TRIBUTARY-SYNTH`)."""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import threading
from collections.abc import Callable
from dataclasses import replace

import pytest
from tributary.receivers.metrics import Metrics
from tributary.receivers.sftp.config import SftpSettings
from tributary.receivers.sftp.poller import CLAIM_PREFIX, SftpPoller
from tributary.receivers.sftp.session import (
    NotRegularFileError,
    RemoteEntry,
    RemoteExistsError,
    RemoteNotFoundError,
    SessionError,
)
from tributary.sdk import Message
from tributary.sdk.keys import parse_key_expression

DIR = "/in"
ARCHIVE = "/in/done"
REJECT = "/in/rejected"
HL7 = b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|F|20260101000000||ORU^R01|C1|P|2.5\rPID|1||SYNTH0001^^^F||TEST^SYNTHETIC\r"
PHI_NAME = "SMITH_JOHN_ACC12345.hl7"


class FakeSession:
    """A dict-backed remote filesystem. `log` records every mutating call in order."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = dict(files or {})
        self.mtimes: dict[str, float] = {}
        self.log: list[tuple[str, str]] = []
        self.fail_remove = False
        self.lock = threading.Lock()
        self.closed = 0
        self.undecodable_names = 0
        self.symlinks: set[str] = set()

    def put(self, name: str, data: bytes, *, directory: str = DIR, mtime: float = 0.0) -> None:
        path = posixpath.join(directory, name)
        self.files[path] = data
        self.mtimes[path] = mtime

    def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
        with self.lock:
            out = [
                RemoteEntry(posixpath.basename(p), len(d), self.mtimes.get(p, 0.0), True)
                for p, d in self.files.items()
                if posixpath.dirname(p) == directory
            ]
            out.append(RemoteEntry("done", 0, 0.0, False))  # a subdirectory: never ingested
            return out[:limit]

    def rename(self, src: str, dst: str) -> None:
        with self.lock:
            if src not in self.files:
                raise RemoteNotFoundError("gone")
            if dst in self.files:
                raise RemoteExistsError("exists")
            self.files[dst] = self.files.pop(src)
            self.mtimes[dst] = self.mtimes.pop(src, 0.0)
            self.log.append(("rename", posixpath.dirname(dst)))

    def read_bounded(self, path: str, max_bytes: int) -> bytes | None:
        with self.lock:
            if path not in self.files:
                raise RemoteNotFoundError("gone")
            if path in self.symlinks:
                raise NotRegularFileError("link")
            data = self.files[path]
        return None if len(data) > max_bytes else data

    def remove(self, path: str) -> None:
        with self.lock:
            if self.fail_remove:
                raise SessionError("remove failed")
            del self.files[path]
            self.log.append(("remove", DIR))

    def exists(self, path: str) -> bool:
        with self.lock:
            return path in self.files

    def close(self) -> None:
        self.closed += 1

    def names(self, directory: str) -> list[str]:
        return sorted(posixpath.basename(p) for p in self.files if posixpath.dirname(p) == directory)


async def eventually(predicate: Callable[[], bool], timeout_s: float = 5.0) -> None:
    """Poll instead of sleeping a guessed time: returns as soon as `predicate` holds, fails at the deadline."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.01)


class Recorder:
    """Publisher that records, and can snapshot the remote directory at publish time."""

    def __init__(self, session: FakeSession | None = None, outcome: bool = True) -> None:
        self.calls: list[Message] = []
        self.outcome = outcome
        self.session = session
        self.seen_during_publish: list[list[str]] = []

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        if self.session is not None:
            self.seen_during_publish.append(self.session.names(DIR))
        return self.outcome


class Crash(BaseException):
    """A process death: not an `Exception`, so nothing in the poller may swallow it."""


class CrashingPublisher:
    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        raise Crash


def settings(**kw: object) -> SftpSettings:
    base = SftpSettings(
        channel="lab-drop",
        channel_version=1,
        content_type="application/hl7-v2+er7",
        host="sftp.synthetic.example",
        port=22,
        host_keys=frozenset({"SHA256:" + "A" * 43}),
        auth_type="password",
        auth_secret_ref="ref",  # noqa: S106
        directory=DIR,
        file_pattern="*.hl7",
        poll_interval_s=1.0,
        min_file_age_s=0.0,
        max_files_per_poll=100,
        after_commit="delete",
        archive_directory=None,
        reject_directory=REJECT,
        nack_on=frozenset(),
        key_expression=None,
        max_message_bytes=1_000_000,
        ack_timeout_s=5.0,
        env="dev",
    )
    if kw.get("key_expression") is not None:
        kw.setdefault("ordering", "per-key")
    return replace(base, **kw)  # type: ignore[arg-type]


def poller(cfg: SftpSettings, pub: object, session: FakeSession, **kw: object) -> SftpPoller:
    return SftpPoller(cfg, pub, lambda: session, clock=lambda: 1000.0, **kw)  # type: ignore[arg-type]


def test_committed_file_is_deleted_only_after_the_record_is_committed() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    pub = Recorder(s)

    assert asyncio.run(poller(settings(), pub, s).poll_once()) == 1
    # While the record was being published the file still existed (under its claim name).
    assert pub.seen_during_publish == [[f"{CLAIM_PREFIX}sftp-0.a.hl7"]]
    assert s.names(DIR) == []
    assert pub.calls[0].payload == HL7 and pub.calls[0].headers["tb-content-type"] == "application/hl7-v2+er7"
    assert pub.calls[0].headers["tb-source"] == "sftp:sftp-0@sftp.synthetic.example"
    assert s.closed == 1


def test_after_commit_move_archives_under_the_original_name() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    asyncio.run(poller(settings(after_commit="move", archive_directory=ARCHIVE), Recorder(s), s).poll_once())
    assert s.names(ARCHIVE) == ["a.hl7"] and s.names(DIR) == []


def test_archive_name_collision_never_overwrites() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    s.put("a.hl7", b"older", directory=ARCHIVE)
    asyncio.run(poller(settings(after_commit="move", archive_directory=ARCHIVE), Recorder(s), s).poll_once())
    names = s.names(ARCHIVE)
    assert len(names) == 2 and "a.hl7" in names and s.files[posixpath.join(ARCHIVE, "a.hl7")] == b"older"


def test_kafka_refusal_leaves_the_file_unclaimed_and_stops_the_cycle() -> None:
    s = FakeSession()
    for n in ("a.hl7", "b.hl7", "c.hl7"):
        s.put(n, HL7)
    pub = Recorder(s, outcome=False)
    p = poller(settings(), pub, s)

    assert asyncio.run(p.poll_once()) == 0
    assert len(pub.calls) == 1  # did not hammer a broker that just refused
    assert s.names(DIR) == ["a.hl7", "b.hl7", "c.hl7"]  # renamed back: not claimed, not lost
    assert s.names(REJECT) == [] and s.names(ARCHIVE) == []
    assert p.metrics.acked == 0 and p.metrics.nacked_unavailable == 1


def test_publisher_exception_is_a_refusal_not_a_loss() -> None:
    class Boom:
        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            raise RuntimeError("SMITH broker text")

    s = FakeSession()
    s.put("a.hl7", HL7)
    assert asyncio.run(poller(settings(), Boom(), s).poll_once()) == 0
    assert "a.hl7" in s.names(DIR)


def test_crash_between_claim_and_commit_loses_nothing_and_the_restart_recovers_it() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    with pytest.raises(Crash):
        asyncio.run(poller(settings(), CrashingPublisher(), s).poll_once())
    assert s.names(DIR) == [f"{CLAIM_PREFIX}sftp-0.a.hl7"]  # still on the server, under our claim

    pub = Recorder(s)
    assert asyncio.run(poller(settings(), pub, s).poll_once()) == 1  # a new process, same instance id
    assert s.names(DIR) == [] and len(pub.calls) == 1 and pub.calls[0].payload == HL7


def test_failure_after_commit_leaves_the_claim_and_reingests_as_a_duplicate_never_a_loss() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    pub = Recorder(s)
    s.fail_remove = True
    p = poller(settings(), pub, s)
    assert asyncio.run(p.poll_once()) == 1
    assert s.names(DIR) == [f"{CLAIM_PREFIX}sftp-0.a.hl7"] and p.metrics.poll_errors == 1

    s.fail_remove = False
    assert asyncio.run(p.poll_once()) == 1
    assert s.names(DIR) == []
    ids = [m.headers["tb-msg-id"] for m in pub.calls]
    assert len(ids) == 2 and ids[0] != ids[1]  # README 3.2.2: a new tb-msg-id, so loaders do not absorb it


def test_two_pollers_racing_for_the_same_files_commit_each_exactly_once() -> None:
    s = FakeSession()
    for i in range(40):
        s.put(f"f{i:02}.hl7", HL7 + str(i).encode())
    pub_a, pub_b = Recorder(), Recorder()
    a = poller(settings(instance="sftp-0"), pub_a, s)
    b = poller(settings(instance="sftp-1"), pub_b, s)

    async def go() -> None:
        await asyncio.gather(a.poll_once(), b.poll_once())

    asyncio.run(go())
    payloads = [m.payload for m in pub_a.calls + pub_b.calls]
    assert len(payloads) == 40 and len(set(payloads)) == 40
    assert s.names(DIR) == []


def test_foreign_claims_are_left_alone_and_counted() -> None:
    s = FakeSession()
    s.put(f"{CLAIM_PREFIX}sftp-9.b.hl7", HL7)
    pub = Recorder()
    p = poller(settings(), pub, s)
    asyncio.run(p.poll_once())
    assert pub.calls == [] and p.foreign_claims == 1 and s.names(DIR) == [f"{CLAIM_PREFIX}sftp-9.b.hl7"]


def test_oversize_file_is_rejected_not_deleted_and_never_published() -> None:
    s = FakeSession()
    s.put("big.hl7", b"x" * 101)
    s.put("ok.hl7", b"x" * 100)
    pub = Recorder()
    p = poller(settings(max_message_bytes=100), pub, s)
    assert asyncio.run(p.poll_once()) == 1
    assert s.names(REJECT) == ["big.hl7"] and len(pub.calls) == 1 and p.metrics.nacked_oversize == 1


def test_empty_file_is_rejected() -> None:
    s = FakeSession()
    s.put("e.hl7", b"")
    pub = Recorder()
    asyncio.run(poller(settings(), pub, s).poll_once())
    assert s.names(REJECT) == ["e.hl7"] and pub.calls == []


def test_unresolved_key_on_a_per_key_channel_is_rejected_never_published_null_keyed() -> None:
    s = FakeSession()
    s.put("nokey.hl7", b"MSH|^~\\&|TRIBUTARY-SYNTH|F\r")
    s.put("key.hl7", HL7)
    pub = Recorder()
    cfg = settings(key_expression=parse_key_expression("hl7:PID.3.1"))
    assert asyncio.run(poller(cfg, pub, s).poll_once()) == 1
    assert s.names(REJECT) == ["nokey.hl7"] and [m.key for m in pub.calls] == [b"SYNTH0001"]


def test_malformed_json_is_rejected_only_when_parse_error_is_declared() -> None:
    for declared, expect_rejected in ((True, True), (False, False)):
        s = FakeSession()
        s.put("a.json", b'{"broken":')
        pub = Recorder()
        cfg = settings(
            file_pattern="*.json",
            content_type="application/json",
            nack_on=frozenset({"parse-error"}) if declared else frozenset(),
        )
        asyncio.run(poller(cfg, pub, s).poll_once())
        assert (s.names(REJECT) == ["a.json"]) is expect_rejected
        assert (pub.calls == []) is expect_rejected


def test_reject_move_failure_never_deletes_the_file() -> None:
    class NoRejectDir(FakeSession):
        def rename(self, src: str, dst: str) -> None:
            if posixpath.dirname(dst) == REJECT:
                raise SessionError("no such directory")
            super().rename(src, dst)

    s = NoRejectDir()
    s.put("e.hl7", b"")
    p = poller(settings(), Recorder(), s)
    asyncio.run(p.poll_once())
    assert s.names(DIR) == [f"{CLAIM_PREFIX}sftp-0.e.hl7"] and p.metrics.poll_errors == 1


def test_only_matching_old_enough_regular_files_with_safe_names_are_read() -> None:
    s = FakeSession()
    s.put("old.hl7", HL7, mtime=900.0)
    s.put("young.hl7", HL7, mtime=995.0)
    s.put("other.txt", HL7, mtime=0.0)
    pub = Recorder()

    class Hostile(FakeSession):
        def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
            base = super().list_dir(directory, limit)
            return [*base, RemoteEntry("../escape.hl7", 5, 0.0, True), RemoteEntry("a/b.hl7", 5, 0.0, True)]

    s.__class__ = Hostile
    p = poller(settings(min_file_age_s=60.0), pub, s)
    assert asyncio.run(p.poll_once()) == 1
    assert len(pub.calls) == 1
    assert s.names(DIR) == ["other.txt", "young.hl7"]


def test_oldest_first_and_max_files_per_poll_bounds_a_cycle() -> None:
    s = FakeSession()
    for i in range(10):
        s.put(f"f{i}.hl7", HL7 + str(i).encode(), mtime=float(100 - i))  # f9 is the oldest
    pub = Recorder()
    assert asyncio.run(poller(settings(max_files_per_poll=3), pub, s).poll_once()) == 3
    assert [m.payload[-1:] for m in pub.calls] == [b"9", b"8", b"7"]
    assert len(s.names(DIR)) == 7  # the other seven are untouched until the next cycle


def test_connect_failure_ends_the_cycle_without_raising() -> None:
    def refuse() -> FakeSession:
        raise SessionError("connect failed")

    m = Metrics()
    p = SftpPoller(settings(), Recorder(), refuse, metrics=m)
    assert asyncio.run(p.poll_once()) == 0 and m.poll_errors == 1


def test_listing_failure_closes_the_session_and_is_counted() -> None:
    class BadList(FakeSession):
        def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
            raise SessionError("timeout")

    s = BadList()
    p = poller(settings(), Recorder(), s)
    assert asyncio.run(p.poll_once()) == 0 and s.closed == 1 and p.metrics.poll_errors == 1


def test_run_loop_polls_until_stopped() -> None:
    s = FakeSession()
    s.put("a.hl7", HL7)
    pub = Recorder()
    p = poller(settings(poll_interval_s=0.05), pub, s)

    async def go() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(p.run(stop))
        await eventually(lambda: len(pub.calls) == 1)
        s.put("b.hl7", HL7)
        await eventually(lambda: len(pub.calls) == 2)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(go())
    assert len(pub.calls) == 2


def test_file_names_and_payload_never_reach_logs_metrics_or_reprs(caplog: pytest.LogCaptureFixture) -> None:
    s = FakeSession()
    s.put(PHI_NAME, HL7)
    s.put("SMITH_REJECT.hl7", b"")
    pub = Recorder()
    p = poller(settings(), pub, s)
    with caplog.at_level(logging.DEBUG):
        asyncio.run(p.poll_once())
    events = [json.loads(r.getMessage()) for r in caplog.records if r.name == "tributary.receivers.sftp"]
    assert {e["event"] for e in events} >= {"sftp_committed", "sftp_rejected"}
    blob = caplog.text + json.dumps(p.metrics.snapshot()) + repr(p.metrics)
    for forbidden in ("SMITH", "ACC12345", "SYNTH0001", "TRIBUTARY-SYNTH"):
        assert forbidden not in blob
    assert "SMITH" not in repr(RemoteEntry(PHI_NAME, 1, 0.0, True))
    committed = next(e for e in events if e["event"] == "sftp_committed")
    assert committed["tb_msg_id"] == pub.calls[0].headers["tb-msg-id"]


def test_a_claim_swapped_for_a_symlink_is_rejected_and_never_read() -> None:
    class Swapping(FakeSession):
        def rename(self, src: str, dst: str) -> None:
            super().rename(src, dst)
            if posixpath.basename(dst).startswith(CLAIM_PREFIX):
                self.symlinks.add(dst)  # the sender swaps the file for a link right after we claim it

    s = Swapping()
    s.put("a.hl7", HL7)
    pub = Recorder()
    assert asyncio.run(poller(settings(), pub, s).poll_once()) == 0
    assert pub.calls == [] and s.names(REJECT) == ["a.hl7"]


def test_undecodable_names_are_counted_and_do_not_stop_the_cycle() -> None:
    class Skipping(FakeSession):
        def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
            self.undecodable_names += 2
            return super().list_dir(directory, limit)

    s = Skipping()
    s.put("a.hl7", HL7)
    p = poller(settings(), Recorder(), s)
    assert asyncio.run(p.poll_once()) == 1 and p.metrics.files_skipped == 2


def test_a_permanently_unproducible_file_is_rejected_not_left_for_the_next_poll() -> None:
    from tributary.receivers.errors import PermanentPublishError

    class Invalid:
        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            raise PermanentPublishError("x")

    s = FakeSession()
    s.put("a.hl7", HL7)
    p = poller(settings(), Invalid(), s)
    assert asyncio.run(p.poll_once()) == 0
    assert s.names(REJECT) == ["a.hl7"] and s.names(DIR) == [] and p.metrics.rejected_envelope == 1
