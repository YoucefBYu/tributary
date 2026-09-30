"""The SFTP receiver's spec lifecycle (invariant 3) against a fake spec feed and an in-memory remote: not ready
until a spec is read, live changes, restart-only changes, refused updates keep the last good spec, a tombstone
pauses polling after the file in flight, and a redeploy polls again. Real `SftpService` and `SftpPoller`, fake
publisher and remote, no Kafka, no SSH."""

from __future__ import annotations

import asyncio
import copy
import json
import posixpath
import threading
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.metrics import Metrics
from tributary.receivers.sftp import poller as poller_mod
from tributary.receivers.sftp.config import sftp_settings_from_spec
from tributary.receivers.sftp.poller import CLAIM_PREFIX
from tributary.receivers.sftp.reload import live_fields_changed, restart_required_fields
from tributary.receivers.sftp.runner import RunnerOptions, SftpRunner, SpecFeed
from tributary.receivers.sftp.service import SftpService
from tributary.receivers.sftp.session import (
    NotRegularFileError,
    RemoteEntry,
    RemoteExistsError,
    RemoteNotFoundError,
)
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec
from tributary.sdk import Message

from tests.support.engine_kafka import write_engine_kafka_file
from tests.support.receiver_specs import sftp_document

HL7 = b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|F|20260101000000||ORU^R01|C1|P|2.5\rPID|1||SYNTH0001^^^F||TEST^SYNTHETIC\r"
SECRET_REF = "SYNTH-SECRET-REF-31ab"  # noqa: S105


def parse(doc: dict[str, Any]) -> ChannelSpec:
    return ChannelSpec.from_document(copy.deepcopy(doc))


class FakeFeed:
    def __init__(self, spec: ChannelSpec | None) -> None:
        self.spec = spec
        self.poll_error: Exception | None = None

    def poll(self) -> None:
        if self.poll_error is not None:
            raise self.poll_error

    def get(self) -> ChannelSpec | None:
        return self.spec

    def close(self) -> None:
        pass


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def _add(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))

    def debug(self, event: str, /, **fields: Any) -> None:
        self._add(event, fields)

    info = warning = error = debug

    def names(self) -> list[str]:
        return [n for n, _ in self.events]

    def of(self, name: str) -> list[dict[str, Any]]:
        return [f for n, f in self.events if n == name]


class Remote:
    """A dict-backed remote filesystem shared by every session the poller opens."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.lock = threading.Lock()
        self.on_read: Callable[[str], None] | None = None
        self.sessions_open = 0
        self.undecodable_names = 0

    def put(self, name: str, data: bytes = HL7, directory: str = "/in") -> None:
        self.files[posixpath.join(directory, name)] = data

    def names(self, directory: str = "/in") -> list[str]:
        return sorted(posixpath.basename(p) for p in self.files if posixpath.dirname(p) == directory)

    # the SftpSession surface the poller uses
    def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
        with self.lock:
            return [
                RemoteEntry(posixpath.basename(p), len(d), 0.0, True)
                for p, d in self.files.items()
                if posixpath.dirname(p) == directory
            ][:limit]

    def rename(self, src: str, dst: str) -> None:
        with self.lock:
            if src not in self.files:
                raise RemoteNotFoundError("gone")
            if dst in self.files:
                raise RemoteExistsError("exists")
            self.files[dst] = self.files.pop(src)

    def read_bounded(self, path: str, max_bytes: int) -> bytes | None:
        with self.lock:
            if path not in self.files:
                raise RemoteNotFoundError("gone")
            data = self.files[path]
        if self.on_read is not None:
            self.on_read(path)
        if not isinstance(data, bytes):
            raise NotRegularFileError("link")
        return None if len(data) > max_bytes else data

    def remove(self, path: str) -> None:
        with self.lock:
            del self.files[path]

    def exists(self, path: str) -> bool:
        with self.lock:
            return path in self.files

    def close(self) -> None:
        pass


class Publisher:
    def __init__(self) -> None:
        self.calls: list[Message] = []
        self.gate: asyncio.Event | None = None
        self.entered = asyncio.Event()
        self.closed = False

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        return True

    def close(self) -> None:
        self.closed = True


class Harness:
    def __init__(self, feed: FakeFeed | None) -> None:
        self.feed = feed if feed is not None else FakeFeed(None)
        self.remote = Remote()
        self.log = Recorder()
        self.publisher = Publisher()
        self.emitted: list[dict[str, Any]] = []
        self.services_made = 0
        self.feed_ready = True

        def open_feed() -> SpecFeed:
            if not self.feed_ready:
                raise ChannelSpecError("channel is not deployed")
            return self.feed

        def make_service(cfg: Any, publisher: Any, metrics: Metrics) -> SftpService:
            self.services_made += 1
            return SftpService(cfg, publisher, lambda: self.remote, metrics=metrics)  # type: ignore[arg-type,return-value]

        self.runner = SftpRunner(
            open_feed=open_feed,
            make_publisher=lambda _cfg: self.publisher,  # type: ignore[arg-type,return-value]
            make_service=make_service,
            options=RunnerOptions(
                env="dev",
                instance="sftp-t",
                topic="dev.lab-drop.raw",
                poll_interval_s=0.01,
                open_retry_initial_s=0.01,
                open_retry_max_s=0.02,
            ),
            log=self.log,
            emit=self.emitted.append,
        )

    async def until(self, predicate: Callable[[], bool], timeout: float = 5.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"condition not met; events={self.log.names()}")
            await asyncio.sleep(0.005)

    async def started(self, n: int = 1) -> None:
        await self.until(lambda: sum(e["event"] == "listening" for e in self.emitted) >= n)


def run(scenario: Callable[[Harness], Awaitable[None]], feed: FakeFeed | None) -> Harness:
    async def go() -> Harness:
        h = Harness(feed)
        task = asyncio.create_task(h.runner.run())
        try:
            await scenario(h)
        finally:
            h.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0
        return h

    return asyncio.run(go())


# -- startup ----------------------------------------------------------------------------------------


def test_nothing_polls_until_a_spec_has_been_read_then_a_file_is_committed_and_removed() -> None:
    async def scenario(h: Harness) -> None:
        h.feed_ready = False
        h.remote.put("a.hl7")
        await h.until(lambda: "spec_unavailable" in h.log.names())
        assert h.emitted == [] and h.publisher.calls == [] and h.remote.names() == ["a.hl7"]
        h.feed_ready = True
        h.feed.spec = parse(sftp_document("lab-drop"))
        await h.started()
        await h.until(lambda: len(h.publisher.calls) == 1)
        await h.until(lambda: h.remote.names() == [])

    h = run(scenario, FakeFeed(None))
    [event] = h.emitted
    assert set(event) == {"event", "channel", "channel_version", "topic", "port", "tls"}


def test_a_spec_this_poller_refuses_stays_not_ready_and_says_why() -> None:
    async def scenario(h: Harness) -> None:
        await h.until(lambda: "spec_rejected" in h.log.names())
        assert h.runner.service is None and h.emitted == []

    run(scenario, FakeFeed(parse(sftp_document("lab-drop", rejectDirectory="/in"))))


# -- live changes -----------------------------------------------------------------------------------


def test_pattern_directory_limits_and_after_commit_apply_live() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.remote.put("a.hl7")
        h.remote.put("b.txt")
        await h.until(lambda: h.remote.names() == ["b.txt"])  # a.hl7 committed, b.txt not selected

        doc = sftp_document("lab-drop", version=2, pattern="*.txt", afterCommit="move", archiveDirectory="/in/done")
        h.feed.spec = parse(doc)
        await h.until(lambda: "spec_applied" in h.log.names())
        assert h.log.of("spec_applied")[0]["changed"] == "channel_version,file_pattern,after_commit,archive_directory"
        await h.until(lambda: h.remote.names("/in/done") == ["b.txt"])  # picked up and archived, no restart
        assert h.runner.restart_requested is False
        assert {m.headers["tb-channel-version"] for m in h.publisher.calls} == {"1", "2"}

        h.remote.put("c.txt", directory="/other")  # a directory change is live too
        h.feed.spec = parse(sftp_document("lab-drop", version=3, directory="/other", pattern="*.txt"))
        await h.until(lambda: h.remote.names("/other") == [])
        assert len(h.publisher.calls) == 3

    run(scenario, FakeFeed(parse(sftp_document("lab-drop", version=1))))


def test_each_file_takes_its_own_snapshot_so_a_change_mid_cycle_applies_to_the_next_file() -> None:
    from dataclasses import replace

    async def go() -> None:
        remote = Remote()
        remote.put("a.hl7")
        remote.put("b.hl7")
        cfg = sftp_settings_from_spec(parse(sftp_document("lab-drop")), env="dev")
        tight = replace(cfg, max_message_bytes=8, channel_version=2)
        service = SftpService(cfg, None, lambda: remote, metrics=Metrics())  # type: ignore[arg-type]

        class Applying(Publisher):
            async def publish(self, message: Message, *, timeout_s: float) -> bool:
                ok = await super().publish(message, timeout_s=timeout_s)
                service.poller.apply_settings(tight)  # the update lands while the first file is being committed
                return ok

        publisher = Applying()
        service.poller.publisher = publisher
        assert await service.poller.poll_once() == 1
        assert len(publisher.calls) == 1  # a.hl7 under the old settings
        assert remote.names() == []  # a.hl7 removed after its commit under the old after-commit rule
        assert remote.names("/in/rejected") == ["b.hl7"]  # b.hl7 met the new 8-byte limit: rejected, not published

    asyncio.run(go())


@pytest.mark.parametrize(
    "bad",
    [
        {"pollIntervalSeconds": 0},
        {"pollIntervalSeconds": -1},
        {"maxFilesPerPoll": 0},
        {"filePattern": ""},
        {"rejectDirectory": "/in"},
        {"afterCommit": "move"},  # needs archiveDirectory
        {"directory": "relative/path"},
    ],
)
def test_bad_values_are_refused_before_apply_and_the_last_good_spec_keeps_polling(bad: dict[str, Any]) -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        doc = sftp_document("lab-drop", version=2)
        doc["spec"]["source"]["sftp"].update(bad)
        h.feed.spec = parse(doc)
        await h.until(lambda: "spec_update_rejected" in h.log.names())
        assert h.runner.service is not None and h.runner.service.cfg.channel_version == 1
        h.remote.put("a.hl7")
        await h.until(lambda: len(h.publisher.calls) == 1)
        assert h.runner.restart_requested is False

    run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))


def test_bad_source_size_and_ack_values_are_refused_too() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        for version, patch in ((2, {"maxMessageBytes": 0}), (3, {"maxMessageBytes": 2_000_000})):
            doc = sftp_document("lab-drop", version=version)
            doc["spec"]["source"].update(patch)
            h.feed.spec = parse(doc)
            await h.until(lambda v=version: any(f["channel_version"] == v for f in h.log.of("spec_update_rejected")))  # type: ignore[misc]
        assert h.runner.service is not None and h.runner.service.cfg.channel_version == 1

    run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))


def test_control_plane_down_the_cached_spec_keeps_polling() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.feed.poll_error = OSError("channels topic unreachable")
        await h.until(lambda: "spec_poll_failed" in h.log.names())
        h.remote.put("a.hl7")
        await h.until(lambda: len(h.publisher.calls) == 1)
        assert len(h.log.of("spec_poll_failed")) == 1
        h.feed.poll_error = None
        await h.until(lambda: "spec_poll_recovered" in h.log.names())

    run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))


# -- restart-only changes ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"host": "other.synthetic.example"}, "host"),
        ({"port": 2222}, "port"),
        ({"secret_ref": SECRET_REF}, "auth_secret_ref"),
        ({"host_key": "SHA256:" + "B" * 43}, "host_keys"),
    ],
)
def test_host_credentials_and_host_key_are_restart_only_and_no_value_is_logged(
    kwargs: dict[str, Any], field: str
) -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.feed.spec = parse(sftp_document("lab-drop", version=2, **kwargs))
        await h.until(lambda: h.runner.restart_requested)

    h = run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))
    assert h.log.of("spec_restart_required")[0]["fields"] == field
    assert "spec_applied" not in h.log.names()
    assert "synthetic.example" not in repr(h.log.events) and SECRET_REF not in repr(h.log.events)
    assert "B" * 43 not in repr(h.log.events)


def test_the_partition_key_is_restart_only() -> None:
    def keyed(version: int, key: str) -> ChannelSpec:
        doc = sftp_document("lab-drop", version=version)
        doc["spec"]["ordering"] = "per-key"
        doc["spec"]["partitionKey"] = key
        return parse(doc)

    async def scenario(h: Harness) -> None:
        await h.started()
        h.feed.spec = keyed(2, "hl7:PID.18")
        await h.until(lambda: h.runner.restart_requested)

    h = run(scenario, FakeFeed(keyed(1, "hl7:PID.3.1")))
    assert h.log.of("spec_restart_required")[0]["fields"] == "partition_key"


def test_the_reload_table_names_fields_only() -> None:
    a = sftp_settings_from_spec(parse(sftp_document("c")), env="dev")
    b = sftp_settings_from_spec(parse(sftp_document("c", version=2, pattern="*.x", poll_interval_s=2)), env="dev")
    assert restart_required_fields(a, b) == []
    assert live_fields_changed(a, b) == ["channel_version", "file_pattern", "poll_interval_s"]


# -- tombstone and redeploy -------------------------------------------------------------------------


def test_tombstone_finishes_the_file_in_flight_claims_nothing_new_and_a_redeploy_polls_again() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.publisher.gate = asyncio.Event()
        h.remote.put("a.hl7")
        await asyncio.wait_for(h.publisher.entered.wait(), 5)  # a.hl7 is in flight, still under its claim
        assert h.remote.names() == [f"{CLAIM_PREFIX}sftp-t.a.hl7"]
        h.remote.put("b.hl7")

        h.feed.spec = None  # the tombstone
        await h.until(lambda: "channel_undeployed" in h.log.names())
        assert h.runner.service is None
        h.publisher.gate.set()
        await h.until(lambda: h.remote.names() == ["b.hl7"])  # a.hl7 committed and removed; b.hl7 never claimed
        await asyncio.sleep(0.1)
        assert len(h.publisher.calls) == 1 and h.remote.names() == ["b.hl7"]

        h.feed.spec = parse(sftp_document("lab-drop", version=2))
        await h.started(2)
        await h.until(lambda: h.remote.names() == [])  # the redeployed poller takes b.hl7
        assert len(h.publisher.calls) == 2 and h.services_made == 2

    h = run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))
    assert h.runner.restart_requested is False


def test_a_file_read_when_the_tombstone_lands_is_handed_back_not_published() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()

        def tombstone(_path: str) -> None:
            h.feed.spec = None
            h.runner.pause_component()  # what the lifecycle does the moment it sees the tombstone

        h.remote.on_read = tombstone
        h.remote.put("a.hl7")
        await h.until(lambda: "channel_undeployed" in h.log.names() or h.runner.service is None)
        await asyncio.sleep(0.1)
        assert h.publisher.calls == []
        assert h.remote.names() == ["a.hl7"]  # renamed back to its own name: a redeploy ingests it

    run(scenario, FakeFeed(parse(sftp_document("lab-drop"))))


# -- entry point ------------------------------------------------------------------------------------


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the refusal")


def _main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *argv: str) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("sys.argv", ["prog", *argv])
    poller_mod.main()


@pytest.mark.parametrize("form", ["separate", "equals"])
def test_channel_spec_flag_is_retired_and_its_value_never_echoed(
    form: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = "SYNTH-SPEC-PATH-5d1e"
    flag = [f"--channel-spec={value}"] if form == "equals" else ["--channel-spec", value]
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *flag, "--channel", "lab-drop", "--env", "dev")
    captured = capsys.readouterr()
    assert exc.value.code == 2 and value not in captured.out + captured.err
    lines = [json.loads(x) for x in captured.err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "CHANNEL_SPEC_FLAG_RETIRED"


@pytest.mark.parametrize(
    "argv",
    [
        ["--channel", "c", "--env", "dev", "--bogus-flag=SYNTH-VALUE-77"],
        ["--env", "dev"],
        ["--channel", "c", "--env", "dev", "--instance", "SYNTH-VALUE-77 bad!"],
        ["--channel", "SYNTH-VALUE-77", "--env", "dev"],
    ],
)
def test_usage_errors_are_one_startup_refused_line_and_echo_nothing(
    argv: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *argv)
    err = capsys.readouterr()
    assert exc.value.code == 2 and "SYNTH-VALUE-77" not in err.err + err.out and "usage:" not in err.err
    assert len([x for x in err.err.splitlines() if "startup_refused" in x]) == 1
