"""`MllpRunner` against a fake spec feed: startup gating, live updates, restart-only changes, tombstone.

Real TCP loopback sockets and the real `MllpServer`; only the spec source and the publisher are fakes, so
there is no Kafka here (the real-broker version is `tests/integration/test_mllp_spec_lifecycle.py`).
Synthetic data only (MSH-3 `TRIBUTARY-SYNTH`); no assertion or log line here carries a payload.
"""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.metrics import Metrics
from tributary.receivers.mllp import server as server_mod
from tributary.receivers.mllp.config import MllpSettings
from tributary.receivers.mllp.runner import MllpRunner, RunnerOptions, SpecFeed
from tributary.receivers.mllp.server import MllpServer
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec
from tributary.sdk import Message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.engine_kafka import write_engine_kafka_file

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
CHANNEL = "adt-passthrough"


def frame(payload: bytes) -> bytes:
    return SB + payload + EB + CR


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


GARBAGE = b"this is not hl7 at all"


def spec_doc(version: int = 1, *, port: int = 0, nack_on: list[str] | None = None, **source: Any) -> dict[str, Any]:
    doc = walking_skeleton_document(mllp_port=port, http_url="http://127.0.0.1:1/x", version=version)
    if nack_on is not None:
        doc["spec"]["source"]["ack"]["nackOn"] = nack_on
    doc["spec"]["source"].update(source)
    return doc


def parse(doc: dict[str, Any]) -> ChannelSpec:
    return ChannelSpec.from_document(copy.deepcopy(doc))


class FakeFeed:
    """The runner's `SpecFeed`: `spec` is whatever the compacted topic would currently say."""

    def __init__(self, spec: ChannelSpec | None) -> None:
        self.spec = spec
        self.poll_error: Exception | None = None
        self.closed = False

    def poll(self) -> None:
        if self.poll_error is not None:
            raise self.poll_error

    def get(self) -> ChannelSpec | None:
        return self.spec

    def close(self) -> None:
        self.closed = True


class Recorder:
    """A `tributary.sdk.Logger` keeping `(event, fields)`."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def _add(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))

    def debug(self, event: str, /, **fields: Any) -> None:
        self._add(event, fields)

    info = warning = error = debug

    def names(self) -> list[str]:
        return [name for name, _ in self.events]

    def of(self, name: str) -> list[dict[str, Any]]:
        return [f for n, f in self.events if n == name]


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
    def __init__(self, feed: FakeFeed | None, *, open_errors: int = 0) -> None:
        self.feed = feed if feed is not None else FakeFeed(None)
        self.open_errors = open_errors
        self.open_calls = 0
        self.log = Recorder()
        self.publisher = Publisher()
        self.emitted: list[dict[str, Any]] = []
        self.publishers_made = 0

        def open_feed() -> SpecFeed:
            self.open_calls += 1
            if self.open_calls <= self.open_errors:
                raise ChannelSpecError("channel is not deployed")
            return self.feed

        def make_publisher(_cfg: MllpSettings) -> Any:
            self.publishers_made += 1
            return self.publisher

        def make_server(cfg: MllpSettings, publisher: Any, metrics: Metrics) -> MllpServer:
            return MllpServer(cfg, publisher, metrics=metrics)

        self.runner = MllpRunner(
            open_feed=open_feed,
            make_publisher=make_publisher,
            make_server=make_server,
            options=RunnerOptions(
                env="dev",
                instance="mllp-t",
                topic="dev.adt-passthrough.raw",
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

    async def listening_port(self) -> int:
        await self.until(lambda: any(e["event"] == "listening" for e in self.emitted))
        return int([e for e in self.emitted if e["event"] == "listening"][-1]["port"])


async def send(port: int, payload: bytes) -> str:
    """One frame on a fresh connection; the MSA-1 code, or `""` if the connection closed unanswered."""
    r, w = await asyncio.open_connection("127.0.0.1", port)
    try:
        w.write(frame(payload))
        await w.drain()
        return await read_ack(r)
    finally:
        w.close()


async def read_ack(r: asyncio.StreamReader, timeout: float = 5.0) -> str:
    buf = bytearray()
    while EB + CR not in buf:
        chunk = await asyncio.wait_for(r.read(65536), timeout)
        if not chunk:
            return ""
        buf += chunk
    return bytes(buf).split(b"\rMSA|")[1].split(b"|")[0].decode()


def run(scenario: Callable[[Harness], Awaitable[None]], feed: FakeFeed | None, **kw: Any) -> Harness:
    async def go() -> Harness:
        h = Harness(feed, **kw)
        task = asyncio.create_task(h.runner.run())
        try:
            await scenario(h)
        finally:
            h.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0
        return h

    return asyncio.run(go())


# -- startup --------------------------------------------------------------------------------------


def test_no_listener_opens_and_nothing_is_acked_until_a_spec_has_been_read() -> None:
    async def scenario(h: Harness) -> None:
        await h.until(lambda: h.open_calls >= 3)  # the channels topic keeps not having the channel
        assert h.emitted == [] and h.runner.server is None
        assert h.publisher.calls == [] and h.publishers_made == 0
        assert "spec_unavailable" in h.log.names()
        h.open_errors = 0  # the spec is deployed
        port = await h.listening_port()
        assert await send(port, adt("C1")) == "AA"

    run(scenario, FakeFeed(parse(spec_doc())), open_errors=10**6)


def test_a_feed_with_a_spec_this_receiver_refuses_stays_not_ready_and_says_why() -> None:
    doc = spec_doc(nack_on=["spec-invalid"])

    async def scenario(h: Harness) -> None:
        await h.until(lambda: "spec_rejected" in h.log.names())
        await asyncio.sleep(0.1)
        assert h.emitted == [] and h.runner.server is None
        assert len(h.log.of("spec_rejected")) == 1  # once per version, not once per tick
        h.feed.spec = parse(spec_doc(2))  # a fixed spec is picked up without a restart
        port = await h.listening_port()
        assert await send(port, adt("C1")) == "AA"

    run(scenario, FakeFeed(parse(doc)))


def test_listening_event_names_the_channel_version_and_port_only() -> None:
    async def scenario(h: Harness) -> None:
        await h.listening_port()

    h = run(scenario, FakeFeed(parse(spec_doc(7))))
    event = h.emitted[0]
    assert event["channel"] == CHANNEL and event["channel_version"] == 7
    assert set(event) == {"event", "channel", "channel_version", "topic", "port", "tls"}


# -- live updates ---------------------------------------------------------------------------------


def test_new_nack_on_takes_effect_without_a_restart() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        server = h.runner.server
        assert await send(port, GARBAGE) == "AR"  # parse-error is in nackOn: refused, not published
        assert h.publisher.calls == []

        h.feed.spec = parse(spec_doc(2, nack_on=["queue-full"]))
        await h.until(lambda: server is not None and server.cfg.channel_version == 2)
        assert h.runner.server is server  # same server object: no restart
        assert await send(port, GARBAGE) == "AA"  # now forwarded to the pipeline as-is
        assert len(h.publisher.calls) == 1
        applied = h.log.of("spec_applied")
        assert applied[-1]["changed"] == "channel_version,nack_on"
        assert applied[-1]["previous_version"] == 1 and len(h.emitted) == 1

    run(scenario, FakeFeed(parse(spec_doc(1))))


def test_new_limits_apply_to_a_connection_that_is_already_open() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        try:
            w.write(frame(adt("C1")))
            assert await read_ack(r) == "AA"
            h.feed.spec = parse(spec_doc(2, maxMessageBytes=64, queue={"maxBytes": 1_048_576}))
            await h.until(lambda: h.runner.server is not None and h.runner.server.cfg.max_message_bytes == 64)
            w.write(frame(adt("C2")))  # about 130 bytes: over the new limit, same connection
            assert await read_ack(r) == "AR"
            w.write(frame(b"MSH|^~\\&|TRIBUTARY-SYNTH|S|||2026||ADT|C3|P|2.5\r"))
            assert await read_ack(r) == "AA"
        finally:
            w.close()
        assert [m.payload for m in h.publisher.calls if m.payload and b"C2" in m.payload] == []

    run(scenario, FakeFeed(parse(spec_doc(1))))


def test_a_spec_that_will_not_build_is_rejected_and_the_last_good_one_keeps_serving() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        for bad in (spec_doc(2, nack_on=["spec-invalid"]), spec_doc(3, nack_on=["parse-error", "bogus-condition"])):
            h.feed.spec = parse(bad)
            await h.until(lambda: len(h.log.of("spec_update_rejected")) >= 1)
        assert await send(port, GARBAGE) == "AR"  # still the version-1 behaviour
        assert h.runner.server is not None and h.runner.server.cfg.channel_version == 1
        assert h.log.of("spec_applied") == []

    run(scenario, FakeFeed(parse(spec_doc(1))))


def test_control_plane_down_the_cached_spec_keeps_serving() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.feed.poll_error = RuntimeError("channels topic unreachable")
        await h.until(lambda: "spec_poll_failed" in h.log.names())
        await asyncio.sleep(0.1)
        assert await send(port, adt("C1")) == "AA"
        assert len(h.log.of("spec_poll_failed")) == 1  # one line per streak
        h.feed.poll_error = None
        await h.until(lambda: "spec_poll_recovered" in h.log.names())

    run(scenario, FakeFeed(parse(spec_doc(1))))


# -- restart-only changes -------------------------------------------------------------------------


def test_a_port_change_logs_and_exits_zero_after_acking_what_is_in_flight() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.publisher.gate = asyncio.Event()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        await asyncio.wait_for(h.publisher.entered.wait(), 5)

        h.feed.spec = parse(spec_doc(2, port=port + 1 if port < 65000 else port - 1))
        await h.until(lambda: h.runner.restart_requested)
        with pytest.raises(OSError):  # listener closed: no new connection
            await asyncio.open_connection("127.0.0.1", port)
        h.publisher.gate.set()
        assert await read_ack(r) == "AA"  # committed before the restart: still ACKed
        w.close()

    h = run(scenario, FakeFeed(parse(spec_doc(1))))
    (event,) = h.log.of("spec_restart_required")
    assert event["fields"] == "port" and event["previous_version"] == 1 and event["channel_version"] == 2
    assert h.log.of("spec_applied") == []
    assert h.publisher.closed


def test_a_tls_change_needs_a_restart_but_a_new_allow_list_does_not() -> None:
    from dataclasses import replace

    from tributary.receivers.mllp.reload import live_fields_changed, restart_required_fields
    from tributary.receivers.tls import TlsSpec

    base = MllpSettings(
        channel=CHANNEL,
        channel_version=1,
        bind="127.0.0.1",
        port=1,
        tls=TlsSpec("srv", "required", "1.2", ("dns:a.example",)),
        ack_timeout_s=1.0,
        nack_on=frozenset(),
        max_message_bytes=100,
        queue_max_bytes=1000,
        queue_max_messages=None,
        env="dev",
    )
    allow = replace(base, tls=TlsSpec("srv", "required", "1.2", ("dns:b.example",)))
    assert restart_required_fields(base, allow) == []
    assert live_fields_changed(base, allow) == ["tls.allowed_client_sans"]
    for changed in (
        replace(base, tls=TlsSpec("other", "required", "1.2", ("dns:a.example",))),
        replace(base, tls=TlsSpec("srv", "required", "1.3", ("dns:a.example",))),
        replace(base, tls=TlsSpec("srv", "none")),
        replace(base, tls=None),
        replace(base, bind="127.0.0.2"),
    ):
        assert restart_required_fields(base, changed)
    assert restart_required_fields(replace(base, tls=None), replace(base, tls=None)) == []


# -- delete and tombstone -------------------------------------------------------------------------


def test_tombstone_refuses_new_connections_but_acks_what_was_already_committed() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.publisher.gate = asyncio.Event()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        await asyncio.wait_for(h.publisher.entered.wait(), 5)  # in flight: handed to the publisher

        h.feed.spec = None  # the tombstone
        await h.until(lambda: "channel_undeployed" in h.log.names())
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", port)
        assert h.runner.server is None

        h.publisher.gate.set()
        assert await read_ack(r) == "AA"  # the frame that was in flight is not dropped

        w.write(frame(adt("C2")))  # a new message on the open connection: refused, told so, not published
        assert await read_ack(r) == "AR"
        assert await r.read(1) == b""  # and the connection closes
        w.close()
        assert len(h.publisher.calls) == 1

    h = run(scenario, FakeFeed(parse(spec_doc(1))))
    assert h.runner.restart_requested is False


def test_a_spec_after_the_tombstone_opens_a_fresh_listener() -> None:
    async def scenario(h: Harness) -> None:
        first = await h.listening_port()
        h.feed.spec = None
        await h.until(lambda: h.runner.server is None)
        h.feed.spec = parse(spec_doc(2))
        await h.until(lambda: len(h.emitted) == 2)
        second = await h.listening_port()
        assert await send(second, adt("C1")) == "AA"
        assert first != 0 and h.publishers_made == 1  # the publisher is reused, not rebuilt

    run(scenario, FakeFeed(parse(spec_doc(1))))


def test_sigterm_style_stop_drains_in_flight_frames_and_exits_zero() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.publisher.gate = asyncio.Event()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        await asyncio.wait_for(h.publisher.entered.wait(), 5)
        h.runner.request_stop()
        await asyncio.sleep(0.1)
        h.publisher.gate.set()
        assert await read_ack(r) == "AA"
        w.close()

    h = run(scenario, FakeFeed(parse(spec_doc(1))))
    assert "receiver_stopped" in h.log.names() and h.feed.closed


def test_logs_carry_no_payload_fragments() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        await send(port, adt("CTRL-SECRET-1"))
        await send(port, GARBAGE)

    h = run(scenario, FakeFeed(parse(spec_doc(1))))
    blob = repr(h.log.events) + repr(h.emitted)
    for needle in ("CTRL-SECRET-1", "TESTPATIENT", "SYNTH0001", "not hl7"):
        assert needle not in blob


# -- edges of a live update (security review of 392a09b) --------------------------------------------


class HeldPublisher:
    """Every publish waits for `release()`; records what it was handed."""

    def __init__(self) -> None:
        self.calls: list[Message] = []
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        self.entered.set()
        await self.gate.wait()
        return True


def one_slot(**kw: Any) -> MllpSettings:
    return MllpSettings(
        channel="adt-passthrough", channel_version=1, bind="127.0.0.1", port=0, tls=None, ack_timeout_s=10.0,
        nack_on=frozenset(), max_message_bytes=100_000, queue_max_bytes=1_000_000, queue_max_messages=1,
        env="dev", **kw,
    )  # fmt: skip


async def _ack(r: asyncio.StreamReader) -> bytes:
    buf = b""
    while EB + CR not in buf:
        chunk = await asyncio.wait_for(r.read(65536), 10)
        if not chunk:
            break
        buf += chunk
    return buf


def test_a_frame_admitted_under_v1_is_stamped_v1_even_if_v2_lands_while_it_waits() -> None:
    async def go() -> None:
        pub = HeldPublisher()
        srv = MllpServer(one_slot(), pub)  # type: ignore[arg-type]
        listener = await srv.start()
        port = listener.sockets[0].getsockname()[1]
        try:
            ra, wa = await asyncio.open_connection("127.0.0.1", port)
            rb, wb = await asyncio.open_connection("127.0.0.1", port)
            wa.write(SB + adt("A") + EB + CR)
            await wa.drain()
            await asyncio.wait_for(pub.entered.wait(), 5)  # A holds the only queue slot
            wb.write(SB + adt("B") + EB + CR)  # B is admitted under v1 and waits for the slot
            await wb.drain()
            await asyncio.sleep(0.1)
            await srv.apply_settings(replace(srv.cfg, channel_version=2))  # v2 lands during that wait
            pub.gate.set()
            assert b"MSA|AA|A" in await _ack(ra) and b"MSA|AA|B" in await _ack(rb)
            versions = {m.payload.split(b"|")[9]: m.headers["tb-channel-version"] for m in pub.calls if m.payload}
            assert versions == {b"A": "1", b"B": "1"}
            wc_r, wc = await asyncio.open_connection("127.0.0.1", port)
            wc.write(SB + adt("C") + EB + CR)
            assert b"MSA|AA|C" in await _ack(wc_r)
            assert pub.calls[-1].headers["tb-channel-version"] == "2"
        finally:
            listener.close()

    asyncio.run(go())


def test_a_frame_waiting_for_the_queue_is_not_published_after_a_tombstone() -> None:
    async def go() -> None:
        pub = HeldPublisher()
        srv = MllpServer(one_slot(), pub)  # type: ignore[arg-type]
        listener = await srv.start()
        port = listener.sockets[0].getsockname()[1]
        try:
            ra, wa = await asyncio.open_connection("127.0.0.1", port)
            rb, wb = await asyncio.open_connection("127.0.0.1", port)
            wa.write(SB + adt("A") + EB + CR)
            await wa.drain()
            await asyncio.wait_for(pub.entered.wait(), 5)
            wb.write(SB + adt("B") + EB + CR)
            await wb.drain()
            await asyncio.sleep(0.1)
            srv.stop_accepting("channel-undeployed")
            pub.gate.set()
            assert b"MSA|AA|A" in await _ack(ra)  # committed before the tombstone: ACKed
            assert b"MSA|AR|B|channel-undeployed" in await _ack(rb)
            assert [m.payload.split(b"|")[9] for m in pub.calls if m.payload] == [b"A"]
            assert srv.budget.used_messages == 0
        finally:
            listener.close()

    asyncio.run(go())


@pytest.mark.parametrize(
    "bad",
    [{"queue": {"maxBytes": 0}}, {"queue": {"maxBytes": 2_000_000, "maxMessages": 0}}, {"maxMessageBytes": 0},
     {"ack": {"mode": "after-commit", "timeoutMs": 0}}, {"port": 70000}, {"queue": {"maxBytes": -5}}],
)  # fmt: skip
def test_zero_or_negative_limits_are_refused_and_the_last_good_spec_keeps_serving(bad: dict[str, Any]) -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.feed.spec = parse(spec_doc(2, **bad))
        await h.until(lambda: "spec_update_rejected" in h.log.names())
        assert h.runner.server is not None and h.runner.server.cfg.channel_version == 1
        assert await send(port, adt("C1")) == "AA"
        assert h.runner.restart_requested is False

    run(scenario, FakeFeed(parse(spec_doc(1))))


def test_settings_reject_non_positive_numbers_as_config_errors() -> None:
    with pytest.raises(ConfigError):
        replace(one_slot(), queue_max_messages=0)
    with pytest.raises(ConfigError):
        replace(one_slot(), queue_max_bytes=0)


def test_an_unwritable_metrics_file_does_not_stop_the_listener(tmp_path: Path) -> None:
    async def go() -> None:
        h = Harness(FakeFeed(parse(spec_doc(1))))
        h.runner._metrics_file = str(tmp_path / "missing-dir" / "m.json")
        h.runner._opts = replace(h.runner._opts, metrics_every_s=0.01)
        task = asyncio.create_task(h.runner.run())
        try:
            port = await h.listening_port()
            await h.until(lambda: "metrics_file_unwritable" in h.log.names())
            assert await send(port, adt("C1")) == "AA"
        finally:
            h.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0

    asyncio.run(go())


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the refusal")


def _main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *argv: str) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("sys.argv", ["prog", *argv])
    server_mod.main()


@pytest.mark.parametrize(
    "argv",
    [
        ["--channel", "c", "--env", "dev", "--bogus-flag=SYNTH-VALUE-77"],
        ["--channel", "c", "--env", "dev", "--profile", "SYNTH-VALUE-77"],
        ["--env", "dev"],
    ],
)
def test_usage_errors_are_one_startup_refused_line_and_echo_nothing(
    argv: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *argv)
    err = capsys.readouterr()
    assert exc.value.code == 2
    assert "SYNTH-VALUE-77" not in err.err + err.out and "usage:" not in err.err
    lines = [json.loads(x) for x in err.err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "ARGUMENTS_INVALID"


def test_a_bad_contracts_dir_exits_2_without_a_traceback(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, "--channel", "c", "--env", "dev", "--contracts-dir", str(tmp_path / "nope"))
    err = capsys.readouterr().err
    assert exc.value.code == 2 and "Traceback" not in err and str(tmp_path) not in err
    lines = [json.loads(x) for x in err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "CONFIG_INVALID"
