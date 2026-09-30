"""The HTTP receiver's spec lifecycle (invariant 3) against a fake spec feed: not ready until a spec is read,
live changes, restart-only changes, refused updates keep the last good spec, tombstone and redeploy, the
control plane going away, and mTLS revocation. Real `HttpServer`, fake publisher, no Kafka."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import shutil
import ssl
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.http import server as server_mod
from tributary.receivers.http.reload import live_fields_changed, restart_required_fields
from tributary.receivers.http.runner import HttpRunner, RunnerOptions, SpecFeed
from tributary.receivers.http.server import HttpServer
from tributary.receivers.secrets import DirSecretResolver
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec
from tributary.sdk import Message

from tests.support.engine_kafka import write_engine_kafka_file
from tests.support.receiver_pki import build_pki, openssl_available
from tests.support.receiver_specs import http_document

BODY = json.dumps({"resourceType": "Patient", "id": "SYNTH1", "subject": {"reference": "Patient/SYNTH1"}}).encode()
TOKEN_A = "synthetic-token-a"  # tb-secret-scan: allow  # noqa: S105
TOKEN_B = "synthetic-token-b"  # tb-secret-scan: allow  # noqa: S105


def parse(doc: dict[str, Any]) -> ChannelSpec:
    return ChannelSpec.from_document(copy.deepcopy(doc))


class FakeFeed:
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
    def __init__(self, feed: FakeFeed | None, *, resolver: DirSecretResolver | None = None) -> None:
        self.feed = feed if feed is not None else FakeFeed(None)
        self.log = Recorder()
        self.publisher = Publisher()
        self.emitted: list[dict[str, Any]] = []
        self.opened = 0

        def open_feed() -> SpecFeed:
            self.opened += 1
            if self.feed.spec is None and self.opened == 1 and not self.feed_ready:
                raise ChannelSpecError("channel is not deployed")
            return self.feed

        self.feed_ready = True
        self._open_feed = open_feed
        self.runner = HttpRunner(
            open_feed=open_feed,
            make_publisher=lambda _cfg: self.publisher,  # type: ignore[arg-type,return-value]
            make_server=lambda cfg, pub, metrics: HttpServer(cfg, pub, metrics=metrics, secret_resolver=resolver),
            options=RunnerOptions(
                env="dev",
                instance="http-t",
                topic="dev.fhir-in.raw",
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

    async def listening_port(self, n: int = 1) -> int:
        await self.until(lambda: sum(e["event"] == "listening" for e in self.emitted) >= n)
        return int([e for e in self.emitted if e["event"] == "listening"][-1]["port"])


def http_request(
    body: bytes = BODY, *, path: str = "/ingest", content_type: str = "application/fhir+json", token: str | None = None
) -> bytes:
    lines = ["POST " + path + " HTTP/1.1", "Host: x", f"Content-Type: {content_type}", f"Content-Length: {len(body)}"]
    if token is not None:
        lines.append(f"Authorization: Bearer {token}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode() + body


async def read_status(r: asyncio.StreamReader, timeout: float = 5.0) -> int:
    raw = await asyncio.wait_for(r.read(65536), timeout)
    return int(raw.split(b" ")[1]) if raw else 0


async def post(port: int, **kw: Any) -> int:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    try:
        w.write(http_request(**kw))
        await w.drain()
        return await read_status(r)
    finally:
        w.close()


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


# -- startup ----------------------------------------------------------------------------------------


def test_nothing_listens_until_a_spec_has_been_read() -> None:
    async def scenario(h: Harness) -> None:
        h.feed_ready = False
        await h.until(lambda: "spec_unavailable" in h.log.names())
        assert h.emitted == [] and h.runner.server is None
        h.feed_ready = True
        h.feed.spec = parse(http_document("fhir-in"))
        port = await h.listening_port()
        assert await post(port) == 202

    run(scenario, FakeFeed(None))


def test_listening_event_names_the_channel_version_and_port_only() -> None:
    async def scenario(h: Harness) -> None:
        await h.listening_port()

    h = run(scenario, FakeFeed(parse(http_document("fhir-in", version=7))))
    [event] = h.emitted
    assert set(event) == {"event", "channel", "channel_version", "topic", "port", "tls"}
    assert event["channel_version"] == 7


def test_a_spec_this_receiver_refuses_stays_not_ready_and_says_why() -> None:
    async def scenario(h: Harness) -> None:
        await h.until(lambda: "spec_rejected" in h.log.names())
        assert h.runner.server is None and h.emitted == []
        assert "nackOn" in h.log.of("spec_rejected")[0]["reason"]

    run(scenario, FakeFeed(parse(http_document("fhir-in", nack_on=("spec-invalid",)))))


# -- live changes -----------------------------------------------------------------------------------


def test_path_accept_and_limits_apply_live_without_a_restart() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        assert await post(port) == 202
        assert await post(port, path="/v2") == 404
        assert await post(port, content_type="application/json") == 415

        h.feed.spec = parse(
            http_document("fhir-in", version=2, path="/v2", accept=("application/json",), maxMessageBytes=150)
        )
        await h.until(lambda: "spec_applied" in h.log.names())
        applied = h.log.of("spec_applied")[0]
        assert applied["changed"] == "channel_version,path,accept,max_message_bytes"
        assert await post(port) == 404  # the old path is gone
        assert await post(port, path="/v2", content_type="application/json") == 202
        assert await post(port, path="/v2", content_type="application/json", body=b"x" * 151) == 413
        assert h.runner.restart_requested is False
        versions = {m.headers["tb-channel-version"] for m in h.publisher.calls}
        assert versions == {"1", "2"}  # each record carries the version that admitted it

    run(scenario, FakeFeed(parse(http_document("fhir-in", version=1))))


def test_a_bearer_secret_is_read_when_the_update_is_applied(tmp_path: Path) -> None:
    for name, token in (("tok-a", TOKEN_A), ("tok-b", TOKEN_B)):
        (tmp_path / name).mkdir()
        (tmp_path / name / "token").write_text(token, encoding="utf-8")
    bearer = lambda ref: {"type": "bearer", "secretRef": ref}  # noqa: E731

    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        assert await post(port, token=TOKEN_A) == 202
        assert await post(port, token=TOKEN_B) == 401

        h.feed.spec = parse(http_document("fhir-in", version=2, auth=bearer("tok-b")))
        await h.until(lambda: "spec_applied" in h.log.names())
        assert await post(port, token=TOKEN_B) == 202
        assert await post(port, token=TOKEN_A) == 401  # rotated out at once, no restart

        # a secret that cannot be read refuses the update; the last good one keeps serving
        h.feed.spec = parse(http_document("fhir-in", version=3, auth=bearer("tok-missing")))
        await h.until(lambda: "spec_update_rejected" in h.log.names())
        assert await post(port, token=TOKEN_B) == 202
        assert h.runner.restart_requested is False

    h = run(
        scenario, FakeFeed(parse(http_document("fhir-in", auth=bearer("tok-a")))), resolver=DirSecretResolver(tmp_path)
    )
    assert TOKEN_A not in repr(h.log.events) and TOKEN_B not in repr(h.log.events)


@pytest.mark.parametrize(
    "bad",
    [
        {"maxMessageBytes": 0},
        {"maxMessageBytes": -5},
        {"queue": {"maxBytes": 0}},
        {"queue": {"maxBytes": 10, "maxMessages": 0}},
        {"maxMessageBytes": 2_000_000},
        {"accept": []},
        {"path": "no-slash"},
        {"ack": {"mode": "after-commit", "nackOn": ["queue-full"], "timeoutMs": 0}},
        {"ack": {"mode": "fire-and-forget"}},
    ],
)
def test_bad_values_are_refused_before_apply_and_the_last_good_spec_keeps_serving(bad: dict[str, Any]) -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        doc = http_document("fhir-in", version=2)
        doc["spec"]["source"].update(bad)
        h.feed.spec = parse(doc)
        await h.until(lambda: "spec_update_rejected" in h.log.names())
        assert h.runner.server is not None and h.runner.server.cfg.channel_version == 1
        assert await post(port) == 202
        assert h.runner.restart_requested is False

    run(scenario, FakeFeed(parse(http_document("fhir-in"))))


def test_control_plane_down_the_cached_spec_keeps_serving() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.feed.poll_error = OSError("channels topic unreachable")
        await h.until(lambda: "spec_poll_failed" in h.log.names())
        await asyncio.sleep(0.05)
        assert await post(port) == 202
        assert len(h.log.of("spec_poll_failed")) == 1  # one line per streak
        h.feed.poll_error = None
        await h.until(lambda: "spec_poll_recovered" in h.log.names())

    run(scenario, FakeFeed(parse(http_document("fhir-in"))))


# -- restart-only changes ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"port": 1}, "port"),
        ({"bind": "127.0.0.2"}, "bind"),
        ({"tls": {"secretRef": "x", "clientAuth": "none"}}, "tls"),
    ],
)
def test_bind_port_and_tls_presence_are_restart_only(change: dict[str, Any], field: str) -> None:
    async def scenario(h: Harness) -> None:
        await h.listening_port()
        doc = http_document("fhir-in", version=2)
        doc["spec"]["source"].update(change)
        h.feed.spec = parse(doc)
        await h.until(lambda: h.runner.restart_requested)

    h = run(scenario, FakeFeed(parse(http_document("fhir-in"))))
    assert h.log.of("spec_restart_required")[0]["fields"] == field
    assert "spec_applied" not in h.log.names()


def test_changing_the_partition_key_is_restart_only() -> None:
    def keyed(version: int, key: str) -> ChannelSpec:
        doc = http_document("fhir-in", version=version)
        doc["spec"]["ordering"] = "per-key"
        doc["spec"]["partitionKey"] = key
        return parse(doc)

    async def scenario(h: Harness) -> None:
        await h.listening_port()
        h.feed.spec = keyed(2, "json:/id")
        await h.until(lambda: h.runner.restart_requested)

    h = run(scenario, FakeFeed(keyed(1, "json:/subject/reference")))
    assert h.log.of("spec_restart_required")[0]["fields"] == "partition_key"


def test_the_reload_table_names_fields_only() -> None:
    from dataclasses import replace

    from tributary.receivers.http.config import http_settings_from_spec
    from tributary.receivers.tls import TlsSpec

    from tests.support.receiver_specs import http_document as doc

    base = http_settings_from_spec(parse(doc("c")), env="dev")
    tls = replace(
        base,
        tls=TlsSpec("s", "required", allowed_client_sans=("dns:a.example.org",)),
        auth=replace(base.auth, mode="mtls"),
    )
    other = replace(tls, tls=TlsSpec("s", "required", allowed_client_sans=("dns:b.example.org",)))
    assert restart_required_fields(tls, other) == []
    assert live_fields_changed(tls, other) == ["tls.allowed_client_sans"]
    assert restart_required_fields(base, tls) == ["tls"]


# -- tombstone and redeploy -------------------------------------------------------------------------


def test_tombstone_answers_what_was_committed_refuses_the_rest_and_a_redeploy_listens_again() -> None:
    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.publisher.gate = asyncio.Event()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(http_request())
        await w.drain()
        await asyncio.wait_for(h.publisher.entered.wait(), 5)  # in flight: handed to the publisher

        # a second connection is open but has sent nothing yet
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        await h.until(lambda: h.runner.server is not None and h.runner.server.open_connections == 2)

        h.feed.spec = None  # the tombstone
        await h.until(lambda: "channel_undeployed" in h.log.names())
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", port)
        assert h.runner.server is None

        h.publisher.gate.set()
        assert await read_status(r) == 202  # committed before the tombstone: still answered, not dropped
        w.close()

        w2.write(http_request())  # a request on the connection that was open: refused, not published
        await w2.drain()
        assert await read_status(r2) == 503
        w2.close()
        assert len(h.publisher.calls) == 1

        h.feed.spec = parse(http_document("fhir-in", version=2))
        port2 = await h.listening_port(2)
        assert await post(port2) == 202
        assert len(h.publisher.calls) == 2

    h = run(scenario, FakeFeed(parse(http_document("fhir-in"))))
    assert h.runner.restart_requested is False


# -- mTLS revocation --------------------------------------------------------------------------------

needs_openssl = pytest.mark.skipif(
    not openssl_available() or shutil.which("openssl") is None, reason="openssl not on PATH"
)


def tls_doc(version: int, sans: list[str]) -> dict[str, Any]:
    return http_document(
        "fhir-in",
        version=version,
        auth={"type": "mtls"},
        tls={"secretRef": "srv-tls", "clientAuth": "required", "allowedClientSans": sans},
    )


@needs_openssl
def test_revoking_a_san_closes_an_open_idle_connection_and_a_new_one_is_refused(tmp_path: Path) -> None:
    client_ctx = build_pki(tmp_path)

    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
        await h.until(lambda: h.runner.server is not None and h.runner.server.open_connections == 1)

        h.feed.spec = parse(tls_doc(2, ["dns:someone-else.example.org"]))
        await h.until(lambda: "spec_applied" in h.log.names())
        assert await asyncio.wait_for(r.read(65536), 5) == b""  # closed at once, not at the idle timeout
        w.close()
        assert h.runner.server is not None and h.runner.server.metrics.rejected_peer_not_allowed == 1

        r2, w2 = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
        w2.write(http_request())
        await w2.drain()
        assert await read_status(r2) == 403  # the same certificate is refused at accept
        w2.close()
        assert h.publisher.calls == []

    run(
        scenario,
        FakeFeed(parse(tls_doc(1, ["dns:sender.example.org"]))),
        resolver=DirSecretResolver(tmp_path),
    )


@needs_openssl
def test_revoking_a_san_answers_a_request_already_handed_to_kafka_and_publishes_nothing_more(tmp_path: Path) -> None:
    client_ctx = build_pki(tmp_path)

    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        h.publisher.gate = asyncio.Event()
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
        w.write(http_request())
        await w.drain()
        await asyncio.wait_for(h.publisher.entered.wait(), 5)

        h.feed.spec = parse(tls_doc(2, ["dns:someone-else.example.org"]))
        await h.until(lambda: "spec_applied" in h.log.names())
        await asyncio.sleep(0.05)
        h.publisher.gate.set()
        assert await read_status(r) == 202  # committed before the revocation: its answer is still written
        assert await asyncio.wait_for(r.read(65536), 5) == b""
        w.close()
        assert len(h.publisher.calls) == 1

    run(
        scenario,
        FakeFeed(parse(tls_doc(1, ["dns:sender.example.org"]))),
        resolver=DirSecretResolver(tmp_path),
    )


@needs_openssl
def test_a_request_whose_body_is_buffered_when_the_san_is_revoked_is_refused_not_published(tmp_path: Path) -> None:
    """The window `w.close()` cannot cover: the whole request is already in the reader's buffer."""
    client_ctx = build_pki(tmp_path)

    async def scenario(h: Harness) -> None:
        port = await h.listening_port()
        assert h.runner.server is not None
        server = h.runner.server
        entered = asyncio.Event()
        release = asyncio.Event()
        original = server.budget.reserve

        async def slow_reserve(n: int, timeout: float) -> bool:
            entered.set()
            await release.wait()
            return await original(n, timeout)

        server.budget.reserve = slow_reserve  # type: ignore[method-assign]
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
        w.write(http_request())
        await w.drain()
        await asyncio.wait_for(entered.wait(), 5)  # head read, waiting for the queue: nothing published yet

        h.feed.spec = parse(tls_doc(2, ["dns:someone-else.example.org"]))
        await h.until(lambda: "spec_applied" in h.log.names())
        release.set()
        with contextlib.suppress(ssl.SSLError, ConnectionError):  # closed, or told 403: nothing is published
            await asyncio.wait_for(r.read(65536), 5)
        w.close()
        await h.until(lambda: server.open_connections == 0)  # the handler has finished, not just the client
        assert h.publisher.calls == []

    run(
        scenario,
        FakeFeed(parse(tls_doc(1, ["dns:sender.example.org"]))),
        resolver=DirSecretResolver(tmp_path),
    )


# -- entry point ------------------------------------------------------------------------------------


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the refusal")


def _main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *argv: str) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("sys.argv", ["prog", *argv])
    server_mod.main()


@pytest.mark.parametrize("form", ["separate", "equals"])
def test_channel_spec_flag_is_retired_and_its_value_never_echoed(
    form: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = "SYNTH-SPEC-PATH-5d1e"
    flag = [f"--channel-spec={value}"] if form == "equals" else ["--channel-spec", value]
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *flag, "--channel", "fhir-in", "--env", "dev")
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
