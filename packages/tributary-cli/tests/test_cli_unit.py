"""CLI unit tests: configuration and token handling, the client's error mapping, and command behaviour against
a scripted transport. No network, no containers."""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
from typing import Any

import pytest
from tributary.cli import config, main
from tributary.cli.client import ApiClient, urllib_transport
from tributary.cli.errors import ApiError, ConfigError, UnreachableError
from typer.testing import CliRunner

TOKEN = "tok-" + "x" * 40  # not a real credential
runner = CliRunner()


def env(**kw: str) -> dict[str, str]:
    return dict(kw)


# -- token sources ---------------------------------------------------------------------------------


def test_token_from_the_environment_and_default_url() -> None:
    cfg = config.load(env(TRIBUTARY_TOKEN=TOKEN))
    assert cfg.token == TOKEN and cfg.base_url == "http://127.0.0.1:8080"
    assert TOKEN not in repr(cfg)  # a log line or traceback never carries it


def test_token_from_a_private_file(tmp_path: Path) -> None:
    f = tmp_path / "token"
    f.write_text(TOKEN + "\n")
    f.chmod(0o600)
    assert config.load(env(TRIBUTARY_TOKEN_FILE=str(f))).token == TOKEN


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o644, 0o660])
def test_a_token_file_open_to_group_or_others_is_refused(tmp_path: Path, mode: int) -> None:
    f = tmp_path / "token"
    f.write_text(TOKEN)
    f.chmod(mode)
    with pytest.raises(ConfigError, match="chmod 600"):
        config.load(env(TRIBUTARY_TOKEN_FILE=str(f)))


def test_refusals(tmp_path: Path) -> None:
    f = tmp_path / "t"
    f.write_text(TOKEN)
    f.chmod(0o600)
    for bad in (
        env(),  # no token at all
        env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_TOKEN_FILE=str(f)),  # two sources
        env(TRIBUTARY_TOKEN_FILE=str(tmp_path)),  # a directory
        env(TRIBUTARY_TOKEN_FILE=str(tmp_path / "missing")),
        env(TRIBUTARY_TOKEN="two words"),  # noqa: S106
        env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="http://api.example.org"),  # token over plain http
        env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="ftp://127.0.0.1"),
        env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="https://user:pw@api.example.org"),
    ):
        with pytest.raises(ConfigError):
            config.load(bad)
    config.load(env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="https://api.example.org/"))
    config.load(env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="http://localhost:8080"))
    config.load(env(TRIBUTARY_TOKEN=TOKEN, TRIBUTARY_API_URL="http://[::1]:8080"))


def test_no_command_accepts_the_token_as_an_argument() -> None:
    import typer.main

    def walk(cmd: Any) -> list[str]:  # typer vendors its own click, so duck-type the command tree
        names = [o for p in cmd.params for o in [*(getattr(p, "opts", None) or []), p.name or ""]]
        for sub in getattr(cmd, "commands", {}).values():
            names += walk(sub)
        return names

    names = walk(typer.main.get_command(main.app))
    assert "--json" in names  # the walk really reached the commands
    assert not [n for n in names if any(w in n.lower() for w in ("token", "password", "secret"))]
    assert "token" not in inspect.signature(ApiClient.__init__).parameters


# -- client ----------------------------------------------------------------------------------------


class Req:
    def __init__(self, method: str, url: str, body: bytes | None, headers: dict[str, str]) -> None:
        self.method, self.url, self.headers = method, url, headers
        self.json = json.loads(body) if body else None


def client_with(handler: Any) -> ApiClient:
    """`handler(Req) -> (status, payload)`; a str payload is sent as is, anything else as JSON."""

    def transport(method: str, url: str, body: bytes | None, headers: dict[str, str], _t: float) -> tuple[int, bytes]:
        status, payload = handler(Req(method, url, body, headers))
        return status, payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()

    return ApiClient(config.ClientConfig("http://127.0.0.1:1", TOKEN), transport=transport)


def test_the_token_is_sent_only_as_a_bearer_header() -> None:
    seen: list[Req] = []

    def handler(req: Req) -> tuple[int, Any]:
        seen.append(req)
        return 200, {"items": [], "next_cursor": None}

    with client_with(handler) as api:
        assert api.list_channels() == []
    assert seen[0].headers["Authorization"] == f"Bearer {TOKEN}"
    assert TOKEN not in seen[0].url


def test_a_redirect_is_an_error_and_the_target_is_never_contacted() -> None:
    """Real sockets: a server that answers 302 to a second server; the second must see no request."""
    import http.server
    import threading

    hits: list[str] = []

    class Target(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            hits.append(self.headers.get("Authorization", ""))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_a: Any) -> None:
            return

    target = http.server.HTTPServer(("127.0.0.1", 0), Target)

    class Redirector(Target):
        def do_GET(self) -> None:
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{target.server_port}/steal")
            self.end_headers()

    front = http.server.HTTPServer(("127.0.0.1", 0), Redirector)
    threads = [threading.Thread(target=x.serve_forever, daemon=True) for x in (target, front)]
    for t in threads:
        t.start()
    try:
        client = ApiClient(config.ClientConfig(f"http://127.0.0.1:{front.server_port}", TOKEN))
        with pytest.raises(ApiError) as e:
            client.environments()
        assert e.value.status == 302 and hits == []
    finally:
        for x in (target, front):
            x.shutdown()
            x.server_close()


def test_problem_responses_become_typed_errors_with_fixed_text() -> None:
    body = {"type": "about:blank", "title": "Forbidden", "status": 403, "code": "FORBIDDEN"}
    with client_with(lambda r: (403, body)) as api, pytest.raises(ApiError) as e:
        api.get_channel("x")
    assert (e.value.code, e.value.exit_code, e.value.message) == ("FORBIDDEN", 3, "403 FORBIDDEN: Forbidden")
    with client_with(lambda r: (502, "<html>secret body</html>")) as api, pytest.raises(ApiError) as e2:
        api.get_channel("x")
    assert e2.value.code == "UNKNOWN" and "secret body" not in e2.value.message and e2.value.exit_code == 4


def test_a_closed_port_and_a_timeout_are_unreachable() -> None:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # closed again on exit: nothing listens
    with pytest.raises(UnreachableError):
        urllib_transport("GET", f"http://127.0.0.1:{port}/", None, {}, 2.0)
    with socket.socket() as silent:  # accepts the connection (backlog) and never answers
        silent.bind(("127.0.0.1", 0))
        silent.listen(1)
        with pytest.raises(UnreachableError, match="in time"):
            urllib_transport("GET", f"http://127.0.0.1:{silent.getsockname()[1]}/", None, {}, 0.5)


def test_path_segments_are_quoted() -> None:
    seen: list[str] = []

    def handler(req: Req) -> tuple[int, Any]:
        seen.append(req.url)
        return 200, {}

    with client_with(handler) as api:
        api.get_channel("a/../b?x=1")
    assert seen == ["http://127.0.0.1:1/v1/channels/a%2F..%2Fb%3Fx%3D1"]


def test_pagination_follows_the_cursor() -> None:
    def handler(req: Req) -> tuple[int, Any]:
        if "cursor=c2" in req.url:
            return 200, {"items": [{"n": 2}], "next_cursor": None}
        return 200, {"items": [{"n": 1}], "next_cursor": "c2"}

    with client_with(handler) as api:
        assert api.list_channels() == [{"n": 1}, {"n": 2}]


# -- commands --------------------------------------------------------------------------------------


@pytest.fixture
def scripted(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, Any]]:
    calls: list[tuple[str, str, Any]] = []
    replies: dict[tuple[str, str], tuple[int, Any]] = {}

    def handler(req: Req) -> tuple[int, Any]:
        path = req.url.split("127.0.0.1:1", 1)[1].split("?", 1)[0]
        calls.append((req.method, path, req.json))
        return replies.get((req.method, path), (200, {"items": [], "next_cursor": None}))

    monkeypatch.setattr(main, "_client", lambda: client_with(handler))
    calls.append(("replies", "", replies))
    return calls


def record(**kw: Any) -> dict[str, Any]:
    return {
        "id": "00000000-0000-4000-8000-000000000000",
        "environment": "dev",
        "channel": "adt",
        "kind": "deploy",
        "version": 2,
        "previous_version": 1,
        "status": "published",
        "requested_by": "u",
        "requested_at": "2026-10-06T00:00:00Z",
        **kw,
    }


def test_deploy_sends_the_guard_only_when_asked_and_prints_the_record(scripted: list[Any]) -> None:
    replies = scripted[0][2]
    replies[("POST", "/v1/environments/dev/channels/adt/deploy")] = (202, record())
    r = runner.invoke(main.app, ["deploy", "dev", "adt", "2"])
    assert r.exit_code == 0 and "v2 published" in r.output
    r = runner.invoke(main.app, ["deploy", "dev", "adt", "2", "--expect", "none"])
    r = runner.invoke(main.app, ["deploy", "dev", "adt", "2", "--expect", "1"])
    bodies = [c[2] for c in scripted if c[1].endswith("/deploy")]
    assert bodies == [
        {"version": 2},
        {"version": 2, "expected_current_version": None},
        {"version": 2, "expected_current_version": 1},
    ]
    assert runner.invoke(main.app, ["deploy", "dev", "adt", "2", "--expect", "zero"]).exit_code == 2


def test_a_failed_deployment_exits_1_and_says_how_to_retry(scripted: list[Any]) -> None:
    scripted[0][2][("POST", "/v1/environments/dev/channels/adt/deploy")] = (
        202,
        record(status="failed", failure_code="PUBLISH_TIMEOUT"),
    )
    r = runner.invoke(main.app, ["deploy", "dev", "adt", "2"])
    assert r.exit_code == 1 and "PUBLISH_TIMEOUT" in r.output


def test_rollback_requires_the_expected_version(scripted: list[Any]) -> None:
    assert runner.invoke(main.app, ["rollback", "dev", "adt"]).exit_code == 2
    scripted[0][2][("POST", "/v1/environments/dev/channels/adt/rollback")] = (202, record(kind="rollback", version=1))
    r = runner.invoke(main.app, ["rollback", "dev", "adt", "--expect", "2", "--to", "1"])
    assert r.exit_code == 0
    assert [c[2] for c in scripted if c[1].endswith("/rollback")] == [{"expected_current_version": 2, "to_version": 1}]


def test_api_errors_map_to_exit_codes_and_one_stderr_line(scripted: list[Any]) -> None:
    problem = {"type": "about:blank", "title": "Conflict", "status": 409, "code": "MANAGEMENT_MODE"}
    scripted[0][2][("POST", "/v1/environments/prod/channels/adt/deploy")] = (409, problem)
    r = runner.invoke(main.app, ["deploy", "prod", "adt", "1"])
    assert r.exit_code == 4 and "MANAGEMENT_MODE" in r.output and TOKEN not in r.output


def test_a_missing_token_is_a_usage_error_before_any_request(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("TRIBUTARY_TOKEN", "TRIBUTARY_TOKEN_FILE"):
        monkeypatch.delenv(name, raising=False)
    r = runner.invoke(main.app, ["channels", "list"])
    assert r.exit_code == 2 and "TRIBUTARY_TOKEN" in r.output


def test_test_command_reports_paths_and_rules_never_values(scripted: list[Any], tmp_path: Path) -> None:
    doc = tmp_path / "c.json"
    doc.write_text('{"apiVersion": "tributary.dev/v1", "kind": "Channel", "metadata": {"name": "SECRET-VALUE"}}')
    scripted[0][2][("POST", "/v1/validate")] = (
        200,
        {"valid": False, "errors": [{"json_path": "$.spec", "validator": "required"}], "warnings": []},
    )
    r = runner.invoke(main.app, ["test", str(doc)])
    assert r.exit_code == 1 and "$.spec (required)" in r.output and "INVALID" in r.output
    assert "SECRET-VALUE" not in r.output


def test_unreadable_and_oversized_inputs_are_usage_errors(scripted: list[Any], tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("a: [unclosed")
    assert runner.invoke(main.app, ["test", str(bad)]).exit_code == 2
    big = tmp_path / "big.json"
    big.write_text(" " * 262145)
    assert runner.invoke(main.app, ["test", str(big)]).exit_code == 2
    assert runner.invoke(main.app, ["test", str(tmp_path / "nope.json")]).exit_code == 2


def test_push_refuses_a_document_that_sets_its_own_version(scripted: list[Any], tmp_path: Path) -> None:
    f = tmp_path / "c.yaml"
    f.write_text("apiVersion: tributary.dev/v1\nkind: Channel\nmetadata:\n  name: adt\n  version: 3\nspec: {}\n")
    r = runner.invoke(main.app, ["channels", "push", str(f)])
    assert r.exit_code == 2 and "metadata.version" in r.output
    assert not [c for c in scripted if c[0] in ("POST",)]


def test_dev_requires_env_with_deploy(scripted: list[Any], tmp_path: Path) -> None:
    f = tmp_path / "c.json"
    f.write_text("{}")
    assert runner.invoke(main.app, ["dev", str(f), "--deploy", "--once"]).exit_code == 2


def test_messages_search_show_and_reprocess_call_the_archive_endpoints(scripted: list[Any]) -> None:
    base = "/v1/environments/dev/channels/adt"
    ev = {"ts": "2026-10-06T07:00:00.000Z", "message_id": "m1", "stage": "sent", "status": "ok", "attempt": 0,
          "destination": "ehr"}  # fmt: skip
    replies = scripted[0][2]
    replies[("POST", base + "/messages/search")] = (200, {"items": [ev], "next_cursor": "CUR"})
    replies[("GET", base + "/messages/m1")] = (
        200,
        {"events": [ev], "destinations": [{"destination": "ehr", "state": "sent", "attempt": 0}],
         "content": [{"stage": "received", "size_bytes": 12, "storage": "inline"}]},
    )  # fmt: skip
    replies[("POST", base + "/messages/m1/reprocess")] = (
        200, {"original_message_id": "m1", "new_message_id": "m2"},
    )  # fmt: skip
    r = runner.invoke(
        main.app,
        [
            "messages",
            "search",
            "adt",
            "--env",
            "dev",
            "--since",
            "2026-10-06T00:00:00.000Z",
            "--stage",
            "sent",
            "--limit",
            "5",
        ],
    )
    assert r.exit_code == 0 and "m1" in r.output and "next: --cursor CUR" in r.output
    body = next(c[2] for c in scripted if c[1].endswith("/messages/search"))
    assert body == {"since": "2026-10-06T00:00:00.000Z", "limit": 5, "stages": ["sent"]}
    shown = runner.invoke(main.app, ["messages", "show", "adt", "m1", "--env", "dev"])
    assert shown.exit_code == 0 and "content received" in shown.output and "12 bytes" in shown.output
    done = runner.invoke(main.app, ["messages", "reprocess", "adt", "m1", "--env", "dev", "--allow-reorder"])
    assert done.exit_code == 0 and "m2" in done.output
    sent = next(c[2] for c in scripted if c[1].endswith("/reprocess"))
    assert sent["allow_reorder"] is True and len(sent["operation_id"]) == 36  # a fresh UUIDv7 each time


def test_replay_start_show_and_exit_codes(scripted: list[Any]) -> None:
    base = "/v1/environments/dev/channels/adt"
    rec = {"replay_id": "r1", "status": "running", "range_start": "a", "range_end": "b"}
    replies = scripted[0][2]
    replies[("POST", base + "/replays")] = (202, rec)
    r = runner.invoke(main.app, ["replay", "start", "adt", "--env", "dev", "--start", "a", "--end", "b"])
    assert r.exit_code == 0 and r.output.startswith("started r1")
    replies[("POST", base + "/replays")] = (200, rec)
    assert runner.invoke(
        main.app, ["replay", "start", "adt", "--env", "dev", "--start", "a", "--end", "b"]
    ).output.startswith("exists")
    replies[("GET", base + "/replays/r1")] = (
        200,
        {**rec, "status": "failed", "failure_code": "SOURCE_ERROR", "republished": 2},
    )
    failed = runner.invoke(main.app, ["replay", "show", "adt", "r1", "--env", "dev"])
    assert failed.exit_code == 1 and "SOURCE_ERROR" in failed.output
    problem = {"type": "about:blank", "title": "Conflict", "status": 409, "code": "ORDERING_WOULD_BREAK"}
    replies[("POST", base + "/replays")] = (409, problem)
    refused = runner.invoke(main.app, ["replay", "start", "adt", "--env", "dev", "--start", "a", "--end", "b"])
    assert refused.exit_code == 4 and "ORDERING_WOULD_BREAK" in refused.output


def test_ps_visible_environment_does_not_leak_the_token_into_output(
    scripted: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRIBUTARY_TOKEN", TOKEN)
    r = runner.invoke(main.app, ["channels", "list"])
    assert TOKEN not in r.output and os.environ["TRIBUTARY_TOKEN"] == TOKEN
