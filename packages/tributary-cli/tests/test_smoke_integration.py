"""CLI smoke tests (W3-7): the real CLI against the real API app on real PostgreSQL, over real HTTP.

The token reaches the CLI through the environment or a file, as it would for an operator. Covers the five
commands (`dev`, `test`, `deploy` with `rollback`/`undeploy`/`status`, `channels`, `messages`) and the
refusals the API makes (management mode, role, token)."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from smoke_kit import LiveApi
from tributary.api.publisher import PublishError
from tributary.cli import main
from typer.testing import CliRunner

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[3]
EXAMPLE = REPO / "contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json"
runner = CliRunner()


def document(name: str = "adt-passthrough", retries: int | None = None) -> dict[str, Any]:
    doc = json.loads(EXAMPLE.read_text())
    doc["metadata"] = {"name": name}
    if retries is not None:
        doc["spec"]["pipeline"]["stepRetries"] = retries
    return doc


def write(tmp_path: Path, doc: dict[str, Any], name: str = "channel.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(doc))
    return path


@pytest.fixture
def cli(api: LiveApi, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("TRIBUTARY_API_URL", api.url)
    monkeypatch.setenv("TRIBUTARY_TOKEN", api.token("tb-admin"))

    def run(*args: str, expect: int = 0) -> Any:
        result = runner.invoke(main.app, list(args))
        assert result.exit_code == expect, (args, result.exit_code, result.output)
        return result

    return run


def test_validate_push_deploy_rollback_undeploy_through_the_cli(cli: Any, api: LiveApi, tmp_path: Path) -> None:
    v1 = write(tmp_path, document(), "v1.json")
    v2 = write(tmp_path, document(retries=4), "v2.json")
    bad = write(tmp_path, {"apiVersion": "tributary.dev/v1", "kind": "Channel", "metadata": {"name": "x"}}, "bad.json")

    assert "valid" in cli("test", str(v1)).output
    invalid = cli("test", str(bad), expect=1)
    assert "INVALID" in invalid.output and "$" in invalid.output

    assert "v1 (created)" in cli("channels", "push", str(v1)).output
    assert "v1 (unchanged)" in cli("channels", "push", str(v1)).output  # idempotent: no duplicate version
    assert "v2 (added)" in cli("channels", "push", str(v2)).output
    assert "latest v2" in cli("channels", "list").output
    assert [v["version"] for v in json.loads(cli("channels", "versions", "adt-passthrough", "--json").output)] == [2, 1]

    assert "v1 published" in cli("deploy", "dev", "adt-passthrough", "1", "--expect", "none").output
    assert "v2 published" in cli("deploy", "dev", "adt-passthrough", "2", "--expect", "1").output
    assert "dev: v2 (published)" in cli("channels", "show", "adt-passthrough").output
    mismatch = cli("deploy", "dev", "adt-passthrough", "1", "--expect", "5", expect=4)
    assert "CURRENT_VERSION_MISMATCH" in mismatch.output
    assert "v1 published" in cli("rollback", "dev", "adt-passthrough", "--expect", "2").output
    assert "adt-passthrough\trollback\tv1\tpublished" in cli("status", "dev").output.replace("  ", "\t")
    assert "undeployed published" in cli("undeploy", "dev", "adt-passthrough", "--expect", "1").output

    # What the CLI did is what reached the channels topic: v1, v2, v1 (the old bytes), then a tombstone.
    records = [r[2] for r in api.publisher.records]
    assert [hashlib.sha256(r).hexdigest() if r else None for r in records[:2]] == [
        json.loads(cli("channels", "versions", "adt-passthrough", "--json").output)[1]["spec_sha256"],
        json.loads(cli("channels", "versions", "adt-passthrough", "--json").output)[0]["spec_sha256"],
    ]
    assert records[2] == records[0] and records[3] is None


def test_git_managed_environment_refuses_a_ui_version_and_accepts_a_git_one(cli: Any, tmp_path: Path) -> None:
    f = write(tmp_path, document("from-api"))
    cli("channels", "push", str(f))
    refused = cli("deploy", "prod", "from-api", "1", expect=4)
    assert "MANAGEMENT_MODE" in refused.output
    g = write(tmp_path, document("from-git"), "g.json")
    cli("channels", "push", str(g), "--git-commit", "a" * 40, "--git-path", "channels/adt.json")
    assert "v1 published" in cli("deploy", "prod", "from-git", "1").output
    assert cli("channels", "push", str(g), "--git-commit", "a" * 40, expect=2)  # half an origin is a usage error


def test_a_failed_publish_exits_1_and_a_retry_succeeds(cli: Any, api: LiveApi, tmp_path: Path) -> None:
    cli("channels", "push", str(write(tmp_path, document())))
    api.publisher.fail_with = PublishError
    failed = cli("deploy", "dev", "adt-passthrough", "1", expect=1)
    assert "failed" in failed.output and "PUBLISH_FAILED" in failed.output
    api.publisher.fail_with = None
    assert "v1 published" in cli("deploy", "dev", "adt-passthrough", "1").output


def test_roles_and_tokens_are_enforced_and_map_to_exit_3(
    cli: Any, api: LiveApi, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli("channels", "push", str(write(tmp_path, document())))
    monkeypatch.setenv("TRIBUTARY_TOKEN", api.token("tb-viewer"))
    cli("channels", "list")  # a viewer may read
    forbidden = cli("deploy", "dev", "adt-passthrough", "1", expect=3)
    assert "FORBIDDEN" in forbidden.output
    monkeypatch.setenv("TRIBUTARY_TOKEN", "not.a.jwt")
    assert "TOKEN_INVALID" in cli("channels", "list", expect=3).output
    assert api.publisher.records == []  # nothing was published by either refused call


def test_token_file_and_the_console_script_work_as_a_subprocess(api: LiveApi, tmp_path: Path) -> None:
    token = tmp_path / "token"
    token.write_text(api.token("tb-viewer"))
    token.chmod(0o600)
    env = {
        "PATH": os.environ["PATH"],
        "TRIBUTARY_API_URL": api.url,
        "TRIBUTARY_TOKEN_FILE": str(token),
    }
    run = subprocess.run(
        [sys.executable, "-m", "tributary.cli.main", "channels", "list", "--json"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0 and json.loads(run.stdout) == []
    token.chmod(0o644)
    bad = subprocess.run(
        [sys.executable, "-m", "tributary.cli.main", "channels", "list"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert bad.returncode == 2 and "chmod 600" in bad.stderr and token.read_text() not in bad.stdout + bad.stderr


def test_dev_validates_and_with_deploy_pushes_and_deploys_each_change(
    cli: Any, api: LiveApi, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = write(tmp_path, document())
    assert "valid" in cli("dev", str(f), "--once").output
    assert api.publisher.records == []  # validate-only writes nothing

    first = cli("dev", str(f), "--env", "dev", "--deploy", "--once")
    assert "v1 (created)" in first.output and "v1 published" in first.output
    again = cli("dev", str(f), "--env", "dev", "--deploy", "--once")
    assert "already v1 in dev" in again.output and len(api.publisher.records) == 1  # idempotent

    # The watch loop: one edit between two checks produces one new version and one deploy.
    def edit(_: float) -> None:
        f.write_text(json.dumps(document(retries=4)))
        monkeypatch.setattr(main, "sleep", lambda _s: None)

    monkeypatch.setattr(main, "sleep", edit)
    watched = cli("dev", str(f), "--env", "dev", "--deploy", "--iterations", "2")
    assert "v2 (added)" in watched.output and "v2 published" in watched.output
    assert len(api.publisher.records) == 2

    f.write_text(json.dumps({"apiVersion": "tributary.dev/v1", "kind": "Channel", "metadata": {"name": "x"}}))
    cli("dev", str(f), "--env", "dev", "--deploy", "--once", expect=1)  # invalid: nothing pushed
    assert len(api.publisher.records) == 2


def test_messages_is_unavailable_until_the_api_has_the_endpoints(cli: Any) -> None:
    assert "W3-6" in cli("messages", expect=69).output
