"""`tributary`: dev, test, deploy (with rollback, undeploy, status), channels, messages.

Every command is a call to the control plane API. The token is read from the environment or a file
(`config.py`), never from an argument. Exit codes are in `errors.py`.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import typer

from . import config
from .client import ApiClient
from .documents import channel_name, load_document, without_version
from .errors import (
    EXIT_FAILED,
    EXIT_OK,
    ApiError,
    CliError,
    InputError,
    UnavailableError,
)

app = typer.Typer(
    name="tributary",
    help="Command line client for the Tributary control plane. Set TRIBUTARY_API_URL and TRIBUTARY_TOKEN "
    "(or TRIBUTARY_TOKEN_FILE); the token is never taken from an argument.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,  # a traceback could carry request data
)
channels_app = typer.Typer(help="List, show and publish channel versions.", no_args_is_help=True)
app.add_typer(channels_app, name="channels")

#: Replaced by tests to drive the watch loop without waiting.
sleep: Callable[[float], None] = time.sleep

JsonOpt = Annotated[bool, typer.Option("--json", help="Print the API's JSON instead of text.")]


def _client() -> ApiClient:
    return ApiClient(config.load())


def out(line: str = "") -> None:
    typer.echo(line)


def emit(data: Any) -> None:
    typer.echo(json.dumps(data, indent=2, sort_keys=True))


def fail(exc: CliError) -> typer.Exit:
    typer.echo(f"error: {exc.message}", err=True)
    return typer.Exit(exc.exit_code)


def guarded[**P](fn: Callable[P, None]) -> Callable[P, None]:
    """Turn typed failures into one stderr line and an exit code; anything else is a bug and propagates."""
    import functools

    @functools.wraps(fn)
    def run(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            fn(*args, **kwargs)
        except CliError as exc:
            raise fail(exc) from None

    return run


def _origin(git_commit: str | None, git_path: str | None) -> dict[str, Any]:
    if git_commit is None and git_path is None:
        return {"kind": "api"}
    if git_commit is None or git_path is None:
        raise InputError("--git-commit and --git-path go together")
    return {"kind": "git", "commit": git_commit, "path": git_path}


GitCommit = Annotated[str | None, typer.Option("--git-commit", help="Mark the version as coming from this commit.")]
GitPath = Annotated[str | None, typer.Option("--git-path", help="Path of the file in the Git repository.")]


# -- test ------------------------------------------------------------------------------------------


def _report(label: str, report: dict[str, Any]) -> bool:
    for issue in report.get("errors", []):
        out(f"{label}: error {issue['json_path']} ({issue['validator']})")
    for issue in report.get("warnings", []):
        out(f"{label}: warning {issue.get('json_path', '$')} ({issue.get('validator', '')})")
    out(f"{label}: {'valid' if report['valid'] else 'INVALID'}")
    return bool(report["valid"])


@app.command("test")
@guarded
def test_cmd(
    files: Annotated[list[Path], typer.Argument(help="Channel documents (YAML or JSON).")],
    as_json: JsonOpt = False,
) -> None:
    """Validate channel documents against the channel-spec schema, on the server. Exit 1 if any is invalid."""
    ok = True
    reports: dict[str, Any] = {}
    with _client() as api:
        for path in files:
            report = api.validate(load_document(path))
            reports[path.name] = report
            if not as_json:
                ok = _report(path.name, report) and ok
            else:
                ok = bool(report["valid"]) and ok
    if as_json:
        emit(reports)
    raise typer.Exit(EXIT_OK if ok else EXIT_FAILED)


# -- channels --------------------------------------------------------------------------------------


@channels_app.command("list")
@guarded
def channels_list(as_json: JsonOpt = False) -> None:
    """List channels with their latest version."""
    with _client() as api:
        items = api.list_channels()
    if as_json:
        return emit(items)
    for c in items:
        out(f"{c['name']}\tlatest v{c['latest_version']}")


@channels_app.command("show")
@guarded
def channels_show(name: str, as_json: JsonOpt = False) -> None:
    """Show a channel and where it is deployed."""
    with _client() as api:
        ch = api.get_channel(name)
    if as_json:
        return emit(ch)
    out(f"{ch['name']}: latest v{ch['latest_version']}")
    for d in ch["deployments"]:
        out(
            f"  {d['environment']}: {'undeployed' if d['version'] is None else 'v' + str(d['version'])} ({d['status']})"
        )


@channels_app.command("versions")
@guarded
def channels_versions(name: str, as_json: JsonOpt = False) -> None:
    """List a channel's versions, newest first."""
    with _client() as api:
        items = api.list_versions(name)
    if as_json:
        return emit(items)
    for v in items:
        out(f"v{v['version']}\t{v['spec_sha256'][:12]}\t{v.get('origin', {}).get('kind', '')}")


def push_document(api: ApiClient, document: dict[str, Any], origin: dict[str, Any]) -> tuple[int, str]:
    """Make the document a version: create the channel, add a version, or reuse the latest when it is
    identical. Returns `(version, "created" | "added" | "unchanged")`."""
    name = channel_name(document)
    if "version" in document["metadata"]:
        raise InputError("metadata.version is assigned by the server; remove it from the file")
    try:
        latest = api.get_channel(name)["latest_version"]
    except ApiError as exc:
        if exc.code != "NOT_FOUND":
            raise
        return api.create_channel(document, origin)["version"], "created"
    current = api.get_version(name, latest)["document"]
    if without_version(current) == document:
        return latest, "unchanged"
    return api.add_version(name, document, origin)["version"], "added"


@channels_app.command("push")
@guarded
def channels_push(
    file: Path,
    git_commit: GitCommit = None,
    git_path: GitPath = None,
    as_json: JsonOpt = False,
) -> None:
    """Create the channel or add a version from a file. An identical file adds nothing."""
    document = load_document(file)
    origin = _origin(git_commit, git_path)
    with _client() as api:
        version, what = push_document(api, document, origin)
    if as_json:
        return emit({"channel": channel_name(document), "version": version, "result": what})
    out(f"{channel_name(document)}: v{version} ({what})")


# -- deploy, rollback, undeploy, status ------------------------------------------------------------


def _expected(raw: str | None) -> int | str | None:
    if raw is None:
        return "absent"
    if raw == "none":
        return None
    try:
        value = int(raw)
    except ValueError:
        raise InputError("--expect takes a version number or 'none'") from None
    if value < 1:
        raise InputError("--expect takes a version number or 'none'")
    return value


def _finish(record: dict[str, Any], as_json: bool) -> None:
    if as_json:
        emit(record)
    else:
        v = record["version"]
        what = "undeployed" if v is None else f"v{v}"
        out(f"{record['kind']} {record['channel']} in {record['environment']}: {what} {record['status']}")
        if record["status"] == "failed":
            out(f"failure: {record.get('failure_code', 'UNKNOWN')} (re-run the same command to retry)")
    raise typer.Exit(EXIT_FAILED if record["status"] == "failed" else EXIT_OK)


@app.command("deploy")
@guarded
def deploy_cmd(
    env: str,
    channel: str,
    version: int,
    expect: Annotated[str | None, typer.Option(help="Guard: the version you believe is current, or 'none'.")] = None,
    as_json: JsonOpt = False,
) -> None:
    """Deploy a channel version to an environment. Prints the deployment record; exit 1 if it failed."""
    guard = _expected(expect)
    with _client() as api:
        record = api.deploy(env, channel, version, expected=guard)
    _finish(record, as_json)


@app.command("rollback")
@guarded
def rollback_cmd(
    env: str,
    channel: str,
    expect: Annotated[int, typer.Option(min=1, help="The version you believe is current (required).")],
    to: Annotated[int | None, typer.Option(min=1, help="Target version. Default: the one published before.")] = None,
    as_json: JsonOpt = False,
) -> None:
    """Roll a channel back in an environment."""
    with _client() as api:
        record = api.rollback(env, channel, expected=expect, to_version=to)
    _finish(record, as_json)


@app.command("undeploy")
@guarded
def undeploy_cmd(
    env: str,
    channel: str,
    expect: Annotated[int, typer.Option(min=1, help="The version you believe is current (required).")],
    as_json: JsonOpt = False,
) -> None:
    """Remove a channel from an environment (publishes a tombstone)."""
    with _client() as api:
        record = api.undeploy(env, channel, expected=expect)
    _finish(record, as_json)


@app.command("status")
@guarded
def status_cmd(env: str, channel: str | None = typer.Argument(None), as_json: JsonOpt = False) -> None:
    """What is deployed in an environment, or the deployment history of one channel in it."""
    with _client() as api:
        items = api.current_deployments(env) if channel is None else api.channel_deployments(env, channel)
    if as_json:
        return emit(items)
    for d in items:
        v = "-" if d["version"] is None else f"v{d['version']}"
        out(f"{d['channel']}\t{d['kind']}\t{v}\t{d['status']}")


# -- dev -------------------------------------------------------------------------------------------


@app.command("dev")
@guarded
def dev_cmd(
    file: Path,
    env: Annotated[str | None, typer.Option(help="With --deploy: the environment to deploy to.")] = None,
    deploy: Annotated[bool, typer.Option("--deploy", help="Push and deploy each valid change.")] = False,
    once: Annotated[bool, typer.Option("--once", help="Run one cycle and exit.")] = False,
    interval: Annotated[float, typer.Option(min=0.2, help="Seconds between file checks.")] = 1.0,
    iterations: Annotated[int | None, typer.Option(hidden=True, min=1)] = None,
) -> None:
    """Watch a channel file: validate on every save and, with --deploy, push and deploy it.

    Without --deploy nothing is written to the API. Exit code of --once: 0 valid (and deployed), 1 not.
    """
    if deploy and env is None:
        raise InputError("--deploy needs --env")
    last = b""
    last_ok = True
    cycle = 0
    with _client() as api:
        while True:
            cycle += 1
            digest = hashlib.sha256(file.read_bytes() if file.exists() else b"").digest()
            if digest != last:
                last = digest
                last_ok = _dev_cycle(api, file, env if deploy else None)
            if once or (iterations is not None and cycle >= iterations):
                raise typer.Exit(EXIT_OK if last_ok else EXIT_FAILED)
            sleep(interval)


def _dev_cycle(api: ApiClient, file: Path, env: str | None) -> bool:
    try:
        document = load_document(file)
        if not _report(file.name, api.validate(document)):
            return False
        if env is None:
            return True
        version, what = push_document(api, document, {"kind": "api"})
        out(f"{channel_name(document)}: v{version} ({what})")
        name = channel_name(document)
        current = next((d for d in api.current_deployments(env) if d["channel"] == name), None)
        if current is not None and current["version"] == version and current["status"] == "published":
            out(f"{name}: already v{version} in {env}")
            return True
        guard = None if current is None else current["version"]
        record = api.deploy(env, name, version, expected=guard)
        out(f"deploy {name} in {env}: v{version} {record['status']}")
        return bool(record["status"] != "failed")
    except ApiError as exc:
        if exc.exit_code == 3:
            raise  # a bad token will not fix itself on the next save: stop
        out(f"{file.name}: {exc.message}")
        return False
    except InputError as exc:
        out(f"{file.name}: {exc.message}")
        return False


# -- messages --------------------------------------------------------------------------------------


@app.command("messages")
@guarded
def messages_cmd() -> None:
    """Search and reprocess messages (not available yet)."""
    raise UnavailableError(
        "this API version has no messages endpoints (openapi/v0 part 2, task W3-6); nothing was sent"
    )


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
