"""`tributary`: dev, test, deploy (with rollback, undeploy, status), channels, messages (search, show, reprocess), replay.

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


# -- messages and replay (metadata only: this tool never prints message content) --------------------


messages_app = typer.Typer(help="Search, inspect and reprocess archived messages.", no_args_is_help=True)
replay_app = typer.Typer(help="Replay a channel's raw records back into the same channel.", no_args_is_help=True)
app.add_typer(messages_app, name="messages")
app.add_typer(replay_app, name="replay")
EnvOpt = Annotated[str, typer.Option("--env", help="The environment.")]


def _event_line(e: dict[str, Any]) -> str:
    dest = f"\t{e['destination']}" if e.get("destination") else "\t-"
    return f"{e['ts']}\t{e['message_id']}\t{e['stage']}\t{e['status']}{dest}\tattempt {e['attempt']}"


@messages_app.command("search")
@guarded
def messages_search(
    channel: str,
    env: EnvOpt,
    since: Annotated[str, typer.Option(help="Start, UTC, e.g. 2026-10-01T00:00:00.000Z.")],
    until: Annotated[str | None, typer.Option(help="End (exclusive).")] = None,
    stage: Annotated[list[str] | None, typer.Option(help="Only these stages (repeat).")] = None,
    status: Annotated[str | None, typer.Option(help="ok, error, timeout or dropped.")] = None,
    destination: Annotated[str | None, typer.Option()] = None,
    error_code: Annotated[str | None, typer.Option()] = None,
    limit: Annotated[int, typer.Option(min=1, max=200)] = 50,
    cursor: Annotated[str | None, typer.Option(help="From a previous page.")] = None,
    as_json: JsonOpt = False,
) -> None:
    """Search events (metadata only), newest first. Prints the cursor for the next page."""
    body: dict[str, Any] = {"since": since, "limit": limit}
    for name, value in (("until", until), ("stages", stage), ("status", status), ("destination", destination),
                        ("error_code", error_code), ("cursor", cursor)):  # fmt: skip
        if value:
            body[name] = value
    with _client() as api:
        page = api.search_messages(env, channel, body)
    if as_json:
        return emit(page)
    for e in page["items"]:
        out(_event_line(e))
    if page["next_cursor"]:
        out(f"next: --cursor {page['next_cursor']}")


@messages_app.command("show")
@guarded
def messages_show(channel: str, message_id: str, env: EnvOpt, as_json: JsonOpt = False) -> None:
    """One message: its timeline, destination states and which content is stored (no bytes)."""
    with _client() as api:
        detail = api.get_message(env, channel, message_id)
    if as_json:
        return emit(detail)
    for e in detail["events"]:
        out(_event_line(e))
    for d in detail["destinations"]:
        out(f"destination {d['destination']}: {d['state']} (attempt {d['attempt']})")
    for c in detail["content"]:
        out(f"content {c['stage']} {c.get('destination', '-')}: {c['size_bytes']} bytes, {c['storage']}")


@messages_app.command("reprocess")
@guarded
def messages_reprocess(
    channel: str,
    message_id: str,
    env: EnvOpt,
    allow_reorder: Annotated[bool, typer.Option("--allow-reorder", help="Accept breaking per-key order.")] = False,
    operation_id: Annotated[str | None, typer.Option(help="Reuse an id to retry the same reprocess.")] = None,
    as_json: JsonOpt = False,
) -> None:
    """Re-publish the original raw record under a new deterministic id. Prints the operation id: re-run with
    --operation-id to retry safely."""
    from tributary.sdk.ids import uuid7

    op_id = operation_id or uuid7()
    with _client() as api:
        result = api.reprocess(env, channel, message_id, {"operation_id": op_id, "allow_reorder": allow_reorder})
    if as_json:
        return emit(result)
    out(f"reprocessed {result['original_message_id']} as {result['new_message_id']} (operation {op_id})")


def _replay_line(r: dict[str, Any]) -> str:
    extra = f" republished {r.get('republished', 0)}" if r["status"] != "running" else ""
    code = f" {r['failure_code']}" if r.get("failure_code") else ""
    return f"{r['replay_id']}\t{r['status']}{code}{extra}\t{r['range_start']} .. {r['range_end']}"


@replay_app.command("start")
@guarded
def replay_start(
    channel: str,
    env: EnvOpt,
    start: Annotated[str, typer.Option(help="Range start, UTC (record timestamp).")],
    end: Annotated[str, typer.Option(help="Range end (exclusive).")],
    allow_reorder: Annotated[bool, typer.Option("--allow-reorder")] = False,
    max_records: Annotated[int, typer.Option(min=1, max=1_000_000)] = 100_000,
    deadline_s: Annotated[int, typer.Option(min=1, max=3600)] = 900,
    replay_id: Annotated[str | None, typer.Option(help="Reuse an id to retry or check the same replay.")] = None,
    as_json: JsonOpt = False,
) -> None:
    """Start a replay (runs in the background; poll with `replay show`)."""
    from tributary.sdk.ids import uuid7

    body = {
        "replay_id": replay_id or uuid7(), "start": start, "end": end, "allow_reorder": allow_reorder,
        "max_records": max_records, "deadline_s": deadline_s,
    }  # fmt: skip
    with _client() as api:
        record, started = api.start_replay(env, channel, body)
    if as_json:
        return emit(record)
    out(("started " if started else "exists ") + _replay_line(record))


@replay_app.command("show")
@guarded
def replay_show(channel: str, replay_id: str, env: EnvOpt, as_json: JsonOpt = False) -> None:
    """One replay and its counts."""
    with _client() as api:
        record = api.get_replay(env, channel, replay_id)
    if as_json:
        return emit(record)
    out(_replay_line(record))
    raise typer.Exit(EXIT_FAILED if record["status"] == "failed" else EXIT_OK)


@replay_app.command("list")
@guarded
def replay_list(channel: str, env: EnvOpt, as_json: JsonOpt = False) -> None:
    """Replays of this channel, newest first."""
    with _client() as api:
        items = api.list_replays(env, channel)
    if as_json:
        return emit(items)
    for r in items:
        out(_replay_line(r))


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
