"""Spawning the real receiver, loader and worker as OS subprocesses for e2e and chaos tests.

Real processes, not in-process calls: a chaos test needs to `kill()` a broker without the worker or
receiver cooperating, and an e2e test should exercise the actual `python -m tributary.receivers.mllp`
/ `python -m tributary.loaders.http` / `python -m tributary.runtime.worker` entry points, the same ones
`deploy/compose/docker-compose.yml` runs — the same reasoning
`packages/tributary-runtime/tests/test_integration_kafka.py::spawn_worker` uses for the worker alone.

Each component prints one JSON line when it is actually ready to receive traffic (receivers log to stderr since W3-17,
loaders and workers to stdout or stderr; `ManagedProcess` merges both): the receiver logs `{"event": "listening", ...}`
(`tributary.receivers.mllp.server.main`), the loader and worker log `{"event": "loader_started", ...}`
/ `{"event": "worker_started", ...}` through `tributary.runtime.logs.JsonLogger`. `wait_for_event`
follows those rather than a fixed sleep, and fails fast if the process exits first instead of hanging
until a timeout that would look like "the fault didn't fire" in a chaos test.
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from tests.support.engine_kafka import engine_kafka_env, receiver_env


class ProcessDiedError(AssertionError):
    pass


def _pump(stream: Any, sink: queue.Queue[bytes | None]) -> None:
    for line in iter(stream.readline, b""):
        sink.put(line)
    sink.put(None)


class ManagedProcess:
    """One subprocess plus a background reader thread draining its (merged) stdout/stderr.

    The reader keeps running for the process's whole life so log lines after the readiness wait are
    never left to fill an OS pipe buffer and deadlock the child — tests that want them can inspect
    `.lines` after the fact.
    """

    def __init__(self, args: list[str], *, env: dict[str, str] | None = None) -> None:
        self.args = args
        self.process = subprocess.Popen(  # noqa: S603 - args are built by this module, not from user input
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            # Its own session and process group: a signal aimed at this process (or at a grandchild it
            # started) can only ever reach its own group, never pytest's.
            start_new_session=True,
        )
        self._queue: queue.Queue[bytes | None] = queue.Queue()
        self._eof = False
        self.lines: list[dict[str, Any]] = []
        #: Every raw output line, JSON or not (a traceback, a stray print), for log-scanning assertions
        #: such as the e2e "no PHI in any captured log line" check. Filled as lines are drained.
        self.raw: list[bytes] = []
        assert self.process.stdout is not None
        self._reader = threading.Thread(target=_pump, args=(self.process.stdout, self._queue), daemon=True)
        self._reader.start()

    def _signal_group(self, sig: int) -> None:
        """Signal this child's own process group, and never pytest's.

        The group id is the child's pid (`start_new_session`); the guard refuses anything else, so a
        wrong value (0, or pytest's own group) can not turn a teardown into `killpg` on the test runner.
        A process that already exited is left alone: its pid could since have been reused.
        """
        if self.process.poll() is not None:
            return
        pgid = self.process.pid
        if pgid <= 1 or pgid == os.getpid() or pgid == os.getpgrp():
            raise RuntimeError("refusing to signal a process group that may be the test runner's")
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, sig)

    def _kill_quietly(self) -> None:
        """Best-effort cleanup before `wait_for_event` raises: a process that is still alive when the
        wait gives up (slow to start, or wedged before ever emitting the event) would otherwise leak
        for the rest of the test session -- nothing else in this class's lifecycle reaps it, because a
        test that got an exception from `wait_for_event` typically never reaches its own `finally`
        block's `terminate_and_wait()` call, since that call is usually written to depend on the very
        variable this constructor's return value would have been assigned to."""
        with contextlib.suppress(Exception):  # best effort; the process may already be gone
            self._signal_group(signal.SIGKILL)
            self.process.wait(timeout=5.0)

    def wait_for_event(self, event: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                raw = self._queue.get(timeout=min(0.5, remaining))
            except queue.Empty:
                if self.process.poll() is not None:
                    raise ProcessDiedError(
                        f"{self.args!r} exited (code {self.process.returncode}) before emitting {event!r}; "
                        f"output so far: {self.lines!r}"
                    ) from None
                continue
            if raw is None:
                self._eof = True
                self._kill_quietly()
                raise ProcessDiedError(f"{self.args!r} closed its output before emitting {event!r}: {self.lines!r}")
            self.raw.append(raw)
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            self.lines.append(record)
            if record.get("event") == event:
                return record
        self._kill_quietly()
        raise ProcessDiedError(f"{self.args!r} did not emit {event!r} within {timeout_s}s: {self.lines!r}")

    def collect_pending(self) -> list[dict[str, Any]]:
        """Non-blocking: drain whatever has already been printed into `.lines` and return the newly
        collected records, without waiting for any particular event.

        For a check that needs the whole log so far rather than one named event -- "no halt event was
        ever printed", say -- because `wait_for_event` only appends a line to `.lines` on its way to a
        match, so anything printed after the last `wait_for_event` call (or printed but never matched,
        such as a chaos scenario's process that runs on after the event it was waiting for) would
        otherwise never reach `.lines` at all: the reader thread keeps queuing lines for the process's
        whole life (this class's docstring), but nothing before this method drains that queue without
        also demanding a specific event to stop at.
        """
        collected: list[dict[str, Any]] = []
        while True:
            try:
                raw = self._queue.get_nowait()
            except queue.Empty:
                break
            if raw is None:
                self._eof = True
                break
            self.raw.append(raw)
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            self.lines.append(record)
            collected.append(record)
        return collected

    def kill_and_wait(self, timeout_s: float = 30.0) -> int:
        self._signal_group(signal.SIGKILL)
        return self.process.wait(timeout=timeout_s)

    def terminate_and_wait(self, timeout_s: float = 15.0) -> int:
        self._signal_group(signal.SIGTERM)
        try:
            return self.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return self.kill_and_wait(timeout_s)


def spawn_mllp_receiver(
    *, channel_spec: Path, env: str, bootstrap: str, instance: str = "mllp-e2e", timeout_s: float = 30.0
) -> ManagedProcess:
    """The real receiver, reading its spec from `_tb.<env>.channels` (invariant 3): the document in
    `channel_spec` is published there first. See `tests.support.mllp_receiver`."""
    from tests.support.mllp_receiver import spawn_mllp_receiver_with_spec_file

    return spawn_mllp_receiver_with_spec_file(
        channel_spec=channel_spec, env=env, bootstrap=bootstrap, instance=instance, timeout_s=timeout_s
    )


def spawn_mllp_receiver_from_topic(
    *,
    channel: str,
    env: str,
    bootstrap: str,
    instance: str = "mllp-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """The receiver in its post-migration form: `--channel <name>`, the spec read from the compacted
    `_tb.<env>.channels` topic (invariant 3). For a spec already on the topic; `spawn_mllp_receiver` above
    publishes a spec file first and then does the same. The spec must be published before this is called."""
    args = [
        sys.executable,
        "-m",
        "tributary.receivers.mllp",
        "--channel",
        channel,
        "--env",
        env,
        "--instance",
        instance,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("listening", timeout_s=timeout_s)
    return proc


def spawn_http_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    timeout_s: float = 30.0,
) -> ManagedProcess:
    proc = ManagedProcess(
        [
            sys.executable,
            "-m",
            "tributary.loaders.http",
            "--channel",
            channel,
            "--destination",
            destination,
            "--env",
            env,
            "--instance-id",
            instance_id,
        ],
        env=receiver_env(engine_kafka_env(bootstrap)),
    )
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_postgresql_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """Same shape as `spawn_http_loader`, for `tributary.loaders.postgresql`'s real entry point
    (`packages/tributary-loaders/src/tributary/loaders/postgresql/main.py`, connector-engineer, W2-6) --
    added for a genuine-redelivery duplicate-delivery test in that package's own style
    (`tests.integration.test_http_loader_duplicate_delivery_uncommitted_redelivery` is the reference
    implementation for HTTP; this loader's connection pool needs `--secrets-dir` pointed at a
    `DirSecretResolver`-shaped directory, `<secrets_dir>/<secretRef>/<key>`, `tributary.loaders.secrets`)
    for the destination's `secretRef` -- there is no default that works against a testcontainers
    PostgreSQL instance."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.postgresql",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_kafka_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """Same shape as `spawn_postgresql_loader`, for `tributary.loaders.kafka`'s real entry point
    (`packages/tributary-loaders/src/tributary/loaders/kafka/main.py`, connector-engineer, W2-6) --
    added for a genuine-redelivery duplicate-delivery test in that package's own style. `bootstrap`
    is the ENGINE's cluster (the one this process reads `out.<dest>`/`_tb.<env>.channels` from); the
    `kafka` destination's OWN bootstrap servers and credentials come from the deployed spec's
    `with.connection` (ADR-007 D11), never from this helper's arguments."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.kafka",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_clickhouse_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """Same shape as `spawn_postgresql_loader`, for `tributary.loaders.clickhouse`'s real entry point
    (`packages/tributary-loaders/src/tributary/loaders/clickhouse/main.py`, connector-engineer, `a31f91a`)
    -- added for a genuine-redelivery duplicate-delivery test in that package's own style
    (`tests.integration.test_postgresql_loader_duplicate_delivery_uncommitted_redelivery` is the sibling
    reference for a real sink this suite controls). `--secrets-dir` needs a `DirSecretResolver`-shaped
    directory, `<secrets_dir>/<secretRef>/<key>`, for the destination's `auth.secretRef` -- there is no
    default that works against a testcontainers ClickHouse instance."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.clickhouse",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_fhir_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """`tributary.loaders.fhir`'s real entry point (connector-engineer, W2-6). Same shape as
    `spawn_clickhouse_loader`; `secrets_dir` is only needed if the destination declares `auth`."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.fhir",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_sftp_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """`tributary.loaders.sftp`'s real entry point (connector-engineer, W2-6). Same shape as
    `spawn_clickhouse_loader`; `secrets_dir` holds `<secretRef>/username` plus `password` or `private_key`."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.sftp",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_s3_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """`tributary.loaders.s3`'s real entry point (connector-engineer, W2-6). Same shape as
    `spawn_clickhouse_loader`; `secrets_dir` holds `<secretRef>/access_key_id` and `secret_access_key`."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.s3",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


def spawn_mllp_loader(
    *,
    channel: str,
    destination: str,
    env: str,
    bootstrap: str,
    instance_id: str = "loader-e2e",
    secrets_dir: Path | str | None = None,
    timeout_s: float = 30.0,
) -> ManagedProcess:
    """Same shape as `spawn_clickhouse_loader`, for `tributary.loaders.mllp`'s real entry point
    (`packages/tributary-loaders/src/tributary/loaders/mllp/main.py`, connector-engineer, W2-6) -- added
    for a genuine-redelivery duplicate-delivery test in that package's own style
    (`tests.integration.test_postgresql_loader_duplicate_delivery_uncommitted_redelivery` is the sibling
    reference for a real sink this suite controls). `--secrets-dir` is only needed when the destination's
    `with.tls.secretRef` names a trust bundle; a loopback destination with no `with.tls` never reads it."""
    args = [
        sys.executable,
        "-m",
        "tributary.loaders.mllp",
        "--channel",
        channel,
        "--destination",
        destination,
        "--env",
        env,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=receiver_env(engine_kafka_env(bootstrap)))
    proc.wait_for_event("loader_started", timeout_s=timeout_s)
    return proc


WORKER_PROGRAM = """
import sys
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.worker import PipelineWorker, WorkerSettings

worker = PipelineWorker(
    settings=WorkerSettings(
        env=sys.argv[1],
        channel=sys.argv[2],
        kafka=KafkaSettings(bootstrap_servers=sys.argv[3]),
        instance_id=sys.argv[4],
        poll_timeout_s=0.1,
    ),
    defaults=load_topic_defaults(),
)
worker.build()
worker.run()
"""


def spawn_pipeline_worker(
    *,
    env: str,
    channel: str,
    bootstrap: str,
    instance_id: str,
    timeout_s: float = 30.0,
    extra_env: dict[str, str] | None = None,
) -> ManagedProcess:
    """A real worker process, resolving its step exactly as a deployed one would: no
    `steps_override`, so `tributary.runtime.steps.load_step` runs for real against whatever
    `pipeline.steps` the published channel spec names — `tests/support/channel_spec.py`'s document
    leaves that as the contract example's own `uses: tributary.core.passthrough`, a
    `tributary.steps` entry point `tributary-runtime` registers, so nothing beyond the installed
    venv is needed for it to resolve. Deliberate: an earlier version of this helper used
    `steps_override` plus a `PYTHONPATH` stand-in step, which would have stayed green through the
    entire step-resolution bug this suite caught in the walking-skeleton example
    (architect/platform-engineer, 2026-09-23) — see `tests/e2e/test_mllp_to_http_e2e.py`'s module
    docstring.
    """
    proc = ManagedProcess(
        [sys.executable, "-c", WORKER_PROGRAM, env, channel, bootstrap, instance_id],
        env={**os.environ, "TB_OPS_PORT": "0", **(extra_env or {})},
    )
    proc.wait_for_event("worker_started", timeout_s=timeout_s)
    return proc
