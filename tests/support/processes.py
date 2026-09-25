"""Spawning the real receiver, loader and worker as OS subprocesses for e2e and chaos tests.

Real processes, not in-process calls: a chaos test needs to `kill()` a broker without the worker or
receiver cooperating, and an e2e test should exercise the actual `python -m tributary.receivers.mllp`
/ `python -m tributary.loaders.http` / `python -m tributary.runtime.worker` entry points, the same ones
`deploy/compose/docker-compose.yml` runs — the same reasoning
`packages/tributary-runtime/tests/test_integration_kafka.py::spawn_worker` uses for the worker alone.

Each component prints one JSON line to stdout (or stderr, merged here) when it is actually ready to
receive traffic: the receiver prints `{"event": "listening", ...}` itself
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
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


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
        )
        self._queue: queue.Queue[bytes | None] = queue.Queue()
        self._eof = False
        self.lines: list[dict[str, Any]] = []
        assert self.process.stdout is not None
        self._reader = threading.Thread(target=_pump, args=(self.process.stdout, self._queue), daemon=True)
        self._reader.start()

    def _kill_quietly(self) -> None:
        """Best-effort cleanup before `wait_for_event` raises: a process that is still alive when the
        wait gives up (slow to start, or wedged before ever emitting the event) would otherwise leak
        for the rest of the test session -- nothing else in this class's lifecycle reaps it, because a
        test that got an exception from `wait_for_event` typically never reaches its own `finally`
        block's `terminate_and_wait()` call, since that call is usually written to depend on the very
        variable this constructor's return value would have been assigned to."""
        with contextlib.suppress(Exception):  # best effort; the process may already be gone
            self.process.kill()
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
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            self.lines.append(record)
            collected.append(record)
        return collected

    def kill_and_wait(self, timeout_s: float = 30.0) -> int:
        self.process.kill()
        return self.process.wait(timeout=timeout_s)

    def terminate_and_wait(self, timeout_s: float = 15.0) -> int:
        if self.process.poll() is None:
            self.process.terminate()
        try:
            return self.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return self.kill_and_wait(timeout_s)


def spawn_mllp_receiver(
    *, channel_spec: Path, env: str, bootstrap: str, instance: str = "mllp-e2e", timeout_s: float = 30.0
) -> ManagedProcess:
    proc = ManagedProcess(
        [
            sys.executable,
            "-m",
            "tributary.receivers.mllp",
            "--channel-spec",
            str(channel_spec),
            "--env",
            env,
            "--bootstrap",
            bootstrap,
            "--instance",
            instance,
        ],
        env=dict(os.environ),
    )
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
            "--bootstrap",
            bootstrap,
            "--instance-id",
            instance_id,
        ],
        env=dict(os.environ),
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
        "--bootstrap",
        bootstrap,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=dict(os.environ))
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
    added for a genuine-redelivery duplicate-delivery test in that package's own style. `--bootstrap`
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
        "--bootstrap",
        bootstrap,
        "--instance-id",
        instance_id,
    ]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    proc = ManagedProcess(args, env=dict(os.environ))
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
    *, env: str, channel: str, bootstrap: str, instance_id: str, timeout_s: float = 30.0
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
        env=dict(os.environ),
    )
    proc.wait_for_event("worker_started", timeout_s=timeout_s)
    return proc
