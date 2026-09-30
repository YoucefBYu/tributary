"""The receivers' shared lifecycle around the live channel spec (invariant 3).

The spec comes from the compacted `_tb.<env>.channels` topic through `tributary.runtime.channels`, the
same reader the worker and loaders use. PostgreSQL, the API and ClickHouse are never involved, so a
receiver keeps serving when all three are down; and when the channels topic itself cannot be read
(`ChannelsUnavailableError`), the last good spec keeps serving (`ChannelCache` holds it; a record that
fails to parse never replaces it).

This module owns everything that is the same for every receiver type. A concrete receiver
(`mllp/runner.py`, `http/runner.py`, `sftp/runner.py`, `kafka_source/runner.py`) subclasses
`ReceiverLifecycle` and supplies only what differs: how a spec becomes settings, how the component is
started, applied live, paused and stopped, and its own live-versus-restart table (`<type>/reload.py`).

States:

* **not ready** (start, and after a tombstone): the component is not running, so nothing is accepted and
  nothing is acknowledged. A missing or unreadable spec, or one this receiver refuses, keeps it here and
  logs why. `open_feed` is retried with a bounded backoff; the process never starts on a guess.
* **serving**: the component runs. Each tick drains new spec records without blocking. A new spec is
  applied live when only live settings changed, or the process exits 0 when a restart-only setting
  changed. A spec that fails to build, or to apply, is logged and the last good one keeps serving.
* **undeployed** (tombstone): `pause` stops taking new work; work already committed still gets its
  acknowledgement. A later spec for the channel starts the component afresh.
* **stopping** (SIGTERM/SIGINT or a restart-only change): `stop` waits, bounded, for admitted work to be
  acknowledged, then closes what is open; the publisher (or consumer) is flushed last.

Nothing here logs a payload, a header value or a record key: only channel names, versions, field names,
ports and error class names.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import ChannelSpecError, ChannelsUnavailableError, safe_token
from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from .errors import ConfigError, SecretUnresolvedError
from .metrics import Metrics

#: Non-blocking polls of the channels consumer per tick. Each is a librdkafka `poll(0)`; the count only
#: bounds how long one tick can spend catching up on a backlog of spec records.
POLLS_PER_TICK = 64


class SpecFeed(Protocol):
    """What a lifecycle needs from a spec source. `LiveSpecFeed` is the production one; tests supply a fake."""

    def poll(self) -> None:
        """Apply any spec records that have already arrived. Never blocks."""

    def get(self) -> ChannelSpec | None:
        """The last good spec for this receiver's channel, or `None` if it is absent or was tombstoned."""

    def close(self) -> None: ...


class LiveSpecFeed:
    """`LiveSpecSource` (from `open_spec_source`) seen as a `SpecFeed`."""

    def __init__(self, source: LiveSpecSource, channel: str) -> None:
        self._source = source
        self._channel = channel

    def poll(self) -> None:
        """Drain the backlog. A `ChannelsUnavailableError` (the topic cannot be shown to be readable) does not
        stop the drain: a record that already arrived is applied; the error is raised once, at the end."""
        unavailable: ChannelsUnavailableError | None = None
        for _ in range(POLLS_PER_TICK):
            try:
                self._source.cache.poll_once(timeout_s=0.0)
            except ChannelsUnavailableError as exc:
                unavailable = exc
        if unavailable is not None:
            raise unavailable

    def get(self) -> ChannelSpec | None:
        return self._source.cache.get(self._channel)

    def close(self) -> None:
        self._source.close()


@dataclass(frozen=True, slots=True)
class RunnerOptions:
    env: str
    instance: str
    topic: str
    poll_interval_s: float = 0.1
    open_retry_initial_s: float = 1.0
    open_retry_max_s: float = 10.0
    bind_retry_s: float = 5.0
    metrics_every_s: float = 5.0


def print_event(record: dict[str, Any]) -> None:
    print(json.dumps(record), flush=True)


class ReceiverLifecycle[S](ABC):
    """Spec feed, not-ready gating, live-vs-restart, tombstone and redeploy, last-good during an outage.

    `S` is the receiver's frozen settings dataclass; equality on it is how "nothing changed" and "the same
    settings that already failed to start" are decided, so it must be a value type.
    """

    #: Log event names a receiver may rename (MLLP says "listener"; a poller has no listener).
    EVENT_START_RETRY = "start_failed"
    EVENT_START_REFUSED = "start_refused"

    def __init__(
        self,
        *,
        options: RunnerOptions,
        log: Logger,
        open_feed: Callable[[], SpecFeed],
        metrics: Metrics | None = None,
        metrics_file: str | None = None,
        emit: Callable[[dict[str, Any]], None] = print_event,
    ) -> None:
        self._open_feed = open_feed
        self._opts = options
        self._log = log
        self.metrics = metrics if metrics is not None else Metrics()
        self._metrics_file = metrics_file
        self._emit = emit
        self._stop = asyncio.Event()
        self._rejected_version: int | None = None
        self._start_failure: tuple[S, float | None] | None = None
        self._poll_failing = False
        self._metrics_failing = False
        self.restart_requested = False
        self.exit_code = 0

    # -- what a concrete receiver supplies ------------------------------------------------------

    @abstractmethod
    def build_settings(self, spec: ChannelSpec) -> S:
        """Spec to settings, or raise. Any exception is a refusal: the last good spec keeps serving."""

    @abstractmethod
    def running_settings(self) -> S | None:
        """The settings of the running component, `None` while not running."""

    @abstractmethod
    def version_of(self, cfg: S) -> int: ...

    @abstractmethod
    def restart_fields(self, old: S, new: S) -> list[str]:
        """Restart-only fields that differ (the receiver's reload table). Names only."""

    @abstractmethod
    def live_fields(self, old: S, new: S) -> list[str]:
        """Live fields that differ. Names only."""

    @abstractmethod
    async def start_component(self, cfg: S) -> dict[str, Any] | None:
        """Start serving `cfg`. `OSError` means "retry later" (a bind that failed); any other exception means
        "refused until the settings change". Returns the event to emit once started, if any."""

    @abstractmethod
    async def apply_component(self, cfg: S) -> None:
        """Apply live changes. Must validate before it mutates: on an exception nothing was applied."""

    @abstractmethod
    def pause_component(self) -> None:
        """A tombstone: stop taking new work now (synchronous: the log line that follows means it is done).
        Work already committed keeps its acknowledgement."""

    @abstractmethod
    async def stop_component(self) -> None:
        """Bounded drain, then close what is open. Runs on shutdown, including after a restart-only change."""

    async def close_resources(self) -> None:  # noqa: B027 - optional hook
        """After the feed is closed: flush and close the publisher or consumer. Default: nothing."""

    def on_tick(self) -> None:  # noqa: B027 - optional hook
        """Called once per serving tick, after reconcile. Default: nothing."""

    # -- public ---------------------------------------------------------------------------------

    def request_stop(self) -> None:
        self._stop.set()

    def report_failure(self, exc: BaseException) -> None:
        """A component's task died with an error nothing handles (a poller or consumer loop). Logged by class name
        only, then the process drains and exits 1 so the supervisor restarts it: a receiver that keeps running
        with its loop dead would look healthy and take nothing in."""
        self._log.error("component_failed", ready=False, error=safe_token(type(exc).__name__))
        self.exit_code = 1
        self._stop.set()

    async def run(self) -> int:
        """Run until stopped. Returns the process exit code: 0 for a signal or a restart-only change."""
        feed: SpecFeed | None = None
        try:
            feed = await self._wait_for_feed()
            if feed is not None:
                await self._serve(feed)
        finally:
            await self.stop_component()
            if feed is not None:
                feed.close()
            await self.close_resources()
            self._log.info("receiver_stopped", restart_requested=self.restart_requested, exit_code=self.exit_code)
        return self.exit_code

    # -- waiting for the first spec -------------------------------------------------------------

    async def _wait_for_feed(self) -> SpecFeed | None:
        delay = self._opts.open_retry_initial_s
        while not self._stop.is_set():
            try:
                return await asyncio.to_thread(self._open_feed)
            except ChannelSpecError:
                # Not deployed yet, or the channels topic cannot be read: stay not ready, act on nothing.
                self._log.warning("spec_unavailable", ready=False, retry_in_s=delay)
            except Exception as exc:  # a client-construction failure must not take the process down either
                self._log.error("spec_source_failed", ready=False, error=safe_token(type(exc).__name__))
            await self._sleep(delay)
            delay = min(delay * 2, self._opts.open_retry_max_s)
        return None

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)

    # -- the serving loop -----------------------------------------------------------------------

    async def _serve(self, feed: SpecFeed) -> None:
        next_metrics = time.monotonic() + self._opts.metrics_every_s
        while not self._stop.is_set():
            self._poll(feed)
            await self._reconcile(feed.get())
            self.on_tick()
            if self._metrics_file and time.monotonic() >= next_metrics:
                next_metrics = time.monotonic() + self._opts.metrics_every_s
                self._write_metrics()
            await self._sleep(self._opts.poll_interval_s)

    def _poll(self, feed: SpecFeed) -> None:
        """A failing poll (the channels topic or its broker is unreachable) is not fatal: the last good spec
        keeps serving. Logged once per streak, and once when it recovers."""
        try:
            feed.poll()
        except Exception as exc:
            if not self._poll_failing:
                self._poll_failing = True
                fields: dict[str, object] = {"error": safe_token(type(exc).__name__), "serving_last_good": True}
                if isinstance(exc, ChannelsUnavailableError):
                    fields["error_code"] = safe_token(exc.error_code)  # a librdkafka name, never broker text
                self._log.warning("spec_poll_failed", **fields)
        else:
            if self._poll_failing:
                self._poll_failing = False
                self._log.info("spec_poll_recovered")

    def _write_metrics(self) -> None:
        if self._metrics_file:
            try:
                with open(self._metrics_file, "w") as f:
                    json.dump(self.metrics.snapshot(), f)
            except OSError:
                # A metrics file that cannot be written must never stop the receiver.
                if not self._metrics_failing:
                    self._metrics_failing = True
                    self._log.warning("metrics_file_unwritable")
            else:
                self._metrics_failing = False

    async def _reconcile(self, spec: ChannelSpec | None) -> None:
        running = self.running_settings()
        if spec is None:
            if running is not None:
                self.pause_component()  # act first: the log line means it is done
                self._log.warning("channel_undeployed", ready=False)
            return
        cfg = self._settings_for(spec, serving=running is not None)
        if cfg is None:
            return
        if running is None:
            await self._start(cfg)
        elif cfg != running:
            await self._update(running, cfg)

    def _settings_for(self, spec: ChannelSpec, *, serving: bool) -> S | None:
        try:
            cfg = self.build_settings(spec)
        except Exception as exc:
            # Broad on purpose: a malformed or unsupported spec must never crash a serving data plane
            # (channel-spec README 6 rule 1: keep the last good one). ConfigError and ValueError carry
            # field names and enum values only; any other class is logged by name.
            if self._rejected_version != spec.version:
                self._rejected_version = spec.version
                fields: dict[str, object] = {"channel_version": spec.version, "ready": serving}
                if isinstance(exc, ValueError | ConfigError):
                    fields["reason"] = str(exc)[:200].replace("\n", " ")
                else:
                    fields["error"] = safe_token(type(exc).__name__)
                self._log.error("spec_update_rejected" if serving else "spec_rejected", **fields)
            return None
        self._rejected_version = None
        return cfg

    async def _update(self, old: S, cfg: S) -> None:
        restart = self.restart_fields(old, cfg)
        if restart:
            self._log.warning(
                "spec_restart_required",
                fields=",".join(restart),
                channel_version=self.version_of(cfg),
                previous_version=self.version_of(old),
                action="exit_0_for_supervisor_restart",
            )
            self.restart_requested = True
            self._stop.set()
            return
        try:
            await self.apply_component(cfg)
        except Exception as exc:
            # Nothing was applied (`apply_component` validates before it mutates): keep the last good spec.
            self._log.error(
                "spec_update_rejected", channel_version=self.version_of(cfg), error=safe_token(type(exc).__name__)
            )
            return
        self._log.info(
            "spec_applied",
            channel_version=self.version_of(cfg),
            previous_version=self.version_of(old),
            changed=",".join(self.live_fields(old, cfg)),
        )

    async def _start(self, cfg: S) -> None:
        now = time.monotonic()
        if self._start_failure is not None:
            failed_cfg, retry_at = self._start_failure
            if failed_cfg == cfg and (retry_at is None or now < retry_at):
                return
        try:
            event = await self.start_component(cfg)
        except SecretUnresolvedError as exc:
            # Not a bind failure: a secret the spec names is not mounted. Retrying in place would hide it;
            # exit 2 (a refused configuration) lets the supervisor restart with its own back-off, visibly.
            self._log.error("startup_refused", ready=False, error=type(exc).__name__, code=exc.code)
            self.exit_code = 2
            self._stop.set()
            return
        except OSError as exc:
            self._start_failure = (cfg, now + self._opts.bind_retry_s)
            self._log.error(self.EVENT_START_RETRY, ready=False, errno=exc.errno or 0)
            return
        except Exception as exc:
            self._start_failure = (cfg, None)
            self._log.error(self.EVENT_START_REFUSED, ready=False, error=safe_token(type(exc).__name__))
            return
        self._start_failure = None
        if event is not None:
            self._emit(event)
