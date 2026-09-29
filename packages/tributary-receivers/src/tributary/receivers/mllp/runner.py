"""The MLLP receiver's lifecycle around the live channel spec (invariant 3).

The spec comes from the compacted `_tb.<env>.channels` topic through `tributary.runtime.channels`, the
same reader the worker and loaders use. PostgreSQL, the API and ClickHouse are never involved, so the
receiver keeps serving when all three are down; and when the channels topic itself cannot be read, the
last good spec keeps serving (`ChannelCache` holds it; a record that fails to parse never replaces it).

States:

* **not ready** (start, and after a tombstone): no listener is open, so nothing can be sent and nothing
  is ACKed. A missing or unreadable spec, or one this receiver refuses, keeps it here and logs why.
  `open_feed` is retried with a bounded backoff; the process never starts a listener on a guess.
* **serving**: the listener is open. Each tick drains new spec records without blocking. A new spec is
  applied live when only live settings changed, or the process exits 0 when a restart-only setting
  changed (`mllp/reload.py` is the table). A spec that fails to build is logged and the last good one
  keeps serving.
* **undeployed** (tombstone): `stop_accepting` closes the listener; frames already committed still get
  their ACK; a frame arriving later on an open connection is NACKed with a fixed text and the connection
  closes. A later spec for the channel opens a fresh listener.
* **stopping** (SIGTERM/SIGINT or a restart-only change): stop accepting, wait bounded for every admitted
  frame's ACK to be written, close the connections, flush the publisher.

Nothing here logs a payload, a header value or a record key: only channel names, versions, field names,
ports and error class names.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import ChannelSpecError, safe_token
from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from ..errors import ConfigError
from ..metrics import Metrics
from .config import MllpSettings, mllp_settings_from_spec
from .kafka_publisher import RawPublisher
from .reload import live_fields_changed, restart_required_fields
from .server import MllpServer

#: Non-blocking polls of the channels consumer per tick. Each is a librdkafka `poll(0)`; the count only
#: bounds how long one tick can spend catching up on a backlog of spec records.
POLLS_PER_TICK = 64


class SpecFeed(Protocol):
    """What the runner needs from a spec source. `LiveSpecFeed` is the production one; tests supply a fake."""

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
        for _ in range(POLLS_PER_TICK):
            self._source.cache.poll_once(timeout_s=0.0)

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


def _print_event(record: dict[str, Any]) -> None:
    print(json.dumps(record), flush=True)


class MllpRunner:
    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[MllpSettings], RawPublisher],
        make_server: Callable[[MllpSettings, RawPublisher, Metrics], MllpServer],
        options: RunnerOptions,
        log: Logger,
        metrics: Metrics | None = None,
        metrics_file: str | None = None,
        emit: Callable[[dict[str, Any]], None] = _print_event,
    ) -> None:
        self._open_feed = open_feed
        self._make_publisher = make_publisher
        self._make_server = make_server
        self._opts = options
        self._log = log
        self.metrics = metrics if metrics is not None else Metrics()
        self._metrics_file = metrics_file
        self._emit = emit
        self._stop = asyncio.Event()
        self._server: MllpServer | None = None
        self._retired: list[MllpServer] = []
        self._publisher: RawPublisher | None = None
        self._rejected_version: int | None = None
        self._start_failure: tuple[MllpSettings, float | None] | None = None
        self._poll_failing = False
        self.restart_requested = False

    @property
    def server(self) -> MllpServer | None:
        return self._server

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> int:
        """Run until stopped. Returns the process exit code: 0 for a signal or a restart-only change."""
        feed: SpecFeed | None = None
        try:
            feed = await self._wait_for_feed()
            if feed is not None:
                await self._serve(feed)
        finally:
            await self._shutdown()
            if feed is not None:
                feed.close()
            if self._publisher is not None:
                await asyncio.to_thread(self._publisher.close)
        return 0

    # -- waiting for the first spec ------------------------------------------------------------

    async def _wait_for_feed(self) -> SpecFeed | None:
        delay = self._opts.open_retry_initial_s
        while not self._stop.is_set():
            try:
                return await asyncio.to_thread(self._open_feed)
            except ChannelSpecError:
                # Not deployed yet, or the channels topic cannot be read: stay not ready, ACK nothing.
                self._log.warning("spec_unavailable", ready=False, retry_in_s=delay)
            except Exception as exc:  # a client-construction failure must not take the process down either
                self._log.error("spec_source_failed", ready=False, error=safe_token(type(exc).__name__))
            await self._sleep(delay)
            delay = min(delay * 2, self._opts.open_retry_max_s)
        return None

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)

    # -- the serving loop ----------------------------------------------------------------------

    async def _serve(self, feed: SpecFeed) -> None:
        next_metrics = time.monotonic() + self._opts.metrics_every_s
        while not self._stop.is_set():
            self._poll(feed)
            await self._reconcile(feed.get())
            self._retired = [s for s in self._retired if s.open_connections]
            if self._metrics_file and time.monotonic() >= next_metrics:
                next_metrics = time.monotonic() + self._opts.metrics_every_s
                self._write_metrics()
            await self._sleep(self._opts.poll_interval_s)

    def _poll(self, feed: SpecFeed) -> None:
        """A failing poll (the channels topic or its broker is unreachable) is not fatal: the last good
        spec keeps serving. Logged once per streak, and once when it recovers."""
        try:
            feed.poll()
        except Exception as exc:
            if not self._poll_failing:
                self._poll_failing = True
                self._log.warning("spec_poll_failed", error=safe_token(type(exc).__name__), serving_last_good=True)
        else:
            if self._poll_failing:
                self._poll_failing = False
                self._log.info("spec_poll_recovered")

    def _write_metrics(self) -> None:
        if self._metrics_file:
            with open(self._metrics_file, "w") as f:
                json.dump(self.metrics.snapshot(), f)

    async def _reconcile(self, spec: ChannelSpec | None) -> None:
        server = self._server
        if spec is None:
            if server is not None:
                server.stop_accepting("channel-undeployed")  # act first: the log line means it is done
                self._log.warning("channel_undeployed", ready=False)
                self._retired.append(server)
                self._server = None
            return
        cfg = self._settings_for(spec, serving=server is not None)
        if cfg is None:
            return
        if server is None:
            await self._start(cfg)
        elif cfg != server.cfg:
            await self._update(server, cfg)

    def _settings_for(self, spec: ChannelSpec, *, serving: bool) -> MllpSettings | None:
        try:
            cfg = mllp_settings_from_spec(spec, env=self._opts.env, instance=self._opts.instance)
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

    async def _update(self, server: MllpServer, cfg: MllpSettings) -> None:
        old = server.cfg
        restart = restart_required_fields(old, cfg)
        if restart:
            self._log.warning(
                "spec_restart_required",
                fields=",".join(restart),
                channel_version=cfg.channel_version,
                previous_version=old.channel_version,
                action="exit_0_for_supervisor_restart",
            )
            self.restart_requested = True
            self._stop.set()
            return
        await server.apply_settings(cfg)
        self._log.info(
            "spec_applied",
            channel_version=cfg.channel_version,
            previous_version=old.channel_version,
            changed=",".join(live_fields_changed(old, cfg)),
        )

    async def _start(self, cfg: MllpSettings) -> None:
        now = time.monotonic()
        if self._start_failure is not None:
            failed_cfg, retry_at = self._start_failure
            if failed_cfg == cfg and (retry_at is None or now < retry_at):
                return
        try:
            if self._publisher is None:
                self._publisher = self._make_publisher(cfg)
            server = self._make_server(cfg, self._publisher, self.metrics)
            listener = await server.start()
        except OSError as exc:
            self._start_failure = (cfg, now + self._opts.bind_retry_s)
            self._log.error("listener_bind_failed", ready=False, errno=exc.errno or 0)
            return
        except Exception as exc:
            self._start_failure = (cfg, None)
            self._log.error("listener_refused", ready=False, error=safe_token(type(exc).__name__))
            return
        self._start_failure = None
        self._server = server
        self._emit(
            {
                "event": "listening",
                "channel": cfg.channel,
                "channel_version": cfg.channel_version,
                "topic": self._opts.topic,
                "port": listener.sockets[0].getsockname()[1],
                "tls": cfg.tls is not None,
            }
        )

    async def _shutdown(self) -> None:
        servers = ([self._server] if self._server is not None else []) + self._retired
        for server in servers:
            server.stop_accepting("shutting-down")
        if servers:
            await asyncio.gather(*(s.drain(s.cfg.ack_timeout_s + s.cfg.write_timeout_s) for s in servers))
        self._server = None
        self._retired = []
        self._log.info("receiver_stopped", restart_requested=self.restart_requested)
