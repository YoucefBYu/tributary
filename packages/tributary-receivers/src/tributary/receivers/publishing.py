"""The lifecycle of a receiver that publishes to `<env>.<channel>.raw` through one `RawPublisher`.

MLLP and HTTP run a listening server; the SFTP poller and the Kafka source run a loop. All four are a
"component" to this module: something that starts on settings, applies live changes, stops taking new work,
and drains what it already took. On top of `ReceiverLifecycle` this adds one raw publisher shared by every
component the process builds over its life, the components a tombstone leaves draining (`_retired`), and the
rule that a producer is never closed under work whose commit is still in flight.

* **tombstone**: the running component stops taking new work (a listener closes and later requests are
  refused with a fixed text; a poller or consumer stops fetching) and moves to `_retired`; whatever it had
  already handed to the publisher still gets its answer. A later spec starts a fresh component on the same
  publisher, after the retired one has drained.
* **a redeploy that changes the publisher's ordering rule** rebuilds the producer, because the key rule is
  fixed at construction. The retired components drain first, then the old producer flushes and closes.
* **stop**: every component stops taking work, drains bounded, closes what it holds; the publisher closes last.
"""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from .lifecycle import ReceiverLifecycle
from .mllp.kafka_publisher import RawPublisher


class PublishingComponent[S](Protocol):
    """What the lifecycle needs from `MllpServer`, `HttpServer`, and the SFTP and Kafka-source services."""

    @property
    def cfg(self) -> S: ...

    @property
    def open_connections(self) -> int:
        """Work still open: connections for a listener, 1 while a poll cycle or a fetch loop runs, else 0."""
        ...

    async def start(self) -> Any:
        """Start. The result is handed to `started_event` (a listener returns its `asyncio` server)."""
        ...

    async def apply_settings(self, new: S) -> None: ...

    def stop_accepting(self, reason: str) -> None: ...

    async def drain(self, timeout_s: float) -> bool: ...


@runtime_checkable
class FailureReporting(Protocol):
    """A component that runs a task of its own (poller, consumer loop) and can say it died."""

    def set_failure_handler(self, handler: Callable[[BaseException], None]) -> None: ...


class PublishingLifecycle[S](ReceiverLifecycle[S]):
    #: The tombstone reason a later request on an open connection is refused with (a fixed string).
    UNDEPLOYED_REASON = "channel-undeployed"
    #: A poller or consumer must not start while its retired predecessor still runs (two pollers of one
    #: instance would both recover the same claim; two consumers of one group would fight over offsets). A
    #: listener may: the old one only answers what it already took, on sockets nothing new reaches.
    DRAIN_RETIRED_BEFORE_START = False

    _server: PublishingComponent[S] | None
    _retired: list[PublishingComponent[S]]
    _publisher: RawPublisher | None
    _publisher_ordering: str

    def _init_listener_state(self) -> None:
        self._server = None
        self._retired = []
        self._publisher = None
        self._publisher_ordering = "none"

    @abstractmethod
    def ordering_of(self, cfg: S) -> str:
        """The publisher's ordering rule for `cfg`: `none` or `per-key`. Fixed for a producer's life."""

    @abstractmethod
    def drain_budget_s(self, cfg: S) -> float:
        """How long a stop or a rebuild waits for a server's admitted work to be answered."""

    @abstractmethod
    def build_publisher(self, cfg: S) -> RawPublisher: ...

    @abstractmethod
    def build_server(self, cfg: S, publisher: RawPublisher) -> PublishingComponent[S]: ...

    @abstractmethod
    def started_event(self, cfg: S, server: PublishingComponent[S], started: Any) -> dict[str, Any]:
        """The event emitted once the listener is up (names, versions, the bound port; nothing else)."""

    # -- ReceiverLifecycle ------------------------------------------------------------------------

    def running_settings(self) -> S | None:
        return self._server.cfg if self._server is not None else None

    def on_tick(self) -> None:
        self._retired = [s for s in self._retired if s.open_connections]

    async def apply_component(self, cfg: S) -> None:
        server = self._server
        if server is None:
            raise RuntimeError("no running server to apply settings to")
        await server.apply_settings(cfg)

    def pause_component(self) -> None:
        server = self._server
        if server is not None:
            server.stop_accepting(self.UNDEPLOYED_REASON)
            self._retired.append(server)
            self._server = None

    async def start_component(self, cfg: S) -> dict[str, Any] | None:
        ordering = self.ordering_of(cfg)
        if self.DRAIN_RETIRED_BEFORE_START:
            await self._drain_retired()
        if self._publisher is not None and self._publisher_ordering != ordering:
            await self._drain_retired()
            old, self._publisher = self._publisher, None
            await asyncio.to_thread(old.close)
        if self._publisher is None:
            self._publisher = self.build_publisher(cfg)
            self._publisher_ordering = ordering
        server = self.build_server(cfg, self._publisher)
        if isinstance(server, FailureReporting):
            server.set_failure_handler(self.report_failure)
        started = await server.start()
        self._server = server
        return self.started_event(cfg, server, started)

    async def _drain_retired(self) -> None:
        retired, self._retired = self._retired, []
        if retired:
            await asyncio.gather(*(s.drain(self.drain_budget_s(s.cfg)) for s in retired))

    async def stop_component(self) -> None:
        servers = ([self._server] if self._server is not None else []) + self._retired
        for server in servers:
            server.stop_accepting("shutting-down")
        if servers:
            await asyncio.gather(*(s.drain(self.drain_budget_s(s.cfg)) for s in servers))
        self._server = None
        self._retired = []

    async def close_resources(self) -> None:
        if self._publisher is not None:
            await asyncio.to_thread(self._publisher.close)
