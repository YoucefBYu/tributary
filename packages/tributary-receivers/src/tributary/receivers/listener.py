"""The lifecycle of a receiver that owns a listening socket and a `RawPublisher` (MLLP, HTTP).

On top of `ReceiverLifecycle`: one raw publisher shared by every server the process builds over its life,
the servers a tombstone leaves draining (`_retired`), and the rule that a producer is never closed under a
frame whose commit is still in flight.

* **tombstone**: the running server stops accepting (its listener closes, a request or frame that arrives
  later on an open connection is refused with a fixed text) and moves to `_retired`; whatever it had already
  handed to the publisher still gets its answer. A later spec starts a fresh server on the same publisher.
* **a redeploy that changes the publisher's ordering rule** rebuilds the producer, because the key rule is
  fixed at construction. The retired servers drain first, then the old producer flushes and closes.
* **stop**: every server stops accepting, drains bounded, closes its connections; the publisher closes last.
"""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import Any, Protocol

from .lifecycle import ReceiverLifecycle
from .mllp.kafka_publisher import RawPublisher


class ListenerServer[S](Protocol):
    """What a listener lifecycle needs from `MllpServer` / `HttpServer`."""

    @property
    def cfg(self) -> S: ...

    @property
    def open_connections(self) -> int: ...

    async def start(self) -> asyncio.base_events.Server: ...

    async def apply_settings(self, new: S) -> None: ...

    def stop_accepting(self, reason: str) -> None: ...

    async def drain(self, timeout_s: float) -> bool: ...


class ListenerLifecycle[S](ReceiverLifecycle[S]):
    #: The tombstone reason a later request on an open connection is refused with (a fixed string).
    UNDEPLOYED_REASON = "channel-undeployed"

    _server: ListenerServer[S] | None
    _retired: list[ListenerServer[S]]
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
    def build_server(self, cfg: S, publisher: RawPublisher) -> ListenerServer[S]: ...

    @abstractmethod
    def started_event(self, cfg: S, server: ListenerServer[S], listener: asyncio.base_events.Server) -> dict[str, Any]:
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
        if self._publisher is not None and self._publisher_ordering != ordering:
            await self._drain_retired()
            old, self._publisher = self._publisher, None
            await asyncio.to_thread(old.close)
        if self._publisher is None:
            self._publisher = self.build_publisher(cfg)
            self._publisher_ordering = ordering
        server = self.build_server(cfg, self._publisher)
        listener = await server.start()
        self._server = server
        return self.started_event(cfg, server, listener)

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
