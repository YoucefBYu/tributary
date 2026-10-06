"""`PostgresqlRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the
runtime's `ConsumerLoop`. Same shape as `http.handler.HttpRecordHandler`; see its docstring for the
reasoning this mirrors (one persistent event loop per handler, not thread-safe, `spec_source.current()`
once per record).

Unlike the HTTP loader, `PostgresqlLoader.config` is **not** refreshed from a hot-reloaded spec on every
record: the connection pool is opened once at startup (`client.create_pool`) against the host, database
and credentials the spec named *then*, and there is no cheap way to "hot-reload" an open pool's target —
a spec change to `with.host`, `with.database`, `with.auth` or `with.table` needs a process restart.
`table` in particular is baked into the loader's per-message query build (`loader._insert_query`) via
`self.config.table`, read fresh each call but never repointed at a different pool. This is a narrower
hot-reload guarantee than `HttpRecordHandler` gives; call it out in any review or doc that compares the
two.

**`loop=` must be the same loop the pool was created on (bug fix).** `asyncpg.Pool` binds internal state
to the event loop it was built against; `main.py` used to build the pool with `asyncio.run(_prepare())` —
a throwaway loop `asyncio.run` closes as soon as it returns — while this handler drove every `deliver()`
call on a second, separate `asyncio.new_event_loop()`. That works for the pool's already-open
connections, but once the pool needs to open a *new* physical connection (after a `CONNECT_FAILED`/
`TIMEOUT` retry drops one), `asyncpg` schedules that on the loop it remembers from `create_pool` — which
is already closed — and raises `RuntimeError: Event loop is closed`, crashing the whole loader process.
`main.py` now creates the loop first and runs `_prepare()` (pool creation, then
`verify_idempotency_constraint`) on it via `loop.run_until_complete`, then passes that identical loop
here instead of letting this handler build its own — so every coroutine touching the pool, from startup
to the last `deliver()` call before shutdown, runs on the one loop `asyncpg` bound the pool to.
`tests/postgresql/test_integration_postgresql_loader.py::test_a_dropped_connection_is_retried_without_
crashing_the_loop` is the regression: it forces exactly the same connection-loss-then-retry shape this
bug needed to reproduce, against a real server and a real dropped connection, not a mock.

`PostgresqlLoader.deliver`'s `reason` on failure already IS the `tb-error-code` this destination type
uses (README 3.4 table: `TIMEOUT`, `CONNECT_FAILED`, `TLS_VERIFY_FAILED`, `PAYLOAD_SHAPE_INVALID`,
`PG_<SQLSTATE>`) — unlike the HTTP loader's `Reason` enum, there is no second mapping table here, only
the one exception: `circuit-open` (lower-case, an implementation-internal state name) becomes
`CIRCUIT_OPEN` to match the `^[A-Z][A-Z0-9_]{0,63}$` code family every other value already satisfies.
"""

from __future__ import annotations

import asyncio

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .loader import PostgresqlLoader


class PostgresqlRecordHandler:
    def __init__(
        self,
        loader: PostgresqlLoader,
        *,
        spec_source: LiveSpecSource,
        destination: str,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._loader = loader
        self._spec_source = spec_source
        self._destination = destination
        # `loop=` defaults to a fresh loop only for callers (tests, or a loader with no pool yet bound
        # to a specific loop) that do not need this; `main.py` always passes the loop the pool was
        # created on (module docstring's bug-fix note) — never build a second one here.
        self._loop = loop if loop is not None else asyncio.new_event_loop()

    def handle(self, record: ConsumedRecord) -> None:
        # Picks up a republished spec for observability only (channel/destination name changes reach
        # the loader's log lines) — see the module docstring for why the connection itself does not.
        self._spec_source.current()
        outcome = self._loop.run_until_complete(self._loader.deliver(record.message))
        if outcome.success:
            return
        error_code = "CIRCUIT_OPEN" if outcome.reason == "circuit-open" else outcome.reason
        raise DeliveryError(error_code, retryable=outcome.retryable)

    def metrics_snapshot(self) -> dict[str, int]:
        snapshot = self._loader.metrics.snapshot()
        snapshot["duplicates_absorbed"] = self._loader.duplicates_absorbed
        return snapshot

    def close(self) -> None:
        self._loop.run_until_complete(self._loader.close())
        self._spec_source.close()
        self._loop.close()
