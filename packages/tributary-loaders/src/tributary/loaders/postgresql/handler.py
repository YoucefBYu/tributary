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
    def __init__(self, loader: PostgresqlLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
        self._loader = loader
        self._spec_source = spec_source
        self._destination = destination
        self._loop = asyncio.new_event_loop()

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
