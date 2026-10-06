"""`KafkaRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the
runtime's `ConsumerLoop`. Same shape as `postgresql.handler.PostgresqlRecordHandler` and
`http.handler.HttpRecordHandler`, but **synchronous, with no event loop of its own**: `confluent_kafka.
Producer.produce`/`flush` are plain blocking calls, and `KafkaLoader.deliver` is a plain method — there
is no coroutine here to drive, unlike the `asyncpg`- and `urllib`-backed loaders.

Like `PostgresqlRecordHandler` (and unlike `HttpRecordHandler`), `KafkaLoader.config`'s connection is
**not** refreshed from a hot-reloaded spec on every record: the producer is opened once at startup
(`client.producer_settings_from_config`) against the bootstrap servers and credentials the spec named
*then*. A spec change to `with.connection`, `with.topic` or `with.key` needs a process restart.
"""

from __future__ import annotations

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .loader import KafkaLoader


class KafkaRecordHandler:
    def __init__(self, loader: KafkaLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
        self._loader = loader
        self._spec_source = spec_source
        self._destination = destination

    def handle(self, record: ConsumedRecord) -> None:
        # Picks up a republished spec for observability only (channel/destination name changes reach
        # the loader's log lines) — see the module docstring for why the connection itself does not.
        self._spec_source.current()
        outcome = self._loader.deliver(record.message)
        if outcome.success:
            return
        error_code = "CIRCUIT_OPEN" if outcome.reason == "circuit-open" else outcome.reason
        raise DeliveryError(error_code, retryable=outcome.retryable)

    def metrics_snapshot(self) -> dict[str, int]:
        return self._loader.metrics.snapshot()

    def close(self) -> None:
        self._loader.close()
        self._spec_source.close()
