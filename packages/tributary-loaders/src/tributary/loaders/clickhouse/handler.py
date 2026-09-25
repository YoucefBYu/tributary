"""`ClickhouseRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the
runtime's `ConsumerLoop`. Synchronous, like `kafka.handler.KafkaRecordHandler` and unlike
`postgresql`/`http` — `urllib` is a plain blocking call and `ClickhouseLoader.deliver` is a plain
method, so there is no event loop to drive here.

Like `PostgresqlRecordHandler`, the transport is **not** refreshed from a hot-reloaded spec on every
record: `client.transport_from_config` builds one `ClickhouseTransport` at startup against the host,
database and credentials the spec named *then*. A spec change to `with.host`, `with.database`,
`with.auth` or `with.table` needs a process restart.
"""

from __future__ import annotations

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .loader import ClickhouseLoader


class ClickhouseRecordHandler:
    def __init__(self, loader: ClickhouseLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
        self._loader = loader
        self._spec_source = spec_source
        self._destination = destination

    def handle(self, record: ConsumedRecord) -> None:
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
