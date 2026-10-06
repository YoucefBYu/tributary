"""`MllpRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the runtime's
`ConsumerLoop`. Synchronous, like `kafka.handler.KafkaRecordHandler` and `clickhouse.handler.
ClickhouseRecordHandler` -- `socket`/`ssl` are plain blocking calls and `MllpLoader.deliver` is a plain
method, so there is no event loop to drive here.

Like `ClickhouseRecordHandler`, the connection is **not** refreshed from a hot-reloaded spec on every
record: `client.connection_from_config` builds one `MllpConnection` at startup against the host, port
and TLS trust bundle the spec named *then*. A spec change to `with.host`, `with.port` or `with.tls`
needs a process restart. The `out.<dest>` offset commits (`ConsumerLoop`, invariant 1) only when
`handle` returns without raising -- which `MllpLoader.deliver` only does for `AA`/`CA` with a matching
control id (`loader.py`'s module docstring)."""

from __future__ import annotations

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .loader import MllpLoader


class MllpRecordHandler:
    def __init__(self, loader: MllpLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
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
