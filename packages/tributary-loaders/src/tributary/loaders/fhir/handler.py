"""`FhirRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the runtime's
`ConsumerLoop`. Synchronous, like `clickhouse.handler`: `FhirLoader.deliver` is one blocking, bounded
call, and `ConsumerLoop` calls `handle` from one thread, one record at a time.

`handle` returns only after the FHIR server has confirmed the transaction (a validated
`transaction-response`), so the loop commits the source offset only after the sink confirms. Any other
outcome raises `DeliveryError(<tb-error-code>, retryable=...)` and the loop retries, dead-letters or
halts per the destination's `retry` block.

The URL, auth and headers are fixed at startup: `main.py` builds the loader against the spec it read
then, and credentials are resolved once. A change to `with.*` needs a restart.
"""

from __future__ import annotations

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .loader import FhirLoader


class FhirRecordHandler:
    def __init__(self, loader: FhirLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
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
