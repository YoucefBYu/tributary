"""HTTP loader (W1-6): consume `<env>.<channel>.out.<destination>`, deliver over HTTP, commit after
delivery, blocking retry with circuit breaker and backoff.

    from tributary.loaders.http import HttpLoader, HttpRecordHandler

`HttpRecordHandler` is the `tributary.runtime.consumer.RecordHandler` this loader gives the runtime's
`ConsumerLoop`: consume, commit-after-delivery, per-partition pause/seek/resume blocking retry,
`onExhaust` (DLQ or halt) all belong to `ConsumerLoop` (W1-6 second round, coordinated with
platform-engineer — an earlier `OutConsumer` in this package duplicated that machinery before
`tributary-runtime` existed; see git history and `handler.py`'s docstring). `HttpLoader.deliver` is
the one attempt `HttpRecordHandler.handle` drives per record.

v0 scope, and what is deliberately not here yet:

* **Auth.** Only `basic` and `bearer`. `oauth2-client-credentials` needs a token endpoint and a
  refresh loop; `mtls` needs client-certificate plumbing. Both are legal per the channel-spec schema
  and both are refused at config-load time (`config.py`) rather than silently skipped.
* **Hot reload.** `main.py` reads the channel spec once from a file; a republished spec needs a
  restart. The pipeline worker already reloads from a live `_tb.<env>.channels` cache
  (`tributary.runtime.channels.ChannelCache`) — this loader does not yet, tracked as a follow-up.
* **Real-cluster verification.** Not run against Kafka from this package directly in this wave;
  `tributary.runtime`'s own integration tests cover `ConsumerLoop` against a real broker, and
  `tests/http/test_loader.py` and `test_circuit_breaker.py` cover everything `HttpLoader` does on
  its own with no Kafka needed.
"""

from __future__ import annotations

from .config import HttpDestinationConfig, http_destination_config_from_channel_spec
from .handler import HttpRecordHandler
from .loader import DeliveryOutcome, HttpLoader

__all__ = [
    "DeliveryOutcome",
    "HttpDestinationConfig",
    "HttpLoader",
    "HttpRecordHandler",
    "http_destination_config_from_channel_spec",
]
