"""HTTP loader (W1-6): consume `<env>.<channel>.out.<destination>`, deliver over HTTP, commit after
delivery, blocking retry with circuit breaker and backoff.

    from tributary.loaders.http import HttpLoader, OutConsumer

v0 scope, and what is deliberately not here yet:

* **Auth.** Only `basic` and `bearer`. `oauth2-client-credentials` needs a token endpoint and a
  refresh loop; `mtls` needs client-certificate plumbing. Both are legal per the channel-spec schema
  and both are refused at config-load time (`config.py`) rather than silently skipped.
* **`retry.policy: retry-topic`.** The schema itself does not allow selecting it in v0
  (ADR-003 D1/D2, unmeasured); `config.py` refuses it defensively too, for a hand-built config.
* **The pipeline.** No `tributary-runtime` yet (W1-4), so `OutConsumer` is this wave's stand-in: it
  owns its own consume/commit/retry loop rather than being driven by a shared runtime stage. See
  `consumer.py` for the agreed target shape once the runtime exists.
* **Real-cluster verification.** Not run against Kafka in this wave (the shared cluster was in use
  for other work); `tests/http/test_consumer.py` exercises the retry/DLQ/halt state machine with a
  fake consumer, and `tests/http/test_integration_http_loader.py` is the guarded, real version.
"""

from __future__ import annotations

from .config import HttpDestinationConfig, http_destination_config_from_channel_spec
from .consumer import OutConsumer
from .loader import DeliveryOutcome, HttpLoader

__all__ = [
    "DeliveryOutcome",
    "HttpDestinationConfig",
    "HttpLoader",
    "OutConsumer",
    "http_destination_config_from_channel_spec",
]
