"""MLLP receiver (W1-5): framing, ACK-after-commit publish to `<env>.<channel>.raw`, TLS/mTLS.

    from tributary.receivers.mllp import serve_from_channel_spec

v0 scope, and what is deliberately not here yet:

* **Ordering.** Only `ordering: none` channels are accepted. `ordering: per-key` needs the
  `keyExpression` evaluator (W2-4, `platform-engineer`); this package raises rather than silently
  publishing with `key=None` for a channel that asked for per-key order (invariant 5).
* **The pipeline.** There is no `tributary-runtime` yet (W1-4), so this receiver publishes directly
  to the raw topic itself, using `tributary.sdk.Message` to build a schema-conformant envelope. Once
  the runtime exists, the publish step here is what it replaces.
* **Multi-process scaling.** One process is one bound socket. Horizontal scale is running more
  receiver processes behind a network load balancer or Kubernetes `Service`, which is what "stateless
  and horizontally scalable" in this connector's charter means; there is no `SO_REUSEPORT` fan-out
  inside one process, and none is needed for that scaling model.
* **Per-connection head-of-line isolation from a single stuck partition.** Phase 0's S2 spike found
  a receiver-wide ACK stall of 7.5 to 12.7 s on one broker kill (`docs/spikes/S2-mllp-kafka.md`); the
  Phase 0 decision record (section 8, item 3) asks for isolation verified by reproducing `leader-kill`
  and comparing `ack_gap_s`. This package bounds the *damage* (a byte-budgeted queue, so a stall
  cannot grow memory without limit, and a bounded per-message publish deadline, so a connection on a
  healthy partition is not blocked by a producer-wide buffer full of stuck records for an unrelated
  partition) but does not eliminate the stall; that needs the chaos re-run this module cannot do by
  itself. Tracked for `platform-engineer`/`architect` follow-up.
"""

from __future__ import annotations

from .server import MllpServer, MllpSettings, serve_from_channel_spec

__all__ = ["MllpServer", "MllpSettings", "serve_from_channel_spec"]
