"""Which HTTP settings a new `spec.version` may change in a running receiver, and which it may not.

Invariant 3: the receiver's spec comes from the compacted `_tb.<env>.channels` topic and is refreshed
while it runs. A republished spec must never need a restart for the settings that can change safely, and
must never be half-applied for the ones that cannot.

**Live (each request takes one snapshot of the settings when it starts; nothing is applied mid-request):**

| Setting | Spec field | Takes effect |
|---|---|---|
| Endpoint path | `source.path` | the next request (a request already past its head keeps the old one) |
| Accepted content types | `source.accept` | the next request |
| Authentication | `source.auth` (`type`, `secretRef`) | the next request; a bearer secret is read when the update is applied, and an unreadable or empty one refuses the update |
| ACK conditions and timeout | `source.ack.nackOn`, `source.ack.timeoutMs` | the next request |
| Message size limit | `source.maxMessageBytes` | the next request |
| Queue limits | `source.queue.maxBytes`, `maxMessages` | immediately (waiting reservations re-check) |
| TLS client allow-list | `source.tls.allowedClientSans` | the next handshake, and every open connection is re-checked now: one whose peer is no longer allowed is closed (a request already handed to Kafka still gets its answer) |
| Stamped channel version | `metadata.version` | the next record published |

`source.ack.mode` has one legal value (`after-commit`, invariant 1), so "changing ack mode" is only a change
the receiver refuses: a spec carrying any other value is rejected and the last good spec keeps serving.

**Restart required (logged as `spec_restart_required`, then a clean exit 0 for the supervisor):**

| Setting | Why it cannot change in place |
|---|---|
| `source.bind`, `source.port` | the listening socket is bound to them |
| `source.tls` presence, `secretRef`, `clientAuth`, `minVersion` | the `SSLContext` (certificate, key, minimum version, client-auth mode) is built once; going from plain to TLS or back is a different listener |
| `spec.ordering`, `spec.partitionKey` | the raw producer's key rule is fixed at construction, and a key that changes under a running channel would move one patient between partitions and reorder them silently (invariant 5) |

The key locator (`keyExpression`, i.e. `spec.partitionKey`) is deliberately in the restart-only table, as it
is for MLLP: it is the one setting whose live change can reorder messages. What does apply live is the
*check* of the current locator against a new `accept` list: a spec whose `accept` no longer suits the locator
is refused and the last good spec keeps serving.

**Refused (last good spec keeps serving):** anything `http_settings_from_spec` raises for: a non-`http`
source, `nackOn: spec-invalid` (reserved), a queue smaller than `maxMessageBytes`, `auth: none` off loopback,
`auth: mtls` without `clientAuth: required`, a non-positive limit.

The supervisor must restart the process on exit 0 (`restart: always` / `unless-stopped`). Before exiting the
receiver stops accepting and lets every request already committed to Kafka write its response.

Field names only are ever returned or logged here, never values.
"""

from __future__ import annotations

from .config import HttpSettings

RESTART_REQUIRED_FIELDS = ("bind", "port", "tls", "ordering", "partition_key")

LIVE_FIELDS = (
    "channel_version",
    "path",
    "accept",
    "auth",
    "ack_timeout_s",
    "nack_on",
    "max_message_bytes",
    "queue_max_bytes",
    "queue_max_messages",
    "tls.allowed_client_sans",
)


def restart_required_fields(old: HttpSettings, new: HttpSettings) -> list[str]:
    """The restart-only fields that differ. `tls` counts only its non-allow-list part."""
    changed = [
        name for name in ("bind", "port", "ordering", "partition_key") if getattr(old, name) != getattr(new, name)
    ]
    if _tls_without_allow_list(old) != _tls_without_allow_list(new):
        changed.append("tls")
    return changed


def live_fields_changed(old: HttpSettings, new: HttpSettings) -> list[str]:
    """The live fields that differ, by name."""
    changed = [name for name in LIVE_FIELDS if not name.startswith("tls.") and getattr(old, name) != getattr(new, name)]
    if _allow_list(old) != _allow_list(new):
        changed.append("tls.allowed_client_sans")
    return changed


def _allow_list(cfg: HttpSettings) -> tuple[str, ...]:
    return cfg.tls.allowed_client_sans if cfg.tls is not None else ()


def _tls_without_allow_list(cfg: HttpSettings) -> object:
    if cfg.tls is None:
        return None
    return (cfg.tls.secret_ref, cfg.tls.client_auth, cfg.tls.min_version)
