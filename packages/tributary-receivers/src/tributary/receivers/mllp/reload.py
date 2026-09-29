"""Which MLLP settings a new `spec.version` may change in a running receiver, and which it may not.

Invariant 3: the receiver's spec comes from the compacted `_tb.<env>.channels` topic and is refreshed
while it runs. A republished spec must never need a restart for the settings that can change safely,
and must never be half-applied for the ones that cannot.

**Live (applied to the next frame or the next connection, no restart):**

| Setting | Spec field | Takes effect |
|---|---|---|
| ACK conditions | `source.ack.nackOn` | the next frame read |
| ACK timeout | `source.ack.timeoutMs` | the next frame read |
| Message size limit | `source.maxMessageBytes` | the next frame read, on every open connection |
| Queue limits | `source.queue.maxBytes`, `maxMessages` | immediately (waiting reservations re-check) |
| TLS client allow-list | `source.tls.allowedClientSans` | the next handshake, i.e. the next connection |
| Stamped channel version | `metadata.version` | the next record published |

`source.ack.mode` has one legal value (`after-commit`, invariant 1), so "changing ack mode" is only a
change that the receiver refuses: a spec carrying any other value is rejected and the last good spec
keeps serving.

**Restart required (logged as `spec_restart_required`, then a clean exit 0 for the supervisor):**

| Setting | Why it cannot change in place |
|---|---|
| `source.bind`, `source.port` | the listening socket is bound to them; rebinding would drop the listener |
| `source.tls` presence, `secretRef`, `clientAuth`, `minVersion` | the `SSLContext` (certificate, key, minimum version, client-auth mode) is built once; certificate paths come from the secrets mount |

The supervisor must restart the process on exit 0 (`restart: always` / `unless-stopped`, a Kubernetes
Deployment). Before exiting the receiver stops accepting new connections and lets every frame that is
already committed to Kafka write its ACK, so a restart drops nothing (invariant 1).

**Refused (last good spec keeps serving):** anything `mllp_settings_from_spec` raises for: `ordering`
other than `none`, a non-`mllp` source, `nackOn: spec-invalid` (reserved, README 3.2.8), a queue smaller
than `maxMessageBytes`.

Field names only are ever returned or logged here, never values: a `secretRef` or an allow-list entry
is operator input, not PHI, but a log line needs no more than which field moved.
"""

from __future__ import annotations

from .config import MllpSettings

RESTART_REQUIRED_FIELDS = ("bind", "port", "tls")

LIVE_FIELDS = (
    "channel_version",
    "ack_timeout_s",
    "nack_on",
    "max_message_bytes",
    "queue_max_bytes",
    "queue_max_messages",
    "tls.allowed_client_sans",
)


def restart_required_fields(old: MllpSettings, new: MllpSettings) -> list[str]:
    """The restart-only fields that differ. `tls` counts only its non-allow-list part."""
    changed = [name for name in ("bind", "port") if getattr(old, name) != getattr(new, name)]
    if _tls_without_allow_list(old) != _tls_without_allow_list(new):
        changed.append("tls")
    return changed


def live_fields_changed(old: MllpSettings, new: MllpSettings) -> list[str]:
    """The live fields that differ, by name."""
    changed = [name for name in LIVE_FIELDS if not name.startswith("tls.") and getattr(old, name) != getattr(new, name)]
    if _allow_list(old) != _allow_list(new):
        changed.append("tls.allowed_client_sans")
    return changed


def _allow_list(cfg: MllpSettings) -> tuple[str, ...]:
    return cfg.tls.allowed_client_sans if cfg.tls is not None else ()


def _tls_without_allow_list(cfg: MllpSettings) -> object:
    if cfg.tls is None:
        return None
    return (cfg.tls.secret_ref, cfg.tls.client_auth, cfg.tls.min_version)
