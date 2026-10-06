"""Server TLS and mTLS, built from a channel spec's `source.tls` object.

Gate finding SR-8: the Phase 0 MLLP spike terminated TLS but never authenticated the client. This
module makes `clientAuth: required` (`contracts/channel-spec/v0` `tls.clientAuth`) do the thing its
name says: `ssl.CERT_REQUIRED` plus a trust store loaded from the same `secretRef`, so a sender must
present a certificate that chains to it. `clientAuth: none` is still a legal, explicit choice — the
schema requires the field with no default specifically so that choice is visible in the spec, not an
inherited default nobody reviewed.
"""

from __future__ import annotations

import hashlib
import logging
import re
import ssl
from dataclasses import dataclass
from typing import Any

from .events import event_logger
from .secrets import SecretResolver

#: The AEAD-only TLS 1.2 list every Tributary TLS client already pins (`ECDHE` key exchange, GCM or ChaCha20).
AEAD_CIPHER_PIN = "ECDHE+AESGCM:ECDHE+CHACHA20"
_MIN_VERSIONS = {"1.2": ssl.TLSVersion.TLSv1_2, "1.3": ssl.TLSVersion.TLSv1_3}
#: Same grammar as `channel-spec/v0` `$defs/clientSan` (set 1.3). No `*` can be written, so a wildcard never matches.
_CLIENT_SAN = re.compile(
    r"^(dns:[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?|uri:[a-z][a-z0-9+.-]{0,31}:[A-Za-z0-9._~:/@%+-]{1,256})$"
)
MAX_ALLOWED_CLIENT_SANS = 32
#: Registered in `contracts/envelope/v0/error-codes.json` (set 1.3); appears in a log line and a counter only.
TLS_PEER_NOT_ALLOWED = "TLS_PEER_NOT_ALLOWED"

_log = logging.getLogger("tributary.receivers.tls")


@dataclass(frozen=True, slots=True)
class TlsSpec:
    """The `source.tls` object of a channel spec, already picked apart."""

    secret_ref: str
    client_auth: str  # "none" | "required"
    min_version: str = "1.2"
    allowed_client_sans: tuple[str, ...] = ()  # `dns:<name>` / `uri:<uri>`; empty = any certificate that chains

    def __post_init__(self) -> None:
        if self.client_auth not in ("none", "required"):
            raise ValueError(f"tls.clientAuth must be 'none' or 'required', not {self.client_auth!r}")
        if self.min_version not in _MIN_VERSIONS:
            raise ValueError(f"tls.minVersion must be '1.2' or '1.3', not {self.min_version!r}")
        sans = self.allowed_client_sans
        if sans:
            if self.client_auth != "required":
                raise ValueError("tls.allowedClientSans requires tls.clientAuth: required")
            if len(sans) > MAX_ALLOWED_CLIENT_SANS or len(set(sans)) != len(sans):
                raise ValueError("tls.allowedClientSans must hold 1-32 unique entries")
            if not all(isinstance(e, str) and _CLIENT_SAN.fullmatch(e) for e in sans):
                raise ValueError(
                    "tls.allowedClientSans entries must be `dns:<lower-case name>` or `uri:<uri>`, no wildcard"
                )


def build_ssl_context(spec: TlsSpec, resolver: SecretResolver, *, cipher_pin: str | None = None) -> ssl.SSLContext:
    """A server `SSLContext` for `spec`. `clientAuth: required` verifies the peer certificate against
    `ca.crt` under the same `secretRef`; there is no fallback to `CERT_OPTIONAL` — a spec author who
    wants that must say `clientAuth: none` and enforce authentication some other way, explicitly."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = _MIN_VERSIONS[spec.min_version]
    if cipher_pin is not None:
        ctx.set_ciphers(cipher_pin)  # TLS 1.2 and below only; TLS 1.3 suites are all AEAD already
    ctx.load_cert_chain(
        certfile=str(resolver.resolve_file(spec.secret_ref, "tls.crt")),
        keyfile=str(resolver.resolve_file(spec.secret_ref, "tls.key")),
    )
    if spec.client_auth == "required":
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=str(resolver.resolve_file(spec.secret_ref, "ca.crt")))
    else:
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def peer_san_allowed(ssl_object: ssl.SSLObject | ssl.SSLSocket | None, allowed: tuple[str, ...]) -> bool:
    """True iff the verified leaf certificate carries at least one `allowed` entry of the same type.
    `dns:` compares case-insensitively, `uri:` byte for byte. Exact comparison only: a certificate's `*.x`
    SAN is never equal to a listed entry (none can contain `*`). No list means no restriction; a list with
    no presented certificate refuses (fail closed)."""
    if not allowed:
        return True
    if ssl_object is None:
        return False
    try:
        cert = ssl_object.getpeercert()
    except ValueError:
        return False
    if not cert:
        return False
    dns = {e[4:] for e in allowed if e.startswith("dns:")}
    uri = {e[4:] for e in allowed if e.startswith("uri:")}
    sans: Any = cert.get("subjectAltName", ())
    for kind, value in sans:
        if not isinstance(value, str):
            continue
        if kind == "DNS" and value.lower() in dns:
            return True
        if kind == "URI" and value in uri:
            return True
    return False


def log_peer_refused(channel: str, ssl_object: ssl.SSLObject | ssl.SSLSocket | None) -> None:
    """One log line: code, channel and the SHA-256 of the presented leaf certificate. Never its subject
    or SANs (attacker-chosen strings; a subject can name a person)."""
    fingerprint = None
    if ssl_object is not None:
        try:
            der = ssl_object.getpeercert(binary_form=True)
        except ValueError:
            der = None
        if der:
            fingerprint = hashlib.sha256(der).hexdigest()
    event_logger("receiver-tls").warning(
        "tls_peer_refused", code=TLS_PEER_NOT_ALLOWED, channel=channel, cert_sha256=fingerprint
    )
