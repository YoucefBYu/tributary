"""Server TLS and mTLS, built from a channel spec's `source.tls` object.

Gate finding SR-8: the Phase 0 MLLP spike terminated TLS but never authenticated the client. This
module makes `clientAuth: required` (`contracts/channel-spec/v0` `tls.clientAuth`) do the thing its
name says: `ssl.CERT_REQUIRED` plus a trust store loaded from the same `secretRef`, so a sender must
present a certificate that chains to it. `clientAuth: none` is still a legal, explicit choice — the
schema requires the field with no default specifically so that choice is visible in the spec, not an
inherited default nobody reviewed.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass

from .secrets import SecretResolver

_MIN_VERSIONS = {"1.2": ssl.TLSVersion.TLSv1_2, "1.3": ssl.TLSVersion.TLSv1_3}


@dataclass(frozen=True, slots=True)
class TlsSpec:
    """The `source.tls` object of a channel spec, already picked apart."""

    secret_ref: str
    client_auth: str  # "none" | "required"
    min_version: str = "1.2"

    def __post_init__(self) -> None:
        if self.client_auth not in ("none", "required"):
            raise ValueError(f"tls.clientAuth must be 'none' or 'required', not {self.client_auth!r}")
        if self.min_version not in _MIN_VERSIONS:
            raise ValueError(f"tls.minVersion must be '1.2' or '1.3', not {self.min_version!r}")


def build_ssl_context(spec: TlsSpec, resolver: SecretResolver) -> ssl.SSLContext:
    """A server `SSLContext` for `spec`. `clientAuth: required` verifies the peer certificate against
    `ca.crt` under the same `secretRef`; there is no fallback to `CERT_OPTIONAL` — a spec author who
    wants that must say `clientAuth: none` and enforce authentication some other way, explicitly."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = _MIN_VERSIONS[spec.min_version]
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
