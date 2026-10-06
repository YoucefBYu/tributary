"""The TLS floor for the HTTPS-speaking loaders (`http`, `fhir`, `s3`): TLS 1.2 or later, and for TLS 1.2
only AEAD suites with forward secrecy (`ECDHE+AESGCM:ECDHE+CHACHA20`). TLS 1.3 suites are all AEAD and
are not touched by the cipher string. Certificate and hostname verification are the `ssl` defaults and
nothing here can turn them off. Same reasoning as `kafka/client.py`'s pinned cipher list: an operator who
runs a destination that only offers CBC suites must upgrade it, not weaken this."""

from __future__ import annotations

import ssl

__all__ = ["CIPHERS", "MINIMUM_VERSION", "default_client_context", "harden"]

CIPHERS = "ECDHE+AESGCM:ECDHE+CHACHA20"
MINIMUM_VERSION = ssl.TLSVersion.TLSv1_2


def harden(context: ssl.SSLContext) -> ssl.SSLContext:
    """Apply the floor to `context` in place and return it."""
    context.minimum_version = MINIMUM_VERSION
    context.set_ciphers(CIPHERS)
    return context


def default_client_context() -> ssl.SSLContext:
    """`ssl.create_default_context()` (OS trust store, verification on) with the floor applied."""
    return harden(ssl.create_default_context())
