"""librdkafka client properties for a Kafka source's own connection (ADR-007 D11).

Server certificate and hostname verification are always on; there is no option that turns them off
(`$defs/clientTls`). Every TLS protocol pins an AEAD-only cipher list: librdkafka has no minimum-version
property (`ssl.min.version` does not exist), so the pin is what makes a pre-TLS-1.2 handshake fail. This
mirrors `tributary.loaders.kafka.client` (separate copy: there is no shared non-SDK home yet), including
its known gap: a broker hand-restricted to DHE-only suites needs `ssl.cipher.suites` widened by an
operator. Secrets are resolved here, at connect time, never stored in settings.
"""

from __future__ import annotations

from typing import Any

from ..secrets import SecretResolver
from .config import SourceSecurity

PINNED_CIPHER_SUITES = "ECDHE+AESGCM:ECDHE+CHACHA20"


def security_properties(security: SourceSecurity, resolver: SecretResolver) -> dict[str, Any]:
    config: dict[str, Any] = {"security.protocol": security.protocol}
    if security.protocol == "PLAINTEXT":
        return config
    config["ssl.cipher.suites"] = PINNED_CIPHER_SUITES
    config["ssl.endpoint.identification.algorithm"] = "https"
    config["enable.ssl.certificate.verification"] = True
    if security.tls is not None and security.tls.secret_ref is not None:
        config["ssl.ca.location"] = str(resolver.resolve_file(security.tls.secret_ref, "ca.crt"))
    if security.protocol == "SASL_SSL":
        if security.secret_ref is None:  # config.py requires it; fail closed regardless
            raise ValueError("SASL_SSL has no secretRef")
        config["sasl.mechanism"] = security.sasl_mechanism
        config["sasl.username"] = resolver.resolve_text(security.secret_ref, "username")
        config["sasl.password"] = resolver.resolve_text(security.secret_ref, "password")
    elif security.secret_ref is not None:  # SSL with an optional client certificate (mTLS)
        config["ssl.certificate.location"] = str(resolver.resolve_file(security.secret_ref, "tls.crt"))
        config["ssl.key.location"] = str(resolver.resolve_file(security.secret_ref, "tls.key"))
    return config
