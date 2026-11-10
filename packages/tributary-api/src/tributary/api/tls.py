"""The API listener's TLS (ADR-004 C1a). The ingress-to-API hop carries bearer tokens and, from W3-6, PHI.

* **Default: TLS on the API's own listener.** Certificate and key are `secretRef` files
  (`<TB_SECRETS_DIR>/<TB_API_TLS_SECRET_REF>/tls.crt` and `tls.key`), read once at start and never logged. Python
  `ssl` defaults, minimum TLS 1.2, and for TLS 1.2 only forward-secret AEAD suites (`ECDHE+AESGCM:ECDHE+CHACHA20`).
* **Plaintext on a non-loopback bind is refused** (`Settings`: `API_TLS_REQUIRED`, exit 2) unless the operator opts
  into `TB_API_TLS_MODE=mesh`. The API cannot see a mesh, so mesh mode binds plaintext and logs
  `api_tls_delegated_to_mesh` at WARN on every start; the chart then admits only the ingress controller.
* Loopback plaintext (lite profile, a local sidecar) starts without a secretRef.
"""

from __future__ import annotations

import ssl

import uvicorn
from fastapi import FastAPI
from tributary.runtime.errors import RuntimeConfigurationError
from tributary.runtime.secrets import DirSecretResolver
from tributary.sdk.context import Logger

from .config import Settings
from .errors import ConfigurationError

TLS12_CIPHERS = "ECDHE+AESGCM:ECDHE+CHACHA20"
CERT_KEY = "tls.crt"
KEY_KEY = "tls.key"


def server_config(app: FastAPI, settings: Settings, resolver: DirSecretResolver | None, log: Logger) -> uvicorn.Config:
    """The uvicorn config for `settings`. With a TLS secretRef the certificate chain and key are loaded here,
    once, and the context is pinned to TLS 1.2 or later; any failure is `API_TLS_INVALID` with fixed text (the
    exception from `ssl` can name a file, so it is dropped)."""
    certfile = keyfile = None
    if settings.tls_secret_ref is not None:
        if resolver is None:
            raise ConfigurationError("API_TLS_INVALID", "the TLS secretRef cannot be resolved without TB_SECRETS_DIR")
        try:
            certfile = str(resolver.resolve_file(settings.tls_secret_ref, CERT_KEY))
            keyfile = str(resolver.resolve_file(settings.tls_secret_ref, KEY_KEY))
        except RuntimeConfigurationError:
            raise ConfigurationError("API_TLS_INVALID", "the TLS certificate or key did not resolve") from None
    config = uvicorn.Config(
        app,
        host=settings.bind,
        port=settings.port,
        access_log=False,
        log_config=None,
        server_header=False,
        limit_concurrency=settings.max_connections,  # beyond it uvicorn answers 503 instead of taking more work
        backlog=max(64, settings.max_connections),
        ssl_certfile=certfile,
        ssl_keyfile=keyfile,
    )
    if certfile is not None:
        try:
            config.load()  # reads the certificate and key now; `Server.run` finds it loaded
            context = config.ssl
            if not isinstance(context, ssl.SSLContext):
                raise ssl.SSLError
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            # TLS 1.2: forward-secret AEAD suites only (ADR-004 C1a). TLS 1.3 suites are not set by this call.
            context.set_ciphers(TLS12_CIPHERS)
        except (ssl.SSLError, OSError, ValueError):
            raise ConfigurationError("API_TLS_INVALID", "the TLS certificate and key are not a usable pair") from None
        log.info("api_tls_enabled", min_version="TLSv1.2")
    elif settings.tls_mode == "mesh":
        log.warning("api_tls_delegated_to_mesh")
    return config
