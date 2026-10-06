"""Building the botocore S3 client for one destination.

`botocore` (Apache-2.0) rather than `boto3`: SigV4 signing, the credential provider chain for `ambient`
auth and the S3 request/response model are what is needed, and the transfer manager `boto3` adds is not.
botocore is human-approved for the runtime, scoped to this loader, including its bundled, unmodified
`cacert.pem` (2026-09-29). This module still prefers the OS CA file (below); the bundled file is only the
fallback when the OS has none.

Hardening, each one a wave 2 review finding on an earlier loader:

* **No proxy from the environment.** `Config(proxies={})` is an explicit empty mapping, not `None`:
  `botocore.endpoint` reads `HTTP(S)_PROXY` only when `proxies is None`.
* **TLS verification on, against the OS trust store** when one exists (`ssl.get_default_verify_paths()`),
  like the `http` loader; otherwise botocore's own default. It is never disabled: there is no path here
  that passes `verify=False`. **TLS 1.2 minimum and ECDHE AEAD suites only** (`tls.py`), applied to the
  SSL context botocore's HTTP session built; `build_client` fails closed (`ConfigError`) if a botocore
  release moves that context, so an upgrade cannot silently drop the floor.
* **The endpoint comes from the spec only**: `ignore_configured_endpoint_urls=True` stops
  `AWS_ENDPOINT_URL[_S3]` and the shared config file from redirecting the payload elsewhere.
* **The shared config and credentials files are ignored** (`~/.aws/config`, `~/.aws/credentials`, and
  whatever `AWS_CONFIG_FILE`/`AWS_SHARED_CREDENTIALS_FILE`/`AWS_PROFILE` name): the session is built with
  both files pointed at `os.devnull` and the profile fixed, so a profile, `credential_process`, an SSO
  login or the `use_accelerate_endpoint` setting has no effect on where the payload goes or who signs it.
* **Bounded waits**: connect and read timeouts are the destination's `timeoutMs`; botocore's own retries
  are off (`max_attempts=1`), because retrying is `ConsumerLoop`'s job and hidden retries multiply the wait.
* **Secrets**: `access-key` credentials come from `secretRef` (`access_key_id`, `secret_access_key`,
  optional `session_token`) and go only into the client. **`ambient` is workload identity or an instance
  profile and nothing else** (contract: "credentials from the runtime environment (workload identity,
  instance profile), never from the spec"): the provider chain is cut down to web-identity
  (`AWS_WEB_IDENTITY_TOKEN_FILE`, IRSA), container credentials (ECS task role, EKS Pod Identity) and the
  instance metadata service. Environment access keys, profiles, SSO, `credential_process` and
  `assume-role` from config are not consulted.
* **Logging**: the `botocore` and `urllib3` loggers are capped at WARNING; their DEBUG output carries
  request parameters and headers.
* **`ExpectedBucketOwner`** is sent on every call by the loader when the destination configures it.
* Path-style addressing when a custom `endpoint` is set (MinIO and other S3-compatible stores).
"""

from __future__ import annotations

import contextlib
import logging
import os
import ssl
from typing import Any

import botocore.session  # type: ignore[import-untyped]  # no stubs; Apache-2.0
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.credentials import create_credential_resolver  # type: ignore[import-untyped]

from ..errors import ConfigError
from ..secrets import SecretResolver
from ..tls import harden
from .config import S3DestinationConfig

__all__ = ["AMBIENT_PROVIDERS", "build_client"]

#: The only botocore credential providers `ambient` may use (`METHOD` names): web identity (IRSA),
#: container credentials (ECS task role, EKS Pod Identity) and the instance metadata service.
AMBIENT_PROVIDERS = frozenset({"assume-role-with-web-identity", "container-role", "iam-role"})


def _system_ca_bundle() -> str | bool:
    """The OS CA file if there is one, else `True` (botocore's default bundle)."""
    cafile = ssl.get_default_verify_paths().cafile
    return cafile if cafile and os.path.isfile(cafile) else True


def _isolated_session() -> Any:
    """A botocore session that reads neither shared file nor any profile (module docstring). The
    variables are re-declared without their environment variable names (`AWS_PROFILE`,
    `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`), so the environment cannot bring a file back."""
    session = botocore.session.Session(
        session_vars={
            "profile": (None, None, None, None),
            "config_file": (None, None, os.devnull, None),
            "credentials_file": (None, None, os.devnull, None),
        }
    )
    session.set_config_variable("csm_enabled", False)  # client-side monitoring: a UDP side channel
    return session


def _restrict_to_ambient_providers(session: Any) -> None:
    resolver = create_credential_resolver(session)
    for provider in list(resolver.providers):
        if provider.METHOD not in AMBIENT_PROVIDERS:
            resolver.remove(provider.METHOD)
    session.register_component("credential_provider", resolver)


def _harden_tls(client: Any) -> None:
    """TLS 1.2+ and AEAD suites on the context botocore's HTTP session built. That context is reached
    through botocore internals; if a release moves it, fail closed instead of running without the floor."""
    try:
        context = client._endpoint.http_session._manager.connection_pool_kw["ssl_context"]
    except (AttributeError, KeyError):
        context = None
    if not isinstance(context, ssl.SSLContext):
        raise ConfigError("s3: cannot apply the TLS floor to this botocore version")
    harden(context)


def build_client(config: S3DestinationConfig, resolver: SecretResolver | None) -> Any:
    """A botocore S3 client. Resolves credentials now (startup), not on the first message."""
    for name in ("botocore", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)
    kwargs: dict[str, Any] = {}
    session = _isolated_session()
    if config.auth_type == "access-key":
        if resolver is None or config.auth_secret_ref is None:
            raise ValueError("access-key auth needs a secret resolver and a secretRef")
        kwargs["aws_access_key_id"] = resolver.resolve_text(config.auth_secret_ref, "access_key_id")
        kwargs["aws_secret_access_key"] = resolver.resolve_text(config.auth_secret_ref, "secret_access_key")
        with contextlib.suppress(FileNotFoundError):  # optional
            kwargs["aws_session_token"] = resolver.resolve_text(config.auth_secret_ref, "session_token")
    else:
        _restrict_to_ambient_providers(session)
    client_config = Config(
        region_name=config.region,
        connect_timeout=config.timeout_s,
        read_timeout=config.timeout_s,
        retries={"total_max_attempts": 1, "mode": "standard"},  # total attempts, so no hidden retry
        proxies={},  # explicit empty mapping: never HTTP(S)_PROXY from the environment
        ignore_configured_endpoint_urls=True,  # AWS_ENDPOINT_URL[_S3] and shared config cannot redirect PHI
        s3={"addressing_style": "path", "use_accelerate_endpoint": False}
        if config.endpoint is not None
        else {"use_accelerate_endpoint": False},
        user_agent_extra="tributary-loader",
    )
    client = session.create_client(
        "s3",
        endpoint_url=config.endpoint,
        config=client_config,
        verify=_system_ca_bundle(),
        **kwargs,
    )
    _harden_tls(client)
    return client
