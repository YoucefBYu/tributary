"""Building the botocore S3 client for one destination.

`botocore` (Apache-2.0) rather than `boto3`: SigV4 signing, the credential provider chain for `ambient`
auth and the S3 request/response model are what is needed, and the transfer manager `boto3` adds is not.
`licence_check.py` passes with it (jmespath MIT, python-dateutil BSD/Apache, urllib3 MIT; no `certifi`).

Hardening, each one a wave 2 review finding on an earlier loader:

* **No proxy from the environment.** `Config(proxies={})` is an explicit empty mapping, not `None`:
  `botocore.endpoint` reads `HTTP(S)_PROXY` only when `proxies is None`.
* **TLS verification on, against the OS trust store** when one exists (`ssl.get_default_verify_paths()`),
  like the `http` loader; otherwise botocore's own default. It is never disabled: there is no path here
  that passes `verify=False`.
* **The endpoint comes from the spec only**: `ignore_configured_endpoint_urls=True` stops
  `AWS_ENDPOINT_URL[_S3]` and the shared config file from redirecting the payload elsewhere.
* **Bounded waits**: connect and read timeouts are the destination's `timeoutMs`; botocore's own retries
  are off (`max_attempts=1`), because retrying is `ConsumerLoop`'s job and hidden retries multiply the wait.
* **Secrets**: `access-key` credentials come from `secretRef` (`access_key_id`, `secret_access_key`,
  optional `session_token`) and go only into the client; `ambient` uses botocore's default chain
  (environment, workload identity, instance profile) and takes nothing from the spec.
* Path-style addressing when a custom `endpoint` is set (MinIO and other S3-compatible stores).
"""

from __future__ import annotations

import contextlib
import os
import ssl
from typing import Any

import botocore.session  # type: ignore[import-untyped]  # no stubs; Apache-2.0
from botocore.config import Config  # type: ignore[import-untyped]

from ..secrets import SecretResolver
from .config import S3DestinationConfig

__all__ = ["build_client"]


def _system_ca_bundle() -> str | bool:
    """The OS CA file if there is one, else `True` (botocore's default bundle)."""
    cafile = ssl.get_default_verify_paths().cafile
    return cafile if cafile and os.path.isfile(cafile) else True


def build_client(config: S3DestinationConfig, resolver: SecretResolver | None) -> Any:
    """A botocore S3 client. Resolves credentials now (startup), not on the first message."""
    kwargs: dict[str, Any] = {}
    if config.auth_type == "access-key":
        if resolver is None or config.auth_secret_ref is None:
            raise ValueError("access-key auth needs a secret resolver and a secretRef")
        kwargs["aws_access_key_id"] = resolver.resolve_text(config.auth_secret_ref, "access_key_id")
        kwargs["aws_secret_access_key"] = resolver.resolve_text(config.auth_secret_ref, "secret_access_key")
        with contextlib.suppress(FileNotFoundError):  # optional
            kwargs["aws_session_token"] = resolver.resolve_text(config.auth_secret_ref, "session_token")
    client_config = Config(
        region_name=config.region,
        connect_timeout=config.timeout_s,
        read_timeout=config.timeout_s,
        retries={"total_max_attempts": 1, "mode": "standard"},  # total attempts, so no hidden retry
        proxies={},  # explicit empty mapping: never HTTP(S)_PROXY from the environment
        ignore_configured_endpoint_urls=True,  # AWS_ENDPOINT_URL[_S3] and shared config cannot redirect PHI
        s3={"addressing_style": "path"} if config.endpoint is not None else {},
        user_agent_extra="tributary-loader",
    )
    session = botocore.session.get_session()
    return session.create_client(
        "s3",
        endpoint_url=config.endpoint,
        config=client_config,
        verify=_system_ca_bundle(),
        **kwargs,
    )
