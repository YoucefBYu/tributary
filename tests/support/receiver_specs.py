"""Channel-spec documents whose `spec.source` is an http, sftp or kafka source, for the receivers' tests.

Built on the walking-skeleton document (`tests.support.channel_spec`): only `spec.source` is replaced, so the
pipeline and destinations stay ones the runtime accepts. Synthetic values only.
"""

from __future__ import annotations

from typing import Any

from tests.support.channel_spec import walking_skeleton_document


def _with_source(channel: str, version: int, source: dict[str, Any]) -> dict[str, Any]:
    doc = walking_skeleton_document(
        mllp_port=0, http_url="http://127.0.0.1:1/x", channel=channel, destination="stub", version=version
    )
    doc["spec"]["source"] = source
    return doc


def http_document(
    channel: str,
    *,
    version: int = 1,
    port: int = 0,
    path: str = "/ingest",
    accept: tuple[str, ...] = ("application/fhir+json",),
    auth: dict[str, Any] | None = None,
    tls: dict[str, Any] | None = None,
    nack_on: tuple[str, ...] = ("queue-full",),
    **source: Any,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": "http",
        "bind": "127.0.0.1",
        "port": port,
        "path": path,
        "accept": list(accept),
        "auth": auth if auth is not None else {"type": "none"},
        "ack": {"mode": "after-commit", "nackOn": list(nack_on), "timeoutMs": 10000},
        "maxMessageBytes": 1_000_000,
        "queue": {"maxBytes": 67108864},
    }
    if tls is not None:
        body["tls"] = tls
    body.update(source)
    return _with_source(channel, version, body)


def sftp_document(
    channel: str,
    *,
    version: int = 1,
    host: str = "127.0.0.1",
    port: int = 22,
    directory: str = "/drop",
    **source: Any,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": "sftp",
        "host": host,
        "port": port,
        "path": directory,
        "auth": {"type": "password", "secretRef": "sftp-cred"},
        "hostKey": {"fingerprint": "SHA256:" + "A" * 43},
        "pollIntervalMs": 1000,
        "pattern": "*.hl7",
    }
    body.update(source)
    return _with_source(channel, version, body)


def kafka_source_document(
    channel: str,
    *,
    version: int = 1,
    bootstrap: str = "127.0.0.1:19092",
    topic: str = "upstream.events",
    group: str = "tb-src-1",
    **source: Any,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": "kafka",
        "bootstrapServers": bootstrap,
        "topic": topic,
        "groupId": group,
    }
    body.update(source)
    return _with_source(channel, version, body)
