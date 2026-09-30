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
    host: str = "sftp.synthetic.example",
    port: int = 22,
    directory: str = "/in",
    pattern: str = "*.hl7",
    poll_interval_s: float = 0.05,
    secret_ref: str = "sftp-cred",  # noqa: S107
    host_key: str = "SHA256:" + "A" * 43,
    **sftp: Any,
) -> dict[str, Any]:
    """An sftp source. `sftp` overrides or adds fields of `source.sftp`."""
    inner: dict[str, Any] = {
        "host": host,
        "port": port,
        "hostKeys": [host_key],
        "auth": {"type": "password", "secretRef": secret_ref},
        "directory": directory,
        "filePattern": pattern,
        "pollIntervalSeconds": poll_interval_s,
        "minFileAgeSeconds": 0,
        "maxFilesPerPoll": 100,
        "afterCommit": "delete",
        "rejectDirectory": f"{directory}/rejected",
    }
    inner.update(sftp)
    body: dict[str, Any] = {
        "type": "sftp",
        "contentType": "application/hl7-v2+er7",
        "ack": {"mode": "after-commit", "nackOn": ["parse-error"]},
        "maxMessageBytes": 1_000_000,
        "sftp": inner,
    }
    return _with_source(channel, version, body)


def kafka_source_document(
    channel: str,
    *,
    version: int = 1,
    bootstrap: str = "127.0.0.1:19092",
    topic: str = "upstream.events",
    security: dict[str, Any] | None = None,
    start_from: str = "earliest",
    nack_on: tuple[str, ...] = ("parse-error",),
    **source: Any,
) -> dict[str, Any]:
    """A kafka source on a foreign cluster; `source` overrides or adds fields of `spec.source`."""
    body: dict[str, Any] = {
        "type": "kafka",
        "contentType": "application/json",
        "ack": {"mode": "after-commit", "nackOn": list(nack_on)},
        "maxMessageBytes": 1_000_000,
        "kafka": {
            "connection": {
                "bootstrapServers": [bootstrap],
                "security": security if security is not None else {"protocol": "PLAINTEXT"},
            },
            "topic": topic,
            "startFrom": start_from,
        },
    }
    body.update(source)
    return _with_source(channel, version, body)
