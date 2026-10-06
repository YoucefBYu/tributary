"""Turning a `contracts/channel-spec/v0` document (`spec.source.type: sftp`) into `SftpSettings`.

Same split as `http.config`: the schema is trusted for the shapes it constrains (a hostname or `::1`,
1-4 `SHA256:` host keys, absolute paths without `.`/`..`), and this module refuses what the schema allows
but this poller cannot honour or what would make it unsafe (a directory that is also the archive or reject
directory would re-read its own output forever). The spec arrives as a parsed
`tributary.runtime.spec.ChannelSpec` from the live `_tb.<env>.channels` reader (`sftp_settings_from_spec`),
and `sftp/reload.py` says which settings apply live: every value applied live is checked here, so a bad
update is a `ConfigError` before anything is applied.

Secret layout for `sftp.auth.secretRef` (a directory of files, `secrets.DirSecretResolver`):
`username` always; `private_key` (an OpenSSH or PEM file, and optionally `passphrase`) for
`private-key`; `password` for `password`. They are read at connect time, never at import.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.keys import KeyExpression

from ..errors import ConfigError
from ..keys import key_expression_from_spec

DEFAULT_ACK_TIMEOUT_MS = 10_000
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000  # envelope inline limit, invariant 8
DEFAULT_MAX_FILES_PER_POLL = 100
_FINGERPRINT = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")
_INSTANCE = re.compile(r"^[a-z0-9-]{1,32}$")  # no dot: it is the delimiter in a claim name

#: `parse-error` is the only `nackOn` condition an sftp source can produce (a file whose key does not
#: resolve, or, if declared, malformed JSON); it is always the reject-directory move. `queue-full` has
#: no meaning for a poller (nothing is admitted until the previous file is committed) and
#: `spec-invalid` is not detectable yet, so both are refused instead of silently never firing.
SUPPORTED_NACK_ON = frozenset({"parse-error"})


@dataclass(frozen=True, slots=True)
class SftpSettings:
    channel: str
    channel_version: int
    content_type: str
    host: str
    port: int
    host_keys: frozenset[str]
    auth_type: str  # "private-key" | "password"
    auth_secret_ref: str = field(repr=False)
    directory: str
    file_pattern: str
    poll_interval_s: float
    min_file_age_s: float
    max_files_per_poll: int
    after_commit: str  # "delete" | "move"
    archive_directory: str | None
    reject_directory: str
    nack_on: frozenset[str]
    key_expression: KeyExpression | None
    max_message_bytes: int
    ack_timeout_s: float
    env: str
    instance: str = "sftp-0"
    connect_timeout_s: float = 15.0
    op_timeout_s: float = 30.0
    max_listing: int = 100_000
    #: `spec.ordering` and the `spec.partitionKey` text. Fixed for the life of the process (`sftp/reload.py`).
    ordering: str = "none"
    partition_key: str | None = None

    def __post_init__(self) -> None:
        if (self.ordering == "per-key") != (self.key_expression is not None):
            raise ConfigError(f"channel {self.channel!r}: ordering per-key needs a key expression, none forbids one")
        for name in ("max_message_bytes", "max_files_per_poll"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigError(f"channel {self.channel!r}: {name} must be a positive integer")
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not 1 <= self.port <= 65535:
            raise ConfigError(f"channel {self.channel!r}: sftp.port must be an integer in 1..65535")
        for name in ("poll_interval_s", "ack_timeout_s"):
            value = getattr(self, name)
            if not isinstance(value, int | float) or isinstance(value, bool) or not 0 < value < float("inf"):
                raise ConfigError(f"channel {self.channel!r}: {name} must be a positive number")
        if not isinstance(self.min_file_age_s, int | float) or not 0 <= self.min_file_age_s < float("inf"):
            raise ConfigError(f"channel {self.channel!r}: sftp.minFileAgeSeconds must be a non-negative number")
        if not isinstance(self.file_pattern, str) or not self.file_pattern:
            raise ConfigError(f"channel {self.channel!r}: sftp.filePattern must be a non-empty string")
        if not _INSTANCE.match(self.instance):
            raise ConfigError(f"instance {self.instance!r} must match {_INSTANCE.pattern}")
        if not self.host_keys or not all(_FINGERPRINT.match(k) for k in self.host_keys):
            raise ConfigError(f"channel {self.channel!r}: sftp.hostKeys must be 1-4 SHA256: fingerprints")
        if self.max_message_bytes > DEFAULT_MAX_MESSAGE_BYTES:
            raise ConfigError(f"channel {self.channel!r}: maxMessageBytes exceeds the inline limit (invariant 8)")
        if self.after_commit not in ("delete", "move"):
            raise ConfigError(f"channel {self.channel!r}: sftp.afterCommit must be 'delete' or 'move'")
        if self.after_commit == "move" and not self.archive_directory:
            raise ConfigError(f"channel {self.channel!r}: afterCommit 'move' requires archiveDirectory")
        if self.auth_type not in ("private-key", "password"):
            raise ConfigError(f"channel {self.channel!r}: sftp.auth.type must be 'private-key' or 'password'")
        for label, path in (
            ("directory", self.directory),
            ("archiveDirectory", self.archive_directory),
            ("rejectDirectory", self.reject_directory),
        ):
            if path is not None and (not path.startswith("/") or ".." in path.split("/") or "." in path.split("/")):
                raise ConfigError(f"channel {self.channel!r}: sftp.{label} must be absolute without '.' or '..'")
        source = posixpath.normpath(self.directory)
        for label, path in (("archiveDirectory", self.archive_directory), ("rejectDirectory", self.reject_directory)):
            if path is not None and posixpath.normpath(path) == source:
                raise ConfigError(
                    f"channel {self.channel!r}: sftp.{label} must differ from directory "
                    "(the poller would re-read its own output)"
                )
        if self.archive_directory is not None and posixpath.normpath(self.archive_directory) == posixpath.normpath(
            self.reject_directory
        ):
            raise ConfigError(f"channel {self.channel!r}: archiveDirectory and rejectDirectory must differ")


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def sftp_settings_from_channel_spec(spec: Mapping[str, Any], *, env: str, instance: str = "sftp-0") -> SftpSettings:
    """Build `SftpSettings` from a parsed channel-spec document. Raises `ConfigError` for anything this
    poller cannot honour; never silently ignores a field the schema allows."""
    metadata = _require(spec, "metadata", "channel spec")
    body = _require(spec, "spec", "channel spec")
    return _settings(
        _require(metadata, "name", "metadata"),
        int(metadata.get("version", 0)),
        _require(body, "ordering", "spec"),
        _require(body, "source", "spec"),
        partition_key=body.get("partitionKey"),
        env=env,
        instance=instance,
    )


def sftp_settings_from_spec(spec: ChannelSpec, *, env: str, instance: str = "sftp-0") -> SftpSettings:
    """Build `SftpSettings` from the runtime's parsed spec (the `_tb.<env>.channels` record)."""
    if not spec.source:
        raise ConfigError(f"channel {spec.name!r}: spec.source is absent")
    return _settings(
        spec.name,
        spec.version,
        spec.ordering,
        spec.source,
        partition_key=spec.partition_key,
        env=env,
        instance=instance,
    )


def _settings(
    channel: str,
    channel_version: int,
    ordering: str,
    source: Mapping[str, Any],
    *,
    partition_key: str | None,
    env: str,
    instance: str,
) -> SftpSettings:
    if source.get("type") != "sftp":
        raise ConfigError(f"channel {channel!r}: source.type is {source.get('type')!r}, not 'sftp'")

    ack = _require(source, "ack", "source")
    if ack.get("mode") != "after-commit":
        raise ConfigError("source.ack.mode must be 'after-commit' (invariant 1; the schema has no other value)")
    nack_on = frozenset(ack.get("nackOn", ()))
    if nack_on - SUPPORTED_NACK_ON:
        raise ConfigError(
            f"channel {channel!r}: source.ack.nackOn declares {sorted(nack_on - SUPPORTED_NACK_ON)}, which an "
            f"sftp source cannot produce (supported: {sorted(SUPPORTED_NACK_ON)}); remove them from the spec"
        )

    content_type = _require(source, "contentType", "source")
    sftp = _require(source, "sftp", "source")
    auth = _require(sftp, "auth", "source.sftp")
    key_expression = key_expression_from_spec(
        {"ordering": ordering, "partitionKey": partition_key},
        channel=channel,
        source_type="sftp",
        content_types=frozenset({content_type}),
    )
    try:
        return SftpSettings(
            channel=channel,
            channel_version=channel_version,
            content_type=content_type,
            host=_require(sftp, "host", "source.sftp"),
            port=sftp.get("port", 22),
            host_keys=frozenset(_require(sftp, "hostKeys", "source.sftp")),
            auth_type=_require(auth, "type", "source.sftp.auth"),
            auth_secret_ref=_require(auth, "secretRef", "source.sftp.auth"),
            directory=_require(sftp, "directory", "source.sftp"),
            file_pattern=_require(sftp, "filePattern", "source.sftp"),
            poll_interval_s=_require(sftp, "pollIntervalSeconds", "source.sftp"),
            min_file_age_s=sftp.get("minFileAgeSeconds", 0),
            max_files_per_poll=sftp.get("maxFilesPerPoll", DEFAULT_MAX_FILES_PER_POLL),
            after_commit=_require(sftp, "afterCommit", "source.sftp"),
            archive_directory=sftp.get("archiveDirectory"),
            reject_directory=_require(sftp, "rejectDirectory", "source.sftp"),
            nack_on=nack_on,
            key_expression=key_expression,
            max_message_bytes=source.get("maxMessageBytes", DEFAULT_MAX_MESSAGE_BYTES),
            ack_timeout_s=ack.get("timeoutMs", DEFAULT_ACK_TIMEOUT_MS) / 1000,
            env=env,
            instance=instance,
            ordering=ordering,
            partition_key=partition_key,
        )
    except TypeError as exc:  # a value of the wrong JSON type reached a constructor: refuse, name no value
        raise ConfigError(f"channel {channel!r}: a source.sftp field has the wrong type") from exc
