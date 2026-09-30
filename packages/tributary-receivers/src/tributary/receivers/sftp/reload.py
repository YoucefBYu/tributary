"""Which SFTP settings a new `spec.version` may change in a running poller, and which it may not.

Invariant 3: the poller's spec comes from the compacted `_tb.<env>.channels` topic and is refreshed while it
runs. A republished spec must never need a restart for the settings that can change safely, and must never be
half-applied for the ones that cannot.

**Live (a poll cycle takes one snapshot for its listing and claim paths, every file takes one for what is done
to that file; nothing is applied mid-file):**

| Setting | Spec field | Takes effect |
|---|---|---|
| Poll interval | `source.sftp.pollIntervalSeconds` | the next wait between cycles |
| Selection | `source.sftp.filePattern`, `minFileAgeSeconds`, `maxFilesPerPoll` | the next cycle |
| Source directory | `source.sftp.directory` | the next cycle. A file this poller had claimed but not committed stays under its `.tb-claim.` name in the old directory (nothing is lost or moved); it is picked up again only if the directory is set back |
| After commit | `source.sftp.afterCommit`, `archiveDirectory`, `rejectDirectory` | the next file |
| ACK conditions and timeout | `source.ack.nackOn`, `source.ack.timeoutMs` | the next file |
| Size limit | `source.maxMessageBytes` | the next file |
| Content type | `source.contentType` | the next file (the key locator is re-checked against it) |
| Stamped channel version | `metadata.version` | the next record published |

**Tombstone pauses polling.** The file in flight is finished (committed and archived, or handed back); nothing
new is claimed or published; the process stays up, not ready. A later spec for the channel starts a new
poller, after the paused one has finished its file.

**Restart required (logged as `spec_restart_required`, then a clean exit 0 for the supervisor):**

| Setting | Why it cannot change in place |
|---|---|
| `source.sftp.host`, `port` | the endpoint the session factory dials |
| `source.sftp.hostKeys` | the pinned host-key fingerprints checked before authentication (README 10.2); changing what is trusted mid-run is a restart |
| `source.sftp.auth` (`type`, `secretRef`) | the credentials the session factory reads |
| `spec.ordering`, `spec.partitionKey` | the raw producer's key rule is fixed at construction; a key that changes under a running channel could reorder a patient's files (invariant 5) |

**Refused (last good spec keeps serving):** anything `sftp_settings_from_spec` raises for: a non-`sftp` source,
`nackOn` other than `parse-error`, an archive or reject directory equal to the source, a non-positive interval or
limit, a malformed host key.

Field names only are ever returned or logged here, never values.
"""

from __future__ import annotations

from .config import SftpSettings

RESTART_REQUIRED_FIELDS = (
    "host",
    "port",
    "host_keys",
    "auth_type",
    "auth_secret_ref",
    "ordering",
    "partition_key",
)

LIVE_FIELDS = (
    "channel_version",
    "content_type",
    "directory",
    "file_pattern",
    "poll_interval_s",
    "min_file_age_s",
    "max_files_per_poll",
    "after_commit",
    "archive_directory",
    "reject_directory",
    "nack_on",
    "max_message_bytes",
    "ack_timeout_s",
)


def restart_required_fields(old: SftpSettings, new: SftpSettings) -> list[str]:
    """The restart-only fields that differ, by name (never a value: `auth_secret_ref` is operator input)."""
    return [name for name in RESTART_REQUIRED_FIELDS if getattr(old, name) != getattr(new, name)]


def live_fields_changed(old: SftpSettings, new: SftpSettings) -> list[str]:
    """The live fields that differ, by name."""
    return [name for name in LIVE_FIELDS if getattr(old, name) != getattr(new, name)]
