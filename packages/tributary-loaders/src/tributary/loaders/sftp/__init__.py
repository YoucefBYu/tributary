"""`sftp` destination loader: writes each message to a deterministically named remote file, via a temporary
name and an atomic rename, over SSH with pinned host keys (`contracts/channel-spec/v0/README.md` 3.4.4).
See `loader.py` for the idempotency argument and `session.py` for host-key verification."""

from __future__ import annotations

from .config import SftpDestinationConfig, sftp_destination_config_from_channel_spec
from .handler import SftpRecordHandler
from .loader import SftpLoader

__all__ = ["SftpDestinationConfig", "SftpLoader", "SftpRecordHandler", "sftp_destination_config_from_channel_spec"]
