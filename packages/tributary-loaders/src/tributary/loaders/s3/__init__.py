"""`s3` destination loader: writes each message as one object under a key derived from `tb-msg-id`, with a
conditional put where the store supports it (`contracts/channel-spec/v0/README.md` 3.4.5). See `loader.py`
for the idempotency argument and `client.py` for the transport hardening."""

from __future__ import annotations

from .config import S3DestinationConfig, s3_destination_config_from_channel_spec
from .handler import S3RecordHandler
from .loader import S3Loader

__all__ = ["S3DestinationConfig", "S3Loader", "S3RecordHandler", "s3_destination_config_from_channel_spec"]
