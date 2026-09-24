"""Typed exceptions for `tributary-loaders`.

Same rule as the SDK and `tributary-receivers`: no PHI in an exception message. Config errors quote
field names and enum values from the channel spec (operator-authored); nothing here embeds a
payload, a record key, a response body or a URL query string.
"""

from __future__ import annotations


class LoaderError(Exception):
    """Base class for every error this package raises."""


class ConfigError(LoaderError):
    """A destination spec cannot be turned into a running loader: missing, contradictory or
    unsupported-in-this-wave configuration. Raised at startup, before any request is sent."""


class IdempotencyConstraintError(LoaderError):
    """A `sink-native` destination's own store does not actually enforce the uniqueness its
    `ON CONFLICT`/dedup-token technique depends on: no unique index or primary key on `idColumn`
    (`postgresql`), or a sorting key that does not start with it (`clickhouse`). Raised at startup —
    `contracts/channel-spec/v0/README.md` 3.4.3: "the loader proves it at startup rather than trusting
    the declaration" — rather than let every delivery silently produce duplicates."""
