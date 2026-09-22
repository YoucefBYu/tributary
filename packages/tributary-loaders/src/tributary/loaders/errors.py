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
