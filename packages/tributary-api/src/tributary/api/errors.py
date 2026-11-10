"""Typed startup refusals. Messages are fixed sentences: they never carry an environment value."""

from __future__ import annotations


class ConfigurationError(Exception):
    """The process must not start. `code` is stable and goes to the single `startup_refused` log line."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
