"""Startup helpers shared by the eight `loaders/*/main.py` entry points."""

from __future__ import annotations

from typing import NoReturn

from tributary.runtime.errors import RuntimeConfigurationError, safe_token
from tributary.runtime.topics import spec_reader_group
from tributary.sdk.context import Logger

EXIT_CONFIG_REFUSED = 2


def _refuse(log: Logger, exc: RuntimeConfigurationError) -> NoReturn:
    # Class name and stable code only: the instance id is operator input, so it is never echoed.
    log.error("startup_refused", error=safe_token(type(exc).__name__), code=safe_token(exc.code))
    raise SystemExit(EXIT_CONFIG_REFUSED)


def spec_reader_group_or_exit(env: str, channel: str, instance: str, log: Logger) -> str:
    """`spec_reader_group` (`tb.<env>.<channel>.channels.<instance>`, inside the ACL-granted PREFIXED
    `tb.<env>.<channel>.channels.`), or one PHI-free `startup_refused` line and exit 2 for a bad id."""
    try:
        return spec_reader_group(env, channel, instance)
    except RuntimeConfigurationError as exc:
        _refuse(log, exc)
