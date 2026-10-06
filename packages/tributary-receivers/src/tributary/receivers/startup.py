"""Startup refusals shared by the receiver entry points: one PHI-free `startup_refused` line, exit 2.

Same line shape as `tributary.runtime.engine_kafka.load_engine_kafka_or_exit` and the loaders'
`startup.py` (`error` = class name, `code` = stable token), and the same rule: an operator-supplied
value (a flag's argument, an instance id) is never echoed.
"""

from __future__ import annotations

from typing import NoReturn

from tributary.runtime.errors import RuntimeConfigurationError, safe_token
from tributary.runtime.topics import spec_reader_group
from tributary.sdk.context import Logger

EXIT_CONFIG_REFUSED = 2

#: The retired file-based spec input (invariant 3: the channel spec comes from `_tb.<env>.channels`).
CHANNEL_SPEC_FLAG_RETIRED = "CHANNEL_SPEC_FLAG_RETIRED"


def refuse(log: Logger, *, error: str, code: str, reason: str | None = None) -> NoReturn:
    fields: dict[str, str] = {"error": safe_token(error), "code": safe_token(code)}
    if reason is not None:
        fields["reason"] = reason  # a fixed sentence written in this package, never operator input
    log.error("startup_refused", **fields)
    raise SystemExit(EXIT_CONFIG_REFUSED)


def refuse_retired_channel_spec_flag(argv: list[str], log: Logger) -> None:
    """Exit 2 if `--channel-spec` (or `--channel-spec=<path>`) is present. Checked on the raw argv, before
    argparse, because argparse echoes an unknown flag's value to stderr."""
    if any(arg == "--channel-spec" or arg.startswith("--channel-spec=") for arg in argv):
        refuse(
            log,
            error="RetiredFlag",
            code=CHANNEL_SPEC_FLAG_RETIRED,
            reason="--channel-spec is retired: publish the spec to _tb.<env>.channels and pass --channel",
        )


def spec_reader_group_or_exit(env: str, channel: str, instance: str, log: Logger) -> str:
    """`spec_reader_group` (`tb.<env>.<channel>.channels.<instance>`, inside the ACL-granted PREFIXED
    `tb.<env>.<channel>.channels.`), or one `startup_refused` line and exit 2 for a bad id."""
    try:
        return spec_reader_group(env, channel, instance)
    except RuntimeConfigurationError as exc:
        refuse(log, error=type(exc).__name__, code=exc.code)
