"""Startup plumbing shared by every data-plane entry point: the instance id rule, a quiet argument parser and
the one-line refusal.

**The instance id is required (W3-28, `topics/v0` 3.3 as restated by W3-29).** A built-in default such as
`pipeline-<pid>` is the same string in every container, because the pid is 1 in every container, so replicas
fence each other out of the consumer group (static membership: the newer member with a given
`group.instance.id` evicts the older one, which then rejoins and evicts the newer one). That is the defect set
1.7 fixed for receivers. So an entry point takes the id from `--instance-id` or `TB_INSTANCE_ID` (the flag
wins) and refuses to start without one: `startup_refused` with `code: INSTANCE_REQUIRED`, exit 2. The value is
used **verbatim** as `group.instance.id`, and it must be a valid instance segment of the spec-reader group
name (`topics.INSTANCE`), which is checked here so a bad value refuses before any Kafka client exists.

**Operator input is never echoed** (invariant 6 and the receivers' rule): argparse's own error text quotes the
offending flag value, so the parser here replaces it with one fixed `startup_refused` line.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from typing import Any, Final, NoReturn

from tributary.sdk.context import Logger

from .errors import RuntimeConfigurationError, safe_token
from .topics import INSTANCE

#: Exit status for a refused start (`observability/v0` O4, `engine-kafka/v0`): the supervisor must not
#: restart-loop on a configuration that cannot work, and an operator greps for it.
EXIT_REFUSED: Final = 2

INSTANCE_ENV: Final = "TB_INSTANCE_ID"
INSTANCE_FLAG: Final = "--instance-id"


class InstanceRequiredError(RuntimeConfigurationError):
    """No `--instance-id` and no `TB_INSTANCE_ID` (or an empty one)."""

    code = "INSTANCE_REQUIRED"


class InstanceInvalidError(RuntimeConfigurationError):
    """An instance id that cannot be a `group.instance.id` and group-name segment. The value is never quoted."""

    code = "ARGUMENTS_INVALID"


def resolve_instance_id(flag: str | None, environ: Mapping[str, str] | None = None) -> str:
    """The flag, else `TB_INSTANCE_ID`. An explicit empty flag is missing, not "fall back to the environment".

    Raises `InstanceRequiredError` when neither is set, `InstanceInvalidError` when the value does not match
    `topics.INSTANCE` (`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`). Neither message carries the value.
    """
    env = os.environ if environ is None else environ
    value = flag if flag is not None else env.get(INSTANCE_ENV)
    if not value:
        raise InstanceRequiredError(
            f"{INSTANCE_FLAG} or {INSTANCE_ENV} is required and must be unique per replica "
            "(a pid default is identical in every container)"
        )
    if INSTANCE.fullmatch(value) is None:
        raise InstanceInvalidError(f"{INSTANCE_FLAG} must match {INSTANCE.pattern}")
    return value


def refuse(log: Logger, *, error: str, code: str, reason: str | None = None) -> NoReturn:
    """One `startup_refused` line, then `SystemExit(2)`. `reason` must be a fixed sentence built from names."""
    fields: dict[str, object] = {"error": safe_token(error), "code": safe_token(code)}
    if reason is not None:
        fields["reason"] = reason
    try:
        log.error("startup_refused", **fields)
    except Exception:  # a guard refusing the reason must not turn exit 2 into a traceback
        fields.pop("reason", None)
        try:
            log.error("startup_refused", **fields)
        except Exception:  # the stream itself is gone; the exit status still says it
            sys.stderr.write('{"event":"startup_refused","level":"error"}\n')
    raise SystemExit(EXIT_REFUSED)


def refuse_configuration(log: Logger, exc: RuntimeConfigurationError) -> NoReturn:
    """`refuse` for one of this module's errors, whose messages are fixed sentences without operator input."""
    reason = str(exc) if isinstance(exc, InstanceRequiredError | InstanceInvalidError) else None
    refuse(log, error=type(exc).__name__, code=exc.code, reason=reason)


class QuietArgumentParser(argparse.ArgumentParser):
    """argparse echoes an unknown flag or a bad value to stderr. Here: one `startup_refused` line, exit 2."""

    def __init__(self, log: Logger, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._tb_log = log

    def error(self, message: str) -> NoReturn:
        del message  # quotes operator input
        refuse(
            self._tb_log,
            error="UsageError",
            code="ARGUMENTS_INVALID",
            reason="invalid command-line arguments; run with --help for the accepted flags",
        )


def add_instance_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        INSTANCE_FLAG,
        dest="instance_id",
        default=None,
        help=f"unique per replica, used verbatim as group.instance.id (required; or {INSTANCE_ENV})",
    )
