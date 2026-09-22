"""Message, @step, ctx interfaces and test helpers.

The public surface a channel bundle imports:

    from tributary.sdk import step, Drop, Message

Everything a step touches outside itself goes through `ctx` (`tributary.sdk.context`), which is a set
of protocols with in-memory fakes in `tributary.sdk.fakes`, so a step is unit-testable with no Kafka
and no network (invariant 7). Test helpers live in `tributary.sdk.testing`.

This package has no third-party dependencies and never opens a socket.
"""

from __future__ import annotations

from .context import Context, KeyValueStore, Logger, SecretStore
from .errors import (
    AttrError,
    DropReasonError,
    EnvelopeError,
    HeaderError,
    HeaderValueError,
    KeyValueConflictError,
    LogFieldError,
    MissingHeaderError,
    PayloadError,
    SecretNotFoundError,
    StepDefinitionError,
    StepError,
    StepReturnError,
    TributaryError,
    UsrHeaderError,
)
from .message import Message
from .outcome import Drop
from .step import Step, StepResult, step

__all__ = [
    "AttrError",
    "Context",
    "Drop",
    "DropReasonError",
    "EnvelopeError",
    "HeaderError",
    "HeaderValueError",
    "KeyValueConflictError",
    "KeyValueStore",
    "LogFieldError",
    "Logger",
    "Message",
    "MissingHeaderError",
    "PayloadError",
    "SecretNotFoundError",
    "SecretStore",
    "Step",
    "StepDefinitionError",
    "StepError",
    "StepResult",
    "StepReturnError",
    "TributaryError",
    "UsrHeaderError",
    "step",
]
