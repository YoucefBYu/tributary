"""`context_extension`: `ctx.fhir` and `ctx.terminology` for steps running in a Tributary worker.

The `tributary.context` entry point (`tributary.runtime.extensions`) names this factory. The worker
calls it once at startup and then calls the returned function once per message branch, before the
first step, so every step on that message shares one `ctx.fhir` (`attach`'s docstring). The runtime
never imports this package: the W2-2 approval made attachment optional, not a hard import.

`attach()` keeps `reply` when the worker's context is a `ReplyContext` (215bec3), which the runtime's
start-up probe demands. `fullUrl`s are stable per message *and* per entry (`stable_entries`): the worker
reuses one `ctx.fhir` across in-place step retries, so a plain counter would drift on a retry after a
partial attempt; a destination fork or a redelivery gets a new context and restarts at the same start.

Production defaults, both with no I/O: `LocalFhir` seeded from `tb-msg-id` (W2-2 open item 3, so a
redelivery or a reprocess produces the same fullUrls) and an empty `LocalTerminology`. A lookup
against an unloaded system raises `TerminologyConfigError` rather than guessing. Loading a
deployment's code tables happens here later, at startup, not per message.
"""

from __future__ import annotations

from collections.abc import Callable

from tributary.sdk import Context, Message

from .bundle import LocalFhir, message_id_factory
from .context import FhirContext, attach
from .terminology import LocalTerminology


def context_extension() -> Callable[[Context, Message], FhirContext]:
    terminology = LocalTerminology()

    def extend(base: Context, message: Message) -> FhirContext:
        return attach(
            base,
            fhir=LocalFhir(id_factory=message_id_factory(message.msg_id), stable_entries=True),
            terminology=terminology,
        )

    return extend
