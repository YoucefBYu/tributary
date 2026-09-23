"""Steps the engine itself provides, registered through the `tributary.steps` entry point.

A channel names one of these with `uses:` (`contracts/channel-spec/v0` README 3.3.1), which resolves in
this process and needs no bundle fetch — unlike `ref:`, which imports a dotted path from the channel's
own bundle and therefore waits on bundle-fetch (not in wave 1).

Nothing here parses a payload or reaches the network. These are the steps whose behaviour is a property
of the engine rather than of a channel, so they live with the runtime and are covered by its tests.
"""

from __future__ import annotations

from tributary.sdk import Message, step


@step
def passthrough(message: Message) -> Message:
    """Deliver the message unchanged: the identity step.

    Not only a test affordance. A pure-routing channel — receive, deliver, no transform — is a real
    integration pattern, and this is the natural default for a scaffolded channel, so it is worth being
    something the engine provides rather than something every bundle re-writes.

    It returns the message rather than `None`, although the SDK defines both as pass-through. An
    explicit return reads as identity to someone using this as their first example of a step, and it
    exercises the same Message-returning path a real step takes instead of the implicit one.
    """
    return message
