"""`spec.source.reply` as receiver settings (channel-spec README 3.2.6), and the refusals the receiver can hold.

The block's presence switches synchronous reply on. `timeoutMs`, `maxPending` and `body` apply to the next
request; presence itself needs a restart (`mllp/reload.py`), because it decides whether this process joins the
`replier` group. `after` has one legal value (`routed`) and `partitions` sizes the topic, which the receiver
never creates, so neither becomes a setting: a value other than `routed` is refused rather than ignored.

Two refusals live here, because the schema holds only the first of them (3.2.6, Refusals):

* `pipeline.transactional: true`: a reply produced inside the transaction is late by up to a batch, and one
  produced outside could answer `AA` for work that is then aborted.
* a splitter: one request would have N outcomes and there is no aggregator. The receiver sees only the spec, so
  it recognises the engine's own `tributary.core.split`; a splitter named by `ref` is known only once the bundle
  loads, which is the worker's refusal (and the sender then gets the truthful `AE` / `REPLY_TIMEOUT`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from ..errors import ConfigError

DEFAULT_TIMEOUT_MS = 10_000
DEFAULT_MAX_PENDING = 10_000
MIN_TIMEOUT_MS, MAX_TIMEOUT_MS = 1_000, 60_000
MAX_PENDING_LIMIT = 50_000

#: The engine's splitter (`tributary.steps` entry point). Anything else is only known once a bundle loads.
SPLITTER_STEPS = frozenset({"tributary.core.split"})

ReplyBody = Literal["ack", "step"]


@dataclass(frozen=True, slots=True)
class ReplySettings:
    timeout_s: float = DEFAULT_TIMEOUT_MS / 1000
    max_pending: int = DEFAULT_MAX_PENDING
    body: ReplyBody = "ack"

    def __post_init__(self) -> None:
        if not MIN_TIMEOUT_MS / 1000 <= self.timeout_s <= MAX_TIMEOUT_MS / 1000:
            raise ConfigError("source.reply.timeoutMs must be between 1000 and 60000")
        if isinstance(self.max_pending, bool) or not 1 <= self.max_pending <= MAX_PENDING_LIMIT:
            raise ConfigError("source.reply.maxPending must be an integer between 1 and 50000")
        if self.body not in ("ack", "step"):
            raise ConfigError("source.reply.body must be 'ack' or 'step'")


def reply_settings(
    source: Mapping[str, Any], *, channel: str, transactional: bool, step_uses: Iterable[str | None]
) -> ReplySettings | None:
    """The `ReplySettings` of a source, or `None` for an async channel. Raises `ConfigError` on a refusal."""
    block = source.get("reply")
    if block is None:
        return None
    if not isinstance(block, Mapping):
        raise ConfigError(f"channel {channel!r}: source.reply must be an object")
    if transactional:
        raise ConfigError(f"channel {channel!r}: source.reply is refused on a transactional pipeline")
    if any(uses in SPLITTER_STEPS for uses in step_uses):
        raise ConfigError(f"channel {channel!r}: source.reply is refused on a channel with a splitter")
    if block.get("after", "routed") != "routed":
        raise ConfigError(f"channel {channel!r}: source.reply.after supports only 'routed'")
    timeout_ms = block.get("timeoutMs", DEFAULT_TIMEOUT_MS)
    max_pending = block.get("maxPending", DEFAULT_MAX_PENDING)
    for name, value in (("timeoutMs", timeout_ms), ("maxPending", max_pending)):
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ConfigError(f"channel {channel!r}: source.reply.{name} must be a number")
    return ReplySettings(
        timeout_s=timeout_ms / 1000,
        max_pending=int(max_pending),
        body=block.get("body", "ack"),
    )
