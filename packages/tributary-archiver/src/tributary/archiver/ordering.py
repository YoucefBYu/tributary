"""Ordering guard for re-injection (invariant 5: never reorder silently).

A replay or reprocess appends OLD records to the channel's own `raw` topic (replay has no other target: a
re-publish into a different channel could move PHI to a weaker channel, so it is not offered; "shadow" in the
contracts means a second consumer group on the same `raw` topic, not a copy). On an `ordering: per-key` channel
the old records land after newer live records for the same key, so a consumer sees the key's history out of
order. That is refused with `ORDERING_WOULD_BREAK` unless the caller passes `allow_reorder=True` explicitly; the
flag is recorded in the audit `requested` event, so an operator's decision to reorder is on the record.

The library does not read channel specs. The caller (the API) supplies a `TraitsLookup`: for a channel name, its
`ChannelTraits` or `None` when unknown. Unknown is refused (fail closed), never assumed to be `none`.

* `ordering: none`: allowed (no per-key promise to break).
* `ordering: per-key`: refused unless `allow_reorder=True`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

ORDERING_WOULD_BREAK = "ORDERING_WOULD_BREAK"


@dataclass(frozen=True, slots=True)
class ChannelTraits:
    ordering: Literal["per-key", "none"]


TraitsLookup = Callable[[str], ChannelTraits | None]


def order_safe(lookup: TraitsLookup, *, channel: str, allow_reorder: bool) -> bool:
    """Whether re-injecting old records into `channel`'s `raw` keeps per-key order, or the caller accepted not to."""
    if allow_reorder:
        return True
    traits = lookup(channel)
    return traits is not None and traits.ordering == "none"
