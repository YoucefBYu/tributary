"""The ordering guard (invariant 5) as a pure function."""

from __future__ import annotations

from tributary.archiver.ordering import ChannelTraits, order_safe


def test_none_is_allowed_per_key_and_unknown_are_not_unless_the_caller_says_so() -> None:
    lookup = {"free": ChannelTraits("none"), "keyed": ChannelTraits("per-key")}.get
    assert order_safe(lookup, channel="free", allow_reorder=False)
    assert not order_safe(lookup, channel="keyed", allow_reorder=False)
    assert not order_safe(lookup, channel="unknown", allow_reorder=False), "unknown fails closed"
    assert order_safe(lookup, channel="keyed", allow_reorder=True)
    assert order_safe(lookup, channel="unknown", allow_reorder=True)
