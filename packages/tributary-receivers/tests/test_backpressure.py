"""`ByteBudget` (gate requirement 5): a memory bound in bytes, not items."""

from __future__ import annotations

import asyncio

import pytest
from tributary.receivers.backpressure import BudgetExceeded, ByteBudget


def test_reserve_within_budget_succeeds() -> None:
    async def go() -> bool:
        budget = ByteBudget(max_bytes=100)
        return await budget.reserve(50, timeout=1)

    assert asyncio.run(go()) is True


def test_reserve_over_budget_times_out_and_charges_nothing() -> None:
    async def go() -> tuple[bool, int]:
        budget = ByteBudget(max_bytes=100)
        await budget.reserve(90, timeout=1)
        ok = await budget.reserve(50, timeout=0.05)
        return ok, budget.used_bytes

    ok, used = asyncio.run(go())
    assert ok is False
    assert used == 90  # the failed reservation charged nothing


def test_release_frees_room_for_a_waiting_reservation() -> None:
    async def go() -> bool:
        budget = ByteBudget(max_bytes=100)
        await budget.reserve(90, timeout=1)

        async def releaser() -> None:
            await asyncio.sleep(0.05)
            await budget.release(90)

        release_task = asyncio.ensure_future(releaser())
        ok = await budget.reserve(50, timeout=2)
        await release_task
        return ok

    assert asyncio.run(go()) is True


def test_max_messages_caps_item_count_even_under_byte_budget() -> None:
    async def go() -> bool:
        budget = ByteBudget(max_bytes=1_000_000, max_messages=1)
        await budget.reserve(10, timeout=1)
        return await budget.reserve(10, timeout=0.05)

    assert asyncio.run(go()) is False


def test_a_single_reservation_bigger_than_the_budget_raises() -> None:
    async def go() -> None:
        budget = ByteBudget(max_bytes=100)
        await budget.reserve(101, timeout=1)

    with pytest.raises(BudgetExceeded):
        asyncio.run(go())


def test_release_never_goes_negative() -> None:
    async def go() -> int:
        budget = ByteBudget(max_bytes=100)
        await budget.release(10)  # release without a matching reserve: must not underflow
        return budget.used_bytes

    assert asyncio.run(go()) == 0
