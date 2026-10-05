"""Fail a hung test fast instead of burning CI time (same layer as `tributary-loaders/tests/conftest.py`)."""

from __future__ import annotations

import faulthandler
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _hang_guard() -> Iterator[None]:
    faulthandler.dump_traceback_later(300, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()
