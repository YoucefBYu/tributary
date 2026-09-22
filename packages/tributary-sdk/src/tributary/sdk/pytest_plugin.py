"""Pytest fixtures for channel bundles: `run_step`, `run_pipeline`, `make_message`, `fake_ctx`.

Registered through the `pytest11` entry point, so a bundle that depends on `tributary-sdk` gets the
fixtures from ADR-001 6.2 without a conftest:

    def test_a01_is_kept(run_step, make_message):
        assert not isinstance(run_step(only_admits, make_message(b"...")), Drop)
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from .fakes import FakeContext
from .testing import make_message as _make_message
from .testing import run_pipeline as _run_pipeline
from .testing import run_step as _run_step


@pytest.fixture
def fake_ctx() -> FakeContext:
    """A fresh in-memory `ctx`. Inspect `fake_ctx.attrs()`, `.usr_headers()` and `.log.records`."""
    return FakeContext()


@pytest.fixture
def make_message() -> Callable[..., object]:
    """Builds a synthetic contract-shaped `Message`."""
    return _make_message


@pytest.fixture
def run_step(fake_ctx: FakeContext) -> Callable[..., object]:
    """Runs one step, defaulting to the session's `fake_ctx` so assertions can read what it recorded."""

    def run(step: object, message: object, **kwargs: object) -> object:
        kwargs.setdefault("ctx", fake_ctx)
        return _run_step(step, message, **kwargs)  # type: ignore[arg-type]

    return run


@pytest.fixture
def run_pipeline(fake_ctx: FakeContext) -> Callable[..., object]:
    """Runs a list of steps in order, stopping at the first `Drop`."""

    def run(steps: object, message: object, **kwargs: object) -> object:
        kwargs.setdefault("ctx", fake_ctx)
        return _run_pipeline(steps, message, **kwargs)  # type: ignore[arg-type]

    return run
