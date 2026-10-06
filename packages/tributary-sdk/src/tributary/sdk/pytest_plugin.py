"""Pytest fixtures for channel bundles: `run_step`, `run_splitter`, `run_pipeline`, `make_message`, `fake_ctx`.

Registered through the `pytest11` entry point, so a bundle that depends on `tributary-sdk` gets the
fixtures from ADR-001 6.2 without a conftest:

    def test_a01_is_kept(run_step, make_message):
        assert not isinstance(run_step(only_admits, make_message(b"...")), Drop)

One hook besides the fixtures: `pytest_configure` turns on strict logging (`TB_LOG_STRICT=1`), so a
refused log line fails the test instead of being written as `log_refused` (`tributary.runtime.logs`).
"""

from __future__ import annotations

import os
from collections.abc import Callable

import pytest

from .fakes import FakeContext
from .testing import make_message as _make_message
from .testing import run_pipeline as _run_pipeline
from .testing import run_splitter as _run_splitter
from .testing import run_step as _run_step

#: `tributary.runtime.logs.STRICT_ENV`, repeated here because the SDK cannot import the runtime.
LOG_STRICT_ENV = "TB_LOG_STRICT"


def pytest_configure(config: pytest.Config) -> None:
    """Test runs log strictly: a refused log line raises `LogFieldError` instead of becoming `log_refused`.

    In production `JsonLogger` never raises (a debugging line must not stop a worker loop or replace
    the exception being reported), so the refusal would be one counted line nobody reads. In a test it
    should fail the test. `setdefault`: a suite that sets `TB_LOG_STRICT=0` keeps the production path,
    and a test can always pass `strict=False` to exercise it.
    """
    os.environ.setdefault(LOG_STRICT_ENV, "1")


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
def run_splitter(fake_ctx: FakeContext) -> Callable[..., object]:
    """Runs one splitter and returns its stamped children, or a `Drop`."""

    def run(node: object, message: object, **kwargs: object) -> object:
        kwargs.setdefault("ctx", fake_ctx)
        return _run_splitter(node, message, **kwargs)  # type: ignore[arg-type]

    return run


@pytest.fixture
def run_pipeline(fake_ctx: FakeContext) -> Callable[..., object]:
    """Runs a list of steps in order, stopping at the first `Drop`."""

    def run(steps: object, message: object, **kwargs: object) -> object:
        kwargs.setdefault("ctx", fake_ctx)
        return _run_pipeline(steps, message, **kwargs)  # type: ignore[arg-type]

    return run
