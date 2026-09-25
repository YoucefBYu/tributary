"""The engine's built-in steps (channel-spec README 3.3.2): filter, enrich, validate and split.

Registered in the `tributary.steps` entry-point group under `tributary.core.*` (see `core_steps`), so a
spec names them with `uses:` and needs no bundle fetch. Each is an ordinary `@step`/`@splitter` with
keyword-only parameters, which makes it unit-testable with `run_step(..., with_=...)` exactly like a
channel's own steps, plus a bind-time check of the `with` shape as a whole.
"""

from __future__ import annotations

from .enrich import enrich
from .filter import filter_step
from .split import split
from .validate import validate

__all__ = ["enrich", "filter_step", "split", "validate"]
