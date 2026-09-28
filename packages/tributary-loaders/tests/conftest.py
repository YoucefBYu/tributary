"""Fail a hung test fast instead of burning hours of CI time.

Root cause this exists for: `postgresql/test_integration_postgresql_loader.py` hung intermittently
(CI head `433d99a`: one run of the file's first test sat for 6 hours until GitHub's own job cap
cancelled it; a sibling run of the identical commit passed in 8 minutes; a local combined run hung for
30+ minutes with only a Postgres container up). Root cause was three unbounded `asyncpg` calls in
`postgresql/client.py`/`loader.py` (`create_pool`, `verify_idempotency_constraint`'s `fetchval` and
`close`'s `pool.close()`), now all bounded with `asyncio.wait_for` -- see those modules' docstrings.
This conftest is the second, independent layer CLAUDE.md's "Test protocol edge cases" asks for: even a
*new* unbounded call, here or in a sibling loader package under this same `tests/` tree, should make one
test fail in about two minutes, not eat a CI job for hours.

**Not `[tool.pytest.ini_options].faulthandler_timeout`** (`pyproject.toml`, shared with every other
package): a per-test-suite timeout belongs to the suite it protects, not the shared root ini file.

**Not a `pytest_load_initial_conftests` hook, and not monkeypatching `_pytest.faulthandler`'s
functions**, both tried first and rejected:

* `pytest_load_initial_conftests(early_config, parser, args)` cannot inject `-o faulthandler_timeout=...`
  from here at all -- confirmed empirically (a scratch conftest with nothing but that hook and a
  print/file-write never runs it), not just by its own docstring's "This hook is not called for conftest
  files": by the time a *conftest's* copy of this hook could run, pytest has already decided the initial
  conftest set and moved on to option parsing that a later `-o` would need to land before.
* Monkeypatching `_pytest.faulthandler.get_timeout_config_value`/`get_exit_on_timeout_config_value`
  (an earlier version of this file) works today but is exactly the "a pytest upgrade could silently
  undo this" risk it looks like it is solving: those two functions are private, undocumented, and a
  future pytest could rename, inline or remove them with no deprecation warning, silently turning this
  file into a no-op.

Instead: `faulthandler` is a stable stdlib module, and `pytest_runtest_protocol` as a hookwrapper is
pytest's own public, documented mechanism for wrapping a test's execution (`_pytest.faulthandler` itself
is implemented this exact way -- see its source). This conftest runs its own
`faulthandler.dump_traceback_later`/`cancel_dump_traceback_later` pair around each test, entirely
independent of the ini system, and gets out of the way -- doing nothing at all -- the moment
`faulthandler_timeout` is configured through the normal ini/`-o` route, so an explicit
`-o faulthandler_timeout=...`/`-o faulthandler_exit_on_timeout=...` on the command line still wins and
is handled solely by pytest's own built-in plugin, not duplicated here.
"""

from __future__ import annotations

import faulthandler
import sys
from collections.abc import Generator

import pytest
from _pytest.config import Config
from _pytest.nodes import Item

#: Generous above this package's slowest observed single test (the three-file combined integration
#: run's individual tests, ~40s each) but nowhere near "eats the CI job": a test still running this long
#: is hung, not slow.
_DEFAULT_TIMEOUT_S = 120.0


def _ini_timeout_configured(config: Config) -> bool:
    return float(config.getini("faulthandler_timeout") or 0.0) > 0


@pytest.hookimpl(wrapper=True, trylast=True)
def pytest_runtest_protocol(item: Item) -> Generator[None, object, object]:
    if _ini_timeout_configured(item.config):
        # An explicit `-o faulthandler_timeout=...` (or an ini value) already asked for something --
        # pytest's own `_pytest.faulthandler` plugin implements this identical hook and will act on it;
        # do not layer a second, differently-configured timer on top of that one.
        return (yield)
    faulthandler.dump_traceback_later(_DEFAULT_TIMEOUT_S, file=sys.stderr, exit=True)
    try:
        return (yield)
    finally:
        faulthandler.cancel_dump_traceback_later()
