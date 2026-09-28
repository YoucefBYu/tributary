"""Fail a hung test fast instead of burning hours of CI time.

Root cause this exists for: `postgresql/test_integration_postgresql_loader.py` hung intermittently
(CI head `433d99a`: one run of the file's first test sat for 6 hours until GitHub's own job cap
cancelled it; a sibling run of the identical commit passed in 8 minutes; a local combined run hung for
30+ minutes with only a Postgres container up). Root cause was two unbounded `asyncpg` calls in
`postgresql/loader.py` (`verify_idempotency_constraint`'s `fetchval` and `close`'s `pool.close()`), now
fixed with `asyncio.wait_for` there -- see that file's docstrings. This conftest is the second,
independent layer CLAUDE.md's "Test protocol edge cases" asks for: even a *new* unbounded call, here or
in a sibling loader package, should make one test fail in about two minutes, not eat a CI job for hours.

Deliberately not `pyproject.toml`'s `[tool.pytest.ini_options]` (shared with every other package): a
per-test-suite `faulthandler_timeout` belongs to the suite it protects, and touching the shared ini file
is a bigger blast radius than this needs. `_pytest.faulthandler` only reads its timeout from
`config.getini(...)`, which is fixed at ini-parse time before any conftest loads, so this wraps the
plugin's own (public-enough, single-function) timeout lookup instead of writing a second ini source --
an explicit `-o faulthandler_timeout=...`/`-o faulthandler_exit_on_timeout=...` on the command line still
wins, this is only the floor when neither is set.
"""

from __future__ import annotations

import _pytest.faulthandler as _faulthandler_plugin
from _pytest.config import Config

#: Generous above this package's slowest observed single test (the three-file combined integration
#: run's individual tests, ~40s each) but nowhere near "eats the CI job": a test still running this long
#: is hung, not slow.
_DEFAULT_TIMEOUT_S = 120.0

_original_get_timeout_config_value = _faulthandler_plugin.get_timeout_config_value


def _get_timeout_config_value(config: Config) -> float:
    configured = _original_get_timeout_config_value(config)
    return configured if configured > 0 else _DEFAULT_TIMEOUT_S


_faulthandler_plugin.get_timeout_config_value = _get_timeout_config_value

#: Dump every thread's traceback AND end the process on timeout (not just dump-and-keep-waiting): a
#: real deadlock in a blocking, non-cancellable call (a socket read with no timeout, `Pool.close()`
#: before its own fix in `postgresql/loader.py`) does not respond to `asyncio.CancelledError` at all, so
#: only exiting the process actually bounds the job. Both this plugin function and
#: `get_timeout_config_value` above read `config.getini(...)` directly at the moment each test starts
#: (`pytest_runtest_protocol`), which is fixed by argument-parsing time -- long before any conftest
#: loads -- so there is no `config.option`/`config.stash` hook left to set instead of wrapping the
#: function itself.
_original_get_exit_on_timeout_config_value = _faulthandler_plugin.get_exit_on_timeout_config_value


def _get_exit_on_timeout_config_value(config: Config) -> bool:
    if config.getini("faulthandler_exit_on_timeout"):
        return _original_get_exit_on_timeout_config_value(config)
    return True


_faulthandler_plugin.get_exit_on_timeout_config_value = _get_exit_on_timeout_config_value
