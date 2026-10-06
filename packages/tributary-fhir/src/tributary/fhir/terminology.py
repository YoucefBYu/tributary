"""`LocalTerminology`: an in-process, table-backed default for the `Terminology` protocol (`context.py`).

Architect review criterion 3: the terminology hot path defaults to local, versioned code tables, because
S1 left about 5 ms/message of budget and none of it was earmarked for a network round trip. This module
never opens a socket -- it looks a `(system, code)` pair up in a table supplied at construction time.

**No licensed code system ships here** (CLAUDE.md invariant 10 / this role's rules): `TABLE` starts
empty, and `load_table`/`LocalTerminology(table=...)` are how a deployment supplies its own LOINC,
SNOMED CT or ICD-10 data (licensed separately, mounted or packaged outside this repository). Tests and
golden files use a small synthetic table instead (`tests/fixtures/fhir` -- W2-3).

A **remote-backed** `Terminology` (a real LOINC/SNOMED terminology service) is a legitimate future
implementation of the same protocol, but it is not this module: it would need an explicit timeout and a
bounded cache (review criterion 3) and, being a network client, is a dependency a *channel bundle* opts
into, not one `tributary-fhir` or `tributary-runtime` carries by default (see `context.py`'s docstring
on attachment).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .errors import TerminologyConfigError


@dataclass(frozen=True, slots=True)
class CodeLookup:
    """One resolved code: its display text and, optionally, the system it was translated from."""

    system: str
    code: str
    display: str


class LocalTerminology:
    """A `Terminology` implementation backed by an in-memory table: `{(system, code): CodeLookup}`.

    Satisfies the `Terminology` protocol structurally (duck-typed, like every `ctx` member in this
    codebase) with no inheritance needed. Doubles as the test fake: it does no I/O, so a mapping step's
    test constructs one with a small synthetic table and gets exactly the "no network" guarantee
    invariant 7 asks for.
    """

    def __init__(
        self, table: Mapping[tuple[str, str], CodeLookup] | None = None, *, known_systems: Iterable[str] = ()
    ) -> None:
        self._table: dict[tuple[str, str], CodeLookup] = dict(table or {})
        self._known_systems: set[str] = {system for system, _ in self._table} | set(known_systems)

    def lookup(self, system: str, code: str) -> CodeLookup | None:
        """The code's `CodeLookup`, or `None` if the system is loaded but the code is not in it.

        Raises `TerminologyConfigError` -- not `None` -- when `system` itself was never loaded: those
        are different failures (criterion 3), and collapsing them would make a typo in a system URI look
        identical to a code that is legitimately absent from a real value set.
        """
        if system not in self._known_systems:
            raise TerminologyConfigError(system)
        return self._table.get((system, code))

    def validate(self, system: str, code: str) -> bool:
        return self.lookup(system, code) is not None

    def systems(self) -> frozenset[str]:
        return frozenset(self._known_systems)


def load_table(entries: Iterable[CodeLookup]) -> dict[tuple[str, str], CodeLookup]:
    """Build a `LocalTerminology` table from an iterable of `CodeLookup`, keyed by `(system, code)`."""
    return {(entry.system, entry.code): entry for entry in entries}
