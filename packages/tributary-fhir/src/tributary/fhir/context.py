"""`FhirContext`: `tributary.sdk.Context` extended with `.fhir` and `.terminology`.

Deliberately absent from `tributary.sdk.context.Context` -- see that module's docstring: the SDK ships
no FHIR or terminology code, so a step that maps to FHIR imports this instead and annotates its `ctx`
parameter with `FhirContext` rather than the bare SDK `Context`:

    from tributary.fhir import FhirContext

    @step
    def to_fhir_bundle(msg: Message, ctx: FhirContext) -> Message: ...

`@step`'s signature check (`tributary.sdk.step._takes_context`) only counts positional parameters, never
their annotation, so this needs no SDK change -- the runtime hands the step whatever object it has, and
Python's structural typing (`Protocol`, `runtime_checkable`) means it only has to have `.fhir` and
`.terminology`, not literally be this class.

**Attachment**, addressing architect review criterion 1. `tributary-runtime` must not gain a
network-client dependency just to run an HL7-to-FHIR channel. Both defaults are local:
`LocalFhir` (`bundle.py`) only builds `BundleEntry` objects, and `LocalTerminology` (`terminology.py`)
looks codes up in an in-process table -- neither imports an HTTP client, and neither is in this
package's dependency list (`pyproject.toml`: `fhir.resources` and `tributary-sdk` only). That means
`tributary-runtime` depending on `tributary-fhir` to call `attach()` does **not** acquire network
capability transitively; the actual choice to do so is platform-engineer's, not decided here. A bundle
that wants a *remote*-backed `Terminology` (criterion 3: an explicit timeout and a bounded cache, never
silent) supplies its own implementation of the `Terminology` protocol below and calls `attach()` itself
-- that dependency is the bundle's alone.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from datetime import datetime
from typing import Protocol, runtime_checkable

from tributary.sdk import Context, KeyValueStore, Logger, SecretStore

from .bundle import BundleEntry, Identifier, Resource
from .terminology import CodeLookup


@runtime_checkable
class FhirInterface(Protocol):
    """Local Bundle-entry construction for a mapping step building a transaction Bundle.

    No network I/O (architect review criterion 4): a remote FHIR read or search is a loader's job at the
    destination stage (`tributary-loaders`' FHIR REST loader, W2-6), not a mapping step's. Every entry
    this produces is conditional, matching ADR-001 6.4's FHIR idempotency row ("conditional
    create/update, `If-None-Exist`, PUT with business identifier"), so a duplicate delivery of the same
    message is harmless (invariant 4).
    """

    def upsert(self, resource: Resource, *, identifier: Identifier | None = None) -> BundleEntry: ...

    def create(self, resource: Resource, *, identifier: Identifier | None = None) -> BundleEntry: ...

    def delete(self, resource_type: str, *, identifier: Identifier) -> BundleEntry: ...


@runtime_checkable
class Terminology(Protocol):
    """Code-system lookups (LOINC, SNOMED CT, ICD-10), synchronous like the rest of `ctx`.

    `lookup` returns `None` only when the lookup **completed** and the code genuinely was not found in a
    *loaded* system. Anything that stops the lookup from completing -- a remote timeout, an unloaded code
    system, a malformed table -- raises a `tributary.fhir.errors.TerminologyError` subclass instead, so a
    step's `stepRetries`-then-DLQ handling (ADR-003 D4) owns it. An implementation that silently returns
    a default or placeholder code instead of raising breaks this contract (architect review criterion 3):
    a wrong-but-present code is worse than a loud failure, because nothing downstream can tell them apart.
    """

    def lookup(self, system: str, code: str) -> CodeLookup | None: ...

    def validate(self, system: str, code: str) -> bool: ...


@runtime_checkable
class FhirContext(Context, Protocol):
    """`Context` plus `.fhir` and `.terminology`. See the module docstring for how one gets attached."""

    @property
    def fhir(self) -> FhirInterface: ...

    @property
    def terminology(self) -> Terminology: ...


class _Attached:
    """`Context` plus `.fhir`/`.terminology`, delegating every base member **explicitly**.

    Not `__getattr__`-based delegation: mypy does not treat `__getattr__` as satisfying a `Protocol`
    (checked against this repo's `mypy --strict` directly), and this class has to type-check as
    `FhirContext` wherever a step's `ctx: FhirContext` parameter is called with one. Not exported --
    use `attach()`, which returns this typed as `FhirContext`.
    """

    def __init__(self, base: Context, fhir: FhirInterface, terminology: Terminology) -> None:
        self._base = base
        self.fhir = fhir
        self.terminology = terminology

    @property
    def channel(self) -> str:
        return self._base.channel

    @property
    def channel_version(self) -> int:
        return self._base.channel_version

    @property
    def instance(self) -> str:
        return self._base.instance

    @property
    def destination(self) -> str | None:
        return self._base.destination

    @property
    def attempt(self) -> int:
        return self._base.attempt

    @property
    def traceparent(self) -> str | None:
        return self._base.traceparent

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._base.vars

    @property
    def log(self) -> Logger:
        return self._base.log

    @property
    def secrets(self) -> SecretStore:
        return self._base.secrets

    @property
    def kv(self) -> KeyValueStore:
        return self._base.kv

    def now(self) -> datetime:
        return self._base.now()

    def set_attr(self, name: str, value: str) -> None:
        self._base.set_attr(name, value)

    def attrs(self) -> Mapping[str, str]:
        return self._base.attrs()

    def set_usr(self, name: str, value: str) -> None:
        self._base.set_usr(name, value)

    def usr_headers(self) -> Mapping[str, str]:
        return self._base.usr_headers()


def attach(base: Context, *, fhir: FhirInterface, terminology: Terminology) -> FhirContext:
    """Compose a base `Context` with `.fhir` and `.terminology`. Pure Python, no I/O of its own.

    One `ctx` spans a whole pipeline (`tributary.sdk.testing.run_pipeline`'s docstring) -- one message's
    trip through its steps, not every message a channel ever sees -- so this is meant to be called once
    per message, before that message's pipeline runs, and the same `FhirContext` passed to every step.

    **Production `fhir=` should be `LocalFhir(id_factory=message_id_factory(message.msg_id))`** (W2-2
    open item 3, `bundle.py`), not `LocalFhir()`'s `uuid_id_factory` default: calling `attach` once per
    message is exactly what makes a message-derived `fullUrl` possible, since the id factory closes over
    that message's `tb-msg-id`. Wiring this call site into the running engine is `tributary-runtime`'s
    (ADR-001 6.2); `tributary.fhir.fakes.fake_fhir_context` already does the test-time equivalent with
    `sequential_id_factory`.
    """
    return _Attached(base, fhir, terminology)
