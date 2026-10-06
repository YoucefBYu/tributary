"""`fake_fhir_context`: a `FhirContext` for testing a mapping step with no network and no Kafka.

Mirrors `tributary.sdk.fakes.FakeContext` -- same idea, extended with `.fhir` and `.terminology`, both
defaulted to the local (no-I/O) implementations so a step under test never needs a real FHIR server or
terminology service (invariant 7). `fhir` defaults to a deterministic `LocalFhir`
(`sequential_id_factory`) rather than the production `uuid_id_factory`, so a golden-file test's output
is reproducible without the test author having to remember to override it.
"""

from __future__ import annotations

from tributary.sdk.fakes import FakeContext

from .bundle import LocalFhir, sequential_id_factory
from .context import FhirContext, attach
from .terminology import LocalTerminology


def fake_fhir_context(
    *,
    base: FakeContext | None = None,
    fhir: LocalFhir | None = None,
    terminology: LocalTerminology | None = None,
) -> FhirContext:
    """Build a `FhirContext` backed by `FakeContext`, `LocalFhir` and `LocalTerminology`.

    Every argument is overridable so a test can inspect the base `FakeContext` afterwards (its
    `.attrs()`, `.usr_headers()`, `.log.records`, the same as `tributary.sdk.testing.run_step`) or
    supply a `LocalTerminology` preloaded with a synthetic code table.
    """
    return attach(
        base or FakeContext(),
        fhir=fhir if fhir is not None else LocalFhir(id_factory=sequential_id_factory()),
        terminology=terminology if terminology is not None else LocalTerminology(),
    )
