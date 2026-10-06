"""FHIR helpers over pydantic v2 models (`fhir.resources.R4B`), and the `ctx.fhir`/`ctx.terminology`
interfaces a mapping step (`tributary-content-hl7-fhir`, W2-3) uses to build a transaction `Bundle`.

    from tributary.fhir import FhirContext, message_identifier

    @step
    def to_fhir_bundle(msg: Message, ctx: FhirContext) -> Message:
        patient = Patient(identifier=[Identifier(system="urn:synth:mrn", value=msg["PID.3.1"])], ...)
        entry = ctx.fhir.upsert(patient)
        ...

**Why R4B, not R4.** `fhir.resources` (BSD) dropped a standalone FHIR R4 package after its v7; R4 lives
on as the R4B sub-package (`fhir.resources.R4B`), a technical-correction superset of R4 rather than a
new edition. The public HL7-v2-to-FHIR Implementation Guide this package's mappings follow (W2-3) is
R4-based; R4B is the closest available typed model set, used throughout instead of hand-rolling R4
models. Every golden-file test's mapping-source comment (W2-3) notes this.

**`ctx.fhir` does no network I/O.** It only builds `BundleEntry` objects, conditional on a business
identifier (ADR-001 6.4's FHIR idempotency row). Talking to a real FHIR server is a loader's job at the
destination stage (`tributary-loaders`, W2-6), which keeps a mapping step fast and unit-testable with no
network (invariant 7) -- see `context.py` for the full design rationale reviewed by `architect`.

This package has no dependency capable of network I/O (`fhir.resources`, `tributary-sdk` only, per
`pyproject.toml`); `LocalTerminology`'s code tables are supplied by the deployment, never bundled here
(invariant 10: no licensed code system ships in this repository).
"""

from __future__ import annotations

from .bundle import (
    TB_MESSAGE_SYSTEM,
    Bundle,
    BundleEntry,
    BundleEntryRequest,
    Identifier,
    LocalFhir,
    Resource,
    message_id_factory,
    message_identifier,
    sequential_id_factory,
    uuid_id_factory,
)
from .context import FhirContext, FhirInterface, FhirReplyContext, Terminology, attach
from .errors import (
    BundleEntryError,
    FhirError,
    TerminologyConfigError,
    TerminologyError,
    TerminologyUnavailableError,
)
from .fakes import fake_fhir_context
from .terminology import CodeLookup, LocalTerminology, load_table

__all__ = [
    "TB_MESSAGE_SYSTEM",
    "Bundle",
    "BundleEntry",
    "BundleEntryError",
    "BundleEntryRequest",
    "CodeLookup",
    "FhirContext",
    "FhirError",
    "FhirInterface",
    "FhirReplyContext",
    "Identifier",
    "LocalFhir",
    "LocalTerminology",
    "Resource",
    "Terminology",
    "TerminologyConfigError",
    "TerminologyError",
    "TerminologyUnavailableError",
    "attach",
    "fake_fhir_context",
    "load_table",
    "message_id_factory",
    "message_identifier",
    "sequential_id_factory",
    "uuid_id_factory",
]
