"""Typed exceptions for `tributary-fhir`.

Same rule as every other package here (`tributary.sdk.errors`, `tributary.hl7v2.errors`): no PHI in an
exception message (invariant 6). Nothing below embeds a resource's field values or a patient identifier.
A resource *type* and a code-system URI are structural, not sender data, and are included where useful.
"""

from __future__ import annotations


class FhirError(Exception):
    """Base class for every error this package raises."""


class BundleEntryError(FhirError):
    """A `FhirInterface` call cannot build a valid Bundle entry.

    Carries the resource type only, never its fields -- a `Patient` with no identifier and no override
    is a modelling bug in the calling step, not evidence about the patient.
    """

    def __init__(self, resource_type: str, reason: str) -> None:
        super().__init__(f"cannot build a Bundle entry for {resource_type}: {reason}")
        self.resource_type = resource_type
        self.reason = reason


class TerminologyError(FhirError):
    """Base class for a terminology lookup that did not complete.

    Distinct from a completed lookup that returns `None` because the code genuinely is not in the
    system (`Terminology.lookup` protocol docstring): this class means the lookup itself failed, and
    review criterion 3 requires that distinction stay visible rather than collapsing into a default.
    """


class TerminologyUnavailableError(TerminologyError):
    """A remote-backed `Terminology` implementation's operation exceeded its timeout or the service is
    unreachable. Never caught inside this package: a step's `stepRetries`-then-DLQ handling owns it
    (ADR-003 D4), so swallowing it here would silently turn a lookup outage into a wrong answer."""

    def __init__(self, system: str) -> None:
        super().__init__(f"terminology system {system!r} is unavailable")
        self.system = system


class TerminologyConfigError(TerminologyError):
    """A `Terminology` implementation was asked about a code system it has no table loaded for."""

    def __init__(self, system: str) -> None:
        super().__init__(f"no code table loaded for terminology system {system!r}")
        self.system = system
