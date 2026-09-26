"""ORU R01 (unsolicited observation result) to a FHIR R4 transaction Bundle.

Registered as the `tributary.content_hl7_fhir.oru_r01` step (`pyproject.toml`). Like `adt.py`, this
expects `tributary.hl7v2.parse` to have already run and left the parsed message in `ctx.vars["hl7"]`.

**Mapping source.** Follows the public HL7 v2-to-FHIR Implementation Guide's ORU_R01-to-Bundle page
(https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/ORU_R01.html) and its OBR-to-DiagnosticReport
and OBX-to-Observation datatype-map pages, read for interoperability and re-implemented clean-room
(CLAUDE.md invariant 10).

**Resources produced, and the identifier each Bundle entry is conditional on** (channel-spec README
3.4.2). Every resource body also carries the identifier its entry is conditional on in its own
`.identifier` (security review MEDIUM 4), so a redelivery's conditional search actually matches what a
previous delivery wrote:

* `Patient` -- same rule as `adt.py`: conditional on its own PID-3 identifier. **State resource, `PUT`.**
* `DiagnosticReport` -- conditional on OBR-3 (Filler Order Number) if present, else OBR-2 (Placer Order
  Number); a lab's own order identifier is exactly the "business identifier" ADR-001 6.4 asks for. If
  neither is present, falls back to `message_identifier(message.msg_id, discriminator="obr-<OBR-1>")`,
  the same message-scoped fallback `adt.py`'s `Encounter` uses, for a sender that omits both. **Treated
  as a state resource, `PUT`** (channel-spec README 3.4.2's state/event split): a corrected or amended
  result for the *same* order should update the one `DiagnosticReport`, not sit next to a duplicate --
  the same convergence reasoning as `Encounter`.
* `Observation` (one per `OBX`) -- these have no order-independent identifier of their own in v2, so
  each is keyed on `message_identifier(message.msg_id, discriminator=f"obx-{OBX-1}")` -- exactly the
  case that function's own docstring names as its motivating example (one identifier per repeating OBX,
  stable across a redelivery of the *same* message because OBX-1's Set ID does not change on retry).
  **Treated as an event resource, `POST` + `ifNoneExist`** (channel-spec README 3.4.2): an `Observation`
  is a record of one result, not something a later message should overwrite in place, and 3.4.2 is
  explicit that event resources "must not be overwritten". `OBX-1` (Set ID) must be present and unique
  within the message -- a blank or repeated Set ID would give two `Observation`s the *same* conditional
  URL inside one transaction Bundle, which is refused with a typed `InvalidFieldError` rather than
  silently producing a Bundle a FHIR server would reject (or worse, collapse into one entry).

This reference mapping supports one `OBR` per message (the common unsolicited single-order-result case)
and numeric (`NM`) and text (`ST`/`TX`) `OBX` value types; other v2 value types (`CE`, `SN`, ...) get a
best-effort `valueString`, not a typed error, because a step that DLQs a result for an unsupported value
type would be a worse failure mode than a slightly-underspecified `Observation.value[x]`. A numeric value
whose exact decimal precision cannot survive `fhir_core`'s `Decimal`-through-`float` JSON encoding (see
`datatypes.decimal_preserves_precision`) is carried as `valueString` instead of a lossy `valueQuantity` --
documented, not silent (security review MEDIUM 4).
"""

from __future__ import annotations

from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.diagnosticreport import DiagnosticReport
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.quantity import Quantity
from fhir.resources.R4B.reference import Reference
from tributary.fhir import Bundle, BundleEntry, FhirContext, message_identifier
from tributary.hl7v2 import Hl7Message, Segment
from tributary.hl7v2.message import STRUCTURAL_CODE
from tributary.hl7v2.step import VARS_KEY
from tributary.sdk import Message, step

from .datatypes import (
    bounded_decimal,
    cwe_components,
    cx_identifier,
    decimal_preserves_precision,
    local_identifier_system,
    sanitize_fhir_code,
    ts_to_fhir_datetime,
)
from .errors import InvalidFieldError, RequiredFieldMissingError, UnsupportedTriggerError, safe_build
from .patient import map_patient

#: A fixed, non-sender-controlled placeholder for `UnsupportedTriggerError` when MSH-9's own text fails
#: `STRUCTURAL_CODE` (security review, MEDIUM 2 -- see `adt.py`'s identical constant).
_NON_STRUCTURAL = "<non-structural>"

#: v2 OBX-11 (Observation Result Status) -> FHIR `Observation.status`. Per the v2-to-FHIR IG's published
#: ConceptMap for this field; a code not listed (rare in practice) falls back to `unknown` rather than
#: guessing `final`, since `unknown` is a legal member of the R4 required binding and `final` is not a
#: safe default for a status this mapping could not actually confirm.
_RESULT_STATUS_TO_OBSERVATION_STATUS = {
    "F": "final",
    "P": "preliminary",
    "C": "corrected",
    "X": "cancelled",
    "R": "preliminary",
}


def _safe_trigger_codes(message_type: str, trigger_event: str) -> tuple[str, str]:
    safe_type = message_type if STRUCTURAL_CODE.match(message_type) else _NON_STRUCTURAL
    safe_trigger = trigger_event if STRUCTURAL_CODE.match(trigger_event) else _NON_STRUCTURAL
    return safe_type, safe_trigger


def _map_observation_value(obx: Segment) -> tuple[str, object]:
    """`(field_name, value)` for `Observation.value[x]`, from OBX-2 (Value Type) and OBX-5 (Value).

    `Decimal`, not `float`: `Quantity.value` is typed (and FHIR-defined) as a decimal, and a binary
    float would silently perturb a lab value's precision. `bounded_decimal` additionally caps the digit
    count and exponent magnitude before this ever touches a FHIR model or its JSON serializer (security
    review, HIGH 1: an unbounded exponent is unbounded CPU and memory at serialization time), and
    `decimal_preserves_precision` falls back to `valueString` rather than let `fhir_core`'s float-based
    JSON encoding silently drop significant digits (security review, MEDIUM 4).
    """
    value_type = obx.field(2)
    raw_value = obx.field(5)
    if value_type == "NM":
        value = bounded_decimal(raw_value)
        if value is not None and decimal_preserves_precision(value):
            unit = obx.field(6) or None

            def build_quantity() -> Quantity:
                quantity = Quantity(value=value)
                if unit:
                    quantity.unit = unit
                return quantity

            return "valueQuantity", safe_build(build_quantity, path="OBX.5", reason="invalid numeric value")
    return "valueString", raw_value


def _map_observation(obx: Segment, *, patient_ref: Reference, identifier: Identifier) -> Observation:
    """One `OBX` -> one `Observation`. IG: "OBX to Observation" (build.fhir.org/ig/HL7/v2-to-fhir)."""
    code, display, system_alias = cwe_components(obx, 3)
    if not code and not display:
        raise RequiredFieldMissingError("OBX.3", reason="no observation code to map Observation.code from")

    def build() -> Observation:
        sanitized_code = sanitize_fhir_code(code) if code else None
        coding = Coding(code=sanitized_code) if sanitized_code else Coding()
        if display:
            coding.display = display
        # OBX-3.3 carries the *coding system alias* (v2 Table 0396, e.g. "LN" for LOINC), not a URI.
        # Only LOINC is translated here (the terminology system this reference set's fixtures use); an
        # unrecognised alias is kept off `Coding.system` rather than invented, per invariant 10 (no
        # licensed code system's URI is asserted here without the code actually being validated against
        # one -- that is `ctx.terminology`'s job, not this mapping's).
        if system_alias == "LN":
            coding.system = "http://loinc.org"

        observation = Observation(
            status="unknown", code=CodeableConcept(coding=[coding]), subject=patient_ref, identifier=[identifier]
        )

        status = _RESULT_STATUS_TO_OBSERVATION_STATUS.get(obx.field(11).strip())
        if status is not None:
            observation.status = status

        field_name, value = _map_observation_value(obx)
        setattr(observation, field_name, value)

        reference_range = obx.field(7)
        if reference_range:
            from fhir.resources.R4B.observation import ObservationReferenceRange

            observation.referenceRange = [ObservationReferenceRange(text=reference_range)]

        return observation

    return safe_build(build, path="OBX", reason="could not build a valid Observation resource")


@step(name="tributary.content_hl7_fhir.oru_r01")
def oru_r01_to_fhir(message: Message, ctx: FhirContext) -> Message:
    """Map an ORU R01 message (already parsed by `tributary.hl7v2.parse`) to a FHIR transaction `Bundle`.

    Raises `UnsupportedTriggerError` if MSH-9 is not `ORU^R01` (security review, MEDIUM 2), `Required
    FieldMissingError` if PID-3, an `OBX`'s value code (OBX-3) or an `OBX`'s own Set ID (OBX-1) is
    absent, `InvalidFieldError` if an `OBX`'s Set ID repeats within the message (it would otherwise give
    two `Observation` entries the same conditional URL in one transaction Bundle), and lets `tributary.
    fhir.errors.BundleEntryError` propagate unchanged for any other Bundle-construction failure.
    """
    hl7 = ctx.vars.get(VARS_KEY)
    if not isinstance(hl7, Hl7Message):
        raise RequiredFieldMissingError(
            "MSH", reason="no parsed HL7 message in ctx.vars; run tributary.hl7v2.parse first"
        )

    msg_type, trigger_event, _structure = hl7.message_type()
    if msg_type != "ORU" or trigger_event != "R01":
        raise UnsupportedTriggerError(*_safe_trigger_codes(msg_type, trigger_event))

    patient = map_patient(hl7)
    patient_entry = ctx.fhir.upsert(patient)
    patient_ref = Reference(reference=patient_entry.fullUrl)
    entries: list[BundleEntry] = [patient_entry]

    obr = hl7.segment("OBR") if hl7.segment_count("OBR") else None

    observation_entries: list[BundleEntry] = []
    seen_obx_set_ids: set[str] = set()
    for obx in hl7.segments("OBX"):
        obx_set_id = obx.field(1)
        if not obx_set_id:
            raise RequiredFieldMissingError("OBX.1", reason="no Set ID to key the Observation entry on")
        if obx_set_id in seen_obx_set_ids:
            raise InvalidFieldError("OBX.1", reason="Set ID repeats within the message; each OBX needs a unique one")
        seen_obx_set_ids.add(obx_set_id)

        obx_identifier = message_identifier(message.msg_id, discriminator=f"obx-{obx_set_id}")
        observation = _map_observation(obx, patient_ref=patient_ref, identifier=obx_identifier)
        # Event resource: POST + ifNoneExist, never PUT (channel-spec README 3.4.2, security review
        # MEDIUM 4) -- an Observation must not be overwritten by a later message.
        entry = ctx.fhir.create(observation, identifier=obx_identifier)
        observation_entries.append(entry)
    entries.extend(observation_entries)

    if obr is not None:
        report_identifier = _diagnostic_report_identifier(
            obr, msg_id=message.msg_id, fallback_system=local_identifier_system(hl7["MSH.4"])
        )
        code, display, _system_alias = cwe_components(obr, 4)

        def build_report() -> DiagnosticReport:
            sanitized_code = sanitize_fhir_code(code) if code else None
            coding = Coding(code=sanitized_code or "unknown")
            if display:
                coding.display = display
            report = DiagnosticReport(
                status="final",
                code=CodeableConcept(coding=[coding]),
                subject=patient_ref,
                identifier=[report_identifier],
            )
            effective = ts_to_fhir_datetime(obr.field(7))
            if effective is not None:
                # Same `fhir.resources`/mypy friction as `patient.py`'s `birthDate`: the FHIR
                # wire-format `str` is what the validator actually accepts.
                report.effectiveDateTime = effective  # type: ignore[assignment]
            if observation_entries:
                report.result = [Reference(reference=entry.fullUrl) for entry in observation_entries]
            return report

        report = safe_build(build_report, path="OBR", reason="could not build a valid DiagnosticReport resource")
        # State resource: PUT, conditional on the same identifier the body now carries (security review
        # MEDIUM 4) -- see the module docstring for why DiagnosticReport is treated as state, not event.
        entries.append(ctx.fhir.upsert(report, identifier=report_identifier))

    bundle = Bundle(type="transaction", entry=entries)
    ctx.log.debug("oru_mapped", entry_count=len(entries), observation_count=len(observation_entries))
    return message.with_payload(
        bundle.model_dump_json(exclude_none=True).encode("utf-8"), content_type="application/fhir+json"
    )


def _diagnostic_report_identifier(obr: Segment, *, msg_id: str, fallback_system: str) -> Identifier:
    """OBR-3 (Filler Order Number), else OBR-2 (Placer Order Number), else a message-scoped fallback.

    Neither OBR-2 nor OBR-3 in this reference set's fixtures carries its own CX assigning-authority
    component (component 4), so `fallback_system` (the sending facility, MSH-4) supplies one -- an order
    number is still a real business identifier from that lab even without an explicit HD in the field.
    """
    filler = cx_identifier(obr, 3, fallback_system=fallback_system)
    if filler is not None:
        return filler
    placer = cx_identifier(obr, 2, fallback_system=fallback_system)
    if placer is not None:
        return placer
    return message_identifier(msg_id, discriminator=f"obr-{obr.field(1)}")
