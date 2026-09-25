"""ORU R01 (unsolicited observation result) to a FHIR R4 transaction Bundle.

Registered as the `tributary.content_hl7_fhir.oru_r01` step (`pyproject.toml`). Like `adt.py`, this
expects `tributary.hl7v2.parse` to have already run and left the parsed message in `ctx.vars["hl7"]`.

**Mapping source.** Follows the public HL7 v2-to-FHIR Implementation Guide's ORU_R01-to-Bundle page
(https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/ORU_R01.html) and its OBR-to-DiagnosticReport
and OBX-to-Observation datatype-map pages, read for interoperability and re-implemented clean-room
(CLAUDE.md invariant 10).

**Resources produced, and the identifier each Bundle entry is conditional on** (channel-spec README
3.4.2):

* `Patient` -- same rule as `adt.py`: conditional on its own PID-3 identifier.
* `DiagnosticReport` -- conditional on OBR-3 (Filler Order Number) if present, else OBR-2 (Placer Order
  Number); a lab's own order identifier is exactly the "business identifier" ADR-001 6.4 asks for. If
  neither is present, falls back to `message_identifier(message.msg_id, discriminator="obr-<OBR-1>")`,
  the same message-scoped fallback `adt.py`'s `Encounter` uses, for a sender that omits both.
* `Observation` (one per `OBX`) -- these have no order-independent identifier of their own in v2, so
  each is keyed on `message_identifier(message.msg_id, discriminator=f"obx-{OBX-1}")` -- exactly the
  case that function's own docstring names as its motivating example (one identifier per repeating OBX,
  stable across a redelivery of the *same* message because OBX-1's Set ID does not change on retry).

This reference mapping supports one `OBR` per message (the common unsolicited single-order-result case)
and numeric (`NM`) and text (`ST`/`TX`) `OBX` value types; other v2 value types (`CE`, `SN`, ...) get a
best-effort `valueString`, not a typed error, because a step that DLQs a result for an unsupported value
type would be a worse failure mode than a slightly-underspecified `Observation.value[x]`.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.diagnosticreport import DiagnosticReport
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.quantity import Quantity
from fhir.resources.R4B.reference import Reference
from tributary.fhir import Bundle, BundleEntry, FhirContext, message_identifier
from tributary.hl7v2 import Hl7Message, Segment
from tributary.hl7v2.step import VARS_KEY
from tributary.sdk import Message, step

from .datatypes import cx_to_identifier, local_identifier_system, ts_to_fhir_datetime
from .errors import RequiredFieldMissingError
from .patient import map_patient

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


def _map_observation_value(obx: Segment) -> tuple[str, object]:
    """`(field_name, value)` for `Observation.value[x]`, from OBX-2 (Value Type) and OBX-5 (Value)."""
    value_type = obx.field(2)
    raw_value = obx.field(5)
    if value_type == "NM":
        try:
            # `Decimal`, not `float`: `Quantity.value` is typed (and FHIR-defined) as a decimal, and a
            # binary float would silently perturb a lab value's precision (e.g. `72` -> `71.99999...`
            # is exactly the kind of drift a clinical result must not pick up in transit).
            quantity_value = Decimal(raw_value)
        except InvalidOperation:
            return "valueString", raw_value
        unit = obx.field(6) or None
        quantity = Quantity(value=quantity_value)
        if unit:
            quantity.unit = unit
        return "valueQuantity", quantity
    return "valueString", raw_value


def _map_observation(obx: Segment, *, patient_ref: Reference) -> Observation:
    """One `OBX` -> one `Observation`. IG: "OBX to Observation" (build.fhir.org/ig/HL7/v2-to-fhir)."""
    code_text = obx.field(3)
    if not code_text:
        raise RequiredFieldMissingError("OBX.3", reason="no observation code to map Observation.code from")
    components = code_text.split("^")
    code, display, system_alias = [*components, "", "", ""][:3]
    coding = Coding(code=code)
    if display:
        coding.display = display
    # OBX-3.3 carries the *coding system alias* (v2 Table 0396, e.g. "LN" for LOINC), not a URI. Only
    # LOINC is translated here (the terminology system this reference set's fixtures use); an
    # unrecognised alias is kept off `Coding.system` rather than invented, per invariant 10 (no licensed
    # code system's URI is asserted here without the code actually being validated against one -- that
    # is `ctx.terminology`'s job, not this mapping's).
    if system_alias == "LN":
        coding.system = "http://loinc.org"

    observation = Observation(status="unknown", code=CodeableConcept(coding=[coding]), subject=patient_ref)

    status = _RESULT_STATUS_TO_OBSERVATION_STATUS.get(obx.field(11))
    if status is not None:
        observation.status = status

    field_name, value = _map_observation_value(obx)
    setattr(observation, field_name, value)

    reference_range = obx.field(7)
    if reference_range:
        from fhir.resources.R4B.observation import ObservationReferenceRange

        observation.referenceRange = [ObservationReferenceRange(text=reference_range)]

    return observation


@step(name="tributary.content_hl7_fhir.oru_r01")
def oru_r01_to_fhir(message: Message, ctx: FhirContext) -> Message:
    """Map an ORU R01 message (already parsed by `tributary.hl7v2.parse`) to a FHIR transaction `Bundle`.

    Raises `RequiredFieldMissingError` if PID-3 or an `OBX`'s value code (OBX-3) is absent, and lets
    `tributary.fhir.errors.BundleEntryError` propagate unchanged for any other Bundle-construction
    failure.
    """
    hl7 = ctx.vars.get(VARS_KEY)
    if not isinstance(hl7, Hl7Message):
        raise RequiredFieldMissingError(
            "MSH", reason="no parsed HL7 message in ctx.vars; run tributary.hl7v2.parse first"
        )

    patient = map_patient(hl7)
    patient_entry = ctx.fhir.upsert(patient)
    patient_ref = Reference(reference=patient_entry.fullUrl)
    entries: list[BundleEntry] = [patient_entry]

    obr = hl7.segment("OBR") if hl7.segment_count("OBR") else None

    observation_entries: list[BundleEntry] = []
    for obx in hl7.segments("OBX"):
        observation = _map_observation(obx, patient_ref=patient_ref)
        obx_identifier = message_identifier(message.msg_id, discriminator=f"obx-{obx.field(1)}")
        entry = ctx.fhir.upsert(observation, identifier=obx_identifier)
        observation_entries.append(entry)
    entries.extend(observation_entries)

    if obr is not None:
        report_identifier = _diagnostic_report_identifier(
            obr, msg_id=message.msg_id, fallback_system=local_identifier_system(hl7["MSH.4"])
        )
        code_text = obr.field(4)
        components = code_text.split("^") if code_text else []
        code, display = [*components, "", ""][:2]
        report = DiagnosticReport(
            status="final",
            code=CodeableConcept(coding=[Coding(code=code or "unknown", display=display or None)]),
            subject=patient_ref,
        )
        effective = ts_to_fhir_datetime(obr.field(7))
        if effective is not None:
            # Same `fhir.resources`/mypy friction as `patient.py`'s `birthDate`: the FHIR wire-format
            # `str` is what the validator actually accepts.
            report.effectiveDateTime = effective  # type: ignore[assignment]
        if observation_entries:
            report.result = [Reference(reference=entry.fullUrl) for entry in observation_entries]
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
    filler = cx_to_identifier(obr.field(3), fallback_system=fallback_system)
    if filler is not None:
        return filler
    placer = cx_to_identifier(obr.field(2), fallback_system=fallback_system)
    if placer is not None:
        return placer
    return message_identifier(msg_id, discriminator=f"obr-{obr.field(1)}")
