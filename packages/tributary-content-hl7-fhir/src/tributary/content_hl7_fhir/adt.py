"""ADT A01 (admit), A03 (discharge) and A08 (update patient information) to a FHIR R4 transaction Bundle.

Registered as the `tributary.content_hl7_fhir.adt` step (`pyproject.toml`). Expects
`tributary.hl7v2.parse` to have already run in the same pipeline and left the parsed message in
`ctx.vars["hl7"]` (`tributary.hl7v2.step.VARS_KEY`) -- this step never re-parses the payload itself.

**Mapping source.** Segment-to-resource shape follows the public HL7 v2-to-FHIR Implementation Guide's
published ADT_A01/A03/A08-to-Bundle page (https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/
ADT_A01.html and its A03/A08 siblings), read for interoperability and re-implemented clean-room
(CLAUDE.md invariant 10). Field-level rules cite the IG page inline, next to the field they cover.

**Resources produced, and the identifier each Bundle entry is conditional on** (channel-spec README
3.4.2 -- every entry conditional on exactly one identifier system, never an unconditional create):

* `Patient` -- conditional on its own PID-3 identifier (`urn:id:<PID-3.4>`, `datatypes.
  local_identifier_system`), a real (synthetic, in these fixtures) business identifier the sending
  system already assigns, per ADR-001 6.4's FHIR idempotency row.
* `Encounter` -- these fixtures carry no PV1-19 (Visit Number), so there is no business identifier to
  key on. Conditional instead on `tributary.fhir.message_identifier(message.msg_id, discriminator=
  "encounter")` -- a message-scoped, non-clinical identifier (never a business or patient identifier,
  per that function's own docstring), which still gives a genuine idempotency key: a redelivery of the
  same message resolves to the same conditional PUT. A deployment whose PV1-19 is populated should key
  on that instead; this reference mapping documents the fallback, not a universal rule.

Practitioner (PV1-7 attending doctor) and Location (PV1-3 assigned location) are **not** built as
Bundle entries in this reference set -- they appear only as display-only `Reference`s on `Encounter`
(no resource, so no identifier question to answer). Mapping them to their own conditional resources is
a natural extension, left out here to keep the first ADT mapping's scope to what W2-3 asks for.
"""

from __future__ import annotations

from fhir.resources.R4B.encounter import Encounter, EncounterLocation, EncounterParticipant
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from tributary.fhir import Bundle, FhirContext, message_identifier
from tributary.hl7v2 import Hl7Message
from tributary.hl7v2.step import VARS_KEY
from tributary.sdk import Message, step

from .datatypes import patient_class_to_encounter_class, ts_to_fhir_datetime
from .errors import RequiredFieldMissingError, UnsupportedTriggerError
from .patient import map_patient

#: The three ADT trigger events this reference mapping supports (W2-3's stated scope). Anything else
#: raises `UnsupportedTriggerError` rather than guessing.
_SUPPORTED_TRIGGERS = frozenset({"A01", "A03", "A08"})

#: PV1-2 (Patient Class) has no encounter-status implication of its own; the *trigger event* decides
#: `Encounter.status` here (IG mapping "EVN.1 / message trigger informs Encounter.status" -- ADT_A01
#: sets `in-progress`, ADT_A03 sets `finished`; ADT_A08 leaves whatever status already applies, and since
#: this mapping has no prior state to read, it also uses `in-progress`, matching "the encounter this
#: update refers to is still open" for the common no-discharge-yet update case).
_STATUS_FOR_TRIGGER = {"A01": "in-progress", "A03": "finished", "A08": "in-progress"}


def _map_encounter(hl7: Hl7Message, *, patient_ref: Reference, status: str, trigger_event: str) -> Encounter | None:
    """PV1 -> `Encounter`, or `None` if the message carries no PV1 at all (ADT_A08 fixtures may omit it
    -- a pure demographic update). IG: "PV1 to Encounter" (build.fhir.org/ig/HL7/v2-to-fhir, PV1.md)."""
    if hl7.segment_count("PV1") == 0:
        return None

    patient_class = hl7.get("PV1.2") or ""
    # `class_fhir` (the Python name for the wire-format, reserved-word field `class`) is a required
    # field, but mypy's PEP 681 dataclass-transform reading of `fhir.resources`' models (no
    # `pydantic.mypy` plugin configured) cannot resolve it as a constructor keyword -- a known friction
    # point of these models under strict mypy, not a real type error; it is genuinely accepted at
    # runtime (`fhir_core.fhirabstractmodel.FHIRAbstractModel.__init__` takes `**data: Any`).
    encounter = Encounter(
        status=status,
        class_fhir=patient_class_to_encounter_class(patient_class),  # type: ignore[call-arg]
        subject=patient_ref,
    )

    location_text = hl7.get("PV1.3") or ""
    if location_text:
        parts = [part for part in location_text.split("^") if part]
        display = "/".join(parts[:3]) if parts else None
        if display:
            encounter.location = [EncounterLocation(location=Reference(display=display))]

    attending_text = hl7.get("PV1.7") or ""
    if attending_text:
        components = attending_text.split("^")
        display = " ".join(part for part in components[1:3] if part) or components[0]
        encounter.participant = [EncounterParticipant(individual=Reference(display=display))]

    hospital_service = hl7.get("PV1.10") or ""
    if hospital_service:
        # No Encounter.serviceType coding table ships here (invariant 10: no licensed code system);
        # kept off the resource entirely rather than fabricating a display-only CodeableConcept the IG
        # does not actually specify a system for at this trigger set.
        pass

    # IG: "the encounter period's bounds prefer the segment's own admit/discharge date-time (PV1-44 for
    # admission, PV1-45 for discharge); EVN-2 (Recorded Date/Time) is the fallback the ADT_A01/A03
    # mapping pages name for a sender that leaves PV1's own timestamp field blank" -- but only for
    # *that* trigger's own end of the period: an A03 (discharge) using EVN-2 for `period.start` would
    # wrongly stamp the *discharge* time as the *admission* time whenever PV1-44 is blank, which this
    # reference set's fixtures deliberately exercise (PV1-44/-45 are both blank; EVN-2 only ever
    # answers "when did the event this message reports happen").
    start = ts_to_fhir_datetime(hl7.get("PV1.44") or "")
    end = ts_to_fhir_datetime(hl7.get("PV1.45") or "")
    if start is None and trigger_event == "A01":
        start = ts_to_fhir_datetime(hl7.get("EVN.2") or "")
    if end is None and trigger_event == "A03":
        end = ts_to_fhir_datetime(hl7.get("EVN.2") or "")
    if start or end:
        # `Period.start`/`.end` are typed as `datetime` for their parsed representation; the FHIR
        # wire-format `str` this reference set builds is what the validator actually accepts (same
        # friction as `birthDate` above).
        encounter.period = Period(start=start, end=end)  # type: ignore[arg-type]

    return encounter


@step(name="tributary.content_hl7_fhir.adt")
def adt_to_fhir(message: Message, ctx: FhirContext) -> Message:
    """Map an ADT A01/A03/A08 message (already parsed by `tributary.hl7v2.parse`) to a FHIR transaction
    `Bundle`, replacing the message payload with the Bundle's JSON.

    Raises `UnsupportedTriggerError` for any other ADT trigger, `RequiredFieldMissingError` if PID-3 is
    absent (there is then nothing to key the `Patient` entry's conditional PUT on), and lets
    `tributary.fhir.errors.BundleEntryError` propagate unchanged for any other Bundle-construction
    failure -- all typed, none of them a bare crash, matching this role's parser and mapping conventions.
    """
    hl7 = ctx.vars.get(VARS_KEY)
    if not isinstance(hl7, Hl7Message):
        raise RequiredFieldMissingError(
            "MSH", reason="no parsed HL7 message in ctx.vars; run tributary.hl7v2.parse first"
        )

    _msg_type, trigger_event, _structure = hl7.message_type()
    if trigger_event not in _SUPPORTED_TRIGGERS:
        raise UnsupportedTriggerError(_msg_type, trigger_event)

    patient = map_patient(hl7)
    patient_entry = ctx.fhir.upsert(patient)
    patient_ref = Reference(reference=patient_entry.fullUrl)

    entries = [patient_entry]

    encounter = _map_encounter(
        hl7, patient_ref=patient_ref, status=_STATUS_FOR_TRIGGER[trigger_event], trigger_event=trigger_event
    )
    if encounter is not None:
        encounter_identifier = message_identifier(message.msg_id, discriminator="encounter")
        entries.append(ctx.fhir.upsert(encounter, identifier=encounter_identifier))

    bundle = Bundle(type="transaction", entry=entries)
    ctx.log.debug("adt_mapped", trigger_event=trigger_event, entry_count=len(entries))
    return message.with_payload(
        bundle.model_dump_json(exclude_none=True).encode("utf-8"), content_type="application/fhir+json"
    )
