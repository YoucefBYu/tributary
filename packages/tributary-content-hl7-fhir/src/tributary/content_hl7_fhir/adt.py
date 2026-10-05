"""ADT A01 (admit), A03 (discharge) and A08 (update patient information) to a FHIR R4 transaction Bundle.

Registered as the `tributary.content_hl7_fhir.adt` step (`pyproject.toml`). Expects
`tributary.hl7v2.parse` to have already run in the same pipeline and left the parsed message in
`ctx.vars["hl7"]` (`tributary.hl7v2.step.VARS_KEY`) -- this step never re-parses the payload itself.

**Mapping source.** Segment-to-resource shape follows the public HL7 v2-to-FHIR Implementation Guide's
published ADT_A01/A03/A08-to-Bundle page (https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/
ADT_A01.html and its A03/A08 siblings), read for interoperability and re-implemented clean-room
(CLAUDE.md invariant 10). Field-level rules cite the IG page inline, next to the field they cover.

**Resources produced, and the identifier each Bundle entry is conditional on** (channel-spec README
3.4.2 -- every entry conditional on exactly one identifier system, never an unconditional create; every
resource body also carries that same identifier in its own `.identifier`, security review MEDIUM 4, so a
redelivery's conditional search actually matches what the previous delivery wrote):

* `Patient` -- conditional on its own PID-3 identifier (`urn:id:<PID-3.4>`, `datatypes.
  local_identifier_system`), a real (synthetic, in these fixtures) business identifier the sending
  system already assigns, per ADR-001 6.4's FHIR idempotency row. **Converges** across a clinical flow:
  every ADT and ORU message for the same patient resolves the same conditional PUT.
* `Encounter` -- conditional on PV1-19 (Visit Number, a CX), when present: a real business identifier
  for *this visit*, so an A01 (admit), a later A08 (update) and the eventual A03 (discharge) for the
  same visit all resolve the same conditional PUT and the one `Encounter` is created once and then
  updated/finished in place -- this is what makes the resource **converge** across a clinical flow, not
  merely survive a redelivery of one message. If PV1-19 is absent, this reference mapping does **not**
  invent a per-visit identifier: `tb-msg-id` identifies one delivery, not a visit, and keying on it
  would give A01/A03/A08 for the same real-world visit three different conditional PUTs, each creating
  its own `Encounter` instead of converging on one (the bug a code review caught in the first version of
  this mapping, `4998f33`). The one exception is ADT_A01 itself: an admit with no PV1-19 still gets a
  message-scoped `Encounter` (`message_identifier(message.msg_id, discriminator="encounter")`), because
  an admit is establishing a *new* encounter and a lone, never-updated resource is at least
  redelivery-idempotent, which is strictly better than either failing the message or fabricating a
  visit number. ADT_A03/A08 with no PV1-19 instead **omit the `Encounter` entry** (`Patient` still
  built), logged at `warning` (trigger code and a fixed reason only, never a value) since an operator
  needs to see a sender that never populates PV1-19 -- see `_encounter_identifier` for the reasoning and
  `docs/MAPPING.md` for the summary table.

Practitioner (PV1-7 attending doctor) and Location (PV1-3 assigned location) are **not** built as
Bundle entries in this reference set -- they appear only as display-only `Reference`s on `Encounter`
(no resource, so no identifier question to answer). Mapping them to their own conditional resources is
a natural extension, left out here to keep the first ADT mapping's scope to what W2-3 asks for.
"""

from __future__ import annotations

from fhir.resources.R4B.encounter import Encounter, EncounterLocation, EncounterParticipant
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from tributary.fhir import Bundle, FhirContext, Identifier, message_identifier
from tributary.hl7v2 import Hl7Message, Segment
from tributary.hl7v2.message import STRUCTURAL_CODE
from tributary.hl7v2.step import VARS_KEY
from tributary.sdk import Message, step

from .datatypes import (
    cx_identifier,
    facility_fallback_system,
    patient_class_to_encounter_class,
    pl_display,
    require_scoped_system,
    ts_to_fhir_datetime,
    xcn_display,
)
from .errors import RequiredFieldMissingError, UnsupportedTriggerError, safe_build
from .patient import map_patient

#: The three ADT trigger events this reference mapping supports (W2-3's stated scope). Anything else
#: raises `UnsupportedTriggerError` rather than guessing.
_SUPPORTED_TRIGGERS = frozenset({"A01", "A03", "A08"})

#: A fixed, non-sender-controlled placeholder for `UnsupportedTriggerError` when MSH-9's own text fails
#: `STRUCTURAL_CODE` (security review, MEDIUM 2: `errors.py` previously *claimed* MSH-9 was already
#: allow-listed before reaching this error, but nothing here actually checked -- this placeholder, and
#: the check right before it, is what makes that claim true).
_NON_STRUCTURAL = "<non-structural>"

#: PV1-2 (Patient Class) has no encounter-status implication of its own; the *trigger event* decides
#: `Encounter.status` here (IG mapping "EVN.1 / message trigger informs Encounter.status" -- ADT_A01
#: sets `in-progress`, ADT_A03 sets `finished`; ADT_A08 leaves whatever status already applies, and since
#: this mapping has no prior state to read, it also uses `in-progress`, matching "the encounter this
#: update refers to is still open" for the common no-discharge-yet update case).
_STATUS_FOR_TRIGGER = {"A01": "in-progress", "A03": "finished", "A08": "in-progress"}


def _safe_trigger_codes(message_type: str, trigger_event: str) -> tuple[str, str]:
    """`(message_type, trigger_event)`, each replaced with `_NON_STRUCTURAL` if it fails
    `STRUCTURAL_CODE` -- for use in an exception, which must never carry raw MSH-9 text (MEDIUM 2)."""
    safe_type = message_type if STRUCTURAL_CODE.match(message_type) else _NON_STRUCTURAL
    safe_trigger = trigger_event if STRUCTURAL_CODE.match(trigger_event) else _NON_STRUCTURAL
    return safe_type, safe_trigger


def _warn_if_invalid_date(ctx: FhirContext, *, path: str, raw: str, parsed: str | None) -> None:
    """Log a `warning` when `raw` was present but failed to parse (`parsed is None`) -- never when the
    field was simply absent, and never the field's own text (code-reviewer follow-up on 1aba292/5c42b2f:
    mirrors the `Encounter`-omission warning's shape -- a path/reason only, invariant 6)."""
    if raw and parsed is None:
        ctx.log.warning("field_invalid_omitted", path=path, reason="invalid_value")


def _map_encounter(
    hl7: Hl7Message, pv1: Segment | None, *, patient_ref: Reference, status: str, trigger_event: str, ctx: FhirContext
) -> Encounter | None:
    """PV1 -> `Encounter`, or `None` if the message carries no PV1 at all (ADT_A08 fixtures may omit it
    -- a pure demographic update). IG: "PV1 to Encounter" (build.fhir.org/ig/HL7/v2-to-fhir, PV1.md)."""
    if pv1 is None:
        return None

    def build() -> Encounter:
        patient_class = pv1.field(2)
        # `class_fhir` (the Python name for the wire-format, reserved-word field `class`) is a required
        # field, but mypy's PEP 681 dataclass-transform reading of `fhir.resources`' models (no
        # `pydantic.mypy` plugin configured) cannot resolve it as a constructor keyword -- a known
        # friction point of these models under strict mypy, not a real type error; it is genuinely
        # accepted at runtime (`fhir_core.fhirabstractmodel.FHIRAbstractModel.__init__` takes `**data: Any`).
        encounter = Encounter(
            status=status,
            class_fhir=patient_class_to_encounter_class(patient_class),  # type: ignore[call-arg]
            subject=patient_ref,
        )

        location_display = pl_display(pv1, 3)
        if location_display is not None:
            encounter.location = [EncounterLocation(location=Reference(display=location_display))]

        attending_display = xcn_display(pv1, 7)
        if attending_display is not None:
            encounter.participant = [EncounterParticipant(individual=Reference(display=attending_display))]

        # IG: "the encounter period's bounds prefer the segment's own admit/discharge date-time (PV1-44
        # for admission, PV1-45 for discharge); EVN-2 (Recorded Date/Time) is the fallback the ADT_A01/
        # A03 mapping pages name for a sender that leaves PV1's own timestamp field blank" -- but only
        # for *that* trigger's own end of the period: an A03 (discharge) using EVN-2 for `period.start`
        # would wrongly stamp the *discharge* time as the *admission* time whenever PV1-44 is blank,
        # which this reference set's fixtures deliberately exercise (PV1-44/-45 are both blank; EVN-2
        # only ever answers "when did the event this message reports happen").
        pv1_44 = pv1.field(44)
        start = ts_to_fhir_datetime(pv1_44)
        _warn_if_invalid_date(ctx, path="PV1.44", raw=pv1_44, parsed=start)

        pv1_45 = pv1.field(45)
        end = ts_to_fhir_datetime(pv1_45)
        _warn_if_invalid_date(ctx, path="PV1.45", raw=pv1_45, parsed=end)

        if start is None and trigger_event == "A01":
            evn2 = hl7.get("EVN.2") or ""
            start = ts_to_fhir_datetime(evn2)
            _warn_if_invalid_date(ctx, path="EVN.2", raw=evn2, parsed=start)
        if end is None and trigger_event == "A03":
            evn2 = hl7.get("EVN.2") or ""
            end = ts_to_fhir_datetime(evn2)
            _warn_if_invalid_date(ctx, path="EVN.2", raw=evn2, parsed=end)
        if start or end:
            # `Period.start`/`.end` are typed as `datetime` for their parsed representation; the FHIR
            # wire-format `str` `ts_to_fhir_datetime` returns (already calendar/clock-validated) is what
            # the validator accepts.
            encounter.period = Period(start=start, end=end)  # type: ignore[arg-type]

        return encounter

    return safe_build(build, path="PV1", reason="could not build a valid Encounter resource")


def _encounter_identifier(pv1: Segment, hl7: Hl7Message, *, msg_id: str, trigger_event: str) -> Identifier | None:
    """The identifier `Encounter`'s Bundle entry is conditional on, or `None` when this mapping should
    not emit an `Encounter` entry at all.

    PV1-19 (Visit Number) first, when present -- a real per-visit business identifier, so A01/A03/A08
    for the *same* visit converge on the same conditional PUT (see `adt.py`'s module docstring for why
    that matters and what breaks without it). `fallback_system` mirrors `oru._diagnostic_report_
    identifier`'s reasoning: this reference set's fixtures carry no assigning-authority component on
    PV1-19 either, so the sending facility (MSH-4.1, the HD's own Namespace ID -- security review LOW:
    reading the whole of MSH-4 would fold an HD's other components, or a custom delimiter's own text,
    into the system URI) supplies one.

    With no PV1-19: `A01` (admitting -- there is no prior `Encounter` to converge with yet) still gets a
    message-scoped identifier, redelivery-idempotent even though it cannot converge with a later
    A03/A08 for the same visit. `A03`/`A08` return `None` instead of inventing one, because a
    message-scoped identifier for those two would create a **second, wrong** `Encounter` next to
    whatever A01 already created (or nothing, if A01 never ran through this mapping) rather than
    updating the original -- silently worse than simply not producing an `Encounter` entry.
    """
    identifier = cx_identifier(pv1, 19, fallback_system=facility_fallback_system(hl7["MSH.4.1"]))
    if identifier is not None:
        # Neither PV1-19.4 nor MSH-4.1: refuse rather than key on a sender-agnostic system (PHI integrity).
        return require_scoped_system(identifier, path="PV1.19.4")
    if trigger_event == "A01":
        # `msg_id` (`tb-msg-id`, always a validated UUID) and the fixed literal `"encounter"` are the
        # only inputs here -- neither is sender-controlled, so unlike `cx_identifier` above (built from
        # PV1-19's own text) there is no `safe_build` wrapping to do: nothing this call sees can fail.
        return message_identifier(msg_id, discriminator="encounter")
    return None


@step(name="tributary.content_hl7_fhir.adt")
def adt_to_fhir(message: Message, ctx: FhirContext) -> Message:
    """Map an ADT A01/A03/A08 message (already parsed by `tributary.hl7v2.parse`) to a FHIR transaction
    `Bundle`, replacing the message payload with the Bundle's JSON.

    Raises `UnsupportedTriggerError` if MSH-9 is not `ADT^{A01,A03,A08}` (security review, MEDIUM 2: the
    message type itself is now checked, not only the trigger event), `RequiredFieldMissingError` if
    PID-3 is absent (there is then nothing to key the `Patient` entry's conditional PUT on), and lets
    `tributary.fhir.errors.BundleEntryError` propagate unchanged for any other Bundle-construction
    failure -- all typed, none of them a bare crash, matching this role's parser and mapping conventions.
    """
    hl7 = ctx.vars.get(VARS_KEY)
    if not isinstance(hl7, Hl7Message):
        raise RequiredFieldMissingError(
            "MSH", reason="no parsed HL7 message in ctx.vars; run tributary.hl7v2.parse first"
        )

    msg_type, trigger_event, _structure = hl7.message_type()
    if msg_type != "ADT" or trigger_event not in _SUPPORTED_TRIGGERS:
        raise UnsupportedTriggerError(*_safe_trigger_codes(msg_type, trigger_event))

    patient = map_patient(hl7, ctx)
    patient_entry = ctx.fhir.upsert(patient)
    patient_ref = Reference(reference=patient_entry.fullUrl)

    entries = [patient_entry]

    pv1 = hl7.segment("PV1") if hl7.segment_count("PV1") else None
    encounter = _map_encounter(
        hl7,
        pv1,
        patient_ref=patient_ref,
        status=_STATUS_FOR_TRIGGER[trigger_event],
        trigger_event=trigger_event,
        ctx=ctx,
    )
    encounter_omitted = False
    if encounter is not None and pv1 is not None:
        encounter_identifier = _encounter_identifier(pv1, hl7, msg_id=message.msg_id, trigger_event=trigger_event)
        if encounter_identifier is not None:
            # Security review, MEDIUM 4: the resource body carries the same identifier its conditional
            # entry is keyed on, so a redelivery's conditional search actually matches what the previous
            # delivery wrote (a `PUT` whose body has no `.identifier` at all is not truly self-describing).
            encounter.identifier = [encounter_identifier]
            entries.append(ctx.fhir.upsert(encounter, identifier=encounter_identifier))
        else:
            # No PV1-19 and this trigger cannot safely fall back to a message-scoped identifier (see
            # `_encounter_identifier`) -- omit the entry rather than create a wrong second Encounter.
            # `warning`, not `debug` (code-reviewer, e48d937/5c42b2f follow-up): an operator needs to see
            # a sender that never populates PV1-19. Trigger code and a fixed reason only, never a value.
            ctx.log.warning("adt_encounter_omitted", trigger_event=trigger_event, reason="no_visit_number")
            encounter_omitted = True

    bundle = Bundle(type="transaction", entry=entries)
    ctx.log.debug(
        "adt_mapped", trigger_event=trigger_event, entry_count=len(entries), encounter_omitted=encounter_omitted
    )
    return message.with_payload(
        bundle.model_dump_json(exclude_none=True).encode("utf-8"), content_type="application/fhir+json"
    )
