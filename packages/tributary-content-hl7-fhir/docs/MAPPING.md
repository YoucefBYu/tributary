# HL7 v2 to FHIR R4 reference mapping — identifiers and sources

This is the one-page index W2-3 asks for: which identifier system each resource type's Bundle entry is
conditional on (channel-spec README 3.4.2 — every entry conditional, never an unconditional create), and
where each mapping rule comes from. The full rule text and rationale live as docstrings and inline
comments next to the code (`adt.py`, `oru.py`, `patient.py`, `datatypes.py`); this file is the index, not
a duplicate.

**Source.** Every rule below is read from the public HL7 v2-to-FHIR Implementation Guide
(https://build.fhir.org/ig/HL7/v2-to-fhir/), its published mapping and datatype-map pages, for
interoperability, and re-implemented clean-room in Python (CLAUDE.md invariant 10). No code from Mirth,
the Open Integration Engine or any other MPL/GPL-licensed source was read or copied.

## Resources and their conditional identifier

**Two different guarantees hide behind "conditional Bundle entry", and this reference mapping is explicit
about which one each resource gets** (this distinction is what the CRITICAL finding on `4998f33` was
about — the first version of this mapping gave `Encounter` only the weaker one):

- **Converges across a clinical flow.** The identifier is a real, visit- or patient-scoped business
  identifier, so *every* message about the same real-world thing — however many messages, however many
  different `tb-msg-id`s, over however long the encounter lasts — resolves the same conditional request.
  A second message doesn't just avoid duplicating the first; it *updates* the same resource.
- **Redelivery-idempotent only.** The identifier is derived from `tb-msg-id`, so *this one message*,
  replayed any number of times, always resolves the same conditional request — but a *different* message
  about the same real-world thing (a later ADT trigger for the same visit, a later result for the same
  order) gets a *different* identifier and therefore a different resource. This is correct for a
  resource that is supposed to be a new instance per message (an `Observation` is one result, not a
  running total), and a bug for one that is supposed to be a single, evolving record (an `Encounter`).

| Resource | Built by | Conditional on | Convergence |
|---|---|---|---|
| `Patient` | `patient.map_patient` (shared by both steps) | PID-3's own identifier: `value` = PID-3.1, `system` = `urn:id:<PID-3.4>` (`datatypes.local_identifier_system`) | **Converges** — every message for the same patient (any PID-3), any trigger, any `tb-msg-id`. |
| `Encounter` | `adt.adt_to_fhir` (`_encounter_identifier`) | PV1-19 (Visit Number, a CX), when present. With no PV1-19: ADT_A01 falls back to `message_identifier(message.msg_id, discriminator="encounter")`; ADT_A03/A08 **omit the `Encounter` entry entirely** rather than fall back. | **Converges** when PV1-19 is present — A01/A03/A08 for the same visit resolve the same conditional PUT, so the one `Encounter` is created once and then updated/finished. With no PV1-19, ADT_A01's own entry is redelivery-idempotent only (it cannot converge with a later message it has no shared key with); ADT_A03/A08 produce no `Encounter` at all rather than create a wrong, non-converging one. |
| `DiagnosticReport` | `oru.oru_r01_to_fhir` | OBR-3 (Filler Order Number), else OBR-2 (Placer Order Number), else `message_identifier(message.msg_id, discriminator="obr-<OBR-1>")` | **Converges** when OBR-2/-3 is present — the lab's own order identifier, shared by every result for that order. The `tb-msg-id` fallback (no OBR-2/-3) is redelivery-idempotent only. |
| `Observation` | `oru.oru_r01_to_fhir` (one per `OBX`) | `message_identifier(message.msg_id, discriminator=f"obx-{OBX-1}")` | **Redelivery-idempotent only, by design.** v2 gives an `OBX` no order-independent identifier of its own, and an `Observation` is meant to be a new instance per result, not a record that later messages update — so there is nothing for it *to* converge with. OBX-1 (Set ID) is stable across a redelivery of the *same* message, which is this reference mapping's actual idempotency need here. |

`Practitioner` (PV1-7) and `Location` (PV1-3) are **not** built as Bundle entries in this reference set —
they appear only as display-only `Reference`s on `Encounter`. No resource, so no identifier question to
answer; a natural extension, out of scope for the first ADT/ORU mapping.

## Trigger events and pages

| Message | Step (`tributary.steps` entry point) | IG page | Notes |
|---|---|---|---|
| ADT^A01 (admit) | `tributary.content_hl7_fhir.adt` | `ADT_A01.html` | `Encounter.status = "in-progress"`; `period.start` from PV1-44, falling back to EVN-2. |
| ADT^A03 (discharge) | `tributary.content_hl7_fhir.adt` | `ADT_A03.html` | `Encounter.status = "finished"`; `period.end` from PV1-45, falling back to EVN-2. `period.start` is **not** backfilled from EVN-2 (that would stamp the discharge event's own time as the admission time). With no PV1-19, no `Encounter` entry is produced at all (see the identifier table above). |
| ADT^A08 (update patient information) | `tributary.content_hl7_fhir.adt` | `ADT_A08.html` | If the message carries no `PV1` at all (a pure demographic update), only `Patient` is produced — no fabricated `Encounter`. Same for a `PV1` with no PV1-19: `Patient` only. |
| ORU^R01 (unsolicited observation result) | `tributary.content_hl7_fhir.oru_r01` | `ORU_R01.html`, `OBR to DiagnosticReport`, `OBX to Observation` | One `OBR` per message (the common unsolicited single-order-result case); `NM` and `ST`/`TX` `OBX` value types, others fall back to `valueString`. |

## Known limitations (documented, not silent)

- `Encounter.class` (PV1-2) uses the IG's published v2-Patient-Class-to-v3-ActCode concept map; an
  unrecognised code falls back to `http://terminology.hl7.org/CodeSystem/v2-0004` with the original
  code, rather than guessing an ActCode.
- `Observation.code`'s coding system (OBX-3.3) is only translated for `LN` (LOINC); another alias is
  left off `Coding.system` rather than invented (invariant 10 — no licensed code system's URI is
  asserted without the code being validated against one, which is `ctx.terminology`'s job).
- v2 `TS` values with a time component get `+00:00` stamped on them (FHIR `dateTime` requires a
  timezone once a time is present; this reference set's fixtures carry none of their own). Treat this
  as "unknown, treated as UTC", not a real offset.
- `Quantity.value` uses `Decimal(OBX-5)`; a value that does not parse as a decimal falls back to
  `valueString` rather than raising.

## FHIR model set

Built on `fhir.resources.R4B` (BSD), not a standalone R4 package — `fhir.resources` dropped one after
v7; R4B is a technical-correction superset of R4. See `tributary-fhir`'s own package docstring for the
full rationale. Golden-file JSON is therefore R4B-shaped; see the root handoff for the R4 JSON Schema
validation item this raises (W2-2 open item 4).
