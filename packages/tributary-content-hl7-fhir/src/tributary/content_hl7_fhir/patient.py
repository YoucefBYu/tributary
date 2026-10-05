"""PID -> `Patient`. Shared by `adt.py` and `oru.py`: the v2-to-FHIR IG's "PID to Patient" mapping
(https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/PID.html) does not vary by trigger event or
message type, so both mapping steps call this one function rather than each carrying their own copy.
"""

from __future__ import annotations

from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from tributary.fhir import FhirContext
from tributary.hl7v2 import Hl7Message, SegmentNotFoundError

from .datatypes import (
    administrative_sex_to_gender,
    cx_identifier,
    dt8_to_fhir_date,
    facility_fallback_system,
    require_scoped_system,
    xad_address,
    xpn_human_name,
    xtn_to_contact_point,
)
from .errors import RequiredFieldMissingError, safe_build


def map_patient(hl7: Hl7Message, ctx: FhirContext) -> Patient:
    """PID -> `Patient`. Raises `RequiredFieldMissingError` if there is no PID segment at all, or if
    PID-3 is absent: with no identifier at all, there is nothing to key the `Patient` Bundle entry's
    conditional PUT on (channel-spec README 3.4.2). Also raises `RequiredFieldMissingError("PID.3.4")` if
    PID-3 carries no assigning authority: a conditional key with no system collides across senders.

    Every field assignment below is a single `safe_build` call (`errors.py`): if `fhir.resources`' own
    validation rejects something this module's datatype conversions did not already catch, the caller
    gets a typed `InvalidFieldError("PID")`, never the pydantic `ValidationError` text underneath it
    (invariant 6 -- that text embeds the rejected value verbatim). `ctx` is used for exactly one thing:
    a `warning` log (path and a fixed reason only, never the value) if PID-7 was present but not a valid
    calendar date -- mirrors `adt.py`'s `Encounter`-omission and invalid-date warnings.
    """
    try:
        pid = hl7.segment("PID")
    except SegmentNotFoundError:
        raise RequiredFieldMissingError("PID.3", reason="no PID segment in the message") from None

    identifier = cx_identifier(pid, 3)
    if identifier is None:
        raise RequiredFieldMissingError("PID.3", reason="no patient identifier to key the Patient entry on")

    # No authority -> no conditional key: an empty-system key collides across senders (PHI integrity).
    require_scoped_system(identifier, path="PID.3.4")

    identifiers: list[Identifier] = [identifier]
    account = cx_identifier(pid, 18, fallback_system=facility_fallback_system(hl7["MSH.4.1"]))
    if account is not None:
        identifiers.append(account)

    def build() -> Patient:
        patient = Patient(identifier=identifiers)

        name = xpn_human_name(pid, 5, use="official")
        if name is not None:
            patient.name = [name]

        gender = administrative_sex_to_gender(pid.field(8))
        if gender is not None:
            patient.gender = gender

        raw_dob = pid.field(7)
        birth_date = dt8_to_fhir_date(raw_dob)
        if birth_date is not None:
            # `birthDate` is typed as `date` for its parsed representation; the FHIR wire-format `str`
            # `dt8_to_fhir_date` returns (already calendar-validated) is what the validator accepts.
            patient.birthDate = birth_date  # type: ignore[assignment]
        elif raw_dob:
            # Present but not a valid calendar date -- omitted, not passed to `birthDate`'s own
            # validator (security review HIGH 2). Logged so an operator can see the sender is doing
            # this (code-reviewer follow-up on 1aba292): path and a fixed reason only, never the value.
            ctx.log.warning("field_invalid_omitted", path="PID.7", reason="invalid_value")

        address = xad_address(pid, 11)
        if address is not None:
            patient.address = [address]

        phone = xtn_to_contact_point(pid.field(13))
        if phone is not None:
            patient.telecom = [phone]

        return patient

    return safe_build(build, path="PID", reason="could not build a valid Patient resource")
