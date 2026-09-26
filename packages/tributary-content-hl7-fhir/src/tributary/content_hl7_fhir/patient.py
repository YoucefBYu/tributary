"""PID -> `Patient`. Shared by `adt.py` and `oru.py`: the v2-to-FHIR IG's "PID to Patient" mapping
(https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/PID.html) does not vary by trigger event or
message type, so both mapping steps call this one function rather than each carrying their own copy.
"""

from __future__ import annotations

from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from tributary.hl7v2 import Hl7Message, SegmentNotFoundError

from .datatypes import (
    administrative_sex_to_gender,
    cx_identifier,
    dt8_to_fhir_date,
    local_identifier_system,
    xad_address,
    xpn_human_name,
    xtn_to_contact_point,
)
from .errors import RequiredFieldMissingError, safe_build


def map_patient(hl7: Hl7Message) -> Patient:
    """PID -> `Patient`. Raises `RequiredFieldMissingError` if there is no PID segment at all, or if
    PID-3 is absent: with no identifier at all, there is nothing to key the `Patient` Bundle entry's
    conditional PUT on (channel-spec README 3.4.2).

    Every field assignment below is a single `safe_build` call (`errors.py`): if `fhir.resources`' own
    validation rejects something this module's datatype conversions did not already catch, the caller
    gets a typed `InvalidFieldError("PID")`, never the pydantic `ValidationError` text underneath it
    (invariant 6 -- that text embeds the rejected value verbatim).
    """
    try:
        pid = hl7.segment("PID")
    except SegmentNotFoundError:
        raise RequiredFieldMissingError("PID.3", reason="no PID segment in the message") from None

    identifier = cx_identifier(pid, 3)
    if identifier is None:
        raise RequiredFieldMissingError("PID.3", reason="no patient identifier to key the Patient entry on")

    identifiers: list[Identifier] = [identifier]
    account = cx_identifier(pid, 18, fallback_system=local_identifier_system(hl7["MSH.4"]))
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

        birth_date = dt8_to_fhir_date(pid.field(7))
        if birth_date is not None:
            # `birthDate` is typed as `date` for its parsed representation; the FHIR wire-format `str`
            # `dt8_to_fhir_date` returns (already calendar-validated) is what the validator accepts.
            patient.birthDate = birth_date  # type: ignore[assignment]

        address = xad_address(pid, 11)
        if address is not None:
            patient.address = [address]

        phone = xtn_to_contact_point(pid.field(13))
        if phone is not None:
            patient.telecom = [phone]

        return patient

    return safe_build(build, path="PID", reason="could not build a valid Patient resource")
