"""PID -> `Patient`. Shared by `adt.py` and `oru.py`: the v2-to-FHIR IG's "PID to Patient" mapping
(https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/PID.html) does not vary by trigger event or
message type, so both mapping steps call this one function rather than each carrying their own copy.
"""

from __future__ import annotations

from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from tributary.hl7v2 import Hl7Message

from .datatypes import (
    administrative_sex_to_gender,
    cx_to_identifier,
    dt8_to_fhir_date,
    local_identifier_system,
    xad_to_address,
    xpn_to_human_name,
    xtn_to_contact_point,
)
from .errors import RequiredFieldMissingError


def map_patient(hl7: Hl7Message) -> Patient:
    """PID -> `Patient`. Raises `RequiredFieldMissingError` if PID-3 is absent: with no identifier at
    all, there is nothing to key the `Patient` Bundle entry's conditional PUT on (channel-spec README
    3.4.2)."""
    mrn_text = hl7.get("PID.3") or ""
    identifier = cx_to_identifier(mrn_text)
    if identifier is None:
        raise RequiredFieldMissingError("PID.3", reason="no patient identifier to key the Patient entry on")

    identifiers: list[Identifier] = [identifier]
    account = cx_to_identifier(hl7.get("PID.18") or "", fallback_system=local_identifier_system(hl7["MSH.4"]))
    if account is not None:
        identifiers.append(account)

    patient = Patient(identifier=identifiers)

    name = xpn_to_human_name(hl7.get("PID.5") or "", use="official")
    if name is not None:
        patient.name = [name]

    gender = administrative_sex_to_gender(hl7.get("PID.8") or "")
    if gender is not None:
        patient.gender = gender

    birth_date = dt8_to_fhir_date(hl7.get("PID.7") or "")
    if birth_date is not None:
        # `fhir.resources`' `birthDate` is typed as `date` for its *parsed* representation, but its
        # pydantic validator accepts (and this reference set passes) the FHIR wire-format `str` --
        # a known friction point of using these models without the `pydantic.mypy` plugin, not a real
        # type error at runtime.
        patient.birthDate = birth_date  # type: ignore[assignment]

    address = xad_to_address(hl7.get("PID.11") or "")
    if address is not None:
        patient.address = [address]

    phone = xtn_to_contact_point(hl7.get("PID.13") or "")
    if phone is not None:
        patient.telecom = [phone]

    return patient
