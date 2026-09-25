"""HL7 v2 datatype fragments (XPN, XAD, XTN, CX, TS/DT) to FHIR data types.

Each function is a small, independently-testable piece of the public HL7 v2-to-FHIR Implementation
Guide's datatype maps (https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/datatype_maps.html),
read for interoperability and re-implemented clean-room (CLAUDE.md invariant 10) -- nothing here was
copied from Mirth, the Open Integration Engine, or any other MPL/GPL-licensed transform.

Every function takes the plain (already-unescaped) field text a `tributary.hl7v2.Hl7Message` accessor
returns, never the raw wire text, and returns `None` (not a typed error) when its input is empty or
absent -- an absent optional v2 field is silence, not a mapping failure. Fields required by a specific
resource are checked by the caller (`adt.py`, `oru.py`), which is where a missing *required* field
becomes a typed `RequiredFieldMissingError`.
"""

from __future__ import annotations

from fhir.resources.R4B.address import Address
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.contactpoint import ContactPoint
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier

#: HD.1 (Namespace ID) -> `Identifier.system`, for an assigning authority that carries no HD.2/HD.3
#: universal id -- every fixture in this reference set is like this (a synthetic MRN authority named
#: only by its local namespace). The v2-to-FHIR "HD to Identifier" datatype map prefers `urn:oid:`/
#: `urn:uuid:` when a universal id and its type are present; with neither, it falls back to a
#: locally-scoped URI built from the namespace id alone, which is what this constructs. A deployment
#: with real OIDs in HD.2/HD.3 should extend this, not silently reuse `urn:id:*` for both.
_LOCAL_SYSTEM_PREFIX = "urn:id:"


def local_identifier_system(namespace_id: str) -> str:
    """A locally-scoped `Identifier.system` URI for an HD with no universal id. See module docstring."""
    return f"{_LOCAL_SYSTEM_PREFIX}{namespace_id}"


# -- CX (Extended Composite ID) -----------------------------------------------------------------------

#: v2 Table 0203 (Identifier Type) code -> `Identifier.type` display text, the handful this reference
#: set's fixtures use. Not exhaustive -- a code not listed here still maps, just without a display.
_IDENTIFIER_TYPE_DISPLAY = {
    "MR": "Medical record number",
    "ACSN": "Accession ID",
}


def cx_to_identifier(cx_text: str, *, fallback_system: str | None = None) -> Identifier | None:
    """A v2 CX (`id^checkDigit^checkDigitScheme^assigningAuthority^identifierTypeCode`) to `Identifier`.

    `fallback_system` is used when the CX carries no assigning authority component (component 4) of its
    own -- typically the sending facility, so a bare account number still gets a (locally-scoped, never
    a business system it does not actually belong to) `Identifier.system`. Returns `None` for empty input.
    """
    if not cx_text:
        return None
    components = cx_text.split("^")
    value = components[0] if components else ""
    if not value:
        return None
    authority = components[3] if len(components) > 3 and components[3] else None
    system = local_identifier_system(authority) if authority else fallback_system
    type_code = components[4] if len(components) > 4 and components[4] else None
    identifier = Identifier(value=value)
    if system is not None:
        identifier.system = system
    if type_code:
        from fhir.resources.R4B.codeableconcept import CodeableConcept

        coding = Coding(system="http://terminology.hl7.org/CodeSystem/v2-0203", code=type_code)
        display = _IDENTIFIER_TYPE_DISPLAY.get(type_code)
        if display:
            coding.display = display
        identifier.type = CodeableConcept(coding=[coding])
    return identifier


# -- XPN (Extended Person Name) -----------------------------------------------------------------------


def xpn_to_human_name(xpn_text: str, *, use: str | None = None) -> HumanName | None:
    """A v2 XPN (`family^given^middle^suffix^prefix^degree`) to `HumanName`. `None` for empty input."""
    if not xpn_text:
        return None
    components = xpn_text.split("^")
    family = components[0] if components else ""
    given: list[str | None] = [part for part in components[1:3] if part]
    name = HumanName()
    if family:
        name.family = family
    if given:
        name.given = given
    if use:
        name.use = use
    return name


# -- XAD (Extended Address) -------------------------------------------------------------------------


def xad_to_address(xad_text: str) -> Address | None:
    """A v2 XAD (`street^otherDesignation^city^state^postalCode^country`) to `Address`."""
    if not xad_text:
        return None
    components = xad_text.split("^")
    components += [""] * (6 - len(components))
    street, other, city, state, postal_code, country = components[:6]
    address = Address()
    lines: list[str | None] = [part for part in (street, other) if part]
    if lines:
        address.line = lines
    if city:
        address.city = city
    if state:
        address.state = state
    if postal_code:
        address.postalCode = postal_code
    if country:
        address.country = country
    return address


# -- XTN (Extended Telecommunication Number) ----------------------------------------------------------


def xtn_to_contact_point(xtn_text: str, *, system: str = "phone", use: str | None = None) -> ContactPoint | None:
    """A v2 XTN, taken as a whole string (this reference set's fixtures predate the v2.5 component form)
    and used verbatim as `ContactPoint.value`. `None` for empty input."""
    if not xtn_text:
        return None
    point = ContactPoint(system=system, value=xtn_text)
    if use:
        point.use = use
    return point


# -- DT / TS (Date, Date/Time) -----------------------------------------------------------------------


def dt8_to_fhir_date(value: str) -> str | None:
    """An 8-digit v2 `DT` (`YYYYMMDD`) to a FHIR `date` (`YYYY-MM-DD`). `None` for empty or short input."""
    if not value or len(value) < 8:
        return None
    return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"


def ts_to_fhir_datetime(value: str) -> str | None:
    """A v2 `TS` (`YYYYMMDD[HHMM[SS]]`, this reference set's fixtures use the 14-digit form) to a FHIR
    `dateTime`. `None` for empty or too-short input.

    FHIR `dateTime` requires a timezone offset once a time is present (R4 datatypes.html#dateTime); v2
    `TS`'s own offset component (`&GMT offset`, e.g. `TS.2`) is not modelled by this reference set's
    plain-string field access, so a time-bearing value is stamped `+00:00` rather than left invalid or
    guessed at a site-specific offset. A deployment with a real sender offset should override this, not
    trust `+00:00` as anything but "unknown, treated as UTC".
    """
    if not value or len(value) < 8:
        return None
    date_part = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    if len(value) < 12:
        return date_part
    hour, minute = value[8:10], value[10:12]
    second = value[12:14] if len(value) >= 14 else "00"
    return f"{date_part}T{hour}:{minute}:{second}+00:00"


# -- coded concept maps --------------------------------------------------------------------------------

#: v2 Table 0001 (Administrative Sex) -> FHIR `Patient.gender` (`administrative-gender`). Per the
#: v2-to-FHIR IG's published ConceptMap for this table; `A` (ambiguous) and `N` (not applicable) both
#: land on `other`/`unknown` per that same map, not a guess of this package's own.
_ADMINISTRATIVE_SEX_TO_GENDER = {
    "M": "male",
    "F": "female",
    "O": "other",
    "A": "other",
    "N": "unknown",
    "U": "unknown",
}


def administrative_sex_to_gender(code: str) -> str | None:
    return _ADMINISTRATIVE_SEX_TO_GENDER.get(code)


#: v2 Table 0004 (Patient Class) -> FHIR `Encounter.class` (`http://terminology.hl7.org/CodeSystem/
#: v3-ActCode`). Per the v2-to-FHIR IG's published ConceptMap "v2-patient-class-to-encounter-class";
#: an unmapped code falls back to `http://terminology.hl7.org/CodeSystem/v2-0004` with the original code
#: rather than guessing an ActCode, so an unusual local Patient Class value is visible, not silently wrong.
_V3_ACT_CODE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ActCode"
_V2_PATIENT_CLASS_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0004"
_PATIENT_CLASS_TO_ENCOUNTER_CLASS = {
    "I": "IMP",  # inpatient
    "O": "AMB",  # outpatient -> ambulatory
    "E": "EMER",  # emergency
    "P": "PRENC",  # preadmit -> pre-admission
    "R": "AMB",  # recurring patient -> ambulatory
    "B": "AMB",  # obstetrics -> ambulatory
}


def patient_class_to_encounter_class(code: str) -> Coding:
    """v2 PV1-2 (Patient Class) to `Encounter.class` (a single `Coding`, not a `CodeableConcept`, per
    R4's `Encounter.class` cardinality)."""
    act_code = _PATIENT_CLASS_TO_ENCOUNTER_CLASS.get(code)
    if act_code is not None:
        return Coding(system=_V3_ACT_CODE_SYSTEM, code=act_code)
    return Coding(system=_V2_PATIENT_CLASS_SYSTEM, code=code)
