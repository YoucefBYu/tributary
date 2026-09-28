"""HL7 v2 datatype fragments (XPN, XAD, XTN, CX, CWE, PL, XCN, TS/DT) to FHIR data types.

Each function is a small, independently-testable piece of the public HL7 v2-to-FHIR Implementation
Guide's datatype maps (https://build.fhir.org/ig/HL7/v2-to-fhir/branches/master/datatype_maps.html),
read for interoperability and re-implemented clean-room (CLAUDE.md invariant 10) -- nothing here was
copied from Mirth, the Open Integration Engine, or any other MPL/GPL-licensed transform.

**Every composite-type function takes a `Segment` and a field number, never pre-extracted text, and
resolves components through `Segment.field(..., component=...)`.** That is not a style choice: a
security review found that splitting a field's own *already-unescaped* text on a hard-coded `"^"` (this
module's previous shape) has two independent bugs --

1. An escaped `\\S\\` in a component (a literal caret in the sender's data, not a separator) is turned
   back into a raw `^` by `unescape()` before the naive split ever runs, so the split then reads it as a
   real component boundary and injects extra, attacker-controlled components (a security review's probe
   changed the identifier *system* this way).
2. A message with custom delimiters (`tests/fixtures/hl7/custom_delimiters.hl7`, `^` is not even the
   component separator there) is silently misparsed, because `"^"` was never actually *that* message's
   separator to begin with.

`Segment.field(component=N)` splits the *raw*, still-escaped field text on the message's own actual
delimiter characters first, and only unescapes the one resolved component afterwards -- exactly the order
that avoids both bugs, and it is already what `tributary-hl7v2` exists to get right. Nothing in this
module re-implements HL7 delimiter or escape handling of its own.

An absent optional v2 field returns `None`, not a typed error -- that is silence, not a mapping failure.
Fields required by a specific resource are checked by the caller (`adt.py`, `oru.py`, `patient.py`),
which is where a missing *required* field becomes a typed `RequiredFieldMissingError`.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from fhir.resources.R4B.address import Address
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.contactpoint import ContactPoint
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from tributary.hl7v2 import Segment

from .errors import safe_build

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


# -- FHIR `code` sanitizing ----------------------------------------------------------------------------

#: The FHIR `code` primitive's own grammar (R4 datatypes.html#code): no leading, trailing or consecutive
#: whitespace. A v2 field handed to a `Coding.code` unsanitized can fail this at the pydantic validator,
#: whose `ValidationError` embeds the rejected text verbatim (`input_value=...`) -- exactly the PHI leak
#: invariant 6 forbids. Sanitizing first (below) means that never happens for a mismatch this predictable.
_FHIR_CODE_PATTERN = re.compile(r"^\S+(\s\S+)*$")


def sanitize_fhir_code(text: str) -> str | None:
    """Strip surrounding whitespace and confirm the result fits the FHIR `code` grammar. `None` (never a
    raised exception) if it still doesn't fit after stripping -- the caller omits `.code` rather than
    handing pydantic something it will reject."""
    stripped = text.strip()
    if not stripped or not _FHIR_CODE_PATTERN.match(stripped):
        return None
    return stripped


# -- CX (Extended Composite ID) -----------------------------------------------------------------------

#: v2 Table 0203 (Identifier Type) code -> `Identifier.type` display text, the handful this reference
#: set's fixtures use. Not exhaustive -- a code not listed here still maps, just without a display.
_IDENTIFIER_TYPE_DISPLAY = {
    "MR": "Medical record number",
    "ACSN": "Accession ID",
    "VN": "Visit number",
}


def cx_identifier(segment: Segment, field: int, *, fallback_system: str | None = None) -> Identifier | None:
    """A v2 CX (`id^checkDigit^checkDigitScheme^assigningAuthority^identifierTypeCode`) at `segment`'s
    `field`, to `Identifier`. `None` when CX.1 (the id itself) is absent, or is whitespace-only --
    a value made only of spaces, tabs or a non-breaking space (`\\xa0`) is treated as missing rather than
    handed to `Identifier.value`'s own validator, which rejects it outright and embeds it verbatim in a
    `ValidationError` (security review, LOW): FHIR's `string` grammar happens to accept a bare literal
    tab but not a bare `\\xa0`, and neither is a meaningful identifier value either way, so both are
    rejected here before that distinction matters.

    `fallback_system` is used when the CX carries no assigning authority (CX.4, an HD) of its own --
    typically the sending facility, so a bare account or visit number still gets a (locally-scoped, never
    a business system it does not actually belong to) `Identifier.system`.

    The whole construction is one `safe_build` call (security review, LOW): if `fhir.resources`' own
    validation rejects something the check above did not already catch, the caller gets a typed
    `InvalidFieldError`, never the pydantic `ValidationError` text underneath it (invariant 6).
    """
    value = segment.field(field, component=1)
    if not value or not value.strip():
        return None
    authority = segment.field(field, component=4) or None
    system = local_identifier_system(authority) if authority else fallback_system
    type_code = segment.field(field, component=5) or None

    def build() -> Identifier:
        identifier = Identifier(value=value)
        if system is not None:
            identifier.system = system
        if type_code:
            from fhir.resources.R4B.codeableconcept import CodeableConcept

            sanitized_type_code = sanitize_fhir_code(type_code)
            if sanitized_type_code is not None:
                coding = Coding(code=sanitized_type_code, system="http://terminology.hl7.org/CodeSystem/v2-0203")
                display = _IDENTIFIER_TYPE_DISPLAY.get(sanitized_type_code)
                if display:
                    coding.display = display
                identifier.type = CodeableConcept(coding=[coding])
        return identifier

    return safe_build(build, path=f"{segment.name}.{field}", reason="could not build a valid Identifier")


# -- XPN (Extended Person Name) -----------------------------------------------------------------------


def xpn_human_name(segment: Segment, field: int, *, use: str | None = None) -> HumanName | None:
    """A v2 XPN (`family^given^middle^suffix^prefix^degree`) at `segment`'s `field`, to `HumanName`.
    `None` when both family and given are absent."""
    family = segment.field(field, component=1)
    given: list[str | None] = [
        part for part in (segment.field(field, component=2), segment.field(field, component=3)) if part
    ]
    if not family and not given:
        return None
    name = HumanName()
    if family:
        name.family = family
    if given:
        name.given = given
    if use:
        name.use = use
    return name


# -- XAD (Extended Address) -------------------------------------------------------------------------


def xad_address(segment: Segment, field: int) -> Address | None:
    """A v2 XAD (`street^otherDesignation^city^state^postalCode^country`) at `segment`'s `field`, to
    `Address`. `None` when every component is absent."""
    street = segment.field(field, component=1)
    other = segment.field(field, component=2)
    city = segment.field(field, component=3)
    state = segment.field(field, component=4)
    postal_code = segment.field(field, component=5)
    country = segment.field(field, component=6)
    if not any((street, other, city, state, postal_code, country)):
        return None
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
    and used verbatim as `ContactPoint.value`. No component splitting happens here, so the caller may
    pass a bare `segment.field(n)` -- there is no `"^"` boundary in this datatype for this shape to
    misread. `None` for empty input."""
    if not xtn_text:
        return None
    point = ContactPoint(system=system, value=xtn_text)
    if use:
        point.use = use
    return point


# -- CWE (Coded with Exceptions) --------------------------------------------------------------------


def cwe_components(segment: Segment, field: int) -> tuple[str, str, str]:
    """CWE.1 (identifier), CWE.2 (text) and CWE.3 (name of coding system) at `segment`'s `field`, read
    component-by-component (see module docstring)."""
    return (
        segment.field(field, component=1),
        segment.field(field, component=2),
        segment.field(field, component=3),
    )


# -- PL (Person Location) ------------------------------------------------------------------------------


def pl_display(segment: Segment, field: int) -> str | None:
    """A human-readable `point of care/room/bed` from a v2 PL at `segment`'s `field`, for a display-only
    `Reference` (this reference set does not build a `Location` resource -- see `adt.py`). `None` if
    every component is absent."""
    parts = [
        segment.field(field, component=1),
        segment.field(field, component=2),
        segment.field(field, component=3),
    ]
    present = [part for part in parts if part]
    return "/".join(present) if present else None


# -- XCN (Extended Composite ID Number and Name) ------------------------------------------------------


def xcn_display(segment: Segment, field: int) -> str | None:
    """A human-readable `family given` (falling back to the bare id) from a v2 XCN at `segment`'s
    `field`, for a display-only `Reference` (this reference set does not build a `Practitioner`
    resource -- see `adt.py`). `None` if every component is absent."""
    id_number = segment.field(field, component=1)
    family = segment.field(field, component=2)
    given = segment.field(field, component=3)
    name = " ".join(part for part in (family, given) if part)
    return name or (id_number or None)


# -- DT / TS (Date, Date/Time) -----------------------------------------------------------------------


def _digits(text: str, length: int) -> bool:
    return len(text) >= length and text[:length].isdigit()


def dt8_to_fhir_date(value: str) -> str | None:
    """An 8-digit v2 `DT` (`YYYYMMDD`) to a FHIR `date` (`YYYY-MM-DD`). `None` for empty, short,
    non-numeric or calendar-invalid input (`19800231` is rejected here, not left for `Patient.birthDate`'s
    own validator to reject with the value embedded in its `ValidationError` text -- invariant 6)."""
    if not _digits(value, 8):
        return None
    year, month, day = int(value[0:4]), int(value[4:6]), int(value[6:8])
    try:
        date(year, month, day)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def ts_to_fhir_datetime(value: str) -> str | None:
    """A v2 `TS` (`YYYYMMDD[HHMM[SS]]`, this reference set's fixtures use the 14-digit form) to a FHIR
    `dateTime`. `None` for empty, short, non-numeric or calendar/clock-invalid input -- same reasoning as
    `dt8_to_fhir_date`.

    FHIR `dateTime` requires a timezone offset once a time is present (R4 datatypes.html#dateTime); v2
    `TS`'s own offset component (`&GMT offset`, e.g. `TS.2`) is not modelled by this reference set's
    plain-string field access, so a time-bearing value is stamped `+00:00` rather than left invalid or
    guessed at a site-specific offset. A deployment with a real sender offset should override this, not
    trust `+00:00` as anything but "unknown, treated as UTC".
    """
    if not _digits(value, 8):
        return None
    year, month, day = int(value[0:4]), int(value[4:6]), int(value[6:8])
    try:
        date(year, month, day)
    except ValueError:
        return None
    if len(value) < 12:
        return f"{year:04d}-{month:02d}-{day:02d}"
    if not _digits(value, 12):
        return None
    hour, minute = int(value[8:10]), int(value[10:12])
    second = 0
    if len(value) >= 14:
        if not _digits(value, 14):
            return None
        second = int(value[12:14])
    try:
        datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}+00:00"


# -- NM (Numeric) ----------------------------------------------------------------------------------

#: Caps on `Decimal(OBX-5)` before it is ever handed to a FHIR model (security review, HIGH): `fhir_core`
#: serialises an integral `Decimal` by formatting it as a plain base-10 integer, so an unbounded exponent
#: is unbounded CPU and memory at *serialization* time, not construction time -- `1E+500000` took 3.3 s
#: and 500 kB in the review's own measurement, and `1E+999999999` would not practically finish (the
#: executor would then retry it three times, per ADR-003 D4). Both caps are checked immediately after
#: `Decimal()` parses the text, before it is used for anything else.
MAX_DECIMAL_SIGNIFICANT_DIGITS = 30
MAX_DECIMAL_EXPONENT_MAGNITUDE = 30


def bounded_decimal(raw_value: str) -> Decimal | None:
    """`Decimal(raw_value)`, or `None` if it is not decimal-shaped, not finite (`NaN`/`Infinity` -- v2 NM
    should never legitimately be either, and `Quantity.value`'s own validator would otherwise embed one
    of those words in a `ValidationError`), or exceeds the digit/exponent caps above."""
    try:
        value = Decimal(raw_value)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    _sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        return None  # defensive: is_finite() already excludes the non-int ('n'/'N'/'F') exponent forms
    if len(digits) > MAX_DECIMAL_SIGNIFICANT_DIGITS or abs(exponent) > MAX_DECIMAL_EXPONENT_MAGNITUDE:
        return None
    return value


def decimal_preserves_precision(value: Decimal) -> bool:
    """Whether `fhir_core`'s actual JSON encoding of `value` (a `Quantity.value`) round-trips through
    `float` without changing the number of significant digits or decimal places the sender sent (security
    review, MEDIUM 4: `"72.10"` serialises as `72.1`, and `"1.00000000000000001"` as `1.0` -- both silent
    precision loss, since `fhir_core` has no arbitrary-precision JSON encoder for `Decimal` and goes
    through `float` regardless of what Python object `.value` holds).

    A whole number (`exponent >= 0`, no fractional digits in the source -- `"72"`, not `"72.0"`) is
    trivially exact for any magnitude this module's own digit cap allows, since it is well within a
    float64's exact-integer range; only a value with an explicit fractional part is checked against a
    real float round-trip, so `"72"` is not penalised for `json.dumps`'s convention of writing floats
    with a trailing `.0`.
    """
    _sign, _digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        return False
    if exponent >= 0:
        return float(value) == value
    try:
        round_tripped = Decimal(str(float(value)))
    except (ValueError, OverflowError):
        return False
    return round_tripped == value and round_tripped.as_tuple().exponent == exponent


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
    return _ADMINISTRATIVE_SEX_TO_GENDER.get(code.strip())


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
    R4's `Encounter.class` cardinality).

    `code` is stripped and checked against the FHIR `code` grammar (`sanitize_fhir_code`) before it is
    ever handed to `Coding.code` -- a leading/trailing space a sender sometimes pads PV1-2 with would
    otherwise fail `Coding.code`'s own validator with the value embedded in its `ValidationError` text
    (security review, MEDIUM 2). A code that still doesn't fit after stripping gets a `Coding` with no
    `.code` at all (system only) rather than one built from unsanitized text.
    """
    normalized = code.strip()
    act_code = _PATIENT_CLASS_TO_ENCOUNTER_CLASS.get(normalized)
    if act_code is not None:
        return Coding(system=_V3_ACT_CODE_SYSTEM, code=act_code)
    sanitized = sanitize_fhir_code(code)
    if sanitized is None:
        return Coding(system=_V2_PATIENT_CLASS_SYSTEM)
    return Coding(system=_V2_PATIENT_CLASS_SYSTEM, code=sanitized)
