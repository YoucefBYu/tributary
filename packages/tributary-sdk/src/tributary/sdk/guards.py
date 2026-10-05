"""PHI guards shared by `ctx.set_attr`, `ctx.set_usr` and `ctx.log`.

These are the invariant-6 checks that the contracts state as producer-side rules, implemented once so
the SDK, the runtime and the connectors cannot disagree about them:

* `_tb.events.attrs` rules — `contracts/events/v0/README.md` section 3.
* `tb-usr-*` rules — `contracts/envelope/v0/README.md` section 2.1.
* structured-log fields — invariant 6, which names log lines first, and
  `contracts/observability/v0/README.md` section 4 (L3 identifier deny-list, L4 content deny-list).

None of this is a proof. A surname is a valid short code, so the deny-list catches the common mistake
and makes intent reviewable; `security-reviewer` and code review remain the real control (events
contract section 3, rule 3).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from .errors import AttrError, LogFieldError, UsrHeaderError, safe_token

ATTR_NAME = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
ATTR_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}$")
HMAC_VALUE = re.compile(r"^[0-9a-f]{64}$")
USR_NAME = re.compile(r"^[a-z0-9-]{1,48}$")
LOG_FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
LOG_EVENT_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

MAX_ATTRS = 16
MAX_ATTRS_BYTES = 1024
MAX_USR_VALUE_BYTES = 256
MAX_LOG_VALUE_CHARS = 256

#: Names that denote a direct identifier. Kept byte-for-byte in step with the deny-list in
#: `contracts/events/v0/event.schema.json`; `tests/test_guards_contract.py` fails if the two drift.
IDENTIFIER_NAMES = frozenset(
    {
        "mrn",
        "pid",
        "patient",
        "patient_id",
        "patientid",
        "patient_name",
        "name",
        "family_name",
        "given_name",
        "surname",
        "first_name",
        "last_name",
        "dob",
        "birth_date",
        "birthdate",
        "ssn",
        "nhs_number",
        "insurance_number",
        "account",
        "account_number",
        "visit_number",
        "encounter_id",
        "accession",
        "accession_number",
        "address",
        "street",
        "postcode",
        "zip",
        "phone",
        "telephone",
        "email",
        "identifier",
        "key",
        "message_key",
        "partition_key",
    }
)

#: Log field names that carry content or free text (`contracts/observability/v0` README 4, rule L4).
#: Kept byte-for-byte in step with the second `propertyNames` deny-list in
#: `contracts/observability/v0/log-line.schema.json`; `tests/test_guards.py` fails if the two drift.
#: Log lines only: an exception's text quotes the offending field value, and a payload or body is PHI
#: by default, so the name itself is refused before any value is looked at.
LOG_CONTENT_NAMES = frozenset(
    {
        "payload",
        "body",
        "content",
        "raw",
        "message",
        "msg",
        "text",
        "segment",
        "headers",
        "traceback",
        "stack",
        "stack_trace",
        "exc_info",
        "exception",
        "exc_text",
        "query",
        "query_string",
        "sql",
    }
)

LogValue = str | int | float | bool | None


def _denotes_identifier(name: str) -> bool:
    """True when the name is on the deny-list, comparing hyphens and underscores as the same character.

    `tb-usr-patient-id` and an attr named `patient_id` are the same mistake with different punctuation.
    """
    return name.replace("-", "_") in IDENTIFIER_NAMES


def validate_attr(name: str, value: str) -> None:
    """Check one `attrs` entry against the events contract. Raises `AttrError`.

    Refuses `_hmac` entries outright: the events contract pins the 64-hex *shape* but the hashing
    mechanism is unresolved and escalated to ADR-004 plus a human, and section 3.1 says producers
    should not emit identifier-derived attrs until then. An unkeyed `sha256(mrn)` has exactly the
    accepted shape and is reversible over a bounded identifier space, so accepting the shape now would
    let the careless version ship first.
    """
    if not isinstance(name, str) or not ATTR_NAME.match(name):
        raise AttrError(f"attr name {safe_token(name)} must match ^[a-z][a-z0-9_]{{0,31}}$")
    if name.endswith("_hmac"):
        raise AttrError(
            f"attr {safe_token(name)}: identifier-derived attrs are not emitted in v0 "
            "(events contract 3.1: the keyed-hash mechanism is unresolved, ADR-004 plus a human decision)"
        )
    if _denotes_identifier(name):
        raise AttrError(
            f"attr name {safe_token(name)} denotes a direct identifier and must not be emitted (invariant 6)"
        )
    if not isinstance(value, str) or not ATTR_VALUE.match(value):
        # The value is never quoted back: free text in attrs is exactly where PHI arrives.
        raise AttrError(
            f"attr {safe_token(name)}: value must be a short code ^[A-Za-z0-9][A-Za-z0-9._:+-]{{0,63}}$ "
            "(no spaces, no HL7 delimiters, no CR/LF)"
        )


def validate_attrs(attrs: Mapping[str, str]) -> None:
    """Check the whole `attrs` object: every entry, the 16-entry cap and the 1024-byte serialized cap."""
    for name, value in attrs.items():
        validate_attr(name, value)
    if len(attrs) > MAX_ATTRS:
        raise AttrError(f"attrs has {len(attrs)} entries, at most {MAX_ATTRS} allowed")
    size = len(json.dumps(attrs, separators=(",", ":"), ensure_ascii=False, sort_keys=True).encode("utf-8"))
    if size > MAX_ATTRS_BYTES:
        raise AttrError(f"attrs serializes to {size} bytes, at most {MAX_ATTRS_BYTES} allowed")


def validate_usr(name: str, value: str) -> None:
    """Check a `tb-usr-*` header name (without the prefix) and value. Raises `UsrHeaderError`.

    The engine never interprets these, but it does propagate and persist them, and a Kafka header is
    readable by anything with topic read access, so "it is only a header" is not a containment
    argument (envelope contract 2.1).
    """
    if not isinstance(name, str) or not USR_NAME.match(name):
        raise UsrHeaderError(f"tb-usr name {safe_token(name)} must match ^[a-z0-9-]{{1,48}}$")
    if _denotes_identifier(name):
        raise UsrHeaderError(
            f"tb-usr name {safe_token(name)} denotes a direct identifier; headers are archived and rendered (invariant 6)"
        )
    if not isinstance(value, str):
        raise UsrHeaderError(f"tb-usr-{safe_token(name)}: value must be text")
    if not value:
        raise UsrHeaderError(f"tb-usr-{safe_token(name)}: empty value; omit the header instead (envelope contract 2)")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        # C0 controls and DEL. The check lives here, not only in `Message.with_header`, because
        # `validate_for_produce` calls this function directly and is the authoritative layer for a
        # hand-built message — the same argument that closed the deny-list bypass (code-reviewer, wave 1).
        raise UsrHeaderError(f"tb-usr-{safe_token(name)}: control characters are not allowed in a header value")
    size = len(value.encode("utf-8"))
    if size > MAX_USR_VALUE_BYTES:
        raise UsrHeaderError(f"tb-usr-{safe_token(name)}: value is {size} bytes, at most {MAX_USR_VALUE_BYTES} allowed")


def validate_log_field(name: str, value: object) -> None:
    """Check one structured-log field. Raises `LogFieldError`.

    Scalars only, bounded length, no CR/LF (a newline in a log value forges a second log line), and no
    deny-listed name: neither a direct identifier (L3) nor a content carrier (L4). `bytes` is rejected
    on purpose: the two `bytes` values in the system are the payload and the record key.
    """
    if not isinstance(name, str) or not LOG_FIELD_NAME.match(name):
        raise LogFieldError(f"log field name {safe_token(name)} must match ^[a-z][a-z0-9_]{{0,31}}$")
    if _denotes_identifier(name):
        raise LogFieldError(
            f"log field {safe_token(name)} denotes a direct identifier; log message ids only (invariant 6)"
        )
    if name in LOG_CONTENT_NAMES:
        raise LogFieldError(
            f"log field {safe_token(name)} carries content or free text; log codes and message ids only "
            "(invariant 6, observability/v0 L4)"
        )
    if isinstance(value, bytes | bytearray | memoryview):
        raise LogFieldError(f"log field {safe_token(name)}: bytes are never logged (payload and record key are bytes)")
    if not isinstance(value, str | int | float | bool | None):
        raise LogFieldError(f"log field {safe_token(name)}: only str, int, float, bool or None may be logged")
    if isinstance(value, str):
        if len(value) > MAX_LOG_VALUE_CHARS:
            raise LogFieldError(f"log field {safe_token(name)}: {len(value)} characters, at most {MAX_LOG_VALUE_CHARS}")
        if "\r" in value or "\n" in value:
            raise LogFieldError(f"log field {safe_token(name)}: CR/LF would forge a second log line")


def validate_log_event(event: str) -> None:
    """Check a log call's event name.

    The event name is the one string a caller could build with an f-string, so it is constrained to a
    short snake_case code for the same reason `attrs` values are: free text is where PHI arrives.
    """
    if not isinstance(event, str) or not LOG_EVENT_NAME.match(event):
        raise LogFieldError(f"log event {safe_token(event)} must match ^[a-z][a-z0-9_]{{0,63}}$")


def validate_log_fields(fields: Mapping[str, object]) -> None:
    """Check every field of one log call."""
    for name, value in fields.items():
        validate_log_field(name, value)
