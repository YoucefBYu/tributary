"""`LocalFhir`: conditional Bundle-entry construction, no network, deterministic with an injected id
factory."""

from __future__ import annotations

import pytest
from fhir.resources.R4B.patient import Patient
from tributary.fhir.bundle import (
    TB_MESSAGE_SYSTEM,
    Identifier,
    LocalFhir,
    message_id_factory,
    message_identifier,
    sequential_id_factory,
    uuid_id_factory,
)
from tributary.fhir.errors import BundleEntryError

SYNTH_IDENTIFIER = Identifier(system="urn:synth:mrn", value="9000001")


def _patient(identifier: Identifier | None = SYNTH_IDENTIFIER) -> Patient:
    return Patient(identifier=[identifier] if identifier else None)


def test_upsert_builds_a_conditional_put() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.upsert(_patient())
    assert entry.request.method == "PUT"
    assert entry.request.url == "Patient?identifier=urn:synth:mrn|9000001"
    assert entry.resource is not None


def test_upsert_does_not_set_if_none_exist() -> None:
    """Security review, LOW: `ifNoneExist` is FHIR's conditional-*create* mechanism and is meaningless
    on a `PUT`, whose condition already lives entirely in the URL -- a server ignores it at best."""
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.upsert(_patient())
    assert entry.request.ifNoneExist is None


def test_upsert_uses_an_explicit_identifier_override() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    override = Identifier(system=TB_MESSAGE_SYSTEM, value="0199-uuid:obx-1")
    entry = fhir.upsert(_patient(), identifier=override)
    assert entry.request.url == f"Patient?identifier={TB_MESSAGE_SYSTEM}|0199-uuid:obx-1"


def test_upsert_raises_without_any_identifier() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    with pytest.raises(BundleEntryError):
        fhir.upsert(_patient(identifier=None))


def test_create_builds_a_conditional_post() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.create(_patient())
    assert entry.request.method == "POST"
    assert entry.request.url == "Patient"
    assert entry.request.ifNoneExist == "identifier=urn:synth:mrn|9000001"


def test_delete_builds_a_conditional_delete() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.delete("Patient", identifier=SYNTH_IDENTIFIER)
    assert entry.request.method == "DELETE"
    assert entry.request.url == "Patient?identifier=urn:synth:mrn|9000001"
    assert entry.resource is None


def test_id_factory_is_injectable_and_deterministic() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory("urn:uuid:golden-"))
    first = fhir.upsert(_patient())
    second = fhir.upsert(_patient())
    assert first.fullUrl == "urn:uuid:golden-1"
    assert second.fullUrl == "urn:uuid:golden-2"


def test_default_id_factory_produces_a_urn_uuid() -> None:
    fhir = LocalFhir()
    entry = fhir.upsert(_patient())
    assert entry.fullUrl is not None
    assert entry.fullUrl.startswith("urn:uuid:")


def test_uuid_id_factory_produces_distinct_values() -> None:
    factory = uuid_id_factory()
    assert factory() != factory()


def test_message_identifier_uses_the_tributary_system() -> None:
    identifier = message_identifier("0199-abc-uuid")
    assert identifier.system == TB_MESSAGE_SYSTEM
    assert identifier.value == "0199-abc-uuid"


def test_message_identifier_with_discriminator() -> None:
    identifier = message_identifier("0199-abc-uuid", discriminator="obx-2")
    assert identifier.value == "0199-abc-uuid:obx-2"


def test_message_identifier_never_equals_a_patient_identifier_system() -> None:
    identifier = message_identifier("0199-abc-uuid")
    assert identifier.system != SYNTH_IDENTIFIER.system


def test_message_id_factory_is_deterministic_and_not_uuid4() -> None:
    """W2-2 open item 3: production `fullUrl`s come from `tb-msg-id`, so a reprocess of the same message
    reproduces the same `fullUrl`s rather than a fresh `uuid4` each run."""
    first_run = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    second_run = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    assert first_run() == second_run()
    assert first_run() == second_run()  # the second call in each run also lines up (per-call sequence)


def test_message_id_factory_differs_across_messages() -> None:
    a = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    b = message_id_factory("0199036e-7a3e-7f00-8c21-000000000000")
    assert a() != b()


def test_message_id_factory_produces_a_urn_uuid() -> None:
    factory = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    assert factory().startswith("urn:uuid:")


# -- hostile identifier content (security review, 4998f33, MEDIUM) -----------------------------------


def test_conditional_url_percent_encodes_unsafe_characters() -> None:
    hostile = Identifier(system="urn:synth:auth with space", value="A&B#C?D")
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.upsert(_patient(hostile))
    assert "&B#C?D" not in entry.request.url
    assert " " not in entry.request.url
    assert entry.request.url.split("identifier=")[1] == "urn:synth:auth%20with%20space|A%26B%23C%3FD"


def test_conditional_url_disambiguates_an_embedded_pipe_from_the_separator() -> None:
    """A literal `|` inside the identifier's own value must not be readable as *the* system/value
    separator -- that would let a crafted CX/HD value pick which system its own value is asserted
    against, or make two different (system, value) pairs render as the same query string."""
    hostile = Identifier(system="urn:synth:mrn", value="A|B")
    plain = Identifier(system="urn:synth:mrn", value="A")
    fhir = LocalFhir(id_factory=sequential_id_factory())
    hostile_entry = fhir.upsert(_patient(hostile))
    plain_entry = fhir.upsert(_patient(plain))
    assert hostile_entry.request.url != "Patient?identifier=urn:synth:mrn|A|B"
    assert hostile_entry.request.url != plain_entry.request.url
    assert "%7C" in hostile_entry.request.url  # the embedded pipe, percent-encoded, not a bare "|"


def test_create_if_none_exist_uses_the_same_escaped_token_as_a_put_url_would() -> None:
    """`create` (`POST`) is where `ifNoneExist` actually matters; it uses the same escaped token
    `upsert`'s URL would, so the two conditional forms never disagree about what a hostile value escapes
    to."""
    hostile = Identifier(system="urn:synth:mrn", value="A,B$C")
    fhir = LocalFhir(id_factory=sequential_id_factory())
    put_entry = fhir.upsert(_patient(hostile))
    post_entry = fhir.create(_patient(hostile))
    assert post_entry.request.ifNoneExist == f"identifier={put_entry.request.url.split('identifier=')[1]}"


def test_delete_also_percent_encodes_hostile_values() -> None:
    hostile = Identifier(system="urn:synth:mrn", value="A B")
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.delete("Patient", identifier=hostile)
    assert " " not in entry.request.url
    assert "%20" in entry.request.url


def test_conditional_url_leaves_ordinary_uri_shaped_systems_readable() -> None:
    """Common, non-hostile FHIR system URIs (colons, slashes) stay literal -- this is a security fix for
    injected content, not a blanket re-encoding of every identifier system in the codebase."""
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.upsert(_patient(Identifier(system=TB_MESSAGE_SYSTEM, value="0199-msg:encounter")))
    assert entry.request.url == f"Patient?identifier={TB_MESSAGE_SYSTEM}|0199-msg:encounter"


def test_stable_entries_reuse_the_full_url_for_a_repeated_request() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory(), stable_entries=True)
    key = Identifier(system="urn:t", value="1")
    first = fhir.upsert(Patient(), identifier=key)
    other = fhir.create(Patient(), identifier=key)
    assert fhir.upsert(Patient(), identifier=key).fullUrl == first.fullUrl
    assert fhir.create(Patient(), identifier=key).fullUrl == other.fullUrl != first.fullUrl
