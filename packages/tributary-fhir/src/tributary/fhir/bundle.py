"""`LocalFhir`: local, no-network Bundle-entry construction for a mapping step (the `FhirInterface`
default, `context.py`).

Every entry is **conditional**, matching ADR-001 6.4's FHIR idempotency row ("conditional create/update,
`If-None-Exist`, PUT with business identifier"): a mapping step never mints a resource id itself, and a
duplicate delivery of the same message produces the same conditional request, which the FHIR server
absorbs. `tributary-loaders`' FHIR REST loader (W2-6) is what actually talks to a server; this module
only builds the `Bundle`/`BundleEntry` a step returns.

FHIR models are R4B (`fhir.resources.R4B`), not R4 -- see `pyproject.toml` and this package's
`__init__.py` for why. `Resource` and `Identifier` are re-exported from there so a mapping step imports
one thing.
"""

from __future__ import annotations

import itertools
import uuid
from collections.abc import Callable, Iterator
from uuid import uuid4

from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.resource import Resource

from .errors import BundleEntryError

__all__ = [
    "TB_MESSAGE_SYSTEM",
    "Bundle",
    "BundleEntry",
    "BundleEntryRequest",
    "Identifier",
    "LocalFhir",
    "Resource",
    "message_id_factory",
    "message_identifier",
    "sequential_id_factory",
    "uuid_id_factory",
]

#: A Tributary-owned identifier system, never a patient or other business identifier (architect review
#: criterion 6). Used by `message_identifier` for a resource that has no natural business identifier of
#: its own to key its conditional Bundle entry on, so the identifier can never collide with -- or be
#: mistaken for -- clinical data.
TB_MESSAGE_SYSTEM = "https://tributary.dev/fhir/identifier/tb-msg-id"


def message_identifier(msg_id: str, *, discriminator: str | None = None) -> Identifier:
    """A `TB_MESSAGE_SYSTEM` identifier derived from `tb-msg-id`, for a resource with no business
    identifier of its own (a lab `Observation` with no OBX-21, say).

    `discriminator` distinguishes more than one such resource built from the same message (for example
    one per repeating `OBX`): pass something stable across retries of the *same* message, such as the
    OBX set id, never a payload value that could vary between an original delivery and its redelivery.
    `msg_id` is already an engine-minted UUIDv7 (`tb-msg-id`, invariant 4), not sender data, so the
    result is never a business or patient identifier -- it identifies "this delivery of this message",
    which is exactly what conditional-create idempotency needs and no more.
    """
    value = msg_id if discriminator is None else f"{msg_id}:{discriminator}"
    return Identifier(system=TB_MESSAGE_SYSTEM, value=value)


def sequential_id_factory(prefix: str = "urn:uuid:test-") -> Callable[[], str]:
    """A deterministic `fullUrl` factory for tests and golden files: `prefix` plus an incrementing
    counter, so a mapping step's output is byte-for-byte reproducible (architect review criterion 5:
    "resource ids and fullUrls must come from an injectable source, not a bare uuid4")."""
    counter: Iterator[int] = itertools.count(1)

    def factory() -> str:
        return f"{prefix}{next(counter)}"

    return factory


def uuid_id_factory() -> Callable[[], str]:
    """A `fullUrl` factory of last resort: a fresh random UUID per call.

    **Not the production default any more** (W2-2 open item 3): a `uuid4` `fullUrl` is unrelated to
    `tb-msg-id`, so a shadow run of the same message, or a reprocess from the `dlq`, produces a Bundle
    that differs from the original in every `fullUrl` even when every resource and every conditional
    request is identical -- which makes a shadow or reprocess diff noisy for no reason. Kept for a caller
    that genuinely has no message id yet (a unit test building a bare `Bundle` outside a pipeline, say);
    a mapping step always has one and should use `message_id_factory` instead.
    """

    def factory() -> str:
        return f"urn:uuid:{uuid4()}"

    return factory


def message_id_factory(msg_id: str) -> Callable[[], str]:
    """The production `fullUrl` factory (W2-2 open item 3): deterministic, derived from `tb-msg-id`, never
    `uuid4`.

    Each call returns `urn:uuid:<a UUID derived from tb-msg-id and a per-call sequence number>`, so a
    message that produces three Bundle entries gets three distinct, but always-the-same-on-redelivery,
    `fullUrl` values. That makes a shadow run or a `dlq` reprocess of the *same* message produce a
    byte-identical Bundle wherever the mapping itself is deterministic, which is exactly what those
    comparisons need and a random `fullUrl` would silently defeat.

    Deterministic, not reversible: `msg_id` (a UUIDv7, never sender data -- `tributary.sdk.ids.uuid7`) is
    fed through `uuid.uuid5` with a fixed, package-private namespace, so the output cannot be mistaken for
    another `tb-msg-id` or fed back through `tributary.sdk.headers.parse_uuid`'s version-7 check.

    A caller attaches this once per message, not once per pipeline run across many messages:

        ctx = attach(base, fhir=LocalFhir(id_factory=message_id_factory(message.msg_id)), terminology=...)

    Wiring that call site into the running engine belongs to whatever attaches `ctx.fhir` at each hop
    (`tributary-runtime`, `platform-engineer`'s side of ADR-001 6.2), not to this package.
    """
    namespace = uuid.UUID("6f6e6f6f-6873-6f6f-6e65-000000000001")  # package-private, never a public system URI
    counter: Iterator[int] = itertools.count(1)

    def factory() -> str:
        return f"urn:uuid:{uuid.uuid5(namespace, f'{msg_id}:{next(counter)}')}"

    return factory


def _first_identifier(resource: Resource) -> Identifier | None:
    identifiers = getattr(resource, "identifier", None)
    if not identifiers:
        return None
    first = identifiers[0] if isinstance(identifiers, list) else identifiers
    return first if isinstance(first, Identifier) else None


def _conditional_url(resource_type: str, identifier: Identifier) -> str:
    return f"{resource_type}?identifier={identifier.system}|{identifier.value}"


class LocalFhir:
    """The `FhirInterface` default: builds `BundleEntry` objects, no network I/O.

    `id_factory` is injected (`sequential_id_factory` for tests and golden files, `uuid_id_factory` by
    default) so `fullUrl` values are reproducible wherever a test needs them to be.
    """

    def __init__(self, *, id_factory: Callable[[], str] | None = None) -> None:
        self._id_factory = id_factory or uuid_id_factory()

    def upsert(self, resource: Resource, *, identifier: Identifier | None = None) -> BundleEntry:
        """A conditional `PUT` entry, keyed on `identifier` or the resource's own first `identifier`.

        Raises `BundleEntryError` if neither is available: an unconditional `PUT` would target a literal
        resource id this step never minted, and an unconditional `POST` would create a duplicate on
        every redelivery -- invariant 4 requires an idempotency technique, and "guess" is not one.
        """
        resource_type = type(resource).__name__
        key = identifier or _first_identifier(resource)
        if key is None:
            raise BundleEntryError(resource_type, "no identifier given and the resource has none of its own")
        url = _conditional_url(resource_type, key)
        return BundleEntry(
            fullUrl=self._id_factory(),
            resource=resource,
            request=BundleEntryRequest(method="PUT", url=url, ifNoneExist=f"identifier={key.system}|{key.value}"),
        )

    def create(self, resource: Resource, *, identifier: Identifier | None = None) -> BundleEntry:
        """A conditional `POST` entry: create unless a resource with this identifier already exists."""
        resource_type = type(resource).__name__
        key = identifier or _first_identifier(resource)
        if key is None:
            raise BundleEntryError(resource_type, "no identifier given and the resource has none of its own")
        return BundleEntry(
            fullUrl=self._id_factory(),
            resource=resource,
            request=BundleEntryRequest(
                method="POST", url=resource_type, ifNoneExist=f"identifier={key.system}|{key.value}"
            ),
        )

    def delete(self, resource_type: str, *, identifier: Identifier) -> BundleEntry:
        """A conditional `DELETE` entry, keyed on `identifier`."""
        return BundleEntry(request=BundleEntryRequest(method="DELETE", url=_conditional_url(resource_type, identifier)))
