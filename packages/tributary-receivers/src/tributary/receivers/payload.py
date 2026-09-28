"""Body checks shared by the `http`, `sftp` and `kafka` receivers for `ack.nackOn: [parse-error]`."""

from __future__ import annotations

import json

__all__ = ["well_formed"]

_JSON_TYPES = frozenset({"application/json", "application/fhir+json"})


def well_formed(content_type: str, body: bytes) -> bool:
    """Only JSON is checked (`json.loads`, in-process, bounded by `maxMessageBytes`); other types pass,
    because a receiver that does not know their grammar must not guess at it. Never raises, and never
    returns or logs any part of `body`."""
    if content_type.split(";", 1)[0].strip().lower() not in _JSON_TYPES:
        return True
    try:
        json.loads(body)
    except (ValueError, RecursionError):
        return False
    return True
