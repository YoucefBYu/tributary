"""Lazy HL7 v2 (ER7) parser, serializer, accessors and ACK builder.

    from tributary.hl7v2 import Hl7Message

    msg = Hl7Message.parse(raw_bytes, encoding="utf-8")
    mrn = msg["PID.3.1"]
    msg.to_bytes() == raw_bytes  # true for any message untouched by with_field

Segments and fields are decoded and split lazily, on first access, and only the segment or field a
caller actually touches is ever decoded (`docs/spikes/S1-hl7-throughput.md`, S1). Parsing is defensive:
malformed input raises a typed `Hl7Error` subclass whose message never contains segment content
(invariant 6) -- see `errors.py`.

`tributary.hl7v2.parse` is also registered as a pipeline step (`tributary.steps` entry point,
`step.py`), so a channel spec can select it with `{"uses": "tributary.hl7v2.parse"}` and no bundle
fetch (ADR-001 6.2).

This package has no third-party dependencies beyond `tributary-sdk` (used only by `step.py`) and never
opens a socket.
"""

from __future__ import annotations

from .ack import AckCode, build_ack
from .delimiters import Delimiters
from .errors import (
    AckBuildError,
    FieldPathError,
    Hl7Error,
    MalformedMessageError,
    PayloadDecodeError,
    SegmentNotFoundError,
)
from .escaping import escape, unescape
from .message import Hl7Message, Segment
from .paths import FieldPath
from .paths import parse as parse_path

__all__ = [
    "AckBuildError",
    "AckCode",
    "Delimiters",
    "FieldPath",
    "FieldPathError",
    "Hl7Error",
    "Hl7Message",
    "MalformedMessageError",
    "PayloadDecodeError",
    "Segment",
    "SegmentNotFoundError",
    "build_ack",
    "escape",
    "parse_path",
    "unescape",
]
