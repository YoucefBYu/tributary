"""`Hl7Message`: a lazily-parsed HL7 v2 (ER7, pipe-delimited) message.

**Lazy, matching S1's winning design** (`docs/spikes/S1-hl7-throughput.md`): parsing splits the message
into segments only -- an O(1) byte split -- and a segment's fields are decoded and split on first access,
then cached. A step that reads three fields out of a 40-segment ORU never decodes the other 39.

**Round-trip guarantee:** `Hl7Message.parse(raw).to_bytes() == raw` always, for any message this class
can parse, because an untouched message never leaves its original bytes -- `to_bytes()` returns the
stored `raw` unless `with_field` was called, in which case the *new* message's `raw` is the deterministic
reconstruction (original bytes, with only the addressed field's segment rebuilt). Hypothesis property
tests in `tests/test_roundtrip.py` cover this across generated delimiters, repetitions and escapes.

Immutable, like `tributary.sdk.Message`: every mutator returns a new `Hl7Message`.
"""

from __future__ import annotations

import re

from . import paths
from .delimiters import Delimiters
from .delimiters import extract as extract_delimiters
from .errors import FieldPathError, MalformedMessageError, PayloadDecodeError, SegmentNotFoundError
from .escaping import escape, unescape
from .paths import FieldPath

#: A structural HL7 v2 code (a message type or trigger event component, e.g. `ADT`, `A01`): three
#: uppercase letters or digits. Used to allowlist sender-supplied `MSH-9` components before they are
#: ever echoed into new content this package produces -- an ACK's own `MSH-9` (`ack.py`) or an
#: `_tb.events` `attrs` entry (`tributary.hl7v2.step`) -- rather than relying on escaping alone (a
#: security review found the escaping route incomplete; an allowlist does not depend on staying complete).
STRUCTURAL_CODE = re.compile(r"^[A-Z0-9]{3}$")


def _detect_separator(raw: bytes) -> bytes:
    """The segment separator: `\\r\\n`, `\\r` or `\\n`, chosen by which one every non-trailing `\\r` in
    the message is (or is not) followed by.

    A single global "does `\\r\\n` occur anywhere" check is what a security and code review both flagged
    (independently): a message that genuinely uses bare `\\r` between segments but happens to carry a
    `\\r\\n` byte pair *inside* one field's data would be misread as CRLF-separated, silently merging
    every bare-`\\r`-terminated segment that follows into one. Scanning every `\\r` and requiring they
    agree turns that silent corruption into a loud, typed `MalformedMessageError` instead.
    """
    lf_after_cr = 0
    cr_alone = 0
    length = len(raw)
    for index, byte in enumerate(raw):
        if byte != 0x0D:  # b"\r"
            continue
        if index + 1 == length:
            continue  # a trailing CR with nothing after it is not evidence either way
        if raw[index + 1] == 0x0A:  # b"\n"
            lf_after_cr += 1
        else:
            cr_alone += 1
    if lf_after_cr and cr_alone:
        raise MalformedMessageError("message mixes CRLF and bare CR segment separators")
    if lf_after_cr:
        return b"\r\n"
    if cr_alone or b"\r" in raw:
        return b"\r"
    return b"\n" if b"\n" in raw else b"\r"


class Segment:
    """A lazy view onto one segment's raw bytes. Field decoding and splitting happen on first access."""

    __slots__ = ("_delimiters", "_encoding", "_fields_cache", "_raw", "name")

    def __init__(self, raw: bytes, *, delimiters: Delimiters, encoding: str) -> None:
        self._raw = raw
        self._delimiters = delimiters
        self._encoding = encoding
        # Fixed at 3 characters, matching `Hl7Message.__init__`'s index: the standard fixes every
        # segment name at exactly 3 characters, so this is a spec assumption, not a heuristic guess (a
        # code review found this used to search for the field separator instead, which made `name` the
        # entire segment for one with no separator in it at all).
        self.name = raw[:3].decode("ascii", errors="replace").upper()
        self._fields_cache: tuple[str, ...] | None = None

    def _fields(self) -> tuple[str, ...]:
        if self._fields_cache is None:
            try:
                text = self._raw.decode(self._encoding)
            except (UnicodeDecodeError, LookupError):
                # `from None`: a `UnicodeDecodeError`'s `.object` attribute holds the undecodable bytes
                # themselves -- chaining it would let that PHI reach anything that walks the exception
                # chain (a debugger, a reflective log serializer), even though nothing in this codebase's
                # own logging does (invariant 6; a security review found this chained).
                raise PayloadDecodeError(self._encoding) from None
            self._fields_cache = tuple(text.split(self._delimiters.field))
        return self._fields_cache

    def raw_bytes(self) -> bytes:
        return self._raw

    def _field_raw(self, number: int) -> str:
        """The raw (still-escaped) text of field `number`, `""` if the field is absent (a truncated
        trailing field is ordinary and not an error). Handles the MSH-1/MSH-2 offset (`delimiters.py`)."""
        fields = self._fields()
        if self.name == "MSH":
            if number == 1:
                return self._delimiters.field
            index = number - 1
        else:
            index = number
        return fields[index] if 0 <= index < len(fields) else ""

    def field(
        self, number: int, *, repetition: int = 1, component: int | None = None, subcomponent: int | None = None
    ) -> str:
        """Bare-integer field access for a `Segment` already obtained from `Hl7Message.segment`/
        `segments` -- convenient when iterating occurrences (`for obx in msg.segments("OBX"): obx.field(5)`)
        without re-parsing a path string that would have to repeat the segment name. Same 1-based
        indexing and the same "missing trailing structure is `''`, not an error" rule as `get`.
        """
        for label, value in (
            ("field", number),
            ("repetition", repetition),
            ("component", component),
            ("subcomponent", subcomponent),
        ):
            if value is not None and value < 1:
                raise FieldPathError(f"{self.name}.{number}", f"{label} must be 1 or greater, got {value}")
        path = FieldPath(
            segment=self.name,
            segment_occurrence=1,
            field=number,
            repetition=repetition,
            component=component,
            subcomponent=subcomponent,
        )
        return self.get(path)

    def get(self, path: FieldPath) -> str:
        """Resolve a parsed path against this segment, returning unescaped (display) text.

        Missing trailing repetitions, components or subcomponents return `""`, matching how real HL7
        senders truncate optional trailing structure -- that is data absence, not a parse error.
        """
        if path.field is None:
            return unescape(self._text(), self._delimiters)
        if self.name == "MSH" and path.field in (1, 2):
            # MSH-1 (the field separator) and MSH-2 (the other four delimiters) *are* the delimiters:
            # splitting or unescaping them with themselves is undefined, not merely wrong -- MSH-2's own
            # text ("^~\&") contains the repetition separator as one of its four characters, so running
            # it through the normal repetition/component split corrupts it. Returned as-is, verbatim.
            return self._field_raw(path.field)
        field_text = self._field_raw(path.field)
        return unescape(_resolve(field_text, path, self._delimiters), self._delimiters)

    def repetitions(self, path: FieldPath) -> tuple[str, ...]:
        if path.field is None:
            raise FieldPathError(path.segment, "repetitions() needs a field index, e.g. 'PID.3'")
        field_text = self._field_raw(path.field)
        reps = field_text.split(self._delimiters.repetition)
        return tuple(
            unescape(_resolve_within_repetition(rep, path, self._delimiters), self._delimiters) for rep in reps
        )

    def raw(self, path: FieldPath) -> str:
        """Like `get`, but skips `unescape`: the still-escaped wire text at `path`.

        For copying a field verbatim into a *new* message (`ack.py`'s echoed `MSH` fields) without ever
        decoding sender-controlled escape sequences into structural characters -- decoding and
        re-encoding is where a security review found a control-character injection (a decoded value
        spliced into a new segment with an incompletely escaping `escape()`). Copying the raw text needs
        no re-escaping at all, because it was already valid wire text the moment it arrived.
        """
        if path.field is None:
            return self._text()
        if self.name == "MSH" and path.field in (1, 2):
            return self._field_raw(path.field)
        return _resolve(self._field_raw(path.field), path, self._delimiters)

    def _text(self) -> str:
        return self._delimiters.field.join(self._fields())


def _resolve_within_repetition(rep_text: str, path: FieldPath, delimiters: Delimiters) -> str:
    if path.component is None:
        return rep_text
    comps = rep_text.split(delimiters.component)
    comp_text = comps[path.component - 1] if 0 <= path.component - 1 < len(comps) else ""
    if path.subcomponent is None:
        return comp_text
    subs = comp_text.split(delimiters.subcomponent)
    return subs[path.subcomponent - 1] if 0 <= path.subcomponent - 1 < len(subs) else ""


def _resolve(field_text: str, path: FieldPath, delimiters: Delimiters) -> str:
    reps = field_text.split(delimiters.repetition)
    rep_text = reps[path.repetition - 1] if 0 <= path.repetition - 1 < len(reps) else ""
    return _resolve_within_repetition(rep_text, path, delimiters)


class Hl7Message:
    """An immutable, lazily-parsed HL7 v2 message."""

    __slots__ = ("_delimiters", "_encoding", "_index", "_raw", "_seg_sep", "_segment_bytes", "_segment_cache")

    def __init__(
        self,
        raw: bytes,
        *,
        encoding: str,
        seg_sep: bytes,
        segment_bytes: tuple[bytes, ...],
        delimiters: Delimiters,
    ) -> None:
        self._raw = raw
        self._encoding = encoding
        self._seg_sep = seg_sep
        self._segment_bytes = segment_bytes
        self._delimiters = delimiters
        self._segment_cache: dict[int, Segment] = {}
        index: dict[str, list[int]] = {}
        for position, chunk in enumerate(segment_bytes):
            if not chunk:
                continue  # a trailing empty chunk from a trailing separator; not a segment
            # The standard fixes every segment name at exactly 3 characters (unlike field/component
            # counts, which are open-ended), so this is a spec assumption, not a heuristic guess.
            name = chunk[:3].decode("ascii", errors="replace").upper()
            index.setdefault(name, []).append(position)
        self._index = index

    # -- construction ------------------------------------------------------------------------------

    #: The envelope contract's inline payload limit (`contracts/envelope/v0`): a message larger than this
    #: was never meant to travel inline on Kafka at all, so `parse`'s default `max_bytes` matches it as a
    #: defensive cap, independent of whatever already enforced (or failed to enforce) that limit upstream.
    INLINE_LIMIT_BYTES = 1_000_000

    @classmethod
    def parse(
        cls,
        raw: bytes,
        *,
        encoding: str = "utf-8",
        max_bytes: int | None = INLINE_LIMIT_BYTES,
        max_segments: int | None = None,
    ) -> Hl7Message:
        """Parse a complete HL7 v2 message from bytes.

        Only segment boundaries and `MSH-1`/`MSH-2` are read eagerly; everything else is lazy. Raises
        `MalformedMessageError` for anything that cannot be assigned segment boundaries or delimiters at
        all -- never a bare `IndexError`, `UnicodeDecodeError` or similar (W2-1: "malformed input raises
        a typed exception").

        `max_bytes` (default `INLINE_LIMIT_BYTES`) and `max_segments` (default unbounded, since real
        batch and large-ORU messages vary widely) are defensive caps, checked before any parsing work:
        without one, a caller assembling an accessor path from unbounded input (a huge segment or
        occurrence count) has no bound on the work `with_field` or `segments` will do for a single
        message. Pass `None` to disable either check explicitly.
        """
        if not isinstance(raw, bytes | bytearray | memoryview):
            raise MalformedMessageError("raw message must be bytes")
        raw = bytes(raw)
        if not raw:
            raise MalformedMessageError("message is empty")
        if max_bytes is not None and len(raw) > max_bytes:
            raise MalformedMessageError(f"message is {len(raw)} bytes, over the {max_bytes}-byte limit")
        seg_sep = _detect_separator(raw)
        segment_bytes = tuple(raw.split(seg_sep))
        if max_segments is not None:
            # Non-empty chunks only, matching `len(Hl7Message(...))`: a trailing separator's empty
            # chunk is bookkeeping, not a segment, and must not count against a caller's stated cap.
            actual_segments = sum(1 for chunk in segment_bytes if chunk)
            if actual_segments > max_segments:
                raise MalformedMessageError(f"message has over {max_segments} segments")
        if not segment_bytes or not segment_bytes[0]:
            raise MalformedMessageError("message has no first segment")
        delimiters = extract_delimiters(segment_bytes[0], encoding=encoding)
        return cls(raw, encoding=encoding, seg_sep=seg_sep, segment_bytes=segment_bytes, delimiters=delimiters)

    # -- serialization -------------------------------------------------------------------------------

    def to_bytes(self) -> bytes:
        """The wire bytes. Byte-identical to the parsed input unless `with_field` produced this message."""
        return self._raw

    def text(self, *, encoding: str | None = None) -> str:
        return self._raw.decode(encoding or self._encoding)

    # -- structure -------------------------------------------------------------------------------

    @property
    def delimiters(self) -> Delimiters:
        return self._delimiters

    @property
    def encoding(self) -> str:
        return self._encoding

    def segment_count(self, name: str) -> int:
        return len(self._index.get(name.upper(), ()))

    def segments(self, name: str) -> tuple[Segment, ...]:
        """Every occurrence of `name`, in message order. Empty tuple if there are none."""
        return tuple(self._segment(position) for position in self._index.get(name.upper(), ()))

    def segment(self, name: str, occurrence: int = 1) -> Segment:
        """The `occurrence`-th (1-based) occurrence of segment `name`. Raises `SegmentNotFoundError`."""
        if occurrence < 1:
            raise SegmentNotFoundError(name, occurrence)
        positions = self._index.get(name.upper(), ())
        if occurrence > len(positions):
            raise SegmentNotFoundError(name, occurrence)
        return self._segment(positions[occurrence - 1])

    def _segment(self, position: int) -> Segment:
        cached = self._segment_cache.get(position)
        if cached is None:
            cached = Segment(self._segment_bytes[position], delimiters=self._delimiters, encoding=self._encoding)
            self._segment_cache[position] = cached
        return cached

    # -- field access ----------------------------------------------------------------------------

    def get(self, path: str, default: str | None = None) -> str | None:
        """The unescaped text at `path`, or `default` if its segment is not present.

        A present segment with an absent field, repetition, component or subcomponent returns `""`, not
        `default`: those are ordinary truncated-trailing-structure cases, not "the message lacks this".
        """
        parsed = paths.parse(path)
        try:
            segment = self.segment(parsed.segment, parsed.segment_occurrence)
        except SegmentNotFoundError:
            return default
        return segment.get(parsed)

    def require(self, path: str) -> str:
        """The unescaped text at `path`. Raises `SegmentNotFoundError` if the segment is absent."""
        parsed = paths.parse(path)
        segment = self.segment(parsed.segment, parsed.segment_occurrence)
        return segment.get(parsed)

    def __getitem__(self, path: str) -> str:
        return self.require(path)

    def raw(self, path: str, default: str | None = None) -> str | None:
        """Like `get`, but skips `unescape`: the still-escaped wire text at `path`, or `default` if its
        segment is not present. See `Segment.raw` for why this exists."""
        parsed = paths.parse(path)
        try:
            segment = self.segment(parsed.segment, parsed.segment_occurrence)
        except SegmentNotFoundError:
            return default
        return segment.raw(parsed)

    def repetitions(self, path: str) -> tuple[str, ...]:
        """Every repetition of the field at `path` (the path's own `[repetition]`, if any, is ignored)."""
        parsed = paths.parse(path)
        segment = self.segment(parsed.segment, parsed.segment_occurrence)
        return segment.repetitions(parsed)

    # -- MSH convenience ---------------------------------------------------------------------------

    def message_type(self) -> tuple[str, str, str]:
        """`(MSH-9.1, MSH-9.2, MSH-9.3)`: message type, trigger event, message structure. `""` for any
        component the sender omitted (common for message structure pre-v2.4)."""
        return (self.get("MSH.9.1") or "", self.get("MSH.9.2") or "", self.get("MSH.9.3") or "")

    def control_id(self) -> str:
        return self.get("MSH.10") or ""

    def version(self) -> str:
        return self.get("MSH.12") or ""

    # -- mutation (copy-on-write) ------------------------------------------------------------------

    def with_field(self, path: str, value: str) -> Hl7Message:
        """A copy with the text at `path` replaced by `value` (auto-escaped), everything else untouched.

        `value` is the *display* text for exactly the level addressed: setting `"PID.5.1"` replaces only
        that component, leaving sibling components, other fields and every other segment byte-identical
        to the source. Setting a bare field (no component index) replaces the whole field's raw text.
        Cannot target `MSH.1` or `MSH.2` (the delimiters themselves, not stored fields -- see `Segment.get`)
        or a segment occurrence that does not exist -- adding new segments is out of scope for v0.
        """
        parsed = paths.parse(path)
        if parsed.field is None:
            raise FieldPathError(path, "with_field needs a field index")
        if parsed.segment.upper() == "MSH" and parsed.field in (1, 2):
            raise FieldPathError(path, "MSH-1 and MSH-2 are the message's delimiters and cannot be set")

        positions = self._index.get(parsed.segment, ())
        if parsed.segment_occurrence > len(positions) or parsed.segment_occurrence < 1:
            raise SegmentNotFoundError(parsed.segment, parsed.segment_occurrence)
        position = positions[parsed.segment_occurrence - 1]
        segment = self._segment(position)

        new_field_text = _with_replacement(segment._field_raw(parsed.field), parsed, value, self._delimiters)
        fields = list(segment._fields())
        field_index = (parsed.field - 1) if segment.name == "MSH" else parsed.field
        while field_index >= len(fields):
            fields.append("")
        fields[field_index] = new_field_text
        new_segment_text = self._delimiters.field.join(fields)
        new_segment_bytes = new_segment_text.encode(self._encoding)

        new_chunks = list(self._segment_bytes)
        new_chunks[position] = new_segment_bytes
        new_raw = self._seg_sep.join(new_chunks)
        return Hl7Message.parse(new_raw, encoding=self._encoding)

    # -- dunder ------------------------------------------------------------------------------------

    def __len__(self) -> int:
        """The number of segments (structural, safe to log -- see `tributary.hl7v2.step`)."""
        return sum(1 for chunk in self._segment_bytes if chunk)

    def __repr__(self) -> str:
        """Structure only -- segment names and counts, never field content (invariant 6)."""
        counts = ", ".join(f"{name}x{len(positions)}" for name, positions in self._index.items())
        return f"Hl7Message(bytes={len(self._raw)}, segments=[{counts}])"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Hl7Message):
            return NotImplemented
        return self._raw == other._raw

    __hash__ = None  # type: ignore[assignment]


def _with_replacement(field_text: str, path: FieldPath, value: str, delimiters: Delimiters) -> str:
    """Rebuild a field's raw text with one repetition/component/subcomponent slot replaced."""
    escaped_value = escape(value, delimiters)
    reps = field_text.split(delimiters.repetition)
    while path.repetition > len(reps):
        reps.append("")
    rep_index = path.repetition - 1

    if path.component is None:
        reps[rep_index] = escaped_value
        return delimiters.repetition.join(reps)

    comps = reps[rep_index].split(delimiters.component)
    while path.component > len(comps):
        comps.append("")
    comp_index = path.component - 1

    if path.subcomponent is None:
        comps[comp_index] = escaped_value
    else:
        subs = comps[comp_index].split(delimiters.subcomponent)
        while path.subcomponent > len(subs):
            subs.append("")
        subs[path.subcomponent - 1] = escaped_value
        comps[comp_index] = delimiters.subcomponent.join(subs)

    reps[rep_index] = delimiters.component.join(comps)
    return delimiters.repetition.join(reps)
