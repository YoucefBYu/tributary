"""HL7 v2 escape sequences (standard section 2.7 / Table 0356 equivalent), decode and encode.

A component's raw text can carry the message's own delimiters or the escape character itself only by
escaping them, so `unescape` turns wire text into display text and `escape` is its exact inverse for a
value a step wants to write back. Round-tripping an untouched message never calls either: `Hl7Message`
returns the original bytes unchanged (see `message.py`), so these only run when a step actually reads or
writes a field's decoded value.
"""

from __future__ import annotations

from .delimiters import Delimiters

#: Highlighting and local-escape markers that have no single-character meaning in this engine. They are
#: still recognised (never left half-decoded) but pass through as empty text, matching what most
#: consumers that do not render rich text do with them.
_STRIPPED = frozenset({"H", "N"})


def unescape(text: str, delimiters: Delimiters) -> str:
    """Decode `\\F\\`, `\\S\\`, `\\T\\`, `\\R\\`, `\\E\\`, `\\Xdd..\\` (hex) and `\\H\\`/`\\N\\`.

    Total: an escape sequence this function does not recognise (a `\\Zxx\\`-style local extension, or a
    trailing unterminated `\\`) is passed through literally rather than raising, because a step reading a
    field should not crash the message over a vendor Z-escape it does not need. That mirrors the header
    parsers' "reject the record, don't crash the process" rule at the level this module owns: a decode
    surprise here is a wrong string, not an unhandled exception.
    """
    esc = delimiters.escape
    if esc not in text:
        return text
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        char = text[i]
        if char != esc:
            out.append(char)
            i += 1
            continue
        end = text.find(esc, i + 1)
        if end == -1:
            # Unterminated escape: keep the literal tail rather than dropping or raising.
            out.append(text[i:])
            break
        body = text[i + 1 : end]
        out.append(_decode_one(body, delimiters))
        i = end + 1
    return "".join(out)


def _decode_one(body: str, delimiters: Delimiters) -> str:
    if body == "F":
        return delimiters.field
    if body == "S":
        return delimiters.component
    if body == "T":
        return delimiters.subcomponent
    if body == "R":
        return delimiters.repetition
    if body == "E":
        return delimiters.escape
    if body in _STRIPPED:
        return ""
    if body.startswith("X") and len(body) > 1:
        hex_digits = body[1:]
        if len(hex_digits) % 2 == 0:
            try:
                return bytes.fromhex(hex_digits).decode("latin-1")
            except ValueError:
                pass
    # Unrecognised sequence (a local Z-escape, malformed hex, etc): return it unchanged, escape markers
    # included, so information is never silently discarded.
    return f"{delimiters.escape}{body}{delimiters.escape}"


def escape(text: str, delimiters: Delimiters) -> str:
    """Encode a display string into HL7 wire text: the exact inverse of `unescape` for the five reserved
    characters (escape sequences this module does not generate on decode, like `\\H\\`, are never produced
    here either -- a step writing a value only needs the delimiters escaped, never highlighting markers).

    Also hex-escapes (`\\Xhh\\`) every C0 control character (`0x00`-`0x1F`) and DEL (`0x7F`), not just CR:
    a bare LF, VT or FS in a value a step writes back (or that `ack.py` echoes) would otherwise be
    indistinguishable from a segment or field boundary once serialized, or from the MLLP end-of-block
    byte once framed -- a security review found exactly that gap (LF injecting a second segment) and it
    generalises to the whole C0 range, not only the two or three bytes a proof of concept happened to use.
    """
    esc = delimiters.escape
    replacements = {
        esc: f"{esc}E{esc}",
        delimiters.field: f"{esc}F{esc}",
        delimiters.component: f"{esc}S{esc}",
        delimiters.subcomponent: f"{esc}T{esc}",
        delimiters.repetition: f"{esc}R{esc}",
    }
    out: list[str] = []
    for char in text:
        if char in replacements:
            out.append(replacements[char])
        elif ord(char) < 0x20 or ord(char) == 0x7F:
            out.append(f"{esc}X{ord(char):02X}{esc}")
        else:
            out.append(char)
    return "".join(out)
