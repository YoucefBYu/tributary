"""`tributary.core.enrich`: add searchable `attrs` and `tb-usr-*` headers (channel-spec README 3.3.2).

    - name: tag
      uses: tributary.core.enrich
      with:
        attrs: ["feed=adt-main"]
        usr: ["route=primary"]
        attrFrom: ["sender=hl7:MSH.3", "trigger=hl7:MSH.9.2"]

`with` admits no nested objects, so each map is a flat array of `name=value` strings.

* `attrs` and `usr` are **static**: the values are in the spec. `usr` is static only, because copying
  payload into a header launders PHI past every guard that looks at payloads.
* `attrFrom` copies a field into an attr, and only from an allow-list of message-header fields that
  describe the *feed*, not the patient: `MSH.3`-`MSH.6`, the `MSH.9` components, `MSH.11`, `MSH.12`,
  `EVN.1`, `json:/resourceType`. The value still passes the `attrs` guard (short code charset,
  identifier names refused); a value that fails it - a sender application with a space in it - is
  skipped and logged by attr name and reason, never by value. Enrichment is metadata: it never fails a
  clinical message.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Final

from tributary.sdk import Context, Message
from tributary.sdk import headers as h
from tributary.sdk.errors import AttrError, UsrHeaderError, safe_token
from tributary.sdk.guards import MAX_ATTRS, validate_attr, validate_usr
from tributary.sdk.keys import ResolvedKey, evaluate_key

from .base import BuiltinStep, check_pairs, locator, pairs, parse_locator, refuse

NAME: Final = "tributary.core.enrich"

#: README 3.3.2, trimmed by the architect on 2026-09-24. Exact locator text.
ATTR_FROM_ALLOWED: Final = frozenset(
    {
        "hl7:MSH.3",
        "hl7:MSH.4",
        "hl7:MSH.5",
        "hl7:MSH.6",
        "hl7:MSH.9.1",
        "hl7:MSH.9.2",
        "hl7:MSH.9.3",
        "hl7:MSH.11",
        "hl7:MSH.12",
        "hl7:EVN.1",
        "json:/resourceType",
    }
)


def _check(step: str, values: Mapping[str, object]) -> None:
    if not any(values.get(key) for key in ("attrs", "usr", "attrFrom")):
        raise refuse(step, "nothing to do: set at least one of `attrs`, `usr`, `attrFrom`")
    static = check_pairs(step, "attrs", values.get("attrs", ()))
    for name, value in static:
        try:
            validate_attr(name, value)
        except AttrError:
            raise refuse(
                step, f"`attrs` entry {safe_token(name)} breaks the events attrs rules (events/v0 3)"
            ) from None
    for name, value in check_pairs(step, "usr", values.get("usr", ())):
        try:
            validate_usr(name, value)
        except UsrHeaderError:
            raise refuse(step, "a `usr` entry breaks the tb-usr-* rules (envelope/v0 2.2)") from None
    derived = check_pairs(step, "attrFrom", values.get("attrFrom", ()))
    for name, text in derived:
        try:
            validate_attr(name, "x")
        except AttrError:
            raise refuse(step, "an `attrFrom` name breaks the events attrs rules (events/v0 3)") from None
        parse_locator(step, "attrFrom", text)
        if text not in ATTR_FROM_ALLOWED:
            raise refuse(
                step,
                "`attrFrom` may only read "
                + ", ".join(sorted(ATTR_FROM_ALLOWED))
                + " (README 3.3.2): other fields can identify a patient",
            )
    names = [name for name, _value in (*static, *derived)]
    if len(set(names)) != len(names):
        raise refuse(step, "an attr is named in both `attrs` and `attrFrom`")
    if len(names) > MAX_ATTRS:
        raise refuse(step, f"more than {MAX_ATTRS} attrs (events/v0 3)")


def _enrich(
    message: Message,
    ctx: Context,
    *,
    attrs: Sequence[str] = (),
    usr: Sequence[str] = (),
    attrFrom: Sequence[str] = (),  # camelCase: the contract's key, bound by exact name
) -> None:
    msg_id = message.get(h.TB_MSG_ID) or ""
    for name, value in pairs(tuple(attrs)):
        _set_attr(ctx, msg_id, name, value)
    for name, value in pairs(tuple(usr)):
        ctx.set_usr(name, value)  # validated at bind; a static value cannot change per message
    if not attrFrom:
        return
    payload = message.payload or b""
    content_type = message.get(h.TB_CONTENT_TYPE) or ""
    for name, text in pairs(tuple(attrFrom)):
        result = evaluate_key(locator(text), payload=payload, content_type=content_type)
        if not isinstance(result, ResolvedKey):
            ctx.log.debug("enrich_attr_skipped", message_id=msg_id, attr=name, reason=result.reason)
            continue
        try:
            value = result.key.decode("ascii")
        except UnicodeDecodeError:
            ctx.log.debug("enrich_attr_skipped", message_id=msg_id, attr=name, reason="ATTR_VALUE_NOT_ASCII")
            continue
        _set_attr(ctx, msg_id, name, value)


def _set_attr(ctx: Context, msg_id: str, name: str, value: str) -> None:
    try:
        ctx.set_attr(name, value)
    except AttrError:
        # The guard's own message is not logged: it is built by the SDK, but the value it judged came
        # from the payload, and "never log a payload-derived value" is simpler to keep than to audit.
        ctx.log.debug("enrich_attr_skipped", message_id=msg_id, attr=name, reason="ATTR_VALUE_INVALID")


enrich = BuiltinStep(_enrich, name=NAME, check=_check)
