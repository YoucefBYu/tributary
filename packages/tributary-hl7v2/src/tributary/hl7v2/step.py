"""`tributary.hl7v2.parse`: the `uses:` step a channel spec names to parse an HL7 v2 payload.

Registered as the `tributary.hl7v2.parse` entry point in the `tributary.steps` group
(`pyproject.toml`), so a channel spec's `{"uses": "tributary.hl7v2.parse"}` resolves it with no bundle
fetch (ADR-001 6.2). See `contracts/channel-spec/v0/examples/valid/adt-to-fhir-per-key-mtls.json`.

The payload stays byte-identical on the wire (invariant 8: opaque bytes, no reformatting by a parse
step) -- this step's job is to make the *parsed* `Hl7Message` available to the steps that follow it, in
`ctx.vars["hl7"]`, so a mapping step (`tributary-content-hl7-fhir`, W2-3) never re-parses what this one
already did.
"""

from __future__ import annotations

from tributary.sdk import Context, Message, step

from .errors import Hl7Error
from .message import STRUCTURAL_CODE, Hl7Message

#: The `ctx.vars` key downstream steps read. Not part of the channel-spec vocabulary -- a plain
#: in-process convention between this step and whatever step runs after it in the same pipeline.
VARS_KEY = "hl7"

#: `ctx.vars` key a channel can set from an earlier step to override the decode charset (see
#: `DEFAULT_ENCODING`).
ENCODING_VARS_KEY = "hl7_encoding"

#: Default charset when the channel has not overridden it. The envelope contract deliberately leaves the
#: charset for `application/hl7-v2+er7` to the channel spec (real feeds are frequently ISO-8859-1), so
#: this is a documented default, not a guess dressed up as one. `steps[].with` is not yet wired through
#: to step construction by the runtime (checked in `tributary.runtime.steps.load_step`, wave 2): until it
#: is, a channel with a non-UTF-8 sender sets `ctx.vars["hl7_encoding"]` from an earlier step.
DEFAULT_ENCODING = "utf-8"

#: The `ctx.vars` key `version` (when given) is stashed under, for a step further down the same pipeline
#: that wants the channel's *declared* version rather than re-deriving one from MSH-12.
VERSION_VARS_KEY = "hl7_version"


@step(name="tributary.hl7v2.parse")
def parse(message: Message, ctx: Context, *, version: str | None = None) -> Message:
    """Parse the message's HL7 v2 payload and stash it in `ctx.vars["hl7"]`. Returns `message` unchanged.

    `version` (optional, bound from the channel spec's `with:`, e.g. `{"version": "2.5.1"}`) declares the
    HL7 version the channel expects senders to use. **What it checks:** nothing about the message's
    conformance -- it is stashed verbatim in `ctx.vars["hl7_version"]` (`VERSION_VARS_KEY`) so a step
    further down the same pipeline (a mapping step choosing between v2.3-era and v2.8-era field layouts,
    say) has a value to branch on that does not depend on MSH-12 being populated, which real senders
    frequently leave blank or get wrong. If MSH-12 *is* present and differs from the declared `version`,
    this is logged as a structural mismatch flag only (never the two version strings themselves, which
    are sender-controlled MSH-9/MSH-12 text -- invariant 6 treats them the same as any other field) and
    the message is **not** failed: this parameter is a declaration for downstream steps to use, not a
    conformance-profile check (that is CLAUDE.md's separate, optional "validation" pipeline step).

    Raises `tributary.hl7v2.errors.Hl7Error` (a malformed message, or a claim-checked payload this step
    cannot yet fetch) rather than returning a `Drop`: a parse failure is a pipeline fault that the
    runtime's step-retry-then-DLQ handling owns (ADR-003 D4), not a filtering decision this step is
    making.
    """
    if message.payload is None:
        raise Hl7Error("cannot parse a claim-checked message (payload is None): fetch it before this step")
    encoding = str(ctx.vars.get(ENCODING_VARS_KEY, DEFAULT_ENCODING))
    hl7 = Hl7Message.parse(message.payload, encoding=encoding)
    ctx.vars[VARS_KEY] = hl7

    msg_type, trigger_event, _ = hl7.message_type()
    if STRUCTURAL_CODE.match(msg_type) and STRUCTURAL_CODE.match(trigger_event):
        # Only ever a known-shape code, never sender-controlled free text: MSH-9 is sender data, and
        # `ctx.set_attr` rejects anything else anyway (a space would DLQ the message) -- but silently
        # losing the attr on a malformed MSH-9 is safer than a parse step that fails on it (a security
        # review found the un-allowlisted version could still put close-to-arbitrary MSH-9 content into
        # an archived, searchable field; this closes that, not just the crash).
        ctx.set_attr("hl7_type", f"{msg_type}_{trigger_event}")

    if version is not None:
        ctx.vars[VERSION_VARS_KEY] = version
        actual_version = hl7.version()
        if actual_version and actual_version != version:
            # Boolean only, never the two version strings (see the docstring above): a mismatch is
            # visible without echoing sender-controlled MSH-12 text into a log line.
            ctx.log.debug("hl7_version_mismatch", declared_version_matches_msh12=False)

    ctx.log.debug("hl7_parsed", segment_count=len(hl7))
    return message
