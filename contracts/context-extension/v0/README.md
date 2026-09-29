# Contract: context extensions (`tributary.context` entry points), v0 (draft)

**Contract id:** `tributary.context-extension/v0`
**Status:** Draft, **new in contract set 1.5 (2026-09-29)**. Additive: no document valid under set 1.4
becomes invalid. Breaking changes create `../v1/` and need an ADR.
**Owner:** `architect`. Propose changes by message with the exact diff and the reason.
**Source:** ADR-007 D21-D22 (amendment 3); security review High on `fb60f3a` (any installed
distribution registering `tributary.context` ran in every worker with the payload and `ctx.secrets`).
**Machine-readable:** [`capabilities.json`](capabilities.json), the registry of capability names.
**Declared by:** `channel-spec/v0` `spec.pipeline.context` (README 3.3.4).

A context extension adds a capability to the `ctx` a step receives, for example `ctx.fhir`, without
the runtime importing the package that owns it. `Context` is frozen for set 1.x (channel-spec README
3.2.6), so this is how a new capability arrives. Extensions are **trusted code**, in the same class as
a `uses` step (ADR-001 section 8). This contract does not sandbox them. It fixes three things: which
extensions run (the spec names them), which distribution may supply each name (this registry), and
what an extension must preserve (section 3).

---

## 1. The entry point

| Item | Value |
|---|---|
| Group | `tributary.context` |
| Entry-point name | The **capability name**. Grammar `^[a-z][a-z0-9_]{0,31}$`. Must be listed in `capabilities.json` |
| Entry-point value | A zero-argument **factory**: `module.path:callable` |
| Factory signature | `() -> ContextExtension` |
| `ContextExtension` | `Callable[[Context, Message], Context]` (`tributary.runtime.context.ContextExtension`) |

```toml
[project.entry-points."tributary.context"]
fhir = "tributary.fhir.runtime:context_extension"
```

**The factory** is called at most once per worker process per capability, before the worker consumes
a record for a channel version that declares it. The runtime may cache its result for the process
lifetime. This is where start-up work belongs, such as reading a code table shipped in the image.

**The returned callable** is called once per `WorkerContext` that runs a step: a message, a split
child, or one destination's copy. It receives that context as `base` and the message the branch
starts from. It is called before the first step on that branch, and every step on the branch receives
the same returned object.

**Return type.** A `Context` that wraps `base`. **If `base` is a `ReplyContext`, the return must be a
`ReplyContext`** whose `reply` forwards to `base.reply` (channel-spec README 3.2.6). A plain `Context`
base may yield a plain `Context`. The capability's own members (`.fhir`, `.terminology`) are typed by
the owning package's sub-protocol (`tributary.fhir.FhirContext`, `FhirReplyContext`).

## 2. How the runtime loads extensions

Normative for `tributary-runtime`. Rules L1-L7.

1. **L1. Only declared capabilities are loaded.** The worker loads exactly the names in the applied
   spec's `spec.pipeline.context`. A channel without `context` gets none. Discovery reads entry-point
   metadata only. **`EntryPoint.load()` is never called for an undeclared name**, so an undeclared
   registrant's module is never imported.
2. **L2. Each declared name resolves to exactly one registrant, from the registered distribution.**
   The name must be in `capabilities.json`. Exactly one installed entry point in the group must carry
   it. Its distribution name, normalised per PEP 503, must equal the registry's `distribution`.
   Otherwise the spec is refused (`ChannelSpecError`). At startup that refuses the worker. On a hot
   reload the previous version keeps running, as for an unresolvable `uses` step. A duplicate name
   **that no spec declares** is not an error, but see L5.
3. **L3. The probe runs on every loaded extension** before the first record for that version, against
   a pipeline context and a destination context. It checks what the worker reads from the base rather
   than from the view:
   - `set_attr`, `set_usr`, `vars` and `reply` delegation;
   - on the pipeline context, a real `view.reply(...)` call that lands in `base.reply_body`; on the
     destination context, `reply` must raise;
   - identity: `view.log is base.log` and `view.secrets is base.secrets`.
   A failure refuses the spec, per L2.
4. **L4. Wrapping order** is by capability name, ascending. `context` is a set, and its order in the
   spec carries no meaning.
5. **L5. Log lines** (structured, no payload, no header values, invariant 6):
   - `context_extension_loaded`: `extension`, `distribution`, `version` (the distribution's version
     from its metadata), `channel`, `channel_version`. One per capability per applied version.
   - `context_extension_skipped`: the same metadata fields plus `reason`, for each installed
     registrant that is not loaded. `reason` is `not-declared` (info level), or `unregistered` or
     `distribution-mismatch` (warning level). The last two are the signal for a transitive or
     typosquatted package in the image, and they do not stop the worker, because nothing was imported.
6. **L6. Hot reload.** A new channel version's `context` is resolved together with its steps, all or
   nothing. A capability dropped from `context` is no longer applied to new branches. Its module stays
   imported, because Python cannot unload one. That is acceptable because it was a declared, reviewed
   capability of an earlier version.
7. **L7. Test override.** An explicit tuple of extensions passed to the worker replaces discovery.
   It exists for tests only, and no CLI flag or environment variable reaches it.

**Lenient consumers, with one exception.** The runtime ignores fields it does not know (channel-spec
1.1). It does **not** ignore a `context` value it cannot satisfy. An unknown or unresolvable name is a
refused spec, never a dropped capability. Otherwise every message would dead-letter on
`AttributeError` with nothing at startup to say why.

## 3. What an extension must keep

Normative for every registrant. The probe (L3) checks P1 mechanically, and only partly. **P2-P6 are
not machine-checked.** They are held by review, by the registrant's own tests, and by the registry
gate in section 4.

| # | Rule | Serves |
|---|---|---|
| P1 | **Delegate, do not replace.** `channel`, `channel_version`, `instance`, `destination`, `attempt`, `traceparent`, `now()`, `set_attr`, `attrs()`, `set_usr`, `usr_headers()` forward to `base`. `vars`, `log`, `secrets` and `kv` return **the base's own objects** (identity, not a copy or a wrapper). A `ReplyContext` base yields a `ReplyContext` whose `reply` forwards to `base.reply` | The worker reads attrs, `tb-usr-*`, `vars` and the reply body from the base, so a wrapper that keeps its own copy loses them silently |
| P2 | **No logger of its own.** No `logging.getLogger`, `print`, or third-party logger, in the factory, the callable, or the capability's methods. Log through `ctx.log` only: event names, `tb-msg-id` and codes, never payload, field values or identifiers. Exceptions are typed, and their text carries no payload or identifier | Invariant 6 |
| P3 | **No global mutable state.** Per-message state lives on the returned wrapper. Factory-level state is read-only once the factory returns. No module-level cache is mutated per message | CLAUDE.md conventions; split children and destination forks must not see each other's state |
| P4 | **No side effects outside `ctx`.** No network, file writes, subprocesses, threads, environment changes or signal handlers, at import, in the factory or per message. A capability computes in process. **A capability that needs I/O (for example a terminology server) is out of set 1.x** and needs an ADR: it would be a side effect outside the loaders' idempotency model | Invariants 2, 4 and 7 |
| P5 | **Deterministic per message.** Output depends only on the message and factory state, so a redelivery or a reprocess produces the same result (for example `fullUrl`s derived from `tb-msg-id`) | Invariant 4 |
| P6 | **Unit-testable without Kafka.** The owning package ships a fake or a fixture that builds the extended context over `FakeContext` (for example `tributary.fhir.fake_fhir_context()`), and its tests show P1 against `FakeContext`, including `reply` | Invariant 7 |
| P7 | **Import has no side effects** beyond definitions. The factory does not require the control plane, PostgreSQL, ClickHouse or the API | Invariant 3 |

## 4. The registry

[`capabilities.json`](capabilities.json) lists every capability name and the one distribution that may
supply it. The `spec.pipeline.context` item `enum` in `channel-spec/v0` holds exactly the same names,
and `contracts/validate.py` refuses to run if the two differ.

**Adding a capability** is an additive contract change, in the same way as a connector type entering
its enum (ADR-007 D10). The owner proposes the name, distribution, provided members and protocol. The
architect approves, and `security-reviewer` reviews the package against section 3 before the name is
listed. The registry exists to create that review gate, not to keep a list of names.

| Capability | Distribution | Provides | Protocol | Owner |
|---|---|---|---|---|
| `fhir` | `tributary-fhir` | `ctx.fhir`, `ctx.terminology` | `tributary.fhir.FhirContext`; `FhirReplyContext` over a `ReplyContext` | `healthcare-standards-engineer` |

## 5. What this does and does not stop

| Threat | Outcome |
|---|---|
| A transitive dependency registers some other name in `tributary.context` | Never imported (L1). `context_extension_skipped`, `unregistered`, warning (L5) |
| A typosquat registers `fhir` alongside `tributary-fhir` | Duplicate name, so a spec declaring `fhir` is refused (L2) |
| A typosquat registers `fhir` and `tributary-fhir` is absent | Distribution mismatch, so refused (L2) |
| A spec author adds a capability silently | Not possible: it is in the spec diff, and the name must be registered |
| A package that **replaces** `tributary-fhir` under the same distribution name | **Not detected at runtime.** The controls are the locked, hash-pinned dependency set, the digest-pinned image (`pipeline.bundle`), `licence_check.py` and the SBOM |
| A registered extension that breaks P2-P6 | **Not detected at runtime.** The probe checks delegation, not behaviour. Review and the registry gate are the controls |

**Give up.** A declared capability is visible to **every** step on the channel, including destination
steps. Per-step scoping would break "one `ctx.fhir` per message branch" (W2-2), because the steps on a
branch share one view. Third-party capabilities need a contract change until the bundle trust model
(ADR-005) defines how a bundle may bring its own.
