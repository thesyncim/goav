# Simplification Target

This is the pre-v1 target freeze. The current surface is governed and tested,
but governed does not mean ready to freeze as v1. The v1 product should be the
small media recipe layer, with advanced runtime and graph features available
only after the normal path is honest and compact.

The product shape is:

```text
Recipe AST -> Plan -> Graph -> Task
```

The normal user model is:

```text
Input -> Stream -> Operations -> Output
```

Everything else is optional: taps, branches, runtime adapter registries,
inspection, control, mutation, and the expert graph escape hatch.

## Supported v1 workflows

These are the workflows the v1 front door should freeze:

1. Reader/file-like packet copy to writer.
2. Single stream decode to sink.
3. Decode -> transform -> encode -> mux.
4. Explicit fanout branches.
5. Custom source and custom sink.
6. Describe/Explain before Build.
7. Structured build errors.

## Experimental or non-v1

These features can exist, but they are not part of the v1 promise unless a
later simplification slice explicitly retains and freezes them:

1. Expert graph.
2. Runtime Attach/Rebranch.
3. Control-plane socket host.
4. OnStream dynamic rule breadth beyond the supported identity cases.
5. Mix/Composite/Select unless explicitly retained.
6. Live provider transcode until compiler support is real.

## Delete or demote before v1

These concepts must not stay in the beginner-facing surface:

- Same-handle destination grouping as a semantic rule.
- `File(name, writer)` as the primary writer-backed output spelling.
- Raw graph event channels as the primary observation model; prefer `Watch`.
- `LiveTask` as the normal task type users must accept.
- Exported pointer-union structs with "exactly one field must be set" rules.
- Prefix-based tap inference.
- Runtime Attach/Rebranch in Tier A unless fully frozen.
- Expert graph examples in beginner-facing docs.
- Duplicate copy spellings in user docs; prefer `Copy`.
- Glossary-dependent naming clusters that require first-time users to learn
  multiple names for one concept.

## API budget

The v1 budget is intentionally smaller than the current governed surface:

| Surface | Target |
| --- | ---: |
| Root package exported identifiers | <= 40 |
| `errcode` exported identifiers | <= 40 |
| README | <= 120 lines |
| Front-door docs | one page |

Current counts live in `docs/API_INVENTORY.md`. They are a checkpoint, not the
v1 target. A simplification slice may break pre-v1 compatibility, but it should
not add new exported symbols unless the addition removes more public surface
than it creates and updates the API inventory in the same change.

## Architecture target

The grammar should only build an immutable recipe representation. The planner
should consume that representation instead of reading or mutating builder
internals. The graph lowerer should consume the plan. Runtime mutation should
be a plan patch, not a parallel mini-language.

The intended boundary is:

```text
grammar builders -> immutable recipe data -> planner -> work plan -> graph
```

The first implementation slice after this target should introduce that data
boundary and pin it with tests that fail if planner files reach back into
grammar-builder internals.

Current progress: `internal/recipeir` carries the first immutable recipe
snapshot, `compileJobRecipe` enters through that snapshot, and branch
composition planning reads normalized intent instead of `streamBuild` builder
records. Transform operations now cross the boundary as typed recipe IR data
instead of a generic root wrapper, and destination sink/byte-stream behavior is
now captured as recipe IR so shape validation does not need to infer that fact
from concrete writer or sink attachments. Input kind and declared source shape
now cross the same boundary for compile-time stream selection and normal media
planner input binding, and dynamic stream-rule summaries feed validation and
Explain from recipe IR. Join
summaries now cross the boundary, and join planning plus join work-plan
rendering enter through explicit IR-derived handoffs instead of reading compile
state directly. The executable join lowerer still owns concrete arms and stages.
The boundary is not complete until remaining root-only attachments such as
concrete join plans and runtime mutation patches move into stable recipe or
plan data.

Runtime lifecycle/observation progress: one-shot `Run` now preserves both run
and close/finalization failures, and `Events()` returns an unfiltered watch
subscription rather than exposing the raw graph event channel. Recipe-built
tasks now keep the same structured workflow report as pre-build `Explain`,
with live graph and tap rows refreshed when task `Explain` is called.

Tap-anchor progress: `Inspectable.Taps()` now reports only typed anchors emitted
by the planner or runtime attach path; graph node names such as `decode-*` and
`select-*` no longer synthesize taps.

Destination-group progress: repeated destination names now require explicit
`Mux(name, destination)` groups; reusing one ungrouped destination value no
longer creates a mux/sink group.

Writer-backed output progress: `Write(name, writer)` is the public recipe
spelling for an already-open writer; `URI(uri)` remains the spelling for outputs
opened by a registered adapter, and `FileInput(name, reader)` stays input-only.

Invalid-struct progress: `TransformSpec` is now an opaque constructor-produced
value, so external callers cannot set both resize and resample fields on one
transform.

Error-contract progress: `BuildError` no longer exposes legacy
`Details`/`Suggestions` fields or parses rendered strings in `Detail(key)`;
typed `Fields` and `Fixes` are the public contract.

## Release gate

Do not cut v1 just because the current docs and pins are green. Cut v1 only
after the release candidate either meets this target or records an explicit
maintainer decision for every retained exception.
