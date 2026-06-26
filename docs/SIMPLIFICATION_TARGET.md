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
composition snapshots are built directly from captured branch facts while
branch composition planning reads captured recipe IR instead of `streamBuild`
builder records, without a builder `composePlan` wrapper; branch
destination-name validation errors now consume stream intent facts rather than
builder records too. Transform operations now cross the boundary as typed
recipe IR data instead of a generic root wrapper, and destination
sink/byte-stream behavior is now captured as recipe IR so shape validation does
not need to infer that fact from concrete writer or sink attachments. Input kind
and declared source shape now cross the same boundary for compile-time stream
selection and normal media planner input/copy binding, and dynamic stream-rule
summaries feed validation and Explain from recipe IR. Join summaries now cross
the boundary, and join planning plus join work-plan rendering enter through
explicit IR-derived handoffs instead of reading compile state directly; join
branch fanout planning now builds recipe IR stream facts instead of synthesized
builder records, and join chain-arm inputs are captured at the join-arm boundary
before join tree planning. Recursive join planning now walks captured join tree
arm facts instead of resolving arm builders or reading chain operation helpers.
Normal work-plan rendering
also now consumes a captured handoff rather than reaching back into compile
state while it renders operations, branches, destinations, decisions, and
diagnostics; that handoff now reads inputs, streams, destinations, and
solver-updated operation lists from the captured recipe IR instead of the
legacy intent mirror, with no fallback to rebuilding from that mirror.
Multi-stream job graph lowering now enters through a
captured handoff before branch-compose graph construction, and single-stream
packet-copy and decode graph lowerers now select streams from recipe IR before
graph construction. Attachment-consistency validation now counts recipe IR
inputs and destinations instead of the legacy intent mirror, and
encode/transform adapter validation now reads recipe IR stream facts at the pass
boundary. Output binding and destination-kind validation now use recipe IR
output refs and destination kinds instead of concrete destination arrays, and
intent-shape validation now starts from recipe IR inputs, streams, and outputs.
Stream-selection and decode-adapter preflight now derive stream chains from
recipe IR before probing live or known input facts. Operation-shape solving and
destination-shape validation now iterate recipe IR streams and destination
metadata before touching concrete output handles. Explicit-runtime detection
and unsupported-graph diagnostics now read recipe IR stream and recipe counts
instead of the legacy intent mirror.
Explicit branch-composition graph lowering also now uses a captured handoff
with cloned branch-compose plan data. The compile state no longer stores the
concrete join plan; graph-plan construction passes the selected join lowerer
directly into work-plan rendering. The executable join lowerer still owns
concrete arms and stages. Media input binding, copy planning, live stream
selection, and multi-input stream resolution now read captured recipe input
facts instead of the legacy intent mirror. Runtime attach/rebranch/detach and
stream-rule attach/remove reactions now capture branch specs, destination facts,
switch policy, attachment target,
and disposition in explicit handoffs before graph locking and patch planning.
Runtime attach inputs now carry each branch's captured runtime recipe and
validated destinations as one branch record, so patch planning no longer
coordinates parallel spec and destination arrays or reads the public `BranchSpec`
after the boundary capture. Runtime attach planning also captures the resolved
source anchor and graph snapshot with that branch record before step planning
and patch finalization. Runtime detach now carries its captured attachment
target and disposition through the locked stop path and child-attachment
recursion instead of unwrapping them into loose arguments, and stream-rule
removals now capture the same runtime detach input for each tracked attachment
before mutation. OnStream branch facts now carry media, operation, destination,
and buffer data in the immutable recipe IR, and stream-rule attach captures
runtime branch inputs before graph locking and patch planning instead of
recapturing public branch specs inside the mutation executor. Runtime
attach/rebranch inputs now also carry branch name, media, source anchor,
operation, and buffer facts as `internal/recipeir` runtime branch data before
patch planning. Runtime destination name, kind, format, and share-key facts now
cross the same boundary; concrete writer, mux, and sink handles still stay at
the mutation edge. Runtime attach patch planning now walks captured
`recipeir.Operation` values directly for shape, sync, component, codec, and tap
decisions and carries runtime branch stream facts as `recipeir.Stream` instead
of rehydrating root operation specs or stream intents after branch capture.
Branch-composition planning now consumes captured recipe IR too; concrete input
and destination handles remain at the attachment edge.
Retained runtime-patch exception for the release candidate: concrete writer,
mux, sink, graph injector, and live stage handles stay at the mutation edge.
Moving those remaining executable handles into deeper stable patch data is
deferred unless the runtime mutation API graduates beyond the governed pre-v1
surface.

Runtime lifecycle/observation progress: one-shot `Run` now preserves both run
and close/finalization failures, and `LiveTask` exposes one observation model:
`Watch(filters...).Events()`, with unfiltered `Watch()` for every task event.
Recipe-built tasks now keep the same structured workflow report as pre-build
`Explain`, with live graph and tap rows refreshed when task `Explain` is
called. Normal recipe and bundle `Build` now return the narrow `Task`
lifecycle; callers that need inspection, watches, controls, or runtime mutation
opt into the full surface with `BuildLive`.

V1 rescope progress: README now meets the <=120 line target and acts as the
adoption front door only; longer live/runtime and extension walkthroughs live
in focused docs. `docs/COMPATIBILITY.md` and `docs/ROADMAP.md` now describe
runtime mutation, control-plane hosts, advanced observation, joined-stream
breadth, and expert graph features as governed pre-v1 behavior rather than
automatic v1 promises unless the release decision explicitly retains them.

Tap-anchor progress: `Inspectable.Taps()` now reports only typed anchors emitted
by the planner or runtime attach path; graph node names such as `decode-*` and
`select-*` no longer synthesize taps.

Destination-group progress: repeated destination names now require explicit
`Mux(name, destination)` groups; reusing one ungrouped destination value no
longer creates a mux/sink group.

Writer-backed output progress: `Write(name, writer)` is the public recipe
spelling for an already-open writer; `URI(uri)` remains the spelling for outputs
opened by a registered adapter, and `FileInput(name, reader)` stays input-only.

Invalid-struct progress: the root no longer exports `goav.TransformSpec`;
callers still use the `Resize` and `Resample` constructors, but the transform
value type is no longer part of the public contract. `BranchSpec` values now
carry their unexported constructor origin, so zero values and non-branch policy
specs are refused before planned or runtime branch mutation treats them as real
branches. `Destination` handles now carry the same constructor-origin marker,
so zero values and zero-derived option copies are refused before they can
masquerade as writer, URI, sink, or custom outputs. `InputSpec` values now do
the same for file-like reader, custom source, and provider inputs, including
zero-derived options or wrappers. `Job` values now carry constructor origin too,
so public zero jobs fail as
unconstructed recipes with `goav.From(...)` guidance instead of blending into
normal empty-input validation.

API budget progress: exported `goav.ContextCloser` was removed from the root
surface; built runtime tasks still expose `CloseContext(ctx)` structurally, and
callers that only need context-aware shutdown can assert a local interface.
Exported `goav.Observable` was also removed; task event access is `Watch`,
while narrow event consumers can still use local structural interfaces.
Exported `goav.Controllable` was removed the same way; live control remains on
the explicit `BuildLive`/`LiveTask` path, and control-only helpers can accept a
local structural interface.
Exported `goav.Explainer` was removed as a separate alias; built-task
explanation remains on the explicit `BuildLive`/`LiveTask` path, and
`Job.Explain(ctx)` remains the pre-build path.
Exported `goav.Inspectable` was removed; runtime inspection remains on
the explicit `BuildLive`/`LiveTask` path, and inspection-only helpers can accept
a local structural interface.
Exported `goav.Mutable` was removed; runtime branch mutation remains on
the explicit `BuildLive`/`LiveTask` path, and mutation-only helpers can accept a
local structural interface.
Exported `goav.MediaOption` was removed while keeping `Name`, `MIME`, and
`Metadata` usable as shared input/destination options.
Exported `goav.InputOption` and `goav.DestinationOption` were also removed;
option constructors still pass directly to recipe constructors and `.With(...)`.
Exported `goav.Codec` was removed as a separate input-option spelling; custom
sources and source providers now declare input codec facts through their
`shape.Spec`/`SourceShape` instead of a root option overlay.
Exported `goav.InputStream` was removed while keeping
`InputSpec.Stream(stream)` as the branch attach anchor for app-owned dynamic
tracks.
Exported `goav.Tap` was removed; callers now choose `FrameTap` or `PacketTap`
explicitly so public tap handles always state their media domain.

Error-contract progress: `BuildError` no longer exposes legacy
`Details`/`Suggestions` fields or parses rendered strings in `Detail(key)`;
typed detail/fix records are now package-private, with public reads through
`Detail(key)`, `DetailLines()`, and `FixLines()`. The unused
`goav.RecipePatch` edit-hint DTO was also removed from the root surface, as
were the exported `goav.Detail` and `goav.Fix` DTO names.
Runtime-branch mutation leaf codes are no longer exported `errcode` constants;
they remain typed internally and still map to `FamilyRuntimeBranch`, but the
experimental mutation surface no longer expands the public refusal catalog.
Reusable Flow leaf codes are no longer exported `errcode` constants either;
they remain typed inside the root package and still map to `FamilyFlow`, while
public callers match the stable family and read the detailed `flow_*` string
only when they need that internal leaf.
Planned branch and branch-buffer leaf codes now follow the same rule: branch
grammar and buffer safety refusals still return exact typed `branch_*`,
`copy_*`, `packet_branch_*`, `decode_*`, and `buffer_*` strings, but the public
matching contract is `FamilyBranch` or `FamilyBuffer` instead of one exported
constant per planner edge case.
Destination and output leaf codes now follow the same family-first contract:
recipes still return exact typed `output_*` and `destination_*` details, while
public callers match `FamilyDestination` for stable handling.
Media-operation leaf codes now do the same for transform, shape, codec-adapter,
and encode refusals: detailed `transform_*`, `shape_*`, `*_adapter_*`, and
`encode_*` strings stay typed internally, while public code switches use the
stable transform/shape/codec/encode families.
Input/source and stream-selection leaf codes are also internal typed details
now; callers match `FamilyInput` or `FamilyStream` for stable handling, which
brings the public `errcode` surface below the v1 budget.

## Release gate

Do not cut v1 just because the current docs and pins are green. Cut v1 only
after the release candidate either meets this target or records an explicit
maintainer decision for every retained exception.
