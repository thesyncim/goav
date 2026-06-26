# Changelog

This is the human change record for goav. Keep it useful for adopters: name
what changed, why it matters, and any migration or verification notes a user
would need.

The project is pre-v1. Until the first tagged release, use commit history and
pull-request descriptions as the detailed change record. Release entries should
name user-visible API changes, behavior changes, new adapters, performance
methodology changes, and migration notes.

## Unreleased

- Stream chains now require an explicit `.Decode()` before frame-domain
  consumers (`.Do`, `.Resize`, `.Resample`, real `.Encode`, frame taps, and
  decoded-frame sinks). Use `.Copy()` when a chain should stay packet-domain.
- Removed legacy `BuildError.Details` and `BuildError.Suggestions` fields;
  callers should use typed `Fields`/`Fixes`, `Detail(key)`, `DetailLines()`,
  and `FixLines()`.
- Made `TransformSpec` opaque so callers use constructors instead of setting
  resize/resample fields directly.
- Removed same-handle destination grouping: repeated destination names now
  require explicit `Mux(name, destination)` groups, even when the same
  destination value is reused.
- Removed graph node-name prefix fallback from `Inspectable.Taps()`; taps now
  come from typed planner/runtime anchors instead of `decode-*` or `select-*`
  naming conventions.
- Typed resize/resample data in the immutable recipe IR instead of carrying
  root transform wrappers through the planner boundary.
- Clarified front-door docs so `.Copy()` is the packet-preserving recipe
  spelling; `codec.Copy()` remains an internal/lowering `CodecSpec` value.
- Renamed writer-backed destinations to `Write(name, writer)` so output recipes
  do not imply that the constructor opens filesystem paths; `URI(uri)` keeps
  the adapter-opened output path and `FileInput` remains the reader-side file
  input spelling.
- Updated front-door docs to use task capability vocabulary for runtime attach
  examples instead of teaching `LiveTask` as the normal user type, and to
  describe `Watch` as the primary observation surface.
- Made recipe-built task `Explain` preserve the structured pre-build workflow
  report while refreshing live graph and tap rows at call time.
- Captured destination sink/byte-stream behavior in the immutable recipe IR so
  destination shape validation reads planner-visible data instead of concrete
  writer or sink attachments.
- Captured input kind and declared custom/provider source shape in the
  immutable recipe IR so compile-time stream selection can use boundary data
  instead of concrete input attachments.
- Captured dynamic stream-rule summaries in the immutable recipe IR so
  validation and Explain can read rule matcher/branch/destination facts without
  concrete runtime rule attachments.
- Captured join summary facts in the immutable recipe IR and made join
  stream-set planning use IR input facts instead of concrete input attachments.
- Routed join planning through an explicit IR-derived handoff so the planner no
  longer reaches through compile state for stream-set facts.
- Routed join work-plan rendering through an explicit handoff so the join
  renderer consumes captured input, destination, and output-format facts.
- Routed normal work-plan rendering through an explicit handoff so operation,
  branch, destination, decision, and diagnostic rendering consumes captured
  planner data.
- Moved normal media-planner input binding and selected-stream resolution onto
  recipe IR input facts instead of concrete input attachments.
- Moved copy-branch source-shape planning onto recipe IR input facts.
- Added the pre-v1 simplification target, started the immutable recipe IR
  boundary used by compile-time planning, and tightened task runtime semantics
  so `Run` preserves both execution and finalization failures while `Events`
  returns an independent unfiltered watch subscription.
- Clarified the bundled-adapter contract: `goav/bundle` is a package in the
  root module, not a nested module. Importing `github.com/thesyncim/goav` does
  not import bundled adapter packages into the root package dependency graph;
  the root module still lists bundled backend requirements until/unless
  `goav/bundle` becomes a nested module.
- Added `bundle.Describe`, `goav.ContextCloser`, and `inspect` convenience
  helpers (`Subscribe`, `Snapshot`, `Stats`, `Render`) so bundled structural
  planning, context-aware shutdown, and observation helpers have first-class
  names.
- Tightened pre-v1 runtime contracts around explicit runtimes, Mux-first
  destination grouping, join validation parity, and Family-first structured
  error matching. Detailed `errcode.Code` values remain diagnostic leaves
  within stable `errcode.Family` categories.
- Reduced and layered the pre-v1 public API: bundled adapters now live behind
  `goav/bundle`, live controls and watch filters moved into explicit vocabulary
  packages, `Task` is the minimal run/close lifecycle, richer runtime behavior
  is exposed through opt-in capability interfaces, and `Mux(name, destination)`
  declares shared mux/sink grouping without depending only on reused Go values.
- Added typed `BuildError` fields and fixes (`Detail`, `Fix`, `RecipePatch`,
  and `Detail(key)`) while preserving existing rendered details and
  suggestions for human-facing diagnostics.
- Hardened `Emit` message ownership so packet, frame, and event deliveries no
  longer share one mutable message slot when an emitter buffers or retains
  pointers.
- Lowered every module directive to the minor Go floor (`go 1.26`) and moved CI
  to the latest Go 1.26 patch instead of making a local patch release the
  library minimum.
- Aligned branch transform shape errors across build and runtime attach paths,
  retiring the runtime-specific transform-media code in favor of the shared
  `operation_shape_mismatch` family.
- Reworked the human-facing docs around reader-first navigation while keeping
  the checked API, operation, release, and evidence pins current.
- Added grammar-shaped live-room sync with `SyncPolicy`, `.Sync(...)`,
  `SyncTolerance`, and `SyncDropLate` for branch-local audio/video alignment;
  hold-late behavior is the default and does not add a separate public option.
- Added `AtMediaTime(...)` rebranch boundaries, per-rule `OnStream(...,
  OnRemove(...))` removal disposition, and destination commit/abort/error
  lifecycle events.
- Updated the dynamic-audio-room and Gio WebRTC showcase examples around the
  live-room runtime path with sync policy, live rebranching, and dynamic branch
  behavior.
- Added repository trust documents and CI artifact guidance for the v1
  credibility pass.
- Added checked error, operations, extension-cookbook, composability-law, and
  release-process documentation.
- Added standalone external example modules for custom sources, provider
  sources, destinations, filters, codecs, joins, transactional writers, and
  control-plane hosts.
- Added perf-lab benchmark smoke for latency quantiles, heap/RSS capture,
  SourcePush pressure, attach/detach under load, fanout sweeps,
  Matroska/WebM corpus paths, and real Opus adapter throughput.
- Added release automation with CLI checksums, module SBOM, build metadata, and
  provenance artifacts.
- Added compatibility policy and release-note template for the v0/v1 release
  decision.
- Recorded GitHub repository metadata, topic, homepage, and no-release-yet
  posture in checked repository trust documentation.
- Added PR evidence template and README trust badges for Go version and release
  notes.
