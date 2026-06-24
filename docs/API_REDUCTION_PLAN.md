# API Reduction Plan

`goav` is pre-release, so the public API should be shaped for the library it
wants to be instead of preserving every exported convenience already present.
The current root package mixes recipe grammar, runtime construction, adapter
bundles, inspection, live mutation, control, rendering-adjacent concepts, and
expert seams. This plan shrinks the front door and moves advanced capability
behind explicit package or interface boundaries.

## Goal

Make the root package a small recipe grammar:

- build inert recipes from inputs, stream selectors, operations, branches, joins,
  and destinations
- keep `Task` as the minimal runnable lifecycle and expose richer runtime work
  as opt-in task capability interfaces
- expose structured build refusals
- keep runtime/adapters, inspection, live mutation, control, and expert graph
  doors opt-in

Target root surface after the reduction: roughly 40 to 70 package-level
identifiers.

## Current Baseline

The checked-in public-surface pin currently approves:

- 134 root `goav` package identifiers
- 147 `errcode` identifiers
- 9 `graphrender` identifiers
- 13 `lifecycle` identifiers
- 28 `plan` identifiers
- 4 `snapshot` identifiers

The root package also imports the standard adapter bundle through `defaults.go`.
As a result, importing `github.com/thesyncim/goav` pulls codec/container adapter
packages and backend codec modules even when an application only wants to build
recipe data.

## Target Package Shape

Keep in `goav`:

- recipe constructors and builders: `From`, `Job`, stream chains, `Branch`,
  `Flow`, joins, and destination basics
- minimal media aliases needed by simple custom callbacks
- `BuildError` and stable error families
- minimal `Task` with `Run` and `Close`
- opt-in task capability interfaces: `Explainer`, `Inspectable`, `Mutable`,
  `Controllable`, `Observable`, and `LiveTask`

Move or keep outside the front door:

- bundled runtime and adapters: `goav/std`
- control vocabulary: `goav/control`
- task inspection, stats, snapshots, watches, and render helpers: `goav/inspect`
- adapter registration contracts: `goav/adapter` or existing seam packages
- expert graph escape hatches: `goav/x/expert` or the existing expert package

## Work Slices

1. **Inventory and baseline**
   - Record current symbol counts and root dependency behavior.
   - Keep `testdata/api_surface.txt` as the live approval list while symbols move.

2. **Lazy runtime**
   - Stop constructing the standard runtime in `newJob`.
   - Require an explicit runtime at build time or an explicit standard helper.
   - Return `runtime_missing` for nil or omitted runtime on build paths.

3. **Standard package**
   - Add `goav/std` for `New`, `MustNew`, `Build`, `Run`, and standard adapter
     options.
   - Move direct imports of bundled codecs, formats, and filters out of the root
     package.
   - Prove `go list -deps github.com/thesyncim/goav` does not include standard
     codec backends unless `std` is imported.

4. **Explicit copy**
   - Make whole-job `Copy` record an explicit operation or remove it.
   - Ensure `Describe`/`Explain` can distinguish intentional packet copy from a
     recipe that forgot to declare work.

5. **Task split**
   - Narrow `Task` to `Run(context.Context) error` and `Close() error`.
   - Introduce opt-in interfaces for description/explanation, inspection,
     mutation, control, watching, and stats.
   - Keep the standard build path returning `LiveTask`, and update consumers to
     accept narrower interfaces whenever they only need one capability.

6. **Constructor strictness**
   - Stop returning nil from helper constructors on nil callbacks.
   - Make runtime options reject nil registry callbacks, factories, and invalid
     settings at construction time.

7. **Error families**
   - Introduce stable error families such as `InvalidRecipe`, `MissingAdapter`,
     `IncompatibleAdapter`, `UnsupportedShape`, and `RuntimeRejected`.
   - Move implementation-specific errcodes behind compatibility aliases or
     internal details.
   - Replace string-only details with typed details and fixes.

8. **Explicit destination groups** — landed
   - Introduce explicit mux/group builders.
   - Stop relying on Go value identity as the only way to group destination
     branches.

9. **Unified lowering**
   - Lower stream chains, branches, joins, and runtime attach through the same
     operation model.
   - Keep build and attach errors aligned for the same invalid operation chain.
   - First parity guard landed with
     `TestBuildAndAttachReturnSameErrorForSameInvalidBranch`.

10. **Docs rewrite**
    - Keep the README focused on the small grammar and one or two advanced entry
      points.
    - Finish with a Markdown-wide consistency pass and a README that works as a
      credible front door, not just as a passing line-count artifact.
    - Generate standard adapter capability docs from descriptors so the docs and
      registered set cannot drift.

## Immediate Tests

Add focused coverage as the slices land:

- `TestRootImportDoesNotPullStdAdapters`
- `TestBuildWithoutRuntimeReturnsStructuredError`
- `TestUseRuntimeNilReturnsStructuredError`
- `TestJobCopyAppearsInExplain`
- `TestNilPacketFuncDoesNotBecomeSilentNilStage`
- `TestNilSinkFuncDoesNotBecomeSilentNilSink`
- `TestStdFormatsDocsMatchRegisteredFormats`
- `TestDestinationGroupSurvivesWithAndCopy`
- `TestBuildAndAttachReturnSameErrorForSameInvalidBranch`
- `TestErrorDetailsAreTyped`
