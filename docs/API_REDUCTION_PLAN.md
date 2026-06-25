# API Reduction Plan

`goav` is pre-release, so the public API should be shaped for the library it
wants to be instead of preserving every exported convenience already present.
Before this reduction, the root package mixed recipe grammar, runtime
construction, adapter bundles, inspection, live mutation, control,
rendering-adjacent concepts, and expert seams. This plan shrinks the front door
and moves advanced capability behind explicit package or interface boundaries.

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

## Initial Baseline

The baseline public-surface pin approved:

- 134 root `goav` package identifiers
- 147 `errcode` identifiers
- 9 `graphrender` identifiers
- 13 `lifecycle` identifiers
- 28 `plan` identifiers
- 4 `snapshot` identifiers

The root package also imported the bundled adapter package through
`defaults.go`. As a result, importing `github.com/thesyncim/goav` pulled
codec/container adapter packages and backend codec modules even when an
application only wanted to build recipe data.

Current counts and the root dependency proof live in `docs/API_INVENTORY.md`.

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

- bundled runtime and adapters: `goav/bundle`
- control vocabulary: `goav/control`
- task inspection, stats, snapshots, watches, and render helpers: `goav/inspect`
- adapter registration contracts: `goav/adapter` or existing seam packages
- expert graph escape hatches: `goav/x/expert` or the existing expert package

## Work Slices

1. **Inventory and baseline** — landed
   - Record current symbol counts and root dependency behavior.
   - Keep `testdata/api_surface.txt` as the live approval list while symbols move.

2. **Lazy runtime** — landed
   - Stop constructing the bundled runtime in `newJob`.
   - Require an explicit runtime for adapter-backed build paths, or use an
     explicit bundle helper.
   - Return `runtime_missing` for nil runtimes and for omitted runtimes on
     adapter-backed build paths, including dynamic `OnStream` branch
     destinations and operations.

3. **Bundle package** — landed
   - Add `goav/bundle` for `New`, `MustNew`, `Build`, `Run`, and bundled adapter
     options.
   - Move direct imports of bundled codecs, formats, and filters out of the root
     package.
   - Prove `go list -deps github.com/thesyncim/goav` does not include standard
     codec backends unless `bundle` is imported.

4. **Explicit copy** — landed
   - Make whole-job `Copy` record an explicit operation or remove it.
   - Ensure `Describe`/`Explain` can distinguish intentional packet copy from a
     recipe that forgot to declare work.

5. **Task split** — landed
   - Narrow `Task` to `Run(context.Context) error` and `Close() error`.
   - Introduce opt-in interfaces for description/explanation, inspection,
     mutation, control, watching, and stats.
   - Keep the standard build path returning `LiveTask`, and update consumers to
     accept narrower interfaces whenever they only need one capability.

6. **Constructor strictness** — landed for constructors
   - Stop returning nil from helper constructors on nil callbacks.
   - Make runtime options reject nil registry callbacks, factories, and invalid
     settings at construction time.
   - Add strict `component.NewPacketStage`, `NewFrameStage`, `NewEventStage`,
     and `NewSink` constructors, plus `MustStage`/`MustSink`, for callers that
     want callback mistakes reported immediately.
   - Make payload-bearing control constructors validate immediately and keep
     `control.Control` opaque so callers cannot build loose tagged unions.

7. **Emit ownership** — landed
   - Stop reusing one mutable `pipeline.Message` inside `Emit`.
   - Give every Packet/Frame/Event emit independent message ownership, so
     buffered or retaining emitters cannot observe later emits mutating earlier
     deliveries.
   - Document and pin the explicit one-allocation `source.Push` safety cost.

8. **Error families** — started; typed fields landed
   - Introduce stable error families such as `InvalidRecipe`, `MissingAdapter`,
     `IncompatibleAdapter`, `UnsupportedShape`, and `RuntimeRejected`.
   - Move implementation-specific errcodes behind compatibility aliases or
     internal details.
   - Replace string-only details with typed details and fixes: `BuildError`
     now carries typed `Fields []Detail`, `Fixes []Fix`, optional
     `RecipePatch` hints, and `Detail(key)` access while preserving legacy
     `Details`/`Suggestions` rendering during the transition.

9. **Explicit destination groups** — landed
   - Introduce explicit mux/group builders.
   - Stop relying on Go value identity as the only way to group destination
     branches.
   - Keep `Mux(name, destination)` as the public grouping constructor; the
     option-only grouping helper is internal.

10. **Unified lowering** — landed
   - Lower stream chains, branches, joins, and runtime attach through the same
     operation model.
   - Keep build and attach errors aligned for the same invalid operation chain.
   - Phase order and parity are pinned by
     `TestRecipeCompilePhaseSequencesArePinned` and
     `TestBuildAndAttachReturnSameErrorForSameInvalidBranch`.

11. **Rebranch policy cleanup** — landed
    - Remove the public no-op rebranch failure-policy helper. Failed rebranch
      already keeps the old branch attached as the only supported policy.

12. **Codec-change policy cleanup** — landed
    - Remove the public helper for the only supported codec-change policy.
      Recipes use the default live receive behavior when
      `.OnCodecChange(...)` is omitted, and custom policies still fail with a
      structured build error.

13. **Sync policy cleanup** — landed
    - Move shared media-timeline policy constructors to `goav/flow`, keeping
      root `.Sync(...)` as recipe grammar while removing flow-control
      vocabulary from the root package.

14. **Detach lifecycle cleanup** — landed
    - Move standalone detach destination-outcome options to `goav/lifecycle`,
      keeping root `Detach` and `OnRemove` as runtime grammar while removing
      lifecycle disposition constructors from the root package.

15. **Rebranch lifecycle cleanup** — landed
    - Move rebranch switch boundaries and old-branch disposition options to
      `goav/lifecycle`, keeping `Attachment.Rebranch` typed through
      `lifecycle.RebranchArg` while removing rebranch policy constructors from
      the root package.

16. **Docs rewrite** — landed
    - Keep the README focused on the small grammar and one or two advanced entry
      points.
    - Finish with a Markdown-wide consistency pass and a README that works as a
      credible front door, not just as a passing line-count artifact.
    - Generate bundled adapter capability docs from `bundle.Options()` and
      registered descriptors so the docs and registered set cannot drift.

17. **Go version floor** — landed
    - Lower every module directive from the patch-level local toolchain floor
      to the minor-version support floor (`go 1.26`).
    - Keep CI on the latest Go 1.26 patch with `1.26.x`, plus the rolling
      `stable` job, instead of making a patch release the library minimum.
    - Do not claim a lower floor yet: first-party backend modules currently
      require Go 1.26.

18. **Close/remove diagnostics** — landed
    - Direct and buffered graph remove/close waits report `pipeline.CloseWaitError`
      wrapping `pipeline.ErrCloseWait` instead of hanging silently.
    - Diagnostics include operation, node, timeout, and pending delivery count
      where the runner can know it.

## Immediate Tests

Add focused coverage as the slices land:

- `TestRootImportDoesNotPullBundledAdapters`
- `TestBuildWithoutRuntimeReturnsStructuredError`
- `TestUseRuntimeNilReturnsStructuredError`
- `TestJobCopyAppearsInExplain`
- `TestNilPacketFuncDoesNotBecomeSilentNilStage`
- `TestNilSinkFuncDoesNotBecomeSilentNilSink`
- `TestStrictStageConstructorsRejectNilCallbacks`
- `TestStrictSinkConstructorRejectsNilCallback`
- `TestControlHasNoExportedFields`
- `TestValidatedControlConstructorsRejectInvalidPayloads`
- `TestEmitMessagesAreIndependentWhenEmitterRetainsPointers`
- `TestBundledAdapterCapabilityDocsMatchDescriptors`
- `TestMuxSurvivesWithAndCopy`
- `TestBuildAndAttachReturnSameErrorForSameInvalidBranch`
- `TestRecipeCompilePhaseSequencesArePinned`
- `TestErrorDetailsAreTyped`
- `TestCompatibilityPolicyPinsReleaseDecisionEvidence`
- `TestCIWorkflowCoversTrustGates`
- `TestGraphDirectRemoveReportsStuckNode`
- `TestGraphBufferedRemoveReportsStuckNode`
