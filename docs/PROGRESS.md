# Progress

Current-state summary. The driving spec is `docs/NORTH_STAR.md`; history lives
in git, not here.

## What works today

- Composition front door: variadic `From(inputs...)` with `InputName` narrowing;
  audio/video stream selection; ordered operations (`Decode`, `Copy`, `Shape`,
  `Resize`, `Resample`, `Do`, `Encode`, `Tap`); typed `Branch`/`Destination`/`Flow`.
- Convergence: `Mix`, `Composite` (`.Region(x, y)`), and `Select` join N source
  chains into one composable stream; `SelectActive` switches live via `task.Control`.
- Control plane: untargeted `Keyframe` rides the data path to encoders;
  `.AtTap(name)` targets by tap; `.At(node)` is expert-only.
- Runtime attach: atomic grouped `Task.Attach` from typed taps with rollback,
  dependent-branch detach, pause/resume/stop/rebranch, and branch-local buffers
  (`flow.Blocking/DropOldest/DropNewest/Latest/Unbounded`, MaxLatency/MaxBytes shedding).
- Flow control: lock-free data plane (per-node atomics, snapshot routing, no
  per-message mutex); `Blocking` backpressures without teardown; per-branch drop
  accounting; result-aware `SourcePush` (`PushResult{Accepted, Dropped}`).
- Inspection: `Explain(ctx)`, `Describe()`, `Events`, `Snapshot` with typed
  `TaskState`/`BranchState`/`DestinationState`.
- Extensibility: per-runtime registries, layered `Default(opts...)` (last-wins),
  direct `WithDecoder/WithEncoder/WithFilter/WithMuxer/WithDemuxer/WithProber`,
  custom source/`Do`/`Sink`/`Writer` components, generic `Codec` specs.
- Codec settings: one owner (`codec.CodecSettings` via `codec` options); tier-2
  `codec.Control` raw callback; Opus/VP8/VP9 full verticals, H264/AV1 decode-first.
- Adapters: IVF, Annex B, Matroska/WebM, gopus, govpx, goav1, goh264 (tagged),
  resize, resample — all with allocation guards.

## Non-negotiables

- Pure Go, no cgo, no FFmpeg/GStreamer runtime dependency.
- Hot paths: caller-owned buffers, reused result structs, zero steady-state
  allocation, lock-free per-message paths; allocation guards required.
- Pion types stay at RTP/WebRTC package boundaries.
- One way to express work: one planner, one operation model, one destination
  model, one branch model, one runtime extension model.

## Active goal

The public grammar is:

```text
From(input) -> Chain -> operations -> Tap -> Branch -> Destination -> Task
```

- direct streams are syntax sugar for an implicit `Branch("main")`;
- every fluent operation appends exactly one `OperationSpec`; direct streams,
  planned branches, runtime branches, and flows share one ordered list;
- `Destination` is the routing handle: reusing the same `Destination` value
  groups branches into one sink or mux destination;
- `provider.Destination` is the extension point for custom byte/object/sink
  behavior; goav owns destination identity so shared groups are reliable;
- Direct `.To(...)` streams are only ergonomic syntax for the same branch model;
- normal workflows lower from `input -> stream -> operations -> tap -> branch -> destination` into `WorkPlan -> pipeline.Graph -> Task`;
  runtime attach lowers the same branch model into `WorkPatch`;
- shape validation is central across inputs, operations, flows, taps, branches,
  and destinations; `BranchBuffer` policy and lifecycle are planner/executor work;
- observation stays branch composition: `Branch + Do + Sink`, `Events`, `Snapshot`;
- normal composition does not import `goav/transcode`, carry labels, or dispatch
  by workflow kind; `branchComposePlan`, `runtimeBranch`, `destinationNames`
  bridge state, and string output refs were removal targets, never extension
  points — the `runtimeBranch`/`mediaPlan` parallel IRs are deleted, routing is
  by destination handle, `destinationNames` survives only as display-name
  overrides, and `branchComposePlan` stays as the recipe→lowering hand-off.

A flow is reusable operations plus media kind; it has no destination, source,
runtime state, or lifecycle policy.

## Done Criteria

| Gate | Evidence | State |
| --- | --- | --- |
| Simple high-level API | `From`, stream selection, ordered operations, typed taps, branches, direct `File`/`URI`/`Sink` destinations, custom `Writer` destinations with `provider.Info`, stable destination handles for shared mux/sink groups, flows, runtime attach, custom sources, `Explain(ctx)` | active |
| One grammar, one engine | one planner emits `WorkPlan` for build and `WorkPatch` for attach; the internal IR collapse is the open work (NORTH_STAR attack plan) | in progress |
| Allocation-guarded hot paths | `testing.AllocsPerRun` guards across core/RTP/codec/format/adapters; no cgo (`hygiene_test.go`) | active |
| Validation gates | `go test ./...`, adapter tag builds, allocation and lifecycle tests | active |
