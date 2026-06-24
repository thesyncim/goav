# Progress

Read this when you want the current shape without archaeology.
`docs/NORTH_STAR.md` says where the project is trying to go.
`docs/ROADMAP.md` separates stable, experimental, deferred, planned, and
non-goal work. `docs/V1_CREDIBILITY_AUDIT.md` points reviewers to the evidence
behind the current v1-credibility pass. This page answers the practical
question: what can a reader build on today, which seams are intentional, and
which pieces are still being finished?

## Working Surface

The normal path should feel small even when the runtime is doing serious work:

- Composition starts from `From(inputs...)`, narrows by stream selectors, and
  then applies ordered operations: `Decode`, `Copy`, `Shape`, `Auto`,
  `Require`, `Prefer`, `Resize`, `Resample`, `Do`, `Encode`, and `Tap`.
- Branching is the one split model. Direct streams, planned branches, runtime
  branches, and flows share one ordered operation list. A direct stream is
  syntax for the same branch model.
- `Destination` is the routing handle. Reusing one destination value groups
  branches into one mux or sink group.
- Initial builds and live edits lower through the same vocabulary: full jobs
  become `WorkPlan`; runtime attachment becomes `WorkPatch`.
- Observation stays ordinary composition: `Branch + Do + Sink`, `Events`,
  `Watch`, `Snapshot`, `Stats`, `Explain`, and graph rendering.

The public grammar remains:

```text
From(input) -> Chain -> operations -> Tap -> Branch -> Destination -> Task
```

A flow is reusable operations. It has no destination, source, runtime state, or
lifecycle policy. That restraint is intentional: use a flow to name work, not
to hide topology.

Compatibility pins:

- normal workflows lower from `input -> stream -> operations -> tap -> branch -> destination` into `WorkPlan -> pipeline.Graph -> Task`.
- runtime attach lowers the same branch model into `WorkPatch`.
- direct streams are syntax sugar for an implicit `Branch("main")`.
- `Destination` is the routing handle: reusing the same `Destination` value
  groups branches into one sink or mux destination.
- `provider.Destination` is the extension point for custom byte, object, and
  sink behavior.
- Direct `.To(...)` streams are only ergonomic syntax for the same branch
  model.
- normal composition does not import `goav/transcode`.
- `branchComposePlan`, `runtimeBranch`, `destinationNames`, and string output
  refs were migration markers, not extension points.
- `shape.Spec` carries the media contract, including custom source facts.

## Working Today

- Recipe entry points cover variadic `From(inputs...)`, `InputName`,
  `StreamID`, `StreamName`, and `StreamIndex` narrowing.
- Audio/video workflows can select streams, preserve packets, decode/encode,
  resize/resample, run custom stages, tap typed media, fan out branches, share
  destinations, and reuse flows.
- Combining media is part of the front door: Mix, Composite, Select, nested
  joins, tap-backed join arms, and custom joins all describe their shape.
- Live control is now a first-class path: atomic grouped `Mutable.Attach`,
  dependent-branch detach, pause/resume/stop, and gapless
  `Attachment.Rebranch`, including media-time switch boundaries.
- BranchBuffer policies cover `flow.Blocking`, `DropOldest`, `DropNewest`,
  `Latest`, `Unbounded`, MaxLatency, MaxBytes, and branch-local drop counters.
- Shared `SyncPolicy` gates on stream chains and branches for live-room
  packet/frame timeline alignment; late sync drops report through branch stats.
- Task controls include `Keyframe`, `SetBitrate`, `Seek`, `Rate`, `Segment`,
  `SelectActive`, `Deliver`, `.AtTap(name)`, and expert-only `.At(node)`.
- Dynamic streams use `InputSpec.Stream` runtime anchors for app-owned tracks,
  plus `OnStream` rules, `OnRemove` detach disposition, and
  `av.EventStreamAdded` for automatic discovery.
- Destination commit/abort/error lifecycle events are watchable for task and
  runtime-branch destinations.
- Deterministic testing comes from `goavtest` sources, collectors, fake codecs,
  fake containers, and fake clocks.
- Generated-source CLI pipelines and `goav ctl` sockets support live
  inspection, attach, rebranch, detach, controls, and graph rendering.

## V1 Credibility Evidence

- The README is an adoption front door with one compile-pinned Go snippet and
  links to deeper docs.
- Error docs include a checked catalog row for every current errcode, with
  named coverage instead of catalog-only placeholders.
- External-style examples for custom filters, transactional writers, custom
  codecs, and custom joins live in standalone modules with expected output and
  failure cases.
- The performance lab writes baseline, latency, RSS, pressure, control, fanout,
  container, and pprof artifacts under `bench-results/`; CI uploads smoke
  artifacts, and the release workflow emits checksums, SBOM, buildinfo, and
  provenance metadata.
- Composability laws, API-surface governance, release docs, and the PR evidence
  notes are pinned by doc tests.

## Extension Points

- Produce a custom source with `goav.Source(...)`, `shape.Spec`, and
  `goav.SourcePush`.
- Bring in transports through `provider.Source`; RTP and WebRTC live in nested
  modules.
- Write custom byte/object destinations through `goav.Writer(...)`,
  `goav.Custom(...)`, `provider.Info`, and `provider.TransactionalWriter`.
- Add in-process hooks through `EventFunc`, `FrameFunc`, `PacketFunc`, and
  `SinkFunc`.
- Register runtime adapters per runtime with `WithDecoder`, `WithEncoder`,
  `WithFilter`, `WithMuxer`, `WithDemuxer`, and `WithProber`.
- Host controls through package `ctl`: explicit command manifests, custom
  branch-pipeline steps, custom encoder spellings, capabilities reports, and
  generated help.

## Non-Negotiables

- Core stays pure Go: no cgo, no FFmpeg/GStreamer runtime dependency.
- Hot paths use caller-owned buffers, reused result structs, allocation guards,
  and direct per-message dispatch.
- Pion dependencies stay in `rtpav` and `webrtcav`, not the root module; the
  root dependency allowlist remains limited to sibling modules plus the
  reviewed modernc runtime set used by the built-in AAC backend.
- shape validation is central across inputs, operations, flows, taps, branches,
  joins, destinations, runtime attach, and controls.
- Normal workflows use the recipe grammar. Expert graph handles remain off the
  front door.

## Remaining Work

- Finish folding residual `streamIntent` validation/planning readers into the
  operation/work-plan model. Explain stream rows and adapter requirements, plus
  mux compatibility, already read codec facts from `WorkPlan` operations.
- Expand `SwitchAt` boundaries beyond frame/keyframe/media-time only if future
  live-control workflows prove they need additional switch points.
- Finish the full time-shape story: pipeline-wide clock service, A/V sink sync,
  and pull scheduling beyond the branch-local `SyncPolicy` gate.
- Make the v1 release decision, including the minimum supported Go version.
