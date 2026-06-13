# Progress

This file is the compact current-state tracker. `docs/NORTH_STAR.md` defines
the design contract; `docs/ROADMAP.md` separates stable, experimental,
deferred, planned, and non-goal work; `docs/V1_CREDIBILITY_AUDIT.md` maps the
current v1-credibility evidence to files, tests, and workflows.

## Working Surface

- Composition starts from `From(inputs...)`, narrows by stream selectors, and
  applies ordered operations: `Decode`, `Copy`, `Shape`, `Auto`, `Require`,
  `Prefer`, `Resize`, `Resample`, `Do`, `Encode`, and `Tap`.
- Direct streams, planned branches, runtime branches, and flows share one
  ordered operation list. A direct stream is syntax for the same branch model.
- `Destination` is the routing handle. Reusing one destination value groups
  branches into one mux or sink group.
- Runtime attach lowers the same branch model into `WorkPatch`; initial build
  lowers the full job into `WorkPlan`.
- Observation stays ordinary composition: `Branch + Do + Sink`, `Events`,
  `Watch`, `Snapshot`, `Stats`, `Explain`, and graph rendering.

The public grammar remains:

```text
From(input) -> Chain -> operations -> Tap -> Branch -> Destination -> Task
```

A flow is reusable operations. It has no destination, source, runtime state, or
lifecycle policy.

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

- Variadic `From(inputs...)` with `InputName`, `StreamID`, `StreamName`, and
  `StreamIndex` narrowing.
- Audio/video stream selection, packet-preserving copy, decode/encode,
  resize/resample, custom stages, typed taps, branch fanout, shared
  destinations, and reusable flows.
- Mix, Composite, Select, nested joins, tap-backed join arms, and custom joins.
- Atomic grouped `Task.Attach`, dependent-branch detach, pause/resume/stop,
  and gapless `Attachment.Rebranch`.
- BranchBuffer policies: `flow.Blocking`, `DropOldest`, `DropNewest`,
  `Latest`, `Unbounded`, MaxLatency, MaxBytes, and branch-local drop counters.
- Task controls: `Keyframe`, `SetBitrate`, `Seek`, `Rate`, `Segment`,
  `SelectActive`, `Deliver`, `.AtTap(name)`, and expert-only `.At(node)`.
- Dynamic streams through `InputSpec.Stream` runtime anchors for app-owned
  tracks, plus `OnStream` rules and `av.EventStreamAdded` for automatic
  discovery.
- Deterministic testing through `goavtest` sources, collectors, fake codecs,
  fake containers, and fake clocks.
- Generated-source CLI pipelines and `goav ctl` sockets for live inspection,
  attach, rebranch, detach, controls, and graph rendering.

## V1 Credibility Evidence

- README is now an adoption front door with one compile-pinned Go snippet and
  links to deeper docs.
- Error docs include a checked catalog row for every current errcode, with
  named coverage instead of catalog-only placeholders.
- External-style examples for custom filters, transactional writers, custom
  codecs, and custom joins live in standalone modules with expected output and
  failure cases.
- The performance lab writes baseline, latency, RSS, pressure, control, fanout,
  container, and pprof artifacts under `bench-results/`; CI uploads smoke
  artifacts and the release workflow emits checksums, SBOM, buildinfo, and
  provenance metadata.
- Composability laws, API-surface governance, release docs, and the PR evidence
  draft are pinned by doc tests.

## Extension Points

- Custom source production through `goav.Source(...)`, `shape.Spec`, and
  `goav.SourcePush`.
- Transport providers through `provider.Source`; RTP and WebRTC live in nested
  modules.
- Custom byte/object destinations through `goav.Writer(...)`,
  `goav.Custom(...)`, `provider.Info`, and `provider.TransactionalWriter`.
- In-process hooks through `EventFunc`, `FrameFunc`, `PacketFunc`, and
  `SinkFunc`.
- Runtime adapters through per-runtime registration:
  `WithDecoder`, `WithEncoder`, `WithFilter`, `WithMuxer`, `WithDemuxer`,
  and `WithProber`.
- Control hosts through package `ctl`: explicit command manifests, custom
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

- Fold the remaining `streamIntent` normalization layer into operation readers.
- Add dedicated commit lifecycle events.
- Add per-rule removal detach policy for `OnStream`.
- Expand `SwitchAt` boundaries beyond frame/keyframe.
- Finish time-shape work: pipeline-wide clock service, A/V sink sync, and pull
  scheduling.
- Make the v1 release decision, including the minimum supported Go version.
