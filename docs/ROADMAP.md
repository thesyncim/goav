# Roadmap

`PROGRESS.md` is the compact current-state tracker; `NORTH_STAR.md` is the
canonical direction. This roadmap keeps the priority view.

GoAV should not become a GStreamer clone. The remaining work is the Go-native
work-planning layer: stable destinations, one ordered operation model, formal
media shapes, branch-local buffering, branch-based observation from typed taps,
task/branch/destination lifecycle, custom sources, and one planner for build and
runtime attach. Public vocabulary stays `Input`, `Stream`, `Tap`, `Branch`,
`Destination`, `Flow`, and `Task`; operations are chain methods.

## Priorities

1. Make the declarative grammar the only normal composer:
   `input -> stream -> operations -> tap -> branch -> destination` lowers into
   `WorkPlan -> pipeline.Graph -> Task`. Runtime attach lowers the same branch
   model into `WorkPatch`. The planner owns operations, taps, branches,
   destinations, edges, decisions, and diagnostics; the executor instantiates
   the plan instead of dispatching by workflow kind.
2. Collapse `Target` into `Destination`. `File`, `URIOut`, `Writer`, `Object`,
   `Sink`, and `Custom` return stable goav-owned destination handles. Reusing a
   handle groups branches into one sink or mux destination; a different handle
   with the same name is a planning error. Custom behavior is provided through
   destination providers. *(Public surface done; string-routing residue inside
   the planner remains.)*
3. Treat direct streams as implicit branches: copy-to-file, decode-to-sink,
   encode-to-destination, branch composition, and mixed audio/video destinations
   are branch plans over one ordered operation list, not workflow modes.
4. Make runtime attachment a patch of the same plan model. `Task.Attach`
   compiles `Branch(...)` plus existing `info.Tap` into `WorkPatch`, validates
   before graph mutation, allocates only downstream nodes, reuses upstream work
   from taps, and detaches only branch-owned nodes.
5. Upgrade shape validation to shape solving: `shape.Spec` contracts for
   streams, codecs, filters, containers, flows, taps, branches, sinks,
   destinations, and custom sources, with expected-vs-actual diagnostics and
   opt-in auto-insertion of safe conversions.
6. Keep observation as branch composition: diagnostic work is
   `Branch(...).From(tap).Do(...).To(Sink(...))`; `Snapshot` reports task state,
   branches, taps, destinations, lifecycle, and scoped stats.
7. Time/clock/seek (north-star theme C): TimeShape, sync policies, and pull
   scheduling — the remaining feature frontier after graph shape and the
   control plane.
8. Remove old workflow residue from normal composition: `transcode` imports,
   `branchComposePlan`, string output refs, `destinationNames` bridge fields,
   `runtimeBranch`, and workflow-kind compiler switches.
9. Keep first-page examples executable with `goav.Default()`, or clearly label
   adapter requirements; treat adapter coverage as product surface after the
   planner can absorb it.
10. Prepare v0.1 only after README examples compile/run or name their adapter
    requirements, default and tagged tests pass, core stays cgo-free, hot-path
    allocation guards stay green, and one public RTP/WebRTC record branch plus
    one public file transcode branch work end to end. Confirm `go 1.26` in
    `go.mod` is intentional before tagging.

## Phase status

- Phase 0 (API sketch): core media types, codec/format/pipeline/RTP contracts — done.
- Phase 1 (realtime receive): WebRTC track adapter, RTP loss surface, Opus
  depacketize/decode — done.
- Phase 2 (video receive): VP8/VP9 depacketize + govpx decode/encode active;
  Opus/VP8/VP9 are the full encode/decode verticals; H264/AV1 stay
  receive/decode-first until their encode adapters are equally solid.
- Phase 3 (recording/remux): IVF and Annex B packet recording, WebRTC receive to
  file, multi-destination fanout — active.
- Phase 3b (transcode ladders): shared decode, per-branch encoders, resize and
  resample adapters, multi-mux destinations — active.
- Phase 4 (H264/AV1 decode): depacketization, descriptor-only availability,
  build-tagged decoders, allocation/lifecycle hardening — active.
- Phase 5 (high-level planner): fluent recipes, `MediaPlan` IR, reusable flows,
  runtime stage/sink attachments with rollback, detail-aware introspection —
  active; richer stats and tracing remain future work.
