# goav and GStreamer

GStreamer is a mature, general-purpose multimedia framework: elements, pads,
caps negotiation, and pipeline state machines, a vast plugin ecosystem, and
hardware-accelerated backends, refined over decades (see the GStreamer
Application Development Manual for the model). goav does not replace that.
It is a Go-native recipe grammar and runtime for application-owned media
workflows: typed construction, structured errors, inspectable plans, runtime
attach, and pure-Go single-binary deployment.

The trade, stated plainly: GStreamer gives you a multimedia operating layer
and the ecosystem that comes with it; goav gives you a small governed Go API
where the pipeline is ordinary, testable application code. Pick GStreamer
when you need its breadth — exotic formats, hardware codecs, playback
stacks, auto-plugging. Pick goav when the media workflow belongs to a Go
application and you want one static binary and a contract you can pin in CI.

Every goav cell below cites the repo test, benchmark, or document that backs
it; **roadmap** marks what does not exist yet. GStreamer cells describe the
framework generically, as its own documentation presents it; no version
claims are made.

| Capability | GStreamer | goav |
|---|---|---|
| Pipeline construction | Elements linked by pads into bins/pipelines; textual `gst-launch` syntax; C API with bindings | Typed Go grammar `From(...).….To(...)`; recipes are values; surface governed by `api_surface_pin_test.go` + `docs/API_SURFACE.md`; 13 runnable examples in `example_test.go` |
| Media negotiation | Automatic caps negotiation between pads, with converter elements inserted by the author or auto-pluggers | Explicit shape solving: every operation validated before resources open; conversions inserted only under a declared `.Auto(...)` policy, asserted with `.Require(...)`, biased with `.Prefer(...)` (`shape_solver_test.go`, `shape_require_prefer_test.go`) |
| Dynamic streams | `pad-added` signals and auto-plugging (`decodebin`/`uridecodebin`) handled in application callbacks | Declarative `OnStream(match, branches...)` rules: late streams attach planned branches through the same planner, detach with drain on removal (`stream_rule_test.go`: `TestOnStreamAttachesLateBranchAndDetachesOnRemoval`) |
| Fanout | `tee` element plus per-branch `queue`s, leaky modes for shedding | `Branches(...)` with branch-local buffer policies (`flow.Blocking`/`DropOldest`/`Latest`) and a pinned ownership contract — a mutating branch cannot corrupt a sibling (`branch_buffer_test.go`, `copy_contract_test.go`; `BenchmarkBranchFanout`) |
| Mux groups | Muxer elements with request pads linked per stream | Reusing one destination value groups branches into one mux/sink group; same-name distinct handles refuse (`TestFromMultiInputPlanDedupesSharedDestination`, `TestFromMultiInputRejectsConflictingDestinationHandles`; `BenchmarkSharedMuxGroup`) |
| Custom source | `appsrc`, or a `GstBaseSrc` subclass | `goav.Source(fn)` push API with per-push `Accepted`/`Dropped` results (`source_push_test.go`), or the `provider.Source` transport seam — RTP/WebRTC are ordinary providers (`adapterproof/adapter_compat_test.go`) |
| Custom destination | `appsink`, or a `GstBaseSink` subclass | `goav.Writer`/`Custom`/`Sink` destinations, including transactional commit/abort uploads (`task_invariants_test.go`: `TestTransactionalCommitFailureSurfacesFromTaskClose`; `adapterproof/adapter_compat_test.go`) |
| Custom codec / filter / container | GstElement plugin API in C (or bindings), installed and discovered as plugins | Exported factory interfaces plus per-runtime `With*` registration; one toy implementation of every seam runs end to end in `adapterproof/adapter_compat_test.go` (guide: `docs/ADAPTER_AUTHORING.md`) |
| Error reporting | `GError` messages on the pipeline bus, element-defined | One structured `BuildError` everywhere: a typed code from the `errcode` catalog, failing operation/node, machine-readable details, concrete fixes (`docs/ERRORS.md`; `errors_pin_test.go`; 10 acceptance snippets in `error_acceptance_test.go`) |
| Graph inspection | `GST_DEBUG_BIN_TO_DOT_FILE` dot dumps; bus messages | `Explain(ctx)`/`Describe()` before any resource opens — plans, decisions, diagnostics as data (`plan.Report`); described and built graphs are guarded equal (`TestJoinDescribeEqualsBuild*` in `join_plan_test.go`, `join_nested_test.go`) |
| Runtime mutation | Pad probes and blocking for dynamic relinking; powerful, manual | Atomic grouped `Attach` with full rollback (`TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`), gapless boundary-gated `Rebranch` (`runtime_branch_control_test.go`), per-branch `Pause`/`Resume`; race-safe snapshots (`task_invariants_test.go`) |
| Deployment model | Shared C libraries plus runtime plugin scanning; system or bundled installs | Pure Go, `CGO_ENABLED=0`, one static binary; cgo-free core is pinned (`hygiene_test.go`: `TestNoCGOImports`) and CI builds with CGO disabled (`.github/workflows/ci.yml`) |
| Performance proof status | Mature C implementation, decades of production tuning; no claim measured here | Contract + benchmarks present: allocation pins in plain `go test`, 16 measured workloads (`bench_test.go`, `perf_pin_test.go`, `docs/PERFORMANCE.md`); **no cross-framework comparison performed** |
| Ecosystem maturity | Decades old; hundreds of plugins across the base/good/bad/ugly modules; hardware backends (VA-API, NVDEC, V4L2, ...); large community — **GStreamer wins this row decisively** | Young; the standard adapter set is IVF, Annex B, Matroska/WebM, Opus, VP8/VP9 (full verticals), H264/AV1 (decode-first), resize/resample (`docs/ADAPTERS.md`) |

## What goav deliberately does not have

- A plugin ecosystem or binary plugin loading — extensions are Go packages
  compiled in through the seams above (`docs/ADAPTER_AUTHORING.md`).
- Hardware codec backends in core — core stays cgo-free
  (`TestNoCGOImports`); acceleration belongs in external adapters. Roadmap
  for adapters, non-goal for core (`docs/ROADMAP.md`).
- Playback/display stacks, device discovery, auto-pluggers — out of scope;
  goav assumes the application owns its endpoints.
- A/V sink synchronization and pull scheduling — deferred, analysed in
  `docs/NORTH_STAR.md` (theme C) and `docs/ROADMAP.md`. Roadmap.

## On performance comparisons

This repo claims only what it proves: `docs/PERFORMANCE.md` separates
proven allocation pins, measured benchmarks, experimental costs, and an
explicit not-proven list. No goav-vs-GStreamer measurement exists, so none
is implied; the only comparative data in-repo is the optional external-tool
container benches (ffprobe/ffmpeg/mkvmerge) in `container/matroska` and
`container/webm`.
