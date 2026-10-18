# goav: architecture review and improvement plan

Reviewed: `main` at `1f6d049c` (2026-10-18), read-only checkout. In-flight work judged from
`origin/feat/dag-direct` (`8d0586a3`, `be973281`). Every finding below cites file:line in that
checkout; where a claim was checkable by running something, it was run (`go test ./...`,
`go test -race` on the root and `pipeline` packages, `go vet`, `deadcode`, `go doc`).

## Executive verdict

goav is not a toy. It is the most complete pure-Go media pipeline I know of: five codec
verticals, three real containers, a planner that refuses bad recipes with codes and fixes before
anything opens, a data plane with genuine branch-local flow control, a task-wide time domain
(rate, pause, seek-with-flush, playout, QoS), a control plane, a pipeline language, and a test
culture (2,049 tests, allocation pins, golden oracle, race-clean, ~14 s per package) that most
C media stacks would envy. The grammar on the front page is good and small.

What keeps it short of state of the art is not missing polish; it is unfinished consolidation
and three structural holes:

1. **Three grammars and five operation records under one front door.** The value grammar
   (`Stream`) compiles onto a retained mutable builder grammar (`recipeJob`/`streamChain`/
   `branchBuilder`/`mixStream`/...), which snapshots into `internal/recipeir`, which is converted
   1:1 into a parallel `intent`/`streamIntent`/`operationSpec` layer, which a 4-way lowerer
   dispatch turns into `composeGraph` or `joinPlan`, which produce `workPlan` + `pipeline.Spec`.
   Roughly 23k of the root package's 34k non-test lines are this machinery. The project's own
   rule (one implementation per concept) is violated at the heart of the compiler, and that is
   exactly why the `internal/` split failed: the coupling the ARCHITECTURE doc measured is the
   coupling between these duplicate layers.
2. **Two runners with different failure semantics.** In a buffered (every realtime decode/encode)
   graph a failing sink does not stop the task; its error is held until every source ends on its
   own. For a live source that is never. The direct runner fails fast. `Task.Run`'s contract
   ("returns the first fatal node error") is only true for one of them.
3. **A live feature set that stops one step short of deployment.** No H.264 encoder, no AAC
   encoder, no RTP/WebRTC egress in the library (the ladder example hand-rolls a sink), no
   SRT/RTMP/MPEG-TS, no URI scheme resolution behind `goav.URI`. The pure-Go constraint is a real
   product choice, but the gap between "pure Go" and "deployable live pipeline" has to be closed
   deliberately, in nested modules where cgo is acceptable, or it stays a demo.

The recommended order is: fix the runner semantics and the dynamic-stream race (days), then
collapse the compiler to one grammar, one operation record, one lowering (weeks, and it unlocks
the package split that was abandoned), then make buffered execution reference-counted instead of
copy-per-edge, then ship the four features that unlock real deployments (H.264/AAC encode in a
cgo-permitted nested module, WebRTC/RTP egress with WHIP/WHEP, LL-HLS done at the sample level,
SRT+TS). Pre-v1 breakage is cheap now and expensive later; take it.

## Findings by area

Severity: **critical** (wrong results or stuck systems for real users), **high** (blocks the
stated goals or misleads users), **medium** (real cost, workaround exists), **low** (hygiene).

### 1. Architecture

**A1. The compiler has three grammars. (high)**
Evidence: `stream.go:23-95` defines the value grammar (`Stream`, `streamNode`, `streamOp`).
`stream_compile.go:30-71` lowers Outputs onto the *old* mutable builder grammar: `compileInputOutputs`
(`:184-268`) calls `job.Video(...)`, `chain.Copy()`, `chain.Branches(...)`, `applyChainOps` replays
every `streamOp` as a `streamChain` method call (`:286-319`), `applyBranchOps` does it again for
`branchBuilder` (`:321-355`), and joins go through `legacyJoinFromRoot` (`:589-624`) into
`legacyMix`/`legacyComposite`/`legacySelect`/`legacyJoin` (`audio_mix.go:354`, `video_composite_build.go:18`,
`select_build.go:18`, `join_custom.go:71`). The builders are `recipeJob` (`recipe.go:307-324`),
`streamChain` (`chain.go:266-270`), `branchBuilder`/`branchRecipe` (`branch.go:123-160`), each
with their own validation and error paths (`chain.go:18-197`, `branch.go:714-1290`). The value
grammar is the only public one; the builder grammar survives because `export_test.go:111-133`
re-exports it (`From`, `Branch`, `LegacyMix`, ...) and 178 test call sites across 12 files use it.
Why it matters: every grammar feature is implemented and validated twice (three times for joins),
`stream_compile.go` has to reverse-engineer fork structure into trunk+branches that the builder
understands (`commonPrefix`, `joinTakes`, `workLeaf`), and known limitations are artifacts of that
translation, e.g. "a recipe runs one join today" (`stream_compile.go:57`), "a join's output takes
a tap only right after the join" (`:457`), and "work shared below the first fork is duplicated
per fork" (`docs/ARCHITECTURE.md:322`). These are the Phase B items in flight; they are symptoms.

**A2. Five parallel operation records. (high)**
`streamOp` (`stream.go:78`), `operationSpec` (`intent.go:62`), `recipeir.Operation`
(`internal/recipeir/recipe.go:212`), `workOperation` (`work_plan.go:51`), `planOperation`
(`media_plan.go:30`), plus `plan.Operation` (public, `plan/plan.go:87`) and `composeNode`
(`compose_dag.go:41`). `operationSpec` and `recipeir.Operation` have identical fields; `recipe_ir.go`
(831 lines) is nothing but bidirectional converters (`recipeIRFromIntent`/`intentFromRecipeIR`
`:156-177`, `recipeIROperationFromSpec`/`operationSpecFromRecipeIR` `:432-471`, and 25 more).
`streamIntent` is still read by 17 non-test files (`validate_codec.go`, `branch_compose_plan.go`,
`media_plan_build.go`, ...). The ROADMAP calls this "the `streamIntent` normalization fold ...
fold them as they are touched"; it has not been folded, it has been mirrored.

**A3. The 4-way lowerer dispatch is a workflow switch in disguise. (medium, in flight)**
`media_plan_spec.go:228-242` tries join, then single-stream packet-copy, then single-stream decode,
then multi-stream job, then branch composer. Three of the four already end in the same
`composeLowerer` (`compose_lowerer.go:14-29`) and `composeGraph` (`compose_dag.go:68-81`), and
`composeGraph` already carries join node kinds (`compose_dag.go:29-35`, `lowerJoinNode` at `:366-371`).
The collapse is the right move; it should land *after* A1/A2 or it will be collapsed onto the
duplicate layers.

**A4. The `internal/` split failed for a reason that is now fixable. (medium)**
`docs/ARCHITECTURE.md:218-246` records the measurement: the planner's closure was 99.5% of the
root package because grammar objects (`branchRecipe`, `jobStreamBuild`, `InputSpec`,
`destinationSpec`, `joinSpec`) are read from planner files. With A1 and A2 done, the planner's
only input is `recipeir.Recipe` plus the two concrete-handle lists (inputs, destinations) that
the doc already calls "the documented IR exception". That is a narrow waist, and the DTO path the
doc proposes becomes a one-package move instead of a slow incremental one.

**A5. Layering direction is sound.** Verified with `go list`: root imports only leaf packages
(`av codec control errcode filter flow format inspect internal/recipeir lifecycle pipeline plan
provider runconfig shape snapshot source`); only `bundle`, `cmd/goav`, `ctlserver`, `expert`,
`goavtest`, `launch` import the root. Nested modules depend on root, never the reverse. No cgo in
the root module; `modernc.org/*` arrives via `goaac` (`go mod why`), allow-listed by test.

**A6. Dead code is low, accidental complexity is high.** `deadcode -test ./...` reports only 9
unreachable functions (exported API nobody calls: `bundle.Explain`, `launch.Join`, `launch.Dir`,
`lifecycle.SwitchBoundary.Kind/MediaTime`, `rootFiles.*`). Without `-test`, 282 functions are
reachable only from tests; 4 are unexported. So the problem is not dead code; it is live code
doing the same thing several times (A1-A3), and 52 "contract" test files (9,288 lines) pinning
unexported helpers in place (see T3).

**A7. `container/matroska` is a 14.7k-line general EBML/Matroska library inside a pipeline repo
(low/medium).** `demuxer.go` 6,646 lines, `muxer.go` 4,809, `matroska_test.go` 17,379 lines in
one file, with chapters, tags, attachments, content encryption, H.265 metadata. It is well
bounded (`ebml.ReaderOptions{MaxElementSize}` `demuxer.go:150`, fuzzers present) but it is 43% of
all non-root non-test code. `webm` is a thin profile wrapper (`container/webm/format.go:20-30`).
Treat it as frozen infrastructure, not as a place to add features.

### 2. Runtime data plane

**R1. Buffered graphs swallow node failures until the sources end. (critical)**
`pipeline/buffered.go:1072-1090` (`runNode`): the first `deliver` error is stored in `first` and
the worker keeps draining and dropping; the error is only reported *after* the queue closes.
`Run` (`:367-435`) closes queues only after `sources.Wait()` returns, and nothing cancels the
sources (no `context.WithCancel` in `buffered.go`; grep confirms the only mention is a comment at
`:853`). So a `MuxStage.Handle` write error (`format/stage.go:408`, e.g. disk full, broken pipe)
in a realtime task is invisible until the live source ends. Compare `pipeline/direct.go:729-775`
and `:407-426`: the direct runner propagates the error through `Emit` to the source and cancels
sibling sources. `goav.go:42-46` promises "returns the first fatal node error". Realtime recipes
that decode or encode are always buffered (`compose_lowerer.go:117-127`, `buffer_budget.go:15-24`),
so the common live path is the broken one. No test pins sink-failure propagation for the buffered
runner (`pipeline/buffered_test.go` has no such case; the only root "failure" tests are
commit/rollback tests in `task_invariants_test.go`, `graph_test.go`).

**R2. Every buffered edge memcpy's every decoded frame. (high, structural)**
`bindFanout` (`buffered.go:1312-1402`) copies any non-`BufferImmutable` payload into slot backing;
decoders emit `BufferOwned` (`adapters/govpx/decoder_govpx.go:270-284`, `gopus/decoder.go:234`)
or `BufferBorrowed` (`adapters/goav1/decoder_goav1.go:480-489`, `goh264/decoder_goh264.go:266-274`),
never immutable, because the codec stage reuses its scratch (`codec/stage.go:359-391` emits
pointers into `s.result.Frames`, reset on the next call). The borrowed-packet refcount sharing
(`:1404-1429`) covers packets only; `docs/ROADMAP.md:163-168` admits frames copy per target. A
1080p I420 frame is 3 MB; a decode feeding a 3-rung ladder copies 9 MB per frame on top of the
encoders' work, and the copy budget machinery (`buffer_budget.go`, `CopyPacketBytes/CopyFrameBytes`,
`ErrBufferedMessageUnsafe`, `ErrMessageTooLarge`, `flow.BufferCopyBounds/CopyMode`) exists only to
make this safe. GStreamer, libav and gstreamer-rs all solve this with refcounted buffers from pools
and writable-only-when-unique semantics.

**R3. "Lock-free data plane" is overstated. (medium, honesty)**
The direct runner takes `handleMu` per delivery (`direct.go:753-756`) and does two `gate` atomics
plus two `defer`s; with several sources (PR #187) deliveries into a shared mux contend on it. The
buffered producer takes `node.queueMutex` per enqueue (`buffered.go:913`) and uses two channel ops
per message (queue + free list). None of this is wrong (a `Stage` must be serialized), but
`README.md:174-176`, `docs/ARCHITECTURE.md:297-298` and `docs/PERFORMANCE.md` say "no per-message
mutex". Say what it is: per-node serialization with lock-free routing snapshots.

**R4. Blocking backpressure polls with `time.After`. (medium)**
`enqueueBlocking` (`buffered.go:978-1011`) releases the mutex and waits on `time.After(100µs)`;
under sustained backpressure (the normal state when a pure-Go encoder is slower than a live
source) the producer wakes 10k times/s and allocates a timer each time. `DropBlock` is the default
for promoted ladders and realtime recipes (`buffer_budget.go:15-24`, `compose_lowerer.go:104`).

**R5. The timeline spawns a goroutine per paced sleep. (medium)**
`timeline.go:225-245` (`sleepInner`) starts a goroutine, a child context and a channel for every
`Sleep`, i.e. per paced packet (`format/pace.go:63-93`) and per playout-gated message. It exists so
fake clocks can be interrupted; the monotonic clock could be waited on directly with a timer plus
the `change` channel. This contradicts the "allocate nothing per message" story on the realtime path
that pins do not cover (pins run on fake clocks).

**R6. Multi-source offline runs are non-deterministic. (high, surfaces with PR #187)**
`direct.go:407-426` runs every source on its own goroutine; `buffered.go:406-415` likewise. Two
file demuxers race into one mux under `handleMu`; interleaving depends on scheduling. The Matroska
muxer tolerates it by opening a new cluster whenever the block timecode leaves the int16 window
(`container/matroska/muxer.go:622-632`, `:361-369`), and progressive MP4 rebuilds tables, so the
files are *valid*, but output bytes differ run to run, A/V interleaving is whatever the goroutines
produced, and HLS segmentation of such a stream is arbitrary. PR #187's
`TestCopyFromTwoFilesIntoOneMux` decodes tracks separately and so cannot see it. The ROADMAP
deferred pull scheduling with the argument that backpressure covers it; backpressure bounds
memory, it does not order packets. ffmpeg's `av_interleaved_write_frame`/lowest-DTS input choice
exists for this reason.

**R7. Time domain: solid, with two honest limits.** The `timeline` (rate, pause, re-anchoring,
`timeline.go:69-149`), seek flush (`pipeline/flush.go`, probe→flush→deliver ordering in
`task_control.go:97-129`), readiness (`pipeline/readiness.go`) and QoS (`qos.go`) are clean and
race-clean. Limits: `Pause` stops time, not data (`goav.go:91-100`), so an unpaced task refuses it,
and `rtpav`'s jitter latency is invisible to `path_latency.go:19-24`. Both are documented; neither
is urgent.

**R8. Error channel is lossy.** `reportBufferedError` (`buffered.go:769-777`) drops errors when
`errs` (capacity nodes+sources+16) is full; `Run` returns one. Low.

### 3. API design

**P1. `Runtime` is invisible in godoc. (high, first impression)**
`goav.go:38` `type Runtime = runtime`; `go doc . Runtime` prints the alias and `New`, nothing else:
`Run`, `Build`, `Describe`, `Explain` (`stream_compile.go:716-749`) are methods on the unexported
type and pkg.go.dev will not list them. The first thing a reader looks up after `New` is empty.

**P2. The front door is right; the periphery is 23 packages. (medium)**
Root is 127 exported symbols, the grammar reads well, and the error contract is the best part of
the project. But a live application imports `goav, bundle, codec, control, inspect, lifecycle,
snapshot, flow, shape, runconfig, source, component, plan, errcode` — several of which are tiny
(`snapshot` 8 symbols, `inspect` 12, `errcode` 12, `provider` 17). The split kept root small by
moving concepts rather than removing them. gstreamer-rs splits by domain (`gst`, `gst_video`,
`gst_app`), not by noun. Pion is many packages but each is a protocol.

**P3. `goav.URI` is a destination without a resolver. (medium)**
`destination.go:275-289` builds a `format.Output{URI: uri}`; there is no protocol registry, no
`http/rtmp/srt` handling anywhere (`grep` over `input.go destination.go source.go runtime_demux.go
mux_destination.go` finds only `ProtocolFile`). A user who writes `goav.URI("srt://...")` gets a
format refusal. Either it is the seam for protocol providers or it should not exist.

**P4. Grammar oddities.** `Stream.SyncByPTS()` is a method on every Stream but valid only on a
join output (`stream.go:210-221`); it belongs on the join constructor. `StreamOption` is
`func(*streamSelectConfig)` with an unexported parameter type (`chain.go:203`), which godoc
renders as an uncallable type. `InputSpec.Track` returns an unexported type (`source.go:52`).
`bundle.Default()` is a process-wide shared runtime (`bundle/bundle.go:52-57`) in a project whose
rule is "no global state"; it is fine, but the docs should say it exists.

**P5. Extension points are genuinely good.** `provider.Source`/`Destination`, `codec`/`format`/
`filter` factories, `goav.Join`, `WrapSource`, `runconfig.WithShapeDelta`, and the external proof
in `adapterproof/` are better than what GStreamer offers in-process. Keep them as they are.

**P6. Control plane: reasonable, unauthenticated by design.** `ctlserver` serves JSON-RPC over
unix/tcp with no auth (`ctlserver/http.go:16-18` says "behind its own authentication"); `Files` is
off by default (`server.go:66-70`). Attach programs can write files once `Files` is granted. Fine
for unix sockets; a TCP listener should at least document that it is a trusted-network interface,
and the CLI's `--control` flag should default to unix.

### 4. Correctness and robustness

**C1. (critical) Buffered failure propagation**: see R1.

**C2. (high) `OnStream` attaches asynchronously, so the first media of a discovered stream is
lost to the rule's branch.** `runtime_stream_rule.go:61-95` subscribes to `stream_added` through
`Watch`, i.e. the graph event channel → distributor goroutine → rule goroutine → `attachRuntimeBranches`.
The source keeps emitting the new stream's packets meanwhile (`rtpav/source.go:341-369` appends
the stream and continues). The branch misses everything up to attach completion; for video that
is the first keyframe, so the branch waits for the next one. The comment at `:15-19` even notes a
shed announce "would be a silent non-attach". `stream_rule_test.go` only asserts eventual
delivery (`waitForCount(..., 1, ...)` at `:408,:506`), so the gap is unpinned. GStreamer makes
`pad-added` synchronous on the streaming thread for exactly this reason.

**C3. (high) Non-deterministic multi-source interleaving**: see R6.

**C4. (medium) Decoder scratch frames are reused while downstream may still hold them.**
`codec/stage.go:359-391` emits `&s.result.Frames[i]`; `DecodeResult.Reset` (`codec/interfaces.go`)
clears them on the next packet. In direct graphs delivery is synchronous, so a sink that retains
a frame (anything that does not copy, unlike `goavtest.Collector` which deep-copies,
`goavtest/collector.go:166-182`) sees it overwritten. The ownership vocabulary (`av/types.go`
`BufferOwned` "belongs to the receiver, which may mutate them") says the opposite of what happens:
the receiver gets a borrowed scratch buffer labelled owned. Buffered graphs mask it by copying
(R2). This is the same root cause as R2 and has the same fix.

**C5. (medium) `Inject` can call `pending.Add` concurrently with `pending.Wait`.**
`buffered.go:416` waits on `pending` after the sources exit; a control injected from another
goroutine in that window (`inject.go` → `enqueue` → `pending.Add(1)`), or a stage that emits from
a goroutine of its own, adds from zero while `Wait` runs. Workers re-emitting during drain are
safe because they still hold their own count. The WaitGroup contract forbids a positive `Add`
concurrent with `Wait` at zero; the race detector flags it under the right interleaving. Low
probability, ugly when it happens; 0.1's cancel-on-error restructuring is the place to fix it
(track in-flight messages with an explicit counter and a done channel instead of a WaitGroup).

**C6. (low) Jitter ring loss accounting.** `rtpav/jitter.go:78-83` adds the whole sequence delta to
`Lost` when a packet lands past the ring, although some of those sequence numbers may be buffered.
Stats only.

**C7. (low) `MuxStage` forwards frames.** `format/stage.go:417-418` emits frame messages downstream
of a muxer; harmless but a sign the message kinds are not typed at the edge.

Verified clean: `go vet ./...` (no findings), `go test -race ./pipeline .` (pass), full
`go test ./...` (pass, no skips except goaac-on-target and shell-out docs tests), fuzzers for
Matroska/WebM/MP4/EBML/launch exist and run in CI, container input is bounded (`mp4/limits.go`,
`ebml.ReaderOptions.MaxElementSize`).

### 5. Feature gaps, ranked by impact for real users

1. **H.264 encoding (critical for deployment).** Decode is behind a build tag
   (`adapters/goh264/*.go:1 //go:build goav_goh264`); encode is descriptor-only
   (`docs/ROADMAP.md:201-208`). HLS to Apple devices, RTMP to any CDN, SRT to any broadcast
   chain, and most WebRTC interop need H.264. VP9/AV1-only output is a lab configuration.
2. **AAC encoding.** `adapters/goaac/decoder.go:36` is decode-only; Opus is the only audio
   encoder. HLS/RTMP/TS audio is AAC in practice.
3. **RTP/WebRTC egress + WHIP/WHEP.** No packetizers or local-track destination in `rtpav`/
   `webrtcav` (grep for `Packetiz|TrackLocal` finds nothing outside examples); the ladder example
   hand-rolls `TrackLocalStaticSample.WriteSample` in a sink (`examples/webrtc-runtime-ladder/
   branches.go:201-232`). The library can receive WebRTC but not serve it.
4. **Transport protocols: SRT, RTMP/FLV, MPEG-TS, RTSP.** `av.FormatFLV` is probed
   (`format/probe.go:63`) but no FLV demuxer/muxer is registered; `FormatOgg` likewise. The
   ROADMAP lists SRT/NDI as "planned via provider.Source" with no owner.
5. **LL-HLS (in flight) and HLS serving.** The packager writes files; there is no origin, no
   blocking reload, no parts. See the in-flight judgement below.
6. **Frame-domain toolbox.** Only `Resize` and `Resample`. No pixel-format conversion as a filter
   (NV12/RGB), no overlay/text/crop/pad, no deinterlace, no audio gain/normalize/channel mixing
   beyond resample. Composite is a blit, Mix a sum.
7. **Observability export.** `Stats`/`Snapshot`/`Watch` are good primitives; nothing exports them
   (Prometheus/OTel). Cheap to add as a package, high value for operators.
8. **Seeking inputs beyond files.** `format.Seeker` exists for Matroska/MP4; fine.

### 6. Tests and quality infrastructure

**T1. Strengths.** 2,049 tests, 66 benchmarks, 7 fuzzers; golden oracle of 38 recipes pinning
Describe/Explain/refusals (`compiler_oracle_test.go`); `Describe()==Build()` laws; allocation pins
in plain `go test`; CI matrix with scalar and purego lanes, race, bench smoke, PGO freshness,
alloc ratchet, fuzz smoke, nested modules and all examples (`.github/workflows/ci.yml`). Root
package tests run in 10.8 s. This is a serious suite.

**T2. The suite pins the legacy grammar.** 178 `goav.From(` call sites in 12 files reach the
builder grammar through `export_test.go:111-133`. Deleting the builder (A1) means rewriting or
deleting those tests; many duplicate value-grammar tests (238 `.Video()./.Audio().` sites).

**T3. Helper-contract tests prove less than they claim.** 52 files / 9,288 lines of
`*_contract_test.go`, `*_helper_contract_test.go` pin unexported helpers, e.g.
`explain_helper_contract_test.go:15-33` asserts `workOperationEncodeCodec` returns its input
field. These freeze implementation shape, not behavior, and will all break in A1/A2. The
`docs_*`/`readme_*`/`citation` meta-tests are good and should stay.

**T4. Unpinned behaviors that matter:** buffered sink-failure propagation (R1); first-packet
delivery for `OnStream` branches (C2); byte-deterministic output for multi-input offline runs
(R6); frame lifetime past `Handle` (C4); backpressure CPU under sustained block (R4).

**T5. Flakiness risk is moderate and managed.** 24 `time.Sleep` in tests, 68 poll loops with
10 s deadlines (`waitForCondition`), fake clock for pacing. CI runs `-race` only in the full lane.
No `t.Parallel` hazards found.

**T6. Single-file giants.** `matroska_test.go` 17,379 lines, `recipe_api_test.go` 4,966,
`webm` 5,581 lines of tests for 829 lines of code. Not wrong, but review-hostile.

### 7. Performance architecture

Per-message overhead (direct): one atomic routing snapshot load, counters, `handleMu`, two gate
atomics, two defers, string compares on `Kind`/`Policy`/`StreamID` (`direct.go:851-885`). Fine for
video; marginal for RTP audio at thousands of packets/s across many branches. Buffered adds a
mutex, two channel ops, and a payload copy per edge (R2). The structural ceilings are:

- **Copy-per-edge frames (R2)**: the one that caps fan-out scaling.
- **Codec threading**: VP9 encode picks tile threads (`adapters/govpx/encoder_vp9_govpx.go:198-222`),
  AV1 decode has a tile worker pool (`adapters/goav1/decoder_goav1.go:69-79`), AV1 encode and VP8
  are single-threaded, and a `Stage` is serialized per node, so pipeline parallelism comes only
  from buffered promotion of ladders (`docs/FLOW_CONTROL.md:28-36`). That is the right model
  (GStreamer's queue-per-branch), but frame copies make each extra worker cost 3 MB/frame.
- **SIMD**: portable `simd` kernels for mix/resize/resample under `GOEXPERIMENT=simd`, no asm
  (`TestNoRepositoryAssembly`). Good; the codecs are external and own their SIMD.
- **Memory**: decode scratch is preallocated at stream dimensions (`runtime_decode.go:305-335`);
  slot backing grows to fit (`buffered.go:1285-1306`). Bounded, no pools across stages; R2's fix
  introduces the pool.
- **Allocation claims are honest** where pinned, and wrong on two unpinned paths (R5, R4).

### In-flight work, judged

**PR #187 (`feat/dag-direct`): several file inputs, joins over files.** Direction is right and the
mechanism is clean: a `qualifiedDemuxer` stamps `1@a.webm` ids in place (`runtime_demux.go` diff),
selects bind to one input, joins bind file arms at open and conform formats as frames arrive.
Two concerns: (1) it inherits R6, so `a.Video().To(out), b.Video().To(out)` produces
non-deterministic interleaving and the new golden/oracle entries cannot catch it; merge it, but
land the ordered scheduler (Phase 0.4) right behind it and add a byte-stable or
DTS-monotonic assertion to `multi_file_test.go`. (2) "conform as frames arrive" builds converters
at run time from the first frame (commit `be973281`), which is the first place a conversion is
decided outside the solver; keep it, but make `Explain` show the conform node with its *planned*
input (it does, per the commit message) and make sure the runtime converter is the same filter
factory the solver would pick, pinned by a test.

**LL-HLS (parts, blocking reloads, rendition reports, delta updates).** The current packager
re-parses the fMP4 byte stream box by box (`container/hls/hls.go:155-222`, `readTracks`,
`fragmentTimes`) and cuts segments at keyframe-aligned fragments produced by the muxer
(`container/mp4/muxer.go:33-36`: "a moof per video keyframe"). Parts need sub-keyframe fragments
(200-500 ms) and a muxer that cuts on a duration, not only on keyframes; blocking reloads and
delta updates need an HTTP origin that owns playlist state, not a `Store` that writes files. If
LL-HLS is built on byte re-parsing it will work, but it will be the second place that infers media
structure from muxed bytes. Prefer: give `mp4.FormatMuxer` an explicit fragment policy
(`FragmentDuration`, `PartDuration`) and a fragment callback or `WriteResult` events
(`format/stage.go:46` already carries events), and make the packager a sink over fragments. Serve
through an `hls.Origin` `http.Handler` that implements `_HLS_msn/_HLS_part` blocking, preload
hints, and `#EXT-X-SKIP` from in-memory state; the file `Store` stays as the archival/CDN path.

**Phase B slices (forks across inputs, join beside streams, shared work below the first fork,
lowerer collapse).** All four are consequences of A1: `compileInputOutputs` can only express
"one trunk, N leaves" because the builder grammar only has `chain.Branches(...)`, and
`compileOutputs` refuses a join beside other streams because `recipeJob` holds one `join`
(`recipe.go:321`). Doing them as slices on top of the builder adds translation code that A1 then
deletes. Recommendation: finish the lowerer collapse (it is below the builder and survives), pause
the other three, and get them for free from Phase 1.

## The plan

Ordered. Each item: goal, design sketch, what gets deleted or simplified, risk, effort
(S ≤ 2 days, M ≤ 1 week, L ≤ 3 weeks, XL more), verification.

### Phase 0: correctness first (one or two weeks total)

**0.1 Fail fast in the buffered runner.** (critical, S)
Goal: a node error stops the task promptly in both runners, with the same contract.
Design: `bufferedRunner.Run` derives `runCtx, cancel := context.WithCancel(ctx)`; `runNode`
records the first error into a `firstErr` (`atomic.Pointer[error]` or `sync.Once`) and calls
`cancel()`; sources see `ctx.Err()` on their next `Emit`/read and return; `Run` returns the
recorded node error (not `context.Canceled`) when one exists. Keep fan-out backpressure isolation
(`buffered.go:855-860`) as is: a *shed* is not a failure. Mirror the direct runner's
"first error cancels siblings" wording in `Graph.Run` docs.
Delete/simplify: the `first != nil` drain-without-delivering loop becomes a plain return.
Risk: tasks that relied on a dead branch being ignored; none should.
Verify: new tests in `pipeline/buffered_test.go` and root: a live `goavtest.LiveAudio` source
with a sink that errors on the 10th message; `Run` returns that error within 1 s in direct,
buffered, and ladder-promoted graphs; destinations abort (`finishDestinations`).

**0.2 Export `Runtime` as a real type.** (high, S)
Design: rename `runtime` → `Runtime` (struct), drop the alias; keep fields unexported. Fix the
`runtime` package-name shadowing where it bites (`import "runtime"` in `direct.go` is in another
package; root has none).
Verify: `go doc . Runtime` lists `Run/Build/Describe/Explain`; `TestReadmeGoExamplesCompile` passes.

**0.3 Make `OnStream` attach synchronous with the first media.** (high, M)
Goal: a rule-created branch sees the discovered stream's first packet.
Design: route `EventStreamAdded` through a task-owned `announce` stage installed right after each
source with rules (the compiler already inserts select stages per source). On `stream_added` the
stage runs the rule engine *inline* on the data path (`handleStreamAdded` moved off the Watch
goroutine; `attachRuntimeBranches` already takes `attachMu` and the runners support live
`AddStage/Connect`). Because the stage is serialized, the next message of that stream is delivered
after the branch exists. Keep `Watch` publication of the event for observers. For sources that
announce and immediately emit on the same goroutine this is exactly GStreamer's pad-added.
Delete: the `streamRuleEventCapacity` watcher and its "shed announce" caveat.
Risk: attach cost (destination open) now sits on the source's goroutine for one event; bound it
with the attach ctx and document that destination `Open` must be fast or asynchronous.
Verify: test that the first packet after `stream_added` reaches the rule branch (count == all).

**0.4 Deterministic offline multi-source scheduling.** (high, M)
Goal: offline runs with N file sources produce the same bytes every run, interleaved by time.
Design: in the direct runner, when `!config.Realtime` and `len(sources) > 1`, run sources on one
goroutine with a `nextTime()` peek: extend `format.Demuxer` with an optional
`Peek(ctx) (av.Timestamp, bool)` (or have `DemuxSource` pre-read one packet) and always advance
the source with the smallest next DTS/PTS (ffmpeg's rule). Sources without timestamps (custom
push sources, providers) keep their own goroutine. Realtime graphs are untouched: live sources
are paced by the clock and interleave by arrival, which is correct.
Delete: nothing; `handleMu` stays for the realtime/mixed case.
Risk: a peeked packet is held across the scheduler, so `ReadResult` scratch must double-buffer.
Verify: `multi_file_test.go` asserts byte-identical output over 5 runs and monotonic cluster
timecodes/DTS in the mux; re-record the golden oracle.

**0.5 Stop polling under backpressure; stop spawning goroutines to sleep.** (medium, S+S)
`enqueueBlocking`: add a per-node `space chan struct{}` (capacity 1) that `releaseSlot` signals;
wait on `space`/`ctx.Done()`/`queueClosed` instead of `time.After`. `timeline.sleepInner`: for the
monotonic clock, `select` on a `time.Timer`, the `change` channel and `ctx.Done()` directly; keep
the goroutine path only for clocks that are not `*monotonicClock` (fakes).
Verify: a benchmark of DropBlock backpressure shows ~0 allocs/op and CPU idle while blocked;
`TestGraphDirectRunAllocs`-style pin for paced sleeps with the real clock.

### Phase 1: one compiler (the big structural move)

**1.1 Compile the value grammar straight into `recipeir.Recipe`; delete the builder grammar.** (XL)
Goal: `Stream` tree → `recipeir.Recipe` in one pass; no `recipeJob`, `streamChain`,
`branchBuilder`, `mixStream`, `compositeStream`, `legacySelectStream`, `joinStreamBuilder`.
Design: `recipeir.Recipe` becomes a DAG, not "streams + branches + one join": nodes are
`{Input|Select|Op|Join|Output}` with parent edges; forks are nodes with several children; joins
are nodes with several parents; names come from `As`; buffers/sync/playout are ops. The
`streamNode` tree already *is* this DAG (`stream.go:29-35` has `parent`, roots carry inputs or
joins); `compileOutputs` only has to intern shared nodes (same `*streamNode` → same IR node) and
validate (domain rules from `chain.go:18-197` and `branch.go:714-860`, moved to one validator over
IR nodes). The "one join per recipe", "join beside streams", "tap only right after join" and
"shared work below first fork" restrictions disappear because nothing forces trunk/branch shape.
Delete: `recipe.go` (except `BuildError`), `chain.go`, `branch.go` (builders), `join_build.go`,
`select_build.go`, `video_composite_build.go` builder half, `audio_mix.go:354+`, `join_custom.go`
builder half, `stream_compile.go:184-673`, `export_test.go` builder re-exports, and the 12 test
files' `From(` usages (rewrite the ones that test behavior with the value grammar; drop the ones
that test builder plumbing). Expect roughly −6k non-test lines.
Risk: large; the golden oracle (38 recipes) and the Describe==Build laws are the safety net.
Add oracle entries for the new shapes (join beside streams, nested forks sharing a transform)
*before* starting, with expected graphs written by hand.
Verify: oracle unchanged for existing entries except where the restriction removal changes the
plan deliberately; `TestCompositionLawsHoldForGeneratedRecipes` extended with multi-join and
deep-fork generators.

**1.2 One operation record.** (L, can run in parallel with 1.1 once the IR shape is agreed)
Goal: `recipeir.Operation` is the only pre-lowering operation record; `workOperation` the only
post-lowering one; `plan.Operation` is a projection for Explain.
Design: delete `intent`, `inputIntent`, `streamIntent`, `destinationIntent`, `operationSpec`
(`intent.go`) and all converters (`recipe_ir.go`), `cloneStreamIntent` etc. (`media_plan_spec.go:
314-331`). The 17 `streamIntent` readers take `recipeir.Stream`/`[]recipeir.Operation`. Move
`operation_facts.go` helpers onto `recipeir` (as functions in `internal/recipeir` or a small
`internal/irfacts`).
Delete: ~1.4k lines (`intent.go`, `recipe_ir.go`) plus clone helpers.
Verify: oracle; `TestPlannerFilesDoNotReadBuilderInternals` becomes "planner files import only
recipeir".

**1.3 Finish the lowerer collapse (in flight) on the new IR.** (M)
`graphPlanLowererForState` → one `lowerRecipe(recipe) (composeGraph, error)`; `joinPlan` becomes
join nodes inside `composeGraph` (the node kinds and `lowerJoinNode` already exist,
`compose_dag.go:29-35, :366`). `workPatch` (attach) lowers a sub-DAG anchored at a tap through the
same function; `Attach` stops being a second planner (`runtime_attach.go` is 2,208 lines; most of
`planAttachBranchSteps` duplicates compose lowering).
Delete: `media_plan_spec.go` dispatch, `join_plan.go` planning half, most of `work_patch.go`,
`branch_compose_plan.go`/`branch_compose_build.go` where they re-derive what the DAG states.
Verify: `Describe()==Build()` for attach patches (already tested); `BenchmarkAttachDetachUnderLoad`
unchanged or better.

**1.4 Then split the package.** (M)
With 1.1-1.3 the planner's inputs are `recipeir.Recipe` + concrete handles. Move
`internal/compile` (validate, solve shapes, lower to `composeGraph`, emit `pipeline.Spec` and
`workPlan`) out of root; root keeps grammar values, `Runtime`, `task`, attach/control/watch,
destination handles, and the builder that opens adapters. Re-run the type-checked reference graph
the ARCHITECTURE doc describes to confirm the closure is now a minority of root.
Verify: `TestRecipeIRImportsOnlyLeafPackages` plus a new "root does not import internal/compile
internals beyond its entry function" pin.

### Phase 2: one runtime, reference-counted

**2.1 Share the runner topology; keep two delivery strategies.** (M)
`direct.go` and `buffered.go` duplicate `AddSource/AddStage/AddSink/Connect/Disconnect/Remove/
Spec/Stats/readiness/publishEvent/close` (~600 lines). Extract `topology` (nodes, routes,
snapshot rebuild, spec, stats, readiness) and make `directRunner`/`bufferedRunner` delivery
strategies over it. 0.1's error contract lives in the shared part.
Verify: existing pipeline suite; `TestGraphDirectRunAllocs`/`TestGraphBufferedSteadyEmitAllocs`
hold.

**2.2 Reference-counted payloads instead of copy-per-edge.** (L, the main performance move)
Goal: a decoded frame or demuxed packet is allocated once, shared by reference across every
branch, released when the last consumer is done; no `CopyPacketBytes/CopyFrameBytes` budgets.
Design: `av.Buffer` gains `Retain()`/`Release()` backed by `Owner` (a pool); payloads are
immutable after emit by contract ("writable only when unique", libav/GStreamer rule); a stage that
needs to mutate calls `MakeWritable` which copies iff refs > 1. Codec/filter stages allocate
result frames from a per-stage pool sized by `DecodeBounds.MaxFramesPerInput` × (downstream queue
capacity + 1) instead of reusing one scratch (`codec/stage.go:359-391` emits pool frames and
releases its own ref after `Emit` returns). The buffered runner binds a slot by retaining, not
copying (`bindFanout` shrinks to the immutable path); the direct runner changes nothing except
that sinks may now retain frames legally (fixes C4). `goavtest.Collector` stops deep-copying.
Delete: `bufferedMessage.packetBacking/frameBacking/planes`, `bindSharedBorrowedPacket`,
`growBacking`, `validateBufferedMessage`'s copy branches, `buffer_budget.go` copy budgets,
`flow.BufferCopyBounds/BufferCopyMode/CopyMode`, `ErrBufferedMessageUnsafe/ErrMessageTooLarge`
and their `BuildError` mappings (`runtime.go:569-616`), `BufferOwnership` as a tri-state (keep
`Immutable` semantics only).
Risk: use-after-release bugs. Mitigate with a `-tags goavdebug` poison-on-release mode and race
tests that hold frames across pushes; keep the per-branch mutation-isolation tests
(`TestGraphBufferedFanoutMutatingSinkCannotCorruptSibling`) passing via `MakeWritable`.
Verify: `BenchmarkBranchFanout/branches=8` B/op drops to ~0 per message with real I420 frames;
RSS of a 3-rung 1080p ladder drops by the removed copies.

**2.3 Typed hot-path tags.** (S, optional)
`MessageKind`, `RoutePolicy`, `DropPolicy` as small integers with `String()`; intern `StreamID`
per route to compare an int. Only after 2.2; measure first.

### Phase 3: API coherence (pre-v1 breakage welcome)

**3.1 Fold the noun packages.** (M)
`inspect` (12), `snapshot` (8), `lifecycle` (31) → one `task` package (`task.Snapshot`,
`task.Watch` filters, `task.DrainBranch`, `task.SwitchAt`), or into root if root stays under ~180
symbols after 1.x. Keep `plan`, `errcode`, `shape`, `flow`, `control`, `provider`, `codec`,
`format`, `filter`, `pipeline`, `av`, `source`, `component`, `runconfig`. Update
`docs/API_SURFACE.md` in the same change.
Verify: `TestReadmeGoExamplesCompile`, docs link tests, examples build.

**3.2 Make `goav.URI` real or remove it.** (M to make real)
Design: `runconfig.WithProtocol(scheme string, p provider.Protocol)` where `Protocol` opens a
`provider.Source` or `provider.Destination` from a URL; `goav.URI(u)` (destination) and a new
`goav.Open(u)` (input) resolve through it; file scheme built in. Nested modules register `srt`,
`rtmp`, `whip`/`whep`. The `launch` language gains `srt://...` sources and sinks for free.

**3.3 Grammar nits.** (S) Move `SyncByPTS` to a join option (`goav.Mix(a, b, goav.ByPTS())`);
give `StreamOption`/`DestinationOption`/`InputOption` the same sealed-interface shape; export the
`Track` anchor type or return `Stream`.

**3.4 Say what the data plane is.** (S) Replace "no per-message mutex"/"lock-free" in README,
ARCHITECTURE and PERFORMANCE with "lock-free routing snapshots, per-node serialization, bounded
queues"; list R4/R5 as measured once fixed.

### Phase 4: features that unlock deployments

**4.1 H.264 + AAC encoders in a cgo-permitted nested module now; pure Go later.** (M now, XL later)
`github.com/thesyncim/goav/x264av` (or `openh264av`) and `fdkaacav` as nested modules behind the
existing `codec.EncoderFactory`, registered with `runconfig.WithEncoder`. The core stays
`CGO_ENABLED=0`; users who need H.264 today opt in. In parallel, a pure-Go baseline/main profile
I/P encoder (CAVLC, no B-frames, 16x16/8x8 ME, deblocking) is an XL project but it is the single
feature that makes "pure Go, one binary" true for live streaming; goh264's decoder tables and
bitstream code are a head start. Bundle it when its speed is within 3x of x264 `ultrafast`.
Verify: the existing `BenchmarkRealH264Encode` stops skipping; HLS/MP4 outputs play in Safari.

**4.2 RTP/WebRTC egress with WHIP/WHEP.** (L)
`rtpav`: packetizers for Opus/VP8/VP9/AV1/H.264 (Pion has them; wrap behind a `Packetizer`
interface like `Depacketizer`), an RTP `provider.Destination` writing to a `PacketWriter`, RTCP
PLI/FIR → `control.Keyframe` through the existing `EventKeyframeRequired` path, REMB/TWCC →
`EventQoS`/bitrate controls. `webrtcav`: `Track` destination over `TrackLocalStaticRTP`; a WHIP
client (`webrtcav/whip`) and a WHEP server handler. Replace the ladder example's hand-rolled sink
with it.
Verify: `examples/webrtc-runtime-ladder` uses only library destinations; an interop test against
Pion's WHIP/WHEP reference.

**4.3 LL-HLS at the sample level with an origin.** (L; redirect the in-flight work)
As judged above: fMP4 muxer fragment policy (`FragmentDuration`, `PartDuration`), fragment events
on `WriteResult`, packager over fragments, `hls.Origin` `http.Handler` with blocking playlist
reload (`_HLS_msn`, `_HLS_part`), preload hints, delta updates, rendition reports across a
`Ladder`. Keep `Store` for files.
Verify: Apple's `mediastreamvalidator` on a 10-minute live run; hls.js in a browser test.

**4.4 SRT ingest/egress + MPEG-TS.** (L)
Nested module `srtav` over `datarhei/gosrt` (pure Go) as `provider.Source`/`Destination`;
`container/mpegts` mux/demux in-house (PES/PAT/PMT/PCR with H.264/AAC/Opus), registered by
`bundle`. This is the broadcast on-ramp and what most "I want to replace ffmpeg for live" users
ask for first.

**4.5 RTMP/FLV ingest.** (M) FLV demuxer/muxer (the probe exists), an RTMP client/server in a
nested module. Lower than 4.4 because RTMP is fading, but still the OBS path.

**4.6 Frame toolbox.** (M each) Pixel-format convert filter (I420↔NV12↔RGBA, 10-bit), overlay/
crop/pad, audio gain/normalize/channel map. All through `filter.Factory`, all solver-visible via
`shape.Conversions`.

**4.7 Metrics bridge.** (S) `goavmetrics` package: poll `Stats`/`Snapshot`, publish OTel metrics
and `expvar`; event log from `Watch`.

### Phase 5: tests and quality

**5.1 Delete helper-contract tests as their helpers go (Phase 1); replace with behavior tests on
the value grammar.** (M, folded into Phase 1 work)

**5.2 Pin the unpinned.** (S each) R1 propagation; C2 first-packet; R6 determinism; C4 frame
retention across pushes; R4 idle CPU under block.

**5.3 Property/fuzz additions.** (M) Recipe fuzzer over the value grammar (random DAGs →
`Describe` never panics, `Describe==Build`); mp4 mux→demux round trip fuzz; `rtpav` depacketizer
fuzzers; `ctlserver` JSON-RPC fuzz.

**5.4 Split the giants.** (S) `matroska_test.go` and `recipe_api_test.go` by topic.

## What not to do

- **Do not attempt the `internal/` package split before Phase 1.** It failed once on the
  bidirectional coupling that the builder/intent duplication creates; it will fail again.
- **Do not add the three remaining Phase B slices on top of the builder grammar.** They are
  artifacts of `chain.Branches` and `recipeJob.join`; Phase 1 removes the constraint instead of
  translating around it.
- **Do not expose a pull-scheduling or clock-selection API.** The offline scheduler (0.4) is an
  internal runner policy; the public contract stays "sources run, consumers backpressure".
- **Do not make `Pause` gate the data plane.** Time-only pause is coherent and cheap; a data gate
  would need per-node state and reorder semantics for no user demand.
- **Do not put cgo or hardware codecs in the root module.** The nested-module rule already allows
  them (Pion lives that way); keep the `CGO_ENABLED=0` core promise.
- **Do not grow `container/matroska`** beyond conformance fixes; it is complete for goav's needs.
- **Do not chase GStreamer plugin parity, a plugin loader, or a JIT.** The project's thesis
  (typed values, structured errors, one binary) is right; breadth comes from nested modules.
- **Do not add more noun packages** to shrink the root symbol count; count concepts, not symbols.
- **Do not keep the builder grammar alive for tests.** If a behavior is worth pinning, it is
  expressible in the value grammar; if it is not, it was pinning plumbing.

## Appendix: numbers

- Root package: 79 non-test files, 34,326 lines; 153 test files, 50,962 lines; 127 exported
  symbols. Planner/builder/attach files: 22,871 lines (67% of root).
- Public packages: 23 in the root module (+3 nested modules, +11 example modules); ~1,300
  exported symbols total.
- `pipeline`: 3,765 lines (`buffered.go` 1,592, `direct.go` 927).
- Containers: matroska 14,659 / mp4 3,203 / hls 757 / webm 829 / ebml 685 non-test lines.
- Tests: 2,049 `Test`, 66 `Benchmark`, 7 `Fuzz`; full root-module suite passes in ~14 s wall per
  package; `-race` on root and `pipeline` passes; `go vet` clean; `deadcode -test` 9 findings.
- Git: 1,103 commits since 2026-06-05, 63 since July; no tags yet.
