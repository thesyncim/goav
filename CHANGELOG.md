# Changelog

This is the human change record for goav. Keep it useful for adopters: name
what changed, why it matters, and any migration or verification notes a user
would need.

The project is pre-v1. Until the first tagged release, use commit history and
pull-request descriptions as the detailed change record. Release entries should
name user-visible API changes, behavior changes, new adapters, performance
methodology changes, and migration notes.

## Unreleased

- Recipes can be written as values. An input's `Video()`, `Audio()`, or
  `Stream()` is a `goav.Stream`; operations (`Decode`, `Encode`, `Resize`,
  `Resample`, `Do`, `Playout`, `Sync`, `Tap`, `Buffer`, `Auto`, `Require`,
  `Prefer`, `Shape`, `As`) return new Streams and leave theirs unchanged, so a
  Stream used twice fans out and the work before the fork runs once.
  `stream.To(destinations...)` makes an `Output`, `input.To(...)` copies every
  stream of an input, and the runtime runs Outputs together:
  `rt.Run(ctx, hd.To(hdOut), sd.To(sdOut))`, with `rt.Build`, `rt.Explain`, and
  `rt.Describe` beside it. Streams sent to one Destination value share its
  container. `goav.Mix`, `goav.Composite` (Layers placed with `goav.At`),
  `goav.Select`, and `goav.Join` take Streams and return one. Breaking: the
  builder joins are `goav.LegacyMix`, `LegacyComposite`, `LegacySelect`, and
  `LegacyJoin`, and `InputSpec.Stream(av.Stream)` (the attach anchor) is
  `InputSpec.Track`; the builder grammar goes away once everything moves to
  values. A join's output still runs alone in one run (`stream_beside_join`).

- `control.SetBitrate(stream, bps)` and `control.Keyframe(stream)` reach the
  encoder that emits `stream`, so one ladder rung or simulcast layer can be
  retargeted by its branch name: `SetBitrate("hd", 1_200_000)` changes only the
  hd encoder. Stream-addressed encoder controls entered at the graph's select
  nodes, which drop ids other than their source's, so a rendition id never
  arrived and a source id retargeted every rendition at once. A source id still
  reaches every encoder of that source. `codec.EncoderStage.OutputStreamID`
  reports the id a stage stamps.

- Packet stages run after the encoder and after `.Copy()`. `.Do(...)` after
  `.Encode(...)` was refused for every stage, so bitstream filters, SEI
  injection, and packet encryption had no place in a chain; a stage that
  consumes packets (`component.PacketFunc`) now runs on the encoded packets,
  in the declared order, and the encoded stream keeps its source stream id. A
  lone copied stream with steps (`.Copy().Playout(...)`, `.Copy().Do(packetStage)`)
  lowers too: it was refused with `recipe_graph_unsupported` although the same
  chain built beside a second stream. Frame stages after `.Encode` or `.Copy`
  are still refused. `.Encode(...).Playout(...)` and `.Encode(...).Sync(...)`
  now gate the encoded packets as declared; a lone chain moved the gate ahead of
  the encoder, pacing frames instead.

- Joins are described and built from the same schedule as everything else:
  the join tree (arm inputs, their event-keeping decodes, conversion stages,
  tap-arm restamps, nested joins, the convergence stage) and its downstream
  (planned branches, or the downstream conversions, encoder, and destinations)
  are nodes of one `composeGraph`. The join planner's separate describe walk,
  build walk, and the edge regrouping that kept them in step are gone; the
  `TestCompilerOracle` goldens for every join kind are unchanged.

- Every recipe that is not a join lowers through one `composeLowerer`: a
  single stream, a selected or whole-job packet copy, several stream chains, or
  branches are all sources feeding one `composeGraph`, described and built from
  the same routes. The build no longer re-reads node names and outputs from the
  Explain work plan or cross-checks the two (about 1,400 lines), whole-job copy
  joined the graph, and one rule decides buffered execution. Net -1,489 lines;
  the `TestCompilerOracle` goldens are unchanged.

- Branch compositions are described and built from one schedule
  (`composeGraph`): the select, decode, and shared-step nodes per selector
  group, each branch's private steps and encoder, then the outputs. `Describe`
  emits it and the build adds the same nodes in the same order, connecting each
  node's inputs as it is added, so the built graph is the described one by
  construction rather than by three walks that had to agree: the old describe
  walk, the build walk, and a third set of node names threaded in from the work
  plan. Single streams, selected packet copies, multi-stream jobs, branches, and
  join branches all lower through it; the `TestCompilerOracle` goldens are
  unchanged. Net -840 lines.

- `Describe` names a playout or sync gate as the built graph does,
  `playout-<branch>-<policy>` and `sync-<branch>-<policy>`; it reported
  `playout-<branch>`, so a described graph did not match the task it built.
  `TestCompilerOracle` caught it: it pins `Describe`, `Explain`, and each
  refusal's code, family, operation, and node for a 35-recipe corpus that
  crosses every lowering path (single stream, packet copy, multi-stream,
  branches, stream rules, every join kind), as goldens under
  `testdata/golden/oracle`, and checks the built graph equals `Describe` for
  every recipe that builds. It is the safety net for replacing the recipe
  lowerers with one compiler.

- The WebM format adapter is the Matroska one driving WebM's profile-checked
  muxer and demuxer, instead of a 300-line copy of it, so the two cannot drift
  (the Opus `CodecDelay` fix had to land twice). `matroska.NewFormatMuxer` and
  `matroska.NewFormatDemuxer` take the packet muxer or demuxer to drive
  (`matroska.TrackMuxer`, `matroska.TrackDemuxer`). Breaking: a zero
  `webm.FormatMuxer` or `webm.FormatDemuxer` is no longer usable; call
  `webm.NewFormatMuxer()` or `webm.NewFormatDemuxer()`.

- A job no longer drops a chain without a word. A plain chain routed before or
  after a `.Branches(...)` chain on the same job was missing from the graph
  while `Describe` and `Run` succeeded; it now lowers as a stream with one
  branch, and shares the mux of any destination handle a branch also writes,
  so `.Video().Copy().Branches(...).Audio()...To(a)` puts audio into `a`. Three
  silent drops now fail the build: a chain declared on a `Mix`, `Composite`, or
  `Select` job (`stream_beside_join`), `.Region(x, y)` on a chain that ends in
  `To` or `Branches` or on a top-level `Composite` or `Select`
  (`stream_region_outside_composite`), and `.Region` on a `Mix` or `Select`
  arm (`mix_arm`, `select_arm`). Error fixes that named
  removed APIs (`Inspectable.Taps()`, `Mutable.Attach`, `goav.FPS`) now name
  `task.Taps()`, `task.Attach`, and `codec.FPS`, and
  `TestStringLiteralAPIReferencesResolve` fails the build when a string names
  an API that does not exist.

- Opus audio starts on time. A packet's PTS is now the time of its first
  decoded sample, so an Opus stream's first packet presents one pre-skip
  (6.5 ms at 48 kHz) before zero, as in FFmpeg. The decoder drops the
  pre-skip, so decoded audio no longer starts with 6.5 ms of encoder
  lookahead and no longer trails video by that much. The encoder declares
  its lookahead as pre-skip, so goav-written Opus no longer plays 6.5 ms
  late in every player, and the flush drains the lookahead, so the last
  6.5 ms of input is no longer dropped. Matroska and WebM subtract
  `CodecDelay` on read and add it back on write; MP4 Opus `ExtraData` is the
  RFC 7845 OpusHead instead of raw `dOps`, so MP4 Opus remuxes into
  Matroska. Encoders can report parameters known only once open through the
  new optional `codec.ParameterReporter`. Breaking: code that read Opus
  packet PTS or MP4 Opus `ExtraData` directly sees the new values.

- `Explain` reports a `buffered_execution` decision when the build runs the
  graph buffered although the runtime's own policy is direct: live inputs, a
  join, a declared branch buffer, or an encode ladder. `Describe` shows the
  same graph shape either way, so the report now says when branches and join
  arms run in parallel.

- Resize no longer aliases. The bundled resize filter point-sampled
  (nearest neighbor), so every downscaled rendition of a ladder showed
  jagged edges, moiré, and shimmer in motion. Each plane is now resampled
  separably: an axis that shrinks averages area, so each output sample is
  the coverage-weighted mean of the source it covers, and an axis that grows
  interpolates linearly. Coefficients are fixed point, built once per
  geometry, so the steady state stays allocation-free. A same-size plane is
  still copied. The kernel costs about 6x point sampling (62 µs for a
  320x180 I420 half-scale on an M4 Max), which is small next to encoding.

- The default event buffer is 256 events, up from 16, for the task event
  stream and for each `Watch` subscriber. Events never block the data
  plane, so a full buffer sheds. At 16, a single teardown burst (every
  branch detaching and each destination committing) overflowed it, and
  callers lost lifecycle events such as the final end of stream. An event is
  a small struct, so the deeper default costs about 40 KB per buffer.

- Encode ladders run their rungs in parallel by default. A fanout used to
  run every branch inside its upstream's call, so a three-rung VP9 ladder
  encoded one rung after another. A fanout where two or more branches
  encode, with only built-in operations and goav-owned outputs (Write/File),
  now runs each branch on its own worker when one source feeds those
  branches and every output is fed by a single branch. A 720p three-rung
  ladder from a WebM file dropped from ~370 ms to ~130 ms on an M4 Max, and
  each rung carries exactly the frames it carries when built alone. A Sink or `.Do` stage keeps the fanout serial,
  because the caller would see the concurrency. A shared output also keeps
  it serial, because parallel writers would interleave its packets
  nondeterministically.
- Buffered branches work on file inputs. A buffered graph refused to build
  when geometry or audio format was unknown before Build ("cannot derive a
  copy budget: missing width"), which is every decoded file stream without
  a Resize. Copy backing now grows to fit, so unknown facts fall back to
  ceilings that cost nothing until used: 8K UHD 4:4:4 for video, and
  192 kHz, 8 channels of f32 for audio.

- MP4 edit lists are honored. Encoders write an `elst` that starts
  presentation past B-frame composition delay and AAC priming, and the
  demuxer ignored it. In the ffmpeg-written fixture, audio content trailed
  the video by the 23 ms of priming; such offsets reach tens of milliseconds
  on typical B-frame files. Leading empty edits now delay a track and the
  first media edit starts its presentation. All tracks then lift together so
  nothing presents before zero, because muxers refuse negative presentation
  times. Malformed lists are ignored.

- Offline joins backpressure each arm. Mix and Composite buffered arm
  frames without bound while waiting for a slower sibling, so an offline
  composite of a fast-decoding small file and a slow large one pulled the
  whole fast stream into memory. An offline arm may now lead the join by 16
  buffered frames before its upstream waits.
  The wait runs on the arm's own goroutine through a new optional
  `pipeline.Admitter` stage capability, which both runners call before
  delivery takes the target's lock, so the sibling the join waits on keeps
  flowing. Output is unchanged. Cancelling or closing the task frees a
  waiting arm. Live joins keep buffering: a sparse arm (a slide feed beside
  a camera) needs the fast arm to lead by its whole period, and holding a
  live source back would drop its frames. A tree with a tap arm, which
  shares an upstream with another arm, also keeps buffering, because holding
  it back could deadlock.

- Buffered graphs no longer reserve worst-case copy budgets per queue slot.
  Every slot of every node used to preallocate the graph-wide packet and frame
  budgets. A video packet budget starts at 4 MB, so a slot carrying 10 KB
  encoded packets reserved megabytes, and a 720p three-rung ladder on 32-deep
  (realtime-default) queues peaked at 1.9 GB of heap. Budgets up to 64 KiB
  (RTP, audio) are still reserved up front for an allocation-free cold path.
  Larger ones grow to fit the payloads each slot actually carries, then copy
  without allocating. The same ladder now peaks at ~450 MB, with unchanged
  output and wall time.

- Branch buffers declared in a recipe are honored. `Branch(name).Buffer(...)`
  inside `Branches(...)` used to reach only the recipe report: every built
  node ran on the runtime default, so a preview branch's `flow.DropNewest`
  stalled the record branch beside it instead of shedding. The branch's own
  nodes now run behind its declared queue, with copy bounds sized from its
  shapes as in `Attach`. On an offline job a declared buffer promotes the
  graph to buffered, so the branch runs on its own worker. Giving each rung of
  an encode ladder `.Buffer(flow.Blocking(4))` encodes the rungs in parallel:
  a three-rung 720p VP9 ladder ran 2.7× faster on an M4 Max.

- `bundle` surface, 17 exports to 10 (**breaking**). The partial-runtime
  constructors (`NewFormats`/`NewCodecs`/`NewFilters` and their `MustNew*`
  twins), the three `*Options` slices, and `BuildLive` are replaced by three
  composable options: `bundle.Formats()`, `bundle.Codecs()`, and
  `bundle.Filters()`. A partial runtime is now `goav.New(bundle.Filters(),
  myCodec)`.
  - Migration: `bundle.MustNewFilters(x)` becomes
    `goav.New(bundle.Filters(), x)`, or `bundle.MustNew(x)`, since
    registration is last-wins. `bundle.BuildLive(ctx, job)` becomes
    `job.UseRuntime(bundle.MustNew()).BuildLive(ctx)`.

- Builder coherence: the stream chain type is exported as `goav.Chain`,
  so helpers can package reusable steps
  (`func normalize(c *goav.Chain) *goav.Chain`). Before, it was the one
  grammar builder users could not name. Reusing a chain value after it ended
  at `.To(...)`, a common attempt to fork one stream, used to fail with
  "recipe intent did not match a supported graph plan". It is now refused as
  `stream_step_after_destination`, with the two real forks as fixes:
  `.Branches(...)`, or a new `.Audio()`/`.Video()` stream.

- Destination identity (**breaking**). A destination handle is now the
  output's identity: pass the same value to several chains or branches and
  they feed one mux or sink. The one-file recipe is simply
  `out := goav.Write("out.webm", w)` then `.Video()…To(out).Audio()…To(out)`;
  before, it was refused with "attached more than once" unless wrapped in
  `goav.Mux`. `Mux` is removed. Separately constructed handles that share a
  name are still refused, so sharing stays explicit, and `With` copies keep
  their handle's identity. Runtime `Attach` groups share by handle the same
  way. Refusal fixes now say to reuse the destination value.
  - Migration: replace `goav.Mux(name, dst)` with `dst`, passed to every
    chain or branch that shares it.

- API purge (**breaking**): removed exports that could not be used or only
  restated something else.
  - `inspect.Render`, `Snapshot`, `Stats`, `Subscribe`: one-line wrappers.
    Use the task's own methods, and `graphrender` for flowcharts.
  - `BuildError.EffectivePhase`: it returned `Phase`.
  - `OnCodecChange`: its parameter type could not be constructed outside the
    package. The never-settable codec-change policy plumbing behind it, and
    its two unreachable refusal codes, are gone too.
  - Top-level `goav.Resize`/`goav.Resample`: they returned a type no public
    API accepted. The `.Resize`/`.Resample` chain methods are unchanged.
  - Resize modes are now first-class options:
    `.Resize(1280, 720, filter.Fit)` keeps the aspect ratio inside the box,
    and `filter.Fill` crops to cover it.

- MP4 and H.264 correctness:
  - The MP4 demuxer converts H.264 samples from length-prefixed (AVCC) to
    Annex B on read, which is the convention inside goav and what Matroska
    already does. MP4 `Copy()` to `.h264` used to write a file that started
    with a raw length prefix and had no parameter sets; the Annex B muxer now
    writes the avcC SPS/PPS ahead of keyframes that lack them, and it converts
    any length-prefixed payload it is handed.
  - MP4 samples are delivered in decode-time order across tracks. Before,
    they came in file-offset order, so non-interleaved files delivered all
    video and then all audio.
  - MP4 inputs implement `format.Seeker`, so `control.Seek`/`Segment` work on
    the most common container. The next read resumes at the latest video
    keyframe that presents at or before the target.
  - The `goav_goh264` build had not compiled: its tests used a goh264 encoder
    API that no longer exists. The tests now take their input from the MP4
    fixture, and CI builds and tests the tag, so the H.264 decoder cannot rot
    unseen again. With the tag, H.264 MP4 decode transcodes end to end.

- README rewritten around a runnable quick start: the CLI generates the
  input clip and the Go snippet transcodes it with default options, executed
  by `TestReadmeHeadlineRecipeRunsWithDefaults`. It also shows the real
  `Describe` output and `BuildError` text, a capability matrix, recipes,
  live/performance highlights with measured SIMD numbers, and the module map.

- File jobs run as fast as the machine allows (**behavior change**). Realtime
  now follows the inputs. A job is realtime (paced, buffered between stages,
  low-latency encoder presets) when one of its inputs is live: RTP, WebRTC,
  or playout providers, and push sources whose shape declares
  `shape.Realtime(true)`. File-to-file work runs offline. Before, every
  runtime defaulted to realtime: a two-hour remux took two hours, and
  decode/resize/encode of a file failed outright with `missing width`,
  because realtime graphs pre-size copy buffers from plan-time geometry that
  file probes don't carry. `runconfig.WithRealtime(true)` still paces file
  inputs on purpose, for example to simulate a live feed.
  `TestReadmeHeadlineRecipeRunsWithDefaults` executes the README's headline
  recipe with no options.
  - Migration: a job that relied on file pacing, or on Pause/Rate/Seek over
    a file, builds its runtime with `runconfig.WithRealtime(true)`. A custom
    `goav.Source` that is live declares `shape.Realtime(true)`.
  - Fixed: a `Select` nested in a `Composite` under an offline runtime failed
    with `buffered edges unsupported by direct graph`. The root's
    realtime-only buffer pin shadowed the nested Select's unconditional one.
  - The runtime-branch refusal for a buffered branch on a direct task now
    names the real fixes instead of claiming branches only work on direct
    graphs.
  - Decoding a file whose codec is descriptor-only in the build (H.264 without
    `-tags goav_goh264`) is refused as a typed `decode_adapter_unavailable`
    error that names the tag, instead of a bare `codec: unavailable`.

- Audio transcodes work end to end:
  - The Opus encoder re-frames its input. Any decoder or resampler frame
    size (AAC's 1024 samples, a resampled 1115) becomes fixed 20 ms packets
    on an exact sample-count timeline. Opus-sized input still encodes in
    place. End of stream flushes the tail. Audio encode results now carry
    eight packet slots.
  - `Resample` streams across frames: it carries the interpolation phase and
    the last input sample. Frame boundaries no longer click, the output count
    is exact, and output timestamps are contiguous. A stateless per-frame
    ceil had drifted by about half a sample per frame, roughly 1.4 s per hour.
  - Encoders declare accepted sample rates (`codec.Capabilities.SampleRates`;
    Opus: 8/12/16/24/48 kHz). An unsupported rate is refused as a
    `*BuildError` naming the exact `.Resample(rate, channels)` to insert,
    instead of the adapter's bare `codec: unsupported format`. Any other
    encoder-open failure is also wrapped as a typed error with the stream's
    format.
    Decoder-open failures are typed the same way; a backend unavailable on
    the platform (goaac builds for darwin/arm64 and linux/arm64) reports
    `decode_adapter_unavailable`, and `errors.Is` still reaches the cause.

- Media correctness and hardening:
  - The MP4 demuxer now bounds sample tables with a file-wide budget tied to
    the file size. A few hundred crafted bytes could previously force
    gigabyte allocations (field-less `trun` runs, uniform `stsz`, unbounded
    `stz2`). The sample bounds check is overflow-safe, and the MP4 fuzz target
    runs in CI.
  - `Composite` paints uncovered canvas black (Y=16, U=V=128) instead of
    green.
  - The IVF demuxer keeps a refused frame's header, so retrying after
    `ErrPayloadTooSmall` reads the same frame instead of desynchronizing.

- Refusal and diagnostics fixes: "add an encoder" suggestions now follow the
  stream's media. Video refusals name `codec.VP8`/`VP9`/`AV1` and never an
  audio codec; audio refusals name `codec.Opus`. Twelve copy-pasted hint
  strings collapse into one helper, and the hints now include the bundled AV1
  encoder, which they previously left out. Mermaid output from `graphrender` and
  `inspect.Render` now uses Mermaid's own label syntax (`<br>` line breaks,
  `#quot;`/`#35;` entities) instead of Go escapes, which rendered as literal
  `\n` text.

- Go SIMD by default in the gate: CI, the bench scripts, and release
  binaries now build with `GOEXPERIMENT=simd`. That lights up goav's own mix
  kernel and the SIMD paths in the `goav1`, `gopus`, and `govpx` backends.
  The hand-written amd64/arm64 assembly mix kernels are gone; a single
  portable `simd` kernel replaces them (NEON, AVX/AVX2/AVX-512, wasm SIMD128)
  and now covers any arm count, where the assembly only handled two arms. It
  stays bit-exact with the scalar loop, which remains the default and
  `-tags purego` path for importers. A new test keeps the repository free of
  assembly, and a scalar CI lane covers builds without the experiment.

- Go 1.27 floor and dependency refresh: every module now declares
  `go 1.27.0` (pinning `toolchain go1.27.2` for contributors), because the
  first-party `goav1` and `gopus` backends now require Go 1.27. The
  `thesyncim/*` codec backends move to their latest revisions (`gopus`
  v0.2.2), and the transport and example modules pick up current `pion/*`
  releases, including a fix for the retracted `pion/dtls/v3` v3.1.3. CI runs on
  Go 1.27 with staticcheck 2026.2 and current GitHub Actions. The modules are
  also modernized with Go 1.27 `go fix`, and the PGO profile is regenerated
  for the new compiler.

- Plan-time latency budgets and docs consolidation: `Explain` now reports one
  `path_latency_budget` decision per synced/playout path — the declared sync
  tolerance, playout offset, and the runtime's declared buffered `MaxLatency`
  summed into a per-path figure, with queue capacity reported as a message
  count, not a duration (playout offsets stay independent of sync tolerance;
  no defaulting). The time-domain program is complete: its plan moved to
  `docs/history/TIME_DOMAIN_PLAN.md`, `docs/NORTH_STAR.md` folded into
  `docs/ROADMAP.md`, and `docs/FLOW_CONTROL.md` owns the
  sync/playout/pause/flush/QoS/latency contract prose.

- QoS feedback: overdue playout admits, sync-gate sheds, and buffered
  `MaxLatency` sheds now publish `av.EventQoS` through `Watch` (stream, node,
  lateness, delivered-or-dropped via `av.QoSMetadata`/`av.EventQoSReport`),
  one report per producer per second. The new `runconfig.WithQoSPolicy(policy)`
  maps each report to `control.Control` values the task issues to itself;
  refused controls surface as `av.EventQoS` with `Cause`.

- Accurate seek: `task.Control` with `control.Seek`/`control.Segment` now
  flushes the stale media queued downstream of the repositioned source on
  buffered tasks, so a seek repositions promptly instead of draining the old
  position's backlog to sinks. Flushed packets/frames are counted under the
  new `pipeline.DropFlush` stats reason; queued events always survive. Direct
  (unbuffered) tasks seek exactly as before.

- Task pause and readiness: `LiveTask` gained `Pause(ctx)`/`Resume(ctx)` —
  Pause freezes the shared task timeline (paced sources and playout gates
  stall coherently), Resume continues from the frozen reading. Pause pauses
  time, not the data plane: free-running tasks are refused with
  `format.ErrRateUnsupported` like `control.Rate`. `snapshot.Task` gained
  `Paused`, and the new `av.EventTaskReady` arrives through `Watch` when
  every sink received its first media message (see `docs/FLOW_CONTROL.md`).

- Synchronized sink playout: `flow.Playout(name)` (options as self-methods
  `WithOffset`/`WithDropLate`) is a deliver-when-due policy for the sink
  boundary — `.Playout(policy)` on a stream chain or branch holds each
  packet/frame until its media time (plus the offset) is due on the task
  timeline, the sink-sync half of GStreamer. Branches reusing one policy value
  share the first-message anchor so audio/video delivery interleaves by PTS;
  `EventDiscontinuity` resets the anchor; events pass ungated. Hold-late is
  the default, `WithDropLate(tolerance)` sheds overdue media and reports the
  drops through normal branch stats (`pipeline.DropSync`). Playout gates count
  as paced timeline consumers, so an untargeted `control.Rate` retimes playout
  gaps (2x halves the wall gap) and pausing the internal timeline freezes
  delivery coherently.

- Task-wide timeline: every task owns one shared clock service (an internal
  timeline consumed as `av.Clock`) that the realtime demux pacer and
  clock-aware sources sleep on; `control.Rate` re-anchors it so every paced
  source changes pace together, mid-sleep included, and `playoutav` schedules
  on the injected clock instead of wall timers (sources implementing
  `UseClock(av.Clock)` receive the task clock after open). **Breaking**: rate
  controls are task-wide and no longer target sources — `control.Rate` with
  `At`/`AtTap` is refused, `av.EventRate` is no longer delivered to
  `pipeline.ControllableSource.Control` (sources keep seek/segment), and
  `goav ctl control rate` drops its `source=`/`node=` arguments. Tasks with
  nothing paced to scale still refuse rate with `format.ErrRateUnsupported`.

- Exported the grammar's sealed builder and option types — `BranchBuilder`
  (Branch), `FlowBuilder` (Flow), `MixStream` (Mix), `CompositeStream`
  (Composite), `SelectStream` (Select), `JoinStream` (Join), the `JoinArm`
  join-arm interface, `StreamOption`, `InputOption`, `DestinationOption`, and
  `MediaOption` — so godoc/pkg.go.dev can link them and callers can name
  variables and helpers. All stay sealed (unexported fields or methods): the
  constructors remain the only construction surface.
- **Breaking**: renamed `codec.CodecSpec` to `codec.Spec`; the constructors
  (`codec.VP9`, `codec.Opus`, `codec.Copy`, `codec.Auto`, ...) are unchanged,
  so only code naming the type spells the new name.
- **Breaking**: renamed the `runtime` config package to `runconfig` and the
  `ctl` socket-host package to `ctlserver`; a contract test now rejects any
  package named `runtime` or `ctl` in the module.
- Added the `playoutav` nested module: scheduled packet/frame/event playout
  through the `provider.Source` seam with zero core changes.
- Added NEON (arm64) and SSE2 (amd64) audio-mix kernels behind CPU dispatch
  with byte-exact scalar equivalence tests; composite/resample/resize keep
  measured scalar fast paths until an asm prototype beats them >=1.5x.
- Added opt-in per-runtime media pools (`runconfig.WithMediaPools`),
  refcounted zero-copy buffered fanout for borrowed packets, a committed PGO
  profile with a CI freshness check, and committed alloc/byte benchmark
  baselines with a CI ratchet (`scripts/bench/baseline.sh`).
- Closed five composability asymmetries: join-arm taps, branch auto-insert
  parity with direct chains, nested Select routing via `AtTap`, the
  attach-after-sibling-detach refusal now names its fix, and join outputs fan
  out through `.Branches(...)`; a generated-recipe corpus pins the laws.
- Opened the solver-delta boundary (`shape.AllowCustom` +
  `runconfig.WithShapeDelta`) and added stateful `OnStream` matchers
  (`source.MatchFirst/MatchAfter/MatchWithin`).
- Documented the `goav run` CLI schema (`docs/CLI.md`), indexed the living
  docs (`docs/README.md`), and moved finished campaign artifacts to
  `docs/history/`.
- Fixed a linux/amd64 SIGSEGV in VP9 encode: govpx's SSE2 kernels used
  aligned stores against Go's 8-byte-aligned stack; goav now pins the fixed
  govpx, and all codec backends are bumped to their latest commits.
- Replaced the removed source-scanning pin tests' stale doc citations with
  living evidence, and added doc-honesty pins: living docs may only cite
  tests/scripts that exist, `docs/ERROR_CATALOG.md` stays in lockstep with
  `errcode/errcode.go`, and every README Go block must compile.
- CI now runs the race detector across every package, resolves the latest Go
  patch release (fixes a stale-toolchain govulncheck failure), collects
  coverage without running the suite twice, and honors the `skip-changelog`
  label regardless of label order.
- The dynamic audio room example sizes its event buffers above its teardown
  burst. Watch sheds when a buffer is full, and the demo asserts every
  lifecycle event, so it failed intermittently under load.

## Pre-v1 consolidation - 2026-06-26

These notes were previously mislabeled as a `v0.1.0` release. No tag has been
cut; goav has never been released. They remain the consolidated record of the
pre-v1 API reduction.

- Shrank the README under the pre-v1 120-line target and moved long
  live/runtime walkthrough expectations into focused docs.
- Clarified that runtime mutation/control/expert graph features are governed
  pre-v1 behavior, not automatic v1 promises unless the release decision
  explicitly retains them.
- Removed exported `goav.ContextCloser`; runtime tasks still expose
  `CloseContext(ctx)` structurally, and callers that need context-aware
  shutdown can define that small interface locally.
- Removed exported `goav.Observable`; event access remains on the explicit
  `BuildLive`/`LiveTask` path, and callers that only need `Events`/`Watch` can
  use a local structural interface.
- Removed exported `goav.Controllable`; live control remains on the explicit
  `BuildLive`/`LiveTask` path, and control-only helpers can accept a local
  structural interface.
- Removed exported `goav.Explainer`; built-task explanation remains on
  the explicit `BuildLive`/`LiveTask` path, and `Job.Explain(ctx)` remains the
  pre-build path.
- Removed exported `goav.Inspectable`; runtime inspection remains on
  the explicit `BuildLive`/`LiveTask` path, and inspection-only helpers can
  accept a local structural interface.
- Removed exported `goav.Mutable`; runtime branch mutation remains on
  the explicit `BuildLive`/`LiveTask` path, and mutation-only helpers can accept
  a local structural interface.
- Removed exported `goav.MediaOption`; `Name`, `MIME`, and `Metadata` still work
  as shared input/destination options.
- Removed exported `goav.InputOption` and `goav.DestinationOption`; option
  constructors such as `Codec`, `Format`, `Name`, `MIME`, and `Metadata`
  still pass directly to recipe constructors and `.With(...)`.
- Removed exported `goav.InputStream`; `InputSpec.Stream(stream)` still returns
  the attach anchor used with `Branch(...).From(...)`.
- Removed unused `goav.RecipePatch`; `BuildError` fix messages now read
  through `FixLines()` without an edit-patch DTO.
- Stream chains now require an explicit `.Decode()` before frame-domain
  consumers (`.Do`, `.Resize`, `.Resample`, real `.Encode`, frame taps, and
  decoded-frame sinks). Use `.Copy()` when a chain should stay packet-domain.
- Removed legacy `BuildError.Details` and `BuildError.Suggestions` fields;
  callers should use `Detail(key)`, `DetailLines()`, and `FixLines()`.
- Removed exported `goav.TransformSpec`; callers still use `Resize` and
  `Resample` constructors, but the transform value type is no longer part of
  the root contract.
- Removed same-handle destination grouping: repeated destination names now
  require explicit `Mux(name, destination)` groups, even when the same
  destination value is reused.
- Removed graph node-name prefix fallback from `Inspectable.Taps()`; taps now
  come from typed planner/runtime anchors instead of `decode-*` or `select-*`
  naming conventions.
- Typed resize/resample data in the immutable recipe IR instead of carrying
  root transform wrappers through the planner boundary.
- Clarified front-door docs so `.Copy()` is the packet-preserving recipe
  spelling; `codec.Copy()` remains an internal/lowering `Spec` value.
- Renamed writer-backed destinations to `Write(name, writer)` so output recipes
  do not imply that the constructor opens filesystem paths; `URI(uri)` keeps
  the adapter-opened output path and `FileInput` remains the reader-side file
  input spelling.
- Changed recipe and bundled `Build` helpers to return the narrow `Task`
  lifecycle (`Run`, `Close`) and added `BuildLive` for callers that need
  inspection, watches, controls, or runtime mutation.
- Captured OnStream branch media, operation, destination, and buffer facts in
  the immutable recipe IR, and lowered stream-rule attach through captured
  runtime branch inputs before graph mutation.
- Captured runtime attach/rebranch branch recipes as `internal/recipeir`
  runtime branch data before patch planning, leaving destination handles at the
  mutation edge.
- Captured runtime attach destination name, kind, format, and share-key facts
  as `internal/recipeir` data while keeping concrete writer/sink handles at the
  mutation edge.
- Switched runtime attach patch planning to walk captured `recipeir.Operation`
  values directly instead of rehydrating root operation specs after branch
  capture.
- Carried the full captured recipe IR through compile state and made normal
  work-plan handoff inputs, streams, destinations, and solver-updated
  operations read from that recipe boundary.
- Removed the normal work-plan handoff's legacy intent fallback so rendering
  requires the captured recipe IR.
- Removed the branch-composition builder `composePlan` wrapper and moved branch
  destination-name validation errors onto stream intent facts instead of
  `streamBuild` builder records.
- Built branch-composition recipe snapshots directly from captured branch facts
  instead of routing through a temporary branch job wrapper.
- Routed join branch fanout planning through recipe IR stream facts instead of
  synthesized `jobStreamBuild`/`streamBuild` records.
- Captured join chain-arm inputs at the join-arm boundary so join tree
  planning and join input snapshots no longer read through chain builders.
- Captured join tree arm facts before planning so recursive join planning,
  leaf input collection, and tap-name discovery no longer call back into arm
  builders or chain operation helpers.
- Routed media input binding, copy planning, live stream selection, and
  multi-input stream resolution through captured recipe input facts instead of
  the legacy intent mirror.
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
- Removed the concrete join-plan attachment from compile state; graph-plan
  construction now passes the selected join lowerer into work-plan rendering.
- Routed normal work-plan rendering through an explicit handoff so operation,
  branch, destination, decision, and diagnostic rendering consumes captured
  planner data.
- Routed multi-stream job graph lowering through an explicit handoff before
  branch-compose graph construction.
- Routed single-stream packet-copy and decode graph lowering through explicit
  handoffs before graph construction.
- Routed explicit branch-composition graph lowering through an explicit handoff
  with cloned branch-compose plan data.
- Routed branch-composition planning through captured recipe IR while keeping
  concrete input and destination handles at the attachment edge.
- Routed runtime attach through an explicit input handoff that captures branch
  specs and destination facts before graph locking and patch planning.
- Captured runtime attach branch specs and validated destinations as one
  per-branch input record before patch planning.
- Captured each runtime attach branch's resolved source anchor and graph
  snapshot in the same planning input used by patch lowering.
- Routed runtime rebranch through an explicit input handoff that captures
  replacement specs, switch policy, and old-branch disposition before mutation.
- Routed runtime detach through an explicit input handoff that captures
  attachment target and disposition before closing or stopping runtime branches.
- Carried the runtime detach input through the locked stop path and
  child-attachment recursion instead of unwrapping attachment/disposition
  arguments.
- Routed dynamic stream-rule attach reactions through the same runtime attach
  input handoff before mutating the live graph.
- Routed dynamic stream-rule remove reactions through runtime detach inputs
  captured for each tracked attachment before mutation.
- Marked `BranchSpec` values with their constructor origin so zero values are
  refused before planned or runtime branch mutation treats them as real specs.
- Marked `Destination` handles with their constructor origin so zero values
  and zero-derived option copies cannot masquerade as real outputs.
- Marked `InputSpec` values with their constructor origin so zero values and
  zero-derived option or wrapper copies cannot masquerade as real inputs.
- Marked `Job` values with their constructor origin so zero values refuse with
  `goav.From(...)` guidance instead of looking like ordinary empty recipes.
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
- Added `bundle.Describe` and `inspect` convenience helpers (`Subscribe`,
  `Snapshot`, `Stats`, `Render`) so bundled structural planning and
  observation helpers have first-class names.
- Tightened pre-v1 runtime contracts around explicit runtimes, Mux-first
  destination grouping, join validation parity, and Family-first structured
  error matching. Detailed `errcode.Code` values remain diagnostic leaves
  within stable `errcode.Family` categories.
- Reduced and layered the pre-v1 public API: bundled adapters now live behind
  `goav/bundle`, live controls and watch filters moved into explicit vocabulary
  packages, `Task` is the minimal run/close lifecycle, richer runtime behavior
  is exposed through opt-in capability interfaces, and `Mux(name, destination)`
  declares shared mux/sink grouping without depending only on reused Go values.
- Added typed `BuildError` fields and fixes (`Detail`, `Fix`, and
  `Detail(key)`) while preserving existing rendered details and
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
- Added `AtMediaTime(...)` rebranch boundaries, dynamic `OnStream(...)`
  stream-rule removal, and destination commit/abort/error lifecycle events.
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
