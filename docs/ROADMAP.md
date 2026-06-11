# Roadmap — stability tiers and the road to v1

goav is pre-v1. This document states what is stable now, what is
experimental, what is deliberately deferred, what is planned, what is a
non-goal, and exactly what must hold before a v1 tag. Every claim cites the
test, benchmark, or document that backs it, or is marked **roadmap**.
`docs/NORTH_STAR.md` keeps the evidence-cited acceptance scoreboard;
`docs/PROGRESS.md` is the compact tracker; how goav relates to GStreamer is
`docs/GSTREAMER_ALTERNATIVE.md`.

## The settled model

The declarative grammar is the only normal composer:
`input -> stream -> operations -> tap -> branch -> destination` lowers into
`WorkPlan -> pipeline.Graph -> Task`.
Make runtime attachment a patch of the same plan model:
`Task.Attach` compiles the same branch grammar into `WorkPatch`, validates
before graph mutation, and rolls back fully on failure
(`TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`).
Collapse `Target` into `Destination` is done: `File`, `URI`, `Writer`,
`Sink`, and `Custom` return stable goav-owned destination handles, and
reusing one handle groups branches into one mux/sink group
(`TestFromMultiInputPlanDedupesSharedDestination`).

## v0 STABLE

Stable means: pinned against silent change, documented, and test-enforced —
not "frozen forever". The governed surface is 314 approved identifiers
(`api_surface_pin_test.go` + `testdata/api_surface.txt`: 124 root, 142
`errcode`, 28 `plan`, 13 `lifecycle`, 4 `snapshot`, 3 `graphrender`), every
exported symbol documented (`doc_pin_test.go`), tiered in
`docs/API_SURFACE.md`:

- **Tier A — the grammar.** `From`/stream selection/operations
  (`Decode`/`Copy`/`Resize`/`Resample`/`Do`/`Encode`)/`Shape`/`Auto`/
  `Require`/`Prefer`/`Tap`/`Branches`/`To`/`OnStream`; `Mix`/`Composite`/
  `Select`; `Flow`; `Task` verbs (`Run`/`Events`/`Watch`/`Snapshot`/`Stats`/
  `Attach`/`Detach`/`Rebranch`/`Control`); `Default`/`New`/`UseRuntime`;
  structured `BuildError` + the `errcode` catalog; the `plan`, `snapshot`,
  `lifecycle`, `shape`, `flow`, and `av` vocabulary packages.
- **Tier B — extension seams.** `provider.Source` and `Source(fn)` push
  sources; `provider.Destination`/`Writer`/`Sink` destinations;
  `EventFunc`/`FrameFunc`/`PacketFunc`/`SinkFunc` hooks; codec/format/filter
  factory interfaces with per-runtime `With*` registration; `goavtest`.
  Every seam has an external toy implementation run end to end
  (`adapterproof/adapter_compat_test.go`, guide `docs/ADAPTER_AUTHORING.md`).
- **Tier C — expert.** `expert.Graph(runtime)` handles, `pipeline` graph
  machinery, prebuilt codec/format/filter stages — off the grammar, still
  governed.

The error contract (`errors_pin_test.go`, `error_acceptance_test.go`,
`docs/ERRORS.md`) and the runtime invariants — close idempotency, close
during run, race-safe snapshots under attach/detach, commit-failure
propagation (`task_invariants_test.go`), watcher isolation (`watch_test.go`),
drop observability (`TestFrontDoorDropReasonsReadableWithoutPipeline`) — are
stable contracts.

## EXPERIMENTAL

Exists and is tested, but numbers or semantics are expected to move
(`docs/PERFORMANCE.md` "Experimental" is the performance side of this list):

- **Mix join step cost** — ceiling-pinned at 16 allocs/step
  (`TestAudioMixStepAllocCeiling`); slot reuse should lower it. Composite
  shares the join machinery.
- **Buffered copy-mode fanout cost** — per-target copy of borrowed payloads
  (`pipeline` `BenchmarkBufferedFanout/copy`); refcounted zero-copy fanout
  would remove it.
- **Attach-under-load cost** — `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation dominated by planning; not a data-plane figure.
- **OnStream rule breadth** — identity matches only (`MatchMedia`/
  `MatchCodec`/`MatchStreamID`/`MatchStream(fn)`); conditions beyond stream
  identity and a per-rule removal detach policy are roadmap
  (`docs/NORTH_STAR.md` §11, scoreboard item 40).
- **Join nesting depth** — nested joins are proven at the tested depths
  (`join_nested_test.go`, `TestJoinDescribeEqualsBuildNestedMix`); deeper
  nesting compiles through the same recursion but has no dedicated proof or
  cost model.

### Known gaps (core review)

- **Live (blocking) join arms need an explicitly buffered runtime** — the
  direct runner starts sources sequentially and delivers inline, so a
  Mix/Composite arm that blocks in `Start` (any live source) starves later
  arms on the default runtime. Workaround, pinned by
  `TestMixSyncByPTSSeekArmMidRun`: `WithBufferPolicy` (non-lossy `DropBlock`;
  add `CopyPacketBytes`/`CopyFrameBytes` budgets when an arm decodes, since
  decoder output buffers are mutable and refuse to queue by reference).
  Select already pins a buffered graph for control delivery; doing the same
  for Mix/Composite needs an answer for decode-arm copy budgets first.
- **No shape solving downstream of a join** — the solver runs per ARM
  (`solveArmConversion`: arms converge on the first arm's format), but the
  joined stream's own `.Encode(...)` and `.Branches(...)` paths lower without
  it: a 44.1kHz mix into `codec.Opus()` (48kHz) plans no conversion and no
  refusal, and a join branch's `.Auto(...)` is silently inert. Joins whose
  arms already match the target format are unaffected. Fix direction: run the
  branch/encode operation list through the same chain solver the stream path
  uses, seeded with the joined stream's shape.

## Descriptor-only and deferred

- **H264/AV1 recipe encode** — descriptor-only: registry descriptors are
  discoverable while factory lookup returns `codec.ErrUnavailable`
  (`docs/ARCHITECTURE.md` "Codec backends"); decode/receive verticals are
  active.
- **A/V sink sync, pipeline-wide clock service, pull scheduling** — the
  theme-C endgame; pull scheduling is the keystone. The time-axis controls
  (`Seek`/`Rate`/`Segment`) and clock-paced realtime file playback already
  ship (`task_seek_test.go`, `task_time_control_test.go`); the rest is
  analysed in `docs/NORTH_STAR.md` ("Time/sync", attack-plan stage 7).
  Roadmap.
- **Internal-package layering** — measured on the cross-file reference graph
  and rejected: no boundary worth a package today (`docs/ARCHITECTURE.md`
  "Package layering"). Revisit only with a data-transfer seam.
- **`goav.Intent` / `goav.OperationSpec` leakage** — frozen by the surface
  pin so the list only shrinks; needs a design call (`docs/API_SURFACE.md`
  tier D).
- **Plain `task.Detach` drain/abort verbs** and **dedicated
  attach/detach/commit lifecycle events** — drain-commit is pinned where
  exposed (rebranch dispositions, stream removal); the standalone verbs and
  events are roadmap (`docs/NORTH_STAR.md` scoreboard items 25, 26, 30).
- **`streamIntent` normalization fold** — internal debt tracked in
  `docs/NORTH_STAR.md` "Execution order".

## Planned

- **Playout/SRT/NDI providers** through the `provider.Source` seam — by
  design zero core changes (seam proven by
  `adapterproof/adapter_compat_test.go`). Roadmap.
- **Tail-latency benchmarking** — p50/p95/p99 need a histogram harness;
  ns/op is an average (`docs/PERFORMANCE.md` "Not proven"). Roadmap.
- **PGO workflow** — profile capture over the canonical suite
  (`scripts/bench/run.sh` is the entry point) feeding default-on
  profile-guided builds. Roadmap.
- **Additional `SwitchAt` boundaries** beyond `NextFrame`/`NextKeyframe`
  (`rebranch_policy.go`). Roadmap.
- **Mux-group timebase validation** (`docs/NORTH_STAR.md` scoreboard item
  42). Roadmap.

## NON-GOALS

- **GStreamer plugin parity.** goav is not a general multimedia framework;
  matching element-for-element would reproduce the surface the grammar
  exists to avoid (`docs/GSTREAMER_ALTERNATIVE.md`).
- **Hardware codec backends in core.** Core stays pure Go — pinned by
  `TestNoCGOImports` (`hygiene_test.go`); acceleration belongs in external
  adapters behind the `codec` seams, where cgo is the adapter's choice.
- **cgo in core.** Same pin; single-binary `CGO_ENABLED=0` deployment is a
  headline property (`.github/workflows/ci.yml` builds with it).
- **Global registries.** Registries are per-runtime; two runtimes in one
  process must never see each other's adapters (`docs/ARCHITECTURE.md`:
  `goav.New` is the composition root).
- **JIT / runtime code generation.** The planner emits a static graph;
  per-message dispatch stays direct calls. The win would belong to codecs
  (external anyway) and the cost is un-debuggable, un-pinnable hot paths.

## V1 freeze criteria

The checklist that gates the tag; each item names its current evidence.

- [x] **Approved API surface** — `api_surface_pin_test.go` +
  `testdata/api_surface.txt` (both-direction pin), with dynamic package
  discovery asserting every module package is governed
  (`TestEveryPublicPackageIsGoverned`).
- [x] **Compile-tested examples** — 13 `Example*` functions run under
  `go test` (`example_test.go`); the `examples/webrtc-runtime-ladder` module
  builds and tests in CI.
- [x] **Structured errors enforced** — `errors_pin_test.go` (catalog-code
  pin) + 10 acceptance snippets (`error_acceptance_test.go`).
- [x] **Benchmarks present** — 16 measured workloads (`bench_test.go`) +
  pipeline/container suites; bench smoke runs in CI; methodology in
  `docs/PERFORMANCE.md`.
- [x] **Adapter guide** — `docs/ADAPTER_AUTHORING.md` with the executable
  five-seam proof (`adapterproof/adapter_compat_test.go`).
- [x] **Race tests pass** — CI runs `go test -race` on root, `pipeline`,
  `goavtest`, `format` (`.github/workflows/ci.yml`).
- [x] **Container fuzz/corpus** — `FuzzDemuxerMalformedInputs` with seed
  corpora (`container/matroska`, `container/webm` `fuzz_test.go` +
  `testdata/fuzz`) plus external field-corpus tests
  (`TestExternalMatroskaFieldCorpus`, `TestExternalWebMFieldCorpus`).
- [x] **No global state** — registries live on the runtime
  (`docs/ARCHITECTURE.md`); audited 2026-06: remaining package-level vars
  are error sentinels, immutable profile tables, and atomic ID counters.
  No pin test exists for this — the audit is repeated at review.
- [x] **No undocumented exported symbols** — `doc_pin_test.go` across every
  discovered public package (dynamic module walk; `adapters/*` and
  `container/*` sit behind the codec/format seams and are excluded by the
  decision recorded in `docs/API_SURFACE.md`).
- [ ] **Release decision** — confirm the `go 1.26.2` directive in `go.mod`
  is the intended minimum supported Go, write the tag's compatibility note,
  and cut v1. Not done; the only open item is a maintainer call, not code.
