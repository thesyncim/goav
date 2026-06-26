# Roadmap: stability tiers and the road to v1

goav is pre-v1. Use this roadmap to answer a human question: what can I rely
on today, what is still allowed to move, and what would be dishonest to imply
as done? It separates stable, experimental, deliberately deferred, planned,
and non-goal work, then names the checks that must hold before a v1 tag. Every
claim cites the test, benchmark, or document that backs it, or is marked
**roadmap**.

The nearby docs split the work by reader need: `docs/NORTH_STAR.md` keeps the
evidence-cited acceptance scoreboard; `docs/PROGRESS.md` is the compact
tracker; `docs/SIMPLIFICATION_TARGET.md` freezes the smaller pre-v1 target;
`docs/V1_CREDIBILITY_AUDIT.md` maps the v1-credibility pass to reviewer
evidence; `docs/REPOSITORY_TRUST.md` records the GitHub metadata and release
posture; how goav relates to GStreamer is `docs/GSTREAMER_ALTERNATIVE.md`.

## The settled model

The shortest version: recipes are the product surface; plans, graphs, and
runtimes are implementation layers. The declarative grammar is the only normal
composer:
`input -> stream -> operations -> tap -> branch -> destination` lowers into
`WorkPlan -> pipeline.Graph -> Task`.

Live mutation follows the same promise. Make runtime attachment a patch of the same plan model:
`Mutable.Attach` compiles the same branch grammar into `WorkPatch`, validates
before graph mutation, and rolls back fully on failure
(`TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`).

The destination model has also been simplified. Collapse `Target` into `Destination` is done: `Write`, `URI`, `Writer`,
`Sink`, and `Custom` return stable goav-owned destination handles.
`Mux(name, destination)` is the preferred way to declare one shared mux/sink
group when branches build matching destinations independently. Reusing one
ungrouped handle is rejected; grouping is explicit (`TestMuxPreferredOverHandleIdentity`,
`TestMuxSurvivesWithAndCopy`, `TestSameHandleGroupingRequiresMux`).

## Governed pre-v1 surface

Governed here means "changes are deliberate, tested, and recorded", not "this
whole surface is the v1 promise." The governed surface is 162 approved
identifiers (`api_surface_pin_test.go` + `testdata/api_surface.txt`: 40 root,
22 `control`, 8 `inspect`, 27 `errcode`, 28 `plan`, 24 `lifecycle`,
4 `snapshot`, 9 `graphrender`), every exported symbol is documented
(`doc_pin_test.go`), and the current inventory is tiered in
`docs/API_SURFACE.md`:

- **Tier A inventory: the grammar and task capabilities.** `From`/stream
  selection/operations
  (`Decode`/`Copy`/`Resize`/`Resample`/`Do`/`Encode`)/`Shape`/`Auto`/
  `Require`/`Prefer`/`Tap`/`Branches`/`To`/`OnStream`; `Mix`/`Composite`/
  `Select`; `Flow`; `Task` lifecycle (`Run`/`Close`) plus structural
  built-task `CloseContext(ctx)`; opt-in task capability
  interfaces for `Explain`, inspection (`Describe`/`Taps`/`Snapshot`/`Stats`),
  mutation (`Attach`/`Detach` with `DrainBranch`/`AbortBranch`, `Rebranch`),
  controls (`Control`), and observation (`Watch`; unfiltered `Watch()` observes
  every task event);
  `New`/`UseRuntime` and the `bundle` runtime helpers;
  structured `BuildError` with stable families, detailed codes, typed fields/fixes;
  the `plan`, `snapshot`, `lifecycle`, `shape`, `flow`, and `av` vocabulary
  packages. Runtime mutation/control/advanced observation and joined-stream
  breadth are governed pre-v1 behavior, not normal v1 promises unless the
  release decision explicitly retains them.
- **Tier B: extension points.** `provider.Source` and `Source(fn)` push
  sources; `provider.Destination`/`Writer`/`Sink` destinations;
  `EventFunc`/`FrameFunc`/`PacketFunc`/`SinkFunc` hooks; codec/format/filter
  factory interfaces with per-runtime `With*` registration; `goavtest`.
  Every extension point has an external toy implementation run end to end
  (`adapterproof/adapter_compat_test.go`, guide `docs/ADAPTER_AUTHORING.md`).
- **Tier C: expert.** `expert.Graph(runtime)` handles, `pipeline` graph
  machinery, prebuilt codec/format/filter stages. These are off the grammar,
  governed as escape hatches, and advanced/non-v1 unless explicitly retained.

The error contract (`errors_pin_test.go`, `error_acceptance_test.go`,
`docs/ERRORS.md`) and runtime invariants are pinned current contracts: close
idempotency, close during run, race-safe snapshots under attach/detach,
commit-failure propagation (`task_invariants_test.go`), watcher isolation
(`watch_test.go`), and drop observability
(`TestFrontDoorDropReasonsReadableWithoutPipeline`). They prove the current
surface is controlled; they do not by themselves promote every advanced
capability into v1.

## Experimental

These pieces exist and are tested, but their numbers or exact semantics may
still move. `docs/PERFORMANCE.md` "Experimental" is the performance side of
this list:

- **Buffered copy-mode fanout cost**: per-target copy of borrowed payloads
  (`pipeline` `BenchmarkBufferedFanout/copy`); refcounted zero-copy fanout
  would remove it.
- **Attach-under-load cost**: `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation dominated by planning; not a data-plane figure.
- **OnStream rule breadth**: identity matches only (`source.MatchMedia`/
  `source.MatchCodec`/`source.MatchStreamID`/`source.MatchStream(fn)`); conditions beyond stream
  identity remain roadmap. Rule-created branches drain when the matched stream
  disappears.
- **Join nesting depth**: nested joins are proven at the tested depths
  (`join_nested_test.go`, `TestJoinDescribeEqualsBuildNestedMix`); deeper
  nesting compiles through the same recursion but has no dedicated proof or
  cost model.

## Descriptor-only and deferred

- **H264 recipe encode**: descriptor-only. Registry descriptors are
  discoverable while factory lookup returns `codec.ErrUnavailable`
  (`docs/ARCHITECTURE.md` "Codec backends"); decode/receive verticals are
  active.
- **A/V sink sync, pipeline-wide clock service, pull scheduling**: branch-local
  `flow.SyncPolicy` gates now align or shed packet/frame messages on shared live
  timelines. That closes the live-room branch-local problem, but the theme-C
  endgame is still pull scheduling and sink-level A/V synchronization. The
  time-axis controls (`Seek`/`Rate`/`Segment`) and clock-paced realtime file
  playback already ship (`task_seek_test.go`, `task_time_control_test.go`);
  the rest is analysed in `docs/NORTH_STAR.md` ("Time/sync", attack-plan
  stage 7). Roadmap.
- **Internal-package layering**: measured on the cross-file reference graph
  and still not ready for a package split. The data-transfer boundary has
  started with `internal/recipeir`; normal recipe work-plan handoffs,
  branch-composition planning, media input selection, OnStream branch facts,
  and runtime attach/rebranch branch, operation, and destination facts now
  cross explicit DTOs, but root-only attachments remain before planner
  internals can move behind enforced package boundaries
  (`docs/ARCHITECTURE.md` "Package layering").
- **Destination lifecycle events**: task and runtime-branch destinations now
  publish commit/abort/error events. Standalone `Mutable.Detach` has explicit
  drain/abort outcomes, branch attach/detach events are watchable, and
  stream-rule removals drain rule-created branches.
- **`streamIntent` normalization fold**: Explain stream rows and adapter
  requirements, plus mux compatibility, now consume codec facts from `WorkPlan`
  operations. Remaining validation/planning readers are tracked in
  `docs/NORTH_STAR.md` "Execution order".

## Planned

- **Playout/SRT/NDI providers** through the `provider.Source` extension point:
  by design zero core changes (extension point proven by
  `adapterproof/adapter_compat_test.go`). Roadmap.
- **Extension closure: done for the current grammar.** Everything the
  grammar accepts is implementable externally: adapters
  (five-extension-point proof),
  custom joins (`goav.Join`, symmetry proof in
  `adapterproof/join_proof_test.go`), input decoration (`goav.WrapSource`),
  custom controls (`Deliver`+`AtTap`). The recorded boundary: new solver
  delta classes beyond resample/resize/convert remain core work
  (`docs/API_SURFACE.md` "Extension closure"). New grammar verbs reopen the
  question and must ship with their extension point.
- **Published performance baselines**: the perf-lab harness reports p50/p95/p99,
  heap/RSS, pressure, attach/detach, fanout, container corpus, and real-Opus
  smoke, but release-quality long-run artifacts are still missing
  (`docs/PERFORMANCE.md`). Roadmap.
- **PGO workflow**: profile capture over the canonical suite
  (`scripts/bench/run.sh` is the entry point) feeding default-on
  profile-guided builds. Roadmap.
- **Additional `lifecycle.SwitchAt` boundaries** beyond `NextFrame`/
  `NextKeyframe`/`AtMediaTime` (`rebranch_policy.go`). Roadmap.

## Non-goals

- **GStreamer plugin parity.** goav is not a general multimedia framework;
  matching element-for-element would reproduce the surface the grammar
  exists to avoid (`docs/GSTREAMER_ALTERNATIVE.md`).
- **Hardware codec backends in core.** Core stays pure Go, pinned by
  `TestNoCGOImports` (`hygiene_test.go`); acceleration belongs in external
  adapters behind the `codec` extension points, where cgo is the adapter's
  choice.
- **cgo in core.** Same pin; single-binary `CGO_ENABLED=0` deployment is a
  headline property (`.github/workflows/ci.yml` builds with it).
- **Global registries.** Registries are per-runtime; two runtimes in one
  process must never see each other's adapters (`docs/ARCHITECTURE.md`:
  `goav.New` is the composition root).
- **JIT / runtime code generation.** The planner emits a static graph;
  per-message dispatch stays direct calls. The win would belong to codecs
  (external anyway) and the cost is un-debuggable, un-pinnable hot paths.

## V1 freeze criteria

These checks prove the current surface is governed. They do not, by
themselves, mean the current surface is the v1 target. The release candidate
must also satisfy `docs/SIMPLIFICATION_TARGET.md`, or explicitly document every
retained exception.

The checklist below gates the tag. Each item names its current evidence.

- [x] **Approved API surface**: `api_surface_pin_test.go` +
  `testdata/api_surface.txt` (both-direction pin), with dynamic package
  discovery asserting every module package is governed
  (`TestEveryPublicPackageIsGoverned`).
- [x] **Compile-tested examples**: root `Example*` functions run under
  `go test` (`example_test.go`); the `examples/webrtc-runtime-ladder` module
  builds and tests in CI.
- [x] **Operation reference**: `docs/OPERATIONS.md` covers the front-door
  chain operations by input shape, output shape, domain, inserted conversions,
  primary refusals, and runtime attach behavior; `operations_doc_test.go` pins
  the required sections and front-door links.
- [x] **Structured errors enforced**: `errors_pin_test.go` (catalog-code
  pin) + complete acceptance coverage rows in `docs/ERROR_CATALOG.md`
  generated from `error_catalog_pin_test.go`; every current errcode names a
  bad recipe, rendered-error assertion or golden-equivalent coverage, a fix,
  a cause/sentinel when present, and the test that owns it.
- [x] **Benchmarks present**: 16 measured workloads (`bench_test.go`) +
  pipeline/container suites plus perf-lab latency/RSS/pressure/control/fanout/
  container/real-Opus smoke; bench artifacts run in CI; methodology in
  `docs/PERFORMANCE.md`.
- [x] **Adapter guide**: `docs/ADAPTER_AUTHORING.md` with the executable
  extension-point proof (`adapterproof/adapter_compat_test.go`) and copyable
  external modules under `examples/custom-*`, `examples/transactional-writer`,
  and `examples/control-plane-host`.
- [x] **Extension cookbook**: `docs/EXTENSION_COOKBOOK.md` maps source,
  destination, transactional writer, filter, codec, join, and control-plane
  host seams to copyable code shapes and example modules.
- [x] **Composability laws**: `docs/COMPOSABILITY_LAWS.md` maps direct stream
  equivalence, flow restraint, Build/Attach parity, Describe/Build equality,
  Explain diagnostics, destination handles, branch isolation, rollback, and
  external parity to executable tests.
- [x] **Race tests pass**: CI runs `go test -race` on root, `pipeline`,
  `goavtest`, `format` (`.github/workflows/ci.yml`).
- [x] **Container fuzz/corpus**: `FuzzDemuxerMalformedInputs` with seed
  corpora (`container/matroska`, `container/webm` `fuzz_test.go` +
  `testdata/fuzz`) plus external field-corpus tests
  (`TestExternalMatroskaFieldCorpus`, `TestExternalWebMFieldCorpus`).
- [x] **No global state**: registries live on the runtime
  (`docs/ARCHITECTURE.md`); audited 2026-06: remaining package-level vars
  are error sentinels, immutable profile tables, and atomic ID counters.
  No pin test exists for this; the audit is repeated at review.
- [x] **No undocumented exported symbols**: `doc_pin_test.go` across every
  discovered public package (dynamic module walk; `adapters/*` and
  `container/*` sit behind the codec/format extension points and are excluded by the
  decision recorded in `docs/API_SURFACE.md`).
- [x] **Dependency purity**: importing the root package does not pull bundled
  adapter packages into its dependency graph. The root module still carries
  bundled backend requirements because `goav/bundle` is not a nested module;
  revisit a nested-module split only if SBOM or scanner pressure justifies it
  (`TestRootModuleDependencyPurity`). The Pion ecosystem lives in the nested
  `rtpav` and `webrtcav` modules. Nested modules tag independently with
  prefixed tags (`rtpav/vX.Y.Z`, `webrtcav/vX.Y.Z`), so a root v1 does not
  freeze the transport modules and vice versa; webrtcav requires rtpav
  requires the root, so tag the root first, then rtpav, then webrtcav.
- [x] **Release automation**: `.github/workflows/release.yml` validates
  existing signed tags, runs checks in the tagged module directory, builds root
  CLI archives, and creates GitHub release notes; root CLI releases include
  checksums, Go module SBOM, per-binary buildinfo, and provenance metadata.
  `docs/RELEASING.md` documents signed-tag ownership and tag order.
- [ ] **Release decision**: confirm the `go 1.26` directive in `go.mod` is the
  intended minimum supported Go, close or explicitly waive every item in
  `docs/SIMPLIFICATION_TARGET.md`, fill the compatibility note template in
  `docs/COMPATIBILITY.md`, and cut v1. Not done; this is a maintainer product
  decision plus any remaining simplification work.
