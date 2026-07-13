# Roadmap: stability tiers and the road to v1

goav is pre-v1. Use this roadmap to answer a human question: what can I rely
on today, what is still allowed to move, and what would be dishonest to imply
as done? It separates stable, experimental, deliberately deferred, planned,
and non-goal work, then names the checks that must hold before a v1 tag. Every
claim cites the test, benchmark, or document that backs it, or is marked
**roadmap**.

The nearby docs split the work by reader need: `docs/NORTH_STAR.md` keeps the
evidence-cited acceptance scoreboard, `docs/V1_SCOPE.md` names the release
scope, `docs/REPOSITORY_TRUST.md` records the GitHub metadata and release
posture, and `docs/GSTREAMER_ALTERNATIVE.md` explains how goav relates to
GStreamer. Earlier planning artifacts now live in `docs/history/`.

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
whole surface is the v1 promise." The governed surface is the tier inventory
in `docs/API_SURFACE.md`, reviewed on every change: the machine-enforced
approved-identifier pins were deliberately removed on 2026-06-27, CI still
requires a doc comment on every public package, and the doc-citation pins
(`docs_citation_contract_test.go`) keep the governance docs from citing
enforcement that no longer exists. The inventory:

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
  structured `BuildError` with stable families, detailed codes,
  `Detail(key)`, `DetailLines()`, and `FixLines()`;
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

The error contract (`error_acceptance_test.go`,
`TestErrorCatalogDocMatchesErrcodeCatalog`, `docs/ERRORS.md`) and runtime
invariants are pinned current contracts: close
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

- **Buffered mutable frame/owned-payload fanout**: borrowed packet fanout now
  shares one graph-owned refcounted copy (`pipeline`
  `TestBufferedFanoutRefcountAllocs`, `BenchmarkBufferedFanout/refcount`);
  owned packets, frame planes, and defensive `CopyAlways` mode still copy per
  target to preserve branch-local mutation isolation.
- **Attach-under-load cost**: `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation dominated by planning; not a data-plane figure.
- **OnStream rule breadth**: identity matches (`source.MatchMedia`/
  `source.MatchCodec`/`source.MatchStreamID`/`source.MatchStream(fn)`) plus
  stateful `source.MatchFirst(n)`, `source.MatchAfter(d)`, and
  `source.MatchWithin(d)` conditions ship. Rule-created branches drain when
  the matched stream disappears.
- **Join nesting depth**: nested joins are proven at the tested depths
  (`join_nested_test.go`, `TestJoinDescribeEqualsBuildNestedMix`); deeper
  nesting compiles through the same recursion but has no dedicated proof or
  cost model.
- **PGO profile workflow**: `default.pgo` is a checked-in workflow artifact
  generated by `scripts/bench/pgo.sh` and freshness-checked in CI by
  `scripts/bench/check-pgo.sh`. It is not a performance claim; measured deltas
  still need same-machine before/after evidence.
- **Benchmark allocation/byte ratchet**:
  `bench-results/reference/{root,pipeline,soak}.json` is checked in and
  enforced by `scripts/bench/baseline.sh check` in CI. It gates `allocs/op`
  and `B/op` only; timing, RSS, and latency claims still need release
  artifacts indexed by a `scripts/bench/perf-lab.sh` manifest.
- **Runtime media pools**: `runconfig.WithMediaPools(true)` enables
  per-runtime scratch pooling for runtime-owned metadata maps and join
  frame-plane slices. The allocation path is pinned, but the option stays
  explicit while attach/detach churn workloads gather real release evidence.
- **Custom shape solver deltas**: external adapters can register per-runtime
  `.Auto(...)` delta contributors with `runconfig.WithShapeDelta` and require
  callers to opt in with `shape.AllowCustom("name")`. The external proof is
  `adapterproof.TestExternalShapeDeltaContributorAppearsInExplain`; this is
  governed pre-v1 API design, not a v1 promise yet.

## Descriptor-only and deferred

- **H264 recipe encode**: deliberately descriptor-only in the default bundle.
  Registry descriptors are discoverable while factory lookup returns
  `codec.ErrUnavailable` (`docs/ARCHITECTURE.md` "Codec backends"). Decode and
  receive verticals are active; encode is not advertised as runnable because
  the repo does not ship a vetted pure-Go H.264 encoder backend with
  caller-owned output buffers and allocation pins. Applications that need H.264
  encode register their chosen backend explicitly with `runconfig.WithEncoder`
  (or a codec adapter) instead of getting an unproven bundled path.
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

- **SRT/NDI providers** through the `provider.Source` extension point:
  by design zero core changes. The third-provider claim is now proven by
  `playoutav`, a nested scheduled-playout module with an external compat test;
  SRT/NDI remain roadmap.
- **Extension closure: done for the current grammar.** Everything the
  grammar accepts is implementable externally: adapters
  (`adapterproof/adapter_compat_test.go`),
  custom joins (`goav.Join`, symmetry proof in
  `adapterproof/join_proof_test.go`), input decoration (`goav.WrapSource`),
  custom controls (`Deliver`+`AtTap`), and custom `.Auto(...)` solver deltas
  (`shape.AllowCustom` + `runconfig.WithShapeDelta`). New grammar verbs reopen
  the question and must ship with their extension point.
- **Published performance baselines**: the allocation/byte CI ratchet now
  ships, and the perf-lab harness reports p50/p95/p99, heap/RSS, pressure,
  soak drift, live-room attach/detach churn, fanout, container corpus,
  real-Opus smoke, and a release manifest that records host/toolchain/git
  provenance plus separate soak benchtimes. The churn soak summary carries
  `max_rss_B` when the host exposes RSS, and release-quality soaks run through
  wall-clock test harnesses with a recorded paced churn interval. The
  long-run artifact path has been locally proven; fresh latency/RSS/throughput
  artifacts still need to be attached per release candidate
  (`docs/PERFORMANCE.md`). Roadmap.
- **Additional `lifecycle.SwitchAt` boundaries** beyond `NextFrame`/
  `NextKeyframe`/`AtMediaTime` (`rebranch_policy.go`). Roadmap.

## Non-goals

- **GStreamer plugin parity.** goav is not a general multimedia framework;
  matching element-for-element would reproduce the surface the grammar
  exists to avoid (`docs/GSTREAMER_ALTERNATIVE.md`).
- **Hardware codec backends in core.** Core stays pure Go — every CI build
  and test runs `CGO_ENABLED=0`, so a cgo import cannot land; acceleration
  belongs in external adapters behind the `codec` extension points, where cgo
  is the adapter's choice.
- **cgo in core.** Same gate; single-binary `CGO_ENABLED=0` deployment is a
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
must also satisfy `docs/history/SIMPLIFICATION_TARGET.md`, or explicitly document every
retained exception.

The checklist below gates the tag. Each item names its current evidence.

- [x] **Approved API surface**: the tier inventory in `docs/API_SURFACE.md`,
  updated in the same change as any new export and enforced in review. The
  mechanical both-direction pin was removed 2026-06-27; the doc-citation pins
  (`docs_citation_contract_test.go`) keep the inventory's evidence honest.
- [x] **Compile-tested examples**: root `Example*` functions run under
  `go test` (`example_test.go`); the `examples/webrtc-runtime-ladder` module
  builds and tests in CI.
- [x] **Operation reference**: `docs/OPERATIONS.md` covers the front-door
  chain operations by input shape, output shape, domain, inserted conversions,
  primary refusals, and runtime attach behavior. Its former section pin was
  removed with the doc source pins (2026-06-27); accuracy is review-owned.
- [x] **Structured errors enforced**: `error_acceptance_test.go` builds the
  bad recipes and asserts rendered refusals, and
  `TestErrorCatalogDocMatchesErrcodeCatalog` keeps `docs/ERROR_CATALOG.md` in
  lockstep with `errcode/errcode.go`; every current errcode names a bad
  recipe, coverage assertion, a fix, and the test that owns it.
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
- [x] **Race tests pass**: CI runs `go test -race ./...` across the whole
  root module plus each nested module (`.github/workflows/ci.yml`).
- [x] **Container fuzz/corpus**: `FuzzDemuxerMalformedInputs` with seed
  corpora (`container/matroska`, `container/webm` `fuzz_test.go` +
  `testdata/fuzz`) plus external field-corpus tests
  (`TestExternalMatroskaFieldCorpus`, `TestExternalWebMFieldCorpus`).
- [x] **No global state**: registries live on the runtime
  (`docs/ARCHITECTURE.md`); audited 2026-06: remaining package-level vars
  are error sentinels, immutable profile tables, and atomic ID counters.
  No pin test exists for this; the audit is repeated at review.
- [x] **Documented public packages**: CI's package-documentation smoke fails
  any public package without a package doc; per-symbol doc comments are
  review-enforced since the per-symbol scanner was removed 2026-06-27
  (`adapters/*` and `container/*` sit behind the codec/format extension
  points and are excluded by the decision recorded in `docs/API_SURFACE.md`).
- [x] **Dependency purity**: importing the root package does not pull bundled
  adapter packages into its dependency graph. The root module still carries
  bundled backend requirements because `goav/bundle` is not a nested module;
  revisit a nested-module split only if SBOM or scanner pressure justifies it
  (`TestRootModuleDependencyPurity`). The Pion ecosystem lives in the nested
  `rtpav` and `webrtcav` modules; `playoutav` is nested for the same
  provider-seam isolation without Pion. Nested modules tag
  independently with prefixed tags (`rtpav/vX.Y.Z`, `webrtcav/vX.Y.Z`,
  `playoutav/vX.Y.Z`), so a root v1 does not freeze the transport modules and
  vice versa; webrtcav requires rtpav requires the root, so tag the root first,
  then rtpav, then webrtcav/playoutav as needed.
- [x] **Release automation**: `.github/workflows/release.yml` validates
  existing signed tags, runs checks in the tagged module directory, builds root
  CLI archives, and creates GitHub release notes; root CLI releases include
  checksums, Go module SBOM, per-binary buildinfo, and provenance metadata.
  `docs/RELEASING.md` documents signed-tag ownership and tag order.
- [ ] **Release decision**: confirm the `go 1.26` directive in `go.mod` is the
  intended minimum supported Go, close or explicitly waive every item in
  `docs/history/SIMPLIFICATION_TARGET.md`, fill the compatibility note template in
  `docs/COMPATIBILITY.md`, and cut v1. Not done; this is a maintainer product
  decision plus any remaining simplification work.
