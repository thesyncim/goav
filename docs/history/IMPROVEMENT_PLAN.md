# Improvement Plan: modularity, composability, clarity, performance

Status: implementation complete (2026-07-03); release-quality artifact
generation is locally proven, while attaching fresh artifacts remains
per-release work. Owner: maintainer. This plan follows repo culture: every item
names its evidence and the executable pin that proves it done. Nothing here
reopens settled non-goals (no cgo, no JIT, no global registries, no GStreamer
element parity — `docs/ROADMAP.md` "Non-goals").

## Baseline (what is true today)

- Root package: **31,730 non-test LOC in ~90 flat files** (+45k test LOC),
  deliberately one compilation unit. A package split was measured NO-GO in
  2026-06: bidirectional field-level coupling, no narrow waist
  (`docs/ARCHITECTURE.md` "Package layering").
- The narrow waist has **started**: builders snapshot into `internal/recipeir`
  (251 LOC of pure DTOs) before compile; the boundary is pinned by
  `TestPlannerFilesDoNotReadBuilderInternals` (file-scope) and
  `TestJoinPlannerReadsRecipeIRNotBuilderInternals` (receiver-scope).
- Governed surface: 162 approved identifiers across 9 packages
  (`api_surface_pin_test.go`), every export documented (`doc_pin_test.go`),
  142-code error catalog, composability laws mapped to tests.
- Performance: 17 alloc pins at 0 (or ≤1) allocs/op across direct/buffered
  graphs, mix/composite/select, codec/format/filter stages, RTP depacketizers
  (`docs/PERFORMANCE.md`). Data plane is lock-free by design.
- Module topology: root `go list ./...` sees 47 packages; nested modules
  `rtpav`/`webrtcav`/`playoutav` require root, never the reverse; root
  go.mod requires only `thesyncim/*` (pinned by
  `TestRootModuleDependencyPurity`).

The plan is five workstreams. **A** (modularity) and **C** (clarity) contain
breaking renames and must land **before** the v0.1.0 tag or be explicitly
waived; B, D, E are additive and can land in any order after Phase 0.

---

## Workstream A — Modularity: finish the narrow waist, then split (or record the honest NO-GO)

The 2026-06 measurement found ~20 planner files (~14.8k LOC) reading
unexported fields of 31 grammar files, and grammar files reading planner state
back. Three couplings carry almost all of it. Break them in order; each is
independently valuable (testability, reasoning) even if the final split never
happens.

### A1. Complete the BranchSpec → IR snapshot (primary seam)

**Status (2026-07-02): implemented.** Planner/lowerer files consume captured
`internal/recipeir` facts instead of mutable `BranchSpec`/`jobStreamBuild`
internals, pinned by `TestPlannerFilesDoNotReadBuilderInternals` and
`TestRecipeIRImportsOnlyLeafPackages`. Join planning has its own file-scope
pin under A4.

**What:** Planner passes consume only `internal/recipeir` DTOs. Extend the
capture at the builder→compile edge so `BranchSpec.operations`,
`.destinations`, `.source`, `.media`, `.branchBuffer`, `.err` are never read
by planner files (today's readers: `media_plan_build.go`,
`branch_compose_build.go`, `recipe_compile.go`, `runtime_attach.go`).

**How:** Grow `recipeir.BranchDraft` to carry the remaining root-only
attachments that ARCHITECTURE.md names as "still travel beside the IR":
`InputSpec` facts, stream rules, destination refs (by ID — see A2), join facts.
Capture once, at one edge, immutably.

**Acceptance:** `TestPlannerFilesDoNotReadBuilderInternals` extended from its
current file list to *all* planner/lowerer files with **zero** exemptions;
`TestRecipeIRImportsOnlyLeafPackages` stays green.
**Size:** ~2–3k LOC over several slices. **Risk:** low — mechanical, the
pattern is proven by the slices already landed.

### A2. Destination handles: ID indirection instead of field access

**Status (2026-07-02): implemented.** Destination facts are captured for
planner decisions while concrete destination handles remain opaque until
opening/lowering time. `TestPlannerNeverReadsDestinationSpecFields` pins that
planner files do not inspect `destinationSpec` fields.

**What:** Planner reads of `destinationSpec.output/.format/.name`
(`media_plan_build.go:86,387,703,720`, `branch_compose_build.go`) go through
an immutable ID→facts map captured at the same edge. The mutable spec stays
grammar-side.

**Acceptance:** new pin `TestPlannerNeverReadsDestinationSpecFields`
(AST-scan, same mechanism as the existing boundary pins).
**Size:** ~1–2k LOC. **Risk:** low. The documented "IR exception" (concrete
destination handles ride with the snapshot) stays — handles are opaque IDs
plus facts, which is exactly the exception's intent.

### A3. Operation lowering: resolver DTOs instead of kind-switch on grammar structs

**Status (2026-07-02): implemented.** Planner/lowerer files consume operation
facts lowered from recipe IR; root `operationSpec` compatibility lives in
`operation_facts.go`. `TestPlannerLowersOperationsViaIR` pins the boundary.

**What:** The planner dispatch on `operationSpec.kind/.Filter/.Codec`
(`media_plan_build.go:1599–1778` and join builders) moves behind a lowering
table: each operation kind is captured as a self-describing IR node the
planner resolves without touching the grammar struct.

**Acceptance:** new pin `TestPlannerLowersOperationsViaIR` (no `operationSpec`
identifier in planner files). **Size:** ~1.5–2k LOC. **Risk:** medium — this
is where subtle behavior lives (auto-insert decisions, shape transitions);
gate every slice on the full contract-test suite, which is dense here
(`*_contract_test.go` ≈ 45 files).

### A4. Reverse direction: stop grammar files reading planner state

**Status (2026-07-02): implemented.** Grammar files read compile state only
through query seams, and join grammar/capture is split from join planning.
`TestGrammarFilesDoNotReadPlannerState` and
`TestJoinPlannerReadsRecipeIRNotBuilderInternals` pin both directions.

**What:** The other half of the bidirectionality: `recipe.go`, `source.go`,
`stream_rule.go` read `recipe_compile.go` state; `audio_mix.go`,
`video_composite_build.go`, `select_build.go` reach into 46 identifiers of
`join_build.go`. Replace the join reads with a join-profile table (mix,
composite, select become rows/values, not friends-with-the-planner), and give
the compile state an explicit query interface for the three grammar readers.
Physically split `join_build.go` (2,246 LOC) into grammar-capture vs planner
files — ARCHITECTURE.md already names this the "remaining nicety" that would
upgrade the receiver-scoped pin to file scope.

**Acceptance:** the join pin moves from receiver scope to file scope; a new
`TestGrammarFilesDoNotReadPlannerState` AST pin covers the reverse direction.
**Size:** ~1k LOC. **Risk:** low.

### A5. Re-measure, then split — or record the second honest NO-GO

**Status (2026-07-02): measured and recorded as NO-GO.** ARCHITECTURE.md
records the refreshed type-checked reference graph: the planner/lowerer seed
closes over 63 of 64 root non-test files (99.5% of root LOC), so a physical
`internal/planner` move would effectively move the root package. The A1-A4
pins stay as the enforceable reasoning boundary.

**What:** After A1–A4, recompute the type-checked cross-file reference graph
(the 2026-06 methodology). If the planner file set is now closed under
intra-package dependencies, move it to `internal/planner` (public API
untouched: grammar types keep root identity, so no `reflect.Type.PkgPath`
churn). If it is *not* closed, update ARCHITECTURE.md with the new
measurement and stop — the pins from A1–A4 already deliver the reasoning
benefit; the package boundary is the trophy, not the point.

**Acceptance:** either `internal/planner` exists with an import-direction pin,
or ARCHITECTURE.md "Package layering" records the refreshed measurement and
what remains. **Size:** file moves + import fixes if GO; a paragraph if NO-GO.
**Risk:** none — both outcomes are wins under the repo's honesty rule.

---

## Workstream B — Composability: close the asymmetries, make the laws generative

`docs/COMPOSABILITY_LAWS.md` maps nine laws to tests — all enforced. But the
exploration found five *asymmetries* where an operation legal at top level is
rejected (or silently different) when nested. Each needs a decision: make it
compose, or pin the refusal with an exact-fix error (the repo's error contract
demands refusals name the fix).

| # | Asymmetry | Evidence | Proposed decision |
|---|-----------|----------|-------------------|
| B1 | Join arms cannot declare `.Tap(...)`; must pre-declare and reference | `join_build.go` arm validation; no join-context tap capture | **Status: implemented.** Arm taps lower through the same anchor model; pinned by `TestJoinArmTapComposes`. |
| B2 | `Resample` on a packet-domain *branch* fails where top-level auto-inserts decode | `validate_codec.go` shape check; planner auto-insert only on the direct chain | **Status: implemented.** Branch plans request the same decode+transform solver delta as direct chains; pinned by `TestBranchAutoInsertMatchesDirectChain`. |
| B3 | `Select(Select(a,b), Select(c,d))` builds but control targeting semantics are undocumented | `join_build.go` join-tree validation assumes one select per decision point | **Status: implemented.** Inner selects are targeted by tap (`AtTap`); untargeted ambiguous controls refuse. Pinned by `TestNestedSelectRoutingPinned`. |
| B4 | Taps listed at build time can vanish at runtime after sibling detach; attach then fails "tap not found" | `runtime_attach.go` anchor check vs `Inspectable.Taps()` | **Status: implemented.** The designed refusal now names the fix from the attacher viewpoint; pinned by `TestAttachAfterSiblingDetachRefusalNamesFix`. |
| B5 | Join output accepts `.Encode().To()` but not `.Branches(...)` fanout with per-branch encode | join output surface in `join_build.go` | **Status: implemented.** Join outputs lower through branch composition like ordinary stream points; pinned by `TestJoinOutputBranches`. |

**Acceptance:** one law test per row added to `COMPOSABILITY_LAWS.md`'s table
(`TestJoinArmTapComposes` / `TestBranchAutoInsertMatchesDirectChain` /
`TestNestedSelectRoutingPinned` / `TestAttachAfterSiblingDetachRefusalNamesFix`
/ `TestJoinOutputBranches`). **Size:** B4 is docs+test only; B1/B2/B5 are real
features (~1–2k LOC each); B3 is a decision + small change.

### B6. Generative law testing

**Status (2026-07-02): implemented.** `TestCompositionLawsHoldForGeneratedRecipes`
runs a deterministic seed corpus over `goavtest` fakes. It pins
`Describe()` = built graph, direct-chain = explicit `Branch("main")`,
build-time branch lowering = runtime `Attach` branch lowering (normalizing
runtime branch namespaces), and nested Mix = flat Mix for generated
no-clipping cases. Set `GOAV_COMPOSITION_LONG=1` to expand the corpus locally.

**What:** The laws are pinned by hand-picked cases. Add a recipe-tree
generator (std `testing/quick`-style, in-repo — dependency purity forbids a
property-testing dep) that builds random valid recipes over `goavtest` fakes
and asserts the laws hold structurally: `Describe()` ≡ built graph,
build-time branch ≡ runtime `Attach` of the same grammar, nested joins ≡
flattened equivalents where the laws say so.

**Acceptance:** `TestCompositionLawsHoldForGeneratedRecipes` in the standard
suite (bounded corpus, deterministic seed) + an optional long-run mode.
**Size:** ~1k LOC once; every future grammar change is then tested against
the whole composition space, not just the cases someone thought of.
**Risk:** flake-free only if generation is seed-deterministic — make the seed
a constant, grow the corpus deliberately.

---

## Workstream C — Understandability: topology, names, docs

### C1. Rename the `runtime` config package (breaking — pre-tag only)

**Status (2026-07-02): implemented as `runconfig`.** No package named
`runtime` remains in the module; `package_name_contract_test.go` pins the
name, and runtime options now live under `github.com/thesyncim/goav/runconfig`.

`goav/runtime` (19 exported config options) collides with the root `Runtime`
type and ten root `runtime_*.go` files. Users reading `runtime.WithRealtime()`
cannot tell it configures a `goav.Runtime` without reading the import path
twice. Rename to `runtimeconfig`, or fold the options into root (root is at
40/162 governed identifiers — check the budget) — decide once, before the tag.

**Acceptance:** `api_surface_pin_test.go` updated; no package named `runtime`
in the module. **Size:** mechanical rename + docs.

### C2. Disambiguate `control` vs `ctl` (breaking if renamed — pre-tag only)

**Status (2026-07-02): implemented as `ctlserver`.** The socket host package
is `ctlserver`, `control` documents the in-process vocabulary, and
`package_name_contract_test.go` rejects a package named `ctl`.

`control` = in-process typed media controls; `ctl` = external socket protocol
binding that aliases `control` types (`ctl/ctlserver.go:13–27`). The names differ by
two letters and the docs call both "control plane". Rename `ctl` →
`ctlserver` (or `launch`), and give both packages first-line doc sentences
that name the other ("this is the socket protocol; for in-process controls
see control").

**Acceptance:** doc pins already force package docs; add the cross-reference
to both. **Size:** trivial; the cost is only deciding.

### C3. Docs triage: separate contracts from campaign artifacts

**Status (2026-07-02): implemented.** Finished campaign artifacts live in
`docs/history/`, `docs/README.md` indexes the living docs, and
`TestDocsIndexAndHistoryReferences` pins the index/history contract.

`docs/` has 31 files. Roughly a third are finished-campaign artifacts
(`history/API_REDUCTION_PLAN.md`, `history/SIMPLIFICATION_TARGET.md`,
`history/PROGRESS.md`, `history/V1_CREDIBILITY_AUDIT.md`,
`history/V1_CREDIBILITY_PR.md`, `history/API_INVENTORY.md`) that used to
overlap the living docs (`API_SURFACE.md`, `ROADMAP.md`, `NORTH_STAR.md`).
Every stale plan a new reader finds costs trust in the accurate ones.

**What:** After the tag decision consumes them (`ROADMAP.md` "Release
decision" cites SIMPLIFICATION_TARGET), move campaign artifacts to
`docs/history/` with a one-line tombstone each, and fold
NORTH_STAR/ROADMAP/PROGRESS into two documents: ROADMAP (forward) and
NORTH_STAR (evidence scoreboard). Target: ≤ 20 living docs, each with a
one-line "read this when…" entry in a `docs/README.md` index.

**Acceptance:** `docs/README.md` index exists; no living doc references a
moved one except via history. **Size:** editorial, half a day.

### C4. `cmd/goav`: document the schema or demote the tool

**Status (2026-07-02): implemented by documenting the narrow schema.**
`docs/CLI.md` defines the `goav run` pipeline grammar and when to use CLI vs
library; `TestCLIDocumentationPinsRunSchema` pins the required fragments and
help text link.

The CLI (~1.1k non-test LOC + ~1.75k in `internal/{arg,cli,codec,file,source,transform}args`)
accepts a pipeline schema that is only learnable from `main_test.go`. Either
document the schema in `docs/CLI.md` with the "when CLI vs when library"
answer, or mark the command explicitly experimental in its own `--help`.
Don't expand it before that question is answered.

**Acceptance:** `docs/CLI.md` + a doc pin, or an experimental banner test.

---

## Workstream D — Performance: from "pinned honest" to "state of the art"

The current posture is unusually strong (17 zero/one-alloc pins, lock-free
data plane, honest PERFORMANCE.md). The ladder below is ordered by
value-per-risk; each rung keeps the pins green and adds new ones.

### D1. Refcounted zero-copy fanout (removes the last per-target copy)

**Status (2026-07-02): implemented.** Borrowed packet fanout now makes one
graph-owned copy and shares refcounted views across subscribers; the old
per-target copy baseline remains benchmarkable as
`BenchmarkBufferedFanout/copy` via defensive `CopyAlways`, and the new path is
`BenchmarkBufferedFanout/refcount`.

**What:** Buffered copy-mode fanout copied borrowed payloads per target
(`BenchmarkBufferedFanout/copy`); the refcounted path now implements the
successor. Atomic-refcount payload sharing returns the buffer to its slot on
last release; borrowed payloads get one copy total instead of one per target.

**Acceptance:** new pin `TestBufferedFanoutRefcountAllocs` (0 allocs/op steady
state); `BenchmarkBufferedFanout/refcount` beats `/copy` at fanout ≥ 2;
race detector clean (CI already runs `-race`). Opt-in `flow.BufferPolicy`
mode first, default later. **Risk:** medium — refcount bugs are use-after-free
bugs; gate with a dedicated stress test under race.

### D2. Pooling the remaining per-stream allocations

**Status (2026-07-02): implemented.** `runconfig.WithMediaPools(true)`
installs per-runtime scratch pools (never global) for runtime-owned metadata
maps and built-in join frame-plane slices. Mix/composite stages return their
stage-owned 1-plane and 3-plane scratch on `Close`, including pending cloned
frames drained from join sync state. Public event metadata is deliberately not
pooled because watch subscribers may retain those maps.

**What:** `av.Metadata` (map) is allocated fresh per stream; frame-plane
slices are stage-owned but not shared across attach/detach cycles. Add
runtime-scoped pools (per-runtime, never global — registry rule applies to
pools too), opt-in via a runtime option.

**Acceptance:** `TestMetadataPoolAllocs`, `TestFramePlanePoolAllocs`; existing
0-alloc pins unchanged. **Size:** ~0.5–1k LOC. **Risk:** low.

### D3. PGO as a shipped workflow (already "Planned" in ROADMAP)

**Status (2026-07-02): implemented.** `default.pgo` and
`default.pgo.meta` are checked in, `scripts/bench/pgo.sh` regenerates them
from the canonical root benchmark suite, and CI runs
`scripts/bench/check-pgo.sh` to fail when the suite or generator changes
without refreshing the profile.

**What:** Capture a profile from the canonical perf-lab suite
(`scripts/bench/run.sh`), commit `default.pgo`, and add a CI job that fails if
the profile is older than N releases or the suite composition changed.

**Acceptance:** `default.pgo` in repo; CI freshness check; PERFORMANCE.md
records measured delta (report the real number even if small — honesty rule).
**Risk:** none; PGO is additive.

### D4. SIMD inner loops behind CPU-feature dispatch (no cgo, no JIT)

**Status (2026-07-02): implemented for mix; composite/resample/resize measured
and scalar-retained.** Audio mix accumulation now runs through an internal
selected-kernel seam with scalar fallback. On arm64, exactly two S16 arms use a
NEON `SQADD` kernel (`audio_mix_kernel_arm64.s`); on amd64, exactly two S16
arms use SSE2 `PADDSW` (`audio_mix_kernel_amd64.s`); other arm counts remain
scalar. `TestMixSIMDMatchesScalar` checks the selected kernel byte-for-byte
against the scalar reference over generated clamp, tail, and multi-arm corpora,
and `BenchmarkMixS16Kernel` reports selected-vs-scalar microbench rows. Local
evidence on Apple M4 Max (darwin/arm64, `-benchtime=300ms -count=3`): two-arm
960-sample chunks measured ~38.6-41.3 ns/op selected vs ~1.76-1.88 us/op
scalar, both at 0 allocs/op. The same host also executes the amd64 test binary
under VirtualApple/Rosetta: `GOARCH=amd64` measured ~41.0-44.9 ns/op selected
vs ~1.85-1.87 us/op scalar, both at 0 allocs/op. Composite/resample/resize
now have dedicated scalar inner-loop benchmarks (`BenchmarkCopyPlaneKernel`,
`BenchmarkCompositeI420BlitKernel`, `BenchmarkResampleS16Kernel`,
`BenchmarkScalePlaneNearestKernel`, `BenchmarkScaleI420Kernel`) and narrowly
scoped scalar fast paths for the cases that dominate their current workload
(inside-canvas composite row copies, equal-rate resample copy/channel remap,
integer-scale resize). No custom asm is selected for those kernels until a
prototype beats these measured scalar baselines by >=1.5x on the target
architecture.

**What:** The hot arithmetic — audio mix accumulation (`audio_mix.go` step),
I420 composite blit (`video_composite.go`), resample/resize kernels in
`adapters/resample`, `adapters/resize` — is scalar Go. Add Go-assembler
kernels (`.s` files — pure-Go constraint bars cgo, not asm) selected once at
startup by CPU feature detection, scalar fallback always present and
cross-checked.

**Acceptance:** per-kernel equivalence tests (`TestMixSIMDMatchesScalar`
byte-exact over generated corpora), benchmarks showing the delta per
architecture, `TestNoCGOImports` stays green. **Size:** the largest item in
this plan; do mix first (smallest kernel, clearest win), measure, then decide
whether composite/resample earn their asm. **Risk:** medium-high
(asm correctness, arm64+amd64 maintenance) — the equivalence-test-first
pattern is mandatory, and any kernel that doesn't show ≥1.5× stays scalar.

### D5. Published baselines, soak artifacts, and a regression ratchet

**Status (2026-07-03): implemented and locally proven.**
`bench-results/reference/{root,pipeline,soak}.json` is checked in,
`scripts/bench/baseline.sh generate` refreshes it, and
`scripts/bench/baseline.sh check` runs in CI. The ratchet is intentionally
narrow: it gates `allocs/op` and `B/op` only. `scripts/bench/perf-lab.sh` now
writes `bench-results/manifest/perf-lab-<timestamp>.json` with host/toolchain/
git provenance, `PERF_RELEASE_QUALITY`, the general benchmark benchtime,
separate soak benchtimes, `PERF_GO_TEST_TIMEOUT`, and every generated artifact
path. It emits `bench-results/soak/record-drift-<timestamp>.json` for heap
drift, GC cycles, and GC pause plus
`bench-results/soak/live-room-churn-<timestamp>.json` for the combined
live-room sync + attach/detach churn scenario, including p99, drop counts,
max A/V drift, paced churn interval, and `max_rss_B` when the host exposes RSS.
CI uploads the manifest and soak summaries from its smoke run. Release-quality
soaks use wall-clock test harnesses instead of Go benchmark calibration, and a
local maintainer run has proven the 1h record-drift + 1h paced-churn artifact
path. Fresh artifact attachment and any performance claims remain per-release
work.

**What:** ROADMAP names "release-quality long-run artifacts" as missing. Run
the perf lab on documented reference hardware; commit baseline JSON to
`bench-results/` (the README scaffold already exists); add a multi-hour soak
(live-room sync + attach/detach churn) whose artifact (RSS ceiling, p99,
drop counts) is committed per release. The current CI ratchet compares the
current tree against the committed reference JSON through
`scripts/bench/baseline.sh check`; same-machine timing comparison remains
advisory through `scripts/bench/compare.sh` and `scripts/bench/ci-compare.sh`.
Wall-clock deltas stay advisory because shared CI runners lie.

**Acceptance:** committed allocation/byte baseline artifacts + check script +
CI job are in place. Reference-hardware perf-lab output and multi-hour soak
artifacts remain machine-time-heavy release work.

---

## Workstream E — Flexibility: grow at the seams, not the core

### E1. Prove the provider seam with a third transport (SRT or playout)

**Status (2026-07-02): implemented with `playoutav`.** The new nested module
adapts scheduled packets, frames, and events through `provider.Source`, with an
external compat test proving `goav.Input(playoutav.New(...))` works for frame
and packet domains. The reviewed core diff for the provider seam is zero:
runtime/provider code did not change; only docs and CI/release enumeration were
updated around the new module.

This item existed because ROADMAP promised playout/SRT/NDI "by design zero
core changes." Build **one** (SRT is the most demanded; playout is the most
design-revealing since it pulls on sink-sync) as a nested module like `rtpav`,
and record any core change it forced — if the answer isn't zero, the seam gets
fixed, which is the real point.

**Acceptance:** nested module + adapterproof-style compat test + a line in
ADAPTERS.md; core diff reviewed against the zero-change claim.

### E2. Pluggable solver deltas (the recorded extension-closure boundary)

**Status (2026-07-02): implemented.** `shape.AllowCustom("name")` extends the
`.Auto(...)` policy vocabulary, and `runconfig.WithShapeDelta` registers a
per-runtime contributor that can return a fresh stage operation for that
custom class. `adapterproof.TestExternalShapeDeltaContributorAppearsInExplain`
proves an external toy delta is selected by `Explain(ctx)` with the same
shape-conversion diagnostic path as built-ins.

`docs/API_SURFACE.md` used to record that new solver delta classes beyond
resample/resize/convert were core work. That boundary is now opened through
explicit custom policies: external adapters register a per-runtime contributor
and callers opt in with `.Auto(shape.AllowCustom("name"))`, preserving the
same explain/refusal quality as built-ins without adding global state.

**Acceptance:** an adapterproof test registering a toy delta externally and
watching `Explain(ctx)` cite it; the B2 uniformity rule (deltas apply at
every chain position) inherited automatically. **Risk:** medium — this is API
design, do it as a written proposal first.

### E3. OnStream conditions beyond identity

**Status (2026-07-02): implemented.** `source.MatchFirst(n)`,
`source.MatchAfter(d)`, and `source.MatchWithin(d)` ship, with runtime
matcher state pinned by `TestStreamMatchContracts` and
`TestOnStreamMatchFirstLimitsRuntimeAttachments`.

ROADMAP marks rule breadth (beyond Match{Media,Codec,StreamID,Stream(fn)})
as roadmap. `MatchStream(fn)` already admits arbitrary predicates — what's
missing is *stateful* conditions (nth stream, after-time, count-limited).
Ship the two or three that real sessions need (`MatchFirst(n)`,
time-windowed) rather than a condition language.

### E4. Finish H264 recipe encode or say why not

**Status (2026-07-02): documented as deliberate decode-only posture.**
ROADMAP records that default-bundle H.264 encode remains descriptor-only
because the repo does not ship a vetted pure-Go encoder backend with
caller-owned output buffers and allocation pins; applications can register an
encoder explicitly with `runconfig.WithEncoder` or a codec adapter.

Descriptor-only today (`codec.ErrUnavailable`). Either wire the `goh264`
encode factory through the recipe path, or record in ROADMAP why decode-only
is the deliberate posture (patent posture / backend quality). The current
half-state ("registry lists it, lookup refuses") is the only place the
grammar advertises something it won't do.

---

## Phase 0 — hygiene (do immediately, no dependencies)

1. `.gitignore` the two new example binaries:
   `/examples/dynamic-audio-room/dynamic-audio-room`,
   `/examples/gio-webrtc-showcase/gio-webrtc-showcase` (currently untracked
   noise in every `git status`).
2. Delete stray local test binaries (`goav1.test`, `pipeline.test` — ignored
   but present).
3. Decide C1/C2 renames **now** (they gate the tag).

---

## Sequencing and dependencies

```text
Phase 0  hygiene + C1/C2 rename decisions        (hours)
Phase 1  B4 doc pins, B3 decision, C3 docs triage, C4 CLI doc   (days)
Phase 2  A1 → A2 → A3 → A4 (narrow waist, slice by slice)       (the long haul)
Phase 3  A5 re-measure → split or NO-GO record
Phase 4  B1/B2/B5 compose features + B6 generative laws
         (B2 before E2 — uniform deltas first, then pluggable)
Phase 5  D1 → D2 → D3 → D5 → D4 (perf ladder, cheapest risk first;
         D4 SIMD only after D5 baselines exist to prove it)
Phase 6  E1 → E2 → E3/E4 (flexibility at the seams)
```

The v0.1.0 tag can be cut after Phase 1 (renames done, nothing else is
breaking). Phases 2–6 are all tag-compatible.

## Risk register

| Risk | Mitigation |
|------|------------|
| A3 lowering changes auto-insert behavior subtly | contract tests are the gate; land behind the existing golden Describe/Explain equality pins |
| D1 refcount use-after-free | opt-in first, dedicated stress under `-race`, keep copy mode as fallback |
| D4 asm divergence between architectures | byte-exact equivalence tests vs scalar are mandatory per kernel; scalar path never deleted |
| B1/B5 grammar growth reopens surface freeze | each adds ≤2 governed identifiers; run the api-surface pin math before design |
| Docs triage deletes something still cited | move to `docs/history/`, never delete; grep for inbound links first |
| Plan rot (this document becomes C3's next victim) | this file moves to `docs/history/` when its phases close; ROADMAP absorbs the survivors |

## What this plan deliberately does not do

- No rewrite. The grammar, planner model (`BranchSpec -> WorkPlan/WorkPatch`),
  and lock-free pipeline are the assets; every workstream strengthens the
  existing seams rather than replacing them.
- No new dependencies. Dependency purity (`TestRootModuleDependencyPurity`)
  is a feature; B6 builds its own generator, D4 uses Go asm, not intrinsic
  libraries.
- No speculative packages. A5 explicitly allows the second honest NO-GO;
  boundaries are proven by pins, not by directory layout.
