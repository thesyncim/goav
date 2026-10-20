# Compatibility

goav is pre-v1. This page owns the release promise: what a v1 tag keeps
stable, what ships today but stays governed pre-v1, the compatibility rules
every tag follows, and the note a maintainer fills in before cutting one. It
is the line reviewers hold new work against. [`ROADMAP.md`](ROADMAP.md)
records when governed features might graduate, and
[`history/SIMPLIFICATION_TARGET.md`](history/SIMPLIFICATION_TARGET.md) records
why the surface was trimmed.

## v1 scope

The v1 product is a small, composable, explicit, pure-Go media recipe layer:

```text
input.Video/Audio/Stream() -> operations -> taps -> forks -> .To(destinations) -> rt.Run
```

The user model is `Input -> Stream -> Operations -> Output -> Task`. A recipe
is inert Go data until `rt.Describe`, `rt.Explain`, `rt.Build`, or `rt.Run`
compiles it; failures surface as structured `*goav.BuildError` values before
resources open.

### v1-supported workflows

These workflows are the v1 front door. A v1 release keeps them working, and
the beginner docs teach only these.

| Workflow | Spelling | Frozen because |
| --- | --- | --- |
| Packet copy (record/remux) | `input.To(goav.Write(...))`, `input.Video().To(...)` | The simplest correct path; no decode, no codec adapter. |
| Decode to sink | `input.Audio().Decode().To(goav.Sink(...))` | Hands decoded frames to application code with one operation. |
| Decode -> transform -> encode -> mux | `.Decode().Resize(...).Encode(codec).To(...)` | The canonical transcode; the shape solver inserts conversions explicitly. |
| Explicit fanout | reuse a Stream: `v := input.Video().Decode()`, then `v.Resize(...).Encode(...).To(a)` and `v.Encode(...).To(b)` | One stream point fans out with each fork owning its destinations. |
| Custom source and custom sink | `goav.Source(...)`, `goav.Sink(...)`, `goav.Custom(...)` | Application-owned media boundaries plug in with no global process state. |
| Describe / Explain before Run | `rt.Describe(outputs...)`, `rt.Explain(ctx, outputs...)` | The plan is inspectable before any resource opens. |
| Structured build errors | `*goav.BuildError` (Family, Code, Phase, fixes) | Every user-actionable refusal is machine-readable and carries a fix. |

A new contributor can implement a custom source, a custom sink, and a simple
decode pipeline with only these workflows and the
[extension cookbook](EXTENSION_COOKBOOK.md), without reading the expert graph,
runtime mutation, or control-plane docs.

### v1 guarantees

These invariants hold across the v1 surface and are enforced by tests:

1. **Front-door model is stable.** The shape
   `input streams -> operations -> taps -> forks -> .To(destinations) -> rt.Run`
   does not change.
2. **No required graph handles.** Normal users never need graph handles,
   string routing, runtime internals, or workflow-specific APIs.
3. **One lowering path.** `rt.Describe`, `rt.Explain`, `rt.Build`, and runtime
   `Attach` share one operation validation and lowering; there is no hidden
   second path ([composition laws](OPERATIONS.md#composition-laws)).
4. **`rt.Run` is the narrow front door**: it runs Outputs to completion.
   Inspection, events, live control, and runtime mutation are opt-in through
   `rt.Build` / `LiveTask`.
5. **Structured errors.** Every user-actionable failure is a
   `*goav.BuildError` with stable `Family`/`Code`, an explicit `Phase`,
   operation/node context, machine-readable details, and concrete fixes.
6. **No global adapter registry.** Adapter registration is explicit and
   per-runtime; there is no process-global state.
7. **Root import stays pure.** Importing `github.com/thesyncim/goav` does not
   pull bundled adapter packages into the root package dependency graph.
8. **No hot-path allocation regressions.** Existing allocation pins hold, and
   performance claims are backed by reproducible benchmark artifacts.

## Governed pre-v1

These features are implemented, tested, and documented, but they are **not
part of the v1 promise** unless a release decision explicitly retains and
freezes them. Governed means pinned by tests and described in a focused doc,
not unreliable: the beginner surface does not depend on them, and v1 does not
commit to their shape. Treat the tier-A list in
[`API_SURFACE.md`](API_SURFACE.md) as an inventory to shrink or explicitly
retain, not as an automatic v1 promise.

| Feature | Lives in | Why it is not yet frozen |
| --- | --- | --- |
| Expert graph | [`expert`](API_SURFACE.md) (tier C), `graphrender` | Handle-based graph escape hatch off the grammar; not needed for the normal path. |
| Runtime Attach / Detach / Rebranch, raw controls, advanced observation | [`OPERATIONS.md`](OPERATIONS.md), `LiveTask` | Live graph mutation and control; they depend on the `LiveTask` from `rt.Build`, outside the `rt.Run` front door. |
| Control-plane socket host | [`CONTROL_PLANE.md`](CONTROL_PLANE.md), `ctlserver` | Out-of-band live control over a socket or HTTP; an application-hosting concern. |
| Broad `OnStream` dynamic rules | [`USE_CASES.md`](USE_CASES.md) | Only identity stream-rule cases are v1; broader dynamic breadth stays governed. |
| `Mix` / `Composite` / `Select` / custom `Join` | [`MULTI_INPUT.md`](MULTI_INPUT.md), [`OPERATIONS.md`](OPERATIONS.md) | N-to-1 convergence; powerful but not a beginner workflow. |
| Live provider transcode | [`USE_CASES.md`](USE_CASES.md) | Works (`TestRecipeResolvedBuildUsesMediaPlanRTPEncodeOutput`); a provider such as RTP lives in a nested module, outside the beginner path. |

Reaching for one of these is fine; they are first-class, governed features.
The freeze only guarantees that `rt.Run(ctx, outputs...)` never forces a
beginner through them.

## Current runtime and transport migration

The pre-v1 runtime contract update changes `PacketReader.ReadRTP` to four
results and makes `pipeline.Graph.CleanupError` mandatory. Built-task Run success
means execution ended; actual destination commit/abort/close follows finalization.
Early Close abandons active outputs deterministically. Failure isolation is
opt-in on the existing Output value, with FailTask remaining the default.

See [Runtime contracts and migration](RUNTIME_CONTRACTS.md) for exact states,
wrapper migration, RTP epoch scope, and source compatibility notes. Update root
and transport module pins together. Execution metadata is additive JSON; clients
must accept the new finalizing states and destination IDs. This change does not
claim a new v1 freeze or comparative performance leadership.

## Module and dependency decision

`goav/bundle` is a **package in the root module**, not a nested module, by
decision rather than deferral:

- Package-level purity is sufficient. `TestRootImportDoesNotPullBundledAdapters`
  proves importing the root package pulls no bundled adapter, and
  `TestRootModuleDependencyPurity` keeps root-module requirements to the
  permissively licensed backends (GPL code is the nested `gplcodec`).
- A nested `bundle` module would add a `go.mod`, per-module tags, and release
  coordination for no isolation the package boundary does not already give.
  `rtpav`, `webrtcav`, and `playoutav` are nested because they own
  source-provider vocabularies and dependency trees outside the root grammar;
  `bundle` carries only `thesyncim/*` backends.

The root module therefore still requires the bundled backends. The decision is
revisited only if SBOM or scanner pressure demands module-level isolation; see
[`history/API_INVENTORY.md`](history/API_INVENTORY.md) for the dependency
baseline.

## Compatibility rules

- Root module: pre-v1. The front-door grammar, structured errors, task model,
  and extension seams are governed and tested, but breaking changes may still
  land before v1 when they are recorded in `CHANGELOG.md`.
- Structured errors: switch on `BuildError.Family` first. `BuildError.Code` is
  a detailed diagnostic leaf that may grow within a family before v1. Read
  machine facts with `Detail[T](key)` and render diagnostics with
  `DetailLines()` / `FixLines()`; the backing detail/fix records are
  intentionally not public DTOs.
- Extension seams stay documented and tested but may grow as new adapters need
  capability. Every new exported symbol needs the API-restraint checklist in
  [`API_SURFACE.md`](API_SURFACE.md).
- Nested modules: `rtpav`, `webrtcav`, and `playoutav` tag independently as
  `rtpav/vX.Y.Z`, `webrtcav/vX.Y.Z`, and `playoutav/vX.Y.Z`. A root release
  does not freeze them, and a transport release names the root version it was
  tested against.
- Example modules: `examples/*` are copyable adoption artifacts, not imported
  library APIs. They keep building but promise nothing beyond the public APIs
  they demonstrate.
- Minimum Go version: the module directive is `go 1.27.0`. First-party codec
  backends (`goav1`, `gopus`) require Go 1.27, so the floor is a minor
  version, not a patch-level toolchain pin. Before a tag, the maintainer
  confirms it is the intended minimum for every module being released.

## Draft compatibility note: root v0.1.0

Compatibility note for root v0.1.0

Minimum Go version:
- go 1.27.0. The root module, nested transport modules, and standalone example
  modules use the 1.27 module floor as of 2026-10-17. The maintainer still
  needs to confirm this floor before signing the tag. Release security evidence
  should use the latest Go 1.27 patch toolchain; the local govulncheck pass
  below used Go 1.27.2.

Module scope:
- Root module tag: `v0.1.0`.
- Nested modules are not included in the root tag. `rtpav`, `webrtcav`,
  `playoutav`, `player`, `libcodec`, `gplcodec`, and `cmd/goav` require separate prefixed tags if released.
- Tag order remains root first, then `rtpav`, then `webrtcav`/`playoutav`/`player`/`libcodec`/`gplcodec`,
  then `cmd/goav`, as needed.

API surface:
- Added exported symbols: `BuildError` helper methods, the value grammar
  (`Stream`, `Output`, `JoinArg`, `At`, `ByPTS`,
  `Runtime.Run`/`Build`/`Describe`/`Explain`, `LiveTask.Spec`/`Tap`/`Track`/
  `Attach`), task capability vocabulary (snapshots, states, watch filters, and
  detach/rebranch policies, all in the root), `Write(name, writer)`,
  `bundle.Describe`, sync policy
  options, media-time rebranch boundaries, and the sealed option types
  (`StreamOption`, `InputOption`, `DestinationOption`, `MediaOption`) exported
  for godoc linkability while constructors remain the only construction surface.
- Removed or renamed exported symbols: `ContextCloser`, `Observable`,
  `Controllable`, `Explainer`, `Inspectable`, `Mutable`, `InputStream`,
  `RecipePatch`, `TransformSpec`, legacy `BuildError.Details`, legacy
  `BuildError.Suggestions`, `codec.CodecSpec` (now `codec.Spec`), path-style
  writer destinations, same-handle destination grouping, and
  graph-node-prefix tap fallback.
- API-restraint links for additions: `docs/API_SURFACE.md`,
  `docs/history/SIMPLIFICATION_TARGET.md`, and `docs/history/V1_CREDIBILITY_AUDIT.md`.

Behavior changes:
- `rt.Run(ctx, outputs...)` runs a recipe to completion; `rt.Build` returns a
  `LiveTask` for inspection, watches, controls, or runtime mutation.
- Streams require explicit `.Decode()` before frame-domain consumers; a stream
  routed without `.Decode()` copies, keeping packet-domain recipes packet preserving.
- A reused destination handle is the shared output: one value passed to
  several Outputs feeds one mux or sink (`Mux` was removed).
- Compile-time planning now uses captured immutable recipe IR for inputs,
  streams, destinations, joins, stream rules, graph lowerers, and work-plan
  rendering.
- Runtime attach, detach, rebranch, and stream-rule mutation capture explicit
  input handoffs before graph mutation.
- Runtime mutation, control hosts, raw controls, advanced observation,
  `OnStream`, joins, composite/select, and `expert.Graph` remain governed
  pre-v1 features, not automatic v1 promises.

Adapter and extension changes:
- Bundled adapters remain behind `goav/bundle`; importing the root package does
  not import bundled adapter packages into the root package dependency graph.
- External example modules cover custom sources, provider sources, custom
  destinations, filters, codecs, joins, transactional writers, and
  control-plane hosts.

Migration notes:
- Replace root lifecycle/control interfaces with local structural interfaces
  or the explicit `LiveTask` capabilities `rt.Build` returns.
- Replace builder chains (`goav.From(in)...To(out).Run(ctx)`, `.Branches(...)`)
  with Stream values: `rt.Run(ctx, in.Video()...To(out))`, forks by reuse and `.As(name)`.
- Replace `Writer`/path-style output spelling with `Write(name, writer)` or
  `URI(uri)` as appropriate.
- Replace `Mux(name, destination)` with the destination value itself, passed
  to every Output that shares it.
- Switch structured-error callers from legacy details/suggestions fields to
  `Detail[T](key)`, `DetailLines()`, and `FixLines()`.
- Add explicit `.Decode()` before frame transforms, real encoders, frame taps,
  or decoded-frame sinks; leave `.Decode()` out for packet-preserving flows.

Evidence:
- Local root tests: `go test -p 1 ./...`.
- Local patched-toolchain tests: `GOTOOLCHAIN=go1.27.2 go test -p 1 ./...`.
- Local pure-Go tests: `CGO_ENABLED=0 go test -p 1 ./...`.
- Local race subset:
  `CGO_ENABLED=1 go test -race -count=1 . ./pipeline ./goavtest ./format ./ctlserver ./launch ./graphrender`.
- Local static checks: `go vet ./...` and
  `go run honnef.co/go/tools/cmd/staticcheck@latest ./...`.
- Local formatting check: `test -z "$(gofmt -l .)"`.
- Local security scan:
  `GOTOOLCHAIN=go1.27.2 go run golang.org/x/vuln/cmd/govulncheck@latest ./...`
  reported no called vulnerabilities.
- Local benchmark smoke:
  `BENCH_COUNT=1 scripts/bench/run.sh -bench 'BenchmarkLatencyRecordPackets|BenchmarkSourcePush' .`.
- Local perf-lab smoke: `PERF_BENCHTIME=1x scripts/bench/perf-lab.sh`.
- Nested module tests: every `examples/*/go.mod`, `goavtest/expect`, `rtpav`,
  `webrtcav`, and `playoutav` passed `go test -p 1 ./...`.
- Root dependency check: `go list -deps github.com/thesyncim/goav` had no
  adapter, container, `rtpav`, `webrtcav`, or `playoutav` package leak.
- CI release workflow, signed-tag validation, and release-quality benchmark
  artifacts still need release-day evidence.

Deferred / not claimed:
- No production performance leadership claim is made from this local pass.
- Signed tag, GitHub release, full CI matrix, and long perf-lab artifacts
  remain release-day work.
- v1 stability is not claimed by `v0.1.0`; this is the first pre-v1 root
  release candidate note.

## Release compatibility note template

Copy this section into the release notes or PR body before cutting a tag. Empty
bullets are release blockers, not placeholders to leave behind.

```text
Compatibility note for <module> <version>

Minimum Go version:
- go <version>

Module scope:
- Root / rtpav / webrtcav / playoutav / other:
- Dependency/tag order:

API surface:
- Added exported symbols:
- Removed or renamed exported symbols:
- API-restraint checklist links for additions:

Behavior changes:
- Build/Explain/Attach/Rebranch compatibility:
- Structured error code/catalog changes:
- Runtime control or snapshot/event changes:
- Advanced/non-v1 exceptions retained in this release:

Adapter and extension changes:
- Codec/filter/format/provider/control-host changes:
- External example modules updated:

Migration notes:
- Required user action:
- Known incompatibilities:
- Deprecated paths:

Evidence:
- CI run:
- Runtime tests:
- Race tests:
- Staticcheck/vet/gofmt:
- Benchmark/perf-lab artifacts:
- Security scan:

Deferred / not claimed:
- Performance claims not supported by attached artifacts:
- Roadmap items not included in this tag:
```

## Release gate

Do not cut v1 just because docs and pins are green. Cut it only when the
release candidate meets the v1 scope above or records a maintainer decision
for every retained governed-pre-v1 exception, the compatibility note is filled
for the tag, the release workflow has validated the signed tag, the release
decision item in [`ROADMAP.md`](ROADMAP.md) is closed by the maintainer, and
every exception to `docs/history/SIMPLIFICATION_TARGET.md` is written down.
The maintainer's `v0.1.0` tag is the final scope-freeze action; it does not
itself claim v1 stability.
