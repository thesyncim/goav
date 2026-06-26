# Compatibility policy

goav is pre-v1. This page is the release promise ledger: what users can rely
on today, what a future v1 should mean, and what a maintainer must write down
before cutting a tag.

The repository can be release-ready only when each tag states what
compatibility it is promising and what evidence backs that promise.

## Current promise

- Root module: pre-v1. The front-door grammar, structured errors, task model,
  and extension seams are governed and tested, but breaking changes may still
  land before v1 when they are recorded in `CHANGELOG.md`.
- Root package dependency scope: importing `github.com/thesyncim/goav` does not
  import bundled adapter packages into the root package dependency graph.
  `goav/bundle` is currently a package in the root module, not a nested module,
  so the root module still carries bundled backend requirements until/unless a
  nested-module split is justified by SBOM or scanner pressure.
- Structured errors: applications should switch on `BuildError.Family` first.
  `BuildError.Code` remains a detailed diagnostic leaf that may grow within a
  family before v1; rendered details and suggestions are compatibility output,
  while typed `Fields` and `Fixes` are the production data model.
- Nested modules: `rtpav` and `webrtcav` tag independently with
  `rtpav/vX.Y.Z` and `webrtcav/vX.Y.Z`. A root release does not freeze the
  transport modules, and a transport release must name the root version it was
  tested against.
- Example modules: `examples/*` are copyable adoption artifacts, not imported
  library APIs. They should keep building, but they do not carry compatibility
  promises beyond the public APIs they demonstrate.
- Minimum Go version: the current module directive is `go 1.26`. First-party
  codec backends currently require Go 1.26, so the release floor is a minor
  version, not a patch-level local toolchain pin. Before a tag, the maintainer
  must confirm that directive is the intended supported minimum for every
  module being released.

## V1 promise draft

A v1 tag should promise the smaller normal-user workflows targeted by
`docs/SIMPLIFICATION_TARGET.md`, not the entire current governed inventory.
Use the Tier A surface in `docs/API_SURFACE.md` as an inventory to shrink or
explicitly retain, not as an automatic v1 promise:

- the supported `From(...) -> stream selection -> operations -> branches ->
  destinations` workflows named in `docs/SIMPLIFICATION_TARGET.md`;
- `Describe`/`Explain` before `Build`, plus `Task.Run`/`Task.Close` for built
  tasks;
- structured `*goav.BuildError` refusals with stable families and typed
  fields/fixes;
- custom source and custom sink seams needed by the supported workflows.

Runtime mutation (`Attach`, `Detach`, `Rebranch`), control-plane hosts, raw
controls, advanced observation, `OnStream` breadth, `Mix`/`Composite`/`Select`,
and `expert.Graph` are advanced/non-v1 unless the release compatibility note
explicitly retains them and records the exception to `docs/SIMPLIFICATION_TARGET.md`.

The extension seams remain documented and tested, but they may grow as new
adapters require additional capability. New exported symbols still require the
API-restraint checklist in `docs/API_SURFACE.md`.

## Draft compatibility note: root v0.1.0

Compatibility note for root v0.1.0

Minimum Go version:
- go 1.26. The root module, nested transport modules, and standalone example
  modules use the 1.26 module floor as of 2026-06-26. The maintainer still
  needs to confirm this floor before signing the tag.

Module scope:
- Root module tag: `v0.1.0`.
- Nested modules are not included in the root tag. `rtpav` and `webrtcav`
  require separate prefixed tags if released.
- Tag order remains root first, then `rtpav`, then `webrtcav`.

API surface:
- Added exported symbols: typed `BuildError` helpers/fields, `BuildLive`,
  task capability vocabulary, `Mux(name, destination)`, `Write(name, writer)`,
  `bundle.Describe`, inspect helpers, sync policy options, and media-time
  rebranch boundaries.
- Removed or renamed exported symbols: `ContextCloser`, `Observable`,
  `Controllable`, `Explainer`, `Inspectable`, `Mutable`, `MediaOption`,
  `InputOption`, `DestinationOption`, `InputStream`, `RecipePatch`,
  `TransformSpec`, legacy `BuildError.Details`, legacy
  `BuildError.Suggestions`, path-style writer destinations, same-handle
  destination grouping, and graph-node-prefix tap fallback.
- API-restraint links for additions: `docs/API_SURFACE.md`,
  `docs/SIMPLIFICATION_TARGET.md`, and `docs/V1_CREDIBILITY_AUDIT.md`.

Behavior changes:
- Normal `Build` returns the narrow `Task` lifecycle; callers that need
  inspection, watches, controls, or runtime mutation use `BuildLive`.
- Stream chains require explicit `.Decode()` before frame-domain consumers;
  `.Copy()` keeps packet-domain recipes packet preserving.
- Reused destination handles no longer imply grouping; shared outputs require
  explicit `Mux(name, destination)`.
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
  or explicit `BuildLive` capabilities.
- Replace `Writer`/path-style output spelling with `Write(name, writer)` or
  `URI(uri)` as appropriate.
- Replace repeated destination handles with `Mux(name, destination)` for shared
  mux/sink grouping.
- Switch structured-error callers from legacy details/suggestions fields to
  `Detail(key)`, `DetailLines()`, and `FixLines()`.
- Add explicit `.Decode()` before frame transforms, real encoders, frame taps,
  or decoded-frame sinks; use `.Copy()` for packet-preserving flows.

Evidence:
- Local root tests: `go test -p 1 ./...`.
- Local pure-Go tests: `CGO_ENABLED=0 go test -p 1 ./...`.
- Local static checks: `go vet ./...` and
  `go run honnef.co/go/tools/cmd/staticcheck@latest ./...`.
- Nested module tests: every `examples/*/go.mod`, `goavtest/expect`, `rtpav`,
  and `webrtcav` passed `go test -p 1 ./...`.
- Root dependency check: `go list -deps github.com/thesyncim/goav` had no
  adapter, container, `rtpav`, or `webrtcav` package leak.
- CI release workflow, signed-tag validation, race tests, govulncheck, and
  release-quality benchmark artifacts still need release-day evidence.

Deferred / not claimed:
- No production performance leadership claim is made from this local pass.
- Signed tag, GitHub release, race matrix, security scan, and long perf-lab
  artifacts remain release-day work.
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
- Root / rtpav / webrtcav / other:
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

## Release blocking rule

Do not cut v1 until the compatibility note above is filled for the tag, the
release workflow has validated the signed tag, the release decision item in
`docs/ROADMAP.md` has been closed by the maintainer, and every exception to
`docs/SIMPLIFICATION_TARGET.md` is written down.
