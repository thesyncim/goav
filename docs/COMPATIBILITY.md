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

A v1 tag should promise that the Tier A surface in `docs/API_SURFACE.md` is
stable for normal users:

- the `From(...) -> stream selection -> operations -> taps -> branches ->
  destinations -> task` grammar;
- `Task`, runtime attach/rebranch/detach, watch/events/snapshot/stats, and
  structured `*goav.BuildError` refusals;
- the public vocabulary packages named Tier A in `docs/API_SURFACE.md`;
- documented compatibility behavior for Build, Explain, Attach, and Rebranch.

The extension seams remain documented and tested, but they may grow as new
adapters require additional capability. New exported symbols still require the
API-restraint checklist in `docs/API_SURFACE.md`.

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
release workflow has validated the signed tag, and the release decision item in
`docs/ROADMAP.md` has been closed by the maintainer.
