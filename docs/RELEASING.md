# Releasing

Releases are tag-driven. CI can validate a tag and create the GitHub release,
but the maintainer owns the compatibility decision and signed tag. Treat this
as a release-day checklist: it tells you what evidence to refresh before a tag
exists, and what automation is expected after the tag is pushed.

## Before Tagging

1. Move notable `CHANGELOG.md` entries from `Unreleased` into a versioned
   section.
2. Run the local gates:

   ```sh
   CGO_ENABLED=0 go test ./...
   CGO_ENABLED=1 go test -race -count=1 . ./pipeline ./goavtest ./format ./cmd/goav ./ctlserver ./internal/launchctl ./graphrender
   CGO_ENABLED=0 go vet ./...
   staticcheck ./...
   govulncheck ./...
   test -z "$(gofmt -l .)"
   scripts/bench/run.sh
   PERF_RELEASE_QUALITY=true PERF_BENCHTIME=2000x PERF_SOAK_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_INTERVAL=100ms scripts/bench/perf-lab.sh
   ```

   `PERF_RELEASE_QUALITY=true` rejects fixed-count soak benchtimes; keep the
   soak knobs duration-based so the manifest cannot label a smoke run as
   release evidence. In release-quality mode those knobs feed wall-clock soak
   tests, and `PERF_GO_TEST_TIMEOUT` defaults to `0` so Go's default 10-minute
   test timeout does not kill hour-scale soaks; set it explicitly when you want
   a bounded watchdog. `PERF_LIVE_ROOM_CHURN_INTERVAL` paces the attach/detach
   churn loop and is recorded in the generated JSON.

3. Run nested modules and examples:

   ```sh
   for mod in goavtest/expect rtpav webrtcav playoutav; do
     (cd "$mod" && CGO_ENABLED=0 go test ./... && CGO_ENABLED=1 go test -race ./... && CGO_ENABLED=0 go vet ./...)
   done
   for mod in examples/*/go.mod; do
     dir="$(dirname "$mod")"
     (cd "$dir" && CGO_ENABLED=0 go build ./... && CGO_ENABLED=0 go test ./...)
   done
   ```

4. Confirm the minimum Go version in every module that will be tagged.
5. Write the compatibility note using `docs/COMPATIBILITY.md`: public API
   changes, behavior changes, adapter changes, performance methodology changes,
   migration notes, and deferred claims.

## Acceptance Gate Matrix

The signed tag should not be cut until each row has fresh evidence from CI or a
clean local runner. If a row is intentionally skipped, the release notes should
say why.

| Gate | Required evidence |
|---|---|
| Pure-Go runtime tests | `CGO_ENABLED=0 go test ./...` passes in the root module, plus pure-Go tests in `goavtest/expect`, `rtpav`, `webrtcav`, `playoutav`, and every `examples/*/go.mod` module. |
| Race coverage | `CGO_ENABLED=1 go test -race` passes for the governed runtime packages and nested transport modules. |
| Static analysis and formatting | `CGO_ENABLED=0 go vet ./...`, `staticcheck ./...`, `govulncheck ./...`, and `test -z "$(gofmt -l .)"` pass. |
| README and docs examples | `TestReadmeGoExamplesCompile`, the doc-citation pins in `docs_citation_contract_test.go`, package-doc smoke, and example-module tests pass. |
| Error catalog | `TestErrorCatalogDocMatchesErrcodeCatalog` and the acceptance tests prove every current errcode has catalog/docs/test ownership. |
| Hot-path allocations | Allocation guard tests and benchmark smoke pass without documented regressions. |
| Public API restraint | `docs/API_SURFACE.md` (the reviewed approved list), the PR template, and CI package-doc smoke show no ungoverned public package or undocumented export. Any new export needs the API-restraint checklist. |
| CI artifacts | Coverage, fuzz-smoke, benchmark/perf-lab, benchstat, CodeQL, govulncheck, and release artifacts are present for the release candidate. |
| Performance claims | `docs/PERFORMANCE.md` classifies claims as proven, measured, experimental, or not proven; release notes avoid comparative leadership without reproducible numbers; any release performance claim cites a perf-lab manifest generated with `PERF_RELEASE_QUALITY=true`. |

## Tags

Use signed tags when cutting an actual release:

```sh
git tag -s v0.1.0 -m "goav v0.1.0"
git push origin v0.1.0
```

Nested modules use prefixed tags:

```sh
git tag -s rtpav/v0.1.0 -m "rtpav v0.1.0"
git tag -s webrtcav/v0.1.0 -m "webrtcav v0.1.0"
git tag -s playoutav/v0.1.0 -m "playoutav v0.1.0"
```

Tag order follows dependencies: root first, then `rtpav`, `webrtcav`, and
`playoutav` when those modules are released.

Nested-module tags need the standard multi-module dance, because consumers
ignore `replace` directives: in-repo the nested `go.mod` files require the
placeholder `github.com/thesyncim/goav v0.0.0` plus `replace ... => ../` for
day-to-day development, and a tag cut in that state would be unresolvable for
everyone else. Before tagging a nested module: tag the root first, update the
nested `go.mod` to require the real root tag, drop the `replace` line, run the
module's tests against the published root, commit, then tag. The release
workflow refuses nested-module tags whose `go.mod` still carries a `replace`
or the `v0.0.0` placeholder. After tagging, the `replace` can return on main
for development convenience.

## Automation

`.github/workflows/release.yml` runs on release tags and on manual dispatch for
an existing tag. It validates the tag shape, verifies the signed tag, checks out
that tag, runs tests and vet in the tagged module directory, then creates a
GitHub release with generated notes.

For root tags (`vX.Y.Z`) the workflow also builds pure-Go `goav` CLI archives
for Linux and macOS on amd64/arm64. The CLI is a deliberately narrow demo and
smoke-test utility (`docs/CLI.md`) — the library is the product; the archives
exist so the release pipeline exercises real cross-platform builds. It uploads:

- `SHA256SUMS`
- `sbom-go-modules.json` from `go list -m -json all`
- `provenance.txt` with tag, commit, workflow run, and Go toolchain details
- one `*.buildinfo.txt` file per binary from `go version -m`

Nested-module tags create source releases only, but they still run tests and
vet in the matching nested module before release creation.

Manual dispatch defaults to a draft release. Tag pushes create ready releases;
use manual draft mode when rehearsing release notes before announcing.
