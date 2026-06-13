# Releasing

Releases are tag-driven. CI can validate a tag and create the GitHub release,
but the maintainer owns the compatibility decision and signed tag.

## Before Tagging

1. Move notable `CHANGELOG.md` entries from `Unreleased` into a versioned
   section.
2. Run the local gates:

   ```sh
   CGO_ENABLED=0 go test ./...
   CGO_ENABLED=1 go test -race -count=1 . ./pipeline ./goavtest ./format ./cmd/goav ./ctl ./internal/launchctl ./graphrender
   CGO_ENABLED=0 go vet ./...
   staticcheck ./...
   govulncheck ./...
   test -z "$(gofmt -l .)"
   scripts/bench/run.sh
   scripts/bench/perf-lab.sh
   ```

3. Run nested modules and examples:

   ```sh
   for mod in rtpav webrtcav; do
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
clean local runner.

| Gate | Required evidence |
|---|---|
| Pure-Go runtime tests | `CGO_ENABLED=0 go test ./...` passes in the root module, plus pure-Go tests in `rtpav`, `webrtcav`, and every `examples/*/go.mod` module. |
| Race coverage | `CGO_ENABLED=1 go test -race` passes for the governed runtime packages and nested transport modules. |
| Static analysis and formatting | `CGO_ENABLED=0 go vet ./...`, `staticcheck ./...`, `govulncheck ./...`, and `test -z "$(gofmt -l .)"` pass. |
| README and docs examples | `TestReadmeGoBlocksCompileAsExternalConsumer`, doc pins, package-doc smoke, and example-module tests pass. |
| Error catalog | `error_catalog_pin_test.go`, `errors_pin_test.go`, and acceptance tests prove every current errcode has catalog/docs/test ownership. |
| Hot-path allocations | Allocation guard tests and benchmark smoke pass without documented regressions. |
| Public API restraint | `api_surface_pin_test.go`, `doc_pin_test.go`, `docs/API_SURFACE.md`, and the PR template show no ungoverned public package or undocumented export. Any new export needs the API-restraint checklist. |
| CI artifacts | Coverage, fuzz-smoke, benchmark/perf-lab, benchstat, CodeQL, govulncheck, and release artifacts are present for the release candidate. |
| Performance claims | `docs/PERFORMANCE.md` classifies claims as proven, measured, experimental, or not proven; release notes avoid comparative leadership without reproducible numbers. |

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
```

Tag order follows dependencies: root first, then `rtpav`, then `webrtcav`.

## Automation

`.github/workflows/release.yml` runs on release tags and on manual dispatch for
an existing tag. It validates the tag shape, verifies the signed tag, checks out
that tag, runs tests and vet in the tagged module directory, then creates a
GitHub release with generated notes.

For root tags (`vX.Y.Z`) the workflow also builds pure-Go `goav` CLI archives
for Linux and macOS on amd64/arm64. It uploads:

- `SHA256SUMS`
- `sbom-go-modules.json` from `go list -m -json all`
- `provenance.txt` with tag, commit, workflow run, and Go toolchain details
- one `*.buildinfo.txt` file per binary from `go version -m`

Nested-module tags create source releases only, but they still run tests and
vet in the matching nested module before release creation.

Manual dispatch defaults to a draft release. Tag pushes create ready releases;
use manual draft mode when rehearsing release notes before announcing.
