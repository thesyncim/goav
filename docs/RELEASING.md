# Releasing

Releases are signed, tag-driven, and automated by `.github/workflows/release.yml`.

## Pre-Release Checklist

1. Fold `changelog.d/` fragments into `CHANGELOG.md`:
   ```sh
   scripts/changelog-release.sh VERSION
   ```
2. Run local checks across root, nested modules, and examples:
   ```sh
   export GOEXPERIMENT=simd
   scripts/test-all-modules.sh --race
   staticcheck ./... && govulncheck ./... && test -z "$(gofmt -l .)"
   scripts/bench/baseline.sh check
   ```
3. For release-quality performance evidence, generate a perf-lab manifest (note `PERF_GO_TEST_TIMEOUT` defaults to `0` in release-quality mode):
   ```sh
   PERF_RELEASE_QUALITY=true PERF_BENCHTIME=2000x PERF_SOAK_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_INTERVAL=100ms scripts/bench/perf-lab.sh
   ```
4. Verify external consumer resolution against a published commit:
   ```sh
   bash scripts/check-consumers.sh PUBLISHED_CANDIDATE_SHA
   ```

## Module Pinning

Nested modules (`cameraav`, `rtpav`, `webrtcav`, `player`, `libcodec`, `gplcodec`, `goavtest/expect`) and examples (`examples`, `examples/gio-webrtc-showcase`, `examples/webrtc-runtime-ladder`) pin the root module at a resolvable version (with `replace` for local dev; `cmd/goav` has no `replace` so `go install ...@version` works).

Repin every module to a new root version in one command:

```sh
scripts/repin.sh v0.1.0-alpha.8
```

## Signing & Tag Order

Use signed, annotated tags (`git tag -s`):

1. Root first: `vX.Y.Z`
2. Nested modules in dependency order: `goavtest/expect/vX.Y.Z`, `cameraav/vX.Y.Z`, `rtpav/vX.Y.Z`, `webrtcav/vX.Y.Z`, `player/vX.Y.Z`, `libcodec/vX.Y.Z`, `gplcodec/vX.Y.Z`
3. CLI last: `cmd/goav/vX.Y.Z` (pins `cameraav`, `player`, `webrtcav`)

Before tagging a nested module, pin its `goav` dependencies to tags (not pseudo-versions), drop its `replace` directive, commit, and tag; `.github/workflows/release.yml` refuses nested tags with `replace` directives. Rehearse a release without tagging via `gh workflow run release.yml --ref BRANCH -f dry_run=true`.
