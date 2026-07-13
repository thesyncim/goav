# V1 Credibility PR Notes

Historical campaign artifact; current release workflow guidance lives in `docs/RELEASING.md`.

Use this as the release-readiness PR body or review note for the documentation,
evidence, and automation work. It is intentionally written as a human review
map: what changed, why it matters, which checks ran, and what still needs a
maintainer decision before a tag.

## What Changed

- Shrank README into an adoption front door and moved advanced material into
  focused docs.
- Added machine-checked docs for the error catalog, operation grammar,
  extension cookbook, composability laws, performance methodology, repository
  trust, compatibility policy, release process, and v1 credibility audit.
- Added standalone external example modules for custom source, provider source,
  custom destination, custom filter, transactional writer, custom codec, custom
  join, and control-plane host, each with copyable README/test evidence and
  public-package-only imports.
- Added perf-lab benchmarks, benchmark artifact layout, PR benchstat support,
  SourcePush pressure evidence, and CI artifact upload.
- Upgraded CI/release automation with OS/Go matrix coverage, CodeQL,
  govulncheck, staticcheck, fuzz smoke, package-doc smoke, changelog hygiene,
  signed-tag validation, nested-module checks, SBOM, buildinfo, and provenance
  metadata.
- Recorded GitHub description/homepage/topics and no-release-yet posture in
  checked `docs/REPOSITORY_TRUST.md`.

## Why It Improves V1 Credibility

- New users get a small README and a clear next-doc map instead of a long proof
  notebook.
- Extension authors can copy runnable modules without reading root internals.
- Error and operation behavior is navigable and checked against source/tests.
- Performance claims are split into proven, measured, experimental, and not
  proven, with artifact-producing scripts for review and release evidence.
- Release and PR automation now encode trust work instead of relying on memory.
- Maintainers have concrete compatibility-note and repository-metadata evidence
  to review before any v0/v1 tag.

## Tests Run

- `go test ./...`
- `CGO_ENABLED=0 go vet ./...`
- external example module compile checks:
  `examples/custom-source`, `examples/provider-source`,
  `examples/custom-destination`,
  `examples/custom-filter`, `examples/transactional-writer`,
  `examples/custom-codec`, `examples/custom-join`, `examples/control-plane-host`
- root-module compile-only sweep with `go list ./...` + `go test -c`
- nested transport module compile-only sweep for `rtpav` and `webrtcav`
- nested transport module runtime checks for `rtpav` and `webrtcav`
- standalone example module runtime checks for every `examples/*/go.mod`
- workflow YAML parse for `.github/workflows/*.yml`
- package-doc smoke using `go list`
- `bash -n scripts/bench/*.sh`
- `gofmt -l` on touched Go files
- `git diff --check`
- `gh repo view` metadata check for description, pkg.go.dev homepage, topics,
  and no latest release.

Local full runtime `go test ./...` is now part of the safe-point check for this
line of work. Release acceptance still requires CI or a clean local runner for
the complete matrix in `docs/RELEASING.md`.

## Benchmarks Run

- No release-quality benchmark numbers are claimed from this local run.
- The PR adds benchmark/perf-lab scripts and CI artifact upload for
  `bench-results/baseline`, `bench-results/latency`, `bench-results/rss`,
  `bench-results/pressure`, `bench-results/control`, `bench-results/fanout`,
  `bench-results/container`, and `bench-results/pprof`.
- The perf-lab smoke covers p50/p95/p99 latency, heap/RSS, source.Push pressure,
  attach/detach under load, fanout sweeps, Matroska/WebM corpus paths, and
  real Opus encode/decode.
- CI smoke will run `go test -run '^$' -bench . -benchmem -benchtime=1x ./...`
  and `PERF_BENCHTIME=1x scripts/bench/perf-lab.sh`.

## New Risks

- CI is broader and may expose flakiness that was previously invisible,
  especially benchmark smoke, fuzz smoke, staticcheck, govulncheck, and nested
  module gates.
- Release automation now requires signed tags and checks the tagged module,
  which can fail releases that previously would have been allowed through.
- Performance artifacts are smoke evidence by default; reviewers should not
  treat CI timing as a release-quality performance claim.
- Repository metadata is now documented and should be updated deliberately if
  the package positioning changes.

## Deferred Work

- Cut an actual signed v0/v1 release after the maintainer confirms the minimum
  Go version and fills the compatibility note template in
  `docs/COMPATIBILITY.md`.
- Run full runtime acceptance in CI or a clean local environment.
- Produce longer same-machine perf-lab artifacts for tail latency, RSS/soak,
  pressure, attach/detach, fanout, container corpus, and broader real-codec
  throughput before making production performance claims.

## API Restraint

- No public API growth is required by this documentation-evidence pass.
- Existing grammar remains the front door:
  `From -> stream selection -> operations -> taps -> branches -> destinations -> task`.
- New evidence focuses on documentation, examples, tests, performance
  artifacts, and automation rather than adding workflow-specific APIs.
