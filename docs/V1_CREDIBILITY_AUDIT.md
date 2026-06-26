# V1 Credibility Audit

This is the reviewer map for the v1-credibility pass. Read it when you want to
check the claim "this repo is close to release-ready" without spelunking
through the whole tree. Each section points a requested improvement at the
files, tests, and workflows that carry the evidence.

## Front Door

- README is the adoption front door: what goav is, install, one 30-second
  example, common recipes, why goav, and deep links. Longer live/runtime and
  extension walkthroughs live in focused docs.
- Evidence: `README.md`, `TestReadmeStaysFrontDoorSized`,
  `TestReadmeFirstScreenAvoidsGraphInternals`,
  `TestReadmeGoBlocksCompileAsExternalConsumer`.

## Machine-Checked Docs

- Error catalog: every current errcode has coverage metadata for bad recipe,
  rendered-error assertion or golden-equivalent coverage, fix, sentinel/cause,
  and owning test.
- Operation reference: each front-door operation documents shape in/out,
  domain, inserted conversions, primary refusals, and runtime attach behavior.
- Extension cookbook: custom source, provider source, destination,
  transactional writer, custom filter, custom codec, custom join, and
  control-plane host examples.
- Evidence: `docs/ERROR_CATALOG.md`, `error_catalog_pin_test.go`,
  `docs/OPERATIONS.md`, `operations_doc_test.go`,
  `docs/EXTENSION_COOKBOOK.md`, `extension_cookbook_doc_test.go`.

## External Extension Proof

- Standalone external-style modules exist for custom source, provider source,
  custom destination, custom filter, transactional writer, custom codec, custom
  join, and a control-plane host.
- The adapter modules have `go.mod`, `README.md`, `main.go`, `main_test.go`,
  `testdata/expected.txt`, expected output, and a failure example. The
  control-plane host has its own `go.mod`, README, runnable host, socket/CLI
  tests, and capability-validation proof. All import only public packages.
- Tests use `goavtest` fixtures plus `goavtest/expect`; generic structural
  diffs come from `github.com/google/go-cmp/cmp`, while the custom layer stays
  goav-specific (`BuildError`, collector S16 samples, and golden output).
- Evidence: `examples/custom-source`, `examples/provider-source`,
  `examples/custom-destination`, `examples/custom-filter`,
  `examples/transactional-writer`, `examples/custom-codec`,
  `examples/custom-join`, `examples/control-plane-host`,
  `external_examples_module_test.go`.

## Performance Evidence

- The perf lab records p50/p95/p99 latency smoke, heap/sys/RSS smoke,
  drop/backpressure pressure smoke, attach/detach under load, 1/8/64/512
  fanout sweep, Matroska/WebM corpus smoke, real Opus encode/decode
  throughput, PR benchstat artifacts, and the checked `bench-results/`
  artifact layout.
- The performance doc separates proven, measured, experimental, and not-proven
  claims.
- Evidence: `performance_lab_test.go`, `scripts/bench/perf-lab.sh`,
  `scripts/bench/ci-compare.sh`, `bench-results/README.md`,
  `docs/PERFORMANCE.md`, `performance_lab_doc_test.go`.

## CI And Release Trust

- CI covers Linux/macOS and minimum/latest Go, pure-Go build, tests, coverage
  artifact, race subset, bench/perf artifacts, fuzz smoke, vet, package-doc
  smoke, staticcheck, govulncheck, nested transport modules, external examples,
  changelog hygiene, and signed-tag validation.
- CodeQL runs separately.
- Release automation verifies existing signed tags, runs checks in the tagged
  module directory, builds root CLI archives, and emits checksums, Go module
  SBOM, buildinfo, and provenance metadata.
- Repository metadata is explicit: GitHub description, pkg.go.dev homepage,
  focused topics, no-release-yet posture, and local trust files are recorded
  for reviewers.
- Evidence: `.github/workflows/ci.yml`, `.github/workflows/codeql.yml`,
  `.github/workflows/release.yml`, `docs/RELEASING.md`,
  `docs/REPOSITORY_TRUST.md`, `release_workflow_doc_test.go`,
  `repository_trust_doc_test.go`, `performance_lab_doc_test.go`.

## Composability And API Restraint

- Composability laws map direct stream equivalence, flow restraint,
  Build/Attach parity, Describe/Build equality, Explain diagnostics,
  destination-handle grouping, branch isolation, rollback, and external parity
  to executable tests.
- API restraint is recorded in the PR template and API surface docs: exported
  symbols require a grammar reason, shape facts, Explain/Describe/Snapshot
  representation, Build/Attach behavior, pre-resource failure mode, and
  external adapter/test path.
- Evidence: `docs/COMPOSABILITY_LAWS.md`, `composability_law_doc_test.go`,
  `docs/API_SURFACE.md`, `.github/PULL_REQUEST_TEMPLATE.md`,
  `docs/V1_CREDIBILITY_PR.md`, `release_workflow_doc_test.go`.

## Remaining Release Decision

- The code/docs automation is v1-credible, but the tag itself still needs the
  maintainer release decision: confirm the minimum Go version, write the
  compatibility note, and cut an actual signed release tag.
- Evidence: `docs/ROADMAP.md`, `docs/RELEASING.md`,
  `docs/COMPATIBILITY.md`.
