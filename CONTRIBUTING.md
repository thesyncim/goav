# Contributing

Thanks for helping make goav easier to adopt.

## Development Checks

Run the local gates that match CI before opening a pull request:

```sh
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go vet ./...
gofmt -w .
scripts/bench/run.sh
```

Use `staticcheck ./...` and `govulncheck ./...` when those tools are
installed. Transport modules are nested modules; run their checks from
`rtpav/` and `webrtcav/` when a change touches them.

Update `CHANGELOG.md` for user-visible work. Internal-only pull requests may
use the `skip-changelog` label; CI treats the label as an explicit reviewer
signal rather than silently accepting missing release notes.

## Public Surface

New exported identifiers are deliberate API changes. Before adding one, record:

- why existing grammar cannot express the feature
- the operation record it appends
- shape facts consumed and produced
- `Explain`, `Describe`, and `Snapshot` representation
- build and runtime attach behavior
- pre-resource failure mode
- external adapter or test path

If those answers require a separate workflow path, keep the feature out of the
front-door API until the shared planner can express it.

## Errors

Build, explain, attach, and validation refusals should use `*goav.BuildError`
with an `errcode.Code`, operation, node when available, reason, details,
suggestions for user-fixable failures, and an `errors.Is`-reachable cause when
there is a sentinel.

## Performance Claims

Do not add performance claims without evidence. Put enforced allocation
properties in tests, measured throughput in benchmarks, and unproven areas in
`docs/PERFORMANCE.md`.
