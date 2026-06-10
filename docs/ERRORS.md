# Errors

goav raises one structured error from every build, validation, attach, and
explain path: `goav.BuildError`. The contract, enforced by a source-scanning
pin test (`errors_pin_test.go`):

- **Code** — a typed `goav.ErrorCode` identifying the refusal class. Every
  code is an exported constant in `errors.go` (the catalog), grouped by area
  with a one-line comment saying when it fires. Codes are stable; rendered
  text may improve.
- **Operation / Node** — where it happened (`build stream`, `attach runtime
  branch`; the chain, branch, tap, or destination name).
- **Reason** — one line saying why, including actual vs expected where it
  applies (`mix arm "b" cannot be converted to the join format (audio 44.1kHz
  1ch s16 -> audio 48kHz 2ch s16)`).
- **Details** — machine-readable `key=value` facts (`codec=opus`,
  `format=webm`, `actual_shape=...`). Internal invariants that are not
  user-fixable carry a Details line explaining what happened instead of a fix.
- **Suggestions** — concrete fixes for user-fixable refusals, as real API
  calls: `add .Auto(shape.AllowResample())`, `insert .Resample(48000, 2)
  explicitly`, `encode the mixed audio first:
  goav.Mix(a, b).Encode(codec.Opus(...))`.
- **Cause** — a sentinel (`goav.ErrUnsupportedBuild`, `goav.ErrNilSink`, ...)
  reachable through `errors.Is`.

One renderer produces one shape:

```
goav: cannot <operation> for <node>: <reason>
Details:
  - key=value
Suggestions:
  - the exact call to make
```

## Matching programmatically

```go
task, err := job.Build(ctx)
if err != nil {
    var buildErr *goav.BuildError
    if errors.As(err, &buildErr) {
        switch buildErr.Code {
        case goav.CodeEncodeAdapterMissing:
            // register an adapter, or fall back to Copy()
        case goav.CodeShapeConversionRefused:
            // widen the .Auto(...) policy
        }
    }
    if errors.Is(err, goav.ErrUnsupportedBuild) {
        // any build-shape refusal
    }
}
```

The full code list lives in [`errors.go`](../errors.go) — stable,
autocompletable (`goav.Code...`), and greppable by value (`rg encode_missing`).

## Runtime errors

Runtime paths keep the same floor: errors name the node and say what to do.
`Task.Control` validations reject bad values before delivering (`goav:
SetBitrate needs a positive rate in bits per second, got 0`) and wrap
`pipeline.ErrUnknownNode` for unknown targets; per-node control failures are
collected as `goav: control to "node": ...`. Attach/Rebranch refusals are full
`BuildError` values (the `runtime_branch_*` codes). Failed stream-rule
reactions surface as `av.EventAttachError` events carrying the stream id,
branch name, and cause. Join stages name the offending arm (`goav: audio mix
requires s16, got f32 on arm "b"`).

## Diagnostics

Explain reports reuse the same vocabulary for things that did NOT fail the
build: `info.Diagnostic`/`info.Decision` codes such as
`shape_conversion_inserted` and `shape_preference_ignored` are declared in the
same catalog.
