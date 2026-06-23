# Errors

When a recipe is wrong, goav should tell the caller what failed, where it
failed, and how to fix it. Build, validation, attach, rebranch, and explain
paths use one structured shape for that job: `goav.BuildError`.

The contract is enforced by a source-scanning pin test (`errors_pin_test.go`):

- **Code**: a typed `errcode.Code` identifying the refusal class. Every
  code is an exported constant in the `errcode` package (the catalog), grouped
  by area with a one-line comment saying when it fires. Codes are stable;
  rendered text may improve. The type is an open string: external components
  emit their own vendor-prefixed codes through the same `BuildError` shape.
- **Operation / Node**: where it happened (`build stream`, `attach runtime
  branch`; the chain, branch, tap, or destination name).
- **Reason**: one line saying why, including actual vs expected where it
  applies (`mix arm "b" cannot be converted to the join format (audio 44.1kHz
  1ch s16 -> audio 48kHz 2ch s16)`).
- **Details**: machine-readable `key=value` facts (`codec=opus`,
  `format=webm`, `actual_shape=...`). Internal invariants that are not
  user-fixable carry a Details line explaining what happened instead of a fix.
- **Suggestions**: concrete fixes for user-fixable refusals, as real API
  calls: `add .Auto(shape.AllowResample())`, `insert .Resample(48000, 2)
  explicitly`, `encode the mixed audio first:
  goav.Mix(a, b).Encode(codec.Opus(...))`.
- **Cause**: a sentinel (`goav.ErrUnsupportedBuild`, `goav.ErrNilSink`,
  `pipeline.ErrBufferedMessageUnsafe`, ...) reachable through `errors.Is`.

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
        case errcode.EncodeAdapterMissing:
            // register an adapter, or fall back to Copy()
        case errcode.ShapeConversionRefused:
            // widen the .Auto(...) policy
        }
    }
    if errors.Is(err, goav.ErrUnsupportedBuild) {
        // any build-shape refusal
    }
}
```

The full checked code list lives in
[`docs/ERROR_CATALOG.md`](ERROR_CATALOG.md), generated from
[`errcode/errcode.go`](../errcode/errcode.go): stable, autocompletable
(`errcode.`), and greppable by value (`rg encode_missing`). Every current catalog row names coverage.
If a future row appears as `catalog-only`, the pin test fails until it gets a
bad recipe, rendered error coverage, fixed recipe guidance, sentinel/cause,
and test name.

## Runtime errors

Runtime paths keep the same floor: errors name the node and say what to do.
`Task.Control` validations reject bad values before delivering (`goav:
SetBitrate needs a positive rate in bits per second, got 0`) and wrap
`pipeline.ErrUnknownNode` for unknown targets; per-node control failures are
collected as `goav: control to "node": ...`. Attach/Rebranch refusals are full
`BuildError` values (the `runtime_branch_*` codes). Failed stream-rule
branch attachments surface as `av.EventAttachError` events carrying the stream id,
branch name, and cause. Join stages name the offending arm (`goav: audio mix
requires s16, got f32 on arm "b"`). Buffered payload safety backstops are also
structured at the task boundary: a mutable payload reaching `flow.CopyNever`
returns `errcode.BufferPayloadUnsafe` with the `CopyNever` branch names and
still matches `pipeline.ErrBufferedMessageUnsafe`; a payload larger than the
configured copy bounds returns `errcode.BufferPayloadTooLarge` and still
matches `pipeline.ErrMessageTooLarge`.

## Diagnostics

Explain reports reuse the same vocabulary for things that did NOT fail the
build: `plan.Diagnostic`/`plan.Decision` codes such as
`shape_conversion_inserted` and `shape_preference_ignored` are declared in the
same catalog.
