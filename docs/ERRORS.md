# Errors

When a recipe is wrong, goav should tell the caller what failed, where it
failed, and how to fix it. Build, validation, attach, rebranch, and explain
paths use one structured shape for that job: `goav.BuildError`.

The contract is enforced by a source-scanning pin test (`errors_pin_test.go`):

- **Family**: a typed `errcode.Family` identifying the stable broad class
  (`destination`, `runtime_branch`, `codec`, ...). Switch on this when a caller
  wants category-level handling.
- **Code**: a typed `errcode.Code` identifying the detailed diagnostic leaf.
  Codes are exported constants in the `errcode` package and may grow within a
  family before v1 as diagnostics become more precise. Applications should
  switch on `BuildError.Family` first, then inspect `Code` only when they need
  a specific leaf. The type is an open string: external components emit their
  own vendor-prefixed codes through the same `BuildError` shape.
- **Operation / Node**: where it happened (`build stream`, `attach runtime
  branch`; the chain, branch, tap, or destination name).
- **Reason**: one line saying why, including actual vs expected where it
  applies (`mix arm "b" cannot be converted to the join format (audio 44.1kHz
  1ch s16 -> audio 48kHz 2ch s16)`).
- **Fields / Details**: typed `goav.Detail` facts for applications, rendered
  as legacy `key=value` details for humans (`codec=opus`, `format=webm`,
  `actual_shape=...`). Internal invariants that are not user-fixable carry a
  detail explaining what happened instead of a fix.
- **Fixes / Suggestions**: typed `goav.Fix` values for tools, plus rendered
  suggestion text for humans. Fix messages should be real API calls:
  `add .Auto(shape.AllowResample())`, `insert .Resample(48000, 2) explicitly`,
  `encode the mixed audio first: goav.Mix(a, b).Encode(codec.Opus(...))`.
  A fix may carry a `goav.RecipePatch` hint for applications that can offer
  automatic recipe edits.
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
        switch buildErr.Family {
        case errcode.FamilyCodec:
            // register an adapter, or choose a codec family fallback
        case errcode.FamilyShape:
            // inspect the detailed code below
        }
        switch buildErr.Code {
        case errcode.EncodeAdapterMissing:
            // register an adapter, or fall back to Copy()
        case errcode.ShapeConversionRefused:
            // widen the .Auto(...) policy
        }
        if codecValue, ok := buildErr.Detail("codec"); ok {
            if codecID, ok := codecValue.(av.CodecID); ok {
                _ = codecID // choose a runtime or fallback by codec
            }
        }
    }
    if errors.Is(err, goav.ErrUnsupportedBuild) {
        // any build-shape refusal
    }
}
```

The full checked code list lives in
[`docs/ERROR_CATALOG.md`](ERROR_CATALOG.md), generated from
[`errcode/errcode.go`](../errcode/errcode.go): autocompletable
(`errcode.`), greppable by value (`rg encode_missing`), and grouped under a
stable family. Every current catalog row names coverage.
If a future row appears as `catalog-only`, the pin test fails until it gets a
bad recipe, rendered error coverage, fixed recipe guidance, sentinel/cause,
and test name.

## Runtime errors

Runtime paths keep the same floor: errors name the node and say what to do.
`Controllable.Control` validations reject bad values before delivering (`goav:
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
