# Custom filter

This module shows a third-party frame filter registered from outside the root
module. The filter implements `filter.Factory`, is registered with
`runconfig.WithFilter`, and is selected by the normal `.Resample(...)` grammar.

Copy this when a frame transform should be shape-aware, explainable, and usable
from both build-time recipes and runtime branches.

Run it:

```sh
go run .
```

Expected output:

```text
frames: [[1 1 2 2] [3 3]]
```

The checked output lives in `testdata/expected.txt`.

Failure example: changing the recipe to `.Resample(44100, 1)` makes the filter
return `filter.ErrUnsupportedFormat` because this toy implementation only
supports exact 2x S16 mono upsampling.
