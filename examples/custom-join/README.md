# Custom Join Example

This module shows a third-party `goav.Join` convergence stage. The stage
interleaves S16 mono frames from two arms and then emits a normal joined stream
that can be sent to `.To(...)`, tapped, branched, or encoded.

Run it:

```sh
go run .
```

Expected output:

```text
joined: [[1] [2] [3] [4]]
```

The checked output lives in `testdata/expected.txt`.

Failure example: changing one arm to a video source makes the join planner
refuse the recipe before resources open because the stage contract accepts
audio frames only.
