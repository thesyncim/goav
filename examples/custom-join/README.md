# Custom join

> Demonstrates custom `goav.Join` convergence — a governed pre-v1 feature, not
> the v1 front door. See [V1 scope](../../docs/V1_SCOPE.md).

This module shows a third-party `goav.Join` convergence stage. The stage
interleaves S16 mono frames from two arms and then emits a normal joined stream
that can be sent to `.To(...)`, tapped, branched, or encoded.

Copy this when several arms should converge in a way `Mix`, `Composite`, and
`Select` do not cover.

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
