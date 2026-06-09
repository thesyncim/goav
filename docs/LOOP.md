# Working Loop

`goav` grows by recursive, evidence-driven slices:

1. Name the user workflow in one sentence.
2. Express it with the shortest fluent API that should feel natural.
3. Describe the explicit graph before adding runtime behavior.
4. Add one compiler pass, adapter, stage, or format boundary.
5. Keep hot-path data caller-owned and preallocated.
6. Add the narrow allocation, lifecycle, and event tests for that slice.
7. Remove any abstraction that does not make the next slice simpler.
8. Update `docs/PROGRESS.md` with evidence and the next pressure point.

Graph descriptions stay at the workflow level: named nodes, named routes, short
node details. Executor vocabulary must not leak into fluent APIs or graph specs.

## Compiler Rule

High-level features lower through recipe intent:

```text
Recipe API -> Intent -> compiler passes -> pipeline.Spec -> pipeline.Graph
```

The pass chain stays explicit and boring: validate intent, probe inputs,
resolve stream selectors, resolve formats/codecs, insert demux or depacketize
boundaries, insert decode/transforms/encode/mux, assign routes and buffer
policy, emit `pipeline.Spec`, build the graph from the same plan. A new
workflow adds planner data or a compiler pass, not another fixed matcher.
`Describe` and `Build` use the same resolved plan so the described and runnable
graphs stay equivalent. Fail early with actionable diagnostics; do not guess
across codec, format, or protocol boundaries.

## Growth Rule

Prefer one reusable contract over many parallel helpers: one result struct per
hot path, one registry per capability family, one intent compiler path, one
adapter package per integration, one generic `Codec` spec. Complex workflows
become compositions of existing pieces, not new special cases. Reusable flows
expand into the same recipe intent as hand-written steps, never a parallel
graph API.

## Stop Conditions

Pause a slice when any of these are unclear: which package owns the behavior;
who owns buffer lifetime; how loss/discontinuity/codec change/backpressure is
represented; how the graph is described and inspected; which allocation guard
proves the hot path. Clarify the contract before adding another implementation.
