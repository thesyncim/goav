# Working Loop

`goav` should grow by recursive, evidence-driven slices. Each slice starts with
the simplest user-facing expression, compiles it to an explicit graph, proves the
hot path, then folds the lesson back into the tracker.

The loop is intentionally small:

1. Name the user workflow in one sentence.
2. Express it with the shortest fluent API that should feel natural.
3. Describe the explicit graph before adding runtime behavior.
4. Add one private compiler, adapter, stage, or format boundary.
5. Keep hot-path data caller-owned and preallocated.
6. Add the narrow allocation, lifecycle, and event tests for that slice.
7. Remove any abstraction that does not make the next slice simpler.
8. Update `docs/PROGRESS.md` with evidence and the next pressure point.

Graph descriptions should stay at the workflow level: named nodes connected by
named routes, with short node details when they make the workflow easier to
read. Stream and event matching are options on a route. Lower-level executor
vocabulary should not leak into fluent APIs or graph specs unless a future
advanced stage truly needs it.

## Compiler Rule

High-level features must lower through recipe intent first. The desired path is:

```text
Recipe API
  -> Intent
  -> compiler passes
  -> pipeline.Spec
  -> pipeline.Graph
```

The pass chain should stay explicit and boring:

- validate intent
- probe inputs
- resolve stream selectors
- resolve formats and codecs
- insert demux or depacketize boundaries
- insert decode, transforms, encode, and mux
- assign routes and buffer policy
- emit `pipeline.Spec`
- build the runnable graph from the same plan

The current fixed graph compilers are migration scaffolding. A new fixed matcher
is allowed only as a temporary bridge for one proven workflow, and it should
shrink into an intent pass as soon as the shared compiler can express it.

`Describe` and `Build` must use the same resolved plan so the described graph
and runnable graph stay equivalent. Unsupported combinations should fail early
with actionable diagnostics that preserve the unsupported-build sentinel where
compatibility requires it. Do not guess across codec, format, or protocol
boundaries.

## Growth Rule

Prefer one reusable contract over many parallel helpers:

- One result struct per hot path.
- One explicit registry per capability family.
- One intent compiler path for recipe workflows.
- One adapter package per codec/container integration.
- One test fixture per boundary when possible.

Complex workflows should become compositions of existing compiler/stage pieces,
not new special cases.

Reusable flows/subflows are allowed when they name a repeated stream chain and
make a complex recipe shorter. They should expand into the same recipe intent as
hand-written stream steps, not create a parallel graph API.

## Stop Conditions

Pause a slice before adding code when any of these are unclear:

- Which package owns the behavior.
- Who owns the buffer lifetime.
- How loss, discontinuity, codec change, or backpressure is represented.
- How the graph will be described and inspected.
- Which allocation guard proves the hot path.

The next useful action is then to clarify the contract, not to add another
implementation path.
