# Working Loop

`goav` should grow by recursive, evidence-driven slices. Each slice starts with
the simplest user-facing expression, compiles it to an explicit graph, proves the
hot path, then folds the lesson back into the tracker.

The loop is intentionally small:

1. Name the user workflow in one sentence.
2. Express it with the shortest fluent API that should feel natural.
3. Render the explicit graph before adding runtime behavior.
4. Add one private compiler, adapter, stage, or format boundary.
5. Keep hot-path data caller-owned and preallocated.
6. Add the narrow allocation, lifecycle, and event tests for that slice.
7. Remove any abstraction that does not make the next slice simpler.
8. Update `docs/PROGRESS.md` with evidence and the next pressure point.

Graph descriptions should stay at the workflow level: named nodes connected by
named connections, with short node details when they make the workflow easier to
read. Stream and event routing are options on a connection. Lower-level executor
vocabulary should not leak into fluent APIs or rendered specs unless a future
advanced stage truly needs it.

## Compiler Rule

High-level builder features must compile through private graph compilers. A new
compiler is allowed only when it owns one clear shape, for example:

```text
Input -> DemuxSource -> MuxStage...
Input -> DemuxSource -> stream select -> DecoderStage -> Sink
Input -> Decode -> Filter branches -> Encode -> Mux outputs
TrackRemote -> RTP source -> Depacketizer -> DecoderStage
```

The compiler must support both:

- `Describe`, so users can inspect text, DOT, and Mermaid graphs before running.
- `Build`, so the rendered graph and runnable graph stay equivalent.

Unsupported combinations should fail early with the existing unsupported-builder
error. Do not guess across codec, format, or protocol boundaries.

## Growth Rule

Prefer one reusable contract over many parallel helpers:

- One result struct per hot path.
- One explicit registry per capability family.
- One graph compiler per fluent workflow shape.
- One adapter package per codec/container integration.
- One test fixture per boundary when possible.

Complex workflows should become compositions of existing compiler/stage pieces,
not new special cases.

## Stop Conditions

Pause a slice before adding code when any of these are unclear:

- Which package owns the behavior.
- Who owns the buffer lifetime.
- How loss, discontinuity, codec change, or backpressure is represented.
- How the graph will be rendered.
- Which allocation guard proves the hot path.

The next useful action is then to clarify the contract, not to add another
implementation path.
