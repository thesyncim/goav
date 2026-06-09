# Unify sources & destinations — nothing special, compose elegantly

The smell: RTP, file, custom, WebRTC are over-specialized. `InputSpec` has three
fields (`input`/`rtp`/`source`); the build branches on them **57 times** across 5
files and runs *separate compile paths* (`compileRTPDecodeEncodeToOutput` vs
`compileDecodeEncodeToOutput`). Destinations are the same: `File`/`URI`/`Object`/
`Sink` special-cased. A new source/sink type means touching the core.

## Target: one contract per side, built-ins are implementations

```text
Source  : opens to a running pipeline source + its streams.   RTP, file, custom,
          WebRTC all implement it. From(src), Mix(src...) take any Source.
Destination : opens to a pipeline sink (bytes/mux/frames).    File, URI, Object,
          Sink all implement it.
```

The runtime opens any Source/Destination uniformly — **no branch on kind**. New
kinds (a generator, a protocol, an uploader) just implement the contract: no core
change. That is the elegance and composability — every input/output composes the
same way, and the public surface shrinks to the contract + a few constructors.

## Migration (incremental, each slice green)

1. **DONE — single source-opening seam.** `InputSpec.openGraphSource(ctx,
   *builder) (pipeline.Source, []av.Stream, MediaDomain, error)` — every input
   kind resolves through one method; callers (Mix today) never branch on kind.
   Returns *all* streams so the caller selects what it needs (composable). RTP
   returns a clear `source_unsupported` until folded in (step 2).
2. Fold RTP (and WebRTC) into `openGraphSource` (reuse the recipe's
   `rtpInputSpec`→`rtpInput` conversion). Then `Mix`/`From` take any input kind.
3. Collapse `InputSpec`'s `input`/`rtp`/`source` fields behind the opener; route
   the main build (`compileDecodeEncodeToOutput` / `compileRTPDecodeEncodeToOutput`)
   through the one seam, deleting the parallel RTP path. Removes the 57 branches.
4. Same on the output side: one `openGraphSink(ctx, *builder, streams)` seam;
   collapse `File`/`URI`/`Object`/`Sink` special-casing.
5. Surface: the built-in constructors return the contract type; consider moving
   protocol-specific ones (RTP/WebRTC) toward adapter packages so the core grammar
   only knows `Source`/`Destination`.
