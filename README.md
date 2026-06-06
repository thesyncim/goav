# goav

`goav` is a pure-Go realtime media runtime in progress. The public API is being
shaped around one idea:

```text
A beginner expresses what they want.
An intermediate user customizes streams, codecs, transforms, and outputs.
An expert can drop into the explicit graph.
```

The runtime still compiles jobs into inspectable media graphs. The first thing a
normal user sees is now a recipe, not graph wiring.

## 30-Second Examples

Record a WebRTC video track:

```go
task, err := goav.Record(
    goav.WebRTCTrack(track),
    goav.FileOutput("recording.ivf", file),
).Build(ctx)
if err != nil {
    return err
}
return task.Run(ctx)
```

Record audio and video tracks together:

```go
task, err := goav.From(goav.WebRTCTrack(audio)).
    And(goav.WebRTCTrack(video)).
    To(goav.FileOutput("call.webm", file)).
    Build(ctx)
```

Record an RTP packet reader:

```go
task, err := goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.FileOutput("recording.ivf", file),
).Build(ctx)
if err != nil {
    return err
}
return task.Run(ctx)
```

Remux or fan out a file-like input:

```go
task, err := goav.Record(
    goav.FileInput("input.ivf", in),
    goav.FileOutput("preview.ivf", preview),
).Build(ctx)
```

Build a small video ladder:

```go
task, err := goav.Transcode(goav.FileInput("input.webm", in)).
    Video("720p").Resize(1280, 720).VP9(2_000_000).To("web").
    Video("360p").Resize(640, 360).VP9(600_000).To("preview").
    Output("web", goav.FileOutput("web.webm", web)).
    Output("preview", goav.FileOutput("preview.webm", preview)).
    Build(ctx)
```

Inspect the graph data before running:

```go
job := goav.Record(
    goav.FileInput("input.ivf", in),
    goav.FileOutput("preview.ivf", preview),
)

spec, err := job.Describe()
if err != nil {
    return err
}
for _, edge := range spec.Edges {
    fmt.Printf("%s -> %s\n", edge.From, edge.To)
}
```

## Common Recipes

- `goav.Record(input, output)` records, remuxes, or fans out packet streams.
- `goav.From(input).To(output...)` is the generic recipe form.
- `goav.From(input).And(other).To(output)` records repeated RTP/WebRTC receive
  inputs through one shared output graph.
- `goav.From(input).Audio().Decode().To(goav.FrameSink(frames))` decodes one
  selected audio stream without manual selectors.
- `goav.From(input).Audio().Decode().Resample(16_000, goav.Mono).Opus(48_000).To(output)`
  resamples and encodes one selected audio stream.
- `goav.From(input).Video().Decode().Resize(1280, 720).VP9(2_000_000).To(output)`
  resizes and encodes one selected video stream.
- `goav.From(input).Audio().Decode().Do(meter).Opus(96_000).To(output)` adds a
  stream-local custom stage before encoding.
- A `From` stream recipe carries one `Audio()` or `Video()` chain; use
  `Transcode` when one input needs multiple branches.
- `goav.Decode(input, sink)` decodes one selected stream into a frame sink.
- `goav.Transcode(input)` builds named audio or video branches and outputs.
  Transcode branch `.To(...)` accepts either a named output label or an
  `OutputSpec` such as `goav.FileOutput(...)`; each branch must route to an
  output and currently carries at most one resize or resample transform.
  Branch names and output names must be unique; share one output by reusing its
  label in `.To(...)` on each branch.
  Resize dimensions, resample rates, and channel counts must be positive.
- `goav.WebRTCTrack(track)` adapts a Pion `TrackRemote` into the same realtime
  receive path as RTP.
- `goav.RTP(reader).Name("audio").Codec(goav.Opus())` describes live receive
  intent without making the caller wire depacketizers by hand for Opus, VP8,
  VP9, H264, or AV1; `reader` must be a non-nil Pion-backed packet reader.
- `goav.FileInput`, `goav.URI`, `goav.FileOutput`, and `goav.URIOutput` cover
  ordinary input and output declarations. `FrameSink` requires a non-nil sink,
  and `FileOutput` requires a writer. Output names are unique within a recipe.

If `Audio()` or `Video()` matches more than one stream, build errors list the
available streams and suggest `StreamID`, `StreamName`, or `StreamIndex(0)`.
Stream indexes are zero-based and must be non-negative.

Recipes compile into the existing runtime builder, so `Describe`, `Build`,
`Run`, task events, and graph specs stay the same.

## Choosing Codecs And Formats

Recipes carry intent: input kind, selected stream, target codec, transforms, and
outputs. The runtime chooses concrete adapters from its registries.

For small examples, the package-level recipes use `goav.Default()`. Applications
that need exact adapter control can pass a runtime explicitly:

```go
rt := goav.New(goav.WithFormatAdapter(ivf.Register))

task, err := goav.Record(
    goav.FileInput("input.ivf", in),
    goav.FileOutput("preview.ivf", preview),
    goav.UseRuntime(rt),
).Build(ctx)
```

The explicit registration path remains important for embedded builds and narrow
deployments. The recipe API is the stable front door as the default adapter
bundle grows.

Recipe encode conveniences currently target Opus, VP8, and VP9. H264 and AV1
codec specs are useful for receive, record, and decode paths while recipe encode
support continues to mature. Recipe encode bitrates cannot be negative, and
explicit sample-rate or channel overrides must be positive.

## Inspect The Graph

Every recipe can describe the graph it will build:

```go
spec, err := job.Describe()
if err != nil {
    return err
}
for _, node := range spec.Nodes {
    fmt.Printf("%s: %s\n", node.Name, node.Kind)
}
```

`pipeline.Spec` is the core graph object: structured nodes and edges only.
Text and diagram exporters live in the small `graphrender` utility package, so
generated output can evolve without becoming part of runtime composition.

```go
text := graphrender.Render(spec, graphrender.Text)
```

## Custom Processing

Use function adapters when you want a small hook without implementing the full
graph interfaces:

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

sink := goav.SinkFunc("frames", func(ctx context.Context, msg goav.Message) error {
    return collect(msg)
})
```

Packet, frame, event, and sink helpers are meant for metering, stats, preview,
analysis, and quick integration points. Function helpers return nil for nil
callbacks so recipes fail with ordinary stage or sink guidance before graph
compilation. Full stages are still available when a component needs explicit
lifecycle control.

## Expert Graph API

The graph engine remains the escape hatch:

```go
rt := goav.Default()

graph := rt.Graph()
src := graph.Source("source", source)
dec := graph.Stage("decode", decode)
recordOut := graph.Sink("record", record)
previewOut := graph.Sink("preview", preview)

graph.Connect(src.Stream("audio"), dec.In())
graph.Connect(dec.Out(), recordOut.In(), previewOut.In())

task, err := graph.Build(ctx)
```

This layer exposes named sources, stages, sinks, typed handles, route policies,
buffer policies, and graph specs. It is valuable for custom realtime systems,
but it is no longer the first API a normal record/transcode workflow has to
learn.

## Project Shape

The public contract is moving toward three layers:

```text
recipes       record, decode, transcode, analyze
intent graph  inputs, streams, transforms, outputs, policies
runtime graph sources, stages, sinks, routes, messages
```

Implemented today:

- recipe `Record`, `From`, `Decode`, and transcode-ladder builders;
- stream-scoped recipe builders for selected audio/video decode, custom stages,
  resize/resample transforms, and Opus/VP8/VP9 encode paths;
- file, URI, RTP, WebRTC track, codec, resize, resample, and output specs;
- one file-output constructor, `FileOutput`, instead of duplicate aliases;
- multi-input realtime recipes with `From(input).And(other...)`;
- actionable stream-selection diagnostics and first-stream `StreamIndex(0)`;
- `Describe` graph specs plus optional `graphrender` exporters;
- function adapters for packet, frame, event, and sink hooks;
- handle-based expert graph wiring through `Runtime.Graph()`;
- runtime graph compilers for remux/fanout, live RTP record/fanout, selected
  decode, selected encode, and shared-decode transcode plans;
- pure-Go RTP/WebRTC receive boundaries and optional codec/filter/format
  adapters.

Advanced implementation notes live in:

- `docs/ARCHITECTURE.md`
- `docs/USE_CASES.md`
- `docs/RTP_WEBRTC.md`
- `docs/ADAPTERS.md`
- `docs/PERFORMANCE.md`
- `docs/PROGRESS.md`
