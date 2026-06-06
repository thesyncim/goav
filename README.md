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

Record an RTP/WebRTC video packet reader:

```go
task, err := goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.File("recording.ivf", file),
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

Inspect before running:

```go
job := goav.Record(
    goav.FileInput("input.ivf", in),
    goav.FileOutput("preview.ivf", preview),
)

report, err := job.Explain(ctx)
if err != nil {
    return err
}
fmt.Println(report.Text())
fmt.Println(report.Mermaid())
```

## Common Recipes

- `goav.Record(input, output)` records, remuxes, or fans out packet streams.
- `goav.From(input).To(output...)` is the generic recipe form.
- `goav.Decode(input, sink)` decodes one selected stream into a frame sink.
- `goav.Transcode(input)` builds named audio or video branches and outputs.
- `goav.RTP(reader).Name("audio").Codec(goav.Opus())` describes live receive
  intent without making the caller wire depacketizers by hand for common codecs.
- `goav.FileInput`, `goav.URI`, `goav.FileOutput`, and `goav.URIOutput` cover
  ordinary input and output declarations.

Recipes compile into the existing runtime builder, so `Describe`, `Build`,
`Run`, task events, and graph rendering stay the same.

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

## Inspect The Graph

Every recipe can explain the graph it will build:

```go
spec, err := job.Describe()
_ = spec.Render("text")
_ = spec.Render("dot")
_ = spec.Render("mermaid")
```

`Explain` adds the recipe intent beside the rendered graph so logs and tests can
show both the user request and the compiled plan.

## Custom Processing

Use function adapters when you want a small hook without implementing the full
graph interfaces:

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
    level := measure(frame)
    return emit.Event(av.Event{Type: av.EventStats, StreamID: frame.StreamID})
})

sink := goav.SinkFunc("frames", func(ctx context.Context, msg goav.Message) error {
    return collect(msg)
})
```

Packet, frame, event, and sink helpers are meant for metering, stats, preview,
analysis, and quick integration points. Full stages are still available when a
component needs explicit lifecycle control.

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
buffer policies, and rendered graph specs. It is valuable for custom realtime
systems, but it is no longer the first API a normal record/transcode workflow
has to learn.

## Project Shape

The public contract is moving toward three layers:

```text
recipes       record, decode, transcode, analyze
intent graph  inputs, streams, transforms, outputs, policies
runtime graph sources, stages, sinks, routes, messages
```

Implemented today:

- recipe `Record`, `From`, `Decode`, and transcode-ladder builders;
- file, URI, RTP, codec, resize, resample, and output specs;
- `Explain` reports over the existing text, DOT, and Mermaid graph renderers;
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
