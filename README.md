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
return goav.Record(
    goav.WebRTCTrack(track),
    goav.FileOutput("recording.ivf", file),
).Run(ctx)
```

Record an RTP packet reader:

```go
return goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.FileOutput("recording.ivf", file),
).Run(ctx)
```

Remux or fan out a file-like input:

```go
return goav.Record(
    goav.FileInput("input.ivf", in),
    goav.FileOutput("archive.ivf", archive),
    goav.FileOutput("preview.ivf", preview),
).Run(ctx)
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

The examples above use containers available in `goav.Default()` today. Workflows
that need WebM, Ogg, or tagged encoder adapters are called out explicitly below.

## Adapter-Backed Workflows

These examples are intentionally outside the 30-second section: they need
adapters beyond today's default IVF and Annex B containers. The API shape stays
the same; only the selected runtime's adapter set changes.

Record a live call into one muxed WebM output when WebM is available:

```go
return goav.From(goav.WebRTCTrack(audio)).
    And(goav.WebRTCTrack(video)).
    To(goav.FileOutput("call.webm", file)).
    Run(ctx)
```

Build a muxed audio/video ladder when WebM plus VP8/VP9 and Opus encode paths
are available. Output labels are mux groups; several branches can feed the same
label:

```go
return goav.Transcode(goav.FileInput("source.webm", in)).
    Video("v1080").Resize(1920, 1080).VP9(4_000_000).To("watch").
    Video("v720").Resize(1280, 720).VP9(2_000_000).To("watch", "mobile").
    Video("v360").Resize(640, 360).VP8(600_000).To("mobile").
    Audio("a128").Resample(48_000, goav.Stereo).Opus(128_000).To("watch").
    Audio("a48").Resample(48_000, goav.Stereo).Opus(48_000).To("mobile").
    Output("watch", goav.FileOutput("watch.webm", watch)).
    Output("mobile", goav.FileOutput("mobile.webm", mobile)).
    Run(ctx)
```

Reuse flow branches for live or file inputs. `Tee` is the planned media split:
one decoded stream feeds named flow branches, each with its own transform,
encoder, and output.

```go
voice := goav.AudioFlow("voice").
    Resample(16_000, goav.Mono).
    OpusVoice()

archive := goav.AudioFlow("archive").
    Resample(48_000, goav.Stereo).
    OpusMusic()

return goav.From(goav.FileInput("input.webm", in)).
    Audio().
    Tee(
        voice.To(goav.FileOutput("voice.ogg", voiceFile)),
        archive.To(goav.FileOutput("archive.ogg", archiveFile)),
    ).
    Run(ctx)
```

## Workflow Shapes

Record and fan out packet streams:

```go
return goav.Record(input, archive, preview).Run(ctx)
```

Decode one stream into frames:

```go
return goav.From(input).
    Audio().
    To(goav.FrameSink(frames)).
    Run(ctx)
```

Transform and encode one selected stream:

```go
return goav.From(input).
    Video().
    Resize(1280, 720).
    VP9(2_000_000).
    To(goav.FileOutput("preview.ivf", preview)).
    Run(ctx)
```

Insert custom work in the stream chain:

```go
return goav.From(input).
    Audio().
    Do(meter).
    Opus(96_000).
    To(output).
    Run(ctx)
```

Tee one decoded stream into reusable branches:

```go
archive := goav.VideoFlow("archive").VP8(2_000_000)
thumbs := goav.VideoFlow("thumbs").Resize(320, 180).VP8(300_000)

return goav.From(input).
    Video().
    Tee(
        archive.To(goav.FileOutput("archive.ivf", archiveFile)),
        thumbs.To(goav.FileOutput("thumbs.ivf", thumbsFile)),
    ).
    Run(ctx)
```

Compose several audio/video branches into muxed outputs:

```go
return goav.Transcode(input).
    Video("v720").Resize(1280, 720).VP9(2_000_000).To("main").
    Audio("a96").Resample(48_000, goav.Stereo).Opus(96_000).To("main").
    Output("main", output).
    Run(ctx)
```

Name live receive behavior explicitly:

```go
return goav.From(goav.WebRTCTrack(track)).
    Video().
    OnCodecChange(goav.RealtimeCodecChangePolicy()).
    To(goav.FrameSink(preview)).
    Run(ctx)
```

Runtime branches use a separate control-plane verb. `Attach` adds a new branch
to a built direct task graph while it is running, and the returned attachment
can be stopped later:

```go
task, err := job.Build(ctx)
if err != nil {
    return err
}
go task.Run(ctx)

screenshots, err := task.Attach(ctx,
    goav.Branch("screenshots").
        FromDecodedVideo().
        Do(screenshotStage).
        To(goav.SinkFunc("screenshots-out", collectScreenshot)),
)
if err != nil {
    return err
}

// Later, stop the branch without stopping the main task.
return screenshots.Stop(ctx)
```

Use `FromDecodedAudio(...)` or `FromDecodedVideo(...)` for the common raw frame
anchors, with the same stream selectors as `Audio(...)` and `Video(...)`.
Use `.From(node)` with `task.Describe()` when attaching to an expert graph node.
Buffered runtime attachments and live muxed `FileOutput` attachment fail
explicitly today; those need queue/worker and mux-output control-plane slices.

`Record` and `From(input).To(...)` are packet-preserving forms. A stream chain
such as `From(input).Audio()` or `From(input).Video()` sends decoded frames to
frame sinks or encoded packets to file/URI outputs, not both. Transcode outputs
are labels for mux groups; reuse a label from several branches when they should
land in the same output.

`goav.WebRTCTrack(track)` adapts a Pion `TrackRemote` into the same receive
path as `goav.RTP(reader).Name("audio").Codec(goav.Opus())`. Raw RTP recipes
require codec intent; WebRTC tracks derive it from Pion track metadata. Opus,
VP8, VP9, H264, and AV1 receive/decode intents are recognized, while recipe
encode conveniences are currently strongest for Opus, VP8, and VP9.

`FileInput`, `URI`, `FileOutput`, `URIOutput`, and `FrameSink` cover ordinary
boundaries. Writer-only outputs need a filename, MIME type, URI, or explicit
format so the container is not guessed from nothing. Output names are unique
within a recipe.

If `Audio()` or `Video()` matches more than one stream, build errors list the
available streams and suggest `StreamID`, `StreamName`, or `StreamIndex(0)`.
Stream indexes are zero-based and must be non-negative.
For RTP/WebRTC inputs described by recipe intent, and file/protocol inputs whose
format prober already reports streams, those stream-selection errors are
reported before receivers or demuxers are opened.

Use `.Run(ctx)` when the recipe is the whole job. Use `.Build(ctx)` when the
caller needs a `Task` for graph specs, events, or explicit lifecycle control.
`Describe()` resolves the same graph spec that `Build(ctx)` returns on the task.
`Task.Stats()` reports packet/frame/event totals, event counts by type, buffered
drops, and the last event observed by the graph.

## Choosing Codecs And Formats

Recipes carry intent: input kind, selected stream, target codec, transforms, and
outputs. The runtime chooses concrete adapters from its registries.

Package-level recipes use `goav.Default()`, which currently includes concrete
IVF and Annex B format adapters. Containers such as WebM or Ogg need a matching
adapter in the selected runtime; missing demuxers or muxers fail with actionable
diagnostics. Exact adapter registration remains available for embedded and
narrow deployments in [docs/ADAPTERS.md](docs/ADAPTERS.md).
Build-time recipe checks validate input demuxers and output muxers when a
container can be inferred from the input or output name, MIME type, URI, or
explicit output format. When an input prober already reports stream metadata,
build-time checks also validate obvious `Audio()`/`Video()` selection errors;
the selected codec can also be checked against registered decoder adapters.
Otherwise full stream discovery still belongs to graph build.
Inferred output formats are used internally when opening muxers; graph details
only show a format when the recipe explicitly used `.Format(...)`.

Recipe encode conveniences currently target Opus, VP8, and VP9. H264 and AV1
codec specs are useful for receive, record, and decode paths while recipe encode
support continues to mature. Recipe encode bitrates cannot be negative, and
explicit sample-rate or channel overrides must be positive.
Build-time recipe checks validate concrete encoder adapters for explicit encode
intents and concrete decoder adapters when a live RTP/WebRTC decode codec is
already known. `Describe()` remains graph inspection and does not require those
adapters to be present.
Resize and resample recipes also validate their filter adapters during build.

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

`pipeline.Spec` is the core graph object: structured nodes and edges only. The
optional `graphrender` package exposes a single URI-based render entry point,
so generated output can evolve without becoming part of runtime composition.

```go
text, err := graphrender.RenderURI(spec, "goav:graph")
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

This layer accepts `pipeline.Source`, `pipeline.Stage`, and `pipeline.Sink`
components, then connects them through typed handles, route policies, buffer
policies, and graph specs. It is valuable for custom realtime systems, but it
is no longer the first API a normal record/transcode workflow has to learn.
The reusable component catalog in [docs/COMPONENTS.md](docs/COMPONENTS.md)
describes the same building blocks recipes compile to and expert graphs wire
directly. Current component proofs cover file remux fanout, RTP Opus decode,
WebRTC TrackSet receive, custom stages, decoder EOS flush, and mux write
events.

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
  decode, selected encode, and shared-decode transcode recipes;
- pure-Go RTP/WebRTC receive boundaries and optional codec/filter/format
  adapters.

Advanced implementation notes live in:

- `docs/ARCHITECTURE.md`
- `docs/COMPONENTS.md`
- `docs/USE_CASES.md`
- `docs/RTP_WEBRTC.md`
- `docs/ADAPTERS.md`
- `docs/PERFORMANCE.md`
- `docs/PROGRESS.md`
