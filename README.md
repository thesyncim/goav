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

Record audio and video tracks together when the selected runtime has a WebM
muxer:

```go
return goav.From(goav.WebRTCTrack(audio)).
    And(goav.WebRTCTrack(video)).
    To(goav.FileOutput("call.webm", file)).
    Run(ctx)
```

Build a small VP9 ladder when the selected runtime has WebM and VP9 encode
adapters:

```go
return goav.Transcode(goav.FileInput("input.webm", in)).
    Video("720p").Resize(1280, 720).VP9(2_000_000).To("web").
    Video("360p").Resize(640, 360).VP9(600_000).To("preview").
    Output("web", goav.FileOutput("web.webm", web)).
    Output("preview", goav.FileOutput("preview.webm", preview)).
    Run(ctx)
```

Reuse audio branches when the selected runtime has the matching input and
output adapters:

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

## Common Recipes

- `goav.Record(input, output...)` records, remuxes, or fans out packet streams.
- `goav.From(input).To(output...)` is the generic recipe form.
  These packet-preserving forms write to `FileOutput` or `URIOutput`; decoded
  frame sinks use `Decode` or a stream-scoped `Audio()`/`Video()` chain.
- `goav.From(input).And(other).To(output)` records repeated RTP/WebRTC receive
  inputs through one shared output graph; explicit realtime input names must be
  unique.
- `goav.From(input).Audio().To(goav.FrameSink(frames))` decodes one selected
  audio stream without manual selectors.
- `goav.From(input).Audio().Resample(16_000, goav.Mono).Opus(48_000).To(output)`
  resamples and encodes one selected audio stream.
- `goav.From(input).Video().Resize(1280, 720).VP9(2_000_000).To(output)`
  resizes and encodes one selected video stream.
- `goav.From(input).Audio().Do(meter).Opus(96_000).To(output)` adds a
  stream-local custom stage before encoding.
- `goav.AudioFlow(name)` and `goav.VideoFlow(name)` define reusable stream
  fragments. Apply one with `.Audio().Apply(flow)` or route several encoded
  branches with `.Audio().Tee(flow.To(output), ...)`.
- `goav.From(input).Video().OnCodecChange(goav.RealtimeCodecChangePolicy())`
  names today's live receive policy: rebind compatible replacement streams,
  request video keyframes, drop until sync, and fail on different decoder
  codecs.
- A `From` stream recipe carries one `Audio()` or `Video()` chain; use
  stream-local `.To(...)` outputs there, and use `Transcode` when one input
  needs multiple branches. A stream chain sends decoded frames to frame sinks
  or encoded packets to file/URI outputs, not both. Declare `.Do(...)`,
  `.Resize(...)`, or `.Resample(...)` before the one terminal encoder, then
  attach outputs with `.To(...)`.
- `goav.Decode(input, goav.FrameSink(frames))` decodes an unambiguous stream
  into a frame sink; use the stream-scoped `From(...).Audio()` or
  `From(...).Video()` shape when selection matters.
- `goav.Transcode(input)` builds named audio or video branches and outputs.
  Transcode branch `.To(...)` accepts output labels, and each label is defined
  once with `.Output(label, goav.FileOutput(...))` or `URIOutput`; each branch
  must route to a muxed output and currently carries at most one resize or
  resample transform.
  Audio and video branches can share one output label when they should be muxed
  into the same file.
  Branch names are required and unique; output labels are required and unique,
  and each branch lists each output once. Share one output by reusing its label
  in `.To(...)` on each branch. Branches decode implicitly, transforms come
  before one terminal encoder, and resize dimensions, resample rates, and
  channel counts must be positive. Branches currently choose concrete Opus,
  VP8, or VP9 recipe encoders; `Auto()` and `Copy()` fail early.
- `goav.WebRTCTrack(track)` adapts a Pion `TrackRemote` into the same realtime
  receive path as RTP.
- `goav.RTP(reader).Name("audio").Codec(goav.Opus())` describes live receive
  intent without making the caller wire depacketizers by hand for Opus, VP8,
  VP9, H264, or AV1; `reader` must be a non-nil Pion-backed packet reader.
  Raw RTP recipes require `.Codec(...)`; `WebRTCTrack(track)` derives that
  intent from Pion track metadata. Unknown WebRTC codecs fail before graph
  build with supported-codec guidance.
  Single-stream readers can provide the stream ID; `.Name(...)` gives the
  graph and stream a stable label when the reader metadata is not enough.
  Unsupported RTP codec intents fail with supported-codec guidance first;
  custom RTP payload adapters stay an advanced path.
- `goav.FileInput`, `goav.URI`, `goav.FileOutput`, and `goav.URIOutput` cover
  ordinary input and output declarations. `FrameSink` requires a non-nil sink,
  and `FileOutput` requires a writer. Writer-only outputs need a filename,
  `.MIME(...)`, URI, or explicit `.Format(...)` so the container is not guessed
  from nothing. Inputs can also use `.MIME(...)` when a reader or URI lacks a
  useful name. Output names are unique within a recipe.

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
