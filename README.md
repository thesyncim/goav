# goav

`goav` is a pure-Go realtime media runtime in progress. It is being shaped
around one public idea:

```text
describe the media work once, then compile it into an inspectable graph
```

The front door is `From(input)`. Simple jobs stay simple; complex jobs stay
composable.

## 30-Second Examples

Packet-preserving RTP/WebRTC record:

```go
return goav.From(goav.RTP(video).Name("video").Codec(goav.VP8())).
    Copy().
    To(goav.FileOutput("recording.ivf", file)).
    Run(ctx)
```

Packet-preserving file fanout:

```go
return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(
        goav.FileOutput("archive.ivf", archive),
        goav.FileOutput("preview.ivf", preview),
    ).
    Run(ctx)
```

Decode one stream to frames:

```go
return goav.From(goav.WebRTCTrack(track)).
    Audio().
    Decode().
    To(goav.FrameSink(frames)).
    Run(ctx)
```

Resize and encode one selected video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    VP9(2_000_000).
    To(goav.FileOutput("preview.ivf", preview)).
    Run(ctx)
```

These examples use containers available in `goav.Default()` today. WebM, Ogg,
WAV, and Y4M are adapter surface, not hidden core magic.

## Paths

Use `Paths` when one selected stream should become several encoded output
paths. A path can start after the operations already declared on the stream, so
splits are natural after decode, after resize or resample, after a custom
stage, or after any declared tap.

```go
return goav.From(input).
    Video().
    Decode().
    Tap("video.decoded").
    Resize(1920, 1080).
    Paths(
        goav.Path("archive").
            VP9(4_000_000).
            To("archive"),
        goav.Path("preview").
            Resize(640, 360).
            Do(frameMeter).
            Tap("video.preview.frames").
            VP8(600_000).
            To("preview"),
    ).
    Outputs(
        goav.Output("archive", goav.FileOutput("archive.ivf", archive)),
        goav.Output("preview", goav.FileOutput("preview.ivf", preview)),
    ).
    Run(ctx)
```

Reusable flows produce the same `PathSpec` values as ad hoc paths, so there is
one way to split:

```go
archive := goav.VideoFlow("archive").
    VP9(2_000_000)

preview := goav.VideoFlow("preview").
    Resize(640, 360).
    VP8(600_000)

return goav.From(goav.RTP(video).Name("video").Codec(goav.VP8())).
    Video().
    Decode().
    Paths(
        archive.To("archive"),
        preview.To("preview"),
    ).
    Outputs(
        goav.Output("archive", goav.FileOutput("archive.ivf", archiveFile)),
        goav.Output("preview", goav.FileOutput("preview.ivf", previewFile)),
    ).
    Run(ctx)
```

## Runtime Branches

Build a task when the application needs graph inspection, events, stats, or late
runtime attachment.

```go
task, err := goav.From(input).
    Video().
    Decode().
    Tap("video.decoded").
    Paths(
        goav.Path("720p").
            Resize(1280, 720).
            Tap("video.720p.frames").
            VP9(2_000_000).
            To("web"),
    ).
    Outputs(goav.Output("web", goav.FileOutput("web.ivf", web))).
    Build(ctx)
if err != nil {
    return err
}

go func() { _ = task.Run(ctx) }()

shots, err := task.Attach(ctx,
    goav.Branch("screenshots").
        FromTap("video.720p.frames").
        To(goav.SinkFunc("screenshots", collectScreenshot)),
)
if err != nil {
    return err
}

return task.Detach(ctx, shots)
```

`Task.Taps()` lists the stable outlets available for runtime attachment.
`Attach` adds a downstream stage/sink branch to a running direct task graph
without rebuilding upstream. `Attachment.Close(ctx)` or `Task.Detach(ctx, h)`
removes that branch. Late muxed outputs and buffered runtime attachment are
separate runtime slices.

## Adapter-Backed Workflows

Output labels are mux groups. Several encoded audio/video branches can feed the
same label when the selected runtime has the needed demuxer, muxer, codec, and
filter adapters.

```go
return goav.From(goav.FileInput("source.webm", in)).
    Video().
    Decode().
    Tap("video.decoded").
    Paths(
        goav.Path("v1080").
            Resize(1920, 1080).
            VP9(4_000_000).
            To("watch"),
        goav.Path("v360").
            Resize(640, 360).
            Do(frameMeter).
            VP8(600_000).
            To("mobile"),
    ).
    Audio().
    Decode().
    Tap("audio.decoded").
    Paths(
        goav.Path("a96").
            Resample(48_000, goav.Stereo).
            Opus(96_000).
            To("watch", "mobile"),
    ).
    Outputs(
        goav.Output("watch", goav.FileOutput("watch.webm", watch)),
        goav.Output("mobile", goav.FileOutput("mobile.webm", mobile)),
    ).
    Run(ctx)
```

Recipe encode conveniences are strongest for Opus, VP8, and VP9. H264 and AV1
are first-class receive/decode codec specs; their recipe encode paths are still
work in progress.

## Inspect And Explain

`Describe()` returns the structured graph spec. Rendering is outside core; graph
generators can consume the spec through one URI-style entry point.

```go
spec, err := job.Describe()
if err != nil {
    return err
}
uri, err := graphrender.RenderURI(spec, "goav:graph")
```

`Explain(ctx)` is workflow-level inspection: inputs, branches, taps, outputs,
planner decisions, adapter requirements, warnings, and the graph.

```go
report, err := job.Explain(ctx)
if err != nil {
    return err
}
for _, tap := range report.Taps {
    fmt.Println(tap.Name, tap.Domain, tap.MediaKind)
}
```

## Custom Processing

Small hooks should not require implementing the full graph interfaces.

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

return goav.From(input).
    Audio().
    Decode().
    Do(meter).
    Opus(96_000).
    To(output).
    Run(ctx)
```

Use `PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` for metering,
analysis, preview, stats, and integration points. Custom components are
orthogonal: a recipe can add a stage before encode, send decoded frames to a
custom sink, or attach a late sink from any declared tap on a running task.

```go
levels := goav.SinkFunc("levels", collectLevel)

return goav.From(input).
    Audio().
    Decode().
    Do(meter).
    To(goav.FrameSink(levels)).
    Run(ctx)
```

Full `pipeline.Source`, `pipeline.Stage`, and `pipeline.Sink` components remain
available through the expert graph API.

## Custom Codecs

Custom codecs use the same recipe concepts as built-ins: register a concrete
decoder or encoder factory on the runtime, then reference it with `Codec`.

```go
desc := goav.CodecDescriptor{
    ID:   "pcm_s16",
    Name: "PCM S16",
    Type: av.MediaAudio,
}

func newRuntime() goav.Runtime {
    return goav.New(
        goav.WithDefaults(),
        goav.WithDecoder(desc, pcmDecoderFactory{}),
        goav.WithEncoder(desc, pcmEncoderFactory{}),
    )
}

pcm := goav.Codec("pcm_s16", av.MediaAudio,
    goav.SampleRate(48_000),
    goav.Channels(goav.Stereo),
)

return goav.From(input).
    Audio().
    Decode().
    Encode(pcm).
    To(output).
    Run(ctx)
```

## Expert Graph API

`Runtime.Graph()` is the escape hatch for manual wiring.

```go
rt := goav.Default()

graph := rt.Graph()
src := graph.Source("source", source)
dec := graph.Stage("decode", decode)
out := graph.Sink("out", sink)

graph.Connect(src.Stream("audio"), dec.In())
graph.Connect(dec.Out(), out.In())

task, err := graph.Build(ctx)
```

The expert layer is still valuable for custom realtime systems. It is no longer
the first thing a normal record, decode, transcode, or analysis workflow has to
learn.

## Current Shape

Implemented now:

- `From(input)` as the public composition front door.
- packet-preserving `Copy().To(...)`;
- stream-scoped decode, custom stages, resize/resample, and Opus/VP8/VP9 encode;
- custom decode/encode registration through `WithDecoder`, `WithEncoder`, and
  generic `Codec` specs;
- custom stages, sinks, adapter hooks, and late runtime branch sinks as optional
  composition points instead of a separate workflow model;
- reusable `AudioFlow` and `VideoFlow` values that become `PathSpec` with
  `.To(label)`;
- grouped `Path(...)` output paths from one selected stream, with ordered custom
  stage, tap, transform, and encode steps;
- output groups through `.Outputs(goav.Output(label, output), ...)`;
- runtime branch attachment through `Task.Taps()` and `Branch(...).FromTap(...)`;
- structured `Explain(ctx)` reports with branch operations, taps, decisions, and
  adapter requirements;
- `Describe()` graph specs, with rendering kept outside core;
- Pion-based RTP/WebRTC receive boundaries;
- pure-Go adapter hooks for codecs, containers, and filters.

Advanced notes live in:

- `docs/ARCHITECTURE.md`
- `docs/COMPONENTS.md`
- `docs/USE_CASES.md`
- `docs/RTP_WEBRTC.md`
- `docs/ADAPTERS.md`
- `docs/PERFORMANCE.md`
- `docs/PROGRESS.md`
