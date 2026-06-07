# goav

`goav` is a pure-Go realtime media runtime in progress. The public contract is
small:

```text
describe the media work once, then compile it into an inspectable graph
```

The front door is `From(input)`. Simple jobs stay simple; complex jobs are built
from the same few concepts.

## Vocabulary

- `Input`: where media comes from.
- `Stream`: the selected audio/video stream.
- `Operation`: decode, resize, resample, custom stage, encode, or copy.
- `Tap`: a stable typed attach point.
- `Branch`: a downstream chain from a stream point or tap.
- `Target`: a named destination group, such as a mux or sink group.
- `Endpoint`: the actual file, URI, writer, or sink.
- `Flow`: a reusable operation sequence, not a destination.
- `Task`: a running graph with attach/detach, events, stats, and taps.

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

Use `Target` when a destination needs a stable logical name; direct endpoints
remain the shortest spelling for one-off outputs.

Decode one WebRTC audio track to frames:

```go
return goav.From(goav.WebRTCTrack(track)).
    Audio().
    Decode().
    To(goav.SinkEndpoint(frames)).
    Run(ctx)
```

`SinkEndpoint` receives frames at decoded points and packets after `.Copy()` or
after an encoder such as `.Opus(...)`, `.VP8(...)`, or `.VP9(...)`. Packet
streams can fan out to mux endpoints and packet sinks from the same encoded or
copied stream.

Resize and encode one video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    VP9(2_000_000).
    To(goav.FileOutput("preview.ivf", preview)).
    Run(ctx)
```

These examples use formats available in `goav.Default()` today. WebM, Ogg, WAV,
and Y4M belong in adapters, not hidden core magic.

## Adapter-Backed Workflows

The next examples show the shape `goav` is designed for when matching container
adapters are registered by the application or an adapter bundle.

### Branches And Targets

Use branches when one selected stream should become multiple downstream targets.
Targets are typed values, so normal recipes do not route by string labels. A
target can be a mux group or a sink endpoint.

```go
archive := goav.Target("archive", goav.FileOutput("archive.ivf", archiveFile))
preview := goav.Target("preview", goav.FileOutput("preview.ivf", previewFile))

return goav.From(input).
    Video().
    Decode().
    Tap("video.decoded").
    Branches(
        goav.Branch("archive").
            Resize(1920, 1080).
            VP9(4_000_000).
            To(archive),
        goav.Branch("preview").
            Resize(640, 360).
            Do(frameMeter).
            Tap("video.preview.frames").
            VP8(600_000).
            To(preview),
    ).
    Run(ctx)
```

Omit `FromTap` when every branch starts from the current stream point. Use
`FromTap` when one branch should start from an earlier stable tap while another
continues from a later operation:

```go
thumbnail := goav.Target("thumbnail",
    goav.SinkEndpoint(goav.SinkFunc("thumbnail", saveFrame)),
)
web := goav.Target("web", goav.FileOutput("web.ivf", webFile))

return goav.From(input).
    Video().
    Decode().
    Tap("video.decoded").
    Resize(1280, 720).
    Tap("video.720p.frames").
    Branches(
        goav.Branch("thumbnail").
            FromTap("video.decoded").
            Resize(320, 180).
            To(thumbnail),
        goav.Branch("web").
            FromTap("video.720p.frames").
            VP9(2_000_000).
            To(web),
    ).
    Run(ctx)
```

Several branches can share one target when the container supports that group:

```go
web := goav.Target("web", goav.FileOutput("web.webm", webFile))

return goav.From(goav.FileInput("source.webm", in)).
    Video().
    Decode().
    Branches(
        goav.Branch("v720").
            Resize(1280, 720).
            VP9(2_000_000).
            To(web),
    ).
    Audio().
    Decode().
    Branches(
        goav.Branch("a96").
            Resample(48_000, goav.Stereo).
            Opus(96_000).
            To(web),
    ).
    Run(ctx)
```

Branches do not have to encode when the target is a sink endpoint. This is the
same operation chain, just ending in frame domain:

```go
thumbnail := goav.Target("thumbnail",
    goav.SinkEndpoint(goav.SinkFunc("thumbnail", saveFrame)),
)

return goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("thumbnail").
            Resize(320, 180).
            To(thumbnail),
    ).
    Run(ctx)
```

Recipe encode conveniences are strongest for Opus, VP8, and VP9. H264 and AV1
are receive/decode codec specs today; recipe encode support for them is still
work in progress.

### Flows

A flow is reusable ordered work: custom stages, taps, transforms, and an
optional terminal encoder. A branch owns the target.
When the reusable work should own the packet-to-frame boundary, start the flow
with `.Decode()` and apply it from a stream chain, packet branch, or packet tap.
`Tap(...)` publishes a frame attach point before encode and a packet attach
point after encode.

```go
voice := goav.AudioFlow("voice").
    Resample(16_000, goav.Mono).
    Tap("audio.voice.frames").
    OpusVoice()

archive := goav.AudioFlow("archive").
    Resample(48_000, goav.Stereo).
    OpusMusic()

voiceTarget := goav.Target("voice", goav.FileOutput("voice.ogg", voiceFile))
archiveTarget := goav.Target("archive", goav.FileOutput("archive.ogg", archiveFile))

return goav.From(goav.WebRTCTrack(audio)).
    Audio().
    Apply(voice).
    To(voiceTarget).
    Run(ctx)
```

Branch when the same media point needs several downstream chains:

```go
return goav.From(goav.WebRTCTrack(audio)).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceTarget),
        goav.Branch("archive").Apply(archive).To(archiveTarget),
    ).
    Run(ctx)
```

Flows can also be applied to runtime branches. The flow still owns only the
operation sequence; the branch owns the target or targets.

```go
record, err := task.Attach(ctx,
    goav.Branch("archive").
        FromTap("audio.decoded").
        Apply(archive).
        To(archiveTarget),
)
```

## Runtime Attach

Build a task when the application needs graph inspection, events, stats, or late
attachment. Place taps where future work may attach: after decode, after resize
or resample, after a custom stage, or after encode.

```go
web := goav.Target("web", goav.FileOutput("web.ivf", webFile))

task, err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Tap("video.720p.frames").
            VP9(2_000_000).
            To(web),
    ).
    Build(ctx)
if err != nil {
    return err
}

go func() { _ = task.Run(ctx) }()

shots, err := task.Attach(ctx,
    goav.Branch("screenshots").
        FromTap("video.720p.frames").
        Resize(320, 180).
        Tap("video.screenshot.frames").
        To(goav.SinkEndpoint(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
return task.Detach(ctx, shots)
```

Frame taps can also grow a late encoded endpoint:

```go
recording := goav.Target("recording", goav.FileOutput("recording.ogg", file))

record, err := task.Attach(ctx,
    goav.Branch("record-audio").
        FromTap("audio.decoded").
        Opus(96_000).
        To(recording),
)
if err != nil {
    return err
}
defer record.Close(ctx)
```

One `Attach` call can add several runtime branches atomically. Later branches
in the same call can use taps published by earlier branches:

```go
group, err := task.Attach(ctx,
    goav.Branch("sampler").
        FromTap("video.720p.frames").
        Resize(320, 180).
        Tap("video.sampled.frames").
        To(goav.SinkEndpoint(goav.SinkFunc("sampler", collectSample))),
    goav.Branch("screenshots").
        FromTap("video.sampled.frames").
        To(goav.SinkEndpoint(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

Reuse the same sink `Target` value inside a grouped attach when several branches
should feed one runtime observer:

```go
samples := goav.Target("samples",
    goav.SinkEndpoint(goav.SinkFunc("samples", collectSample)),
)

group, err := task.Attach(ctx,
    goav.Branch("left").FromTap("video.720p.frames").To(samples),
    goav.Branch("right").FromTap("video.720p.frames").To(samples),
)
```

The same rule works for mux targets: reuse one typed `Target` value when several
encoded packet branches should feed one late recording endpoint.

```go
recording := goav.Target("recording",
    goav.FileOutput("recording.webm", file),
)

group, err := task.Attach(ctx,
    goav.Branch("audio").FromTap("audio.encoded").Copy().To(recording),
    goav.Branch("video").FromTap("video.encoded").Copy().To(recording),
)
```

Packet taps can be copied into a late endpoint:

```go
record, err := task.Attach(ctx,
    goav.Branch("record-packets").
        FromTap("audio.encoded").
        Copy().
        To(goav.Target("recording", goav.FileOutput("recording.ogg", file))),
)
if err != nil {
    return err
}
defer record.Close(ctx)
```

Packet taps can also become late decoded frame branches:

```go
preview, err := task.Attach(ctx,
    goav.Branch("preview").
        FromTap("audio.packets").
        Decode().
        Tap("audio.decoded.preview").
        To(goav.SinkEndpoint(frames)),
)
if err != nil {
    return err
}
defer preview.Close(ctx)
```

`Task.Taps()` lists available attach points. `Attach` adds downstream sink
or endpoint branches to a running direct task graph without rebuilding upstream.
Late branches can apply flows, run custom `.Do(...)` stages, resize/resample
from frame taps, encode Opus/VP8/VP9 from frame taps, copy or decode from
packet taps, and write to one or more typed targets before exposing their own
`.Tap(name)` outlets for later attachments. Observer branches can use
`.Do(goav.FrameFunc(...)).Tap(name).To(goav.SinkEndpoint(...))` to both inspect
frames and publish a downstream attach point. H264 and AV1 recipe encoding
remain work in progress. A grouped attach rolls back the whole group if any
branch cannot be prepared or connected. Detaching a parent attachment also
removes dependent late branches anchored from its taps. `Attachment.Stats()`
reports only the branch-owned node counters; `Task.Stats()` reports the whole
graph. Runtime branches can share one typed sink or mux target value inside an
atomic attach group.
Taps declared after `.Opus(...)`, `.VP8(...)`, `.VP9(...)`, or `.Copy()` are
packet-domain taps.

## Explain And Inspect

`Explain(ctx)` reports the workflow: inputs, branches, targets, taps, stream
caps, operation output caps, adapter requirements with capability details,
warnings, and the planned graph.

```go
report, err := job.Explain(ctx)
for _, tap := range report.Taps {
    fmt.Println(tap.Name, tap.Domain, tap.MediaKind)
}
if err != nil {
    return err
}
```

`Describe()` returns the structured graph spec. Rendering is outside core; graph
generators consume the spec through one URI-style entry point.

```go
spec, err := job.Describe()
if err != nil {
    return err
}
uri, err := graphrender.RenderURI(spec, "goav:graph")
```

## Custom Components

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
    To(goav.SinkEndpoint(levels)).
    Run(ctx)
```

Use `PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` for metering,
analysis, preview, stats, and integration points. Full `pipeline.Source`,
`pipeline.Stage`, and `pipeline.Sink` components remain available through the
expert graph API.

## Custom Codecs

Custom codecs use the same recipe concepts as built-ins: register decoder or
encoder factories in the application runtime, then reference them with generic
`Codec` specs in streams, branches, or flows. Codec descriptors drive
capability checks, so incompatible known media or frame formats fail before
decoder/encoder allocation or graph mutation. Adapter authoring details live in
[`docs/ADAPTERS.md`](docs/ADAPTERS.md).
The reusable component catalog and allocation proof map live in
[`docs/COMPONENTS.md`](docs/COMPONENTS.md).

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
- Packet-preserving `Copy().To(...)`.
- Stream-scoped decode, custom stages, resize/resample, and Opus/VP8/VP9 encode.
- Packet-domain fanout from `.Copy()` or an encoder to both mux endpoints and
  `SinkEndpoint` packet observers.
- Planned packet-copy branches with `.Copy().Branches(...)`, sharing one stream
  selector without creating decoders.
- Typed `Branch`, `Target`, endpoint, and `Flow` composition.
- Runtime branch attachment from named taps with flows, custom stages,
  resize/resample from frame taps, late Opus/VP8/VP9 encode endpoints, packet
  copy endpoints, nested runtime taps, `Attachment.Close(ctx)`, and
  `Task.Detach(ctx, h)`.
- Custom decode/encode registration through `WithDecoder`, `WithEncoder`, and
  generic `Codec` specs.
- Structured `Explain(ctx)` reports and `Describe()` graph specs.
- Pion-based RTP/WebRTC receive boundaries.
- Pure-Go adapter hooks for codecs, containers, and filters.

Advanced notes live in `docs/`.
