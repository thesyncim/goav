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
- `Chain`: selected media plus operations such as decode, resize, resample,
  custom stages, encode, or copy.
- `Tap`: a typed attach point created with `FrameTap` or `PacketTap`.
- `Branch`: a downstream chain from a chain point or tap.
- `Target`: a named mux or sink group.
- `Destination`: a file, URI, writer, object upload, or media sink.
- `Task`: a running graph with attach/detach, events, stats, and taps.

## 30-Second Examples

Packet-preserving RTP/WebRTC record:

```go
return goav.From(goav.RTP(video).Name("video").Codec(goav.VP8())).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Packet-preserving file fanout:

```go
return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(
        goav.File("archive.ivf", archive),
        goav.File("preview.ivf", preview),
    ).
    Run(ctx)
```

Use `Target` when a target needs a stable logical name; direct files, URIs,
and sinks remain the shortest spelling for one-off outputs.

Decode one WebRTC audio track to frames:

```go
return goav.From(goav.WebRTCTrack(track)).
    Audio().
    Decode().
    To(goav.Sink(frames)).
    Run(ctx)
```

`Sink` receives frames at decoded points and packets after `.Copy()` or an
encoder such as `.Opus(...)`, `.VP8(...)`, or `.VP9(...)`. Packet streams can
fan out to file targets and packet sinks from the same encoded or copied chain.

Resize and encode one video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    VP9(2_000_000).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

These examples use formats available in `goav.Default()` today. WebM, Ogg, WAV,
and Y4M belong in adapters, not hidden core magic.

## Composition Patterns

These examples show the shape `goav` is designed for as workflows grow. When an
example uses a container outside the default bundle, register the matching
adapter in the application runtime.

### Branches And Targets

Use branches when one selected stream should become multiple downstream targets.
Targets are typed values, so normal recipes do not route by string labels. A
target can be a mux group or a sink group.

```go
decoded := goav.FrameTap("video.decoded")
previewFrames := goav.FrameTap("video.preview.frames")

archive := goav.Target("archive", goav.File("archive.ivf", archiveFile))
preview := goav.Target("preview", goav.File("preview.ivf", previewFile))

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Branches(
        goav.Branch("archive").
            Resize(1920, 1080).
            VP9(4_000_000).
            To(archive),
        goav.Branch("preview").
            Resize(640, 360).
            Do(frameMeter).
            Tap(previewFrames).
            VP8(600_000).
            To(preview),
    ).
    Run(ctx)
```

Omit `From(...)` when every branch starts from the current chain point. Use a
typed tap when one branch should start from an earlier point while another
continues from a later operation:

```go
decoded := goav.FrameTap("video.decoded")
frames720p := goav.FrameTap("video.720p.frames")

thumbnail := goav.Target("thumbnail",
    goav.Sink(goav.SinkFunc("thumbnail", saveFrame)),
)
web := goav.Target("web", goav.File("web.ivf", webFile))

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Resize(1280, 720).
    Tap(frames720p).
    Branches(
        goav.Branch("thumbnail").
            From(decoded).
            Resize(320, 180).
            To(thumbnail),
        goav.Branch("web").
            From(frames720p).
            VP9(2_000_000).
            To(web),
    ).
    Run(ctx)
```

Several branches can share one target when the container supports that group.
This is the natural shape for WebM once a WebM adapter is registered:

```go
web := goav.Target("web", goav.File("web.webm", webFile))

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

Branches do not have to encode when the target is a sink. This is the
same operation chain, just ending in frame domain:

```go
thumbnail := goav.Target("thumbnail",
    goav.Sink(goav.SinkFunc("thumbnail", saveFrame)),
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

### Reuse

When chain operations repeat, extract a reusable flow. A flow owns only
operations. A branch owns the target.

```go
voiceFrames := goav.FrameTap("audio.voice.frames")

voice := goav.Flow("voice").Audio().
    Resample(16_000, goav.Mono).
    Tap(voiceFrames).
    OpusVoice()

archive := goav.Flow("archive").Audio().
    Resample(48_000, goav.Stereo).
    OpusMusic()

voiceTarget := goav.Target("voice", goav.File("voice.ogg", voiceFile))
archiveTarget := goav.Target("archive", goav.File("archive.ogg", archiveFile))

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
        From(goav.FrameTap("audio.decoded")).
        Apply(archive).
        To(archiveTarget),
)
```

## Runtime Attach

Build a task when the application needs graph inspection, events, stats, or late
attachment. Place taps where future work may attach: after decode, after resize
or resample, after a custom stage, or after encode.

```go
frames720p := goav.FrameTap("video.720p.frames")
screenshotFrames := goav.FrameTap("video.screenshot.frames")

web := goav.Target("web", goav.File("web.ivf", webFile))

task, err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Tap(frames720p).
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
        From(frames720p).
        Resize(320, 180).
        Tap(screenshotFrames).
        To(goav.Sink(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
return task.Detach(ctx, shots)
```

Frame taps can also grow a late encoded target:

```go
audioDecoded := goav.FrameTap("audio.decoded")
recording := goav.Target("recording", goav.File("recording.ogg", file))

record, err := task.Attach(ctx,
    goav.Branch("record-audio").
        From(audioDecoded).
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
frames720p := goav.FrameTap("video.720p.frames")
sampledFrames := goav.FrameTap("video.sampled.frames")

group, err := task.Attach(ctx,
    goav.Branch("sampler").
        From(frames720p).
        Resize(320, 180).
        Tap(sampledFrames).
        To(goav.Sink(goav.SinkFunc("sampler", collectSample))),
    goav.Branch("screenshots").
        From(sampledFrames).
        To(goav.Sink(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

Reuse the same sink `Target` value inside a grouped attach when several branches
should feed one runtime observer:

```go
frames720p := goav.FrameTap("video.720p.frames")
samples := goav.Target("samples",
    goav.Sink(goav.SinkFunc("samples", collectSample)),
)

group, err := task.Attach(ctx,
    goav.Branch("left").From(frames720p).To(samples),
    goav.Branch("right").From(frames720p).To(samples),
)
```

The same rule works for mux targets: reuse one typed `Target` value when several
encoded packet branches should feed one late recording target.

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.Target("recording",
    goav.File("recording.webm", file),
)

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
```

Packet taps can be copied into a late target:

```go
audioEncoded := goav.PacketTap("audio.encoded")

record, err := task.Attach(ctx,
    goav.Branch("record-packets").
        From(audioEncoded).
        Copy().
        To(goav.Target("recording", goav.File("recording.ogg", file))),
)
if err != nil {
    return err
}
defer record.Close(ctx)
```

Packet taps can also become late decoded frame branches:

```go
audioPackets := goav.PacketTap("audio.packets")
previewFrames := goav.FrameTap("audio.decoded.preview")

preview, err := task.Attach(ctx,
    goav.Branch("preview").
        From(audioPackets).
        Decode().
        Tap(previewFrames).
        To(goav.Sink(frames)),
)
if err != nil {
    return err
}
defer preview.Close(ctx)
```

`Task.Taps()` lists available attach points. `Attach` adds downstream sink or
target branches to a running direct task graph without rebuilding upstream.
Late branches can apply flows, run custom `.Do(...)` stages, resize/resample
from frame taps, encode Opus/VP8/VP9 from frame taps, copy or decode from
packet taps, and write to one or more typed targets before exposing their own
typed tap outlets for later attachments. Observer branches can use
`.Do(goav.FrameFunc(...)).Tap(goav.FrameTap(name)).To(goav.Sink(...))` to both inspect
frames and publish a downstream attach point. H264 and AV1 recipe encoding
remain work in progress. A grouped attach rolls back the whole group if any
branch cannot be prepared or connected. Detaching a parent attachment also
removes dependent late branches anchored from its taps. `Attachment.Stats()`
reports only the branch-owned node counters; `Task.Stats()` reports the whole
graph. Runtime branches can share one typed sink or mux target value inside an
atomic attach group.
Taps declared after `.Opus(...)`, `.VP8(...)`, `.VP9(...)`, or `.Copy()` are
packet-domain taps.

## Debug And Diagnostics

Debugging is ordinary composition. Put a typed tap at the point you want to
observe, call `Explain(ctx)` before opening resources, then attach a live branch
when the task is running.

```go
decoded := goav.FrameTap("audio.decoded")

job := goav.From(goav.WebRTCTrack(audio)).
    Audio().
    Decode().
    Tap(decoded).
    To(goav.Sink(playback))

report, err := job.Explain(ctx)
if err != nil {
    return err
}
for _, warning := range report.Warnings {
    log.Printf("goav plan warning code=%s node=%s msg=%s",
        warning.Code, warning.Node, warning.Message)
}
for _, tap := range report.Taps {
    log.Printf("goav tap name=%s domain=%s media=%s",
        tap.Name, tap.Domain, tap.MediaKind)
}

task, err := job.Build(ctx)
if err != nil {
    return err
}
defer task.Close()

go func() {
    for event := range task.Events() {
        log.Printf("goav event type=%s stream=%s reason=%s",
            event.Type, event.StreamID, event.Reason)
    }
}()

go func() {
    if err := task.Run(ctx); err != nil {
        log.Printf("goav stopped: %v", err)
    }
}()

levels, err := task.Attach(ctx,
    goav.Branch("levels").
        From(decoded).
        Do(goav.FrameFunc("rms", func(_ context.Context, frame *goav.Frame, emit goav.Emit) error {
            observeRMS(frame)
            return emit.Frame(frame)
        })).
        To(goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
            return nil
        }))),
)
if err != nil {
    return err
}
defer levels.Close(ctx)

ticker := time.NewTicker(time.Second)
defer ticker.Stop()

for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-ticker.C:
        graphStats := task.Stats()
        levelStats := levels.Stats()
        log.Printf("goav stats packets=%d frames=%d dropped=%d level_frames=%d",
            graphStats.Packets,
            graphStats.Frames,
            graphStats.Dropped,
            levelStats.Frames)
    }
}
```

This works the same for video probes, screenshot collectors, packet loss
diagnostics, late recording branches, and temporary preview sinks. Attachments
are removable, and their stats stay scoped to the nodes they own.

## Explain And Inspect

`Explain(ctx)` reports the workflow: inputs, branches, targets, taps, stream
caps, operation output caps, adapter requirements with capability details,
warnings, and the planned graph. Operation reports mark shared upstream work, so
branch splits after decode, resize, resample, custom stages, or taps are visible
without reading the graph directly.

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
    To(goav.Sink(levels)).
    Run(ctx)
```

Use `PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` for metering,
analysis, preview, stats, and integration points.

## Custom Destinations

Write muxed bytes anywhere that can provide an `io.WriteCloser` with
`goav.Writer(...)`. Use `goav.Object(...)` when the writer has explicit commit
and abort semantics, such as a multipart object-store upload. The destination
opens after goav has selected the format and streams, so object-store uploaders
can see the final target metadata. Transactional writers commit after successful
runs or detach, abort on build, runtime, or attach failure, and close exactly
once.
Normal application workflows should be expressible through declarative recipes.

```go
s3 := goav.Object("s3://bucket/call.ivf",
    func(ctx context.Context, info goav.TargetInfo) (goav.TransactionalDestinationWriter, error) {
        return uploader.Create(ctx, info.Name,
            uploader.ContentType(info.MIMEType),
            uploader.Metadata(info.Metadata),
        )
    },
    goav.Format(av.FormatIVF),
    goav.MIME("video/ivf"),
    goav.Metadata(av.Metadata{"kind": "call-recording"}),
)

return goav.From(input).
    Video().
    Copy().
    To(s3).
    Run(ctx)
```

Use `Target(name, destination)` when multiple branches should feed one named
mux or sink group.

The same destination option style works for built-in destinations:

```go
return goav.From(input).
    Copy().
    To(goav.File("", out, goav.Format(av.FormatIVF)))
```

## Custom Codecs

Custom codecs use the same recipe concepts as built-ins: register decoder or
encoder factories in the application runtime, then reference them with generic
`Codec` specs in streams, branches, or flows. Codec descriptors drive
capability checks, so incompatible known media or frame formats fail before
decoder/encoder allocation or graph mutation. Adapter authoring details live in
[`docs/ADAPTERS.md`](docs/ADAPTERS.md).

Opus, VP8, and VP9 are the full encode/decode recipe verticals. H264 and AV1
receive/decode paths are active while recipe encode remains guarded as work in
progress. Codec-specific knobs stay on the same `CodecSpec`:

```go
vp9 := goav.VP9(
    goav.Bitrate(2_000_000),
    goav.Config(myVP9EncoderConfig),
    goav.Param("deadline", "realtime"),
    goav.Control(myVP9Control),
)

return goav.From(input).
    Video().
    Decode(goav.Config(myVP9DecoderConfig)).
    Resize(1280, 720).
    Encode(vp9).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

Adapters decide which concrete config and control types they understand; the
public grammar stays Input, Chain, Tap, Branch, Target, and Task.
The reusable component catalog and allocation proof map live in
[`docs/COMPONENTS.md`](docs/COMPONENTS.md).

## Current Shape

Implemented now:

- `From(input)` as the public composition front door.
- Packet-preserving `Copy().To(...)`.
- Stream-scoped decode, custom stages, resize/resample, and Opus/VP8/VP9 encode.
- Packet-domain fanout from `.Copy()` or an encoder to both files and packet
  sinks.
- Planned packet-copy branches with `.Copy().Branches(...)`, sharing one stream
  selector without creating decoders.
- Typed `Tap`, `Branch`, `Target`, and reusable chain composition.
- Runtime branch attachment from typed taps with reusable flows, custom stages,
  resize/resample from frame taps, late Opus/VP8/VP9 encode targets,
  packet-copy targets, nested runtime taps, `Attachment.Close(ctx)`, and
  `Task.Detach(ctx, h)`.
- Custom decode/encode registration through `WithDecoder`, `WithEncoder`, and
  generic `Codec` specs.
- Structured `Explain(ctx)` reports and `Describe()` graph specs.
- Pion-based RTP/WebRTC receive boundaries.
- Pure-Go adapter hooks for codecs, containers, and filters.

Advanced notes live in `docs/`.
