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
- `Stream`: which media stream is selected.
- `Operation`: decode, resize, resample, custom stage, encode, or copy.
- `Tap`: a typed attach point created with `FrameTap` or `PacketTap`.
- `Branch`: downstream operations from a stream point or tap.
- `Destination`: a file, URI, writer, object upload, media sink, or shared mux/sink group.
- `Flow`: a reusable operation sequence.
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

Reuse the same destination value when several branches should feed one mux or
sink group.

Decode one WebRTC audio track to frames:

```go
return goav.From(goav.WebRTCTrack(track)).
    Audio().
    Decode().
    To(goav.Sink(frames)).
    Run(ctx)
```

`Sink` receives frames at decoded points and packets after `.Copy()` or
`.Encode(goav.Opus(...))`, `.Encode(goav.VP8(...))`, or
`.Encode(goav.VP9(...))`. Packet streams can
fan out to file destinations and packet sinks from the same encoded or copied
stream point.

Resize and encode one video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    Encode(goav.VP9(goav.Bitrate(2_000_000))).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

These examples use formats available in `goav.Default()` today. WebM, Ogg, WAV,
and Y4M belong in adapters, not hidden core magic.

## Composition Patterns

These examples show the shape `goav` is designed for as workflows grow. When an
example uses a container outside the default bundle, register the matching
adapter in the application runtime.

### Branches And Destinations

Use branches when one selected stream should become multiple downstream
destinations. Destinations are typed values, so normal recipes do not route by
string labels. Reusing one destination value creates a mux group or sink group.

```go
decoded := goav.FrameTap("video.decoded")
previewFrames := goav.FrameTap("video.preview.frames")

archive := goav.File("archive.ivf", archiveFile)
preview := goav.File("preview.ivf", previewFile)

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Branches(
        goav.Branch("archive").
            Resize(1920, 1080).
            Encode(goav.VP9(goav.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Resize(640, 360).
            Do(frameMeter).
            Tap(previewFrames).
            Encode(goav.VP8(goav.Bitrate(600_000))).
            To(preview),
    ).
    Run(ctx)
```

Omit `From(...)` when every branch starts from the current stream point. Use a
typed tap when one branch should start from an earlier point while another
continues from a later operation:

```go
decoded := goav.FrameTap("video.decoded")
frames720p := goav.FrameTap("video.720p.frames")

thumbnail := goav.Sink(goav.SinkFunc("thumbnail", saveFrame))
web := goav.File("web.ivf", webFile)

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
            Encode(goav.VP9(goav.Bitrate(2_000_000))).
            To(web),
    ).
    Run(ctx)
```

Several branches can share one destination when the container supports that group.
This is the natural shape for WebM once a WebM adapter is registered:

```go
web := goav.File("web.webm", webFile)

return goav.From(goav.FileInput("source.webm", in)).
    Video().
    Decode().
    Branches(
        goav.Branch("v720").
            Resize(1280, 720).
            Encode(goav.VP9(goav.Bitrate(2_000_000))).
            To(web),
    ).
    Audio().
    Decode().
    Branches(
        goav.Branch("a96").
            Resample(48_000, goav.Stereo).
            Encode(goav.Opus(goav.Bitrate(96_000))).
            To(web),
    ).
    Run(ctx)
```

Branches do not have to encode when the destination is a sink. This is the
same operation sequence, just ending in frame domain:

```go
thumbnail := goav.Sink(goav.SinkFunc("thumbnail", saveFrame))

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

Branch buffers are branch-local. Use blocking when a branch must preserve every
message, and dropping modes for realtime previews or diagnostics:

```go
return goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("archive").
            Buffer(goav.Blocking(128)).
            Encode(goav.VP9(goav.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Buffer(goav.DropOldest(3)).
            Resize(640, 360).
            To(preview),
        goav.Branch("latest").
            Buffer(goav.Latest()).
            To(goav.Sink(goav.SinkFunc("latest", inspect))),
    ).
    Run(ctx)
```

Recipe encode conveniences are strongest for Opus, VP8, and VP9. H264 and AV1
are receive/decode codec specs today; recipe encode support for them is still
work in progress.

### Reuse

When operations repeat, extract a reusable flow. A flow owns only
operations. A branch owns the destination.

```go
voiceFrames := goav.FrameTap("audio.voice.frames")

voiceCodec := goav.Opus(
    goav.Bitrate(32_000),
    goav.Channels(goav.Mono),
)
archiveCodec := goav.Opus(
    goav.Bitrate(128_000),
    goav.Channels(goav.Stereo),
)

voice := goav.Flow("voice").Audio().
    Resample(16_000, goav.Mono).
    Tap(voiceFrames).
    Encode(voiceCodec)

archive := goav.Flow("archive").Audio().
    Resample(48_000, goav.Stereo).
    Encode(archiveCodec)

voiceOut := goav.File("voice.ogg", voiceFile)
archiveOut := goav.File("archive.ogg", archiveFile)

return goav.From(goav.WebRTCTrack(audio)).
    Audio().
    Apply(voice).
    To(voiceOut).
    Run(ctx)
```

Use a direct stream when one reusable flow feeds one destination. Branch when the
same media point needs several downstream operation sequences:

```go
return goav.From(goav.WebRTCTrack(audio)).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

Flows can also be applied to runtime branches. The flow still owns only the
operation sequence; the branch owns the destination or destinations.

```go
record, err := task.Attach(ctx,
    goav.Branch("archive").
        From(goav.FrameTap("audio.decoded")).
        Apply(archive).
        To(archiveOut),
)
```

## Runtime Attach

Build a task when the application needs graph inspection, events, stats, or late
attachment. Place taps where future work may attach: after decode, after resize
or resample, after a custom stage, or after encode.

```go
frames720p := goav.FrameTap("video.720p.frames")
screenshotFrames := goav.FrameTap("video.screenshot.frames")

web := goav.File("web.ivf", webFile)

task, err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Tap(frames720p).
            Encode(goav.VP9(goav.Bitrate(2_000_000))).
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

Frame taps can also grow a late encoded destination:

```go
audioDecoded := goav.FrameTap("audio.decoded")
recording := goav.File("recording.ogg", file)

record, err := task.Attach(ctx,
    goav.Branch("record-audio").
        From(audioDecoded).
        Encode(goav.Opus(goav.Bitrate(96_000))).
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

Reuse the same sink destination value inside a grouped attach when several
branches should feed one runtime diagnostic sink:

```go
frames720p := goav.FrameTap("video.720p.frames")
samples := goav.Sink(goav.SinkFunc("samples", collectSample))

group, err := task.Attach(ctx,
    goav.Branch("left").From(frames720p).To(samples),
    goav.Branch("right").From(frames720p).To(samples),
)
```

The same rule works for mux destinations: reuse one destination value when
several encoded packet branches should feed one late recording.

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.File("recording.webm", file)

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
```

Packet taps can be copied into a late destination:

```go
audioEncoded := goav.PacketTap("audio.encoded")

record, err := task.Attach(ctx,
    goav.Branch("record-packets").
        From(audioEncoded).
        Copy().
        To(goav.File("recording.ogg", file)),
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

`Task.Taps()` lists available attach points. `Attach` adds downstream
destination branches to a running direct task graph without rebuilding upstream.
Late branches can apply flows, run custom `.Do(...)` stages, resize/resample
from frame taps, encode Opus/VP8/VP9 from frame taps, copy or decode from
packet taps, and write to one or more typed destinations before exposing their own
typed tap outlets for later attachments. Diagnostic branches can use
`.Do(goav.FrameFunc(...)).Tap(goav.FrameTap(name)).To(goav.Sink(...))` to both inspect
frames and publish a downstream attach point. H264 and AV1 recipe encoding
remain work in progress. A grouped attach rolls back the whole group if any
branch cannot be prepared or connected. Detaching a parent attachment also
removes dependent late branches anchored from its taps. `Attachment.Snapshot()`
reports the branch-owned diagnostic view; `Task.Snapshot()` returns one
point-in-time view of graph stats, stable taps, and active runtime branches.
Runtime branches can share one typed sink or mux destination value inside an atomic
attach group.
Taps declared after `.Encode(...)` or `.Copy()` are
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
        state := task.Snapshot()
        activeBranches := 0
        levelFrames := uint64(0)
        for _, branch := range state.Branches {
            if branch.State == "attached" {
                activeBranches++
            }
            if branch.Name == "levels" {
                levelFrames = branch.Stats.Frames
            }
        }
        log.Printf("goav stats packets=%d frames=%d dropped=%d branches=%d level_frames=%d",
            state.Stats.Packets,
            state.Stats.Frames,
            state.Stats.Dropped,
            activeBranches,
            levelFrames)
    }
}
```

This works the same for video probes, screenshot collectors, packet loss
diagnostics, late recording branches, and temporary preview sinks. Attachments
are removable, and snapshots keep their stats scoped to the nodes they own.

## Explain And Inspect

`Explain(ctx)` reports the workflow: inputs, branches, destinations, taps, stream
shapes, operation output shapes, adapter requirements with capability details,
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

Use `Source` when your application already owns packet production and wants to
enter the same recipe grammar as files, RTP, and WebRTC.

```go
input := goav.Source("generated",
    goav.PacketShape(av.MediaAudio, av.CodecOpus,
        goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for packet := range packets {
            if err := push.Packet(&packet); err != nil {
                return err
            }
        }
        return push.EOS()
    },
)

return goav.From(input).
    Audio().
    Copy().
    To(goav.Sink(packetSink)).
    Run(ctx)
```

Frame sources use the same constructor with `FrameShape` and do not allocate a
decoder.

```go
frames := goav.Source("pcm",
    goav.FrameShape(av.MediaAudio,
        goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for frame := range decoded {
            if err := push.Frame(&frame); err != nil {
                return err
            }
        }
        return push.EOS()
    },
)

return goav.From(frames).
    Audio().
    To(goav.Sink(levels)).
    Run(ctx)
```

Packet and frame-domain custom sources participate in the same stream, branch,
destination, explain, and runtime graph path as built-in inputs.

Event-only sources route directly to sinks.

```go
events := goav.Source("diagnostics",
    goav.EventShape(),
    func(ctx context.Context, push goav.SourcePush) error {
        if err := push.Event(goav.Event{Type: av.EventStats}); err != nil {
            return err
        }
        return push.EOS()
    },
)

return goav.From(events).
    To(goav.Sink(stats)).
    Run(ctx)
```

## Custom Destinations

Write muxed bytes anywhere that can provide an `io.WriteCloser` with
`goav.Writer(...)`. Use `goav.Object(...)` when the writer has explicit commit
and abort semantics, such as a multipart object-store upload. The destination
opens after goav has selected the format and streams, so object-store uploaders
can see the final destination metadata. Transactional writers commit after successful
runs or detach, abort on build, runtime, or attach failure, and close exactly
once.
Normal application workflows should be expressible through declarative recipes.
Use `goav.Custom(name, provider)` when a package owns a reusable destination
provider; the returned destination value is still the stable routing handle.

```go
s3 := goav.Object("s3://bucket/call.ivf",
    func(ctx context.Context, info goav.DestinationInfo) (goav.TransactionalDestinationWriter, error) {
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

Reuse one destination value when multiple branches should feed one mux or sink
group.

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
progress. Structural media facts stay on `Shape(...)`; codec behavior stays on
the `CodecSpec`. That keeps width, height, pixel/sample format, framerate/FPS,
sample rate, and channel layout separate from bitrate, rate control, quality,
profiles, deadlines, adapter configs, params, and controls:

```go
vp9 := goav.VP9(
    goav.Bitrate(2_000_000),
    goav.FPS(30),
    goav.KeyframeInterval(60),
    goav.Profile("0"),
    goav.Config(myVP9EncoderConfig),
    goav.Param("deadline", "realtime"),
    goav.Control(myVP9Control),
)

return goav.From(input).
    Video().
    Decode(goav.Config(myVP9DecoderConfig)).
    Resize(1280, 720).
    Shape(goav.Shape(goav.ShapeFramerate(30, 1))).
    Encode(vp9).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

`Shape(...)` annotates the current media point; it is not an escape hatch around
operation contracts. The compiler still checks each step in order, so an encoder
must consume frames, `Copy()` must consume packets, and resize/resample must
consume matching decoded media. File, URI, writer, and object destinations consume
packet-domain media; use `goav.Sink(...)` when a branch should end as frames.

Adapters decide which concrete config and control types they understand; the
public grammar stays Input, Stream, Operation, Tap, Branch, Destination, Flow,
and Task.
The reusable component catalog and allocation proof map live in
[`docs/COMPONENTS.md`](docs/COMPONENTS.md).

## Current Shape

Implemented now:

- `From(input)` as the public composition front door.
- Packet-preserving `Copy().To(...)`.
- Stream-scoped decode, custom stages, resize/resample, and Opus/VP8/VP9 encode.
- `Shape(...)` annotations for structural media facts such as framerate/FPS
  without adding one-off branch verbs, with operation-by-operation shape
  validation.
- Packet-domain fanout from `.Copy()` or an encoder to both files and packet
  sinks.
- Planned packet-copy branches with `.Copy().Branches(...)`, sharing one stream
  selector without creating decoders.
- Typed `Tap`, `Branch`, `Destination`, and reusable operation composition.
- Runtime branch attachment from typed taps with reusable flows, custom stages,
  resize/resample from frame taps, late Opus/VP8/VP9 encode destinations,
  packet-copy destinations, nested runtime taps, `Attachment.Close(ctx)`, and
  `Task.Detach(ctx, h)`.
- Task snapshots with graph stats, stable taps, and active runtime branch states.
- Custom decode/encode registration through `WithDecoder`, `WithEncoder`, and
  generic `Codec` specs.
- Structured `Explain(ctx)` reports and `Describe()` graph specs.
- Pion-based RTP/WebRTC receive boundaries.
- Pure-Go adapter hooks for codecs, containers, and filters.

Advanced notes live in `docs/`.
