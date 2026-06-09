# goav

`goav` is a pure-Go realtime media runtime. The public contract is small:

```text
describe the media work once, then compile it into an inspectable graph
```

The front door is `From(input)`. Simple jobs stay simple; complex jobs are built
from the same few concepts.

## Vocabulary

- `Input`: where media comes from (file, URI, custom source, or any source
  provider via `goav.Input(...)` — `rtpav.Receive` for RTP, `webrtcav.Track`
  for WebRTC, or an external transport package).
- `Stream`: which media stream is selected (`.Audio()`, `.Video()`).
- `Tap`: a named attach point (`Tap`, or `FrameTap`/`PacketTap` to assert the domain).
- `Branch`: downstream operations from a stream point or tap.
- `Destination`: a file, URI, writer, object upload, media sink, or shared mux/sink group.
- `Flow`: a reusable operation sequence.
- `Task`: a running graph with attach/detach, events, snapshots, and live control.

Operations are not a separate noun: they are methods on the chain —
`.Decode()`, `.Copy()`, `.Resize()`, `.Resample()`, `.Do(stage)`, `.Encode(codec)`.

## 30-Second Examples

Packet-preserving RTP/WebRTC record:

```go
return goav.From(goav.Input(rtpav.Receive(video, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))).
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
return goav.From(goav.Input(webrtcav.Track(track))).
    Audio().
    Decode().
    To(goav.Sink(frames)).
    Run(ctx)
```

Resize and encode one video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

`goav.Default()` bundles the standard adapters: IVF, Annex B, Matroska, and WebM
containers; Opus, VP8, VP9, AV1, and H264 codec adapters; resize and resample
filters. Other containers need a registered adapter.

## Composition Patterns

### Branches And Destinations

Use branches when one selected stream should become multiple downstream
destinations. Destinations are typed values, so normal recipes do not route by
string labels. Reusing one destination value creates a mux group or sink group.

```go
decoded := goav.Tap("video.decoded") // domain inferred; FrameTap/PacketTap assert it

archive := goav.File("archive.ivf", archiveFile)
preview := goav.File("preview.ivf", previewFile)

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Branches(
        goav.Branch("archive").
            Resize(1920, 1080).
            Encode(codec.VP9(codec.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Resize(640, 360).
            Do(frameMeter).
            Encode(codec.VP8(codec.Bitrate(600_000))).
            To(preview),
    ).
    Run(ctx)
```

Omit `From(...)` when every branch starts from the current stream point. Use a
typed tap when one branch should start from an earlier point:

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
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Run(ctx)
```

Several branches sharing one destination form a mux group — the natural WebM
shape:

```go
web := goav.File("web.webm", webFile)

return goav.From(goav.FileInput("source.webm", in)).
    Video().
    Decode().
    Branches(
        goav.Branch("v720").
            Resize(1280, 720).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Audio().
    Decode().
    Branches(
        goav.Branch("a96").
            Resample(48_000, codec.Stereo).
            Encode(codec.Opus(codec.Bitrate(96_000))).
            To(web),
    ).
    Run(ctx)
```

A branch ending in a sink stays in frame domain — no encode required.

Branch buffers are branch-local. Use blocking when a branch must preserve every
message, and dropping modes for realtime previews or diagnostics:

```go
return goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("archive").
            Buffer(flow.Blocking(128)).
            Encode(codec.VP9(codec.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Buffer(flow.DropOldest(3)).
            Resize(640, 360).
            To(preview),
        goav.Branch("latest").
            Buffer(flow.Latest()).
            To(goav.Sink(goav.SinkFunc("latest", inspect))),
    ).
    Run(ctx)
```

### Reuse

When operations repeat, extract a reusable flow. A flow owns only operations; a
branch owns the destination.

```go
voice := goav.Flow("voice").Audio().
    Resample(16_000, codec.Mono).
    Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

archive := goav.Flow("archive").Audio().
    Resample(48_000, codec.Stereo).
    Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))

voiceOut := goav.File("voice.ogg", voiceFile)
archiveOut := goav.File("archive.ogg", archiveFile)

return goav.From(goav.Input(webrtcav.Track(audio))).
    Audio().
    Apply(voice).
    To(voiceOut).
    Run(ctx)
```

Use a direct stream when one reusable flow feeds one destination. Branch when the
same media point needs several downstream operation sequences:

```go
return goav.From(goav.Input(webrtcav.Track(audio))).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

Flows also apply to runtime branches attached from taps.

## Multi-Input

`From` is variadic. Several inputs feed one job, each chain narrows to its
input with `goav.InputName(...)`, and reusing one destination value muxes both
encoded streams into one shared container:

```go
camera := goav.Input(webrtcav.Track(videoTrack, rtpav.WithName("camera")))
mic := goav.Input(webrtcav.Track(audioTrack, rtpav.WithName("mic")))
out := goav.File("call.webm", file)

return goav.From(camera, mic).
    Video(goav.InputName("camera")).Decode().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out).
    Audio(goav.InputName("mic")).Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(out).
    Run(ctx)
```

When a selector is ambiguous, the build error lists candidates and suggests the
`InputName`, `StreamID`, `StreamName`, or `StreamIndex` narrowing to use.

## Mix, Composite, Select

Convergence is the dual of `Branches`: N source chains join into one stream.
`Mix` sums audio arms, `Composite` paints video arms onto a canvas at
`.Region(x, y)` offsets, and `Select` forwards exactly one live arm.

```go
return goav.Mix(
    goav.From(mic1).Audio(),
    goav.From(mic2).Audio(),
).Encode(codec.Opus(codec.Bitrate(96_000))).
    To(goav.File("mix.webm", out)).
    Run(ctx)
```

```go
return goav.Composite(
    goav.From(cam).Video().Region(0, 0),
    goav.From(screen).Video().Region(640, 0),
).Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.File("stage.webm", out)).
    Run(ctx)
```

Packet arms decode automatically before the join; mismatched audio arms
resample to the first arm's format. The join output is a normal stream point: it
takes `.Tap(...)`, `.Branches(...)`, and `.Encode(...).To(...)` like any chain.

`Select` switches live through the control plane — no node names:

```go
task, err := goav.Select(
    goav.From(cam1).Video(),
    goav.From(cam2).Video(),
).To(preview).Build(ctx)
// ... while running:
err = task.Control(ctx, goav.SelectActive("cam2"))
```

## Runtime Attach

Build a task when the application needs inspection, events, or late attachment.
Place taps where future work may attach.

```go
frames720p := goav.FrameTap("video.720p.frames")
web := goav.File("web.ivf", webFile)

task, err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Tap(frames720p).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
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
        To(goav.Sink(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
return task.Detach(ctx, shots)
```

Late branches can encode from frame taps, copy or decode from packet taps, and
publish their own taps for later attachments:

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.File("recording.webm", file)

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
```

`Task.Taps()` lists available attach points. One `Attach` call adds several
runtime branches atomically; later branches in the call can use taps published
by earlier ones, and branches can share one typed sink or mux destination value.
Reuse the same destination value inside a grouped attach to form one late
recording or diagnostic group. A grouped attach rolls back fully if any branch
fails; detaching a parent also removes dependent branches anchored from its
taps. Taps declared after `.Encode(...)` or `.Copy()` are packet-domain taps.
H264 and AV1 recipe encoding remain work in progress.

## Live Control

`task.Control(ctx, ...)` drives a running task without naming graph nodes.
Untargeted controls enter at the source boundary and ride the data path:

```go
err := task.Control(ctx, goav.Keyframe("video")) // reaches every live encoder for the stream
```

`.AtTap(name)` narrows a control to one tap's point in the graph;
`goav.Deliver(event)` hands a verbatim event to a stage (this is how
`SelectActive` switches a selector). A node-targeted form exists for expert
graphs only.

## Debug And Diagnostics

Debugging is ordinary composition. Put a typed tap at the point you want to
observe, call `Explain(ctx)` before opening resources, then attach a live branch
while the task is running.

```go
decoded := goav.FrameTap("audio.decoded")

job := goav.From(goav.Input(webrtcav.Track(audio))).
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

go func() { _ = task.Run(ctx) }()

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

state := task.Snapshot()
for _, branch := range state.Branches {
    if branch.State == info.BranchAttached && branch.Name == "levels" {
        log.Printf("goav levels frames=%d", branch.Stats.Frames)
    }
}
log.Printf("goav stats packets=%d frames=%d dropped=%d",
    state.Stats.Packets, state.Stats.Frames, state.Stats.Dropped)
```

`Task.Snapshot()` returns one point-in-time view with typed lifecycle states
(`info.TaskState`, `info.BranchState`, `info.DestinationState`), graph stats,
stable taps, and active runtime branches. `Attachment.Snapshot()` reports the
branch-owned view.
This works the same for video probes, screenshot collectors, packet loss
diagnostics, late recording branches, and temporary preview sinks.

## Explain And Inspect

`job.Explain(ctx)` reports the workflow before resources open: inputs, branches,
destinations, taps, stream shapes, operation output shapes, adapter requirements,
warnings, and the planned graph. `Describe()` returns the structured graph spec;
rendering lives outside core:

```go
spec, err := job.Describe()
if err != nil {
    return err
}
uri, err := graphrender.RenderURI(spec, "goav:graph")
```

## Custom Components

Small hooks should not require implementing the full graph interfaces. Use
`PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` for metering, analysis,
preview, stats, and integration points:

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

Use `Source` when your application already owns media production. Declare the
shape (`shape.Packet`, `shape.Frame`, or `shape.Event`) and push through
`goav.SourcePush`. Every push returns a `PushResult`: `Accepted` means a
downstream target queued the message, `Dropped` means a dropping buffer policy
deliberately shed it — normal realtime behavior reported with a nil error, so
every push is accounted for. The error keeps its meaning: flow control
(`ErrBackpressure`) or fatal.

```go
input := goav.Source("generated",
    shape.Packet(av.MediaAudio, av.CodecOpus,
        shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for packet := range packets {
            if _, err := push.Packet(&packet); err != nil {
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

Frame sources use the same constructor with `shape.Frame` and skip decode:

```go
frames := goav.Source("pcm",
    shape.Frame(av.MediaAudio,
        shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for frame := range decoded {
            if _, err := push.Frame(&frame); err != nil {
                return err
            }
        }
        return push.EOS()
    },
)
```

Event-only sources use `shape.Event()` and `push.Event(...)`, routing directly
to sinks. Custom sources participate in the same stream, branch, destination,
explain, and runtime graph path as built-in inputs.

## Custom Destinations

Write muxed bytes anywhere that can provide an `io.WriteCloser` with
`goav.Writer(...)`. Use `goav.Object(...)` when the writer has explicit commit
and abort semantics, such as a multipart object-store upload. The destination
opens after goav has selected the format and streams, so uploaders see the final
destination metadata. Transactional writers commit after successful runs or
detach, abort on failure, and close exactly once. Normal application
workflows should be expressible through declarative recipes. Use `goav.Custom(name, provider)`
when a package owns a reusable destination provider; the returned destination
value is still the stable routing handle.

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
group. The same destination option style works for built-in destinations:

```go
return goav.From(input).
    Copy().
    To(goav.File("", out, goav.Format(av.FormatIVF)))
```

## Runtimes And Custom Codecs

Registries are per-runtime — there are no global registries. `goav.Default(opts...)`
builds a runtime with the standard codecs, formats, and filters registered, then
applies your options on top; registration is last-wins, so one call can both add
new implementations and override a default. `goav.New(opts...)` starts bare.
Direct value registration covers every family: `WithDecoder`, `WithEncoder`,
`WithFilter`, `WithMuxer`, `WithDemuxer`, and `WithProber`.

Custom codecs use the same recipe grammar as built-ins: register factories, then
reference them with generic `Codec` specs. Codec descriptors drive capability
checks, so incompatible media fails before allocation or graph mutation. Adapter
authoring details live in [`docs/ADAPTERS.md`](docs/ADAPTERS.md).

Opus, VP8, and VP9 are the full encode/decode recipe verticals. H264 and AV1
receive/decode paths are active while recipe encode remains guarded as work in
progress. `Shape(...)` describes structural media compatibility only; encoder
behavior is a two-tier ladder, and the settings live in the `codec` package (not
the goav root) so the grammar stays small. Tier 1 is the common typed settings
(`codec.Bitrate`, `codec.FPS`, `codec.KeyframeInterval`, `codec.Profile`,
`codec.RateControl`, …). Tier 2 is `codec.Control`: a single raw callback handed
the adapter's concrete encoder/decoder, so you type-assert and apply anything the
library exposes — nothing is ever unreachable, and there is no separate config
blob to learn:

```go
// codec is github.com/thesyncim/goav/codec; govpx is github.com/thesyncim/govpx
vp9 := codec.VP9(
    codec.Bitrate(2_000_000),
    codec.FPS(30),
    codec.KeyframeInterval(60),
    codec.Profile("0"),
    codec.Control(func(enc any) error {      // raw escape hatch
        if e, ok := enc.(*govpx.VP9Encoder); ok {
            return e.SetCQLevel(20)          // any native libvpx control
        }
        return nil
    }),
)
```

`Shape(...)` annotates the current media point; it is not an escape hatch around
operation contracts. The compiler checks each step in order: an encoder must
consume frames, `Copy()` must consume packets, and resize/resample must consume
matching decoded media. File, URI, writer, and object destinations consume
packet-domain media; use `goav.Sink(...)` when a branch should end as frames.

Each adapter documents the concrete encoder/decoder type it hands `Control`; the
public grammar stays Input, Stream, Tap, Branch, Destination, Flow, and Task —
operations are methods on the chain, not a separate vocabulary.
The reusable component catalog and allocation proof map live in
[`docs/COMPONENTS.md`](docs/COMPONENTS.md).

## Current Shape

Implemented now:

- Variadic `From(inputs...)` composition with `InputName` narrowing and shared
  multi-input destinations.
- Packet-preserving `Copy().To(...)` and packet-copy `Branches(...)`.
- Stream-scoped decode, custom stages, resize/resample, Opus/VP8/VP9 encode, and
  operation-by-operation shape validation.
- `Mix`/`Composite`/`Select` convergence with composable join outputs and a live
  `SelectActive` switch.
- Typed taps, branches, destinations, flows, and runtime branch attachment with
  atomic grouped attach/rollback and dependent-branch detach.
- Typed task control (`Keyframe`, `Deliver`, `SelectActive`) riding the data path.
- Snapshots with typed task/branch/destination lifecycle states and scoped stats.
- Per-runtime registries with layered `Default(opts...)` and direct
  `WithDecoder`/`WithEncoder`/`WithFilter`/`WithMuxer`/`WithDemuxer`/`WithProber`
  registration.
- Structured `Explain(ctx)` reports and `Describe()` graph specs.
- Pion-based RTP/WebRTC receive boundaries; pure-Go adapters for IVF, Annex B,
  Matroska/WebM, Opus, VP8/VP9, AV1, H264, resize, and resample.

Advanced notes live in `docs/`.
