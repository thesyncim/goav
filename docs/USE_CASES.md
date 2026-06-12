# Use Cases

These scenarios keep the public API honest while implementation slices land.

## WebRTC Receive

Receive Pion `TrackRemote` values, preserve RTP metadata, surface packet loss
and codec changes, then record, decode, transform, or attach analysis.

Packet-preserving receive:

```go
err := goav.From(goav.Input(webrtcav.Track(track))).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Reuse the same file, URI, writer, object upload, or sink destination value when
several branches should feed one mux or sink group.

Decoded preview:

```go
err := goav.From(goav.Input(webrtcav.Track(track))).
    Video().
    Decode().
    To(goav.Sink(preview)).
    Run(ctx)
```

Several realtime inputs compose through the variadic `From(inputs...)`, with
`goav.InputName(...)` narrowing each chain to its input and one shared
destination value muxing the encoded chains together.

## RTP Receive

Raw RTP uses Pion RTP packet types through `rtpav.PacketReader`. RTP inputs need
codec intent so the runtime can choose the depacketizer.

```go
err := goav.From(goav.Input(rtpav.Receive(video, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))).
    Copy().
    To(
        goav.File("recording.ivf", file),
        goav.File("preview.ivf", preview),
    ).
    Run(ctx)
```

Decode and transform one selected stream:

```go
err := goav.From(goav.Input(rtpav.Receive(audio, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
    Audio().
    Decode().
    Resample(16_000, codec.Mono).
    Encode(codec.Opus(codec.Bitrate(48_000))).
    To(goav.File("preview.ogg", preview)).
    Run(ctx)
```

When a media type matches several streams, the build error lists candidates and
suggests `StreamID`, `StreamName`, or `StreamIndex(0)`.

## Branches

`FrameTap` and `PacketTap` name stable points. `Branches` declares downstream
alternatives from one selected stream. Each branch is ordered operations:
custom stages, transforms, taps, optional encode, then typed destinations. Mux
destinations require encoded packets; sink destinations receive decoded frames
before encode or packets after copy/encode.

```go
videoFrames720p := goav.FrameTap("video.720p.frames")
main := goav.File("main.webm", out)

err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Do(frameMeter).
            Tap(videoFrames720p).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(main),
    ).
    Audio().
    Decode().
    Branches(
        goav.Branch("a96").
            Resample(48_000, codec.Stereo).
            Encode(codec.Opus(codec.Bitrate(96_000))).
            To(main),
    ).
    Run(ctx)
```

One destination can be a mux group: several encoded branches feed the same
destination value. A destination can also be a sink after any branch operation
(no encode needed for frame-domain ends).

Branches normally start from the current stream point. When one branch needs an
earlier operation boundary, name that boundary with a typed tap and anchor the
branch with `From`:

```go
videoDecoded := goav.FrameTap("video.decoded")
videoFrames720p := goav.FrameTap("video.720p.frames")
thumbs := goav.Sink(goav.SinkFunc("thumbs", collectThumbnail))
web := goav.File("web.ivf", webFile)

err := goav.From(input).
    Video().
    Decode().
    Tap(videoDecoded).
    Resize(1280, 720).
    Tap(videoFrames720p).
    Branches(
        goav.Branch("thumb").
            From(videoDecoded).
            Resize(320, 180).
            To(thumbs),
        goav.Branch("web").
            From(videoFrames720p).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Run(ctx)
```

## Custom Components

A stage can live inside a normal stream recipe, a sink can receive frames
before encode or packets after copy/encode, and a running task can attach a
sink from a declared tap. Once a stream is in packet domain through `.Copy()`
or an encoder, it can fan out to both mux destinations and packet sinks;
`.Copy().Branches(...)` splits one encoded stream without a decoder, and a
branch can call `.Decode()` first when the split needs raw frames.

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

err := goav.From(input).
    Audio().
    Decode().
    Do(meter).
    To(goav.Sink(goav.SinkFunc("levels", collectLevel))).
    Run(ctx)
```

Custom sources are the input-side equivalent for applications that already own
media production. Packet sources use `shape.Packet`, frame sources use
`shape.Frame` and skip decode, event-only sources use `shape.Event` and route
straight to sinks. Each push returns a `PushResult` reporting `Accepted` and
`Dropped`, so realtime shedding is visible without being an error.

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

err := goav.From(input).
    Audio().
    Copy().
    To(goav.Sink(packetSink)).
    Run(ctx)
```

Custom sources participate in the same stream, branch, destination, explain,
and runtime graph path as built-in inputs.

## Reuse

When operations repeat, extract a flow. A flow is only a reusable ordered
operation sequence; branches own destinations, so reusable and ad hoc splits
use the same API.

```go
voice := goav.Flow("voice").Audio().
    Resample(16_000, codec.Mono).
    Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
archive := goav.Flow("archive").Audio().
    Resample(48_000, codec.Stereo).
    Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))
voiceOut := goav.File("voice.ogg", voiceFile)
archiveOut := goav.File("archive.ogg", archiveFile)

err := goav.From(goav.Input(rtpav.Receive(audio, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

The same flow applies to a direct stream or to a runtime branch attached from a
tap. Use a direct stream when one reusable flow feeds one destination. Use
branches when the same media point needs several downstream operation sequences.

## Runtime Branches

Runtime branches are late control-plane work: analysis, meters, screenshot
collectors, recordings, or temporary sinks that attach to future messages
without rebuilding upstream.

Branch buffers are local to the downstream branch:

```go
goav.Branch("archive").
    Buffer(flow.Blocking(128)).
    To(archive)

goav.Branch("preview").
    Buffer(flow.DropOldest(3)).
    To(preview)

goav.Branch("latest-diagnostics").
    Buffer(flow.Latest()).
    To(goav.Sink(goav.SinkFunc("latest", inspect)))
```

Attach a late branch from a typed tap; encode from frame taps, copy or decode
from packet taps:

```go
audioDecoded := goav.FrameTap("audio.decoded")
recording := goav.File("recording.ogg", file)

recordingHandle, err := task.Attach(ctx,
    goav.Branch("record-audio").
        From(audioDecoded).
        Encode(codec.Opus(codec.Bitrate(96_000))).
        To(recording),
)
if err != nil {
    return err
}
defer recordingHandle.Close(ctx)
```

Attach several late branches in one call when they should appear or disappear
together. A later branch in the same call can anchor from a tap published by an
earlier branch, and one grouped destination value (sink or mux) can receive
several branch outputs:

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.File("recording.webm", file)

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

Use `Task.Taps()` to discover stable outlets and `Task.Detach(ctx, h)` when the
task should own detach semantics. Taps declared after encode or copy are packet
taps. Observer branches can end in a sink while publishing a nested tap with
`.Do(goav.FrameFunc(...)).Tap(goav.FrameTap(name)).To(goav.Sink(...))`. Detaching
a parent removes dependent late branches anchored from its taps. H264 recipe
encoding remains work in progress.

## Debug And Diagnostics

Debugging uses the same composition grammar. Explain the plan before opening the
graph, drain task events while it runs, then attach temporary branches from
typed taps. `Attachment.Snapshot()` reports the attached branch, and
`Task.Snapshot()` reports the whole graph plus active branches with typed
lifecycle states.

```go
audioDecoded := goav.FrameTap("audio.decoded")

job := goav.From(goav.Input(webrtcav.Track(track))).
    Audio().
    Decode().
    Tap(audioDecoded).
    To(goav.Sink(playback))

report, err := job.Explain(ctx)
if err != nil {
    return err
}
for _, warning := range report.Warnings {
    log.Printf("plan warning code=%s node=%s msg=%s",
        warning.Code, warning.Node, warning.Message)
}

task, err := job.Build(ctx)
if err != nil {
    return err
}
go func() {
    for event := range task.Events() {
        log.Printf("media event type=%s stream=%s", event.Type, event.StreamID)
    }
}()
go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("levels").
        From(audioDecoded).
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
levelFrames := uint64(0)
for _, branch := range state.Branches {
    if branch.Name == "levels" {
        levelFrames = branch.Stats.Frames
    }
}
log.Printf("packets=%d frames=%d dropped=%d branches=%d level_frames=%d",
    state.Stats.Packets,
    state.Stats.Frames,
    state.Stats.Dropped,
    len(state.Branches),
    levelFrames)
```

## Generic File Or Protocol Ingest

Packet-preserving file fanout:

```go
err := goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(
        goav.File("recording.ivf", recording),
        goav.File("preview.ivf", preview),
    ).
    Run(ctx)
```

When packet formats already match, recording stays packet-preserving. IVF
covers VP8/VP9/AV1 packet recording; Annex B covers packet-preserving H264
recording after RTP depacketization.

## Resample And Resize

Audio filters express sample rate, channel count/layout, and sample format; the
first concrete adapter covers interleaved `s16` PCM with linear conversion.
Video filters express exact, fit, fill, and passthrough modes; the first
concrete adapter covers planar 8-bit 4:2:0 frames with nearest-neighbor scaling
and caller-owned output planes.
