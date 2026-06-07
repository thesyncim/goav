# Use Cases

These scenarios keep the public API honest while implementation slices land.

## WebRTC Receive

Receive Pion `TrackRemote` values, preserve RTP metadata, surface packet loss
and codec changes, then record, decode, transform, or attach analysis.

Packet-preserving receive:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Reuse the same file, URI, writer, object upload, or sink destination value when
several branches should feed one mux or sink group.

Decoded preview:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Video().
    Decode().
    To(goav.Sink(preview)).
    Run(ctx)
```

Repeated realtime tracks compose through `From(first).And(other...)`. Shared
containers such as WebM need a matching muxer adapter registered on the runtime.

## RTP Receive

Raw RTP uses Pion RTP packet types through `rtpav.PacketReader`. RTP inputs need
codec intent so the runtime can choose the depacketizer.

```go
err := goav.From(goav.RTP(video).Name("video").Codec(goav.VP8())).
    Copy().
    To(
        goav.File("recording.ivf", file),
        goav.File("preview.ivf", preview),
    ).
    Run(ctx)
```

Decode and transform one selected stream:

```go
err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Decode().
    Resample(16_000, goav.Mono).
    Encode(goav.Opus(goav.Bitrate(48_000))).
    To(goav.File("preview.ogg", preview)).
    Run(ctx)
```

When a media type matches several streams, the build error lists candidates and
suggests `StreamID`, `StreamName`, or `StreamIndex(0)`.

## Branches

`FrameTap` and `PacketTap` name stable points. `Branches` declares downstream
alternatives from one selected stream. Each branch is ordered operations: custom
stages, transforms, taps, optional encode, then typed destinations. Mux
destinations require encoded packets; sink destinations can receive decoded frames before encode
or packets after copy/encode. This keeps complex work natural without exposing
graph wiring.

```go
videoDecoded := goav.FrameTap("video.decoded")
videoFrames720p := goav.FrameTap("video.720p.frames")
audioDecoded := goav.FrameTap("audio.decoded")
main := goav.File("main.webm", out)

err := goav.From(input).
    Video().
    Decode().
    Tap(videoDecoded).
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Do(frameMeter).
            Tap(videoFrames720p).
            Encode(goav.VP9(goav.Bitrate(2_000_000))).
            To(main),
    ).
    Audio().
    Decode().
    Tap(audioDecoded).
    Branches(
        goav.Branch("a96").
            Resample(48_000, goav.Stereo).
            Encode(goav.Opus(goav.Bitrate(96_000))).
            To(main),
    ).
    Run(ctx)
```

One destination can be a mux group. Several encoded branches can feed the same
destination. A destination can also be a sink for preview, screenshots, analysis,
or integration work after any branch operation:

```go
thumbs := goav.Sink(goav.SinkFunc("thumbs", collectThumbnail))

err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("thumb").
            Resize(320, 180).
            To(thumbs),
    ).
    Run(ctx)
```

Containers shown outside IVF/Annex B require matching adapters.

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
            Encode(goav.VP9(goav.Bitrate(2_000_000))).
            To(web),
    ).
    Run(ctx)
```

## Custom Components

Custom work should be optional and local. A stage can live inside a normal
stream recipe, a sink can receive frames before encode or packets after
copy/encode, and a running task can attach a sink from a declared tap.
Once a stream is in packet domain through `.Copy()` or an encoder, it can fan
out to both mux destinations and packet sinks.
The same packet-domain rule applies to planned branches: `.Copy().Branches(...)`
can split one selected encoded stream into mux or sink destinations without a
decoder. A branch can also call `.Decode()` first when that packet-domain split
needs raw frames for analysis, preview, or a later frame-domain tap.

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

Branch-local custom stages and transforms share the ordered operation model.
Custom filter adapters and late runtime sink branches extend that same model
instead of growing special-case APIs.

Custom sources are the input-side equivalent for applications that already own
packet production.

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

err := goav.From(input).
    Audio().
    Copy().
    To(goav.Sink(packetSink)).
    Run(ctx)
```

Packet-domain custom sources participate in the same stream, branch,
destination, explain, and runtime graph path as built-in inputs. Frame-domain
sources use `FrameShape` and skip decode.

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

err := goav.From(frames).
    Audio().
    To(goav.Sink(levels)).
    Run(ctx)
```

## Reuse

When operations repeat, extract a flow. A flow is only a reusable ordered
operation sequence: custom stages, taps, transforms, an optional first decode,
and an optional terminal encoder. Branches own destinations, so reusable and ad hoc
splits use the same API.

```go
voiceFrames := goav.FrameTap("audio.voice.frames")
audioDecoded := goav.FrameTap("audio.decoded")

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

err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Apply(voice).
    To(voiceOut).
    Run(ctx)

err = goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Decode().
    Tap(audioDecoded).
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

The same flow shape can be applied to a direct stream or to a runtime
branch attached from a tap. The flow still owns only operations; the stream or
branch owns its destination. Use a direct stream when one reusable flow feeds one
destination. Use branches when the same media point needs several downstream
operation sequences.

```go
archiveHandle, err := task.Attach(ctx,
    goav.Branch("archive-live").
        From(audioDecoded).
        Apply(archive).
        To(archiveOut),
)
```

## Runtime Branches

Runtime branches are late control-plane work: analysis, meters, screenshot
collectors, or temporary sinks that should attach to future messages without
rebuilding upstream.

Branch buffers are local to the downstream branch:

```go
goav.Branch("archive").
    Buffer(goav.Blocking(128)).
    To(archive)

goav.Branch("preview").
    Buffer(goav.DropOldest(3)).
    To(preview)

goav.Branch("latest-diagnostics").
    Buffer(goav.Latest()).
    To(goav.Sink(goav.SinkFunc("latest", inspect)))
```

```go
audioDecoded := goav.FrameTap("audio.decoded")

task, err := job.Build(ctx)
if err != nil {
    return err
}
go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("level-meter").
        From(audioDecoded).
        To(goav.Sink(goav.SinkFunc("levels", collectLevel))),
)
if err != nil {
    return err
}
defer levels.Close(ctx)
```

Late branches can also record future media without rebuilding upstream:

```go
audioDecoded := goav.FrameTap("audio.decoded")
recording := goav.File("recording.ogg", file)

recordingHandle, err := task.Attach(ctx,
    goav.Branch("record-audio").
        From(audioDecoded).
        Encode(goav.Opus(goav.Bitrate(96_000))).
        To(recording),
)
if err != nil {
    return err
}
defer recordingHandle.Close(ctx)
```

Attach several late branches in one call when they should appear or disappear
together. A later branch in the same call can anchor from a tap published by an
earlier branch:

```go
audioDecoded := goav.FrameTap("audio.decoded")
audioMetered := goav.FrameTap("audio.metered")

group, err := task.Attach(ctx,
    goav.Branch("analysis").
        From(audioDecoded).
        Do(goav.FrameFunc("meter", meter)).
        Tap(audioMetered).
        To(goav.Sink(goav.SinkFunc("levels", collectLevel))),
    goav.Branch("dependent").
        From(audioMetered).
        To(goav.Sink(goav.SinkFunc("dependent", collectDependent))),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

One grouped runtime sink destination value can receive several branch outputs:

```go
audioDecoded := goav.FrameTap("audio.decoded")
metered := goav.Sink(goav.SinkFunc("metered", collectMetered))

group, err := task.Attach(ctx,
    goav.Branch("fast").From(audioDecoded).To(metered),
    goav.Branch("slow").From(audioDecoded).To(metered),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

One grouped runtime mux destination value can receive several encoded packet
branches:

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

Use `.Copy()` from a packet tap when the branch should stay encoded:

```go
audioEncoded := goav.PacketTap("audio.encoded")

packets, err := task.Attach(ctx,
    goav.Branch("packet-recording").
        From(audioEncoded).
        Copy().
        To(goav.File("recording.ogg", file)),
)
if err != nil {
    return err
}
defer packets.Close(ctx)
```

Use `.Decode()` from a packet tap when the late branch needs frames:

```go
audioPackets := goav.PacketTap("audio.packets")
previewFrames := goav.FrameTap("audio.preview.frames")

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

Use `Task.Taps()` to discover stable outlets. Use `Task.Detach(ctx, h)` when
the caller wants the task to own detach semantics. Runtime branches can be
attached one at a time or as one atomic group. They can run custom stages,
resize/resample from frame taps, publish additional taps, encode Opus/VP8/VP9
from frame taps, copy packet taps into destinations, decode packet taps into frame
branches, and feed later runtime branches from those taps. Taps declared after
encode or copy are packet taps. Observer branches can end in a sink while
publishing a nested tap with
`.Do(goav.FrameFunc(...)).Tap(goav.FrameTap(name)).To(goav.Sink(...))`. H264 and AV1
recipe encoding remain work in progress. Detaching a parent runtime branch
removes dependent late branches anchored from its taps. Direct and bounded
buffered task graphs both support late stage/sink branches. Runtime branch
groups can share one typed sink or mux destination value.

## Debug And Diagnostics

Debugging uses the same composition grammar. Explain the plan before opening the
graph, drain task events while it runs, then attach temporary branches from
typed taps to collect diagnostics. `Attachment.Snapshot()` reports the attached
branch, and `Task.Snapshot()` reports the whole graph plus active branches.

```go
audioDecoded := goav.FrameTap("audio.decoded")

job := goav.From(goav.WebRTCTrack(track)).
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

When packet formats already match, recording can stay packet-preserving. IVF is
the first concrete focus for VP8, VP9, and AV1 packet recording; Annex B covers
packet-preserving H264 recording after RTP depacketization.

## Resample

Audio filters express sample rate, channel count, channel layout, and sample
format without binding the API to one implementation. The first concrete adapter
covers interleaved `s16` PCM with linear sample-rate conversion and basic
channel conversion.

## Resize

Video filters express exact, fit, fill, and passthrough modes so the same
contract works for ladders, previews, thumbnails, and recording branches. The first
concrete adapter covers planar 8-bit 4:2:0 frames with nearest-neighbor scaling
and caller-owned output planes.
