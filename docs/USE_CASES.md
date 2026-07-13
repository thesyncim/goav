# Use Cases

This is the cookbook layer. It shows the shapes you are likely to copy first,
then points to the deeper reference docs when you need the edge cases.

The examples are intentionally ordinary: they use the same `From -> stream
selection -> operations -> branches -> destinations -> task` grammar whether
the media comes from a file, RTP, WebRTC, or your own application.

## WebRTC Receive

When a Pion `TrackRemote` enters your app, goav treats it as another input.
You can preserve packets for recording, decode for preview, or attach analysis
later without rebuilding the receive path.

Packet-preserving recording:

```go
err := goav.From(goav.Input(webrtcav.Track(track))).
    Copy().
    To(goav.Write("recording.ivf", file)).
    Run(ctx)
```

Give matching file, URI, writer, object upload, or sink destinations the same
`goav.Mux(name, destination)` when several branches should feed one mux or sink
group. Reusing one ungrouped destination value is rejected.

Decoded preview:

```go
err := goav.From(goav.Input(webrtcav.Track(track))).
    Video().
    Decode().
    To(goav.Sink(preview)).
    Run(ctx)
```

Several realtime inputs compose through `From(inputs...)`; use
`goav.InputName(...)` to say which chain reads which input.

## RTP Receive

Raw RTP uses Pion RTP packet types through `rtpav.PacketReader`. Give RTP
inputs codec intent so the runtime can choose the depacketizer before packets
arrive.

```go
err := goav.From(goav.Input(rtpav.Receive(video, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))).
    Copy().
    To(
        goav.Write("recording.ivf", file),
        goav.Write("preview.ivf", preview),
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
    To(goav.Write("preview.ogg", preview)).
    Run(ctx)
```

When a media type matches several streams, the build error lists candidates and
suggests the narrow selector to add: `StreamID` or `StreamIndex(0)`.

## Live Audio Rooms

Rooms are where the runtime model pays off. Participants join, leave, and
reconnect while you still need per-track work for levels, moderation,
transcription, recording, and maybe one mixed output for playback or archive.

Use `goav.Mix(arms...)` when the arms are known when the recipe is built. When
membership changes at runtime, keep each participant as an application-owned
stream. The application owns the registry, attaches ordinary branches from
`input.Stream(participantStream)` before accepting media, emits
`EventStreamAdded` / `EventStreamRemoved`, and feeds any mixed output after the
per-track routes exist.

```go
room := NewRoom("room", 48_000, 1)
input := room.Input()
tracks := NewTrackRecorder()
mix := NewOutputMixer()
meter := NewTrackMeter()
trackMeter := component.FrameFunc("track-meter", func(_ context.Context, frame *av.Frame, emit component.Emit) error {
    meter.Observe(frame)
    return emit.Frame(frame)
})
anchor := goav.Sink(component.SinkFunc("room-anchor", func(context.Context, component.Message) error {
    return nil
}))

task, err := goav.From(input).
    Audio().
    To(anchor).
    BuildLive(ctx)
if err != nil {
    return err
}
go func() { _ = task.Run(ctx) }()

attachments := map[string]goav.Attachment{}
join := func(name string) error {
    track := room.ParticipantStream(name)
    attachment, err := task.Attach(ctx,
        goav.Branch("track-"+name).
            From(input.Stream(track)).
            Do(trackMeter).
            To(tracks.Sink()),
        goav.Branch("mix-"+name).
            From(input.Stream(track)).
            To(mix.Sink()),
    )
    if err != nil {
        return err
    }
    if err := room.Join(ctx, name); err != nil {
        _ = task.Detach(ctx, attachment, lifecycle.AbortBranch())
        return err
    }
    attachments[name] = attachment
    return nil
}

leave := func(name string) error {
    attachment, ok := attachments[name]
    if !ok {
        return fmt.Errorf("%s is not active", name)
    }
    if err := room.Leave(ctx, name); err != nil {
        return err
    }
    if err := task.Detach(ctx, attachment, lifecycle.DrainBranch()); err != nil {
        return err
    }
    delete(attachments, name)
    return nil
}

_ = join("host")
_ = join("music")
_ = room.Push(ctx, map[string][]int16{
    "host":  []int16{100, 100},
    "music": []int16{25, -50},
})
_ = leave("music")
```

`OnStream` is still useful when a source discovers tracks itself and automatic
branch attachments are enough. For app-owned room membership, explicit
`input.Stream(track)` anchors make the first-frame boundary deterministic.

The runnable module `examples/dynamic-audio-room` validates this pattern with
`goavtest/expect`: it proves runtime participant add/remove events, S16
summing and clamping in the output mixer, per-track processing, and
inactive-track rejection. It deliberately does not add a root `Room` API:
rooms are an application pattern built from normal streams, branches,
destinations, snapshots, and detach semantics.

## Branches

`FrameTap` and `PacketTap` name stable points. `Branches` declares downstream
alternatives from one selected stream. Each branch is ordered operations:
custom stages, transforms, taps, optional encode, then typed destinations. Mux
destinations require encoded packets; sink destinations receive decoded frames
before encode or packets after copy/encode.

```go
videoFrames720p := goav.FrameTap("video.720p.frames")
main := goav.Write("main.webm", out)

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

One destination can be a mux group: several encoded branches feed matching
`goav.Mux(name, destination)` values. A destination can also be a sink after
any branch operation (no encode needed for frame-domain ends).

Branches normally start from the current stream point. When one branch needs an
earlier operation boundary, name that boundary with a typed tap and anchor the
branch with `From`:

```go
videoDecoded := goav.FrameTap("video.decoded")
videoFrames720p := goav.FrameTap("video.720p.frames")
thumbs := goav.Sink(component.SinkFunc("thumbs", collectThumbnail))
web := goav.Write("web.ivf", webFile)

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
meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

err := goav.From(input).
    Audio().
    Decode().
    Do(meter).
    To(goav.Sink(component.SinkFunc("levels", collectLevel))).
    Run(ctx)
```

Custom sources are the input-side equivalent for applications that already own
media production. Packet sources use `shape.Packet`, frame sources use
`shape.Frame` and skip decode, event-only sources use `shape.Event` and route
straight to sinks. Each push returns a `source.Result` reporting `Accepted` and
`Dropped`, so realtime shedding is visible without being an error.

```go
input := goav.Source("generated",
    shape.Packet(av.MediaAudio, av.CodecOpus,
        shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push source.Push) error {
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
voiceOut := goav.Write("voice.ogg", voiceFile)
archiveOut := goav.Write("archive.ogg", archiveFile)

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
    To(goav.Sink(component.SinkFunc("latest", inspect)))
```

Attach a late branch from a typed tap; encode from frame taps, copy or decode
from packet taps:

```go
audioDecoded := goav.FrameTap("audio.decoded")
recording := goav.Write("recording.ogg", file)

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
together. A later branch in the same call can anchor from a tap published by
an earlier branch, and one explicit destination group can receive several sink
or mux branch outputs:

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.Mux("recording", goav.Write("recording.webm", file))

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
if err != nil {
    return err
}
defer group.Close(ctx)
```

Use `Inspectable.Taps()` to discover stable outlets and `Mutable.Detach(ctx, h)` for a
plain removal, `lifecycle.DrainBranch()` when the branch output should commit, or
`lifecycle.AbortBranch()` when it should be abandoned. Taps declared after encode or copy
are packet taps. Observer branches can end in a sink while publishing a nested
tap with `.Do(component.FrameFunc(...)).Tap(goav.FrameTap(name)).To(goav.Sink(...))`.
Detaching a parent removes dependent late branches anchored from its taps. H264
recipe encoding remains work in progress.

## Debug And Diagnostics

Debugging uses the same composition grammar. Explain the plan before opening the
graph, watch task events while it runs, then attach temporary branches from
typed taps. `Attachment.Snapshot()` reports the attached branch, and
`Inspectable.Snapshot()` reports the whole graph plus active branches with typed
lifecycle states.

![Runtime pipeline debugging](assets/pipeline-debug.svg)

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

task, err := job.BuildLive(ctx)
if err != nil {
    return err
}
go func() {
    for event := range task.Watch().Events() {
        log.Printf("media event type=%s stream=%s", event.Type, event.StreamID)
    }
}()
go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("levels").
        From(audioDecoded).
        Do(component.FrameFunc("rms", func(_ context.Context, frame *av.Frame, emit component.Emit) error {
            observeRMS(frame)
            return emit.Frame(frame)
        })).
        To(goav.Sink(component.SinkFunc("levels", func(context.Context, component.Message) error {
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
        goav.Write("recording.ivf", recording),
        goav.Write("preview.ivf", preview),
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
