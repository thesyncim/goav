# Use Cases

These scenarios keep the public API honest while implementation slices land.

## WebRTC Receive

Receive Pion `TrackRemote` values, preserve RTP metadata, surface packet loss
and codec changes, then record, decode, transform, or attach analysis.

Packet-preserving receive:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Copy().
    To(goav.FileOutput("recording.ivf", file)).
    Run(ctx)
```

Decoded preview:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Video().
    Decode().
    To(goav.FrameSink(preview)).
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
        goav.FileOutput("recording.ivf", file),
        goav.FileOutput("preview.ivf", preview),
    ).
    Run(ctx)
```

Decode and transform one selected stream:

```go
err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Decode().
    Resample(16_000, goav.Mono).
    Opus(48_000).
    To(goav.FileOutput("preview.ogg", preview)).
    Run(ctx)
```

When a media type matches several streams, the build error lists candidates and
suggests `StreamID`, `StreamName`, or `StreamIndex(0)`.

## Branches

`Tap` names a stable point. `Branches` declares downstream alternatives from
one selected stream. Each branch is an ordered chain: custom stages, transforms,
taps, encode, then typed targets. This keeps complex work natural without
exposing graph wiring.

```go
main := goav.Target("main", goav.FileOutput("main.webm", out))

err := goav.From(input).
    Video().
    Decode().
    Tap("video.decoded").
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Do(frameMeter).
            Tap("video.720p.frames").
            VP9(2_000_000).
            To(main),
    ).
    Audio().
    Decode().
    Tap("audio.decoded").
    Branches(
        goav.Branch("a96").
            Resample(48_000, goav.Stereo).
            Opus(96_000).
            To(main),
    ).
    Run(ctx)
```

One target can be a mux group. Several encoded branches can feed the same
target. Containers shown outside IVF/Annex B require matching adapters.

## Custom Components

Custom work should be optional and local. A stage can live inside a normal
stream recipe, a sink can receive decoded frames, and a running task can attach
a sink from a declared tap.

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

err := goav.From(input).
    Audio().
    Decode().
    Do(meter).
    To(goav.FrameSink(goav.SinkFunc("levels", collectLevel))).
    Run(ctx)
```

Branch-local custom stages and transforms share the ordered operation model.
Custom filter adapters and late muxed runtime outputs should extend that same
model instead of growing special-case APIs.

## Reusable Flows

Flows are reusable operation sequences. Branches own targets, so reusable and
ad hoc splits use the same API.

```go
voice := goav.AudioFlow("voice").Resample(16_000, goav.Mono).OpusVoice()
archive := goav.AudioFlow("archive").Resample(48_000, goav.Stereo).OpusMusic()
voiceTarget := goav.Target("voice", goav.FileOutput("voice.ogg", voiceFile))
archiveTarget := goav.Target("archive", goav.FileOutput("archive.ogg", archiveFile))

err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceTarget),
        goav.Branch("archive").Apply(archive).To(archiveTarget),
    ).
    Run(ctx)
```

## Runtime Branches

Runtime branches are late control-plane work: analysis, meters, screenshot
collectors, or temporary sinks that should attach to future messages without
rebuilding upstream.

```go
task, err := job.Build(ctx)
if err != nil {
    return err
}
go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("level-meter").
        FromTap("audio.decoded").
        To(goav.FrameSink(goav.SinkFunc("levels", collectLevel))),
)
if err != nil {
    return err
}
defer levels.Close(ctx)
```

Use `Task.Taps()` to discover stable outlets. Use `Task.Detach(ctx, h)` when
the caller wants the task to own detach semantics. Runtime branches currently
attach sink-oriented observation work; late muxed recording and buffered dynamic
branch mutation remain explicit roadmap slices.

## Generic File Or Protocol Ingest

Packet-preserving file fanout:

```go
err := goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(
        goav.FileOutput("recording.ivf", recording),
        goav.FileOutput("preview.ivf", preview),
    ).
    Run(ctx)
```

When packet formats already match, recording can stay packet-preserving. IVF is
the first concrete target for VP8, VP9, and AV1 packet recording; Annex B covers
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
