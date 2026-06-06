# Use Cases

These scenarios keep shaping the interfaces as narrow implementation slices
land.

## WebRTC receive

Receive one or more Pion `TrackRemote` values, preserve RTP metadata, detect
loss, emit codec and track lifecycle events, depacketize, decode, and optionally
record or forward.

The current receive boundary accepts tracks from a Pion peer connection, keeps
one long-lived reader per logical stream with `webrtcav.TrackSet`, applies
same-stream replacement tracks as codec-change events, and routes RTCP feedback
through the session peer connection.

The recipe front door accepts a Pion track directly and lowers it through the
same RTP receive graph:

```go
err := goav.Record(
    goav.WebRTCTrack(track),
    goav.FileOutput("recording.ivf", file),
).Run(ctx)
```

Repeated realtime tracks compose through the same front door:

```go
err := goav.From(goav.WebRTCTrack(audio)).
    And(goav.WebRTCTrack(video)).
    To(goav.FileOutput("call.webm", file)).
    Run(ctx)
```

Repeated realtime inputs need distinct names when names are explicit, such as
`audio` and `video`. A shared recording container such as WebM needs a matching
muxer adapter registered on the runtime.

Expected graph:

```text
WebRTC session
  -> track set
  -> RTP receiver
  -> jitter/loss handling
  -> depacketizer
  -> decoder
  -> frame sinks or encoders
```

## RTP receive

Receive raw RTP using Pion RTP packet types, resolve payload type mappings,
surface gaps and discontinuities, emit RTCP feedback, and produce codec packets.

The high-level record/fanout shape accepts one or more packet readers:

```go
err := goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.FileOutput("recording.ivf", file),
    goav.FileOutput("preview.ivf", preview),
).Run(ctx)
```

For multiple live readers, use `From(first).And(other...)` instead of wiring
separate graph sources by hand.

The same receive boundary can decode a selected stream when a matching decoder
factory is registered, and can continue into filters, an explicit target
encoder, and one or more mux outputs:

```go
err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    To(goav.FrameSink(frames)).
    Run(ctx)
```

Audio and video transforms stay stream-local in the recipe:

```go
err := goav.From(input).
    Audio().
    Resample(16_000, goav.Mono).
    Opus(48_000).
    To(goav.FileOutput("preview.ogg", preview)).
    Run(ctx)
```

The `From` stream recipe has one selected stream chain. Use `Transcode` for
multiple audio/video branches from one input. Stream recipes attach outputs on
the stream chain with `.To(...)`; generic `From(input).To(...)` remains the
packet-preserving record/remux shape. A stream chain sends decoded frames to
frame sinks or encoded packets to file/URI outputs, not both. Processing steps
come before one terminal encoder, and outputs attach after that encoder.

When one selected stream should feed several encoded outputs, reuse flow
branches and split with `Tee`:

```go
voice := goav.AudioFlow("voice").Resample(16_000, goav.Mono).OpusVoice()
archive := goav.AudioFlow("archive").Resample(48_000, goav.Stereo).OpusMusic()

err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Tee(
        voice.To(goav.FileOutput("voice.ogg", voiceFile)),
        archive.To(goav.FileOutput("archive.ogg", archiveFile)),
    ).
    Run(ctx)
```

`Tee` is a planned media split for the selected stream. Runtime branches use a
separate control-plane operation when a late branch is a stage and/or sink:

```go
levels, err := task.Attach(ctx,
    goav.Branch("level-meter").
        FromDecodedAudio().
        To(goav.SinkFunc("levels", collectLevel)),
)
if err != nil {
    return err
}
defer levels.Stop(ctx)
```

Use `FromDecodedAudio(...)` or `FromDecodedVideo(...)` for the common raw frame
anchors, with the same selectors as `Audio(...)` and `Video(...)`. Use
`task.Describe()` plus `.From(node)` when attaching to an expert graph node.
Buffered runtime attachments and late muxed recording outputs remain planned
control-plane slices.

When a media type matches several streams, the build error lists the candidates.
Use the same stream-scoped shape with a narrower selector. `StreamIndex(0)`
selects the first stream when that is the intended one:

```go
err := goav.From(input).
    Audio(goav.StreamID("eng")).
    To(goav.FrameSink(frames)).
    Run(ctx)
```

## Generic protocol or file ingest

Receive a live protocol input, file input, or custom source; demux container data
into streams and packets when needed; then feed the same pipeline graph used by
WebRTC and RTP inputs.

The simplest supported shape is direct remux/fanout through registered format
adapters:

```go
err := goav.From(goav.FileInput("input.ivf", in)).
    To(
        goav.FileOutput("recording.ivf", recording),
        goav.FileOutput("preview.ivf", preview),
    ).
    Run(ctx)
```

For one input and packet-preserving outputs, `Record(input, output...)` is the
shorter front door over the same graph shape.

Output names are unique within a recipe, including `FrameSink` sink names.
Use distinct labels when two outputs should both receive the stream.

When packet formats already match, recording can stay packet-preserving. IVF is
the first concrete target for single-stream VP8, VP9, and AV1 packet recording;
Annex B covers packet-preserving H264 recording after RTP depacketization.

Expected graph:

```text
protocol/file source
  -> format.DemuxSource / demuxer
  -> decode
  -> optional filters
  -> encode
  -> output
```

## Multi-layer transcode

Decode once, then fan out into several video resize branches and audio resample
branches. Each branch can encode with its own bitrate, codec parameters, and
output target.

The first recipe compiler lowers the small branch DSL into the shared-decode
plan used by the runtime graph. Outputs can receive one or more named branches:

```go
err := goav.Transcode(goav.FileInput("input.webm", in)).
    Video("720p").Resize(1280, 720).VP9(2_000_000).To("archive").
    Video("360p").Resize(640, 360).VP9(600_000).To("preview").
    Audio("a96").Resample(48_000, goav.Stereo).Opus(96_000).To("archive", "preview").
    Output("archive", goav.FileOutput("archive.webm", archive)).
    Output("preview", goav.FileOutput("preview.webm", preview)).
    Run(ctx)
```

Branch names are required, unique handles; output labels are required, unique
handles. Each branch lists each output once. Route multiple branches to the
same output by reusing the output label in `.To(...)`, not by defining the
output twice. `.Output(label, ...)` defines a muxed `FileOutput` or `URIOutput`
group; decoded frame sinks stay on `Decode` or stream-scoped `From` recipes.
Branches decode implicitly before any resize, resample, or encoder step. The
containers shown here require matching demuxer and muxer adapters.

Resize and resample configs become branch-local filter stages when matching
filter factories are registered. The first concrete filters cover S16 audio
resample/channel conversion and I420/YUV420P video resize.

Recipe encode conveniences currently target Opus, VP8, and VP9. H264 and AV1
remain first-class receive/decode codec specs; recipe encode support for those
codecs is treated as work in progress until the concrete encoders are ready.

Expected graph:

```text
input
  -> demux/depacketize
  -> decode video
     -> resize 720p -> encode VP9 -> mux archive
     -> resize 360p -> encode VP9 -> mux preview
  -> decode audio
     -> resample -> encode Opus -> mux archive, mux preview
```

## Multiple outputs

One receive graph should be able to drive several sinks at once:

- live relay
- recording
- thumbnail or preview
- stats/analysis
- archival transcode

For recipe users, `Tee` is the planned split word. Use it when one selected
stream has several reusable encoded branches. Use `Transcode` when one file or
protocol input needs a declared set of muxed audio/video output groups.
Runtime `Attach` can add a stoppable stage/sink branch to a direct task graph
while it is running, which covers dynamic analysis, meters, and screenshot
collectors that consume raw decoded frames. Late muxed `FileOutput` branches
remain a planned output compiler slice.

Explicit application-owned graphs can use typed handles for this shape:

```go
graph := runtime.Graph()
src := graph.Source("source", source)
dec := graph.Stage("decode", decode)
recordOut := graph.Sink("record", record)
previewOut := graph.Sink("preview", preview)
statsOut := graph.Sink("stats", stats)

graph.Connect(src.Out(), dec.In())
graph.Connect(dec.Out(), recordOut.In(), previewOut.In(), statsOut.In())

task, err := graph.Build(ctx)
```

## Resample

Audio filters should express sample-rate, channel-count, channel-layout, and
sample-format conversion without tying the API to one implementation.
The first concrete adapter covers interleaved `s16` PCM with linear
sample-rate conversion and basic channel conversion.

## Resize

Video filters should express exact, fit, fill, and passthrough modes so the same
contract works for ABR ladders, previews, thumbnails, and recording paths.
The first concrete adapter covers planar 8-bit 4:2:0 frames with
nearest-neighbor scaling and caller-owned output planes.
