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
task, err := goav.Record(
    goav.WebRTCTrack(track),
    goav.FileOutput("recording.ivf", file),
).Build(ctx)
```

Repeated realtime tracks compose through the same front door:

```go
task, err := goav.From(goav.WebRTCTrack(audio)).
    And(goav.WebRTCTrack(video)).
    To(goav.FileOutput("call.webm", file)).
    Build(ctx)
```

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
task, err := goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.FileOutput("recording.ivf", file),
).Build(ctx)
```

For multiple live readers, use `From(first).And(other...)` instead of wiring
separate graph sources by hand.

The same receive boundary can decode a selected stream when a matching decoder
factory is registered, and can continue into filters, an explicit target
encoder, and one or more mux outputs:

```go
task, err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Decode().
    To(goav.FrameSink(frames)).
    Build(ctx)
```

## Generic protocol or file ingest

Receive a live protocol input, file input, or custom source; demux container data
into streams and packets when needed; then feed the same pipeline graph used by
WebRTC and RTP inputs.

The simplest supported shape is direct remux/fanout through registered format
adapters:

```go
task, err := goav.From(goav.FileInput("input.ivf", in)).
    To(
        goav.FileOutput("recording.ivf", recording),
        goav.FileOutput("preview.ivf", preview),
    ).
    Build(ctx)
```

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
task, err := goav.Transcode(goav.FileInput("input.webm", in)).
    Video("720p").Resize(1280, 720).VP9(2_000_000).To("archive").
    Video("360p").Resize(640, 360).VP9(600_000).To("preview").
    Output("archive", goav.FileOutput("archive.webm", archive)).
    Output("preview", goav.FileOutput("preview.webm", preview)).
    Build(ctx)
```

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
  -> decode
  -> branch
     -> resize 1080p -> encode -> output A
     -> resize 720p  -> encode -> output B
     -> resize 360p  -> encode -> output C
```

## Multiple outputs

One receive graph should be able to drive several sinks at once:

- live relay
- recording
- thumbnail or preview
- stats/analysis
- archival transcode

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
