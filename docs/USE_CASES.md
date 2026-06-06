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
task, err := runtime.New().
    RTP(audio, goav.WithRTPDepacketizer(opus)).
    RTP(video, goav.WithRTPDepacketizer(vp8)).
    Output(goav.Output{Name: "recording.webm"}).
    Build(ctx)
```

The same receive boundary can decode a selected stream when a matching decoder
factory is registered, and can continue into filters, an explicit target
encoder, and one or more mux outputs:

```go
task, err := runtime.New().
    RTP(audio, goav.WithRTPDepacketizer(opus)).
    Decode(goav.SelectAudio()).
    Filter(goav.SelectAudio(), meter).
    Encode(goav.SelectAudio(), opusEncode).
    Output(goav.Output{Name: "archive.ogg"}).
    Build(ctx)
```

## Generic protocol or file ingest

Receive a live protocol input, file input, or custom source; demux container data
into streams and packets when needed; then feed the same pipeline graph used by
WebRTC and RTP inputs.

The simplest supported shape is direct remux/fanout through registered format
adapters:

```go
task, err := runtime.New().
    Input(goav.Input{Name: "input"}).
    Output(goav.Output{Name: "recording"}).
    Output(goav.Output{Name: "preview"}).
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

The first plan compiler covers the shared-decode and multiple-encode/output
part of that shape. Outputs can receive every rendition or select a subset by
rendition name or label:

```go
plan := transcode.Plan{
    Input: goav.Input{Name: "input"},
    Renditions: []transcode.Rendition{
        {Name: "main", Selector: goav.SelectVideo(), Encode: mainEncode, Labels: []string{"archive"}},
        {Name: "preview", Selector: goav.SelectVideo(), Encode: previewEncode, Labels: []string{"preview"}},
    },
    Outputs: []transcode.Output{
        {Name: "archive.webm", Renditions: []string{"archive"}},
        {Name: "preview.webm", Renditions: []string{"preview"}},
    },
}
task, err := runtime.New().Transcode(plan).Build(ctx)
```

Resize and resample configs remain plan-level contracts until filter stage
factories land.

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

## Resample

Audio filters should express sample-rate, channel-count, channel-layout, and
sample-format conversion without tying the API to one implementation.

## Resize

Video filters should express exact, fit, fill, and passthrough modes so the same
contract works for ABR ladders, previews, thumbnails, and recording paths.
