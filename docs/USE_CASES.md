# Use Cases

These scenarios keep shaping the interfaces as narrow implementation slices
land.

## WebRTC receive

Receive one or more Pion `TrackRemote` values, preserve RTP metadata, detect
loss, emit codec and track lifecycle events, depacketize, decode, and optionally
record or forward.

Expected graph:

```text
WebRTC session
  -> RTP receiver
  -> jitter/loss handling
  -> depacketizer
  -> decoder
  -> frame sinks or encoders
```

## RTP receive

Receive raw RTP using Pion RTP packet types, resolve payload type mappings,
surface gaps and discontinuities, emit RTCP feedback, and produce codec packets.

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
the first concrete target for single-stream VP8, VP9, and AV1 packet recording.

Expected graph:

```text
protocol/file source
  -> format.DemuxSource / demuxer
  -> decode
  -> branch
```

## Multi-layer transcode

Decode once, then fan out into several video resize branches and audio resample
branches. Each branch can encode with its own bitrate, codec parameters, and
output target.

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
