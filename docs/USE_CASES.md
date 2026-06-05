# Use Cases

These scenarios should keep shaping the interfaces before implementation begins.

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

## RTMP ingest

Receive an RTMP live input, demux FLV-style tags into streams and packets, then
feed the same pipeline graph used by WebRTC and file inputs.

Expected graph:

```text
RTMP input
  -> FLV demux
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

