# RTP and WebRTC

RTP and WebRTC are first-class targets, but they stay in the transport modules.
Use this page when you need to connect Pion traffic to normal goav recipes and
want to know where packet loss, codec switches, feedback, and track replacement
surface.

`goav` uses Pion-native types for transport-level data instead of introducing
replacement RTP/RTCP/WebRTC structs: `rtpav` consumes `pion/rtp.Packet` and
`pion/rtcp.Packet`; `webrtcav` consumes `pion/webrtc/v4` `PeerConnection`,
`TrackRemote`, `RTPReceiver`, and `RTPCodecParameters`.

## Receive path

```text
Pion TrackRemote -> rtpav.PacketReader -> rtpav.JitterBuffer ->
rtpav.Depacketizer -> av.Packet -> codec.DecoderStage -> av.Frame
```

`rtpav` building blocks are deliberately small:

- `StaticPayloadMap` for payload type lookup.
- `SequenceDetector` for explicit gap state.
- `JitterRing` for bounded ordered release.
- Opus/VP8/VP9/AV1/H264 depacketizers for bounded frame assembly and
  loss-aware reset.
- `Source` for reading RTP, applying jitter/depacketizers, and emitting
  pipeline messages plus timestamp-discontinuity events.
- `FeedbackWriter`/`FeedbackResult` for NACK, PLI, and FIR with caller-owned
  scratch.

`webrtcav` building blocks keep WebRTC session concerns on the WebRTC side:
`NewSession` owns Pion receive sessions with a bounded `AcceptTrack(ctx)` queue;
`TrackSet` keeps one long-lived reader per logical stream across track
replacements; `TrackReader.UpdateCodec`/`UpdateTrack` turn renegotiation into
`EventCodecChanged`; and track metadata such as RID, SSRC, stream ID, and track
ID is preserved.

The session-level shape is:

```go
session, err := webrtcav.NewSession(ctx, webrtcav.SessionConfig{})
answer, err := session.SetRemoteDescription(ctx, offer)
remote, err := session.AcceptTrack(ctx)
err := goav.From(goav.Input(webrtcav.Track(remote.Track))).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

For multiple tracks, pass several inputs to the variadic `From`; writing them
to one container requires a muxer adapter for that container:

```go
tracks, err := webrtcav.NewTrackSet(webrtcav.TrackSetConfig{Session: session})
audio, err := tracks.Accept(ctx)
video, err := tracks.Accept(ctx)
err := goav.From(goav.Input(rtpav.Receive(audio.Reader)), goav.Input(rtpav.Receive(video.Reader))).
    To(goav.File("recording.webm", file)).
    Run(ctx)
```

When a later accepted track has the same stream ID, `TrackSet` calls
`UpdateTrack` on the existing reader and returns `TrackReplaced`, so existing
RTP sources observe the codec-change event without rebuilding the graph.

The recipe layer also accepts raw RTP packet readers directly through the
generic provider extension point: `goav.Input(provider)` adapts any source
provider. `rtpav.Receive` and `webrtcav.Track` are the built-in ones, and
external transports (SRT, NDI, ...) plug in the same way with zero goav
changes. Declare codec intent on the provider so it can choose the
depacketizer:

```go
err := goav.From(goav.Input(rtpav.Receive(video, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Each reader can be a raw RTP receiver or a `webrtcav.TrackReader`. A track
reader produced from a session routes RTCP feedback back through the peer
connection. `rtpav.WithName(...)` gives the graph a stable label; explicit
realtime input names must be distinct. Selected live streams decode to sinks,
continue into encoders and mux outputs, and accept filter stages: the same stages
used by file inputs. Single-stream RTP sources stamp EOS with the stream ID so
unrelated inputs do not flush a shared decoder. Decoder factories can provide
adapter-specific reusable state for this path; `rtpav.WithDecodeBounds(...)`
seeds payload, retained-fragment, output-count, and geometry limits (used by the
AV1 adapter for conservative scratch sizing).

## Loss

Loss is visible data, not just an error return: `av.EventPacketLoss`,
`av.EventDiscontinuity`, packet `LossBefore`/`Discontinuous`, and RTCP
feedback requests. Timestamp regressions and configured gaps become
`av.EventDiscontinuity` before the affected packet is delivered.
`rtpav.WithBufferLimits(...)` limits use zero for defaults;
`rtpav.WithMaxTimestampGap(...)` needs a positive duration with a valid
timebase.

## Codec switches

When WebRTC payload mappings or codec parameters change, goav treats it as a
media boundary, not a hidden transport detail. A new payload map appears, the
stream epoch increments, `av.EventCodecChanged` is emitted, depacketizers and
decoders reset or drain, and downstream stages drop until sync.

`TrackReader.UpdateCodec`/`UpdateTrack` emit the event; `rtpav.Source`
refreshes its payload map, adopts same-codec replacement streams (including
targeted old-ID replacement for multi-stream readers), and can hand off to a
different registered depacketizer after refresh. Selected runtime decode graphs
are stricter: they follow same-codec replacement streams and return
`codec.ErrUnsupportedCodecSwitch` when a live event would require a different
decoder factory; dynamic rebind is a future policy.

Stream recipes name the supported live policy explicitly:

```go
err := goav.From(goav.Input(webrtcav.Track(track))).
    Video().
    OnCodecChange(goav.RealtimeCodecChangePolicy()).
    To(goav.Sink(frames)).
    Run(ctx)
```

Custom codec-change policies fail during recipe build until dynamic decoder
rebind exists. Session-level code owns when renegotiation calls `UpdateCodec`.

## Feedback

Feedback stays explicit through `rtpav.FeedbackWriter`: NACK for recoverable
loss, PLI/FIR for keyframe requests, receiver reports for stats.
`rtpav.Source` accepts an explicit `FeedbackWriter` and auto-detects packet
readers that implement `WriteRTCP`, keeping RTCP writes owned by the Pion peer
connection.
