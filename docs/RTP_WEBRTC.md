# RTP and WebRTC

RTP and WebRTC are first-class targets. `goav` uses Pion-native types for
transport-level data instead of introducing replacement RTP/RTCP/WebRTC structs:
`rtpav` consumes `pion/rtp.Packet` and `pion/rtcp.Packet`; `webrtcav` consumes
`pion/webrtc/v4` `PeerConnection`, `TrackRemote`, `RTPReceiver`, and
`RTPCodecParameters`.

## Receive path

```text
Pion TrackRemote -> rtpav.PacketReader -> rtpav.JitterBuffer ->
rtpav.Depacketizer -> av.Packet -> codec.DecoderStage -> av.Frame
```

`rtpav` building blocks: `StaticPayloadMap` (payload type lookup),
`SequenceDetector` (explicit gap state), `JitterRing` (bounded ordered release),
Opus/VP8/VP9/AV1/H264 depacketizers (bounded frame assembly, loss-aware reset),
`Source` (reads RTP, applies jitter/depacketizers, emits pipeline messages and
timestamp-discontinuity events), and `FeedbackWriter`/`FeedbackResult` (NACK,
PLI, FIR with caller-owned scratch).

`webrtcav` building blocks: `NewSession` (Pion receive sessions with a bounded
`AcceptTrack(ctx)` queue), `TrackSet` (one long-lived reader per logical
stream across track replacements), `TrackReader.UpdateCodec`/`UpdateTrack`
(renegotiation → `EventCodecChanged`), and preserved track metadata (RID, SSRC,
stream ID, track ID).

The session-level shape is:

```go
session, err := webrtcav.NewSession(ctx, webrtcav.SessionConfig{})
answer, err := session.SetRemoteDescription(ctx, offer)
remote, err := session.AcceptTrack(ctx)
err := goav.From(goav.WebRTCTrack(remote.Track)).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

For multiple tracks, pass several inputs to the variadic `From`; writing them to
one container requires a muxer adapter for that container:

```go
tracks, err := webrtcav.NewTrackSet(webrtcav.TrackSetConfig{Session: session})
audio, err := tracks.Accept(ctx)
video, err := tracks.Accept(ctx)
err := goav.From(goav.RTP(audio.Reader), goav.RTP(video.Reader)).
    To(goav.File("recording.webm", file)).
    Run(ctx)
```

When a later accepted track has the same stream ID, `TrackSet` calls
`UpdateTrack` on the existing reader and returns `TrackReplaced`, so existing
RTP sources observe the codec-change event without rebuilding the graph.

The recipe layer also accepts raw RTP packet readers directly. RTP inputs need
codec intent so the runtime can choose the depacketizer:

```go
err := goav.From(goav.RTP(video).Name("video").Codec(codec.VP8())).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Each reader can be a raw RTP receiver or a `webrtcav.TrackReader`. A track
reader produced from a session routes RTCP feedback back through the peer
connection. `.Name(...)` gives the graph a stable label; explicit realtime
input names must be distinct. Selected live streams decode to sinks, continue
into encoders and mux outputs, and accept filter stages — the same stages used
by file inputs. Single-stream RTP sources stamp EOS with the stream ID so
unrelated inputs do not flush a shared decoder. Decoder factories can provide
adapter-specific reusable state for this path; `WithRTPDecodeBounds(...)` seeds
payload, retained-fragment, output-count, and geometry limits (used by the AV1
adapter for conservative scratch sizing).

## Loss

Loss is visible data, not just an error return: `av.EventPacketLoss`,
`av.EventDiscontinuity`, packet `LossBefore`/`Discontinuous`, and RTCP feedback
requests. Timestamp regressions and configured gaps become
`av.EventDiscontinuity` before the affected packet is delivered. `RTPBuffer(...)`
limits use zero for defaults; `MaxTimestampGap(...)` needs a positive duration
with a valid timebase.

## Codec switches

When WebRTC payload mappings or codec parameters change: a new payload map
appears, the stream epoch increments, `av.EventCodecChanged` is emitted,
depacketizers and decoders reset or drain, and downstream stages drop until
sync. `TrackReader.UpdateCodec`/`UpdateTrack` emit the event; `rtpav.Source`
refreshes its payload map, adopts same-codec replacement streams (including
targeted old-ID replacement for multi-stream readers), and can hand off to a
different registered depacketizer after refresh. Selected runtime decode graphs
are stricter: they follow same-codec replacement streams and return
`codec.ErrUnsupportedCodecSwitch` when a live event would require a different
decoder factory; dynamic rebind is a future policy.

Stream recipes name the supported live policy explicitly:

```go
err := goav.From(goav.WebRTCTrack(track)).
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
