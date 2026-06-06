# RTP and WebRTC

RTP and WebRTC are first-class targets.

`goav` should use Pion-native types for transport-level data instead of
introducing replacement RTP, RTCP, or WebRTC structs.

Current package boundaries:

```text
rtpav
  uses github.com/pion/rtp.Packet
  uses github.com/pion/rtcp.Packet

webrtcav
  uses github.com/pion/webrtc/v4.PeerConnection
  uses github.com/pion/webrtc/v4.TrackRemote
  uses github.com/pion/webrtc/v4.RTPReceiver
  uses github.com/pion/webrtc/v4.RTPCodecParameters
```

## Receive path

The intended receive path is:

```text
Pion TrackRemote
  -> rtpav.Receiver
  -> rtpav.JitterBuffer
  -> rtpav.Depacketizer
  -> av.Packet
  -> codec.DecoderStage
  -> codec.Decoder
  -> av.Frame
```

Current `rtpav` building blocks:

- `StaticPayloadMap` for payload type lookup.
- `SequenceDetector` for explicit gap state.
- `JitterRing` for bounded sequence-ordered packet release.
- `OpusDepacketizer` for borrowed RTP Opus payloads into `av.Packet`.
- `VP8Depacketizer`, `VP9Depacketizer`, `AV1Depacketizer`, and
  `H264Depacketizer` for bounded frame assembly into packet-preserving video
  `av.Packet` values.
- `Source` for reading RTP packets, applying optional jitter, depacketizing, and
  emitting normal pipeline messages.
- Depacketizers receive realtime events before graph delivery, so loss-aware
  payload handlers can reset or drop incomplete frames.
- `codec.DecoderStage` for turning depacketized packet messages into decoded
  frame messages while preserving loss and lifecycle events.
- `codec.EncoderStage` for later relay/transcode branches that turn processed
  frames back into packet messages without changing the event model.

Current `webrtcav` building blocks:

- `PeerConnectionSessionFactory` and `NewSession` for Pion `PeerConnection`
  receive sessions.
- bounded `AcceptTrack(ctx)` queue with stream-added and backpressure events.
- `TrackSet` for turning accepted remote tracks into one long-lived reader per
  logical stream.
- `TrackRemoteAdapter` for Pion `TrackRemote`.
- stream and payload-map mapping from Pion `RTPCodecParameters`.
- `TrackReader.UpdateCodec(ctx, update)` for turning renegotiated Pion codec
  parameters or custom payload maps into `EventCodecChanged`.
- `TrackReader.UpdateTrack(ctx, remote)` for replacing the underlying Pion
  track for the same logical stream while emitting the same codec-change event.
- preservation of track metadata such as RID, SSRC, stream ID, and track ID.
- EOS events when the track reader reaches end-of-stream.

The session-level shape is:

```go
session, err := webrtcav.NewSession(ctx, webrtcav.SessionConfig{})
answer, err := session.SetRemoteDescription(ctx, offer)
remote, err := session.AcceptTrack(ctx)
reader, err := webrtcav.NewTrackRemoteAdapter().AdaptTrack(ctx, remote)
```

For multiple tracks, the orchestration boundary is explicit:

```go
tracks, err := webrtcav.NewTrackSet(webrtcav.TrackSetConfig{Session: session})
update, err := tracks.Accept(ctx)
reader := update.Reader
```

When a later accepted track has the same stream ID, `TrackSet` calls
`UpdateTrack` on the existing reader and returns `TrackReplaced`, so existing
RTP sources can observe the codec-change event without rebuilding the whole
application graph.

The runtime builder can compile packet-reader recording graphs directly:

```go
task, err := runtime.New().
    RTP(audio, goav.WithRTPName("audio"), goav.WithRTPDepacketizer(opus)).
    RTP(video, goav.WithRTPName("video"), goav.WithRTPDepacketizer(vp8)).
    Output(goav.Output{Name: "recording.webm", Writer: file}).
    Build(ctx)
```

Each reader can be a raw RTP receiver or a `webrtcav.TrackReader` produced from
a Pion `TrackRemote`. A track reader produced from a WebRTC session can also
route RTCP feedback back through the session peer connection. The generated
graph is one `rtpav.Source` per reader feeding shared `format.MuxStage` outputs;
rendered specs show simple node-to-node connections, and events remain visible
through the task event channel while mux stages receive packet messages for each
output.

It can also decode a selected live stream directly into frames:

```go
task, err := runtime.New().
    RTP(audio, goav.WithRTPName("audio"), goav.WithRTPDepacketizer(opus)).
    Decode(goav.SelectAudio()).
    Sink(frames).
    Build(ctx)
```

For repeated RTP/WebRTC inputs, the generated graph feeds all sources into the
selector before the decoder. Single-stream RTP sources stamp EOS with the stream
ID so unrelated inputs do not flush the selected decoder.
Optional filter stages can be inserted after `Decode(...)` and before `Sink(...)`
when their selector matches the decoded stream.

The same selected live stream can continue into an encoder and one or more mux
outputs when the target codec is explicit:

```go
task, err := runtime.New().
    RTP(audio, goav.WithRTPName("audio"), goav.WithRTPDepacketizer(opus)).
    Decode(goav.SelectAudio()).
    Filter(goav.SelectAudio(), resample).
    Encode(goav.SelectAudio(), opusEncode).
    Output(goav.Output{Name: "archive.ogg", Writer: archive}).
    Output(goav.Output{Name: "preview.ogg", Writer: preview}).
    Build(ctx)
```

## Loss

Loss is not just an error return. It should become visible as:

- `av.EventPacketLoss`
- `av.EventDiscontinuity`
- packet `LossBefore`
- packet `Discontinuous`
- RTCP feedback requests where useful

## Codec switches

WebRTC payload type mappings and codec parameters can change across
renegotiation or track replacement.

The intended model is:

1. A new payload map appears.
2. The relevant stream epoch increments.
3. `av.EventCodecChanged` is emitted.
4. Depacketizers and decoders reset or drain.
5. Downstream stages drop until sync if needed.

`TrackReader.UpdateCodec` accepts a new Pion `RTPCodecParameters` value or an
explicit payload map, increments the stream epoch when the caller does not
provide one, and emits `EventCodecChanged`. `TrackReader.UpdateTrack` applies
the same update while swapping the RTP reader to a replacement Pion track.
`rtpav.Source` refreshes its receiver payload map when it observes the event.
Matching depacketizers update their stream epoch from the event; video
depacketizers drop partial frames and request sync before emitting packets for
the new epoch.

Session-level code still owns the policy decision for when renegotiation should
call `UpdateCodec`. Accepted replacement tracks for the same stream can flow
through `TrackSet`.

## Feedback

The feedback path should stay explicit through `rtpav.FeedbackWriter`.

Initial feedback targets:

- NACK for recoverable packet loss.
- PLI/FIR for video keyframe requests.
- Receiver reports for stats and sender-side adaptation.

`rtpav.FeedbackResult` currently builds NACK, PLI, and FIR packets using
caller-owned scratch storage. Session-level code remains responsible for sending
those packets through the appropriate Pion RTCP writer.

`rtpav.Source` accepts an explicit `FeedbackWriter`, and it also auto-detects
packet readers that implement `WriteRTCP`. WebRTC track readers created from a
session use that path to keep RTCP writes owned by the Pion peer connection.
