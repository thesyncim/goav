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
  -> rtpav.PacketReader
  -> rtpav.FeedbackWriter
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
- `Source` can emit timestamp discontinuity events when depacketized packet
  timestamps move backward or exceed a configured max gap.
- Depacketizers receive realtime events before graph delivery, so loss-aware
  payload handlers can reset or drop incomplete frames.
- `codec.DecoderStage` for turning depacketized packet messages into decoded
  frame messages while preserving loss and lifecycle events.
- `codec.EncoderStage` for later relay/transcode branches that turn processed
  frames back into packet messages without changing the event model.

Current `webrtcav` building blocks:

- `NewSession` for Pion `PeerConnection` receive sessions.
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
err := goav.Record(
    goav.WebRTCTrack(remote.Track),
    goav.FileOutput("recording.ivf", file),
).Run(ctx)
```

For multiple tracks, the orchestration boundary is explicit. Writing them to one
container requires a muxer adapter for that container:

```go
tracks, err := webrtcav.NewTrackSet(webrtcav.TrackSetConfig{Session: session})
audio, err := tracks.Accept(ctx)
video, err := tracks.Accept(ctx)
err := goav.From(goav.RTP(audio.Reader)).
    And(goav.RTP(video.Reader)).
    To(goav.FileOutput("recording.webm", file)).
    Run(ctx)
```

When a later accepted track has the same stream ID, `TrackSet` calls
`UpdateTrack` on the existing reader and returns `TrackReplaced`, so existing
RTP sources can observe the codec-change event without rebuilding the whole
application graph.

The recipe layer can also accept raw RTP packet readers directly:

```go
err := goav.Record(
    goav.RTP(video).Name("video").Codec(goav.VP8()),
    goav.FileOutput("recording.ivf", file),
).Run(ctx)
```

Each reader can be a raw RTP receiver or a `webrtcav.TrackReader` produced from
a Pion `TrackRemote`. Repeated realtime inputs use the same recipe shape:
`goav.From(first).And(second...)`. A track reader produced from a WebRTC session
can also route RTCP feedback back through the session peer connection. Built-in
RTP codec intent is lowered after `PacketReader.Streams(ctx)` is available, so
single-stream readers can provide the packet stream identity; `.Name(...)`
remains useful when the graph and packets need a stable label independent of
reader metadata. Explicit realtime input names must be distinct. The generated
graph is one `rtpav.Source` per reader feeding shared `format.MuxStage` outputs;
graph specs show simple node-to-node routes, and events remain visible through
the task event channel while mux stages receive packet messages for each output.

It can also decode a selected live stream directly into frames:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Audio().
    To(goav.FrameSink(frames)).
    Run(ctx)
```

For repeated RTP/WebRTC inputs, the generated graph feeds all sources into the
selector before the decoder. Single-stream RTP sources stamp EOS with the stream
ID so unrelated inputs do not flush the selected decoder.
Optional filter stages can be inserted on the stream chain before
`.To(goav.FrameSink(...))` when their selector matches the decoded stream.
Decoder factories can optionally provide adapter-specific reusable state for
this high-level path. `WithRTPDecodeBounds(...)` lets an RTP input seed payload,
retained-fragment, output-count, and geometry limits into that state provider.
That lets the AV1 adapter bind conservative scratch and a worker pool for
stream-scoped RTP decode recipes while applications with exact stream knowledge
can still pass tuned state through the lower-level codec API.
The high-level AV1 path receives depacketized packets from `rtpav`; lower-level
callers that intentionally keep raw AV1 RTP aggregation payload bytes can use
the tagged concrete decoder's `DecodeRTPPayloadInto` method.

The same selected live stream can continue into an encoder and one or more mux
outputs when the target codec is explicit:

```go
err := goav.From(goav.RTP(audio).Name("audio").Codec(goav.Opus())).
    Audio().
    Do(resample).
    Opus(96_000).
    To(
        goav.FileOutput("archive.ogg", archive),
        goav.FileOutput("preview.ogg", preview),
    ).
    Run(ctx)
```

## Loss

Loss is not just an error return. It should become visible as:

- `av.EventPacketLoss`
- `av.EventDiscontinuity`
- packet `LossBefore`
- packet `Discontinuous`
- RTCP feedback requests where useful

Timestamp regressions and configured timestamp gaps also become
`av.EventDiscontinuity` before the affected packet is delivered downstream.
`RTPBuffer(...)` limits use zero for defaults and positive values for explicit
bounds; `MaxTimestampGap(...)` needs a positive duration with a valid timebase
when enabled.

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
For single-stream receivers, a codec-change event that carries a replacement
stream also updates source stream identity, timestamp tracking, and EOS
identity. Multi-stream sources can also accept a targeted replacement when
`Event.StreamID` names the old stream and `Event.Stream` carries the replacement
identity; after the source accepts it, downstream events and packets use the
replacement stream ID. Matching video depacketizers adopt the replacement stream
when the codec still matches, drop partial frames, and request sync before
emitting packets for the new epoch. If a payload-map refresh changes the codec
and a matching depacketizer is present, `rtpav.Source` can emit packets for the
new codec. Selected runtime decode graphs are intentionally stricter: they
follow same-codec replacement streams, keep ID-pinned selectors strict, and
return `codec.ErrUnsupportedCodecSwitch` when a live event would require a
different decoder factory. AV1 decode currently sync-gates depacketized
low-overhead OBU packets after loss until a packet keyframe marker or parseable
sequence-header/key-frame payload appears. The concrete tagged decoder also has
a raw AV1 RTP payload path that retains fragments across payloads and can
recover after loss while preserving known sequence state. The high-level
stream-scoped RTP decode recipe covers same-stream and replacement-stream AV1
codec changes with payload-map refresh, old-ID or replacement-ID event
targeting, and resumed decode on the next sync packet; dynamic graph rebind for
new-codec switches is still a future policy.

Stream recipes can name the supported live policy explicitly:

```go
err := goav.From(goav.WebRTCTrack(track)).
    Video().
    OnCodecChange(goav.RealtimeCodecChangePolicy()).
    To(goav.FrameSink(frames)).
    Run(ctx)
```

Custom codec-change policies fail during recipe build until dynamic decoder
rebind and opt-out sync behavior are implemented.

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
