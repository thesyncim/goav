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
- `Source` for reading RTP packets, applying optional jitter, depacketizing, and
  emitting normal pipeline messages.
- `codec.DecoderStage` for turning depacketized packet messages into decoded
  frame messages while preserving loss and lifecycle events.

Current `webrtcav` building blocks:

- `TrackRemoteAdapter` for Pion `TrackRemote`.
- stream and payload-map mapping from Pion `RTPCodecParameters`.
- preservation of track metadata such as RID, SSRC, stream ID, and track ID.
- EOS events when the track reader reaches end-of-stream.

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

## Feedback

The feedback path should stay explicit through `rtpav.FeedbackWriter`.

Initial feedback targets:

- NACK for recoverable packet loss.
- PLI/FIR for video keyframe requests.
- Receiver reports for stats and sender-side adaptation.

`rtpav.FeedbackResult` currently builds NACK, PLI, and FIR packets using
caller-owned scratch storage. Session-level code remains responsible for sending
those packets through the appropriate Pion RTCP writer.

`rtpav.Source` accepts an explicit `FeedbackWriter`, so WebRTC sessions can own
RTCP writes while track readers remain packet-only boundaries.
