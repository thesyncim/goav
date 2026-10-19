- RTP and WebRTC egress. `rtpav.NewPacketizer` packetizes H264 (single NAL,
  STAP-A, FU-A, with SPS/PPS before every IDR), VP8, VP9, AV1, and Opus with no
  allocation per packet, each round-tripping through its depacketizer.
  `rtpav.Send` is an RTP output over a `PacketWriter` (`rtpav.DialUDP`) with
  sender reports, NACK retransmission, and PLI/FIR and REMB turned into keyframe
  and bitrate controls on the encoder feeding the stream.
  `webrtcav.LocalTrack` sends a stream on a Pion local track, and
  `webrtcav.WHIP` is an RFC 9725 client for `goav.URI("whip://…")` (also
  `whips`, `whip+http`): Bearer auth, trickle PATCH, Link ice-server, TWCC
  bandwidth estimates, and DELETE on close. `goav run` grants whip URLs.
- Media destinations (a `pipeline.Sink` behind `goav.Sink` or a URL scheme)
  may declare what they take (`InputShapes`), learn their streams
  (`OpenStreams`), and use the task's control path (`UseControl`). The planner
  encodes frames routed to one to its first codec with a registered encoder,
  and refuses other packets with the exact fix.
- Breaking (webrtcav): WHEP's options are now shared with WHIP:
  `WHEPToken`/`WHEPClient`/`WHEPSession` are `Token`/`HTTPClient`/`PeerConfig`
  (type `Option`), and a refused request is a `*webrtcav.StatusError`.
