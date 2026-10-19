- RTP and WebRTC egress follow-ups. A media sink may report `Live`, and RTP
  `Send`, WHIP, and `webrtcav.LocalTrack` do: a recipe ending in one plans
  realtime, so `file in.mp4 ! uri "whips://…"` publishes at the file's media
  time instead of in one burst. REMB and TWCC bitrate targets stay between
  64 kb/s and the encoder's configured bitrate (2.5 Mb/s when none is set). A
  NACK is answered as it arrives, not on the next packet. WHIP and WHEP follow
  only 307/308 redirects (a 301/302/303 is refused rather than replayed as a
  GET), and a trickle PATCH sends `If-Match: *` when the server gave no
  entity-tag. `LocalTrack` reopens when its branch reattaches, continuing the
  track's RTP sequence; the runtime ladder example uses it.
- Breaking (webrtcav): the WHEP/WHIP options are `WithToken`,
  `WithHTTPClient`, and `WithSession`.
