- Added FLV (`container/flv`, in `bundle`): a demuxer and muxer for H.264 and
  AAC with legacy tags and Enhanced RTMP v2 (E-FLV) HEVC, AV1, VP9, VP8, Opus,
  and FLAC, ModEx, and multitrack; `file x.flv` reads and writes. Its AMF0 is
  `container/flv/amf0`.
- Added the `rtmpav` nested module: `rtmpav.RTMP()` is the rtmp and rtmps
  scheme (publish with `goav.URI`, play with `goav.URIInput` under
  `goav.Reconnect`), negotiating Enhanced RTMP codecs, and `rtmpav.Listen` an
  ingest for OBS and FFmpeg publishers. `cmd/goav` grants rtmp and rtmps.
- A media sink may implement `EncodeShapes`, the codecs frames encode to when
  it takes more as packets. Reports redact an RTMP URL's stream key and query.
