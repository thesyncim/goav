// Package rtmpav moves media between goav and RTMP and RTMPS peers:
// publishing to YouTube, Twitch, or a local MediaMTX or nginx-rtmp, playing
// from them, and taking OBS's publish. RTMP is the protocol here, in the
// standard library alone (the handshake, signed and simple, the chunk
// stream, AMF0 commands, and Enhanced RTMP's codec negotiation); the media
// is FLV, which goav's container/flv reads and writes, Enhanced RTMP's
// HEVC, AV1, VP9, Opus, and multitrack included.
//
// RTMP is the rtmp and rtmps URL scheme, registered with runconfig.Scheme:
// goav.URI publishes, goav.URIInput plays. Listen is an ingest server whose
// Input is the live input of one stream.
package rtmpav
