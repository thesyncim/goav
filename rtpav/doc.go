// Package rtpav turns RTP into goav media: a PacketReader seam over any
// transport (Pion types are used directly), jitter buffering, per-codec
// depacketizers (Opus, VP8, VP9, H264, AV1), and RTCP feedback builders.
// rtpav.Receive adapts a reader into a goav source provider, so RTP inputs
// plug into From(goav.Input(...)) like any other input.
package rtpav
