// Package rtpav moves goav media over RTP. In: a PacketReader seam over any
// transport (Pion types are used directly), jitter buffering, per-codec
// depacketizers (Opus, VP8, VP9, H264, AV1), and RTCP feedback builders;
// rtpav.Receive adapts a reader into a goav source provider for
// goav.Input(...). Out: the matching packetizers, and rtpav.Send, a media
// destination over a PacketWriter (DialUDP) with sender reports, NACK
// retransmission, and keyframe and bitrate feedback.
package rtpav
