// Package ts reads and writes MPEG-2 transport streams (ISO/IEC 13818-1),
// the container broadcast, SRT, and UDP carry: H.264 and HEVC as Annex B
// access units, AAC as ADTS, and Opus in its transport stream mapping
// (registration "Opus", a control header per packet).
//
// The muxer writes one program: a PAT and PMT at the start, before every
// video keyframe, and at least every 100ms; PES packets with PTS and DTS; a
// PCR on the video PID (the first PID without video) at most 40ms apart;
// random-access indicators on keyframes; and, with MuxerOptions.MuxRate, null
// packets that hold the stream at a constant bitrate. H.264 and HEVC access
// units gain an access unit delimiter, and keyframes their parameter sets,
// when they lack them.
//
// The demuxer follows the first program through PAT and PMT version
// changes, reassembles PES packets, unwraps the 33-bit timestamps, resyncs
// after garbage, and reports continuity-counter loss (Packet.LossBefore and
// Packet.Corrupt), signalled discontinuities (av.EventDiscontinuity), and
// streams a later PMT adds or drops (av.EventStreamAdded, EventStreamRemoved).
// Stream ids are PIDs in decimal; the muxer keeps a stream's PID when its id
// is one.
package ts
