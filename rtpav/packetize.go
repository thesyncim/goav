package rtpav

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// defaultMTU is the RTP packet size WebRTC stacks send: it leaves room for
// SRTP and header extensions on a 1500-byte path.
const defaultMTU = 1200

// rtpHeaderSize is the fixed RTP header, the part of the MTU a payload cannot
// use.
const rtpHeaderSize = 12

// minMTU is the smallest packet size a packetizer accepts: a fixed header,
// the largest payload descriptor (VP9 with its scalability structure), and
// room for media.
const minMTU = rtpHeaderSize + 16

// SendOption configures the RTP send side: NewPacketizer reads WithMTU, and
// Send reads them all.
type SendOption func(*sendConfig)

type sendConfig struct {
	mtu      int
	payloads PayloadMap
	ssrcs    map[av.StreamID]uint32
	history  int
	reports  time.Duration
}

func newSendConfig(options []SendOption) sendConfig {
	config := sendConfig{mtu: defaultMTU, history: defaultHistory, reports: defaultReportInterval}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	config.mtu = max(config.mtu, minMTU)
	return config
}

// WithMTU caps every RTP packet at size bytes, its 12-byte header included
// (default 1200). Sizes below 28 are raised to 28.
func WithMTU(size int) SendOption {
	return func(c *sendConfig) { c.mtu = size }
}

// PacketizeResult is the caller-owned output buffer packetizers fill: one
// access unit's RTP packets in send order, their payloads held in one arena
// the result reuses. Payloads stay valid until Reset; a result reused across
// access units allocates only while it grows to the largest one.
type PacketizeResult struct {
	Packets []rtp.Packet
	arena   []byte
	ends    []int
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *PacketizeResult) Reset() {
	for i := range r.Packets {
		r.Packets[i] = rtp.Packet{}
	}
	r.Packets = r.Packets[:0]
	r.arena = r.arena[:0]
	r.ends = r.ends[:0]
}

// open starts the next packet; its payload is the bytes written until close.
func (r *PacketizeResult) open(payloadType uint8, timestamp uint32) {
	r.Packets = append(r.Packets, rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: payloadType, Timestamp: timestamp}})
}

func (r *PacketizeResult) write(b ...byte) { r.arena = append(r.arena, b...) }

func (r *PacketizeResult) close() { r.ends = append(r.ends, len(r.arena)) }

// seal points every packet's payload at its arena bytes, after the arena
// stopped growing, and marks the last packet when marker is set.
func (r *PacketizeResult) seal(marker bool) {
	start := 0
	for i := range r.Packets {
		r.Packets[i].Payload = r.arena[start:r.ends[i]:r.ends[i]]
		start = r.ends[i]
	}
	if marker && len(r.Packets) != 0 {
		r.Packets[len(r.Packets)-1].Marker = true
	}
}

// Packetizer splits one codec's av.Packets into RTP packets, the inverse of a
// Depacketizer: PacketizeInto appends one access unit's packets to out, with
// the payload type, timestamp (the packet's PTS on the payload clock), marker,
// and payload set. The sender owns SSRC and sequence numbers.
type Packetizer interface {
	Codec() av.CodecID
	PacketizeInto(context.Context, *av.Packet, PayloadCodec, *PacketizeResult) error
}

// NewPacketizer builds the packetizer of the stream's codec: Opus, VP8, VP9,
// H264 (single NAL, STAP-A, FU-A, with SPS/PPS before every IDR), or AV1 (OBU
// aggregation). The stream's extradata seeds the parameter sets an IDR or
// AV1 keyframe is sent with; in-band ones replace them. WithMTU bounds the
// packets. A codec with no built-in packetizer returns ErrPacketizerNotFound.
func NewPacketizer(stream av.Stream, options ...SendOption) (Packetizer, error) {
	config := newSendConfig(options)
	budget := config.mtu - rtpHeaderSize
	switch stream.Codec.ID {
	case av.CodecOpus:
		return &opusPacketizer{budget: budget, clock: clockOr(stream.Codec.ClockRate, 48000)}, nil
	case av.CodecVP8:
		return &vp8Packetizer{budget: budget, clock: clockOr(stream.Codec.ClockRate, defaultVideoClockRate), pictureID: randomPictureID()}, nil
	case av.CodecVP9:
		return &vp9Packetizer{budget: budget, clock: clockOr(stream.Codec.ClockRate, defaultVideoClockRate), pictureID: randomPictureID(), width: stream.Codec.Width, height: stream.Codec.Height}, nil
	case av.CodecH264:
		return newH264Packetizer(stream, budget), nil
	case av.CodecAV1:
		return newAV1Packetizer(stream, budget), nil
	default:
		return nil, ErrPacketizerNotFound
	}
}

func clockOr(clock uint32, fallback uint32) uint32 {
	if clock != 0 {
		return clock
	}
	return fallback
}

// rtpTimestamp is the packet's presentation time on the RTP clock, wrapped
// to 32 bits; a packet without a PTS falls back to its DTS.
func rtpTimestamp(packet *av.Packet, payload PayloadCodec, clock uint32) (uint32, error) {
	ts := packet.PTS
	if !ts.Base.Valid() {
		ts = packet.DTS
	}
	if payload.ClockRate != 0 {
		clock = payload.ClockRate
	}
	value, ok := av.RescaleValue(ts.Value, ts.Base, av.RTPTimeBase(clock))
	if !ok {
		return 0, ErrNoTimestamp
	}
	return uint32(value), nil
}

// randomPictureID starts a picture-id sequence at a random 15-bit value.
func randomPictureID() uint16 { return uint16(rand.N(0x8000)) }

// evenChunk is the fragment size that splits n bytes into the fewest
// fragments of at most limit bytes, all within one byte of each other, so the
// last packet is not a runt.
func evenChunk(n int, limit int) int {
	if n <= limit {
		return n
	}
	count := (n + limit - 1) / limit
	return (n + count - 1) / count
}
