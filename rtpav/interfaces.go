package rtpav

import (
	"context"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// Pion aliases, re-exported so rtpav signatures read in one vocabulary.
type (
	// Packet is an RTP packet (pion rtp.Packet).
	Packet = rtp.Packet
	// Header is an RTP header (pion rtp.Header).
	Header = rtp.Header
	// Feedback is an RTCP packet (pion rtcp.Packet).
	Feedback = rtcp.Packet
)

// PayloadCodec maps one RTP payload type to its negotiated codec: the codec
// parameters plus the SDP facts (MIME, clock rate, channels, fmtp) the
// receiver learned out of band.
type PayloadCodec struct {
	PayloadType uint8
	Parameters  av.CodecParameters
	MIMEType    string
	ClockRate   uint32
	Channels    uint16
	FMTP        string
	Attributes  av.Metadata
}

// PayloadMap resolves RTP payload types to codecs. Epoch increments when the
// mapping renegotiates, so consumers detect codec changes without comparing
// whole maps.
type PayloadMap interface {
	Epoch() av.Epoch
	Lookup(payloadType uint8) (PayloadCodec, bool)
	Codecs() []PayloadCodec
}

// SequenceState reports what RTP sequence tracking observed around the
// current packet: missing sequence numbers, loss before this packet, and
// discontinuity.
type SequenceState struct {
	SSRC             uint32
	Cycles           uint32
	Expected         uint16
	Missing          []uint16
	MissingTruncated bool
	LossBefore       bool
	Discontinuous    bool
}

// JitterStats is a point-in-time view of a jitter buffer: occupancy, loss,
// lateness, reordering, and estimated delay.
type JitterStats struct {
	SSRC           uint32
	Buffered       int
	Expected       uint16
	Lost           uint64
	Late           uint64
	Reordered      uint64
	EstimatedDelay time.Duration
}

// JitterResult is the caller-owned output buffer jitter operations fill:
// packets released in order, events, RTCP feedback to send, and the observed
// sequence state.
type JitterResult struct {
	Ready    []*rtp.Packet
	Events   []av.Event
	Feedback []rtcp.Packet
	State    SequenceState
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *JitterResult) Reset() {
	for i := range r.Ready {
		r.Ready[i] = nil
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	for i := range r.Feedback {
		r.Feedback[i] = nil
	}
	r.Ready = r.Ready[:0]
	r.Events = r.Events[:0]
	r.Feedback = r.Feedback[:0]
	r.State = SequenceState{}
}

// JitterBuffer reorders out-of-order RTP: PushInto admits a packet and
// releases whatever became in-order, PopInto drains one ready packet,
// FlushInto drains everything at end of stream.
type JitterBuffer interface {
	PushInto(context.Context, *rtp.Packet, *JitterResult) error
	PopInto(context.Context, *rtp.Packet) (bool, error)
	FlushInto(context.Context, *JitterResult) error
	Stats() JitterStats
}

// DepacketizeResult is the caller-owned output buffer depacketizers fill:
// reassembled codec packets, events (loss, discontinuity), and RTCP feedback
// to send.
type DepacketizeResult struct {
	Packets  []av.Packet
	Events   []av.Event
	Feedback []rtcp.Packet
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *DepacketizeResult) Reset() {
	for i := range r.Packets {
		r.Packets[i].Reset()
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	for i := range r.Feedback {
		r.Feedback[i] = nil
	}
	r.Packets = r.Packets[:0]
	r.Events = r.Events[:0]
	r.Feedback = r.Feedback[:0]
}

// Depacketizer reassembles one codec's RTP payloads into av.Packets:
// PushInto consumes RTP in order (possibly emitting zero or more packets),
// FlushInto drains a partial tail, HandleEvent reacts to in-band events.
type Depacketizer interface {
	Codec() av.CodecID
	PushInto(context.Context, *rtp.Packet, PayloadCodec, *DepacketizeResult) error
	FlushInto(context.Context, *DepacketizeResult) error
	HandleEvent(context.Context, *av.Event) error
}

// FeedbackWriter sends RTCP feedback (NACK, PLI, FIR) back toward the
// sender; receivers that cannot send feedback simply do not implement it.
type FeedbackWriter interface {
	WriteRTCP(context.Context, []rtcp.Packet) error
}

// PacketReader is the transport seam Receive adapts: it declares the streams
// it carries, resolves payload types, and delivers RTP packets until io.EOF.
// Events surfaces transport-side events (loss, stream changes); nil is fine.
type PacketReader interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() PayloadMap
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	Close() error
}
