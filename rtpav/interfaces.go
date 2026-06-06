package rtpav

import (
	"context"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

type Packet = rtp.Packet
type Header = rtp.Header
type Feedback = rtcp.Packet

type PayloadCodec struct {
	PayloadType uint8
	Parameters  av.CodecParameters
	MIMEType    string
	ClockRate   uint32
	Channels    uint16
	FMTP        string
	Attributes  av.Metadata
}

type PayloadMap interface {
	Epoch() av.Epoch
	Lookup(payloadType uint8) (PayloadCodec, bool)
	Codecs() []PayloadCodec
}

type SequenceState struct {
	SSRC             uint32
	Cycles           uint32
	Expected         uint16
	Missing          []uint16
	MissingTruncated bool
	LossBefore       bool
	Discontinuous    bool
}

type JitterStats struct {
	SSRC           uint32
	Buffered       int
	Expected       uint16
	Lost           uint64
	Late           uint64
	Reordered      uint64
	EstimatedDelay time.Duration
}

type JitterResult struct {
	Ready    []*rtp.Packet
	Events   []av.Event
	Feedback []rtcp.Packet
	State    SequenceState
}

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

type JitterBuffer interface {
	PushInto(context.Context, *rtp.Packet, *JitterResult) error
	PopInto(context.Context, *rtp.Packet) (bool, error)
	FlushInto(context.Context, *JitterResult) error
	Stats() JitterStats
}

type DepacketizeResult struct {
	Packets  []av.Packet
	Events   []av.Event
	Feedback []rtcp.Packet
}

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

type Depacketizer interface {
	Codec() av.CodecID
	PushInto(context.Context, *rtp.Packet, PayloadCodec, *DepacketizeResult) error
	FlushInto(context.Context, *DepacketizeResult) error
	HandleEvent(context.Context, *av.Event) error
}

type FeedbackWriter interface {
	WriteRTCP(context.Context, []rtcp.Packet) error
}

type PacketReader interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() PayloadMap
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	Close() error
}
