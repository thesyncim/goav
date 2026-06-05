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
	SSRC          uint32
	Cycles        uint32
	Expected      uint16
	Missing       []uint16
	LossBefore    bool
	Discontinuous bool
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

type JitterBuffer interface {
	Push(context.Context, *rtp.Packet) (JitterResult, error)
	Pop(context.Context) (*rtp.Packet, error)
	Flush(context.Context) ([]*rtp.Packet, error)
	Stats() JitterStats
}

type DepacketizeResult struct {
	Packets  []av.Packet
	Events   []av.Event
	Feedback []rtcp.Packet
}

type Depacketizer interface {
	Codec() av.CodecID
	Push(context.Context, *rtp.Packet, PayloadCodec) (DepacketizeResult, error)
	Flush(context.Context) (DepacketizeResult, error)
	HandleEvent(context.Context, av.Event) error
}

type FeedbackWriter interface {
	WriteRTCP(context.Context, []rtcp.Packet) error
}

type Receiver interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() PayloadMap
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	FeedbackWriter
	Close() error
}

type ReceiverFactory interface {
	NewReceiver(context.Context, ReceiverConfig) (Receiver, error)
}

type ReceiverConfig struct {
	Streams       []av.Stream
	Payloads      PayloadMap
	Jitter        JitterBuffer
	Depacketizers []Depacketizer
	Feedback      FeedbackWriter
	Metadata      av.Metadata
}
