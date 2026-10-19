package rtpav

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const (
	// defaultHistory is the packets per stream kept for NACK retransmission:
	// about a second of 4 Mb/s video.
	defaultHistory = 512
	// defaultReportInterval is the sender-report cadence.
	defaultReportInterval = time.Second
	// keyframeRequestGap rate-limits keyframe controls per stream: a burst
	// of PLI/FIR from one loss event asks once.
	keyframeRequestGap = 250 * time.Millisecond
	// nackQueue bounds the NACKs waiting for the data path to retransmit;
	// more are shed, and the receiver asks again.
	nackQueue = 16
)

// PacketWriter is the transport seam Send writes through: RTP out, RTCP
// both ways. ReadRTCP blocks until feedback arrives and fails once the
// writer closes. DialUDP returns one over UDP; webrtcav adapts Pion tracks.
type PacketWriter interface {
	FeedbackWriter
	WriteRTP(context.Context, *rtp.Packet) error
	ReadRTCP(context.Context) ([]rtcp.Packet, error)
	Close() error
}

// WithPayloads sets the payload type and clock rate each codec is sent with,
// looked up by codec, and limits what Send accepts to those codecs; the
// default sends Opus as 111, VP8 as 96, VP9 as 98, H264 as 102, and AV1 as 45.
func WithPayloads(payloads PayloadMap) SendOption {
	return func(c *sendConfig) { c.payloads = payloads }
}

// WithSSRC sends the stream under ssrc instead of a random one.
func WithSSRC(stream av.StreamID, ssrc uint32) SendOption {
	return func(c *sendConfig) {
		if c.ssrcs == nil {
			c.ssrcs = map[av.StreamID]uint32{}
		}
		c.ssrcs[stream] = ssrc
	}
}

// WithHistory keeps the last packets of each stream for NACK retransmission
// (default 512); 0 keeps none, for a transport that retransmits itself.
func WithHistory(packets int) SendOption {
	return func(c *sendConfig) { c.history = max(packets, 0) }
}

// WithReportInterval sets the RTCP sender-report cadence (default 1s); 0
// sends none, for a transport that reports itself.
func WithReportInterval(interval time.Duration) SendOption {
	return func(c *sendConfig) { c.reports = max(interval, 0) }
}

// defaultPayloads are the payload types Send uses without WithPayloads, in
// the order a planner prefers them: the WebRTC mandatory codecs first.
var defaultPayloads = []PayloadCodec{
	{PayloadType: 111, ClockRate: 48000, Channels: 2, Parameters: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
	{PayloadType: 96, ClockRate: 90000, Parameters: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
	{PayloadType: 102, ClockRate: 90000, Parameters: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}},
	{PayloadType: 98, ClockRate: 90000, Parameters: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo}},
	{PayloadType: 45, ClockRate: 90000, Parameters: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo}},
}

// Send is the RTP output: a media destination for goav.Sink (or a
// provider.DestinationScheme) that packetizes the packets routed to it and
// writes them through writer, one SSRC per stream. It declares the codecs it
// accepts, so the planner encodes frames to one of them; learns its streams,
// with their extradata, before the first packet; sends RTCP sender reports;
// answers NACK from a bounded history; and turns PLI and FIR into a keyframe
// control and REMB into a bitrate control on the encoder that feeds the
// stream. Closing it sends BYE and closes writer.
func Send(writer PacketWriter, options ...SendOption) pipeline.Sink {
	return &sender{writer: writer, config: newSendConfig(options), nacks: make(chan *rtcp.TransportLayerNack, nackQueue)}
}

type sender struct {
	writer  PacketWriter
	config  sendConfig
	control atomic.Pointer[func(context.Context, control.Control) error]

	// streams is set once by OpenStreams, before the first Handle and the
	// feedback loop; after that only Handle touches their send state.
	streams []*sendStream
	result  PacketizeResult
	nacks   chan *rtcp.TransportLayerNack

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// sendStream is one stream's send state, owned by the data path.
type sendStream struct {
	stream     av.Stream
	packetizer Packetizer
	payload    PayloadCodec
	ssrc       uint32
	seq        uint16
	offset     uint32
	history    []sentPacket
	packets    uint32
	octets     uint32
	reported   time.Time
	// lastKeyframe is when feedback last asked for a keyframe; only the
	// feedback loop reads it.
	lastKeyframe time.Time
	bitrate      int
}

// sentPacket is one history slot: the packet as sent, its payload copied.
type sentPacket struct {
	packet rtp.Packet
	buf    []byte
	valid  bool
}

func (s *sender) Name() string { return "rtp-out" }

// InputShapes is what Send accepts: packets of a codec it can packetize, or
// of the codecs WithPayloads names, in preference order.
func (s *sender) InputShapes() shape.Set {
	var set shape.Set
	for _, payload := range s.payloadCodecs() {
		id := payload.Parameters.ID
		set = append(set, shape.Packet(cmp.Or(payload.Parameters.Type, codecMedia(id)), id))
	}
	return set
}

// UseControl binds the task's control path, which feedback drives.
func (s *sender) UseControl(fn func(context.Context, control.Control) error) {
	if fn != nil {
		s.control.Store(&fn)
	}
}

// OpenStreams prepares one SSRC, sequence, and packetizer per stream and
// starts reading feedback.
func (s *sender) OpenStreams(ctx context.Context, streams []av.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.writer == nil {
		return ErrNilWriter
	}
	if s.streams != nil {
		return errors.New("rtpav: Send opened twice")
	}
	used := map[uint32]bool{0: true}
	for _, stream := range streams {
		payload, ok := s.payloadFor(stream.Codec.ID)
		if !ok {
			return fmt.Errorf("rtpav: send %s: %w: %s", stream.ID, ErrPacketizerNotFound, stream.Codec.ID)
		}
		if stream.Codec.ClockRate == 0 {
			stream.Codec.ClockRate = payload.ClockRate
		}
		packetizer, err := NewPacketizer(stream, WithMTU(s.config.mtu))
		if err != nil {
			return fmt.Errorf("rtpav: send %s: %w: %s", stream.ID, err, stream.Codec.ID)
		}
		ssrc, fixed := s.config.ssrcs[stream.ID]
		for !fixed && used[ssrc] {
			ssrc = rand.Uint32()
		}
		used[ssrc] = true
		s.streams = append(s.streams, &sendStream{
			stream:     stream,
			packetizer: packetizer,
			payload:    payload,
			ssrc:       ssrc,
			seq:        uint16(rand.N(1 << 16)),
			offset:     rand.Uint32(),
			history:    newHistory(s.config.history, s.config.mtu),
		})
	}
	if s.streams == nil {
		s.streams = []*sendStream{}
	}
	loop, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go s.feedback(loop)
	return nil
}

// Handle sends one packet: retransmissions first, then the packet's RTP,
// then a sender report when one is due.
func (s *sender) Handle(ctx context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessagePacket || msg.Packet == nil {
		return nil
	}
	if s.streams == nil {
		return errors.New("rtpav: Send received a packet before OpenStreams")
	}
	s.retransmit(ctx)
	stream := s.streamFor(msg.Packet.StreamID)
	if stream == nil {
		return fmt.Errorf("rtpav: send: %w: %s", ErrUnknownStream, msg.Packet.StreamID)
	}
	s.result.Reset()
	if err := stream.packetizer.PacketizeInto(ctx, msg.Packet, stream.payload, &s.result); err != nil {
		return fmt.Errorf("rtpav: send %s: %w", stream.stream.ID, err)
	}
	for i := range s.result.Packets {
		packet := &s.result.Packets[i]
		packet.SSRC = stream.ssrc
		packet.SequenceNumber = stream.seq
		packet.Timestamp += stream.offset
		stream.seq++
		if err := s.writer.WriteRTP(ctx, packet); err != nil {
			return err
		}
		stream.remember(packet)
		stream.packets++
		stream.octets += uint32(len(packet.Payload))
	}
	if len(s.result.Packets) != 0 {
		s.report(ctx, stream, s.result.Packets[0].Timestamp)
	}
	return nil
}

func (s *sender) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.config.reports > 0 && len(s.streams) != 0 {
			bye := &rtcp.Goodbye{}
			for _, stream := range s.streams {
				bye.Sources = append(bye.Sources, stream.ssrc)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = s.writer.WriteRTCP(ctx, []rtcp.Packet{bye})
			cancel()
		}
		if s.writer != nil {
			s.closeErr = s.writer.Close()
		}
		if s.done != nil {
			<-s.done
		}
	})
	return s.closeErr
}

func (s *sender) streamFor(id av.StreamID) *sendStream {
	for _, stream := range s.streams {
		if stream.stream.ID == id {
			return stream
		}
	}
	if len(s.streams) == 1 {
		return s.streams[0]
	}
	return nil
}

func (s *sender) payloadCodecs() []PayloadCodec {
	if s.config.payloads != nil {
		return s.config.payloads.Codecs()
	}
	return defaultPayloads
}

func (s *sender) payloadFor(id av.CodecID) (PayloadCodec, bool) {
	for _, payload := range s.payloadCodecs() {
		if payload.Parameters.ID == id {
			if payload.ClockRate == 0 {
				payload.ClockRate = clockOr(payload.Parameters.ClockRate, defaultClock(id))
			}
			return payload, true
		}
	}
	return PayloadCodec{}, false
}

// newHistory makes the NACK history: slots packets, their payload room cut
// from one allocation, so the data path never allocates for it.
func newHistory(slots int, mtu int) []sentPacket {
	history := make([]sentPacket, slots)
	arena := make([]byte, slots*mtu)
	for i := range history {
		history[i].buf = arena[i*mtu : i*mtu : (i+1)*mtu]
	}
	return history
}

// remember keeps the packet as sent for a NACK to retransmit.
func (s *sendStream) remember(packet *rtp.Packet) {
	if len(s.history) == 0 {
		return
	}
	slot := &s.history[int(packet.SequenceNumber)%len(s.history)]
	slot.buf = append(slot.buf[:0], packet.Payload...)
	slot.packet = rtp.Packet{Header: packet.Header, Payload: slot.buf}
	slot.packet.Extensions = nil
	slot.valid = true
}

// report sends a sender report for the stream when one is due, mapping the
// packet just sent to the wall clock.
func (s *sender) report(ctx context.Context, stream *sendStream, timestamp uint32) {
	if s.config.reports <= 0 {
		return
	}
	now := time.Now()
	if now.Sub(stream.reported) < s.config.reports {
		return
	}
	stream.reported = now
	sr := &rtcp.SenderReport{
		SSRC:        stream.ssrc,
		NTPTime:     ntpTime(now),
		RTPTime:     timestamp,
		PacketCount: stream.packets,
		OctetCount:  stream.octets,
	}
	_ = s.writer.WriteRTCP(ctx, []rtcp.Packet{sr})
}

// retransmit resends the packets queued NACKs name that the history still
// holds, on the data path that owns the history.
func (s *sender) retransmit(ctx context.Context) {
	for {
		var nack *rtcp.TransportLayerNack
		select {
		case nack = <-s.nacks:
		default:
			return
		}
		stream := s.streamBySSRC(nack.MediaSSRC)
		if stream == nil || len(stream.history) == 0 {
			continue
		}
		for _, pair := range nack.Nacks {
			stream.resend(ctx, s.writer, pair.PacketID)
			for bit := range uint16(16) {
				if pair.LostPackets&(1<<bit) != 0 {
					stream.resend(ctx, s.writer, pair.PacketID+bit+1)
				}
			}
		}
	}
}

func (s *sendStream) resend(ctx context.Context, writer PacketWriter, seq uint16) {
	slot := &s.history[int(seq)%len(s.history)]
	if slot.valid && slot.packet.SequenceNumber == seq {
		_ = writer.WriteRTP(ctx, &slot.packet)
	}
}

func (s *sender) streamBySSRC(ssrc uint32) *sendStream {
	for _, stream := range s.streams {
		if stream.ssrc == ssrc {
			return stream
		}
	}
	return nil
}

// feedback reads RTCP until the writer closes: NACKs queue for the data
// path, PLI and FIR ask the stream's encoder for a keyframe, and REMB
// retargets the encoders of the streams it names.
func (s *sender) feedback(ctx context.Context) {
	defer close(s.done)
	for {
		packets, err := s.writer.ReadRTCP(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var timeout interface{ Timeout() bool }
			if errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}
			return
		}
		for _, packet := range packets {
			switch p := packet.(type) {
			case *rtcp.TransportLayerNack:
				select {
				case s.nacks <- p:
				default:
				}
			case *rtcp.PictureLossIndication:
				s.keyframe(ctx, p.MediaSSRC)
			case *rtcp.FullIntraRequest:
				for _, entry := range p.FIR {
					s.keyframe(ctx, entry.SSRC)
				}
			case *rtcp.ReceiverEstimatedMaximumBitrate:
				s.remb(ctx, p)
			}
		}
	}
}

func (s *sender) keyframe(ctx context.Context, ssrc uint32) {
	stream := s.streamBySSRC(ssrc)
	if stream == nil || stream.stream.Type == av.MediaAudio {
		return
	}
	if now := time.Now(); now.Sub(stream.lastKeyframe) >= keyframeRequestGap {
		stream.lastKeyframe = now
		s.send(ctx, control.Keyframe(stream.stream.ID).WithReason("rtcp keyframe request"))
	}
}

// remb splits a receiver's bitrate estimate across the video streams it
// names and retargets each encoder when its share moves by a tenth.
func (s *sender) remb(ctx context.Context, remb *rtcp.ReceiverEstimatedMaximumBitrate) {
	var named []*sendStream
	for _, ssrc := range remb.SSRCs {
		if stream := s.streamBySSRC(ssrc); stream != nil && stream.stream.Type == av.MediaVideo {
			named = append(named, stream)
		}
	}
	if len(named) == 0 || remb.Bitrate <= 0 {
		return
	}
	share := int(remb.Bitrate) / len(named)
	for _, stream := range named {
		if stream.bitrate != 0 && abs(share-stream.bitrate)*10 < stream.bitrate {
			continue
		}
		stream.bitrate = share
		if ctrl, err := control.SetBitrate(stream.stream.ID, share); err == nil {
			s.send(ctx, ctrl.WithReason("rtcp remb"))
		}
	}
}

func (s *sender) send(ctx context.Context, ctrl control.Control) {
	if fn := s.control.Load(); fn != nil {
		_ = (*fn)(ctx, ctrl)
	}
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// ntpTime is t as a 64-bit NTP timestamp (RFC 3550 section 4).
func ntpTime(t time.Time) uint64 {
	const ntpEpochOffset = 2208988800
	seconds := uint64(t.Unix() + ntpEpochOffset)
	fraction := uint64(t.Nanosecond()) << 32 / uint64(time.Second)
	return seconds<<32 | fraction
}

func codecMedia(id av.CodecID) av.MediaType {
	if id == av.CodecOpus {
		return av.MediaAudio
	}
	return av.MediaVideo
}

func defaultClock(id av.CodecID) uint32 {
	if codecMedia(id) == av.MediaAudio {
		return 48000
	}
	return defaultVideoClockRate
}
