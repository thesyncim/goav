package rtpav

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
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
	// minEstimateBitrate is the floor a bandwidth estimate retargets a video
	// encoder to.
	minEstimateBitrate = 64_000
	// defaultMaxEstimateBitrate caps the estimates for a stream whose encoder
	// has no configured bitrate: libwebrtc's default maximum.
	defaultMaxEstimateBitrate = 2_500_000
)

// PacketWriter is the transport seam Send writes through: RTP out, RTCP
// both ways. ReadRTCP blocks until feedback arrives and fails once the
// writer closes. WriteRTP is called from the data path and, to retransmit,
// from the feedback loop, so it must be safe for concurrent use. DialUDP
// returns one over UDP; webrtcav adapts Pion tracks.
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
// accepts, so the planner encodes frames to one of them, and that it is
// live, so the planner paces what it is fed in real time; learns its
// streams, with their extradata, before the first packet; sends RTCP sender
// reports; answers NACK from a bounded history as it arrives; and turns PLI
// and FIR into a keyframe control and REMB into a bitrate control on the
// encoder that feeds the stream, between 64 kb/s and the bitrate the recipe
// configured (2.5 Mb/s when it set none). Closing it sends BYE and closes
// writer.
func Send(writer PacketWriter, options ...SendOption) pipeline.Sink {
	return &sender{writer: writer, config: newSendConfig(options)}
}

type sender struct {
	writer  PacketWriter
	config  sendConfig
	control atomic.Pointer[func(context.Context, control.Control) error]

	// streams is set once by OpenStreams, before the first Handle and the
	// feedback loop; after that only Handle touches their send state.
	streams []*sendStream
	result  PacketizeResult

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
	// maxBitrate is the most an estimate retargets the encoder to: the
	// recipe's configured bitrate (av.MetadataBitrate on the stream).
	maxBitrate int
}

// sentPacket is one history slot: the packet as sent, its payload copied.
// The data path writes it and the feedback loop resends it, handing it over
// through state, so neither takes a lock: a slot being resent when its turn
// to be overwritten comes keeps the older packet.
type sentPacket struct {
	state  atomic.Uint32
	packet rtp.Packet
	buf    []byte
}

// History slot states.
const (
	slotEmpty uint32 = iota
	slotWriting
	slotReady
	slotReading
)

func (s *sender) Name() string { return "rtp-out" }

// Live reports that Send consumes in real time: the planner paces a file
// routed to it.
func (s *sender) Live() bool { return true }

// DescribeNode details the node in Describe output.
func (s *sender) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.Name(), Kind: pipeline.NodeSink, Detail: "rtp send"}
}

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
			maxBitrate: configuredBitrate(stream),
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

// Handle sends one packet's RTP, then a sender report when one is due.
func (s *sender) Handle(ctx context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessagePacket || msg.Packet == nil {
		return nil
	}
	if s.streams == nil {
		return errors.New("rtpav: Send received a packet before OpenStreams")
	}
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
	if !slot.state.CompareAndSwap(slotReady, slotWriting) && !slot.state.CompareAndSwap(slotEmpty, slotWriting) {
		return
	}
	slot.buf = append(slot.buf[:0], packet.Payload...)
	slot.packet = rtp.Packet{Header: packet.Header, Payload: slot.buf}
	slot.packet.Extensions = nil
	slot.state.Store(slotReady)
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

// retransmit resends, as soon as the NACK arrives, the packets it names
// that the history still holds.
func (s *sender) retransmit(ctx context.Context, nack *rtcp.TransportLayerNack) {
	stream := s.streamBySSRC(nack.MediaSSRC)
	if stream == nil || len(stream.history) == 0 {
		return
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

func (s *sendStream) resend(ctx context.Context, writer PacketWriter, seq uint16) {
	slot := &s.history[int(seq)%len(s.history)]
	if !slot.state.CompareAndSwap(slotReady, slotReading) {
		return
	}
	if slot.packet.SequenceNumber == seq {
		_ = writer.WriteRTP(ctx, &slot.packet)
	}
	slot.state.Store(slotReady)
}

func (s *sender) streamBySSRC(ssrc uint32) *sendStream {
	for _, stream := range s.streams {
		if stream.ssrc == ssrc {
			return stream
		}
	}
	return nil
}

// feedback reads RTCP until the writer closes: a NACK resends what the
// history holds, PLI and FIR ask the stream's encoder for a keyframe, and
// REMB retargets the encoders of the streams it names.
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
				s.retransmit(ctx, p)
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
	if stream == nil || !isVideo(stream.stream) {
		return
	}
	if now := time.Now(); now.Sub(stream.lastKeyframe) >= keyframeRequestGap {
		stream.lastKeyframe = now
		s.send(ctx, control.Keyframe(stream.stream.ID).WithReason("rtcp keyframe request"))
	}
}

// remb splits a receiver's bitrate estimate across the video streams it
// names and retargets each encoder to its share, held between a floor and
// the stream's configured bitrate, when that target moves by a tenth.
func (s *sender) remb(ctx context.Context, remb *rtcp.ReceiverEstimatedMaximumBitrate) {
	var named []*sendStream
	for _, ssrc := range remb.SSRCs {
		if stream := s.streamBySSRC(ssrc); stream != nil && isVideo(stream.stream) {
			named = append(named, stream)
		}
	}
	if len(named) == 0 || remb.Bitrate <= 0 {
		return
	}
	share := int(remb.Bitrate) / len(named)
	for _, stream := range named {
		target := min(max(share, minEstimateBitrate), stream.maxBitrate)
		if stream.bitrate != 0 && abs(target-stream.bitrate)*10 < stream.bitrate {
			continue
		}
		stream.bitrate = target
		if ctrl, err := control.SetBitrate(stream.stream.ID, target); err == nil {
			s.send(ctx, ctrl.WithReason("rtcp remb"))
		}
	}
}

func (s *sender) send(ctx context.Context, ctrl control.Control) {
	if fn := s.control.Load(); fn != nil {
		_ = (*fn)(ctx, ctrl)
	}
}

// configuredBitrate is the bitrate the recipe configured the stream's
// encoder with, or the default ceiling.
func configuredBitrate(stream av.Stream) int {
	if bitrate, err := strconv.Atoi(stream.Metadata[av.MetadataBitrate]); err == nil && bitrate > 0 {
		return bitrate
	}
	return defaultMaxEstimateBitrate
}

func isVideo(stream av.Stream) bool {
	return stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo || stream.Type == "" && codecMedia(stream.Codec.ID) == av.MediaVideo
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
