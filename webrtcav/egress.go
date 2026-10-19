package webrtcav

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

// LocalTrack is a WebRTC egress destination for goav.Sink: the packets
// routed to it, packetized by rtpav, go out on track, and the keyframe
// requests and bandwidth estimates sender's RTCP carries reach the encoder
// feeding the stream. The track's codec is the one it takes, so the planner
// encodes frames to it. The peer connection's interceptors answer NACK and
// send reports (Pion's default API registers both). It carries one stream
// and reads sender's RTCP; closing it leaves track and sender open, and it
// opens again when its branch reattaches, continuing the track's RTP
// sequence and timeline, so the remote peer sees one stream.
func LocalTrack(track *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender) pipeline.Sink {
	capability := webrtc.RTPCodecCapability{}
	if track != nil {
		capability = track.Codec()
	}
	id, media := codecIDFromMIME(capability.MimeType)
	// One sequence across the senders that feed the track, one per attach.
	continued := &continuity{clock: max(capability.ClockRate, 1)}
	continued.seq.Store(rand.Uint32())
	return &egress{
		name:    "webrtc-track",
		detail:  "webrtc send, codec=" + string(id),
		accepts: shape.Set{shape.Packet(media, id)},
		open: func(ctx context.Context, streams []av.Stream) (*trackWriter, []rtpav.SendOption, error) {
			if track == nil {
				return nil, nil, ErrNilTrack
			}
			if len(streams) != 1 {
				return nil, nil, fmt.Errorf("webrtcav: a local track carries one stream, not %d", len(streams))
			}
			if streams[0].Codec.ID != id {
				return nil, nil, fmt.Errorf("webrtcav: track %s sends %s, not %s", track.ID(), capability.MimeType, streams[0].Codec.ID)
			}
			writer := newTrackWriter(nil, nil)
			writer.add(senderSSRC(sender), track, sender)
			writer.tracks[0].continuity = continued
			return writer, []rtpav.SendOption{rtpav.WithSSRC(streams[0].ID, senderSSRC(sender)), rtpav.WithPayloads(trackPayloads(capability))}, nil
		},
	}
}

// egress is the media sink LocalTrack and WHIP share: it takes the codecs it
// can send, and once it knows its streams opens its transport and an rtpav
// sender over it, which does the packetizing and the feedback.
type egress struct {
	name    string
	detail  string
	accepts shape.Set
	open    func(context.Context, []av.Stream) (*trackWriter, []rtpav.SendOption, error)
	control func(context.Context, control.Control) error
	sender  pipeline.Sink
}

func (e *egress) Name() string { return e.name }

// DescribeNode details the node in Describe output.
func (e *egress) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: e.name, Kind: pipeline.NodeSink, Detail: e.detail}
}
func (e *egress) InputShapes() shape.Set { return e.accepts }

// Live reports that WebRTC egress consumes in real time: the planner paces a
// file routed to it.
func (e *egress) Live() bool { return true }

func (e *egress) UseControl(fn func(context.Context, control.Control) error) { e.control = fn }

func (e *egress) OpenStreams(ctx context.Context, streams []av.Stream) error {
	if e.sender != nil {
		return errors.New("webrtcav: " + e.name + " opened twice")
	}
	writer, options, err := e.open(ctx, streams)
	if err != nil {
		return err
	}
	// Pion's interceptors retransmit and report; the sender only reacts.
	options = append(options, rtpav.WithHistory(0), rtpav.WithReportInterval(0))
	sender := rtpav.Send(writer, options...)
	if e.control != nil {
		sender.(interface {
			UseControl(func(context.Context, control.Control) error)
		}).UseControl(e.control)
	}
	if err := sender.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(ctx, streams); err != nil {
		writer.Close()
		return err
	}
	e.sender = sender
	return nil
}

func (e *egress) Handle(ctx context.Context, msg *pipeline.Message) error {
	if e.sender == nil {
		return errors.New("webrtcav: " + e.name + " received media before OpenStreams")
	}
	return e.sender.Handle(ctx, msg)
}

// Close closes the sender; the egress opens again for a reattached branch.
func (e *egress) Close() error {
	sender := e.sender
	e.sender = nil
	if sender == nil {
		return nil
	}
	return sender.Close()
}

// trackWriter is the rtpav.PacketWriter over a peer connection's local
// tracks: RTP goes to the track whose sender owns the packet's SSRC, and the
// senders' RTCP is read into one feed.
type trackWriter struct {
	pc       *webrtc.PeerConnection
	tracks   []trackOut
	feedback chan []rtcp.Packet
	closed   chan struct{}
	// failed closes when the peer connection fails; writes then refuse.
	failed    <-chan struct{}
	onClose   func() error
	closeOnce sync.Once
}

type trackOut struct {
	ssrc  uint32
	track *webrtc.TrackLocalStaticRTP
	// continuity, when set, renumbers the packets onto the track's own
	// sequence and shifts their timestamps onto its timeline; shift is this
	// writer's, set by its first packet.
	continuity *continuity
	shift      uint32
	shifted    bool
}

// continuity is one local track's RTP sequence and timeline, kept across the
// senders that feed it in turn, so a reattached branch continues the stream
// the remote peer already plays instead of jumping to a new one.
type continuity struct {
	clock   uint32
	started atomic.Bool
	seq     atomic.Uint32
	lastTS  atomic.Uint32
	lastAt  atomic.Int64
}

// write sends packet on the track, renumbered and shifted onto it.
func (out *trackOut) write(packet *rtp.Packet) error {
	c := out.continuity
	if c == nil {
		return out.track.WriteRTP(packet)
	}
	now := time.Now()
	if !out.shifted {
		out.shifted = true
		if c.started.Load() {
			elapsed := max(now.Sub(time.Unix(0, c.lastAt.Load())), 0)
			next := c.lastTS.Load() + uint32(elapsed.Seconds()*float64(c.clock)) + 1
			out.shift = next - packet.Timestamp
		}
	}
	sent := *packet
	sent.SequenceNumber = uint16(c.seq.Add(1))
	sent.Timestamp += out.shift
	c.lastTS.Store(sent.Timestamp)
	c.lastAt.Store(now.UnixNano())
	c.started.Store(true)
	return out.track.WriteRTP(&sent)
}

func newTrackWriter(pc *webrtc.PeerConnection, failed <-chan struct{}) *trackWriter {
	return &trackWriter{pc: pc, feedback: make(chan []rtcp.Packet, 16), closed: make(chan struct{}), failed: failed}
}

// add routes ssrc to track and reads sender's RTCP until it stops.
func (w *trackWriter) add(ssrc uint32, track *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender) {
	w.tracks = append(w.tracks, trackOut{ssrc: ssrc, track: track})
	if sender == nil {
		return
	}
	go func() {
		for {
			packets, _, err := sender.ReadRTCP()
			if err != nil {
				return
			}
			select {
			case w.feedback <- packets:
			case <-w.closed:
				return
			}
		}
	}()
}

func (w *trackWriter) WriteRTP(_ context.Context, packet *rtp.Packet) error {
	select {
	case <-w.failed:
		return ErrWHIPDisconnected
	default:
	}
	for i := range w.tracks {
		if w.tracks[i].ssrc == packet.SSRC {
			return w.tracks[i].write(packet)
		}
	}
	return fmt.Errorf("webrtcav: no track sends ssrc %d", packet.SSRC)
}

func (w *trackWriter) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	if w.pc == nil {
		return nil
	}
	return w.pc.WriteRTCP(packets)
}

func (w *trackWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	select {
	case packets := <-w.feedback:
		return packets, nil
	case <-w.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *trackWriter) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.closed)
		if w.onClose != nil {
			err = w.onClose()
		}
	})
	return err
}

// senderSSRC is the SSRC Pion sends a sender's track under, which its RTCP
// feedback names.
func senderSSRC(sender *webrtc.RTPSender) uint32 {
	if sender == nil {
		return 0
	}
	if encodings := sender.GetParameters().Encodings; len(encodings) != 0 {
		return uint32(encodings[0].SSRC)
	}
	return 0
}

// trackPayloads is the one payload a track's codec sends; Pion rewrites the
// payload type to the negotiated one.
func trackPayloads(capability webrtc.RTPCodecCapability) rtpav.PayloadMap {
	id, media := codecIDFromMIME(capability.MimeType)
	return rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
		ClockRate:  capability.ClockRate,
		Channels:   capability.Channels,
		MIMEType:   capability.MimeType,
		FMTP:       capability.SDPFmtpLine,
		Parameters: av.CodecParameters{ID: id, Type: media},
	}})
}

// egressCodecs are the codecs WebRTC egress sends, in the order the planner
// prefers them: the WebRTC mandatory-to-implement codecs first.
var egressCodecs = shape.Set{
	shape.Packet(av.MediaAudio, av.CodecOpus),
	shape.Packet(av.MediaVideo, av.CodecVP8),
	shape.Packet(av.MediaVideo, av.CodecH264),
	shape.Packet(av.MediaVideo, av.CodecVP9),
	shape.Packet(av.MediaVideo, av.CodecAV1),
}

// videoFeedback is the RTCP feedback a sent video codec offers; Pion keeps
// what its media engine supports.
var videoFeedback = []webrtc.RTCPFeedback{{Type: "goog-remb"}, {Type: "ccm", Parameter: "fir"}, {Type: "nack"}, {Type: "nack", Parameter: "pli"}, {Type: "transport-cc"}}

// streamCapability is the WebRTC codec a stream is sent as.
func streamCapability(stream av.Stream) (webrtc.RTPCodecParameters, bool) {
	var c webrtc.RTPCodecCapability
	switch stream.Codec.ID {
	case av.CodecOpus:
		c = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1", RTCPFeedback: []webrtc.RTCPFeedback{{Type: "transport-cc"}}}
	case av.CodecVP8:
		c = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000, RTCPFeedback: videoFeedback}
	case av.CodecVP9:
		c = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000, SDPFmtpLine: "profile-id=0", RTCPFeedback: videoFeedback}
	case av.CodecH264:
		c = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=" + h264ProfileLevelID(stream.Codec.ExtraData.Bytes), RTCPFeedback: videoFeedback}
	case av.CodecAV1:
		c = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeAV1, ClockRate: 90000, RTCPFeedback: videoFeedback}
	default:
		return webrtc.RTPCodecParameters{}, false
	}
	return webrtc.RTPCodecParameters{RTPCodecCapability: c}, true
}

// h264ProfileLevelID is the profile-level-id an H264 stream's parameter set
// declares (avcC, or Annex B with an SPS), defaulting to constrained
// baseline 3.1.
func h264ProfileLevelID(extra []byte) string {
	switch {
	case len(extra) >= 4 && extra[0] == 1:
		return hex.EncodeToString(extra[1:4])
	default:
		for i := 0; i+7 < len(extra); i++ {
			if extra[i] == 0 && extra[i+1] == 0 && extra[i+2] == 1 && extra[i+3]&0x1f == 7 {
				return hex.EncodeToString(extra[i+4 : i+7])
			}
		}
	}
	return "42e01f"
}

// mimeName is a MIME type's codec name, as an SDP rtpmap spells it.
func mimeName(mimeType string) string {
	_, name, _ := strings.Cut(mimeType, "/")
	return name
}
