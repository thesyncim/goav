package webrtcav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

// ErrWHEPDisconnected reports a WHEP session whose peer connection failed
// or closed while it played; goav.Reconnect reopens the input after it.
var ErrWHEPDisconnected = errors.New("webrtcav: whep session disconnected")

// WHEP is the whep scheme, a WebRTC-HTTP Egress Protocol client
// (draft-ietf-wish-whep, over the HTTP mechanics of WHIP, RFC 9725), for
// goav.URIInput: whep://host/path plays over https, whep+http://host/path
// over plain http. Register it under each name used:
//
//	runconfig.Scheme("whep", webrtcav.WHEP(webrtcav.WithToken(token)))
//
// Opening the input POSTs an SDP offer with receive-only audio and video,
// applies the answer, and starts once the first negotiated track flows; a
// track silent until later joins then. ICE candidates found after the offer
// go out by PATCH when the server accepts trickle, and STUN/TURN servers an
// answer names (Link rel="ice-server") configure the session: the first to
// name new ones is re-offered with them, later ones to that host start so.
// The input is live, one stream per track, and its
// Close DELETEs the session's resource. A dropped peer connection fails the
// input with ErrWHEPDisconnected; a refused request is a *StatusError. WHEP
// is an input scheme: WHIP publishes.
func WHEP(options ...Option) provider.InputScheme {
	return whepScheme{config: newSignalConfig(options), servers: newICEServers()}
}

type whepScheme struct {
	config  signalConfig
	servers *iceServers
}

func (s whepScheme) Input(u *url.URL) (provider.Input, error) {
	endpoint, err := endpointURL(u, "whep")
	if err != nil {
		return nil, err
	}
	return &whepInput{config: s.config, servers: s.servers, endpoint: endpoint}, nil
}

type whepInput struct {
	config   signalConfig
	servers  *iceServers
	endpoint *url.URL
}

func (*whepInput) SourceShape() shape.Spec {
	return shape.Spec{Domain: shape.DomainPacket, Realtime: true}
}

func (*whepInput) Name() string   { return "whep" }
func (*whepInput) Detail() string { return "whep receive" }

// OpenSource negotiates a session and opens its tracks as one RTP source.
func (in *whepInput) OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error) {
	session, err := negotiate(ctx, in.servers, in.endpoint.Host, in.config.session.Configuration, in.offer)
	if err != nil {
		return nil, nil, err
	}
	if err := session.awaitTracks(ctx); err != nil {
		session.Close()
		return nil, nil, err
	}
	source, streams, err := rtpav.Receive(session, rtpav.WithName("whep")).OpenSource(ctx)
	if err != nil {
		session.Close()
		return nil, nil, err
	}
	return source, streams, nil
}

// offer opens a peer connection under config and negotiates it.
func (in *whepInput) offer(ctx context.Context, config webrtc.Configuration) (*whepSession, error) {
	api := in.config.session.API
	if api == nil {
		api = webrtc.NewAPI()
	}
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}
	s := newWHEPSession(in.config, pc)
	if err := s.offer(ctx, in.endpoint); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// whepSession is one negotiated WHEP session: the peer connection, its
// signaling, and its tracks merged into one rtpav.PacketReader.
type whepSession struct {
	config signalConfig
	pc     *webrtc.PeerConnection
	sig    *signaling
	ctx    context.Context
	cancel context.CancelFunc

	arrived chan arrival
	failed  chan struct{}
	records chan whepRecord
	// events announces a track the answer did not declare (av.EventStreamAdded).
	events chan av.Event
	// declared maps the answer's sending media sections (by mid) to their
	// stream index.
	declared map[string]int

	mu        sync.Mutex
	readers   []TrackReader
	streams   []av.Stream
	payloads  []rtpav.PayloadMap
	closed    bool
	closeOnce sync.Once
	failOnce  sync.Once
}

// One bounded FIFO carries every configuration record and its packet. Each
// record retains its track-local epoch/map; aggregate epochs are not compared.
type whepRecord struct {
	packet   *rtp.Packet
	event    *av.Event
	payloads rtpav.PayloadMap
	err      error
}

func newWHEPSession(config signalConfig, pc *webrtc.PeerConnection) *whepSession {
	ctx, cancel := context.WithCancel(context.Background())
	s := &whepSession{
		config:  config,
		pc:      pc,
		sig:     newSignaling(ctx, "whep", config, pc),
		ctx:     ctx,
		cancel:  cancel,
		arrived: make(chan arrival, 4),
		failed:  make(chan struct{}),
		records: make(chan whepRecord, 64),
		events:  make(chan av.Event, 4),
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		select {
		case s.arrived <- arrival{track: track, receiver: receiver}:
		case <-s.ctx.Done():
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		// Disconnected may recover; failed and closed do not.
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.failOnce.Do(func() { close(s.failed) })
		}
	})
	return s
}

// offer POSTs a receive-only audio and video offer and applies the answer.
func (s *whepSession) offer(ctx context.Context, endpoint *url.URL) error {
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
		if _, err := s.pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			return err
		}
	}
	answer, err := s.sig.exchange(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	s.declare(answer)
	return nil
}

func (s *whepSession) linkedServers() []webrtc.ICEServer { return s.sig.linkedServers() }

// arrival is a track Pion started receiving, with its receiver.
type arrival struct {
	track    *webrtc.TrackRemote
	receiver *webrtc.RTPReceiver
}

// declare reads the answer's sending media sections into the session's
// streams and payload maps before any packet flows, so the input carries
// every negotiated track from the start and a track still silent when the
// first one flows is only a late stream.
func (s *whepSession) declare(answer string) {
	parsed, err := (&webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}).Unmarshal()
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.declared = map[string]int{}
	for _, media := range parsed.MediaDescriptions {
		_, sendonly := media.Attribute("sendonly")
		_, sendrecv := media.Attribute("sendrecv")
		if media.MediaName.Port.Value == 0 || !sendonly && !sendrecv || len(media.MediaName.Formats) == 0 {
			continue
		}
		mid, _ := media.Attribute("mid")
		kind := media.MediaName.Media
		id := kind
		if msid, ok := media.Attribute("msid"); ok {
			if fields := strings.Fields(msid); len(fields) == 2 {
				id = fields[1]
			}
		}
		if slices.ContainsFunc(s.streams, func(stream av.Stream) bool { return stream.ID == av.StreamID(id) }) {
			id += "-" + mid
		}
		codec := sdpCodec(media.Attributes, kind, media.MediaName.Formats[0])
		stream := streamFromRemoteTrack(RemoteTrack{Codec: codec, Stream: av.Stream{ID: av.StreamID(id), Name: id}})
		s.declared[mid] = len(s.streams)
		s.streams = append(s.streams, stream)
		s.payloads = append(s.payloads, trackPayloadMap(stream, rtpav.NewStaticPayloadMap(stream.Epoch, []rtpav.PayloadCodec{payloadCodecFromWebRTC(codec, stream.Codec)})))
	}
}

// sdpCodec is the codec a media section's first payload type names.
func sdpCodec(attributes []sdp.Attribute, kind, format string) webrtc.RTPCodecParameters {
	var codec webrtc.RTPCodecParameters
	if pt, err := strconv.ParseUint(format, 10, 8); err == nil {
		codec.PayloadType = webrtc.PayloadType(pt)
	}
	for _, attribute := range attributes {
		value, ok := strings.CutPrefix(attribute.Value, format+" ")
		switch {
		case !ok:
		case attribute.Key == "rtpmap":
			parts := strings.Split(value, "/")
			codec.MimeType = kind + "/" + parts[0]
			if len(parts) > 1 {
				rate, _ := strconv.ParseUint(parts[1], 10, 32)
				codec.ClockRate = uint32(rate)
			}
			if len(parts) > 2 {
				channels, _ := strconv.ParseUint(parts[2], 10, 16)
				codec.Channels = uint16(channels)
			}
		case attribute.Key == "fmtp":
			codec.SDPFmtpLine = value
		}
	}
	return codec
}

// awaitTracks waits until the first negotiated track flows and starts
// reading; the others join as they start, so a silent camera delays only
// its own stream.
func (s *whepSession) awaitTracks(ctx context.Context) error {
	s.mu.Lock()
	declared := len(s.streams)
	s.mu.Unlock()
	if declared == 0 {
		return errors.New("whep: the server sends no track")
	}
	deadline := time.NewTimer(s.config.connect)
	defer deadline.Stop()
	select {
	case first := <-s.arrived:
		if err := s.adopt(ctx, first); err != nil {
			return err
		}
	case <-s.failed:
		return ErrWHEPDisconnected
	case <-deadline.C:
		return fmt.Errorf("whep: no track started within %s", s.config.connect)
	case <-ctx.Done():
		return ctx.Err()
	}
	go s.adoptLate()
	return nil
}

// adoptLate reads the tracks that start after the first.
func (s *whepSession) adoptLate() {
	for {
		select {
		case late := <-s.arrived:
			if err := s.adopt(s.ctx, late); err != nil {
				select {
				case s.records <- whepRecord{err: err}:
				case <-s.ctx.Done():
				}
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// adopt reads one track under the stream its media section declared. A
// track the answer did not declare joins as a new stream, announced before
// its packets flow so the RTP source learns its payload types first.
func (s *whepSession) adopt(ctx context.Context, a arrival) error {
	mid := ""
	for _, transceiver := range s.pc.GetTransceivers() {
		if transceiver.Receiver() == a.receiver {
			mid = transceiver.Mid()
		}
	}
	s.mu.Lock()
	index, declared := s.declared[mid]
	remote := RemoteTrack{Track: a.track, Feedback: s}
	if declared {
		remote.Stream = av.Stream{ID: s.streams[index].ID, Name: s.streams[index].Name}
	}
	s.mu.Unlock()
	reader, err := NewTrackRemoteAdapter().AdaptTrack(ctx, remote)
	if err != nil {
		return err
	}
	streams, err := reader.Streams(ctx)
	if err != nil || len(streams) == 0 {
		reader.Close()
		return ErrUnknownStream
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		reader.Close()
		return ErrClosed
	}
	if !declared && slices.ContainsFunc(s.streams, func(stream av.Stream) bool { return stream.ID == streams[0].ID }) {
		s.mu.Unlock()
		reader.Close()
		return ErrStreamExists
	}
	s.readers = append(s.readers, reader)
	var added *av.Event
	if declared {
		s.payloads[index] = reader.PayloadMap()
	} else {
		s.streams = append(s.streams, streams[0])
		s.payloads = append(s.payloads, reader.PayloadMap())
		stream := streams[0]
		added = &av.Event{Type: av.EventStreamAdded, StreamID: stream.ID, Stream: &stream, Epoch: stream.Epoch}
		select {
		case s.events <- *cloneReadEvent(added):
		default:
		}
	}
	s.mu.Unlock()
	return s.startTrack(reader, added)
}

// startTrack publishes a late stream before starting its packet pump. Queue
// backpressure applies to configuration too; cancellation releases both waits.
func (s *whepSession) startTrack(reader TrackReader, added *av.Event) error {
	if added != nil {
		select {
		case s.records <- whepRecord{event: cloneReadEvent(added), payloads: reader.PayloadMap()}:
		case <-s.ctx.Done():
			return ErrClosed
		}
	}
	go s.pump(reader)
	return nil
}

// pump moves one track's packets into the merged reader.
func (s *whepSession) pump(reader TrackReader) {
	for {
		packet, event, payloads, err := reader.ReadRTP(s.ctx)
		if packet != nil {
			packet = packet.Clone()
		}
		event = cloneReadEvent(event)
		select {
		case s.records <- whepRecord{packet: packet, event: event, payloads: payloads, err: err}:
		case <-s.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// Streams, PayloadMap, ReadRTP, Events, WriteRTCP, and Close make the
// session the rtpav.PacketReader of its tracks.
func (s *whepSession) Streams(context.Context) ([]av.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	streams := make([]av.Stream, len(s.streams))
	for i, stream := range s.streams {
		streams[i] = cloneReadStream(stream)
	}
	return streams, nil
}

func (s *whepSession) PayloadMap() rtpav.PayloadMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(mergedPayloads(nil), s.payloads...)
}

func (s *whepSession) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	select {
	case record := <-s.records:
		if record.err != nil {
			if s.isClosed() {
				record.err = io.EOF
			} else if errors.Is(record.err, io.EOF) {
				record.err = ErrWHEPDisconnected
			}
		}
		return record.packet, record.event, record.payloads, record.err
	case <-s.failed:
		if s.isClosed() {
			return nil, nil, nil, io.EOF
		}
		return nil, nil, nil, ErrWHEPDisconnected
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, nil, nil, io.EOF
	}
}

func (s *whepSession) Events() <-chan av.Event { return s.events }

func (s *whepSession) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	if len(packets) == 0 || s.isClosed() {
		return nil
	}
	return s.pc.WriteRTCP(packets)
}

func (s *whepSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close ends the session: DELETE its resource, then close the peer
// connection and the tracks.
func (s *whepSession) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		readers := append([]TrackReader(nil), s.readers...)
		close(s.events)
		s.mu.Unlock()
		err = s.sig.close()
		s.cancel()
		for _, reader := range readers {
			reader.Close()
		}
		if closeErr := s.pc.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}

// mergedPayloads is the session inventory. Epoch is an inventory maximum,
// never the generation of a read. Ambiguous PT lookup is refused; each ordered
// read carries its own track's map, including when tracks share a PT and codec.
type mergedPayloads []rtpav.PayloadMap

func (m mergedPayloads) Epoch() av.Epoch {
	var epoch av.Epoch
	for _, payloads := range m {
		epoch = max(epoch, payloads.Epoch())
	}
	return epoch
}

func (m mergedPayloads) Lookup(payloadType uint8) (rtpav.PayloadCodec, bool) {
	var found rtpav.PayloadCodec
	matched := false
	for _, payloads := range m {
		if codec, ok := payloads.Lookup(payloadType); ok {
			if matched {
				return rtpav.PayloadCodec{}, false
			}
			found, matched = codec, true
		}
	}
	return found, matched
}

func (m mergedPayloads) Codecs() []rtpav.PayloadCodec {
	var codecs []rtpav.PayloadCodec
	for _, payloads := range m {
		codecs = append(codecs, payloads.Codecs()...)
	}
	return codecs
}
