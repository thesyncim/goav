package webrtcav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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

// WHEPOption configures WHEP.
type WHEPOption func(*whepConfig)

type whepConfig struct {
	token   string
	client  *http.Client
	session SessionConfig
	// gather bounds the wait for ICE candidates before the offer is sent;
	// candidates found later are trickled when the server accepts them.
	gather time.Duration
	// tracks bounds the wait for the negotiated tracks' first packets.
	tracks time.Duration
}

// WHEPToken sends token as the Bearer credential on every WHEP request
// (POST, PATCH, DELETE), so the URL carries none.
func WHEPToken(token string) WHEPOption {
	return func(c *whepConfig) { c.token = token }
}

// WHEPClient sends the WHEP requests through client; the default bounds each
// request to 15s.
func WHEPClient(client *http.Client) WHEPOption {
	return func(c *whepConfig) { c.client = client }
}

// WHEPSession configures the Pion peer connection: its API (media engine,
// interceptors, setting engine) and Configuration (ICE servers).
func WHEPSession(config SessionConfig) WHEPOption {
	return func(c *whepConfig) { c.session = config }
}

// WHEP is the whep scheme, a WebRTC-HTTP Egress Protocol client
// (draft-ietf-wish-whep, over the HTTP mechanics of WHIP, RFC 9725), for
// goav.URIInput: whep://host/path plays over https, whep+http://host/path
// over plain http. Register it under each name used:
//
//	runconfig.Scheme("whep", webrtcav.WHEP(webrtcav.WHEPToken(token)))
//
// Opening the input POSTs an SDP offer with receive-only audio and video,
// applies the answer, and starts once the first negotiated track flows; a
// track silent until later joins then. ICE candidates found after the offer
// go out by PATCH when the server accepts trickle, and STUN/TURN servers an
// answer names (Link rel="ice-server") configure the session: the first to
// name new ones is re-offered with them, later ones to that host start so.
// The input is live, one stream per track (one track per codec), and its
// Close DELETEs the session's resource. A dropped peer connection fails the
// input with ErrWHEPDisconnected. WHEP is an input scheme: it plays media in
// (publishing needs a WHIP client).
func WHEP(options ...WHEPOption) provider.InputScheme {
	config := whepConfig{gather: 2 * time.Second, tracks: 10 * time.Second}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.client == nil {
		config.client = &http.Client{Timeout: 15 * time.Second}
	}
	return whepScheme{config: config, servers: &iceServers{byHost: map[string][]webrtc.ICEServer{}}}
}

type whepScheme struct {
	config  whepConfig
	servers *iceServers
}

// iceServers remembers, per endpoint host, the STUN/TURN servers its WHEP
// answers named (Link rel="ice-server"), so later sessions configure them
// before their offer.
type iceServers struct {
	mu     sync.Mutex
	byHost map[string][]webrtc.ICEServer
}

func (c *iceServers) get(host string) []webrtc.ICEServer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byHost[host]
}

func (c *iceServers) put(host string, servers []webrtc.ICEServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byHost[host] = servers
}

func (s whepScheme) Input(u *url.URL) (provider.Input, error) {
	if u.Host == "" {
		return nil, errors.New("whep URL has no host")
	}
	endpoint := *u
	endpoint.Scheme = "https"
	if strings.HasSuffix(strings.ToLower(u.Scheme), "+http") {
		endpoint.Scheme = "http"
	}
	return &whepInput{config: s.config, servers: s.servers, endpoint: &endpoint}, nil
}

type whepInput struct {
	config   whepConfig
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
	session, err := in.negotiate(ctx)
	if err != nil {
		return nil, nil, err
	}
	source, streams, err := rtpav.Receive(session, rtpav.WithName("whep")).OpenSource(ctx)
	if err != nil {
		session.Close()
		return nil, nil, err
	}
	return source, streams, nil
}

// negotiate opens a session. The ICE servers an answer names (Link
// rel="ice-server", RFC 9725 section 4.6) only help candidates gathered
// after them, so a session that learns new ones is replaced, once, by one
// configured with them before its offer; later sessions to the host start
// with them. Servers the caller configured (WHEPSession) take precedence.
func (in *whepInput) negotiate(ctx context.Context) (*whepSession, error) {
	config := in.config.session.Configuration
	own := len(config.ICEServers) != 0
	if !own {
		config.ICEServers = in.servers.get(in.endpoint.Host)
	}
	s, err := in.offer(ctx, config)
	if err != nil {
		return nil, err
	}
	if learned := s.linked; !own && len(learned) != 0 && !slices.EqualFunc(learned, config.ICEServers, sameICEServer) {
		in.servers.put(in.endpoint.Host, learned)
		s.Close()
		config.ICEServers = learned
		if s, err = in.offer(ctx, config); err != nil {
			return nil, err
		}
	}
	if err := s.awaitTracks(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
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

func sameICEServer(a, b webrtc.ICEServer) bool {
	return slices.Equal(a.URLs, b.URLs) && a.Username == b.Username && a.Credential == b.Credential
}

// whepSession is one negotiated WHEP session: the peer connection, its
// resource URL, and its tracks merged into one rtpav.PacketReader.
type whepSession struct {
	config whepConfig
	pc     *webrtc.PeerConnection
	ctx    context.Context
	cancel context.CancelFunc

	arrived  chan arrival
	failed   chan struct{}
	packets  chan *rtp.Packet
	readErrs chan error
	// events announces a track the answer did not declare (av.EventStreamAdded).
	events chan av.Event
	// declared maps the answer's sending media sections (by mid) to their
	// stream index.
	declared map[string]int
	// linked is the ICE servers the answer named (Link rel="ice-server").
	linked []webrtc.ICEServer

	wake chan struct{}

	mu       sync.Mutex
	resource *url.URL
	etag     string
	// trickle holds candidates gathered after the offer went out, and
	// trickleEnd the end of gathering, until the trickle loop PATCHes them.
	trickle    []webrtc.ICECandidateInit
	trickleEnd bool
	patches    bool
	posted     bool
	readers    []TrackReader
	streams    []av.Stream
	payloads   []rtpav.PayloadMap
	closed     bool
	closeOnce  sync.Once
	failOnce   sync.Once
}

func newWHEPSession(config whepConfig, pc *webrtc.PeerConnection) *whepSession {
	ctx, cancel := context.WithCancel(context.Background())
	s := &whepSession{
		config:   config,
		pc:       pc,
		ctx:      ctx,
		cancel:   cancel,
		arrived:  make(chan arrival, 4),
		failed:   make(chan struct{}),
		packets:  make(chan *rtp.Packet, 64),
		readErrs: make(chan error, 4),
		events:   make(chan av.Event, 4),
		wake:     make(chan struct{}, 1),
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
	pc.OnICECandidate(s.candidate)
	return s
}

// offer POSTs the offer and applies the answer.
func (s *whepSession) offer(ctx context.Context, endpoint *url.URL) error {
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
		if _, err := s.pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			return err
		}
	}
	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathered := webrtc.GatheringCompletePromise(s.pc)
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	// Half trickle: wait briefly for candidates, then send what there is.
	wait := time.NewTimer(s.config.gather)
	select {
	case <-gathered:
	case <-wait.C:
	case <-ctx.Done():
		wait.Stop()
		return ctx.Err()
	}
	wait.Stop()
	// The offer carries every candidate found so far; only later ones trickle.
	s.mu.Lock()
	local := s.pc.LocalDescription().SDP
	s.trickle, s.trickleEnd = nil, false
	s.mu.Unlock()

	resp, err := s.request(ctx, http.MethodPost, endpoint, "application/sdp", []byte(local))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	answer, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("whep: POST offer answered %s", resp.Status)
	}
	location, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		return errors.New("whep: the answer names no session resource (Location)")
	}
	s.mu.Lock()
	s.resource = resp.Request.URL.ResolveReference(location)
	s.etag = resp.Header.Get("ETag")
	s.patches = strings.Contains(resp.Header.Get("Accept-Patch"), "application/trickle-ice-sdpfrag")
	s.linked = linkedICEServers(resp.Header.Values("Link"))
	s.posted = true
	s.mu.Unlock()
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
		return err
	}
	s.declare(string(answer))
	go s.trickleLoop()
	s.signal()
	return nil
}

// candidate queues an ICE candidate (nil: gathering ended) for the trickle
// loop, which runs once the session resource exists.
func (s *whepSession) candidate(candidate *webrtc.ICECandidate) {
	s.mu.Lock()
	if candidate == nil {
		s.trickleEnd = true
	} else {
		s.trickle = append(s.trickle, candidate.ToJSON())
	}
	posted := s.posted
	s.mu.Unlock()
	if posted {
		s.signal()
	}
}

func (s *whepSession) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// trickleLoop PATCHes queued candidates in order until gathering ends, the
// server refuses PATCH, or the session closes.
func (s *whepSession) trickleLoop() {
	for {
		select {
		case <-s.wake:
		case <-s.ctx.Done():
			return
		}
		s.mu.Lock()
		batch, end, patches := s.trickle, s.trickleEnd, s.patches
		s.trickle, s.trickleEnd = nil, false
		s.mu.Unlock()
		if !patches {
			return
		}
		if len(batch) != 0 || end {
			s.patch(batch, end)
		}
		if end {
			return
		}
	}
}

// patch trickles candidates (and the end of candidates) as an
// application/trickle-ice-sdpfrag PATCH; a server that refuses PATCH (405,
// 501) stops the trickle and gets the candidates through ICE itself.
func (s *whepSession) patch(candidates []webrtc.ICECandidateInit, end bool) {
	s.mu.Lock()
	resource, etag := s.resource, s.etag
	s.mu.Unlock()
	frag, err := sdpFragment(s.pc.LocalDescription(), candidates, end)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	header := http.Header{}
	if etag != "" {
		header.Set("If-Match", etag)
	}
	resp, err := s.requestWith(ctx, http.MethodPatch, resource, "application/trickle-ice-sdpfrag", frag, header)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		s.mu.Lock()
		s.patches = false
		s.mu.Unlock()
	}
}

// sdpFragment renders candidates as RFC 8840 SDP fragment lines for the
// bundled first media section.
func sdpFragment(local *webrtc.SessionDescription, candidates []webrtc.ICECandidateInit, end bool) ([]byte, error) {
	if local == nil {
		return nil, errors.New("no local description")
	}
	parsed, err := (&webrtc.SessionDescription{Type: local.Type, SDP: local.SDP}).Unmarshal()
	if err != nil || len(parsed.MediaDescriptions) == 0 {
		return nil, errors.New("no media section")
	}
	media := parsed.MediaDescriptions[0]
	ufrag, _ := media.Attribute("ice-ufrag")
	pwd, _ := media.Attribute("ice-pwd")
	mid, _ := media.Attribute("mid")
	var b bytes.Buffer
	fmt.Fprintf(&b, "a=ice-ufrag:%s\r\na=ice-pwd:%s\r\n", ufrag, pwd)
	fmt.Fprintf(&b, "m=%s 9 %s %s\r\na=mid:%s\r\n", media.MediaName.Media, strings.Join(media.MediaName.Protos, "/"), strings.Join(media.MediaName.Formats, " "), mid)
	for _, candidate := range candidates {
		fmt.Fprintf(&b, "a=%s\r\n", candidate.Candidate)
	}
	if end {
		b.WriteString("a=end-of-candidates\r\n")
	}
	return b.Bytes(), nil
}

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
		s.payloads = append(s.payloads, rtpav.NewStaticPayloadMap(stream.Epoch, []rtpav.PayloadCodec{payloadCodecFromWebRTC(codec, stream.Codec)}))
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
	deadline := time.NewTimer(s.config.tracks)
	defer deadline.Stop()
	select {
	case first := <-s.arrived:
		if err := s.adopt(ctx, first); err != nil {
			return err
		}
	case <-s.failed:
		return ErrWHEPDisconnected
	case <-deadline.C:
		return fmt.Errorf("whep: no track started within %s", s.config.tracks)
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
				case s.readErrs <- err:
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
	s.readers = append(s.readers, reader)
	if declared {
		s.payloads[index] = reader.PayloadMap()
	} else {
		s.streams = append(s.streams, streams[0])
		s.payloads = append(s.payloads, reader.PayloadMap())
		stream := streams[0]
		select {
		case s.events <- av.Event{Type: av.EventStreamAdded, StreamID: stream.ID, Stream: &stream}:
		default:
		}
	}
	s.mu.Unlock()
	go s.pump(reader)
	return nil
}

// pump moves one track's packets into the merged reader.
func (s *whepSession) pump(reader TrackReader) {
	for {
		packet, err := reader.ReadRTP(s.ctx)
		if err != nil {
			select {
			case s.readErrs <- err:
			case <-s.ctx.Done():
			}
			return
		}
		select {
		case s.packets <- packet:
		case <-s.ctx.Done():
			return
		}
	}
}

// Streams, PayloadMap, ReadRTP, Events, WriteRTCP, and Close make the
// session the rtpav.PacketReader of its tracks.
func (s *whepSession) Streams(context.Context) ([]av.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]av.Stream(nil), s.streams...), nil
}

func (s *whepSession) PayloadMap() rtpav.PayloadMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(mergedPayloads(nil), s.payloads...)
}

func (s *whepSession) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	select {
	case packet := <-s.packets:
		return packet, nil
	case err := <-s.readErrs:
		if s.isClosed() {
			return nil, io.EOF
		}
		if errors.Is(err, io.EOF) {
			return nil, ErrWHEPDisconnected
		}
		return nil, err
	case <-s.failed:
		if s.isClosed() {
			return nil, io.EOF
		}
		return nil, ErrWHEPDisconnected
	case <-ctx.Done():
		return nil, ctx.Err()
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

// Close ends the session: DELETE its resource (bounded, since the run's
// context may be gone), then close the peer connection and the tracks.
func (s *whepSession) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		resource := s.resource
		readers := append([]TrackReader(nil), s.readers...)
		close(s.events)
		s.mu.Unlock()
		if resource != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if resp, deleteErr := s.request(ctx, http.MethodDelete, resource, "", nil); deleteErr == nil {
				resp.Body.Close()
			} else {
				err = deleteErr
			}
			cancel()
		}
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

func (s *whepSession) request(ctx context.Context, method string, target *url.URL, contentType string, body []byte) (*http.Response, error) {
	return s.requestWith(ctx, method, target, contentType, body, nil)
}

func (s *whepSession) requestWith(ctx context.Context, method string, target *url.URL, contentType string, body []byte, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, plainError(err)
	}
	for key, values := range header {
		req.Header[key] = values
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if s.config.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.token)
	}
	resp, err := s.config.client.Do(req)
	if err != nil {
		return nil, plainError(err)
	}
	return resp, nil
}

// plainError drops the URL a *url.Error quotes: it may carry credentials.
func plainError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("whep: %s: %w", strings.ToUpper(urlErr.Op), urlErr.Err)
	}
	return err
}

// mergedPayloads resolves payload types across a session's tracks.
type mergedPayloads []rtpav.PayloadMap

func (m mergedPayloads) Epoch() av.Epoch {
	var epoch av.Epoch
	for _, payloads := range m {
		epoch = max(epoch, payloads.Epoch())
	}
	return epoch
}

func (m mergedPayloads) Lookup(payloadType uint8) (rtpav.PayloadCodec, bool) {
	for _, payloads := range m {
		if codec, ok := payloads.Lookup(payloadType); ok {
			return codec, true
		}
	}
	return rtpav.PayloadCodec{}, false
}

func (m mergedPayloads) Codecs() []rtpav.PayloadCodec {
	var codecs []rtpav.PayloadCodec
	for _, payloads := range m {
		codecs = append(codecs, payloads.Codecs()...)
	}
	return codecs
}

// linkedICEServers reads the STUN/TURN servers of Link header values of
// rel "ice-server" (RFC 9725 section 4.6):
//
//	<turn:turn.example.net?transport=udp>; rel="ice-server"; username="u"; credential="p"
func linkedICEServers(values []string) []webrtc.ICEServer {
	var servers []webrtc.ICEServer
	for _, value := range values {
		for _, link := range splitOutsideQuotes(value, ',') {
			target, params, ok := strings.Cut(strings.TrimSpace(link), ";")
			target = strings.TrimSpace(target)
			if !ok || !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
				continue
			}
			server := webrtc.ICEServer{URLs: []string{target[1 : len(target)-1]}}
			ice := false
			for _, param := range splitOutsideQuotes(params, ';') {
				key, val, _ := strings.Cut(strings.TrimSpace(param), "=")
				val = strings.Trim(strings.TrimSpace(val), `"`)
				switch strings.ToLower(strings.TrimSpace(key)) {
				case "rel":
					ice = slices.Contains(strings.Fields(val), "ice-server")
				case "username":
					server.Username = val
				case "credential":
					server.Credential = val
				}
			}
			if ice {
				servers = append(servers, server)
			}
		}
	}
	return servers
}

// splitOutsideQuotes splits s at sep where sep is not inside quotes or <>.
func splitOutsideQuotes(s string, sep byte) []string {
	var parts []string
	quoted, angled, start := false, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case c == '<' && !quoted:
			angled = true
		case c == '>' && !quoted:
			angled = false
		case c == sep && !quoted && !angled:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}
