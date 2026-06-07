package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/pipeline"
)

//go:embed static/*
var staticFiles embed.FS

var renditionSeq atomic.Uint64

const (
	videoTapName = "video.decoded"
	audioTapName = "audio.decoded"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	app := &server{
		runtime:  goav.Default(),
		sessions: make(map[string]*session),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveStatic)
	mux.HandleFunc("/api/offer", app.handleInitialOffer)
	mux.HandleFunc("/api/sessions/", app.handleSession)

	log.Printf("webrtc runtime ladder listening on http://localhost%s", listenPort(*addr))
	if err := http.ListenAndServe(*addr, logRequest(mux)); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	runtime  goav.Runtime
	mu       sync.Mutex
	sessions map[string]*session
}

type session struct {
	id         string
	pc         *webrtc.PeerConnection
	runtime    goav.Runtime
	ctx        context.Context
	cancel     context.CancelFunc
	created    time.Time
	signalMu   sync.Mutex
	mu         sync.Mutex
	renditions map[string]*rendition

	videoTask  goav.Task
	audioTask  goav.Task
	videoCodec string
	audioCodec string
	lastError  string
}

type rendition struct {
	Spec       renditionSpec                  `json:"spec"`
	Track      *webrtc.TrackLocalStaticSample `json:"-"`
	Sender     *webrtc.RTPSender              `json:"-"`
	Attachment goav.Attachment                `json:"-"`
	Sink       *trackSampleSink               `json:"-"`
	Created    time.Time                      `json:"created"`
}

type renditionSpec struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Codec   string `json:"codec"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	Bitrate int    `json:"bitrate"`
}

type signalRequest struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type signalResponse struct {
	ID   string `json:"id,omitempty"`
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type addRenditionResponse struct {
	Rendition        renditionSpec `json:"rendition"`
	NeedsRenegotiate bool          `json:"needsRenegotiate"`
}

type stateResponse struct {
	ID         string          `json:"id"`
	Created    time.Time       `json:"created"`
	VideoCodec string          `json:"videoCodec,omitempty"`
	AudioCodec string          `json:"audioCodec,omitempty"`
	LastError  string          `json:"lastError,omitempty"`
	Renditions []renditionView `json:"renditions"`
	VideoGraph graphView       `json:"videoGraph"`
	AudioGraph graphView       `json:"audioGraph"`
}

type renditionView struct {
	renditionSpec
	Packets uint64 `json:"packets"`
	Bytes   uint64 `json:"bytes"`
	Bound   bool   `json:"bound"`
}

type graphView struct {
	Nodes []nodeView `json:"nodes"`
	Edges []edgeView `json:"edges"`
}

type nodeView struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type edgeView struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type trackSampleSink struct {
	name     string
	track    *webrtc.TrackLocalStaticSample
	fallback time.Duration
	packets  atomic.Uint64
	bytes    atomic.Uint64
}

func serveStatic(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		http.ServeFileFS(w, r, staticFiles, "static/index.html")
		return
	}
	http.FileServerFS(staticFiles).ServeHTTP(w, r)
}

func (s *server) handleInitialOffer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var offer signalRequest
	if err := decodeJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	session, err := newSession(r.Context(), s.runtime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	answer, err := session.answer(r.Context(), offer)
	if err != nil {
		session.Close()
		writeError(w, http.StatusBadRequest, err)
		return
	}

	s.mu.Lock()
	s.sessions[session.id] = session
	s.mu.Unlock()

	writeJSON(w, signalResponse{ID: session.id, Type: answer.Type.String(), SDP: answer.SDP})
}

func (s *server) handleSession(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/sessions/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	session := s.lookup(parts[0])
	if session == nil {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "offer" && r.Method == http.MethodPost {
		session.handleOffer(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "state" && r.Method == http.MethodGet {
		writeJSON(w, session.State())
		return
	}
	if len(parts) == 2 && parts[1] == "renditions" && r.Method == http.MethodPost {
		session.handleAddRendition(w, r)
		return
	}
	if len(parts) == 3 && parts[1] == "renditions" && r.Method == http.MethodPut {
		session.handleUpdateRendition(w, r, parts[2])
		return
	}
	if len(parts) == 3 && parts[1] == "renditions" && r.Method == http.MethodDelete {
		session.handleDeleteRendition(w, r, parts[2])
		return
	}
	http.NotFound(w, r)
}

func (s *server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func newSession(ctx context.Context, runtime goav.Runtime) (*session, error) {
	api, err := newWebRTCAPI()
	if err != nil {
		return nil, err
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &session{
		id:         randomID(),
		pc:         pc,
		runtime:    runtime,
		ctx:        sessionCtx,
		cancel:     cancel,
		created:    time.Now(),
		renditions: make(map[string]*rendition),
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go session.acceptTrack(track)
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateDisconnected {
			session.setError("peer connection " + state.String())
		}
	})
	for _, spec := range defaultRenditions() {
		if _, err := session.addRendition(ctx, spec); err != nil {
			cancel()
			_ = pc.Close()
			return nil, err
		}
	}
	return session, nil
}

func newWebRTCAPI() (*webrtc.API, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptors); err != nil {
		return nil, err
	}
	return webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptors),
	), nil
}

func defaultRenditions() []renditionSpec {
	return []renditionSpec{
		{Kind: "video", Codec: "vp8", Width: 640, Height: 360, Bitrate: 700_000},
		{Kind: "video", Codec: "vp9", Width: 320, Height: 180, Bitrate: 320_000},
		{Kind: "audio", Codec: "opus", Bitrate: 64_000},
	}
}

func (s *session) handleOffer(w http.ResponseWriter, r *http.Request) {
	var offer signalRequest
	if err := decodeJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	answer, err := s.answer(r.Context(), offer)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, signalResponse{Type: answer.Type.String(), SDP: answer.SDP})
}

func (s *session) answer(ctx context.Context, offer signalRequest) (webrtc.SessionDescription, error) {
	s.signalMu.Lock()
	defer s.signalMu.Unlock()

	if strings.ToLower(offer.Type) != "offer" || offer.SDP == "" {
		return webrtc.SessionDescription{}, fmt.Errorf("expected offer SDP")
	}
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offer.SDP,
	}); err != nil {
		return webrtc.SessionDescription{}, err
	}
	answer, err := s.pc.CreateAnswer(nil)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	gatherDone := webrtc.GatheringCompletePromise(s.pc)
	if err := s.pc.SetLocalDescription(answer); err != nil {
		return webrtc.SessionDescription{}, err
	}
	select {
	case <-gatherDone:
	case <-ctx.Done():
		return webrtc.SessionDescription{}, ctx.Err()
	}
	local := s.pc.LocalDescription()
	if local == nil {
		return webrtc.SessionDescription{}, fmt.Errorf("missing local description")
	}
	return *local, nil
}

func (s *session) handleAddRendition(w http.ResponseWriter, r *http.Request) {
	var spec renditionSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	rendition, err := s.addRendition(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addRenditionResponse{Rendition: rendition.Spec, NeedsRenegotiate: true})
}

func (s *session) handleUpdateRendition(w http.ResponseWriter, r *http.Request, id string) {
	var spec renditionSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	spec.ID = id
	updated, err := s.updateRendition(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addRenditionResponse{Rendition: updated.Spec})
}

func (s *session) handleDeleteRendition(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.deleteRendition(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "needsRenegotiate": true})
}

func (s *session) addRendition(ctx context.Context, spec renditionSpec) (*rendition, error) {
	if err := normalizeRendition(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.renditions[spec.ID]; exists {
		return nil, fmt.Errorf("rendition %q already exists", spec.ID)
	}
	track, err := webrtc.NewTrackLocalStaticSample(codecCapability(spec), spec.ID, "goav-"+s.id)
	if err != nil {
		return nil, err
	}
	sender, err := s.pc.AddTrack(track)
	if err != nil {
		return nil, err
	}
	go drainRTCP(sender)
	r := &rendition{
		Spec:    spec,
		Track:   track,
		Sender:  sender,
		Sink:    newTrackSampleSink(spec, track),
		Created: time.Now(),
	}
	s.renditions[spec.ID] = r
	if err := s.attachRenditionLocked(ctx, r); err != nil {
		s.setErrorLocked(err.Error())
	}
	return r, nil
}

func (s *session) updateRendition(ctx context.Context, spec renditionSpec) (*rendition, error) {
	if err := normalizeRendition(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.renditions[spec.ID]
	if !ok {
		return nil, fmt.Errorf("rendition %q not found", spec.ID)
	}
	if r.Spec.Kind != spec.Kind || r.Spec.Codec != spec.Codec {
		return nil, fmt.Errorf("kind and codec changes create a new track; add a new rendition instead")
	}
	if r.Attachment != nil {
		_ = s.detachLocked(ctx, r)
	}
	r.Spec.Width = spec.Width
	r.Spec.Height = spec.Height
	r.Spec.Bitrate = spec.Bitrate
	if err := s.attachRenditionLocked(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

func (s *session) deleteRendition(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.renditions[id]
	if !ok {
		return fmt.Errorf("rendition %q not found", id)
	}
	_ = s.detachLocked(ctx, r)
	if r.Sender != nil {
		_ = s.pc.RemoveTrack(r.Sender)
	}
	delete(s.renditions, id)
	return nil
}

func (s *session) acceptTrack(track *webrtc.TrackRemote) {
	switch track.Kind() {
	case webrtc.RTPCodecTypeVideo:
		s.startVideoTrack(track)
	case webrtc.RTPCodecTypeAudio:
		s.startAudioTrack(track)
	}
}

func (s *session) startVideoTrack(track *webrtc.TrackRemote) {
	s.mu.Lock()
	if s.videoTask != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	tap := goav.FrameTap(videoTapName)
	task, err := goav.From(goav.WebRTCTrack(track)).
		UseRuntime(s.runtime).
		Video().
		Decode().
		Tap(tap).
		To(goav.Sink(discardSink("video-decoded"))).
		Build(s.ctx)
	if err != nil {
		s.setError(err.Error())
		return
	}

	s.mu.Lock()
	if s.videoTask != nil {
		s.mu.Unlock()
		_ = task.Close()
		return
	}
	s.videoTask = task
	s.videoCodec = codecName(track.Codec().MimeType)
	for _, r := range s.sortedRenditionsLocked("video") {
		if err := s.attachRenditionLocked(s.ctx, r); err != nil {
			s.setErrorLocked(err.Error())
		}
	}
	s.mu.Unlock()

	go s.runTask("video", task)
}

func (s *session) startAudioTrack(track *webrtc.TrackRemote) {
	s.mu.Lock()
	if s.audioTask != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	tap := goav.FrameTap(audioTapName)
	task, err := goav.From(goav.WebRTCTrack(track)).
		UseRuntime(s.runtime).
		Audio().
		Decode().
		Tap(tap).
		To(goav.Sink(discardSink("audio-decoded"))).
		Build(s.ctx)
	if err != nil {
		s.setError(err.Error())
		return
	}

	s.mu.Lock()
	if s.audioTask != nil {
		s.mu.Unlock()
		_ = task.Close()
		return
	}
	s.audioTask = task
	s.audioCodec = codecName(track.Codec().MimeType)
	for _, r := range s.sortedRenditionsLocked("audio") {
		if err := s.attachRenditionLocked(s.ctx, r); err != nil {
			s.setErrorLocked(err.Error())
		}
	}
	s.mu.Unlock()

	go s.runTask("audio", task)
}

func (s *session) runTask(kind string, task goav.Task) {
	if err := task.Run(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.setError(kind + ": " + err.Error())
	}
}

func (s *session) attachRenditionLocked(ctx context.Context, r *rendition) error {
	if r.Attachment != nil {
		return nil
	}
	switch r.Spec.Kind {
	case "video":
		if s.videoTask == nil {
			return nil
		}
		branch := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(videoTapName)).
			Resize(r.Spec.Width, r.Spec.Height)
		var spec goav.BranchSpec
		switch r.Spec.Codec {
		case "vp8":
			spec = branch.VP8(r.Spec.Bitrate).To(goav.Sink(r.Sink))
		case "vp9":
			spec = branch.VP9(r.Spec.Bitrate).To(goav.Sink(r.Sink))
		default:
			return fmt.Errorf("unsupported video encoder %q", r.Spec.Codec)
		}
		attachment, err := s.videoTask.Attach(ctx, spec)
		if err != nil {
			return err
		}
		r.Attachment = attachment
	case "audio":
		if s.audioTask == nil {
			return nil
		}
		spec := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(audioTapName)).
			Resample(48_000, goav.Stereo).
			Opus(r.Spec.Bitrate, goav.Channels(goav.Stereo)).
			To(goav.Sink(r.Sink))
		attachment, err := s.audioTask.Attach(ctx, spec)
		if err != nil {
			return err
		}
		r.Attachment = attachment
	default:
		return fmt.Errorf("unsupported rendition kind %q", r.Spec.Kind)
	}
	return nil
}

func (s *session) detachLocked(ctx context.Context, r *rendition) error {
	if r.Attachment == nil {
		return nil
	}
	var task goav.Task
	if r.Spec.Kind == "video" {
		task = s.videoTask
	} else {
		task = s.audioTask
	}
	if task == nil {
		r.Attachment = nil
		return nil
	}
	err := task.Detach(ctx, r.Attachment)
	r.Attachment = nil
	return err
}

func (s *session) State() stateResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := stateResponse{
		ID:         s.id,
		Created:    s.created,
		VideoCodec: s.videoCodec,
		AudioCodec: s.audioCodec,
		LastError:  s.lastError,
	}
	for _, r := range s.sortedRenditionsLocked("") {
		view := renditionView{renditionSpec: r.Spec, Bound: r.Attachment != nil}
		if r.Sink != nil {
			view.Packets = r.Sink.packets.Load()
			view.Bytes = r.Sink.bytes.Load()
		}
		out.Renditions = append(out.Renditions, view)
	}
	if s.videoTask != nil {
		out.VideoGraph = graphFromSpec(s.videoTask.Describe())
	}
	if s.audioTask != nil {
		out.AudioGraph = graphFromSpec(s.audioTask.Describe())
	}
	return out
}

func (s *session) sortedRenditionsLocked(kind string) []*rendition {
	out := make([]*rendition, 0, len(s.renditions))
	for _, r := range s.renditions {
		if kind == "" || r.Spec.Kind == kind {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Spec.Kind != out[j].Spec.Kind {
			return out[i].Spec.Kind < out[j].Spec.Kind
		}
		return out[i].Created.Before(out[j].Created)
	})
	return out
}

func (s *session) setError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setErrorLocked(message)
}

func (s *session) setErrorLocked(message string) {
	s.lastError = message
	log.Printf("session %s: %s", s.id, message)
}

func (s *session) Close() {
	s.cancel()
	s.mu.Lock()
	if s.videoTask != nil {
		_ = s.videoTask.Close()
	}
	if s.audioTask != nil {
		_ = s.audioTask.Close()
	}
	s.mu.Unlock()
	_ = s.pc.Close()
}

func newTrackSampleSink(spec renditionSpec, track *webrtc.TrackLocalStaticSample) *trackSampleSink {
	fallback := 33 * time.Millisecond
	if spec.Kind == "audio" {
		fallback = 20 * time.Millisecond
	}
	return &trackSampleSink{name: "browser-" + spec.ID, track: track, fallback: fallback}
}

func (s *trackSampleSink) Name() string {
	return s.name
}

func (s *trackSampleSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Kind != "packet" || msg.Packet == nil {
		return nil
	}
	duration := s.fallback
	if d, ok := msg.Packet.Duration.ToDuration(); ok && d > 0 {
		duration = d
	}
	if err := s.track.WriteSample(media.Sample{
		Data:     msg.Packet.Payload.Bytes,
		Duration: duration,
	}); err != nil {
		return err
	}
	s.packets.Add(1)
	s.bytes.Add(uint64(len(msg.Packet.Payload.Bytes)))
	return nil
}

func (s *trackSampleSink) Close() error {
	return nil
}

func discardSink(name string) pipeline.Sink {
	return goav.SinkFunc(name, func(context.Context, goav.Message) error {
		return nil
	})
}

func normalizeRendition(spec *renditionSpec) error {
	spec.Kind = strings.ToLower(strings.TrimSpace(spec.Kind))
	spec.Codec = strings.ToLower(strings.TrimSpace(spec.Codec))
	if spec.ID == "" {
		spec.ID = fmt.Sprintf("%s-%s-%d", spec.Kind, spec.Codec, renditionSeq.Add(1))
	}
	if spec.Bitrate <= 0 {
		if spec.Kind == "audio" {
			spec.Bitrate = 64_000
		} else {
			spec.Bitrate = 600_000
		}
	}
	switch spec.Kind {
	case "video":
		if spec.Codec != "vp8" && spec.Codec != "vp9" {
			return fmt.Errorf("video output supports vp8 or vp9")
		}
		if spec.Width <= 0 {
			spec.Width = 640
		}
		if spec.Height <= 0 {
			spec.Height = 360
		}
	case "audio":
		if spec.Codec != "opus" {
			return fmt.Errorf("audio output supports opus")
		}
	default:
		return fmt.Errorf("kind must be audio or video")
	}
	return nil
}

func codecCapability(spec renditionSpec) webrtc.RTPCodecCapability {
	switch spec.Codec {
	case "opus":
		return webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48_000,
			Channels:     2,
			SDPFmtpLine:  "minptime=10;useinbandfec=1",
			RTCPFeedback: nil,
		}
	case "vp9":
		return webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeVP9,
			ClockRate:   90_000,
			SDPFmtpLine: "profile-id=0",
		}
	default:
		return webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90_000,
		}
	}
}

func drainRTCP(sender *webrtc.RTPSender) {
	for {
		if _, _, err := sender.ReadRTCP(); err != nil {
			return
		}
	}
}

func graphFromSpec(spec pipeline.Spec) graphView {
	view := graphView{
		Nodes: make([]nodeView, 0, len(spec.Nodes)),
		Edges: make([]edgeView, 0, len(spec.Edges)),
	}
	for _, node := range spec.Nodes {
		view.Nodes = append(view.Nodes, nodeView{
			Name:   node.Name,
			Kind:   string(node.Kind),
			Detail: node.Detail,
		})
	}
	for _, edge := range spec.Edges {
		view.Edges = append(view.Edges, edgeView{
			From: edge.From.String(),
			To:   edge.To.String(),
		})
	}
	return view
}

func codecName(mime string) string {
	parts := strings.Split(mime, "/")
	if len(parts) == 2 {
		return strings.ToLower(parts[1])
	}
	return strings.ToLower(mime)
}

func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func listenPort(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return addr
	}
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i:]
	}
	return ":8080"
}
