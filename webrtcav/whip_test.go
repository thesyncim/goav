package webrtcav

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// whipServer is a WHIP endpoint backed by Pion: POST answers an offer and
// hands the tracks it receives to the test, PATCH adds trickled candidates,
// DELETE ends the session.
type whipServer struct {
	t      *testing.T
	api    *webrtc.API
	tracks chan *webrtc.TrackRemote
	status int

	mu       sync.Mutex
	pc       *webrtc.PeerConnection
	auth     []string
	posts    int
	patches  int
	deletes  int
	trickled int
}

func newWHIPServer(t *testing.T) *whipServer {
	return &whipServer{t: t, api: loopbackAPI(), tracks: make(chan *webrtc.TrackRemote, 4)}
}

func (s *whipServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	s.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/whip/live":
		if s.status != 0 {
			http.Error(w, "no", s.status)
			return
		}
		s.post(w, r)
	case r.Method == http.MethodPatch && r.URL.Path == "/session/1":
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.patches++
		pc := s.pc
		s.mu.Unlock()
		for _, line := range strings.Split(string(body), "\r\n") {
			if candidate, ok := strings.CutPrefix(line, "a=candidate:"); ok {
				mid := "0"
				if err := pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: "candidate:" + candidate, SDPMid: &mid}); err == nil {
					s.mu.Lock()
					s.trickled++
					s.mu.Unlock()
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && r.URL.Path == "/session/1":
		s.mu.Lock()
		s.deletes++
		pc := s.pc
		s.mu.Unlock()
		pc.Close()
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (s *whipServer) post(w http.ResponseWriter, r *http.Request) {
	offer, _ := io.ReadAll(r.Body)
	pc, err := s.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		s.t.Error(err)
		return
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) { s.tracks <- track })
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	pc.SetLocalDescription(answer)
	<-gathered
	s.mu.Lock()
	s.posts++
	s.pc = pc
	s.mu.Unlock()
	w.Header().Set("Location", "../session/1")
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Accept-Patch", "application/trickle-ice-sdpfrag")
	w.WriteHeader(http.StatusCreated)
	io.WriteString(w, pc.LocalDescription().SDP)
}

func (s *whipServer) peer() *webrtc.PeerConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pc
}

// pacedVideo is a live I420 source paced at 50 frames a second.
func pacedVideo(name string) goav.InputSpec {
	return goav.Source(name, shape.Frame(av.MediaVideo, shape.Video(64, 64, av.PixelFormatI420), shape.Realtime(true)),
		func(ctx context.Context, push source.Push) error {
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			base := av.TimeBase{Num: 1, Den: 50}
			for n := 0; ; n++ {
				select {
				case <-ctx.Done():
					return nil
				case <-tick.C:
				}
				frame := &av.Frame{
					StreamID: av.StreamID(name), Type: av.MediaVideo,
					PTS:      av.Timestamp{Value: int64(n), Base: base},
					Duration: av.Duration{Value: 1, Base: base},
					Video:    &av.VideoFrame{Width: 64, Height: 64, PixelFormat: av.PixelFormatI420},
					Planes: []av.Plane{
						{Buffer: av.Buffer{Bytes: make([]byte, 64*64)}, Stride: 64},
						{Buffer: av.Buffer{Bytes: make([]byte, 32*32)}, Stride: 32},
						{Buffer: av.Buffer{Bytes: make([]byte, 32*32)}, Stride: 32},
					},
				}
				if _, err := push.Frame(frame); err != nil && !errors.Is(err, goav.ErrBackpressure) {
					return nil
				}
			}
		})
}

// receive decodes a track the WHIP server received, collecting its packets
// and its decoded frames.
func receive(t *testing.T, ctx context.Context, track *webrtc.TrackRemote) (packets, frames *goavtest.Collector, stop func()) {
	t.Helper()
	packets, frames = goavtest.NewCollector(), goavtest.NewCollector()
	in := goav.Input(Track(track)).Video()
	task, err := bundle.MustNew().Build(ctx, in.To(packets.Sink()), in.Decode().To(frames.Sink()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	return packets, frames, func() { cancel(); <-done; task.Close() }
}

func keyframes(c *goavtest.Collector) int {
	n := 0
	for _, packet := range c.Packets() {
		if packet.Keyframe {
			n++
		}
	}
	return n
}

// TestWHIPPublishesToAServer runs a whip URL end to end against a Pion WHIP
// server: the Bearer token on every request, VP8 that arrives and decodes, a
// keyframe answering the server's PLI, candidates trickled by PATCH when the
// offer goes out before gathering ends, and the session DELETEd on close.
func TestWHIPPublishesToAServer(t *testing.T) {
	for _, trickle := range []bool{false, true} {
		server := newWHIPServer(t)
		srv := httptest.NewServer(server)
		scheme := WHIP(Token("tok"), PeerConfig(SessionConfig{API: loopbackAPI()})).(whipScheme)
		if trickle {
			scheme.config.gather = 0
		}
		rt := bundle.MustNew(runconfig.Scheme("whip+http", scheme))
		dest := goav.URI("whip+http://" + strings.TrimPrefix(srv.URL, "http://") + "/whip/live?token=SECRET")
		out := pacedVideo("cam").Video().Encode(codec.VP8(codec.KeyframeInterval(100000))).To(dest)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		task, err := rt.Build(ctx, out)
		if err != nil {
			t.Fatalf("trickle=%v: %v", trickle, err)
		}
		if spec := fmtJSON(t, task.Spec()); strings.Contains(spec, "SECRET") {
			t.Fatalf("spec leaks the token: %s", spec)
		}
		done := make(chan error, 1)
		go func() { done <- task.Run(ctx) }()

		var track *webrtc.TrackRemote
		select {
		case track = <-server.tracks:
		case <-ctx.Done():
			t.Fatalf("trickle=%v: no track", trickle)
		}
		if track.Codec().MimeType != webrtc.MimeTypeVP8 {
			t.Fatalf("track codec = %s", track.Codec().MimeType)
		}
		packets, frames, stop := receive(t, ctx, track)
		if _, err := frames.WaitFrames(ctx, 5); err != nil {
			t.Fatalf("trickle=%v: decoded frames: %v", trickle, err)
		}
		if got := frames.Frames()[0].Video; got == nil || got.Width != 64 || got.Height != 64 {
			t.Fatalf("decoded frame = %+v", got)
		}
		before := keyframes(packets)
		if err := server.peer().WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}}); err != nil {
			t.Fatal(err)
		}
		if err := packets.Wait(ctx, func(c *goavtest.Collector) bool { return keyframes(c) > before }); err != nil {
			t.Fatalf("trickle=%v: no keyframe after PLI (%d before): %v", trickle, before, err)
		}
		stop()
		cancel()
		<-done
		if err := task.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		srv.Close()

		server.mu.Lock()
		if server.posts != 1 || server.deletes != 1 {
			t.Fatalf("trickle=%v: posts=%d deletes=%d, want one session, deleted", trickle, server.posts, server.deletes)
		}
		for _, auth := range server.auth {
			if auth != "Bearer tok" {
				t.Fatalf("Authorization = %q, want the Bearer token", auth)
			}
		}
		if trickle && (server.patches == 0 || server.trickled == 0) {
			t.Fatalf("patches=%d trickled=%d, want candidates trickled", server.patches, server.trickled)
		}
		server.mu.Unlock()
	}
}

// TestWHIPEncodesFramesAndFollowsTheEstimate pins the planner's side of
// WHIP: frames routed to it get a VP8 encode, Describe shows it, and with
// the API WHIP builds itself, the send-side bandwidth estimate (TWCC)
// retargets the encoder.
func TestWHIPEncodesFramesAndFollowsTheEstimate(t *testing.T) {
	server := newWHIPServer(t)
	srv := httptest.NewServer(server)
	defer srv.Close()
	scheme := WHIP().(whipScheme)
	settings := loopbackSettings()
	scheme.settings = &settings
	rt := bundle.MustNew(runconfig.Scheme("whip+http", scheme))
	url := "whip+http://" + strings.TrimPrefix(srv.URL, "http://") + "/whip/live"
	spec, err := rt.Describe(pacedVideo("cam").Video().To(goav.URI(url)))
	if err != nil {
		t.Fatal(err)
	}
	if text := fmtJSON(t, spec); !strings.Contains(text, "codec=vp8") || !strings.Contains(text, `"name":"whip"`) {
		t.Fatalf("describe = %s", text)
	}
	parsed, _ := neturl.Parse(url)
	bound, err := scheme.Destination(parsed)
	if err != nil {
		t.Fatal(err)
	}
	tap := controlTap{egress: bound.(*egress), got: make(chan tapped, 16)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	task, err := rt.Build(ctx, pacedVideo("cam").Video().To(goav.Sink(tap)))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	track := <-server.tracks
	_, frames, stop := receive(t, ctx, track)
	defer stop()
	if _, err := frames.WaitFrames(ctx, 3); err != nil {
		t.Fatalf("decoded frames: %v", err)
	}
	for {
		select {
		case got := <-tap.got:
			if got.ctrl.Type() != control.BitrateType {
				continue
			}
			if got.err != nil || got.ctrl.Bitrate() <= 0 {
				t.Fatalf("bitrate control for %q at %d: %v", got.ctrl.StreamID(), got.ctrl.Bitrate(), got.err)
			}
			cancel()
			<-done
			return
		case <-ctx.Done():
			t.Fatal("no bitrate control from the estimate")
		}
	}
}

// controlTap records the controls a media sink sends.
type controlTap struct {
	*egress
	got chan tapped
}

type tapped struct {
	ctrl control.Control
	err  error
}

func (c controlTap) UseControl(fn func(context.Context, control.Control) error) {
	c.egress.UseControl(func(ctx context.Context, ctrl control.Control) error {
		err := fn(ctx, ctrl)
		select {
		case c.got <- tapped{ctrl, err}:
		default:
		}
		return err
	})
}

// TestWHIPRefusals pins the scheme's refusals: no input (a destination
// scheme), a server that rejects the offer (a *StatusError, without the
// URL's secrets), and a server that refuses the stream's codec.
func TestWHIPRefusals(t *testing.T) {
	rt := bundle.MustNew(runconfig.Scheme("whip", WHIP()))
	if _, err := rt.Describe(goav.URIInput("whip://media.example/live").Video().To(goavtest.NewCollector().Sink())); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("input err = %v", err)
	}

	server := newWHIPServer(t)
	server.status = http.StatusUnauthorized
	srv := httptest.NewServer(server)
	defer srv.Close()
	rt = bundle.MustNew(runconfig.Scheme("whip+http", WHIP(PeerConfig(SessionConfig{API: loopbackAPI()}))))
	host := strings.TrimPrefix(srv.URL, "http://")
	_, err := rt.Build(context.Background(), pacedVideo("cam").Video().To(goav.URI("whip+http://"+host+"/whip/live?token=SECRET")))
	var status *StatusError
	if !errors.As(err, &status) || status.StatusCode != http.StatusUnauthorized || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err = %v, want a redacted 401 StatusError", err)
	}

	h264Only := &webrtc.MediaEngine{}
	if err := h264Only.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}, PayloadType: 102}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatal(err)
	}
	server.status = 0
	settings := loopbackSettings()
	server.api = webrtc.NewAPI(webrtc.WithSettingEngine(settings), webrtc.WithMediaEngine(h264Only))
	_, err = rt.Build(context.Background(), pacedVideo("cam").Video().To(goav.URI("whip+http://"+host+"/whip/live")))
	if !errors.Is(err, ErrWHIPCodecRefused) {
		t.Fatalf("err = %v, want ErrWHIPCodecRefused", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.deletes != 1 {
		t.Fatalf("deletes = %d, want the refused session deleted", server.deletes)
	}
}

// TestWHEPToWHIPRelay pulls a session from a WHEP server and publishes it,
// audio and video together, to a WHIP server: one WHIP session carries both
// tracks, copied, not transcoded.
func TestWHEPToWHIPRelay(t *testing.T) {
	whep := &whepServer{t: t, stop: make(chan struct{})}
	whepSrv := httptest.NewServer(whep)
	defer whepSrv.Close()
	defer close(whep.stop)
	whip := newWHIPServer(t)
	whipSrv := httptest.NewServer(whip)
	defer whipSrv.Close()
	session := PeerConfig(SessionConfig{API: loopbackAPI()})
	rt := bundle.MustNew(runconfig.Scheme("whep+http", WHEP(session)), runconfig.Scheme("whip+http", WHIP(session)))
	in := goav.URIInput("whep+http://" + strings.TrimPrefix(whepSrv.URL, "http://") + "/whep/cam")
	out := goav.URI("whip+http://" + strings.TrimPrefix(whipSrv.URL, "http://") + "/whip/live")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	task, err := rt.Build(ctx, in.Video().To(out), in.Audio().To(out))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	got := map[string]int{}
	for len(got) < 2 {
		var track *webrtc.TrackRemote
		select {
		case track = <-whip.tracks:
		case <-ctx.Done():
			t.Fatalf("tracks = %v", got)
		}
		for range 3 {
			packet, _, err := track.ReadRTP()
			if err != nil {
				t.Fatal(err)
			}
			if len(packet.Payload) == 0 {
				t.Fatal("empty payload")
			}
		}
		got[track.Codec().MimeType]++
	}
	if got[webrtc.MimeTypeVP8] != 1 || got[webrtc.MimeTypeOpus] != 1 {
		t.Fatalf("tracks = %v", got)
	}
	cancel()
	<-done
	task.Close()
	whip.mu.Lock()
	defer whip.mu.Unlock()
	if whip.posts != 1 || whip.deletes != 1 {
		t.Fatalf("whip posts=%d deletes=%d", whip.posts, whip.deletes)
	}
}

// TestLocalTrackSendsAndHearsKeyframeRequests pins the local track
// destination: it takes the track's codec, and a PLI on its sender becomes
// a keyframe control for the stream.
func TestLocalTrackSendsAndHearsKeyframeRequests(t *testing.T) {
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "v", "goav")
	if err != nil {
		t.Fatal(err)
	}
	sink := LocalTrack(track, nil)
	if got := sink.(interface{ InputShapes() shape.Set }).InputShapes(); len(got) != 1 || got[0].Codec != av.CodecVP8 {
		t.Fatalf("input shapes = %v", got)
	}
	var mu sync.Mutex
	var controls []control.Control
	sink.(interface {
		UseControl(func(context.Context, control.Control) error)
	}).UseControl(func(_ context.Context, ctrl control.Control) error {
		mu.Lock()
		defer mu.Unlock()
		controls = append(controls, ctrl)
		return nil
	})
	open := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	})
	if err := open.OpenStreams(context.Background(), []av.Stream{{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264}}}); err == nil {
		t.Fatal("an H264 stream opened a VP8 track")
	}
	sink = LocalTrack(track, nil)
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, ClockRate: 90000}}}); err != nil {
		t.Fatal(err)
	}
	packet := &av.Packet{StreamID: "v", Keyframe: true, Payload: av.Buffer{Bytes: []byte{0x10, 0, 0x9d}}, PTS: av.Timestamp{Value: 1, Base: av.RTPTimeBase(90000)}}
	if err := sink.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func loopbackSettings() webrtc.SettingEngine {
	var settings webrtc.SettingEngine
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	return settings
}

func fmtJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
