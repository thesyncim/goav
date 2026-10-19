package webrtcav

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// loopbackAPI keeps ICE on 127.0.0.1, so the test needs no network.
func loopbackAPI() *webrtc.API {
	var settings webrtc.SettingEngine
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	return webrtc.NewAPI(webrtc.WithSettingEngine(settings))
}

// whepServer is a WHEP endpoint backed by Pion: POST answers an offer and
// sends Opus and VP8 RTP, PATCH adds trickled candidates, DELETE ends the
// session.
type whepServer struct {
	t    *testing.T
	stop chan struct{}

	mu       sync.Mutex
	pc       *webrtc.PeerConnection
	auth     []string
	posts    int
	patches  int
	deletes  int
	trickled int
}

func (s *whepServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	s.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/whep/cam":
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

func (s *whepServer) post(w http.ResponseWriter, r *http.Request) {
	offer, _ := io.ReadAll(r.Body)
	pc, err := loopbackAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		s.t.Error(err)
		return
	}
	audio, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "cam")
	video, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "cam")
	for _, track := range []webrtc.TrackLocal{audio, video} {
		sender, err := pc.AddTrack(track)
		if err != nil {
			s.t.Error(err)
			return
		}
		go func() {
			for {
				if _, _, err := sender.ReadRTCP(); err != nil {
					return
				}
			}
		}()
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)}); err != nil {
		s.t.Error(err)
		return
	}
	answer, _ := pc.CreateAnswer(nil)
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
	go s.send(audio, video)
}

func (s *whepServer) send(audio, video *webrtc.TrackLocalStaticRTP) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for i := uint32(0); ; i++ {
		select {
		case <-tick.C:
		case <-s.stop:
			return
		}
		seq := uint16(i)
		audio.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: i * 960, Marker: true}, Payload: []byte{0xf8, 0xff, 0xfe}})
		video.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: i * 3000, Marker: true}, Payload: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a}})
	}
}

// TestWHEPPlaysAServerSession runs a whep URL end to end against a Pion WHEP
// server: the Bearer token on every request, audio and video packets from
// the negotiated tracks, the session DELETEd on close, and candidates
// trickled by PATCH when the offer goes out before gathering ends.
func TestWHEPPlaysAServerSession(t *testing.T) {
	for _, trickle := range []bool{false, true} {
		server := &whepServer{t: t, stop: make(chan struct{})}
		srv := httptest.NewServer(server)
		scheme := WHEP(WHEPToken("tok"), WHEPSession(SessionConfig{API: loopbackAPI()})).(whepScheme)
		if trickle {
			scheme.config.gather = 0
		}
		rt := bundle.MustNew(runconfig.Scheme("whep+http", scheme))
		in := goav.URIInput("whep+http://" + strings.TrimPrefix(srv.URL, "http://") + "/whep/cam?token=SECRET")
		video, audio := goavtest.NewCollector(), goavtest.NewCollector()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		task, err := rt.Build(ctx, in.Video().To(video.Sink()), in.Audio().To(audio.Sink()))
		if err != nil {
			t.Fatalf("trickle=%v: %v", trickle, err)
		}
		spec, _ := json.Marshal(task.Spec())
		if !strings.Contains(string(spec), "protocol=whep+http") || strings.Contains(string(spec), "SECRET") {
			t.Fatalf("spec = %s", spec)
		}
		done := make(chan error, 1)
		go func() { done <- task.Run(ctx) }()
		if _, err := video.WaitPackets(ctx, 3); err != nil {
			t.Fatalf("trickle=%v: video: %v", trickle, err)
		}
		if _, err := audio.WaitPackets(ctx, 3); err != nil {
			t.Fatalf("trickle=%v: audio: %v", trickle, err)
		}
		cancel()
		<-done
		task.Close()
		close(server.stop)
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
		if packet := video.Packets()[0]; packet.StreamID != "video" || !packet.Keyframe {
			t.Fatalf("video packet = %+v", packet)
		}
	}
}

// TestWHEPRefusals pins the scheme's refusals: no destination, and a server
// that rejects the offer, reported without the URL's secrets.
func TestWHEPRefusals(t *testing.T) {
	u, _ := url.Parse("whep://media.example/live")
	if _, err := WHEP().Destination(u); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("destination err = %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer srv.Close()
	rt := bundle.MustNew(runconfig.Scheme("whep+http", WHEP(WHEPSession(SessionConfig{API: loopbackAPI()}))))
	_, err := rt.Build(context.Background(), goav.URIInput("whep+http://"+strings.TrimPrefix(srv.URL, "http://")+"/live?token=SECRET").Video().To(goavtest.NewCollector().Sink()))
	if err == nil || !strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err = %v, want a redacted 401", err)
	}
}
