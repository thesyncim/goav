package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
)

func newSession(ctx context.Context, runtime *goav.Runtime, browserURL string) (*session, error) {
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
		browserURL: browserURL,
		ctx:        sessionCtx,
		cancel:     cancel,
		created:    time.Now(),
		updated:    time.Now(),
		branches:   make(map[string]*branch),
		listeners:  make(map[string]chan stateResponse),
		video:      newVideoAnalyzer(),
		audio:      newAudioAnalyzer(),
		native:     newNativeAudio(),
		syncPolicy: goav.Sync("browser", goav.SyncTolerance(30*time.Millisecond), goav.SyncDropLate()),
		scenarios:  runPlannerScenarios(ctx),
	}
	session.writeRTCP = pc.WriteRTCP
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go session.acceptTrack(track)
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		session.record("info", "peer", "peer connection "+state.String(), "", "", eventMeta("state", state.String()))
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateDisconnected {
			session.setError("peer connection " + state.String())
		}
	})

	session.record("info", "session", "session created", "", "", eventMeta("id", session.id))
	for _, spec := range defaultBranches() {
		if _, err := session.addBranch(ctx, spec); err != nil {
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
	s.record("info", "signal", "answer created", "", "", eventMeta("type", local.Type.String()))
	s.clearRenegotiate()
	return *local, nil
}
