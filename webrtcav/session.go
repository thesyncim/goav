package webrtcav

import (
	"context"
	"sync"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
)

const (
	defaultSessionTrackBuffer = 8
	defaultSessionEventBuffer = 8
)

type peerConnectionSession struct {
	pc       *webrtc.PeerConnection
	tracks   chan RemoteTrack
	events   chan av.Event
	done     chan struct{}
	metadata av.Metadata
	mu       sync.Mutex
	closed   bool
}

var _ Session = (*peerConnectionSession)(nil)

// NewSession creates a Pion-backed WebRTC receive session.
func NewSession(ctx context.Context, config SessionConfig) (Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	api := config.API
	if api == nil {
		api = webrtc.NewAPI()
	}
	pc, err := api.NewPeerConnection(config.Configuration)
	if err != nil {
		return nil, err
	}
	return newPeerConnectionSession(pc, config), nil
}

func newPeerConnectionSession(pc *webrtc.PeerConnection, config SessionConfig) *peerConnectionSession {
	trackBuffer := positiveOrDefault(config.MaxTracks, defaultSessionTrackBuffer)
	eventBuffer := positiveOrDefault(config.MaxEvents, defaultSessionEventBuffer)
	session := &peerConnectionSession{
		pc:       pc,
		tracks:   make(chan RemoteTrack, trackBuffer),
		events:   make(chan av.Event, eventBuffer),
		done:     make(chan struct{}),
		metadata: mergeMetadata(nil, config.Metadata),
	}
	if pc != nil {
		pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			session.enqueueTrack(session.remoteTrack(track, receiver))
		})
	}
	return session
}

func (s *peerConnectionSession) PeerConnection() *webrtc.PeerConnection {
	return s.pc
}

func (s *peerConnectionSession) AcceptTrack(ctx context.Context) (RemoteTrack, error) {
	if err := ctx.Err(); err != nil {
		return RemoteTrack{}, err
	}
	select {
	case track := <-s.tracks:
		return track, nil
	case <-s.done:
		return RemoteTrack{}, ErrClosed
	case <-ctx.Done():
		return RemoteTrack{}, ctx.Err()
	}
}

func (s *peerConnectionSession) WriteRTCP(ctx context.Context, packets []rtcp.Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(packets) == 0 {
		return nil
	}
	if s.isClosed() {
		return ErrClosed
	}
	if s.pc == nil {
		return nil
	}
	return s.pc.WriteRTCP(packets)
}

func (s *peerConnectionSession) Events() <-chan av.Event {
	return s.events
}

func (s *peerConnectionSession) SetRemoteDescription(ctx context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	if err := ctx.Err(); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if s.isClosed() {
		return webrtc.SessionDescription{}, ErrClosed
	}
	if err := s.pc.SetRemoteDescription(offer); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if err := ctx.Err(); err != nil {
		return webrtc.SessionDescription{}, err
	}
	answer, err := s.pc.CreateAnswer(nil)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	if err := ctx.Err(); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if err := s.pc.SetLocalDescription(answer); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if local := s.pc.LocalDescription(); local != nil {
		answer = *local
	}
	return answer, ctx.Err()
}

func (s *peerConnectionSession) AddICECandidate(ctx context.Context, candidate webrtc.ICECandidateInit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return ErrClosed
	}
	return s.pc.AddICECandidate(candidate)
}

func (s *peerConnectionSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	close(s.events)
	s.mu.Unlock()

	if s.pc == nil {
		return nil
	}
	return s.pc.Close()
}

func (s *peerConnectionSession) remoteTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) RemoteTrack {
	remote := RemoteTrack{
		Track:    track,
		Receiver: receiver,
		Feedback: s,
		Metadata: s.metadata,
	}
	if track != nil {
		remote.Codec = track.Codec()
	}
	if receiver != nil {
		remote.Transceiver = receiver.RTPTransceiver()
	}
	return remote
}

func (s *peerConnectionSession) enqueueTrack(track RemoteTrack) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	select {
	case s.tracks <- track:
		s.mu.Unlock()
		s.emitTrackAdded(track)
	default:
		s.mu.Unlock()
		s.emit(av.Event{
			Type:   av.EventBackpressure,
			Cause:  ErrTrackQueueFull,
			Reason: "webrtc track queue full",
		})
	}
}

func (s *peerConnectionSession) emitTrackAdded(track RemoteTrack) {
	stream := track.Stream
	if stream.ID == "" && track.Track != nil {
		stream = streamFromRemoteTrack(track)
	}
	s.emit(av.Event{
		Type:     av.EventStreamAdded,
		StreamID: stream.ID,
		Epoch:    stream.Epoch,
		Stream:   &stream,
	})
}

func (s *peerConnectionSession) emit(event av.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- event:
	default:
	}
}

func (s *peerConnectionSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
