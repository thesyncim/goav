package webrtcav

import (
	"context"
	"errors"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
)

func TestNewSessionCreatesPeerConnection(t *testing.T) {
	session, err := NewSession(context.Background(), SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if session.PeerConnection() == nil {
		t.Fatal("missing peer connection")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionAcceptTrackAndStreamEvent(t *testing.T) {
	session := newPeerConnectionSession(nil, SessionConfig{MaxTracks: 1, MaxEvents: 2})
	defer session.Close()

	stream := av.Stream{ID: "video", Epoch: 3, Type: av.MediaVideo}
	session.enqueueTrack(RemoteTrack{Stream: stream})

	accepted, err := session.AcceptTrack(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Stream.ID != stream.ID || accepted.Stream.Epoch != stream.Epoch {
		t.Fatalf("track = %+v", accepted)
	}

	select {
	case event := <-session.Events():
		if event.Type != av.EventStreamAdded || event.StreamID != stream.ID || event.Epoch != stream.Epoch {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("missing stream event")
	}
}

func TestSessionTrackQueueBackpressure(t *testing.T) {
	session := newPeerConnectionSession(nil, SessionConfig{MaxTracks: 1, MaxEvents: 2})
	defer session.Close()

	session.enqueueTrack(RemoteTrack{Stream: av.Stream{ID: "first"}})
	session.enqueueTrack(RemoteTrack{Stream: av.Stream{ID: "second"}})

	first := <-session.Events()
	if first.Type != av.EventStreamAdded {
		t.Fatalf("first event = %+v", first)
	}
	second := <-session.Events()
	if second.Type != av.EventBackpressure || !errors.Is(second.Cause, ErrTrackQueueFull) {
		t.Fatalf("second event = %+v", second)
	}
}

func TestSessionAcceptTrackContextAndClose(t *testing.T) {
	session := newPeerConnectionSession(nil, SessionConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.AcceptTrack(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.AcceptTrack(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestSessionNegotiationHonorsClosedAndContext(t *testing.T) {
	session := newPeerConnectionSession(nil, SessionConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := session.SetRemoteDescription(ctx, webrtc.SessionDescription{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if err := session.AddICECandidate(ctx, webrtc.ICECandidateInit{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetRemoteDescription(context.Background(), webrtc.SessionDescription{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if err := session.AddICECandidate(context.Background(), webrtc.ICECandidateInit{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestSessionWriteRTCPContextAndClose(t *testing.T) {
	session := newPeerConnectionSession(nil, SessionConfig{})
	packet := &rtcp.PictureLossIndication{MediaSSRC: 2}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.WriteRTCP(ctx, []rtcp.Packet{packet}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.WriteRTCP(context.Background(), []rtcp.Packet{packet}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}
