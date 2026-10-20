package webrtcav

import (
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
)

func TestTrackReplacementInterruptsSilentTransport(t *testing.T) {
	old := newBlockingTrackRTPReader()
	reader := newTrackReader(opusRemoteTrack(111), old)
	defer reader.Close()
	for i := range 12 {
		done := make(chan error, 1)
		go func() {
			packet, _, _, err := reader.ReadRTP(t.Context())
			if err == nil && packet.SequenceNumber != uint16(i) {
				t.Errorf("replacement packet = %d, want %d", packet.SequenceNumber, i)
			}
			done <- err
		}()
		waitRead(t, old)
		next := newBlockingTrackRTPReader()
		if err := reader.updateTrack(t.Context(), opusRemoteTrack(111), next); err != nil {
			t.Fatal(err)
		}
		waitRead(t, next)
		next.packets <- &rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i)}}
		if err := waitReadResult(t, done); err != nil {
			t.Fatal(err)
		}
		select {
		case event := <-reader.Events():
			if event.Type != av.EventCodecChanged {
				t.Fatalf("replacement emitted %s", event.Type)
			}
		default:
			t.Fatal("missing configuration event")
		}
		old = next
	}
}

func TestLocalTrackReattachmentSharesSenderFeedbackLifetime(t *testing.T) {
	pc, err := loopbackAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "v", "goav")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	var source *senderFeedback
	for range 32 {
		// Also cover constructing a new destination for the same sender.
		sink := LocalTrack(track, sender).(*egress)
		if err := sink.OpenStreams(t.Context(), []av.Stream{{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, ClockRate: 90000}}}); err != nil {
			t.Fatal(err)
		}
		senderFeedbacks.Lock()
		current := senderFeedbacks.readers[sender]
		senderFeedbacks.Unlock()
		if source != nil && current != source {
			t.Fatal("reattach started a competing RTCP reader")
		}
		source = current
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		source.mu.Lock()
		listeners := len(source.listeners)
		source.mu.Unlock()
		if listeners != 0 {
			t.Fatalf("%d stale feedback listeners", listeners)
		}
	}
	if err := pc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.done:
	case <-time.After(time.Second):
		t.Fatal("feedback reader outlived sender")
	}
	senderFeedbacks.Lock()
	_, retained := senderFeedbacks.readers[sender]
	senderFeedbacks.Unlock()
	if retained {
		t.Fatal("closed sender retained")
	}
}
