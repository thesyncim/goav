package webrtcav

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

func TestWHEPDeclaredTrackEndDoesNotDoubleDecrementPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	aRemote := opusRemoteTrack(111)
	aRemote.Stream.ID = "audio"
	aTransport1 := newBlockingTrackRTPReader()
	a1 := newTrackReader(aRemote, aTransport1)
	defer a1.Close()
	aStreams, _ := a1.Streams(ctx)

	s := &whepSession{
		ctx:     ctx,
		cancel:  cancel,
		records: make(chan whepRecord, 8),
		events:  make(chan av.Event, 4),
		state:   whepStreaming,
		declared: map[string]declaredTrack{
			"0": {streamID: "audio", reader: a1, state: declaredActive},
			"1": {streamID: "video", state: declaredPending},
		},
		readers:      []TrackReader{a1},
		streams:      []av.Stream{aStreams[0]},
		payloads:     []rtpav.PayloadMap{a1.PayloadMap()},
		activeTracks: 1,
	}

	// Track on mid "0" ends while mid "1" is still declaredPending.
	go s.pumpTracked(a1, true)
	waitRead(t, aTransport1)
	close(aTransport1.packets)

	_, event, _, err := s.ReadRTP(ctx)
	if err != nil {
		t.Fatalf("first EOF on mid 0 tore down session while mid 1 pending: %v", err)
	}
	if event == nil || event.Type != av.EventStreamRemoved || event.StreamID != "audio" {
		t.Fatalf("event = %+v, want EventStreamRemoved for audio", event)
	}

	// Simulate track re-arrival on the same mid "0" (declaredEnded -> declaredActive),
	// then EOF again: mid "1" must STILL be declaredPending and keep the session alive!
	aTransport2 := newBlockingTrackRTPReader()
	a2 := newTrackReader(aRemote, aTransport2)
	defer a2.Close()

	s.mu.Lock()
	dt := s.declared["0"]
	if dt.state != declaredEnded {
		s.mu.Unlock()
		t.Fatalf("mid 0 state = %v, want declaredEnded", dt.state)
	}
	dt.reader = a2
	dt.state = declaredActive
	s.declared["0"] = dt
	s.readers = append(s.readers, a2)
	s.streams = append(s.streams, aStreams[0])
	s.payloads = append(s.payloads, a2.PayloadMap())
	s.activeTracks++
	s.mu.Unlock()

	go s.pumpTracked(a2, true)
	waitRead(t, aTransport2)
	close(aTransport2.packets)

	_, event, _, err = s.ReadRTP(ctx)
	if err != nil {
		t.Fatalf("second EOF on mid 0 tore down session while mid 1 still pending: %v", err)
	}
	if event == nil || event.Type != av.EventStreamRemoved || event.StreamID != "audio" {
		t.Fatalf("event = %+v, want EventStreamRemoved for audio", event)
	}
	s.mu.Lock()
	stillPending := s.hasPendingDeclaredLocked()
	s.mu.Unlock()
	if !stillPending {
		t.Fatal("mid 1 should still be declaredPending")
	}
}

func TestEgressLifecycleAndConcurrentRaceFree(t *testing.T) {
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}, "audio", "stream")
	if err != nil {
		t.Fatal(err)
	}
	stream := av.Stream{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:        av.CodecOpus,
			Type:      av.MediaAudio,
			ClockRate: 48000,
			Channels:  2,
		},
	}

	sink := LocalTrack(track, nil).(*egress)

	// Handle before OpenStreams fails.
	pkt := av.Packet{StreamID: "audio", PTS: av.RTPToTimestamp(960, 48000), Payload: av.Buffer{Bytes: []byte{1, 2}}}
	msg := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &pkt}
	if err := sink.Handle(context.Background(), msg); err == nil {
		t.Fatal("Handle before OpenStreams succeeded")
	}

	// OpenStreams, UseControl, Close, then Handle -> ErrClosed.
	if err := sink.OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		t.Fatal(err)
	}
	if err := sink.OpenStreams(context.Background(), []av.Stream{stream}); err == nil {
		t.Fatal("double OpenStreams succeeded")
	}
	sink.UseControl(func(context.Context, control.Control) error { return nil })
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Handle(context.Background(), msg); !errors.Is(err, ErrClosed) {
		t.Fatalf("Handle after Close = %v, want ErrClosed", err)
	}

	// Reopening after Close works (LocalTrack reattach contract).
	if err := sink.OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		t.Fatalf("reopen after Close failed: %v", err)
	}

	// Concurrent Handle / UseControl / Close is race-free.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = sink.Handle(context.Background(), msg)
	}()
	go func() {
		defer wg.Done()
		sink.UseControl(func(context.Context, control.Control) error { return nil })
	}()
	go func() {
		defer wg.Done()
		_ = sink.Close()
	}()
	wg.Wait()
}

func TestTrackWriterWriteRTPAfterCloseAndConcurrentContinuity(t *testing.T) {
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}, "audio", "stream")
	if err != nil {
		t.Fatal(err)
	}
	w := newTrackWriter(nil, nil)
	w.add(42, track, nil)
	w.tracks[0].continuity = &continuity{clock: 48000, epoch: time.Now(), seq: 100}

	// Concurrent WriteRTP calls on the same trackOut with continuity must be race-free.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pkt := &rtp.Packet{
				Header:  rtp.Header{SSRC: 42, SequenceNumber: uint16(i), Timestamp: uint32(i * 960)},
				Payload: []byte{1, 2, 3},
			}
			_ = w.WriteRTP(context.Background(), pkt)
		}(i)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	pkt := &rtp.Packet{Header: rtp.Header{SSRC: 42, SequenceNumber: 99, Timestamp: 960}, Payload: []byte{1, 2}}
	if err := w.WriteRTP(context.Background(), pkt); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteRTP after Close = %v, want ErrClosed", err)
	}
}

func TestTrackReaderCloseClearsExpiredAndTransitionsClosed(t *testing.T) {
	remote := opusRemoteTrack(111)
	transport := newBlockingTrackRTPReader()
	r := newTrackReader(remote, transport)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, _, err := r.ReadRTP(ctx)
		done <- err
	}()
	waitRead(t, transport)
	cancel()
	<-done

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	state := r.state
	expired := r.expired
	r.mu.Unlock()
	if state != trackClosed {
		t.Fatalf("state = %v, want trackClosed", state)
	}
	if expired != nil {
		t.Fatal("r.expired was not cleared on Close()")
	}
	if _, _, _, err := r.ReadRTP(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadRTP after Close = %v, want ErrClosed", err)
	}
	if err := r.UpdateCodec(context.Background(), TrackCodecUpdate{Codec: remote.Codec}); !errors.Is(err, ErrClosed) {
		t.Fatalf("UpdateCodec after Close = %v, want ErrClosed", err)
	}
}
