package webrtcav

import (
	"context"
	"sync"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

func TestTrackCodecBurstCoalescesAtPacketBoundary(t *testing.T) {
	r := newTrackReader(opusRemoteTrack(111), &fakeTrackRTPReader{packets: []*rtp.Packet{{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1, 2}}}})
	defer r.Close()
	streams, _ := r.Streams(t.Context())
	source, err := rtpav.NewSource(rtpav.SourceConfig{Receiver: r, Streams: streams, Depacketizers: []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(streams[0])}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if err := r.UpdateCodec(t.Context(), TrackCodecUpdate{Codec: opusRemoteTrack(111).Codec}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	// Draining observer notifications cannot steal the source's configuration.
	var last av.Epoch
	for len(r.Events()) != 0 {
		e := <-r.Events()
		if e.Epoch <= last {
			t.Fatal("updates published out of order")
		}
		last = e.Epoch
	}
	if last != 65 {
		t.Fatalf("latest observer epoch=%d", last)
	}
	current := av.Epoch(1)
	packets := 0
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Event != nil && m.Event.Type == av.EventCodecChanged {
			current = m.Event.Epoch
		}
		if m.Packet != nil {
			packets++
			if current != 65 || m.Packet.CodecEpoch != current {
				t.Errorf("event epoch=%d packet epoch=%d", current, m.Packet.CodecEpoch)
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if packets != 1 {
		t.Fatalf("packets=%d", packets)
	}
}

func TestTrackCodecBurstPreservesInflightPacket(t *testing.T) {
	transport := newBlockingTrackRTPReader()
	r := newTrackReader(opusRemoteTrack(111), transport)
	defer r.Close()
	streams, _ := r.Streams(t.Context())
	source, err := rtpav.NewSource(rtpav.SourceConfig{Receiver: r, Streams: streams, Depacketizers: []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(streams[0])}})
	if err != nil {
		t.Fatal(err)
	}
	epochs := make(chan av.Epoch, 16)
	done := make(chan error, 1)
	go func() {
		current := av.Epoch(1)
		done <- source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
			if m.Event != nil && m.Event.Type == av.EventCodecChanged {
				current = m.Event.Epoch
			}
			if m.Packet != nil {
				if m.Packet.CodecEpoch != current {
					t.Errorf("packet preceded configuration: %d/%d", m.Packet.CodecEpoch, current)
				}
				epochs <- m.Packet.CodecEpoch
			}
			return nil
		}))
	}()
	for cycle := range 8 {
		waitRead(t, transport)
		for range 9 {
			if err := r.UpdateCodec(t.Context(), TrackCodecUpdate{Codec: opusRemoteTrack(111).Codec}); err != nil {
				t.Fatal(err)
			}
		}
		transport.packets <- &rtp.Packet{Header: rtp.Header{PayloadType: 111, Timestamp: uint32(cycle+1) * 960, SequenceNumber: uint16(cycle)}, Payload: []byte{1, 2}}
		if got, want := <-epochs, av.Epoch(1+cycle*9); got != want {
			t.Fatalf("in-flight packet epoch=%d want %d", got, want)
		}
	}
	waitRead(t, transport)
	transport.packets <- &rtp.Packet{Header: rtp.Header{PayloadType: 111, Timestamp: 9 * 960, SequenceNumber: 8}, Payload: []byte{1, 2}}
	if got := <-epochs; got != 73 {
		t.Fatalf("latest packet epoch=%d want 73", got)
	}
	waitRead(t, transport)
	close(transport.packets)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
