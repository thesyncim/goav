package webrtcav

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

type countedWHEPReader struct {
	rtpav.PacketReader
	remaining int
}

func (r *countedWHEPReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	if r.remaining == 0 {
		return nil, nil, nil, io.EOF
	}
	r.remaining--
	return r.PacketReader.ReadRTP(ctx)
}

func TestWHEPConcurrentEqualPayloadTracksKeepIndependentEpochs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	aRemote, bRemote := opusRemoteTrack(111), opusRemoteTrack(111)
	aRemote.Stream.ID, bRemote.Stream.ID = "a", "b"
	aTransport, bTransport := newBlockingTrackRTPReader(), newBlockingTrackRTPReader()
	a, b := newTrackReader(aRemote, aTransport), newTrackReader(bRemote, bTransport)
	defer a.Close()
	defer b.Close()
	aStreams, _ := a.Streams(ctx)
	bStreams, _ := b.Streams(ctx)
	s := &whepSession{ctx: ctx, cancel: cancel, records: make(chan whepRecord, 2), streams: []av.Stream{aStreams[0], bStreams[0]}, payloads: []rtpav.PayloadMap{a.PayloadMap(), b.PayloadMap()}}
	if _, ok := s.PayloadMap().Lookup(111); ok {
		t.Fatal("ambiguous inventory PT resolved to one track")
	}
	pumps := make(chan struct{}, 2)
	go func() { s.pump(a); pumps <- struct{}{} }()
	go func() { s.pump(b); pumps <- struct{}{} }()
	input := rtpav.Receive(&countedWHEPReader{PacketReader: s, remaining: 4})
	source, _, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type packetIdentity struct {
		id    av.StreamID
		epoch av.Epoch
	}
	packets := make(chan packetIdentity, 4)
	done := make(chan error, 1)
	go func() {
		done <- source.Start(ctx, testEmitter(func(_ context.Context, m *pipeline.Message) error {
			if m.Packet != nil {
				packets <- packetIdentity{m.Packet.StreamID, m.Packet.CodecEpoch}
			}
			return nil
		}))
	}()
	waitRead(t, aTransport)
	waitRead(t, bTransport)
	var updates sync.WaitGroup
	updates.Go(func() {
		if err := a.UpdateCodec(ctx, TrackCodecUpdate{Codec: aRemote.Codec}); err != nil {
			t.Error(err)
		}
	})
	updates.Go(func() {
		for range 3 {
			if err := b.UpdateCodec(ctx, TrackCodecUpdate{Codec: bRemote.Codec}); err != nil {
				t.Error(err)
			}
		}
	})
	updates.Wait()
	packet := func(ssrc uint32, seq uint16) *rtp.Packet {
		return &rtp.Packet{Header: rtp.Header{SSRC: ssrc, PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960}, Payload: []byte{1, 2}}
	}
	aTransport.packets <- packet(1, 1)
	bTransport.packets <- packet(2, 1)
	for range 2 {
		p := <-packets
		if p.epoch != 1 {
			t.Fatalf("in-flight packet=%+v", p)
		}
	}
	waitRead(t, aTransport)
	waitRead(t, bTransport)
	aTransport.packets <- packet(1, 2)
	bTransport.packets <- packet(2, 2)
	got := map[av.StreamID]av.Epoch{}
	for range 2 {
		p := <-packets
		got[p.id] = p.epoch
	}
	if got["a"] != 2 || got["b"] != 4 {
		t.Fatalf("independent epochs=%v", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cancel()
	for range 2 {
		select {
		case <-pumps:
		case <-time.After(time.Second):
			t.Fatal("pump outlived cancellation")
		}
	}
}

type queuedTrackReader struct {
	TrackReader
	stream  av.Stream
	records []whepRecord
	at      int
	reads   atomic.Int64
}

func (r *queuedTrackReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.stream}, nil
}
func (r *queuedTrackReader) PayloadMap() rtpav.PayloadMap { return r.records[0].payloads }
func (r *queuedTrackReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	r.reads.Add(1)
	if r.at == len(r.records) {
		<-ctx.Done()
		return nil, nil, nil, ctx.Err()
	}
	record := r.records[r.at]
	r.at++
	return record.packet, record.event, record.payloads, record.err
}

func TestWHEPLateTrackConfigurationWaitsForQueueAndCancels(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		s := &whepSession{ctx: ctx, cancel: cancel, records: make(chan whepRecord, 1)}
		stream := av.Stream{ID: "late", Epoch: 1, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
		mapping := rtpav.NewStaticPayloadMap(1, []rtpav.PayloadCodec{{PayloadType: 111, StreamID: stream.ID, Parameters: stream.Codec, ClockRate: 48000}})
		r := &queuedTrackReader{stream: stream, records: []whepRecord{{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 111}}, payloads: mapping, err: io.EOF}}}
		s.records <- whepRecord{payloads: mapping}
		done := make(chan error, 1)
		go func() {
			done <- s.startTrack(r, &av.Event{Type: av.EventStreamAdded, StreamID: stream.ID, Stream: &stream, Epoch: 1})
		}()
		if abandon {
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("start=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("late-track registration leaked")
			}
			if r.reads.Load() != 0 {
				t.Fatal("started pump before announcement")
			}
			continue
		}
		<-s.records
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		record := <-s.records
		if record.event == nil || record.event.Type != av.EventStreamAdded || record.packet != nil {
			t.Fatalf("first late-track record=%+v", record)
		}
		record = <-s.records
		if record.packet == nil || record.payloads.Epoch() != 1 {
			t.Fatalf("packet record=%+v", record)
		}
		cancel()
	}
}

func TestWHEPQueuedEventsOwnMutableReaderStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream := av.Stream{ID: "a", Epoch: 1, Metadata: av.Metadata{"name": "original"}, Codec: av.CodecParameters{ID: av.CodecOpus}}
	event := av.Event{Type: av.EventStreamAdded, StreamID: "a", Stream: &stream}
	s := &whepSession{ctx: ctx, cancel: cancel, records: make(chan whepRecord, 1)}
	mapping := rtpav.NewStaticPayloadMap(1, nil)
	r := &mutatingRecordReader{event: &event, mapping: mapping, mutated: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.pump(r); close(done) }()
	<-r.mutated
	first := <-s.records
	if first.event.Stream.ID != "a" || first.event.Stream.Metadata["name"] != "original" {
		t.Fatalf("queued event aliased reader: %+v", first.event)
	}
	<-s.records
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("pump did not finish")
	}
}

type mutatingRecordReader struct {
	TrackReader
	event   *av.Event
	mapping rtpav.PayloadMap
	at      int
	mutated chan struct{}
}

func (r *mutatingRecordReader) ReadRTP(context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	r.at++
	if r.at == 1 {
		return nil, r.event, r.mapping, nil
	}
	r.event.Stream.ID = "mutated"
	r.event.Stream.Metadata["name"] = "mutated"
	close(r.mutated)
	return nil, nil, nil, io.EOF
}

func TestNamedTrackInputStartsWithTaggedMap(t *testing.T) {
	remote := opusRemoteTrack(111)
	remote.Stream.ID = "transport-id"
	r := newTrackReader(remote, &fakeTrackRTPReader{packets: []*rtp.Packet{{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1, 2}}}})
	defer r.Close()
	source, _, err := trackInput(r, rtpav.WithName("mic")).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil && m.Packet.StreamID != "mic" {
			t.Fatalf("stream=%s", m.Packet.StreamID)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
}

func TestTrackCodecUpdatesNormalizeOlderEpochs(t *testing.T) {
	remote := opusRemoteTrack(111)
	remote.Stream.Epoch = 7
	r := newTrackReader(remote, &fakeTrackRTPReader{})
	defer r.Close()
	if err := r.UpdateCodec(t.Context(), TrackCodecUpdate{Stream: av.Stream{Epoch: 2}, Codec: remote.Codec}); err != nil {
		t.Fatal(err)
	}
	_, event, payloads, err := r.ReadRTP(t.Context())
	if !errors.Is(err, io.EOF) || event == nil || event.Epoch != 8 || payloads.Epoch() != 8 {
		t.Fatalf("epoch update: event=%+v map=%v err=%v", event, payloads, err)
	}
}
