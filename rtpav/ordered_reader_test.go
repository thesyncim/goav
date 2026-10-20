package rtpav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

type orderedRecord struct {
	packet   *rtp.Packet
	event    *av.Event
	payloads PayloadMap
	err      error
}
type orderedReader struct {
	streams []av.Stream
	records []orderedRecord
	at      int
}

func (r *orderedReader) Streams(context.Context) ([]av.Stream, error) {
	return cloneStreams(r.streams), nil
}
func (r *orderedReader) PayloadMap() PayloadMap {
	if len(r.records) > 0 {
		return r.records[0].payloads
	}
	return NewStaticPayloadMap(0, nil)
}
func (r *orderedReader) Events() <-chan av.Event { return nil }
func (r *orderedReader) Close() error            { return nil }
func (r *orderedReader) ReadRTP(context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) {
	if r.at == len(r.records) {
		return nil, nil, nil, io.EOF
	}
	record := r.records[r.at]
	r.at++
	return record.packet, record.event, record.payloads, record.err
}

type staleReadWrapper struct{ PacketReader }

func (staleReadWrapper) ReadRTP(context.Context) (*rtp.Packet, error) { return nil, io.EOF }

func TestReadDecoratorCannotSilentlyHideConfiguration(t *testing.T) {
	if reflect.TypeOf(staleReadWrapper{}).Implements(reflect.TypeFor[PacketReader]()) {
		t.Fatal("stale two-result decorator still satisfies PacketReader")
	}
	stream := orderedAudioStream("audio", 1)
	next := stream
	next.Epoch = 2
	reader := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{
		{packet: orderedAudioPacket(1), payloads: orderedMap(stream)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: stream.ID, Stream: &next, Epoch: 2}, payloads: orderedMap(next)},
		{packet: orderedAudioPacket(2), payloads: orderedMap(next)},
	}}
	calls := 0
	wrapped := DecorateReader(reader, func(next ReadRTPFunc) ReadRTPFunc {
		return func(ctx context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) { calls++; return next(ctx) }
	})
	source, _, err := Receive(wrapped).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || !reflect.DeepEqual(epochs, []av.Epoch{1, 2}) {
		t.Fatalf("calls=%d epochs=%v", calls, epochs)
	}
}

func TestOrderedRecordsAddEqualCodecTracksAndKeepIndependentEpochs(t *testing.T) {
	a, b := orderedAudioStream("a", 1), orderedAudioStream("b", 1)
	next := a
	next.Epoch = 2
	r := &orderedReader{records: []orderedRecord{
		{event: &av.Event{Type: av.EventStreamAdded, StreamID: a.ID, Stream: &a}, payloads: orderedMap(a)},
		{event: &av.Event{Type: av.EventStreamAdded, StreamID: b.ID, Stream: &b}, payloads: orderedMap(b)},
		{packet: orderedAudioPacket(1), payloads: orderedMap(a)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &next, Epoch: 2}, payloads: orderedMap(next)},
		{packet: orderedAudioPacket(2), payloads: orderedMap(next)},
		{packet: orderedAudioPacket(3), payloads: orderedMap(b)},
	}}
	source, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var identities []string
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			identities = append(identities, string(m.Packet.StreamID))
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(identities, []string{"a", "a", "b"}) || !reflect.DeepEqual(epochs, []av.Epoch{1, 2, 1}) {
		t.Fatalf("streams=%v epochs=%v", identities, epochs)
	}
}

func TestOrderedThreeSameCodecTracksRouteExactIdentity(t *testing.T) {
	streams := []av.Stream{orderedAudioStream("a", 1), orderedAudioStream("b", 1), orderedAudioStream("c", 1)}
	r := &orderedReader{streams: streams}
	for i := range streams {
		r.records = append(r.records, orderedRecord{packet: orderedAudioPacket(uint16(i + 1)), payloads: orderedMap(streams[i])})
	}
	source, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var identities []av.StreamID
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			identities = append(identities, m.Packet.StreamID)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(identities, []av.StreamID{"a", "b", "c"}) {
		t.Fatalf("stream identities=%v", identities)
	}
}

func TestOrderedConfigurationIdentityValidation(t *testing.T) {
	a, b, unknown := orderedAudioStream("a", 2), orderedAudioStream("b", 2), orderedAudioStream("unknown", 2)
	mixed := append(orderedMap(a).Codecs(), PayloadCodec{PayloadType: 112, StreamID: b.ID, Parameters: b.Codec})
	for _, tt := range []struct {
		name   string
		record orderedRecord
		want   error
	}{
		{"event belongs to another map", orderedRecord{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &a, Epoch: 2}, payloads: orderedMap(b)}, ErrInvalidConfiguration},
		{"replacement collides with sibling", orderedRecord{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &b, Epoch: 2}, payloads: orderedMap(b)}, ErrInvalidConfiguration},
		{"map has multiple track scopes", orderedRecord{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &a, Epoch: 2}, payloads: NewStaticPayloadMap(2, mixed)}, ErrInvalidConfiguration},
		{"unknown replacement target", orderedRecord{event: &av.Event{Type: av.EventCodecChanged, StreamID: unknown.ID, Stream: &unknown, Epoch: 2}, payloads: orderedMap(unknown)}, ErrUnknownStream},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{tt.record}}
			source, _, err := Receive(r).OpenSource(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Start(t.Context(), testEmitter(func(context.Context, *pipeline.Message) error { return nil })); !errors.Is(err, tt.want) {
				t.Fatalf("Start=%v, want %v", err, tt.want)
			}
			for i, depacketizer := range source.(*Source).depacketizers {
				stream, _ := depacketizerStream(depacketizer)
				if stream.ID != r.streams[i].ID {
					t.Fatalf("invalid record rebound %q to %q", r.streams[i].ID, stream.ID)
				}
			}
		})
	}
}

func TestOrderedConfigurationUsesStreamIdentityWhenEventIDIsOmitted(t *testing.T) {
	a, b := orderedAudioStream("a", 1), orderedAudioStream("b", 1)
	next := b
	next.Epoch = 2
	r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{
		{event: &av.Event{Type: av.EventCodecChanged, Stream: &next, Epoch: 2}, payloads: orderedMap(next)},
		{packet: orderedAudioPacket(1), payloads: orderedMap(a)},
		{packet: orderedAudioPacket(2), payloads: orderedMap(next)},
	}}
	source, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var identities []av.StreamID
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			identities = append(identities, m.Packet.StreamID)
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(identities, []av.StreamID{"a", "b"}) || !reflect.DeepEqual(epochs, []av.Epoch{1, 2}) {
		t.Fatalf("identities=%v epochs=%v", identities, epochs)
	}
}

func TestOrderedTaggedConfigurationReplacesOldTargetID(t *testing.T) {
	a, b := orderedAudioStream("a", 1), orderedAudioStream("b", 1)
	next := a
	next.ID, next.Epoch = "replacement", 2
	r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &next, Epoch: 2}, payloads: orderedMap(next)},
		{packet: orderedAudioPacket(1), payloads: orderedMap(next)},
		{packet: orderedAudioPacket(2), payloads: orderedMap(b)},
	}}
	source, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var identities []av.StreamID
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			identities = append(identities, m.Packet.StreamID)
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(identities, []av.StreamID{next.ID, b.ID}) || !reflect.DeepEqual(epochs, []av.Epoch{2, 1}) {
		t.Fatalf("identities=%v epochs=%v", identities, epochs)
	}
}

func TestOrderedSingleStreamReplacementWithJitter(t *testing.T) {
	for _, eventOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "event with packet", true: "event only"}[eventOnly], func(t *testing.T) {
			old := orderedAudioStream("old", 1)
			next := orderedAudioStream("replacement", 2)
			update := orderedRecord{event: &av.Event{Type: av.EventCodecChanged, StreamID: old.ID, Stream: &next, Epoch: 2}, payloads: orderedMap(next)}
			// The transport may restart its sequence while retaining its SSRC.
			r := &orderedReader{streams: []av.Stream{old}, records: []orderedRecord{{packet: orderedAudioPacket(100), payloads: orderedMap(old)}}}
			if eventOnly {
				r.records = append(r.records, update, orderedRecord{packet: orderedAudioPacket(1), payloads: orderedMap(next)})
			} else {
				update.packet = orderedAudioPacket(1)
				r.records = append(r.records, update)
			}
			jitter, _ := NewJitterRing(JitterConfig{Capacity: 8})
			source, _, err := Receive(r, WithJitter(jitter)).OpenSource(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var identities []av.StreamID
			if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
				if m.Packet != nil {
					identities = append(identities, m.Packet.StreamID)
				}
				return nil
			})); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(identities, []av.StreamID{old.ID, next.ID}) {
				t.Fatalf("identities=%v", identities)
			}
		})
	}
}

func TestOrderedRecordValidation(t *testing.T) {
	stream := orderedAudioStream("audio", 2)
	for _, tt := range []struct {
		name   string
		record orderedRecord
		want   error
	}{
		{"nil map", orderedRecord{packet: orderedAudioPacket(2)}, ErrInvalidConfiguration},
		{"epoch regression", orderedRecord{packet: orderedAudioPacket(2), payloads: orderedMap(orderedAudioStream("audio", 1))}, ErrEpochRegression},
		{"unannounced epoch", orderedRecord{packet: orderedAudioPacket(2), payloads: orderedMap(orderedAudioStream("audio", 3))}, ErrInvalidConfiguration},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{{packet: orderedAudioPacket(1), payloads: orderedMap(stream)}, tt.record}}
			source, _, err := Receive(r).OpenSource(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			err = source.Start(t.Context(), testEmitter(func(context.Context, *pipeline.Message) error { return nil }))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Start=%v", err)
			}
		})
	}
}

func TestOrderedStreamRemovedDrainsOldJitterMap(t *testing.T) {
	stream := orderedAudioStream("audio", 1)
	r := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{
		{packet: orderedAudioPacket(1), payloads: orderedMap(stream)},
		{packet: orderedAudioPacket(3), payloads: orderedMap(stream)},
		{event: &av.Event{Type: av.EventStreamRemoved, StreamID: stream.ID, Epoch: 2}, payloads: NewStaticPayloadMap(2, nil)},
	}}
	jitter, _ := NewJitterRing(JitterConfig{Capacity: 8})
	source, err := NewSource(SourceConfig{Receiver: r, Streams: r.streams, Jitter: jitter, Depacketizers: []Depacketizer{NewOpusDepacketizer(stream)}})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			count++
			if m.Packet.CodecEpoch != 1 {
				t.Error("old jitter packet used new map")
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("packets=%d", count)
	}
}

func TestConfigurationBoundaryDiscardsIncompleteVideoWithAndWithoutJitter(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		stream := av.Stream{ID: "video", Epoch: 1, Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, ClockRate: 90000}}
		next := stream
		next.Epoch = 2
		mapping := func(s av.Stream) PayloadMap {
			return NewStaticPayloadMap(s.Epoch, []PayloadCodec{{PayloadType: 96, StreamID: s.ID, Parameters: s.Codec, ClockRate: 90000}})
		}
		r := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{
			{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 1, Timestamp: 100}, Payload: []byte{0x7c, 0x85, 1}}, payloads: mapping(stream)},
			{event: &av.Event{Type: av.EventCodecChanged, StreamID: stream.ID, Stream: &next, Epoch: 2}, payloads: mapping(next)},
			{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 2, Timestamp: 200, Marker: true}, Payload: []byte{0x65, 2}}, payloads: mapping(next)},
		}}
		config := SourceConfig{Receiver: r, Streams: r.streams, Depacketizers: []Depacketizer{NewH264Depacketizer(stream)}}
		if buffered {
			config.Jitter, _ = NewJitterRing(JitterConfig{Capacity: 8})
		}
		s, err := NewSource(config)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
			if m.Packet != nil {
				count++
				if m.Packet.CodecEpoch != 2 {
					t.Error("partial old unit escaped")
				}
			}
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		if count != 1 || s.DroppedMessages() != 1 {
			t.Fatalf("jitter=%v packets=%d discarded=%d", buffered, count, s.DroppedMessages())
		}
	}
}

type legacyStaticReader struct {
	payloads PayloadMap
	events   chan av.Event
	read     func() (*rtp.Packet, error)
}

func (r *legacyStaticReader) Streams(context.Context) ([]av.Stream, error) { return nil, nil }
func (r *legacyStaticReader) PayloadMap() PayloadMap                       { return r.payloads }
func (r *legacyStaticReader) Events() <-chan av.Event                      { return r.events }
func (r *legacyStaticReader) Close() error                                 { return nil }
func (r *legacyStaticReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.read != nil {
		return r.read()
	}
	return nil, io.EOF
}
func TestStaticAdapterRejectsConfigurationChanges(t *testing.T) {
	for _, kind := range []av.EventType{av.EventCodecChanged, av.EventStreamAdded, av.EventStreamRemoved} {
		legacy := &legacyStaticReader{payloads: NewStaticPayloadMap(1, nil), events: make(chan av.Event, 1)}
		reader := Static(legacy, legacy.payloads)
		legacy.events <- av.Event{Type: kind}
		if _, _, _, err := reader.ReadRTP(t.Context()); !errors.Is(err, ErrOrderedConfigurationRequired) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestStaticAdapterRefusalAtEOFSurfacesFromSource(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(map[bool]string{false: "configuration event", true: "map drift"}[drift], func(t *testing.T) {
			legacy := &legacyStaticReader{payloads: NewStaticPayloadMap(1, nil), events: make(chan av.Event, 1)}
			legacy.read = func() (*rtp.Packet, error) {
				if drift {
					legacy.payloads = NewStaticPayloadMap(2, nil)
				} else {
					legacy.events <- av.Event{Type: av.EventCodecChanged}
				}
				return nil, io.EOF
			}
			source, err := NewSource(SourceConfig{Receiver: Static(legacy, nil)})
			if err != nil {
				t.Fatal(err)
			}
			events := 0
			if err := source.Start(t.Context(), testEmitter(func(context.Context, *pipeline.Message) error { events++; return nil })); !errors.Is(err, ErrOrderedConfigurationRequired) {
				t.Fatalf("Start=%v", err)
			}
			if events != 0 {
				t.Fatal("refusal announced successful end of stream")
			}
		})
	}
}

func TestOrderedJoinedEOFPreservesRealFailure(t *testing.T) {
	failed := errors.New("transport failed while closing")
	reader := &orderedReader{records: []orderedRecord{{err: errors.Join(failed, io.EOF)}}}
	source, err := NewSource(SourceConfig{Receiver: reader})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Start(t.Context(), testEmitter(func(context.Context, *pipeline.Message) error { return nil })); !errors.Is(err, failed) {
		t.Fatalf("Start=%v", err)
	}
}

func orderedAudioStream(id string, epoch av.Epoch) av.Stream {
	return av.Stream{ID: av.StreamID(id), Epoch: epoch, Type: av.MediaAudio, TimeBase: av.RTPTimeBase(48000), Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, ClockRate: 48000, Channels: 2}}
}
func orderedMap(stream av.Stream) PayloadMap {
	return NewStaticPayloadMap(stream.Epoch, []PayloadCodec{{PayloadType: 111, StreamID: stream.ID, Parameters: stream.Codec, ClockRate: 48000}})
}
func orderedAudioPacket(seq uint16) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960}, Payload: []byte{1, 2}}
}

func TestOrderedMapUsesSessionGenerationForUntaggedStreams(t *testing.T) {
	a := orderedAudioStream("audio", 1)
	v := av.Stream{ID: "video", Epoch: 1, Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000}}
	next := v
	next.Epoch = 2
	mapping := func(epoch av.Epoch) PayloadMap {
		return NewStaticPayloadMap(epoch, []PayloadCodec{{PayloadType: 111, Parameters: a.Codec, ClockRate: 48000}, {PayloadType: 96, Parameters: v.Codec, ClockRate: 90000}})
	}
	reader := &orderedReader{streams: []av.Stream{a, v}, records: []orderedRecord{
		{packet: orderedAudioPacket(1), payloads: mapping(10)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: v.ID, Stream: &next, Epoch: 2}, payloads: mapping(11)},
		{packet: orderedAudioPacket(2), payloads: mapping(11)},
	}}
	s, _, err := Receive(reader).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	packets := 0
	if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			packets++
			if m.Packet.CodecEpoch != 1 {
				t.Error("video update changed audio epoch")
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if packets != 2 {
		t.Fatalf("packets=%d", packets)
	}
}

func TestOrderedRemovalAllowsNewStreamIncarnation(t *testing.T) {
	for _, jittered := range []bool{false, true} {
		for _, combined := range []bool{false, true} {
			name := map[bool]string{false: "direct", true: "jitter"}[jittered] + "/" + map[bool]string{false: "separate records", true: "combined record"}[combined]
			t.Run(name, func(t *testing.T) {
				old, next := orderedAudioStream("audio", 8), orderedAudioStream("audio", 1)
				added := orderedRecord{event: &av.Event{Type: av.EventStreamAdded, StreamID: next.ID, Stream: &next, Epoch: 1}, payloads: orderedMap(next)}
				r := &orderedReader{streams: []av.Stream{old}, records: []orderedRecord{
					{packet: orderedAudioPacket(100), payloads: orderedMap(old)},
					{event: &av.Event{Type: av.EventStreamRemoved, StreamID: old.ID, Epoch: 9}, payloads: NewStaticPayloadMap(9, nil)},
				}}
				if combined {
					added.packet = orderedAudioPacket(1)
					r.records = append(r.records, added)
				} else {
					r.records = append(r.records, added, orderedRecord{packet: orderedAudioPacket(1), payloads: orderedMap(next)})
				}
				var options []ReceiveOption
				if jittered {
					jitter, _ := NewJitterRing(JitterConfig{Capacity: 8})
					options = append(options, WithJitter(jitter))
				}
				s, _, err := Receive(r, options...).OpenSource(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				var epochs []av.Epoch
				if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
					if m.Packet != nil {
						epochs = append(epochs, m.Packet.CodecEpoch)
					}
					return nil
				})); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(epochs, []av.Epoch{8, 1}) {
					t.Fatalf("epochs=%v", epochs)
				}
			})
		}
	}
}

func TestOrderedIndependentTracksCanBeRemovedWithEmptyMaps(t *testing.T) {
	a, b := orderedAudioStream("a", 8), orderedAudioStream("b", 2)
	r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{
		{packet: orderedAudioPacket(1), payloads: orderedMap(a)},
		{packet: orderedAudioPacket(2), payloads: orderedMap(b)},
		{event: &av.Event{Type: av.EventStreamRemoved, StreamID: a.ID, Epoch: 9}, payloads: NewStaticPayloadMap(9, nil)},
		{event: &av.Event{Type: av.EventStreamRemoved, Stream: &b, Epoch: 3}, payloads: NewStaticPayloadMap(3, nil)},
	}}
	source, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var removed []av.StreamID
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Event != nil && m.Event.Type == av.EventStreamRemoved {
			removed = append(removed, m.Event.StreamID)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(removed, []av.StreamID{a.ID, b.ID}) || len(source.(*Source).depacketizers) != 0 {
		t.Fatalf("removed=%v remaining depacketizers=%d", removed, len(source.(*Source).depacketizers))
	}
}

func TestOrderedNamedCodecIntentUsesUniqueTrack(t *testing.T) {
	stream := orderedAudioStream("transport-id", 1)
	r := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{{packet: orderedAudioPacket(1), payloads: orderedMap(stream)}}}
	s, _, err := Receive(r, WithName("mic"), WithCodec(codec.Spec{ID: av.CodecOpus, Type: av.MediaAudio})).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil && m.Packet.StreamID != "mic" {
			t.Fatalf("stream=%s", m.Packet.StreamID)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedNamedInputUpdatesEpochOnTrackReplacement(t *testing.T) {
	old, next := orderedAudioStream("transport-id", 1), orderedAudioStream("replacement-id", 2)
	r := &orderedReader{streams: []av.Stream{old}, records: []orderedRecord{
		{packet: orderedAudioPacket(1), payloads: orderedMap(old)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: next.ID, Stream: &next, Epoch: 2}, packet: orderedAudioPacket(2), payloads: orderedMap(next)},
	}}
	source, _, err := Receive(r, WithName("mic"), WithCodec(codec.Spec{ID: av.CodecOpus, Type: av.MediaAudio})).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochs, []av.Epoch{1, 2}) {
		t.Fatalf("epochs=%v", epochs)
	}
}

func TestOrderedNamedVideoReplacementDiscardsOldAssembly(t *testing.T) {
	old := av.Stream{ID: "transport-id", Epoch: 1, Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, ClockRate: 90000}}
	next := old
	next.ID, next.Epoch = "replacement-id", 2
	mapping := func(stream av.Stream) PayloadMap {
		return NewStaticPayloadMap(stream.Epoch, []PayloadCodec{{PayloadType: 96, StreamID: stream.ID, Parameters: stream.Codec, ClockRate: 90000}})
	}
	r := &orderedReader{streams: []av.Stream{old}, records: []orderedRecord{
		{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 1, Timestamp: 100}, Payload: []byte{0x7c, 0x85, 1}}, payloads: mapping(old)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: next.ID, Stream: &next, Epoch: 2}, payloads: mapping(next)},
		{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 2, Timestamp: 200, Marker: true}, Payload: []byte{0x65, 2}}, payloads: mapping(next)},
	}}
	source, _, err := Receive(r, WithName("camera"), WithCodec(codec.Spec{ID: av.CodecH264, Type: av.MediaVideo})).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var epochs []av.Epoch
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochs, []av.Epoch{2}) || source.(*Source).DroppedMessages() != 1 {
		t.Fatalf("epochs=%v discarded=%d", epochs, source.(*Source).DroppedMessages())
	}
}

func TestOrderedVideoChangeDoesNotResetSiblingAssembly(t *testing.T) {
	a := av.Stream{ID: "a", Epoch: 1, Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, ClockRate: 90000}}
	b, next := a, a
	b.ID = "b"
	next.Epoch = 2
	mapping := func(s av.Stream) PayloadMap {
		return NewStaticPayloadMap(s.Epoch, []PayloadCodec{{PayloadType: 96, StreamID: s.ID, Parameters: s.Codec, ClockRate: 90000}})
	}
	r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{
		{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 1, Timestamp: 100}, Payload: []byte{0x7c, 0x85, 1}}, payloads: mapping(b)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Stream: &next, Epoch: 2}, payloads: mapping(next)},
		{packet: &rtp.Packet{Header: rtp.Header{PayloadType: 96, SequenceNumber: 2, Timestamp: 100, Marker: true}, Payload: []byte{0x7c, 0x45, 2}}, payloads: mapping(b)},
	}}
	s, _, err := Receive(r).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			count++
			if m.Packet.StreamID != "b" || m.Packet.CodecEpoch != 1 {
				t.Errorf("sibling packet=%+v", m.Packet)
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if count != 1 || s.(*Source).DroppedMessages() != 0 {
		t.Fatalf("packets=%d discarded=%d", count, s.(*Source).DroppedMessages())
	}
}

type feedbackOrderedReader struct {
	orderedReader
	writes int
}

func (r *feedbackOrderedReader) WriteRTCP(context.Context, []rtcp.Packet) error {
	r.writes++
	return nil
}
func TestReadDecoratorPreservesFeedback(t *testing.T) {
	r := &feedbackOrderedReader{}
	decorated := DecorateReader(r, func(read ReadRTPFunc) ReadRTPFunc { return read })
	if err := decorated.(FeedbackWriter).WriteRTCP(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if r.writes != 1 {
		t.Fatal("feedback lost through decorator")
	}
}

func TestOrderedFinalPacketWithEOFIsDelivered(t *testing.T) {
	for _, terminal := range []error{io.EOF, fmt.Errorf("read finished: %w", io.EOF)} {
		t.Run(terminal.Error(), func(t *testing.T) {
			stream := orderedAudioStream("audio", 1)
			r := &orderedReader{streams: []av.Stream{stream}, records: []orderedRecord{{packet: orderedAudioPacket(1), payloads: orderedMap(stream), err: terminal}}}
			s, _, err := Receive(r).OpenSource(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			packets := 0
			if err := s.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
				if m.Packet != nil {
					packets++
				}
				return nil
			})); err != nil {
				t.Fatal(err)
			}
			if packets != 1 {
				t.Fatal("lost valid final packet")
			}
		})
	}
}

func TestOneJitterWindowRefusesConcurrentStreamScopes(t *testing.T) {
	for _, added := range []bool{false, true} {
		t.Run(map[bool]string{false: "declared streams", true: "added stream with packet"}[added], func(t *testing.T) {
			a, b := orderedAudioStream("a", 1), orderedAudioStream("b", 1)
			r := &orderedReader{streams: []av.Stream{a, b}, records: []orderedRecord{
				{packet: orderedAudioPacket(1), payloads: orderedMap(a)},
				{packet: orderedAudioPacket(2), payloads: orderedMap(b)},
			}}
			if added {
				r.streams = r.streams[:1]
				r.records[1].event = &av.Event{Type: av.EventStreamAdded, StreamID: b.ID, Stream: &b}
			}
			jitter, _ := NewJitterRing(JitterConfig{Capacity: 8})
			s, _, err := Receive(r, WithJitter(jitter)).OpenSource(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Start(t.Context(), testEmitter(func(context.Context, *pipeline.Message) error { return nil })); !errors.Is(err, ErrJitterStreamConflict) {
				t.Fatalf("shared jitter=%v", err)
			}
		})
	}
}
