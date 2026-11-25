package source

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/raceflag"
	"github.com/thesyncim/goav/pipeline"
)

// recordEmitter is a plain pipeline.Emitter that records what Push hands it.
type recordEmitter struct {
	messages []pipeline.Message
	err      error
}

func (e *recordEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	e.messages = append(e.messages, *msg)
	return e.err
}

// deliveryEmitter exercises the pipeline.DeliveryEmitter fast path.
type deliveryEmitter struct {
	recordEmitter
	delivery pipeline.Delivery
}

func (e *deliveryEmitter) EmitDelivery(ctx context.Context, msg *pipeline.Message) (pipeline.Delivery, error) {
	err := e.Emit(ctx, msg)
	return e.delivery, err
}

func TestPushNilMediaIsANoOp(t *testing.T) {
	emitter := &recordEmitter{}
	push := NewPush(context.Background(), emitter, "s")
	if result, err := push.Packet(nil); err != nil || result != (Result{}) {
		t.Fatalf("nil packet = %+v, %v; want zero result, nil error", result, err)
	}
	if result, err := push.Frame(nil); err != nil || result != (Result{}) {
		t.Fatalf("nil frame = %+v, %v; want zero result, nil error", result, err)
	}
	if len(emitter.messages) != 0 {
		t.Fatalf("nil media reached the emitter: %d messages", len(emitter.messages))
	}
}

func TestPushAppliesDeclaredStreamID(t *testing.T) {
	emitter := &recordEmitter{}
	push := NewPush(context.Background(), emitter, "declared")

	if _, err := push.Packet(&av.Packet{}); err != nil {
		t.Fatal(err)
	}
	if _, err := push.Packet(&av.Packet{StreamID: "explicit"}); err != nil {
		t.Fatal(err)
	}
	if _, err := push.Frame(&av.Frame{}); err != nil {
		t.Fatal(err)
	}
	if _, err := push.Event(av.Event{Type: av.EventType("tick")}); err != nil {
		t.Fatal(err)
	}
	stream := av.Stream{ID: "from-stream"}
	if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &stream}); err != nil {
		t.Fatal(err)
	}

	got := []av.StreamID{
		emitter.messages[0].Packet.StreamID,
		emitter.messages[1].Packet.StreamID,
		emitter.messages[2].Frame.StreamID,
		emitter.messages[3].Event.StreamID,
		emitter.messages[4].Event.StreamID,
	}
	want := []av.StreamID{"declared", "explicit", "declared", "declared", "from-stream"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d stream = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPushResultFromPlainEmitter(t *testing.T) {
	ok := NewPush(context.Background(), &recordEmitter{}, "s")
	result, err := ok.Packet(&av.Packet{})
	if err != nil || !result.Accepted || result.Dropped {
		t.Fatalf("plain emit success = %+v, %v; want accepted", result, err)
	}

	boom := errors.New("boom")
	failing := NewPush(context.Background(), &recordEmitter{err: boom}, "s")
	result, err = failing.Packet(&av.Packet{})
	if !errors.Is(err, boom) || result.Accepted {
		t.Fatalf("plain emit failure = %+v, %v; want refusal with cause", result, err)
	}
}

func TestPushResultFromDeliveryEmitter(t *testing.T) {
	cases := []struct {
		name     string
		delivery pipeline.Delivery
		want     Result
	}{
		{"all delivered", pipeline.Delivery{Delivered: 2}, Result{Accepted: true}},
		{"partially shed", pipeline.Delivery{Delivered: 1, Shed: 1}, Result{Accepted: true, Dropped: true}},
		{"fully shed", pipeline.Delivery{Shed: 2}, Result{Dropped: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			emitter := &deliveryEmitter{delivery: tc.delivery}
			push := NewPush(context.Background(), emitter, "s")
			result, err := push.Packet(&av.Packet{})
			if err != nil || result != tc.want {
				t.Fatalf("delivery %+v = %+v, %v; want %+v", tc.delivery, result, err, tc.want)
			}
		})
	}
}

func TestPushEOSEmitsPerStream(t *testing.T) {
	emitter := &recordEmitter{}
	push := NewPush(context.Background(), emitter, "declared")

	if err := push.EOS(); err != nil {
		t.Fatal(err)
	}
	if err := push.EOS("a", "b"); err != nil {
		t.Fatal(err)
	}

	if len(emitter.messages) != 3 {
		t.Fatalf("EOS produced %d events, want 3", len(emitter.messages))
	}
	want := []av.StreamID{"declared", "a", "b"}
	for i, id := range want {
		event := emitter.messages[i].Event
		if event == nil || event.Type != av.EventEndOfStream || event.StreamID != id {
			t.Fatalf("EOS event %d = %+v, want EndOfStream on %q", i, event, id)
		}
	}
}

type discardEmitter struct{}

func (discardEmitter) Emit(context.Context, *pipeline.Message) error { return nil }

func (discardEmitter) EmitDelivery(context.Context, *pipeline.Message) (pipeline.Delivery, error) {
	return pipeline.Delivery{Delivered: 1}, nil
}

func TestPushConcurrentSafety(t *testing.T) {
	push := NewPush(context.Background(), discardEmitter{}, "s")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pkt := &av.Packet{StreamID: "s"}
			frm := &av.Frame{StreamID: "s"}
			for range 256 {
				if _, err := push.Packet(pkt); err != nil {
					t.Errorf("Packet err = %v", err)
					return
				}
				if _, err := push.Frame(frm); err != nil {
					t.Errorf("Frame err = %v", err)
					return
				}
				if _, err := push.Event(av.Event{Type: av.EventType("tick"), StreamID: "s"}); err != nil {
					t.Errorf("Event err = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestPushZeroAllocs(t *testing.T) {
	if raceflag.Enabled {
		t.Skip("sync.Pool drops pooled messages under -race")
	}
	push := NewPush(context.Background(), discardEmitter{}, "s")
	pkt := &av.Packet{StreamID: "s"}
	frm := &av.Frame{StreamID: "s"}

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Packet(pkt); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Push.Packet allocs = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Frame(frm); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Push.Frame allocs = %v, want 0", allocs)
	}
}
