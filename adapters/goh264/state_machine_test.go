//go:build goav_goh264

package goh264

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestH264StateMachineGuardsAndPendingClear(t *testing.T) {
	ctx := context.Background()

	// Unopened HandleEvent must return ErrUnsupportedFormat.
	unopened := &Decoder{}
	if err := unopened.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("unopened HandleEvent = %v, want ErrUnsupportedFormat", err)
	}

	dec := &Decoder{}
	if err := dec.Open(ctx, codec.DecodeConfig{
		Stream: h264TestStream(),
		Resilience: codec.ResiliencePolicy{
			DropDamagedVideo: true,
			RequestKeyframes: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	// Seed pending timings and verify entering Dropping clears them.
	dec.pushTiming(&av.Packet{PTS: av.Timestamp{Value: 100, Base: av.RTPTimeBase(90000)}})
	dec.pushTiming(&av.Packet{PTS: av.Timestamp{Value: 200, Base: av.RTPTimeBase(90000)}})
	if len(dec.pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(dec.pending))
	}

	out := h264DecodeResult(1)
	if err := dec.DecodeInto(ctx, &av.Packet{LossBefore: true}, &out); err != nil {
		t.Fatal(err)
	}
	if dec.state != h264Dropping {
		t.Fatalf("state = %d, want h264Dropping", dec.state)
	}
	if len(dec.pending) != 0 {
		t.Fatalf("pending after LossBefore = %d, want 0", len(dec.pending))
	}

	// Same check for EventPacketLoss.
	dec.state = h264Open
	dec.pushTiming(&av.Packet{PTS: av.Timestamp{Value: 300, Base: av.RTPTimeBase(90000)}})
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	if dec.state != h264Dropping || len(dec.pending) != 0 {
		t.Fatalf("after EventPacketLoss: state=%d pending=%d", dec.state, len(dec.pending))
	}

	// Same check for Corrupt with DropDamagedVideo.
	dec.state = h264Open
	dec.pushTiming(&av.Packet{PTS: av.Timestamp{Value: 400, Base: av.RTPTimeBase(90000)}})
	out.Reset()
	if err := dec.DecodeInto(ctx, &av.Packet{Corrupt: true}, &out); err != nil {
		t.Fatal(err)
	}
	if dec.state != h264Dropping || len(dec.pending) != 0 {
		t.Fatalf("after Corrupt: state=%d pending=%d", dec.state, len(dec.pending))
	}
}
