package rtpav

import (
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestJitterRingOrdersPackets(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 4), Events: make([]av.Event, 0, 2)}
	p10 := rtp.Packet{SSRC: 1, SequenceNumber: 10}
	p11 := rtp.Packet{SSRC: 1, SequenceNumber: 11}
	p12 := rtp.Packet{SSRC: 1, SequenceNumber: 12}

	if err := jitter.PushInto(ctx, &p10, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Ready) != 1 || result.Ready[0] != &p10 {
		t.Fatalf("ready after p10 = %+v", result.Ready)
	}

	result.Reset()
	if err := jitter.PushInto(ctx, &p12, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Ready) != 0 {
		t.Fatalf("ready after p12 = %+v", result.Ready)
	}

	result.Reset()
	if err := jitter.PushInto(ctx, &p11, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Ready) != 2 || result.Ready[0] != &p11 || result.Ready[1] != &p12 {
		t.Fatalf("ready after p11 = %+v", result.Ready)
	}
}

func TestJitterRingLargeGapEmitsLoss(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 2), Events: make([]av.Event, 0, 1)}
	p10 := rtp.Packet{SSRC: 1, SequenceNumber: 10}
	p14 := rtp.Packet{SSRC: 1, SequenceNumber: 14}

	if err := jitter.PushInto(ctx, &p10, &result); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	if err := jitter.PushInto(ctx, &p14, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != av.EventPacketLoss {
		t.Fatalf("events = %+v", result.Events)
	}
	if !result.State.LossBefore || result.State.Discontinuous {
		t.Fatalf("state = %+v", result.State)
	}
	if len(result.Ready) != 1 || result.Ready[0] != &p14 {
		t.Fatalf("ready = %+v", result.Ready)
	}
	if stats := jitter.Stats(); stats.Lost != 3 || stats.Expected != 15 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestJitterRingResultCapacity(t *testing.T) {
	jitter, err := NewJitterRing(JitterConfig{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{}
	p10 := rtp.Packet{SSRC: 1, SequenceNumber: 10}

	if err := jitter.PushInto(context.Background(), &p10, &result); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestJitterRingSSRCChangeEmitsDiscontinuity(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 2), Events: make([]av.Event, 0, 1)}
	p10 := rtp.Packet{SSRC: 1, SequenceNumber: 10}
	p20 := rtp.Packet{SSRC: 2, SequenceNumber: 20}

	if err := jitter.PushInto(ctx, &p10, &result); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	if err := jitter.PushInto(ctx, &p20, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != av.EventDiscontinuity {
		t.Fatalf("events = %+v", result.Events)
	}
	if result.State.LossBefore || !result.State.Discontinuous {
		t.Fatalf("state = %+v", result.State)
	}
}

func TestJitterRingAllocs(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 4), Events: make([]av.Event, 0, 2)}
	p10 := rtp.Packet{SSRC: 1, SequenceNumber: 10}
	p11 := rtp.Packet{SSRC: 1, SequenceNumber: 11}

	if allocs := testing.AllocsPerRun(1000, func() {
		jitter.Reset()
		result.Reset()
		if err := jitter.PushInto(ctx, &p10, &result); err != nil {
			t.Fatal(err)
		}
		result.Reset()
		if err := jitter.PushInto(ctx, &p11, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("jitter allocs = %v, want 0", allocs)
	}
}

func TestJitterRingLostExcludesBuffered(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 8), Events: make([]av.Event, 0, 4)}
	push := func(seq uint16) {
		t.Helper()
		result.Reset()
		if err := jitter.PushInto(ctx, &rtp.Packet{SSRC: 1, SequenceNumber: seq}, &result); err != nil {
			t.Fatal(err)
		}
	}
	push(10) // expected 11
	push(12) // buffered, 11 missing
	push(13) // buffered
	push(20) // delta 9 past the ring; 12 and 13 arrived, so 7 are lost
	if stats := jitter.Stats(); stats.Lost != 7 {
		t.Fatalf("lost = %d, want 7 (buffered 12 and 13 are not lost)", stats.Lost)
	}
}

// End-of-stream flush releases what is buffered in sequence order, not in
// ring-slot order.
func TestJitterRingFlushReleasesInSequenceOrder(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 8), Events: make([]av.Event, 0, 2)}
	for _, seq := range []uint16{5, 7, 8} {
		if err := jitter.PushInto(ctx, &rtp.Packet{SSRC: 1, SequenceNumber: seq}, &result); err != nil {
			t.Fatal(err)
		}
	}
	if got := jitterSeqs(result.Ready); !equalSeqs(got, []uint16{5}) {
		t.Fatalf("ready = %v, want [5]", got)
	}
	result.Reset()
	if err := jitter.FlushInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if got := jitterSeqs(result.Ready); !equalSeqs(got, []uint16{7, 8}) {
		t.Fatalf("flush = %v, want [7 8]", got)
	}
}

// Slots are positioned relative to the next expected sequence number, so
// packets either side of the uint16 wrap never share a slot.
func TestJitterRingReordersAcrossSequenceRollover(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 3})
	if err != nil {
		t.Fatal(err)
	}
	result := JitterResult{Ready: make([]*rtp.Packet, 0, 4), Events: make([]av.Event, 0, 2)}
	var released []uint16
	for _, seq := range []uint16{65533, 65535, 0, 65534} {
		result.Reset()
		if err := jitter.PushInto(ctx, &rtp.Packet{SSRC: 1, SequenceNumber: seq}, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Events) != 0 {
			t.Fatalf("events after %d = %+v", seq, result.Events)
		}
		released = append(released, jitterSeqs(result.Ready)...)
	}
	if want := []uint16{65533, 65534, 65535, 0}; !equalSeqs(released, want) {
		t.Fatalf("released = %v, want %v", released, want)
	}
	if stats := jitter.Stats(); stats.Lost != 0 || stats.Reordered != 0 || stats.Buffered != 0 || stats.Expected != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func jitterSeqs(packets []*rtp.Packet) []uint16 {
	seqs := make([]uint16, len(packets))
	for i := range packets {
		seqs[i] = packets[i].SequenceNumber
	}
	return seqs
}

func equalSeqs(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
