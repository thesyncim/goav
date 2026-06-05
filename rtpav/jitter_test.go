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
	p10 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	p11 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 11}}
	p12 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 12}}

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
	p10 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	p14 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 14}}

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
	p10 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}

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
	p10 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	p20 := rtp.Packet{Header: rtp.Header{SSRC: 2, SequenceNumber: 20}}

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
	p10 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	p11 := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 11}}

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
