package gopus

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestEncoderFlushIntoTerminatesAfterDrain(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	frame := opusTestFrame()
	result := opusEncodeResult(4, 4096)
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("encoded packets = %d, want 1", len(result.Packets))
	}

	// First flush drains lookahead.
	flush1 := opusEncodeResult(4, 4096)
	if err := encoder.FlushInto(ctx, &flush1); err != nil {
		t.Fatal(err)
	}
	if len(flush1.Packets) == 0 {
		t.Fatal("first flush emitted 0 packets, want lookahead tail")
	}
	if encoder.state != encoderFlushed {
		t.Fatalf("state after full flush = %d, want encoderFlushed", encoder.state)
	}

	// Repeated flushes must return nil with 0 packets (no infinite silence loop).
	for i := range 3 {
		flushN := opusEncodeResult(4, 4096)
		if err := encoder.FlushInto(ctx, &flushN); err != nil {
			t.Fatalf("repeat flush %d: %v", i, err)
		}
		if len(flushN.Packets) != 0 {
			t.Fatalf("repeat flush %d emitted %d packets, want 0", i, len(flushN.Packets))
		}
	}
}

func TestEncoderFlushIntoMultiCallWithSmallResultBuffer(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	// Feed a 1024-sample frame so 64 samples stay pending + 312 lookahead.
	frame := opusTestFrame()
	frame.Planes[0].Buffer.Bytes = make([]byte, 1024*2)
	frame.Audio.Samples = 1024
	result := opusEncodeResult(4, 4096)
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}

	totalFlushed := 0
	for range 10 {
		flush := opusEncodeResult(1, 4096)
		if err := encoder.FlushInto(ctx, &flush); err != nil {
			t.Fatal(err)
		}
		if len(flush.Packets) == 0 {
			break
		}
		totalFlushed += len(flush.Packets)
	}
	if totalFlushed == 0 || totalFlushed >= 10 {
		t.Fatalf("totalFlushed = %d, want finite positive count", totalFlushed)
	}
	if encoder.state != encoderFlushed {
		t.Fatalf("state = %d, want encoderFlushed", encoder.state)
	}
}

func TestEncoderUnopenedLifecycleGuards(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}

	// FlushInto on unopened encoder must not panic or loop infinitely.
	flush := opusEncodeResult(2, 4096)
	if err := encoder.FlushInto(ctx, &flush); err != nil {
		t.Fatalf("unopened flush err = %v, want nil", err)
	}
	if len(flush.Packets) != 0 {
		t.Fatalf("unopened flush packets = %d, want 0", len(flush.Packets))
	}

	// HandleEvent on unopened encoder must not nil-deref e.encoder.
	for _, ev := range []av.Event{
		{Type: av.EventBitrateChanged, Metadata: codec.BitrateMetadata(64_000)},
		{Type: av.EventDiscontinuity},
		{Type: av.EventCodecChanged},
	} {
		if err := encoder.HandleEvent(ctx, &ev); !errors.Is(err, codec.ErrUnsupportedFormat) {
			t.Fatalf("unopened HandleEvent(%v) = %v, want ErrUnsupportedFormat", ev.Type, err)
		}
	}

	// FlushInto after discontinuity before any new frame emits nothing.
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	frame := opusTestFrame()
	res := opusEncodeResult(2, 4096)
	if err := encoder.EncodeInto(ctx, &frame, &res); err != nil {
		t.Fatal(err)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatal(err)
	}
	flush.Reset()
	if err := encoder.FlushInto(ctx, &flush); err != nil {
		t.Fatalf("flush after discontinuity err = %v", err)
	}
	if len(flush.Packets) != 0 {
		t.Fatalf("flush after discontinuity packets = %d, want 0", len(flush.Packets))
	}
}

func TestDecoderLifecycleGuardsAndPLCDoesNotConsumePreSkip(t *testing.T) {
	ctx := context.Background()
	unopened := &Decoder{}
	if err := unopened.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("unopened HandleEvent = %v, want ErrUnsupportedFormat", err)
	}
	if unopened.pendingPLC != 0 {
		t.Fatalf("unopened pendingPLC = %d, want 0", unopened.pendingPLC)
	}

	// Open with a 312-sample pre-skip header.
	stream := opusTestStream()
	stream.Codec.ExtraData = av.Buffer{Bytes: opusHead(1, 48000, 312)}
	dec := &Decoder{}
	if err := dec.Open(ctx, codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	if dec.skip != 312 {
		t.Fatalf("initial skip = %d, want 312", dec.skip)
	}

	// Trigger PLC before the first real packet: synthetic PLC must not eat pre-skip.
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	_, result := newTestDecoder(t)
	if err := dec.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("plc frames = %d, want 1", len(result.Frames))
	}
	if dec.skip != 312 {
		t.Fatalf("skip after PLC = %d, want 312 (preserved)", dec.skip)
	}

	// Trigger discontinuity (resync=true) followed by PLC: resync must remain pending.
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatal(err)
	}
	if !dec.resync {
		t.Fatal("expected resync=true after discontinuity")
	}
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	if err := dec.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatal(err)
	}
	if !dec.resync {
		t.Fatal("PLC cleared resync before a real packet arrived")
	}

	// Close guards HandleEvent and FlushInto without mutating state.
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("closed HandleEvent = %v, want ErrClosed", err)
	}
	if dec.pendingPLC != 0 {
		t.Fatalf("closed pendingPLC = %d, want 0", dec.pendingPLC)
	}
	if err := dec.FlushInto(ctx, nil); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("closed FlushInto = %v, want ErrClosed", err)
	}
}
