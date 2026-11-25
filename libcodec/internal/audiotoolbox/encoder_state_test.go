package audiotoolbox

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

func newFakeEncoder(t *testing.T) (*encoder, *int, *int, *int) {
	t.Helper()
	var setCalls, fillCalls, disposeCalls int
	sym := &symbols{
		setProperty: func(_ uintptr, _ uint32, _ uint32, _ unsafe.Pointer) int32 {
			setCalls++
			return 0
		},
		fill: func(_ uintptr, _ uintptr, user uintptr, packets *uint32, out *bufferList, _ *packetDesc) int32 {
			fillCalls++
			src, ok := sessions.Get(user)
			if !ok {
				*packets = 0
				return noData
			}
			inPackets := uint32(frames)
			var inBuf bufferList
			s := src.supply(&inPackets, &inBuf, nil)
			if s == noData || inPackets == 0 {
				*packets = 0
				return s
			}
			*packets = 1
			out.DataByteSize = 4
			return 0
		},
		dispose: func(_ uintptr) int32 {
			disposeCalls++
			return 0
		},
	}
	e := &encoder{
		sym:      sym,
		conv:     1,
		channels: 1,
		priming:  1024,
		out:      make([]byte, 64),
		feed:     make([]int16, frames),
		state:    atEncOpen,
	}
	e.handle = sessions.New(e)
	t.Cleanup(func() { _ = e.Close() })
	return e, &setCalls, &fillCalls, &disposeCalls
}

func TestEncoderStateMachineTransitions(t *testing.T) {
	e, setCalls, fillCalls, disposeCalls := newFakeEncoder(t)

	// Open state allows Update.
	vals := codec.KnobValues{}.With("bitrate", "128000")
	if err := e.Update("bitrate", vals); err != nil {
		t.Fatalf("Update in atEncOpen = %v", err)
	}
	if *setCalls != 1 || e.state != atEncOpen {
		t.Fatalf("after Update: setCalls=%d state=%v", *setCalls, e.state)
	}

	// First Encode transitions atEncOpen -> atEncStreaming and emits a packet.
	var emitted []*kit.Packet
	emit := func(p *kit.Packet) error {
		emitted = append(emitted, &kit.Packet{PTS: p.PTS, Duration: p.Duration})
		return nil
	}
	pcm := make([]int16, frames+10) // 1 full frame + 10 trailing samples
	if err := e.Encode(&kit.Samples{PCM: pcm, SampleRate: 48000, Channels: 1, PTS: 2048}, emit); err != nil {
		t.Fatalf("Encode = %v", err)
	}
	if e.state != atEncStreaming || len(emitted) != 1 || emitted[0].PTS != 1024 {
		t.Fatalf("after Encode: state=%v emitted=%+v", e.state, emitted)
	}

	// Update allowed while streaming.
	if err := e.Update("bitrate", vals); err != nil {
		t.Fatalf("Update in atEncStreaming = %v", err)
	}
	if *setCalls != 2 {
		t.Fatalf("setCalls = %d, want 2", *setCalls)
	}

	// Flush transitions to atEncDraining and drains trailing samples.
	if err := e.Flush(emit); err != nil {
		t.Fatalf("Flush = %v", err)
	}
	if e.state != atEncDraining || len(emitted) != 2 || len(e.fifo) != 0 {
		t.Fatalf("after Flush: state=%v emitted=%d fifo=%d", e.state, len(emitted), len(e.fifo))
	}

	// Second Flush in atEncDraining is idempotent.
	if err := e.Flush(emit); err != nil {
		t.Fatalf("second Flush = %v", err)
	}

	// Encode and Update after Flush must return ErrClosed.
	if err := e.Encode(&kit.Samples{PCM: make([]int16, frames), PTS: 4096}, emit); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Encode after Flush = %v, want ErrClosed", err)
	}
	if err := e.Update("bitrate", vals); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Update after Flush = %v, want ErrClosed", err)
	}

	// Close disposes once and transitions to atEncClosed.
	if err := e.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if e.state != atEncClosed || e.conv != 0 || *disposeCalls != 1 {
		t.Fatalf("after Close: state=%v conv=%d disposeCalls=%d", e.state, e.conv, *disposeCalls)
	}

	// Operations after Close must return ErrClosed without calling C symbols.
	prevSet, prevFill := *setCalls, *fillCalls
	if err := e.Encode(&kit.Samples{PCM: make([]int16, frames)}, emit); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Encode after Close = %v, want ErrClosed", err)
	}
	if err := e.Flush(emit); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Flush after Close = %v, want ErrClosed", err)
	}
	if err := e.Update("bitrate", vals); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Update after Close = %v, want ErrClosed", err)
	}
	if *setCalls != prevSet || *fillCalls != prevFill {
		t.Fatalf("closed encoder invoked C symbols: setCalls %d->%d, fillCalls %d->%d", prevSet, *setCalls, prevFill, *fillCalls)
	}
}

func TestEncoderFlushBeforeEncode(t *testing.T) {
	e, _, fillCalls, _ := newFakeEncoder(t)
	if err := e.Flush(func(*kit.Packet) error { t.Fatal("unexpected packet"); return nil }); err != nil {
		t.Fatalf("Flush in atEncOpen = %v", err)
	}
	if e.state != atEncDraining || *fillCalls != 0 {
		t.Fatalf("after Flush in atEncOpen: state=%v fillCalls=%d", e.state, *fillCalls)
	}
	if err := e.Encode(&kit.Samples{PCM: make([]int16, frames)}, func(*kit.Packet) error { return nil }); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("Encode after empty Flush = %v, want ErrClosed", err)
	}
}
