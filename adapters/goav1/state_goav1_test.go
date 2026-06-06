//go:build goav_goav1

package goav1

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

func TestDecoderFrameFormatFromBounds(t *testing.T) {
	format, err := DecoderFrameFormatFromBounds(codec.DecodeBounds{
		MaxWidth:  64,
		MaxHeight: 48,
	})
	if err != nil {
		t.Fatal(err)
	}
	if format.Width != 64 || format.Height != 48 || format.BitDepth != 8 ||
		!format.SubsamplingX || !format.SubsamplingY || format.Align != defaultAlign {
		t.Fatalf("format = %+v", format)
	}
	layout, err := backend.FrameRequiredSize(format)
	if err != nil {
		t.Fatal(err)
	}
	if layout.Size == 0 {
		t.Fatalf("layout = %+v", layout)
	}
}

func TestDecoderScratchSizeFromBounds(t *testing.T) {
	size := DecoderScratchSizeFromBounds(codec.DecodeBounds{
		MaxFramesPerInput: 2,
		MaxEventsPerInput: 3,
		MaxPayloadBytes:   100,
		MaxRetainedBytes:  200,
	})
	if size.Events != 3 || size.RTPSpans != 3 || size.RTPBuffer != 300 ||
		size.Event.Outputs != 2 {
		t.Fatalf("size = %+v", size)
	}
}

func TestNewDecoderStateBindsRuntime(t *testing.T) {
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput: 2,
			MaxEventsPerInput: 3,
			MaxPayloadBytes:   100,
			MaxRetainedBytes:  200,
			MaxWidth:          64,
			MaxHeight:         64,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.Stream() == nil || state.BatchRunner() == nil {
		t.Fatal("state did not expose stream and batch runner")
	}

	scratch := state.Scratch()
	size := state.ScratchSize()
	if err := scratch.Check(size); err != nil {
		t.Fatal(err)
	}
	if len(scratch.Events) != 3 || len(scratch.RTPBuffer) != 300 ||
		len(scratch.RTPSpans) != 3 || len(scratch.Outputs) != 2 {
		t.Fatalf("scratch = %+v", scratch)
	}

	runtime := state.Runtime()
	if runtime.State == nil || runtime.Refs == nil || runtime.FramePool == nil ||
		runtime.Align != defaultAlign || runtime.SideData == nil || runtime.Stats == nil {
		t.Fatalf("runtime = %+v", runtime)
	}
	if len(runtime.ReferenceSurfaces) != backend.InterRefsPerFrame ||
		len(runtime.ReferenceFrames) != backend.InterRefsPerFrame ||
		len(runtime.Releases) != backend.RefFrames ||
		len(runtime.Outputs) != 2 {
		t.Fatalf("runtime slices = %+v", runtime)
	}

	if runtime.FramePool.Available() != defaultSurfaceCount {
		t.Fatalf("available = %d", runtime.FramePool.Available())
	}
	if _, frame, err := runtime.FramePool.Acquire(); err != nil || frame == nil {
		t.Fatalf("acquire frame=%p err=%v", frame, err)
	}
	state.Reset()
	if runtime := state.Runtime(); runtime.FramePool.Available() != defaultSurfaceCount {
		t.Fatalf("available after reset = %d", runtime.FramePool.Available())
	}
}

func TestNewDecoderStateKeepsExplicitScratchMax(t *testing.T) {
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput: 1,
			MaxEventsPerInput: 1,
			MaxPayloadBytes:   10,
			MaxRetainedBytes:  10,
			MaxWidth:          64,
			MaxHeight:         64,
		},
		Scratch: backend.DecoderFrameWorkResidualStreamScratchSize{
			Events:    8,
			RTPBuffer: 128,
			RTPSpans:  8,
			Event: backend.DecoderFrameWorkResidualEventScratchSize{
				Outputs: 4,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	size := state.ScratchSize()
	if size.Events != 8 || size.RTPBuffer != 128 || size.RTPSpans != 8 ||
		size.Event.Outputs != 4 {
		t.Fatalf("size = %+v", size)
	}
}

func TestNewDecoderStateRejectsMissingGeometry(t *testing.T) {
	_, err := NewDecoderState(DecoderStateConfig{})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}
