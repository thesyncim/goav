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

func TestDecoderStatePlansBindsAndRunsLowOverhead(t *testing.T) {
	payload := testLowOverheadStream()
	probe := testLowOverheadPlan(t, payload)
	workerPool, err := backend.NewTileWorkerPool(1)
	if err != nil {
		t.Fatal(err)
	}
	defer workerPool.Close()

	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput: 1,
			MaxEventsPerInput: probe.Size.Events,
			MaxPayloadBytes:   len(payload),
			MaxRetainedBytes:  0,
			MaxWidth:          16,
			MaxHeight:         16,
		},
		Format: backend.FrameFormat{
			Width:        16,
			Height:       16,
			BitDepth:     8,
			MonoChrome:   true,
			SubsamplingX: true,
			SubsamplingY: true,
			Align:        defaultAlign,
		},
		Scratch:    probe.Size,
		WorkerPool: workerPool,
	})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := state.PlanLowOverhead(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.HasEvent() || plan.Size.Events != 3 || plan.Size.Event.Outputs != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	if _, ok := state.Runner(); ok {
		t.Fatal("runner should not be bound before BindRunner")
	}

	runner, err := state.BindRunner(plan)
	if err != nil {
		t.Fatal(err)
	}
	if current, ok := state.Runner(); !ok || current != runner {
		t.Fatalf("bound runner current=%p ok=%v want=%p", current, ok, runner)
	}
	runnerState := runner.State()
	if !runnerState.Bound ||
		runnerState.EventCapacity != plan.Size.Events ||
		runnerState.OutputCapacity != plan.Size.Event.Outputs {
		t.Fatalf("runner state = %+v", runnerState)
	}

	var result backend.DecoderFrameWorkResidualStreamResult
	if err := runner.RunLowOverheadInto(&result, payload, nil); err != nil {
		t.Fatal(err)
	}
	if result.EventCount != 3 ||
		result.Run.CompletedFrames != 1 ||
		result.Run.OutputCount != 1 ||
		len(result.Run.Outputs) != 1 ||
		result.Run.Outputs[0] == nil ||
		result.Run.Outputs[0] != result.Run.Last.Output {
		t.Fatalf("result = %+v", result)
	}
	if state.stats.TXBs == 0 || state.stats.Residuals == 0 {
		t.Fatalf("stats = %+v", state.stats)
	}

	state.Reset()
	if _, ok := state.Runner(); ok {
		t.Fatal("runner should be cleared after Reset")
	}
}

func TestNewDecoderStateRejectsMissingGeometry(t *testing.T) {
	_, err := NewDecoderState(DecoderStateConfig{})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func testLowOverheadPlan(t *testing.T, payload []byte) backend.DecoderFrameWorkResidualStreamPlan {
	t.Helper()
	var stream backend.DecoderStream
	events := make([]backend.DecoderEvent, 4)
	spans := make([]backend.TileSpan, 1)
	jobs := make([]backend.TileJob, 1)
	batches := make([]backend.TileBatch, 1)
	plan, err := backend.DecoderFrameWorkResidualLowOverheadStreamPlan(
		stream,
		payload,
		1,
		events,
		spans,
		jobs,
		batches,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testLowOverheadStream() []byte {
	var stream []byte
	stream = appendTestLowOverheadOBU(stream, backend.OBUSequenceHeader, testSequenceHeaderPayload())
	stream = appendTestLowOverheadOBU(stream, backend.OBUFrameHeader, testFrameHeaderPayload())
	stream = appendTestLowOverheadOBU(stream, backend.OBUTileGroup, []byte{0x80})
	return stream
}

func appendTestLowOverheadOBU(dst []byte, typ backend.OBUType, payload []byte) []byte {
	if len(payload) > 0x7f {
		panic("test payload too large")
	}
	var header [2]byte
	n, err := backend.PutOBUHeader(header[:], backend.OBUHeader{Type: typ, HasSizeField: true})
	if err != nil {
		panic(err)
	}
	dst = append(dst, header[:n]...)
	dst = append(dst, byte(len(payload)))
	dst = append(dst, payload...)
	return dst
}

func testSequenceHeaderPayload() []byte {
	var w testBitWriter
	w.writeBits(0, 3)  // seq_profile
	w.writeBool(true)  // still_picture
	w.writeBool(true)  // reduced_still_picture_header
	w.writeBits(5, 5)  // seq_level_idx[0]
	w.writeBits(7, 4)  // frame_width_bits_minus_1
	w.writeBits(7, 4)  // frame_height_bits_minus_1
	w.writeBits(15, 8) // max_frame_width_minus_1
	w.writeBits(15, 8) // max_frame_height_minus_1
	w.writeBool(false) // use_128x128_superblock
	w.writeBool(true)  // enable_filter_intra
	w.writeBool(true)  // enable_intra_edge_filter
	w.writeBool(false) // enable_superres
	w.writeBool(true)  // enable_cdef
	w.writeBool(false) // enable_restoration
	w.writeBool(false) // high_bitdepth
	w.writeBool(true)  // mono_chrome
	w.writeBool(false) // color_description_present_flag
	w.writeBool(false) // color_range
	w.writeBool(false) // film_grain_params_present
	return w.trailingBits()
}

func testFrameHeaderPayload() []byte {
	var w testBitWriter
	w.writeBool(true)  // disable_cdf_update
	w.writeBool(false) // render_and_frame_size_different
	w.writeBool(false) // uniform_tile_spacing_flag
	w.writeBits(0, 8)  // base_q_idx
	w.writeBool(false) // y_dc_delta_q
	w.writeBool(false) // using_qmatrix
	w.writeBool(false) // segmentation_enabled
	w.writeBool(false) // reduced_tx_set
	return w.bytes()
}

type testBitWriter struct {
	buf [128]byte
	bit int
}

func (w *testBitWriter) writeBits(value uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		if (value>>uint(i))&1 != 0 {
			w.buf[w.bit>>3] |= 1 << uint(7-(w.bit&7))
		}
		w.bit++
	}
}

func (w *testBitWriter) writeBool(value bool) {
	if value {
		w.writeBits(1, 1)
		return
	}
	w.writeBits(0, 1)
}

func (w *testBitWriter) bytes() []byte {
	return w.buf[:(w.bit+7)>>3]
}

func (w *testBitWriter) trailingBits() []byte {
	w.writeBits(1, 1)
	for w.bit&7 != 0 {
		w.writeBits(0, 1)
	}
	return w.buf[:w.bit>>3]
}
