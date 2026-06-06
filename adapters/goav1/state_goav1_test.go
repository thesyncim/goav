//go:build goav_goav1

package goav1

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
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

func TestDecoderFrameFormatFromStreamPixelFormats(t *testing.T) {
	tests := []struct {
		name         string
		pixelFormat  string
		mono         bool
		subsamplingX bool
		subsamplingY bool
	}{
		{name: "default", subsamplingX: true, subsamplingY: true},
		{name: "i420", pixelFormat: av.PixelFormatI420, subsamplingX: true, subsamplingY: true},
		{name: "yuv420p", pixelFormat: av.PixelFormatYUV420P, subsamplingX: true, subsamplingY: true},
		{name: "i422", pixelFormat: av.PixelFormatI422, subsamplingX: true},
		{name: "yuv422p", pixelFormat: av.PixelFormatYUV422P, subsamplingX: true},
		{name: "i444", pixelFormat: av.PixelFormatI444},
		{name: "yuv444p", pixelFormat: av.PixelFormatYUV444P},
		{name: "gray8", pixelFormat: av.PixelFormatGray8, mono: true, subsamplingX: true, subsamplingY: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			format, err := DecoderFrameFormatFromStream(av.Stream{
				Codec: av.CodecParameters{
					Width:       64,
					Height:      48,
					PixelFormat: test.pixelFormat,
				},
			}, codec.DecodeBounds{})
			if err != nil {
				t.Fatal(err)
			}
			if format.Width != 64 || format.Height != 48 || format.BitDepth != 8 ||
				format.MonoChrome != test.mono ||
				format.SubsamplingX != test.subsamplingX ||
				format.SubsamplingY != test.subsamplingY {
				t.Fatalf("format = %+v", format)
			}
			if _, err := backend.FrameRequiredSize(format); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDecoderFrameFormatFromStreamRejectsUnsupportedPixelFormat(t *testing.T) {
	_, err := DecoderFrameFormatFromStream(av.Stream{
		Codec: av.CodecParameters{
			Width:       64,
			Height:      48,
			PixelFormat: "nv12",
		},
	}, codec.DecodeBounds{})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
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

func TestDecoderStatePlansBindsAndRunsRTPPayloads(t *testing.T) {
	payloads := testFragmentedRTPPayloads()
	probe := testRTPPayloadsPlan(t, payloads)
	workerPool, err := backend.NewTileWorkerPool(1)
	if err != nil {
		t.Fatal(err)
	}
	defer workerPool.Close()

	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput: 1,
			MaxEventsPerInput: probe.Size.Events,
			MaxPayloadBytes:   128,
			MaxRetainedBytes:  128,
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

	runner, err := state.BindRunner(probe)
	if err != nil {
		t.Fatal(err)
	}
	var result backend.DecoderFrameWorkResidualStreamResult
	for i := range payloads {
		plan, err := state.PlanRTPPayload(payloads[i])
		if err != nil {
			t.Fatalf("plan payload %d: %v", i, err)
		}
		if !decoderStreamScratchFits(probe.Size, plan.Size) {
			t.Fatalf("payload %d plan does not fit probe: plan=%+v probe=%+v", i, plan.Size, probe.Size)
		}
		if err := runner.RunRTPPayloadInto(&result, payloads[i], nil); err != nil {
			t.Fatalf("run payload %d: %v", i, err)
		}
	}
	if result.Run.CompletedFrames != 1 ||
		result.Run.OutputCount != 1 ||
		result.Run.Outputs[0] == nil ||
		state.runner.RTPUsed != 0 {
		t.Fatalf("result = %+v retained=%d", result, state.runner.RTPUsed)
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

func testRTPPayloadsPlan(t *testing.T, payloads [][]byte) backend.DecoderFrameWorkResidualStreamPlan {
	t.Helper()
	var stream backend.DecoderStream
	events := make([]backend.DecoderEvent, 4)
	rtpBuffer := make([]byte, 256)
	rtpSpans := make([]backend.RTPObuSpan, 4)
	spans := make([]backend.TileSpan, 1)
	jobs := make([]backend.TileJob, 1)
	batches := make([]backend.TileBatch, 1)
	plan, err := backend.DecoderFrameWorkResidualRTPPayloadsStreamPlan(
		stream,
		0,
		payloads,
		1,
		rtpBuffer,
		rtpSpans,
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

func testRTPPayload() []byte {
	elements := [...]backend.RTPElement{
		{Data: testRTPElement(backend.OBUSequenceHeader, testSequenceHeaderPayload())},
		{Data: testRTPElement(backend.OBUFrameHeader, testFrameHeaderPayload())},
		{Data: testRTPElement(backend.OBUTileGroup, []byte{0x80})},
	}
	payload := make([]byte, 128)
	n, err := backend.PutRTPPayload(payload, backend.RTPAggregationHeader{
		ElementCount:                uint8(len(elements)),
		StartsNewCodedVideoSequence: true,
	}, elements[:])
	if err != nil {
		panic(err)
	}
	return payload[:n]
}

func testRTPFramePayload() []byte {
	elements := [...]backend.RTPElement{
		{Data: testRTPElement(backend.OBUFrameHeader, testFrameHeaderPayload())},
		{Data: testRTPElement(backend.OBUTileGroup, []byte{0x80})},
	}
	payload := make([]byte, 128)
	n, err := backend.PutRTPPayload(payload, backend.RTPAggregationHeader{
		ElementCount: uint8(len(elements)),
	}, elements[:])
	if err != nil {
		panic(err)
	}
	return payload[:n]
}

func testFragmentedRTPPayloads() [][]byte {
	payloads := make([][]byte, 0, 4)

	sequence := [...]backend.RTPElement{
		{Data: testRTPElement(backend.OBUSequenceHeader, testSequenceHeaderPayload())},
	}
	var packet [128]byte
	n, err := backend.PutRTPPayload(packet[:], backend.RTPAggregationHeader{
		ElementCount:                1,
		StartsNewCodedVideoSequence: true,
	}, sequence[:])
	if err != nil {
		panic(err)
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))

	frameHeader := testRTPElement(backend.OBUFrameHeader, testFrameHeaderPayload())
	n, next, more, err := backend.PutRTPFragment(packet[:], frameHeader, 0, 2, false)
	if err != nil {
		panic(err)
	}
	if !more {
		panic("expected frame header RTP fragment")
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))
	n, _, more, err = backend.PutRTPFragment(packet[:], frameHeader, next, len(packet), false)
	if err != nil {
		panic(err)
	}
	if more {
		panic("unexpected third frame header RTP fragment")
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))

	tile := [...]backend.RTPElement{
		{Data: testRTPElement(backend.OBUTileGroup, []byte{0x80})},
	}
	n, err = backend.PutRTPPayload(packet[:], backend.RTPAggregationHeader{ElementCount: 1}, tile[:])
	if err != nil {
		panic(err)
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))
	return payloads
}

func testRTPElement(typ backend.OBUType, payload []byte) []byte {
	var header [2]byte
	n, err := backend.PutOBUHeader(header[:], backend.OBUHeader{Type: typ})
	if err != nil {
		panic(err)
	}
	element := make([]byte, 0, n+len(payload))
	element = append(element, header[:n]...)
	element = append(element, payload...)
	return element
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
