//go:build goav_goav1

package goav1

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

func TestDecoderDecodesLowOverheadIntoBorrowedFrame(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(1, 1)
	pkt := &av.Packet{
		StreamID:   "video",
		CodecEpoch: 7,
		Payload:    av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		PTS:        av.RTPToTimestamp(9000, 90000),
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Keyframe:   true,
		Metadata:   av.Metadata{"rid": "q"},
	}
	if err := decoder.DecodeInto(context.Background(), pkt, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := &result.Frames[0]
	if frame.StreamID != "video" || frame.CodecEpoch != 7 || frame.Type != av.MediaVideo {
		t.Fatalf("frame identity = %+v", frame)
	}
	if frame.Video == nil || frame.Video.Width != 16 || frame.Video.Height != 16 ||
		frame.Video.PixelFormat != av.PixelFormatGray8 {
		t.Fatalf("video = %+v", frame.Video)
	}
	if len(frame.Planes) != 1 ||
		frame.Planes[0].Buffer.Ownership != av.BufferBorrowed ||
		len(frame.Planes[0].Buffer.Bytes) == 0 ||
		frame.Planes[0].Stride == 0 {
		t.Fatalf("planes = %+v", frame.Planes)
	}
	if frame.PTS != pkt.PTS || frame.Duration != pkt.Duration || frame.Metadata["rid"] != "q" {
		t.Fatalf("timestamps/metadata = %+v", frame)
	}
}

func TestDecoderDecodesRTPPayloadIntoBorrowedFrame(t *testing.T) {
	payload := testRTPPayload()
	decoder, workerPool := newTestRTPDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(1, 1)
	pkt := &av.Packet{
		StreamID:   "video",
		CodecEpoch: 7,
		Payload:    av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		PTS:        av.RTPToTimestamp(9000, 90000),
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Keyframe:   true,
		Metadata:   av.Metadata{"rid": "q"},
	}
	if err := decoder.DecodeRTPPayloadInto(context.Background(), pkt, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := &result.Frames[0]
	if frame.StreamID != "video" || frame.CodecEpoch != 7 || frame.Type != av.MediaVideo {
		t.Fatalf("frame identity = %+v", frame)
	}
	if frame.Video == nil || frame.Video.Width != 16 || frame.Video.Height != 16 ||
		frame.Video.PixelFormat != av.PixelFormatGray8 {
		t.Fatalf("video = %+v", frame.Video)
	}
	if len(frame.Planes) != 1 ||
		frame.Planes[0].Buffer.Ownership != av.BufferBorrowed ||
		len(frame.Planes[0].Buffer.Bytes) == 0 ||
		frame.Planes[0].Stride == 0 {
		t.Fatalf("planes = %+v", frame.Planes)
	}
	if frame.PTS != pkt.PTS || frame.Duration != pkt.Duration || frame.Metadata["rid"] != "q" {
		t.Fatalf("timestamps/metadata = %+v", frame)
	}
}

func TestDecoderPixelFormatMapping(t *testing.T) {
	tests := []struct {
		name        string
		format      backend.FrameFormat
		pixelFormat string
		planeCount  int
		wantErr     bool
	}{
		{
			name:        "gray8",
			format:      backend.FrameFormat{Width: 16, Height: 16, BitDepth: 8, MonoChrome: true},
			pixelFormat: av.PixelFormatGray8,
			planeCount:  1,
		},
		{
			name:        "i420",
			format:      backend.FrameFormat{Width: 16, Height: 16, BitDepth: 8, SubsamplingX: true, SubsamplingY: true},
			pixelFormat: av.PixelFormatI420,
			planeCount:  3,
		},
		{
			name:        "i422",
			format:      backend.FrameFormat{Width: 16, Height: 16, BitDepth: 8, SubsamplingX: true},
			pixelFormat: av.PixelFormatI422,
			planeCount:  3,
		},
		{
			name:        "i444",
			format:      backend.FrameFormat{Width: 16, Height: 16, BitDepth: 8},
			pixelFormat: av.PixelFormatI444,
			planeCount:  3,
		},
		{
			name:    "unsupported",
			format:  backend.FrameFormat{Width: 16, Height: 16, BitDepth: 8, SubsamplingY: true},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pixelFormat, planeCount, err := backendFramePixelFormat(test.format)
			if test.wantErr {
				if !errors.Is(err, codec.ErrUnsupportedFormat) {
					t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if pixelFormat != test.pixelFormat || planeCount != test.planeCount {
				t.Fatalf("pixel=%s planes=%d, want %s/%d", pixelFormat, planeCount, test.pixelFormat, test.planeCount)
			}
		})
	}
}

func TestDecoderPixelFormatAliases(t *testing.T) {
	tests := map[string]string{
		av.PixelFormatYUV420P: av.PixelFormatI420,
		av.PixelFormatYUV422P: av.PixelFormatI422,
		av.PixelFormatYUV444P: av.PixelFormatI444,
		av.PixelFormatGray8:   av.PixelFormatGray8,
		"":                    "",
	}
	for in, want := range tests {
		if got := normalizeDecoderPixelFormat(in); got != want {
			t.Fatalf("normalize %q = %q, want %q", in, got, want)
		}
	}
}

func TestDecoderFillDecodedFramePlanarFormats(t *testing.T) {
	tests := []struct {
		name        string
		format      backend.FrameFormat
		pixelFormat string
	}{
		{
			name:        "i422",
			format:      backend.FrameFormat{Width: 16, Height: 8, BitDepth: 8, SubsamplingX: true},
			pixelFormat: av.PixelFormatI422,
		},
		{
			name:        "i444",
			format:      backend.FrameFormat{Width: 16, Height: 8, BitDepth: 8},
			pixelFormat: av.PixelFormatI444,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := &Decoder{stream: av.Stream{ID: "video", Epoch: 4}}
			src := backend.Frame{Format: test.format}
			src.Y.Pix = make([]byte, 128)
			src.Y.Stride = 16
			src.Y.Width = 16
			src.Y.Height = 8
			src.U.Pix = make([]byte, 64)
			src.U.Stride = 8
			src.V.Pix = make([]byte, 64)
			src.V.Stride = 8

			frame := av.Frame{Planes: make([]av.Plane, 0, 3)}
			err := decoder.fillDecodedFrame(&av.Packet{
				StreamID:   "packet-video",
				CodecEpoch: 9,
			}, &src, &frame)
			if err != nil {
				t.Fatal(err)
			}
			if frame.StreamID != "packet-video" || frame.CodecEpoch != 9 || frame.Type != av.MediaVideo {
				t.Fatalf("frame identity = %+v", frame)
			}
			if frame.Video == nil || frame.Video.Width != 16 || frame.Video.Height != 8 ||
				frame.Video.PixelFormat != test.pixelFormat {
				t.Fatalf("video = %+v", frame.Video)
			}
			if decoder.stream.Codec.PixelFormat != test.pixelFormat {
				t.Fatalf("stream codec = %+v", decoder.stream.Codec)
			}
			if len(frame.Planes) != 3 {
				t.Fatalf("planes = %d, want 3", len(frame.Planes))
			}
			for i := range frame.Planes {
				if frame.Planes[i].Buffer.Ownership != av.BufferBorrowed ||
					len(frame.Planes[i].Buffer.Bytes) == 0 ||
					frame.Planes[i].Stride == 0 {
					t.Fatalf("plane %d = %+v", i, frame.Planes[i])
				}
			}
		})
	}
}

func TestDecoderRTPPayloadAfterLossPreservesSequenceState(t *testing.T) {
	key := testRTPPayload()
	delta := testRTPFramePayload()
	decoder, workerPool := newTestRTPDecoder(t, key, delta)
	defer workerPool.Close()

	result := testDecodeResult(2, 1)
	if err := decoder.DecodeRTPPayloadInto(context.Background(), &av.Packet{
		StreamID: "video",
		Payload:  av.Buffer{Bytes: key, Ownership: av.BufferImmutable},
		Keyframe: true,
	}, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("initial frames = %d, want 1", len(result.Frames))
	}

	result.Reset()
	if err := decoder.DecodeRTPPayloadInto(context.Background(), &av.Packet{
		StreamID:   "video",
		Payload:    av.Buffer{Bytes: delta, Ownership: av.BufferImmutable},
		Keyframe:   true,
		LossBefore: true,
	}, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 || len(result.Requests) != 0 {
		t.Fatalf("after-loss result = %+v", result)
	}
}

func TestDecoderRTPPayloadRetainsFragments(t *testing.T) {
	payloads := testFragmentedRTPPayloads()
	decoder, workerPool := newTestRTPDecoder(t, payloads...)
	defer workerPool.Close()

	result := testDecodeResult(1, 1)
	for i := range payloads {
		err := decoder.DecodeRTPPayloadInto(context.Background(), &av.Packet{
			StreamID: "video",
			Payload:  av.Buffer{Bytes: payloads[i], Ownership: av.BufferImmutable},
			Keyframe: true,
		}, &result)
		if err != nil {
			t.Fatalf("payload %d: %v", i, err)
		}
	}
	if len(result.Frames) != 1 || len(result.Requests) != 0 {
		t.Fatalf("fragmented result = %+v", result)
	}
}

func TestDecoderRequestsKeyframeAfterLoss(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(0, 1)
	if err := decoder.DecodeInto(context.Background(), nil, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 1 ||
		result.Requests[0].Type != codec.ControlRequestKeyframe ||
		result.Requests[0].StreamID != "video" {
		t.Fatalf("requests = %+v", result.Requests)
	}
}

func TestDecoderDropsUntilSyncAfterLoss(t *testing.T) {
	payload := testLowOverheadStream()
	unsyncable := testUnsyncableLowOverheadPayload()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(1, 1)
	if err := decoder.DecodeInto(context.Background(), nil, &result); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID: "video",
		Payload:  av.Buffer{Bytes: unsyncable, Ownership: av.BufferImmutable},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 0 ||
		len(result.Requests) != 1 ||
		result.Requests[0].Type != codec.ControlRequestKeyframe ||
		result.Requests[0].Reason != "av1 waiting for keyframe" {
		t.Fatalf("result = %+v", result)
	}

	result.Reset()
	err = decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID:   "video",
		Payload:    av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		Keyframe:   true,
		LossBefore: true,
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 || len(result.Requests) != 0 {
		t.Fatalf("result after keyframe = %+v", result)
	}

	result.Reset()
	err = decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID: "video",
		Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 0 {
		t.Fatalf("requests after recovery = %+v", result.Requests)
	}
}

func TestDecoderDetectsSyncFromLowOverheadPayload(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(1, 1)
	if err := decoder.DecodeInto(context.Background(), nil, &result); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID:   "video",
		Payload:    av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		LossBefore: true,
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 || len(result.Requests) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDecoderCodecChangedDropsUntilSync(t *testing.T) {
	payload := testLowOverheadStream()
	unsyncable := testUnsyncableLowOverheadPayload()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	updated := av.Stream{
		ID:    "video2",
		Epoch: 9,
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatGray8,
		},
	}
	if err := decoder.HandleEvent(context.Background(), &av.Event{
		Type:   av.EventCodecChanged,
		Stream: &updated,
	}); err != nil {
		t.Fatal(err)
	}

	result := testDecodeResult(1, 1)
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		Payload: av.Buffer{Bytes: unsyncable, Ownership: av.BufferImmutable},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 0 ||
		len(result.Requests) != 1 ||
		result.Requests[0].StreamID != "video2" {
		t.Fatalf("result = %+v", result)
	}

	result.Reset()
	err = decoder.DecodeInto(context.Background(), &av.Packet{
		Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 ||
		result.Frames[0].StreamID != "video2" ||
		result.Frames[0].CodecEpoch != 9 {
		t.Fatalf("frames = %+v", result.Frames)
	}
}

func TestDecoderDecodeIntoAllocs(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	ctx := context.Background()
	result := testDecodeResult(1, 0)
	pkt := &av.Packet{
		Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		Keyframe: true,
	}
	decoder.resetAfterLoss()
	if err := decoder.DecodeInto(ctx, pkt, &result); err != nil {
		t.Fatal(err)
	}

	decoder.resetAfterLoss()
	result.Reset()
	if err := decoder.DecodeInto(ctx, pkt, &result); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		result.Reset()
		if err := decoder.DecodeInto(ctx, pkt, &result); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func TestDecoderKeyframeRequestAllocs(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	ctx := context.Background()
	result := testDecodeResult(0, 1)
	if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		result.Reset()
		if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func TestDecoderResultFullBeforeRun(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	result := testDecodeResult(0, 0)
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		Keyframe: true,
	}, &result)
	if !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
	if len(result.Frames) != 0 {
		t.Fatalf("frames = %d, want 0", len(result.Frames))
	}
}

func TestDecoderCloseLifecycle(t *testing.T) {
	payload := testLowOverheadStream()
	decoder, workerPool := newTestDecoder(t, payload)
	defer workerPool.Close()

	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	result := testDecodeResult(1, 0)
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
	}, &result)
	if !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func newTestDecoder(t *testing.T, payload []byte) (*Decoder, *backend.TileWorkerPool) {
	t.Helper()
	probe := testLowOverheadPlan(t, payload)
	return newTestDecoderWithScratch(t, len(payload), len(payload), probe.Size)
}

func newTestRTPDecoder(t *testing.T, payloads ...[]byte) (*Decoder, *backend.TileWorkerPool) {
	t.Helper()
	probe := testRTPPayloadsPlan(t, payloads)
	maxPayload := 0
	for i := range payloads {
		if len(payloads[i]) > maxPayload {
			maxPayload = len(payloads[i])
		}
	}
	return newTestDecoderWithScratch(t, maxPayload, maxPayload, probe.Size)
}

func newTestDecoderWithScratch(t *testing.T, maxPayload int, maxRetained int, scratch backend.DecoderFrameWorkResidualStreamScratchSize) (*Decoder, *backend.TileWorkerPool) {
	t.Helper()
	workerPool, err := backend.NewTileWorkerPool(1)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput:   1,
			MaxEventsPerInput:   scratch.Events,
			MaxRequestsPerInput: 1,
			MaxPayloadBytes:     maxPayload,
			MaxRetainedBytes:    maxRetained,
			MaxWidth:            16,
			MaxHeight:           16,
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
		Scratch:    scratch,
		WorkerPool: workerPool,
	})
	if err != nil {
		workerPool.Close()
		t.Fatal(err)
	}
	factory := NewDecoderFactory()
	codecDecoder, err := factory.NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{
			ID: "video",
			Codec: av.CodecParameters{
				ID:          av.CodecAV1,
				Width:       16,
				Height:      16,
				PixelFormat: av.PixelFormatGray8,
			},
		},
		Resilience: codec.ResiliencePolicy{
			DropDamagedVideo: true,
			RequestKeyframes: true,
		},
		Bounds:      state.Bounds(),
		OpaqueState: state,
	})
	if err != nil {
		workerPool.Close()
		t.Fatal(err)
	}
	decoder, ok := codecDecoder.(*Decoder)
	if !ok {
		workerPool.Close()
		t.Fatalf("decoder type = %T", codecDecoder)
	}
	return decoder, workerPool
}

func testDecodeResult(maxFrames, maxRequests int) codec.DecodeResult {
	frames := make([]av.Frame, maxFrames)
	for i := range frames {
		frames[i].Planes = make([]av.Plane, 0, 3)
	}
	return codec.DecodeResult{
		Frames:   frames[:0],
		Requests: make([]codec.ControlRequest, 0, maxRequests),
	}
}

func testUnsyncableLowOverheadPayload() []byte {
	var payload []byte
	return appendTestLowOverheadOBU(payload, backend.OBUTileGroup, []byte{0x80})
}
