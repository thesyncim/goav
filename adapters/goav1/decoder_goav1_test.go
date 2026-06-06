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
	workerPool, err := backend.NewTileWorkerPool(1)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput:   1,
			MaxEventsPerInput:   probe.Size.Events,
			MaxRequestsPerInput: 1,
			MaxPayloadBytes:     len(payload),
			MaxRetainedBytes:    0,
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
		Scratch:    probe.Size,
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
