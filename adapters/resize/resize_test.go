package resize

import (
	"bytes"
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

func TestRegister(t *testing.T) {
	registry := filter.NewRegistry()
	Register(registry)

	factory, err := registry.Factory(filter.FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if factory == nil {
		t.Fatal("nil factory")
	}
	descriptors := registry.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Name != filter.FactoryResize || descriptors[0].Input != av.MediaVideo {
		t.Fatalf("descriptors = %+v", descriptors)
	}
}

func TestFilterResizesI420Exact(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Width: 2, Height: 2}, videoStream(4, 4, av.PixelFormatYUV420P))
	frame := videoFrame("video", 4, 4, av.PixelFormatYUV420P,
		[]byte{
			0, 1, 2, 3,
			4, 5, 6, 7,
			8, 9, 10, 11,
			12, 13, 14, 15,
		},
		[]byte{
			100, 101,
			102, 103,
		},
		[]byte{
			200, 201,
			202, 203,
		},
	)
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(2, 2)}[:0]}

	if err := resizer.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	output := &result.Frames[0]
	if output.Video == nil || output.Video.Width != 2 || output.Video.Height != 2 || output.Video.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("video = %+v", output.Video)
	}
	if got, want := output.Planes[0].Buffer.Bytes, []byte{0, 2, 8, 10}; !bytes.Equal(got, want) {
		t.Fatalf("y = %v, want %v", got, want)
	}
	if got, want := output.Planes[1].Buffer.Bytes, []byte{100}; !bytes.Equal(got, want) {
		t.Fatalf("u = %v, want %v", got, want)
	}
	if got, want := output.Planes[2].Buffer.Bytes, []byte{200}; !bytes.Equal(got, want) {
		t.Fatalf("v = %v, want %v", got, want)
	}
	for i := range output.Planes {
		if output.Planes[i].Buffer.Ownership != av.BufferOwned {
			t.Fatalf("plane[%d] ownership = %s", i, output.Planes[i].Buffer.Ownership)
		}
	}
}

func TestFilterPassthroughCopiesI420(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Mode: filter.ResizePassthrough}, videoStream(2, 2, av.PixelFormatI420))
	frame := videoFrame("video", 2, 2, av.PixelFormatI420, []byte{1, 2, 3, 4}, []byte{5}, []byte{6})
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(2, 2)}[:0]}

	if err := resizer.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	output := &result.Frames[0]
	if !bytes.Equal(output.Planes[0].Buffer.Bytes, []byte{1, 2, 3, 4}) ||
		!bytes.Equal(output.Planes[1].Buffer.Bytes, []byte{5}) ||
		!bytes.Equal(output.Planes[2].Buffer.Bytes, []byte{6}) {
		t.Fatalf("planes = %+v", output.Planes)
	}
	if &output.Planes[0].Buffer.Bytes[0] == &frame.Planes[0].Buffer.Bytes[0] {
		t.Fatal("passthrough borrowed source plane instead of writing caller-owned output")
	}
}

func TestFilterFitModePreservesAspect(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Width: 4, Height: 4, Mode: filter.ResizeFit}, videoStream(8, 4, av.PixelFormatYUV420P))
	frame := patternedVideoFrame("video", 8, 4, av.PixelFormatYUV420P)
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(4, 2)}[:0]}

	if err := resizer.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	output := &result.Frames[0]
	if output.Video == nil || output.Video.Width != 4 || output.Video.Height != 2 {
		t.Fatalf("video = %+v, want 4x2", output.Video)
	}
}

func TestFilterFillModeCropsAndScales(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Width: 4, Height: 4, Mode: filter.ResizeFill}, videoStream(8, 4, av.PixelFormatYUV420P))
	frame := patternedVideoFrame("video", 8, 4, av.PixelFormatYUV420P)
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(4, 4)}[:0]}

	if err := resizer.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	output := &result.Frames[0]
	if got, want := output.Planes[0].Buffer.Bytes[:4], []byte{2, 3, 4, 5}; !bytes.Equal(got, want) {
		t.Fatalf("first y row = %v, want %v", got, want)
	}
	if got, want := output.Planes[1].Buffer.Bytes, []byte{41, 42, 45, 46}; !bytes.Equal(got, want) {
		t.Fatalf("u = %v, want %v", got, want)
	}
}

func TestFilterRejectsUnsupportedPixelFormat(t *testing.T) {
	_, err := NewFactory().NewFilter(context.Background(), filter.Config{
		Stream: videoStream(4, 4, "nv12"),
		Video:  &filter.ResizeConfig{Width: 2, Height: 2},
	})
	if err != filter.ErrUnsupportedFormat {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestFilterRequiresOutputCapacity(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Width: 4, Height: 4}, videoStream(4, 4, av.PixelFormatYUV420P))
	frame := patternedVideoFrame("video", 4, 4, av.PixelFormatYUV420P)
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(2, 2)}[:0]}

	err := resizer.FilterInto(context.Background(), &frame, &result)
	if err != filter.ErrOutputBufferTooSmall {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestFilterAllocs(t *testing.T) {
	resizer := newTestFilter(t, filter.ResizeConfig{Width: 2, Height: 2}, videoStream(4, 4, av.PixelFormatYUV420P))
	frame := patternedVideoFrame("video", 4, 4, av.PixelFormatYUV420P)
	result := filter.Result{Frames: []av.Frame{preallocVideoFrame(2, 2)}[:0]}
	ctx := context.Background()

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := resizer.FilterInto(ctx, &frame, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func newTestFilter(t *testing.T, config filter.ResizeConfig, stream av.Stream) *Filter {
	t.Helper()
	frameFilter, err := NewFactory().NewFilter(context.Background(), filter.Config{
		Stream: stream,
		Video:  &config,
	})
	if err != nil {
		t.Fatal(err)
	}
	resizer, ok := frameFilter.(*Filter)
	if !ok {
		t.Fatalf("filter = %T", frameFilter)
	}
	return resizer
}

func videoStream(width int, height int, pixelFormat string) av.Stream {
	return av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:          av.CodecH264,
			Type:        av.MediaVideo,
			Width:       width,
			Height:      height,
			PixelFormat: pixelFormat,
		},
	}
}

func videoFrame(streamID av.StreamID, width int, height int, pixelFormat string, y []byte, u []byte, v []byte) av.Frame {
	return av.Frame{
		StreamID: streamID,
		Type:     av.MediaVideo,
		Video: &av.VideoFrame{
			Width:       width,
			Height:      height,
			PixelFormat: pixelFormat,
		},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: y, Ownership: av.BufferOwned}, Stride: width},
			{Buffer: av.Buffer{Bytes: u, Ownership: av.BufferOwned}, Stride: width / 2},
			{Buffer: av.Buffer{Bytes: v, Ownership: av.BufferOwned}, Stride: width / 2},
		},
	}
}

func patternedVideoFrame(streamID av.StreamID, width int, height int, pixelFormat string) av.Frame {
	y := make([]byte, width*height)
	for i := range y {
		y[i] = byte(i)
	}
	chromaWidth := width / 2
	chromaHeight := height / 2
	u := make([]byte, chromaWidth*chromaHeight)
	v := make([]byte, chromaWidth*chromaHeight)
	for i := range u {
		u[i] = byte(40 + i)
		v[i] = byte(80 + i)
	}
	return videoFrame(streamID, width, height, pixelFormat, y, u, v)
}

func preallocVideoFrame(width int, height int) av.Frame {
	return av.Frame{
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height/4)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height/4)}},
		},
	}
}
