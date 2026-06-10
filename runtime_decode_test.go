package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
)

type decodeTestDecoderFactory struct {
	decoder *decodeTestDecoder
	config  codec.DecodeConfig
}

func (f *decodeTestDecoderFactory) NewDecoder(_ context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	f.config = config
	return f.decoder, nil
}

type decodeStateTestFactory struct {
	decodeTestDecoderFactory
	state any
}

func (f *decodeStateTestFactory) NewDecodeState(_ context.Context, config codec.DecodeConfig) (any, error) {
	if config.OpaqueState != nil || config.Stream.ID == "" || config.Bounds.MaxFramesPerInput == 0 {
		return nil, ErrUnsupportedBuild
	}
	f.state = &struct{ stream av.StreamID }{stream: config.Stream.ID}
	return f.state, nil
}

type decodeTestDecoder struct {
	decodes int
	flushes int
	closed  bool
}

func (d *decodeTestDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecOpus}
}

func (d *decodeTestDecoder) Open(context.Context, codec.DecodeConfig) error {
	return nil
}

func (d *decodeTestDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	d.decodes++
	return nil
}

func (d *decodeTestDecoder) FlushInto(context.Context, *codec.DecodeResult) error {
	d.flushes++
	return nil
}

func (d *decodeTestDecoder) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (d *decodeTestDecoder) Close() error {
	d.closed = true
	return nil
}

type decodeTestDemuxer struct {
	streams []av.Stream
	packets []av.Packet
	opened  bool
	closed  bool
	read    int
}

type decodeTestDemuxerFactory struct {
	demuxer format.Demuxer
}

func (f decodeTestDemuxerFactory) NewDemuxer(context.Context, format.ProbeResult) (format.Demuxer, error) {
	return f.demuxer, nil
}

func (d *decodeTestDemuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (d *decodeTestDemuxer) Open(context.Context, format.Input, format.OpenOptions) error {
	d.opened = true
	return nil
}

func (d *decodeTestDemuxer) Streams() []av.Stream {
	return d.streams
}

func (d *decodeTestDemuxer) ReadInto(_ context.Context, out *format.ReadResult) error {
	if d.read >= len(d.packets) {
		return io.EOF
	}
	packet := &d.packets[d.read]
	d.read++
	out.PacketReady = true
	*out.Packet = *packet
	return nil
}

func (d *decodeTestDemuxer) Close() error {
	d.closed = true
	return nil
}

func TestDecodeRejectsIncompatibleDescriptorBeforeOpeningDecoder(t *testing.T) {
	custom := av.CodecID("x_audio")
	streams := []av.Stream{{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:           custom,
			Type:         av.MediaAudio,
			SampleFormat: av.SampleFormatF32,
		},
	}}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoderFactory := &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{
		ID:   custom,
		Type: av.MediaAudio,
		Capabilities: codec.Capabilities{
			SampleFormats: []string{av.SampleFormatS16},
		},
	}, decoderFactory))

	_, err := From(FileInput("input.ogg", nil)).
		UseRuntime(New(formats, codecs)).
		Audio().
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "decode_adapter_incompatible" ||
		!strings.Contains(err.Error(), "field=sample_format") ||
		!strings.Contains(err.Error(), "requested=f32") ||
		!strings.Contains(err.Error(), "supported=s16") {
		t.Fatalf("err = %v, want runtime decode descriptor config error", err)
	}
	if decoderFactory.config.Stream.ID != "" {
		t.Fatalf("decoder opened before descriptor preflight: %+v", decoderFactory.config)
	}
}

func TestDecodeUsesFactoryStateProvider(t *testing.T) {
	streams := []av.Stream{{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:     av.CodecVP8,
			Type:   av.MediaVideo,
			Width:  16,
			Height: 16,
		},
	}}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{StreamID: "video", Payload: av.Buffer{Bytes: []byte{1}}}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	factory := &decodeStateTestFactory{
		decodeTestDecoderFactory: decodeTestDecoderFactory{decoder: decoder},
	}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, factory))
	sink := &runtimeTestSink{name: "frames"}

	task, err := From(FileInput("input.ivf", nil)).
		UseRuntime(New(formats, codecs)).
		Video().
		To(Sink(sink)).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if factory.state == nil || factory.config.OpaqueState != factory.state {
		t.Fatalf("state provider=%p config=%p", factory.state, factory.config.OpaqueState)
	}
	if factory.config.Stream.ID != "video" || !factory.config.Realtime || !factory.config.LowLatency {
		t.Fatalf("decode config: %+v", factory.config)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || sink.frames != 1 {
		t.Fatalf("decodes=%d frames=%d", decoder.decodes, sink.frames)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeResultForVideoPreallocatesPlaneBacking(t *testing.T) {
	result := decodeResultForStream(av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:     av.CodecH264,
			Type:   av.MediaVideo,
			Width:  640,
			Height: 360,
		},
	}, codec.DecodeBounds{})
	frames := result.Frames[:cap(result.Frames)]
	if len(frames) != 1 || cap(frames[0].Planes) < 3 {
		t.Fatalf("frames=%d plane cap=%d", len(frames), cap(frames[0].Planes))
	}
	assertVideoDecodePlaneCapacity(t, frames[0], 640, 360)
}

func TestDecodeResultForVideoUsesDefaultBackingWhenGeometryIsUnknown(t *testing.T) {
	result := decodeResultForStream(av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecVP8,
			Type: av.MediaVideo,
		},
	}, codec.DecodeBounds{})
	frames := result.Frames[:cap(result.Frames)]
	if len(frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(frames))
	}
	assertVideoDecodePlaneCapacity(t, frames[0], defaultVideoDecodeWidth, defaultVideoDecodeHeight)
}

func TestDecodeResultForVideoUsesBoundsCapacity(t *testing.T) {
	result := decodeResultForStream(av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecH264,
			Type: av.MediaVideo,
		},
	}, codec.DecodeBounds{
		MaxFramesPerInput:   2,
		MaxEventsPerInput:   3,
		MaxRequestsPerInput: 4,
		MaxWidth:            1280,
		MaxHeight:           720,
	})
	frames := result.Frames[:cap(result.Frames)]
	if len(frames) != 2 ||
		cap(frames[0].Planes) < 3 ||
		cap(frames[1].Planes) < 3 ||
		cap(result.Events) != 3 ||
		cap(result.Requests) != 4 {
		t.Fatalf("result shape frames=%d planes=%d/%d events=%d requests=%d",
			len(frames), cap(frames[0].Planes), cap(frames[1].Planes), cap(result.Events), cap(result.Requests))
	}
	assertVideoDecodePlaneCapacity(t, frames[0], 1280, 720)
	assertVideoDecodePlaneCapacity(t, frames[1], 1280, 720)
}

func assertVideoDecodePlaneCapacity(t *testing.T, frame av.Frame, width int, height int) {
	t.Helper()
	if len(frame.Planes) != 3 {
		t.Fatalf("planes = %d, want 3", len(frame.Planes))
	}
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	if cap(frame.Planes[0].Buffer.Bytes) != width*height ||
		cap(frame.Planes[1].Buffer.Bytes) != chromaWidth*chromaHeight ||
		cap(frame.Planes[2].Buffer.Bytes) != chromaWidth*chromaHeight {
		t.Fatalf("plane capacity = %d %d %d",
			cap(frame.Planes[0].Buffer.Bytes),
			cap(frame.Planes[1].Buffer.Bytes),
			cap(frame.Planes[2].Buffer.Bytes))
	}
}

func TestDecodeBoundsForVideoStream(t *testing.T) {
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:     av.CodecAV1,
			Type:   av.MediaVideo,
			Width:  1280,
			Height: 720,
		},
	}
	result := decodeResultForStream(stream, codec.DecodeBounds{})

	bounds := decodeBoundsForStream(stream, result, codec.DecodeBounds{})
	if bounds.MaxFramesPerInput != 1 ||
		bounds.MaxEventsPerInput != 1 ||
		bounds.MaxRequestsPerInput != 1 ||
		bounds.MaxWidth != 1280 ||
		bounds.MaxHeight != 720 {
		t.Fatalf("bounds = %+v", bounds)
	}
}

func TestDecodeBoundsForVideoStreamMergesRuntimeBounds(t *testing.T) {
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:     av.CodecAV1,
			Type:   av.MediaVideo,
			Width:  640,
			Height: 360,
		},
	}
	requested := codec.DecodeBounds{
		MaxFramesPerInput:   2,
		MaxEventsPerInput:   3,
		MaxRequestsPerInput: 4,
		MaxPayloadBytes:     4096,
		MaxRetainedBytes:    8192,
		MaxWidth:            1280,
		MaxHeight:           720,
	}
	result := decodeResultForStream(stream, requested)

	bounds := decodeBoundsForStream(stream, result, requested)
	if bounds.MaxFramesPerInput != 2 ||
		bounds.MaxEventsPerInput != 3 ||
		bounds.MaxRequestsPerInput != 4 ||
		bounds.MaxPayloadBytes != 4096 ||
		bounds.MaxRetainedBytes != 8192 ||
		bounds.MaxWidth != 1280 ||
		bounds.MaxHeight != 720 {
		t.Fatalf("bounds = %+v", bounds)
	}
}
