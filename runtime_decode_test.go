package goav

import (
	"context"
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

func TestRuntimeBuilderInputDecodeSink(t *testing.T) {
	streams := []av.Stream{{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   2,
		},
	}}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := codec.NewRegistry(codec.WithDecoder(codec.Descriptor{
		ID:   av.CodecOpus,
		Type: av.MediaAudio,
	}, decoderFactory))
	sink := &runtimeTestSink{name: "frames"}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Input(Input{Name: "input.ogg"}).
		Decode(SelectAudio()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 4 || len(planned.Edges) != 3 {
		t.Fatalf("planned nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "input.ogg -> select-audio") ||
		!strings.Contains(planned.String(), "select-audio -> decode-audio") ||
		!strings.Contains(planned.String(), "decode-audio -> frames") {
		t.Fatalf("planned spec:\n%s", planned.String())
	}
	if !strings.Contains(planned.String(), "source input.ogg [demux]") ||
		!strings.Contains(planned.String(), "stage select-audio [select, type=audio]") ||
		!strings.Contains(planned.String(), "stage decode-audio [packets -> frames, type=audio]") {
		t.Fatalf("planned details:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if planned.String() != spec.String() || planned.Mermaid() != spec.Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), spec.String())
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !demuxer.opened || decoder.decodes != 1 || decoder.flushes != 1 {
		t.Fatalf("opened=%v decodes=%d flushes=%d", demuxer.opened, decoder.decodes, decoder.flushes)
	}
	if sink.frames != 1 || sink.lastFrame.StreamID != "audio" {
		t.Fatalf("frames=%d last=%+v", sink.frames, sink.lastFrame)
	}
	if decoderFactory.config.Stream.ID != "audio" || !decoderFactory.config.Realtime || !decoderFactory.config.LowLatency {
		t.Fatalf("decode config: %+v", decoderFactory.config)
	}

	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v sink=%v", demuxer.closed, decoder.closed, sink.closed)
	}
}

func TestRuntimeBuilderInputDecodeFilterSink(t *testing.T) {
	streams := []av.Stream{{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   2,
		},
	}}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := codec.NewRegistry(codec.WithDecoder(codec.Descriptor{
		ID:   av.CodecOpus,
		Type: av.MediaAudio,
	}, &decodeTestDecoderFactory{decoder: decoder}))
	filter := &runtimeTestStage{name: "meter"}
	sink := &runtimeTestSink{name: "frames"}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Input(Input{Name: "input.ogg"}).
		Decode(SelectAudio()).
		Filter(SelectAudio(), filter).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 5 || len(planned.Edges) != 4 {
		t.Fatalf("planned nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "decode-audio -> meter") ||
		!strings.Contains(planned.String(), "meter -> frames") {
		t.Fatalf("planned spec:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || filter.count != 1 || sink.frames != 1 {
		t.Fatalf("decodes=%d flushes=%d filter=%d frames=%d", decoder.decodes, decoder.flushes, filter.count, sink.frames)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !filter.closed || !sink.closed {
		t.Fatalf("closed demux=%v filter=%v sink=%v", demuxer.closed, filter.closed, sink.closed)
	}
}

func TestRuntimeBuilderDecodeFilterRequiresMatchingStream(t *testing.T) {
	streams := []av.Stream{{
		ID:    "audio",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := codec.NewRegistry(codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))

	_, err := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Input(Input{Name: "input.ogg"}).
		Decode(SelectAudio()).
		Filter(SelectVideo(), &runtimeTestStage{name: "resize"}).
		Sink(&runtimeTestSink{name: "frames"}).
		Build(context.Background())
	if err != ErrUnsupportedBuild {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after unsupported filter selector")
	}
}

func TestRuntimeBuilderInputDecodeSinkSelectsMatchingStream(t *testing.T) {
	streams := []av.Stream{
		{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8}},
		{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}},
	}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{
			{StreamID: "video", Payload: av.Buffer{Bytes: []byte{9}}},
			{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{1}}},
		},
	}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := codec.NewRegistry(codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus}, decoderFactory))
	sink := &runtimeTestSink{name: "frames"}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Input(Input{Name: "input.ogg"}).
		Decode(SelectAudio()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(planned.String(), "input.ogg -> select-audio") {
		t.Fatalf("planned spec:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if planned.String() != spec.String() || planned.Mermaid() != spec.Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), spec.String())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoderFactory.config.Stream.ID != "audio" || decoder.decodes != 1 || sink.frames != 1 {
		t.Fatalf("stream=%s decodes=%d frames=%d", decoderFactory.config.Stream.ID, decoder.decodes, sink.frames)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBuilderDecodeRequiresUnambiguousStream(t *testing.T) {
	streams := []av.Stream{
		{ID: "audio-main", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}},
		{ID: "audio-alt", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}},
		{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8}},
	}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := codec.NewRegistry(codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))

	_, err := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Input(Input{Name: "input.ogg"}).
		Decode(SelectAudio()).
		Sink(&runtimeTestSink{name: "frames"}).
		Build(context.Background())
	if err != ErrUnsupportedBuild {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestDecodeResultForVideoPreallocatesPlaneSlots(t *testing.T) {
	result := decodeResultForStream(av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecH264,
			Type: av.MediaVideo,
		},
	})
	frames := result.Frames[:cap(result.Frames)]
	if len(frames) != 1 || cap(frames[0].Planes) < 3 {
		t.Fatalf("frames=%d plane cap=%d", len(frames), cap(frames[0].Planes))
	}
}
