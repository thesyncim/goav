package goav

import (
	"context"
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
		format.WithDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
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
	if len(planned.Nodes) != 3 || len(planned.Edges) != 2 {
		t.Fatalf("planned nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "input.ogg:out -> decode-audio:inout") ||
		!strings.Contains(planned.String(), "decode-audio:inout -> frames:in") {
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

func TestRuntimeBuilderDecodeRequiresUnambiguousStream(t *testing.T) {
	streams := []av.Stream{
		{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}},
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
