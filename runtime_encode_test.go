package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/rtpav"
)

type encodeTestEncoderFactory struct {
	encoder  *encodeTestEncoder
	encoders []*encodeTestEncoder
	config   codec.EncodeConfig
	configs  []codec.EncodeConfig
}

func (f *encodeTestEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	f.configs = append(f.configs, config)
	if f.encoder == nil {
		encoder := &encodeTestEncoder{}
		f.encoders = append(f.encoders, encoder)
		return encoder, nil
	}
	return f.encoder, nil
}

type encodeTestEncoder struct {
	encodes int
	flushes int
	closed  bool
}

func (e *encodeTestEncoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecPCM}
}

func (e *encodeTestEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}

func (e *encodeTestEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID = frame.StreamID
	packet.CodecEpoch = frame.CodecEpoch
	packet.PTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Payload.Bytes = append(packet.Payload.Bytes, 7)
	e.encodes++
	return nil
}

func (e *encodeTestEncoder) FlushInto(context.Context, *codec.EncodeResult) error {
	e.flushes++
	return nil
}

func (e *encodeTestEncoder) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (e *encodeTestEncoder) Close() error {
	e.closed = true
	return nil
}

func TestRuntimeBuilderInputDecodeFilterEncodeOutputs(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	filter := &runtimeTestStage{name: "meter"}

	builder := newTestBuilder(t, formats, codecs).
		Input(Input{Name: "input.ogg"}).
		Decode(testSelectAudio()).
		Filter(testSelectAudio(), filter).
		Encode(testSelectAudio(), pcmEncodeConfig()).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 6 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "meter -> encode-audio") ||
		!strings.Contains(specText(planned), "encode-audio -> archive.ogg") ||
		!strings.Contains(specMermaid(planned), "preview.ogg\\nstage") {
		t.Fatalf("planned:\n%s\nmermaid:\n%s", specText(planned), specMermaid(planned))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || encoder.encodes != 1 || encoder.flushes != 1 {
		t.Fatalf("decodes=%d decoder flushes=%d encodes=%d encoder flushes=%d", decoder.decodes, decoder.flushes, encoder.encodes, encoder.flushes)
	}
	if filter.count != 3 {
		t.Fatalf("filter count = %d, want stream event, frame, and EOS", filter.count)
	}
	if encoderFactory.config.Stream.ID != "audio" ||
		encoderFactory.config.Parameters.ID != av.CodecPCM ||
		!encoderFactory.config.Realtime ||
		!encoderFactory.config.LowLatency {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	for i := range muxers.muxers {
		muxer := muxers.muxers[i]
		if !muxer.opened || muxer.writes != 1 || muxer.streamCount != 1 || muxer.lastStream != "audio" {
			t.Fatalf("muxer[%d] opened=%v writes=%d streams=%d last=%s", i, muxer.opened, muxer.writes, muxer.streamCount, muxer.lastStream)
		}
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoder.closed || !filter.closed {
		t.Fatalf("closed demux=%v decoder=%v encoder=%v filter=%v", demuxer.closed, decoder.closed, encoder.closed, filter.closed)
	}
	for i := range muxers.muxers {
		if !muxers.muxers[i].closed {
			t.Fatalf("muxer[%d] not closed", i)
		}
	}
}

func TestRuntimeBuilderRTPDecodeFilterEncodeOutput(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	filter := &runtimeTestStage{name: "meter"}

	builder := newTestBuilder(t, formats, codecs).
		RTP(receiver,
			WithRTPName("live-audio"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
		).
		Decode(testSelectAudio()).
		Filter(testSelectAudio(), filter).
		Encode(testSelectAudio(), pcmEncodeConfig()).
		Output(Output{Name: "encoded.ogg"})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 6 || len(planned.Edges) != 5 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "live-audio -> select-audio") ||
		!strings.Contains(specText(planned), "meter -> encode-audio") ||
		!strings.Contains(specText(planned), "encode-audio -> encoded.ogg") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || encoder.encodes != 1 || encoder.flushes != 1 || filter.count != 2 {
		t.Fatalf("decodes=%d decoder flushes=%d encodes=%d encoder flushes=%d filter=%d", decoder.decodes, decoder.flushes, encoder.encodes, encoder.flushes, filter.count)
	}
	if len(muxers.muxers) != 1 || !muxers.muxers[0].opened || muxers.muxers[0].writes != 1 {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !decoder.closed || !encoder.closed || !filter.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed receiver=%v decoder=%v encoder=%v filter=%v mux=%v", receiver.closed, decoder.closed, encoder.closed, filter.closed, muxers.muxers[0].closed)
	}
}

func TestRuntimeBuilderDecodeEncodeRequiresMatchingStream(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	_, err := newTestBuilder(t, formats, codecs).
		Input(Input{Name: "input.ogg"}).
		Decode(testSelectAudio()).
		Encode(testSelectVideo(), pcmEncodeConfig()).
		Output(Output{Name: "archive.ogg"}).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_stream_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_stream_mismatch wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "selected: audio[0]") || !strings.Contains(err.Error(), "requested: type=video") {
		t.Fatalf("err = %v, want selected and requested stream details", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after unsupported encode selector")
	}
}

func TestRuntimeBuilderDecodeEncodeRequiresTargetCodec(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
	)
	codecs := withTestCodecs(testCodecDecoder(
		codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio},
		&decodeTestDecoderFactory{decoder: &decodeTestDecoder{}},
	))

	_, err := newTestBuilder(t, formats, codecs).
		Input(Input{Name: "input.ogg"}).
		Decode(testSelectAudio()).
		Encode(testSelectAudio(), codec.EncodeConfig{}).
		Output(Output{Name: "archive.ogg"}).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_target_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no target codec") || !strings.Contains(err.Error(), "codec.EncodeConfig.Parameters.ID") {
		t.Fatalf("err = %v, want target codec guidance", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after missing encode target")
	}
}

func audioOpusTestStream() av.Stream {
	return av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func pcmEncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Parameters: av.CodecParameters{
			ID:           av.CodecPCM,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
		},
	}
}
