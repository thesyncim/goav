package goav

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

func TestRuntimeBuilderTranscodeBranchesRenditionsToOutputs(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := codec.NewRegistry(
		codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		codec.WithEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{
			{
				Name:     "audio-main",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "audio-low",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:       "archive.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"archive"},
			},
			{
				Name:       "preview.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"audio-low"},
			},
		},
	}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 7 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "decode-audio -> encode-audio-main") ||
		!strings.Contains(planned.String(), "decode-audio -> encode-audio-low") ||
		!strings.Contains(planned.String(), "encode-audio-main -> archive.ogg") ||
		!strings.Contains(planned.String(), "encode-audio-low -> preview.ogg") {
		t.Fatalf("planned:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() || planned.Render("mermaid") != task.Describe().Render("mermaid") {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 {
		t.Fatalf("decodes=%d flushes=%d", decoder.decodes, decoder.flushes)
	}
	if len(encoderFactory.encoders) != 2 || encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("encoders=%d first=%+v second=%+v", len(encoderFactory.encoders), encoderAt(encoderFactory.encoders, 0), encoderAt(encoderFactory.encoders, 1))
	}
	if len(encoderFactory.configs) != 2 ||
		encoderFactory.configs[0].Stream.ID != "audio-main" ||
		encoderFactory.configs[1].Stream.ID != "audio-low" {
		t.Fatalf("encode configs: %+v", encoderFactory.configs)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	archive := muxers.muxers[0]
	preview := muxers.muxers[1]
	if !archive.opened || archive.writes != 2 || !streamIDsEqual(archive.openedStreams, []av.StreamID{"audio-main", "audio-low"}) ||
		!streamIDsEqual(archive.writtenStreams, []av.StreamID{"audio-main", "audio-low"}) {
		t.Fatalf("archive opened=%v writes=%d opened streams=%+v written=%+v", archive.opened, archive.writes, archive.openedStreams, archive.writtenStreams)
	}
	if !preview.opened || preview.writes != 1 || !streamIDsEqual(preview.openedStreams, []av.StreamID{"audio-low"}) ||
		!streamIDsEqual(preview.writtenStreams, []av.StreamID{"audio-low"}) {
		t.Fatalf("preview opened=%v writes=%d opened streams=%+v written=%+v", preview.opened, preview.writes, preview.openedStreams, preview.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoderFactory.encoders[0].closed || !encoderFactory.encoders[1].closed ||
		!archive.closed || !preview.closed {
		t.Fatalf("closed demux=%v decoder=%v encoders=%+v archive=%v preview=%v", demuxer.closed, decoder.closed, encoderFactory.encoders, archive.closed, preview.closed)
	}
}

func TestRuntimeBuilderBufferedTranscodeRequiresPacketCopyBound(t *testing.T) {
	builder, _, _, _, _ := newBufferedTranscodeCopyFixture(pipeline.BufferPolicy{
		Capacity: 8,
		Drop:     pipeline.DropOldest,
	})

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(context.Background()); !errors.Is(err, pipeline.ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

func TestRuntimeBuilderBufferedTranscodeCopiesEncodedPacketsToOutputs(t *testing.T) {
	builder, demuxer, muxers, decoder, encoderFactory := newBufferedTranscodeCopyFixture(pipeline.BufferPolicy{
		Capacity:        8,
		Drop:            pipeline.DropOldest,
		CopyPacketBytes: 8,
	})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(planned.String(), "decode-audio -> encode-audio-main") ||
		!strings.Contains(planned.String(), "encode-audio-low -> preview.ogg") {
		t.Fatalf("planned:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() || planned.Render("mermaid") != task.Describe().Render("mermaid") {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || len(encoderFactory.encoders) != 2 ||
		encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("decodes=%d encoders=%+v", decoder.decodes, encoderFactory.encoders)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	archive := muxers.muxers[0]
	preview := muxers.muxers[1]
	if !streamIDsUnorderedEqual(archive.writtenStreams, []av.StreamID{"audio-main", "audio-low"}) ||
		!bytes.Equal(archive.writtenPayloads, []byte{7, 7}) {
		t.Fatalf("archive written streams=%+v payloads=%+v", archive.writtenStreams, archive.writtenPayloads)
	}
	if !streamIDsEqual(preview.writtenStreams, []av.StreamID{"audio-low"}) ||
		!bytes.Equal(preview.writtenPayloads, []byte{7}) {
		t.Fatalf("preview written streams=%+v payloads=%+v", preview.writtenStreams, preview.writtenPayloads)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoderFactory.encoders[0].closed || !encoderFactory.encoders[1].closed ||
		!archive.closed || !preview.closed {
		t.Fatalf("closed demux=%v decoder=%v encoders=%+v archive=%v preview=%v", demuxer.closed, decoder.closed, encoderFactory.encoders, archive.closed, preview.closed)
	}
}

func TestRuntimeBuilderTranscodeAppliesResampleBranch(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := codec.NewRegistry(
		codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		codec.WithEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	resampler := &transcodeTestFilter{}
	filterFactory := &transcodeTestFilterFactory{filter: resampler}
	filters := filter.NewRegistry(filter.WithFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, filterFactory))
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{
			{
				Name:     "audio-main",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
			},
			{
				Name:     "audio-low",
				Selector: SelectAudio(),
				Resample: &filter.ResampleConfig{
					SampleRate: 16000,
					Channels:   1,
				},
				Encode: codec.EncodeConfig{
					Parameters: av.CodecParameters{ID: av.CodecPCM, Type: av.MediaAudio},
				},
			},
		},
		Outputs: []transcode.Output{{Name: "archive.ogg", Format: av.FormatOgg}},
	}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs), WithFilterRegistry(filters)).New().
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 7 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "decode-audio -> resample-audio-low") ||
		!strings.Contains(planned.String(), "resample-audio-low -> encode-audio-low") {
		t.Fatalf("planned:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() || planned.Render("mermaid") != task.Describe().Render("mermaid") {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if filterFactory.config.Stream.ID != "audio" || filterFactory.config.Audio == nil ||
		filterFactory.config.Audio.SampleRate != 16000 || filterFactory.config.Audio.Channels != 1 {
		t.Fatalf("filter config: %+v", filterFactory.config)
	}
	if resampler.frames != 1 || resampler.flushes != 1 {
		t.Fatalf("resampler frames=%d flushes=%d", resampler.frames, resampler.flushes)
	}
	if len(encoderFactory.encoders) != 2 || encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("encoders=%d first=%+v second=%+v", len(encoderFactory.encoders), encoderAt(encoderFactory.encoders, 0), encoderAt(encoderFactory.encoders, 1))
	}
	if len(encoderFactory.configs) != 2 ||
		encoderFactory.configs[1].Parameters.SampleRate != 16000 ||
		encoderFactory.configs[1].Parameters.ClockRate != 16000 ||
		encoderFactory.configs[1].Parameters.Channels != 1 {
		t.Fatalf("encode configs: %+v", encoderFactory.configs)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 2 ||
		!streamIDsEqual(muxers.muxers[0].writtenStreams, []av.StreamID{"audio-main", "audio-low"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !resampler.closed {
		t.Fatal("resampler not closed")
	}
}

func newBufferedTranscodeCopyFixture(policy pipeline.BufferPolicy) (Builder, *decodeTestDemuxer, *remuxTestMuxerFactory, *decodeTestDecoder, *encodeTestEncoderFactory) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload: av.Buffer{
				Bytes:     []byte{1, 2, 3},
				Ownership: av.BufferImmutable,
			},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := codec.NewRegistry(
		codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		codec.WithEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{
			{
				Name:     "audio-main",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "audio-low",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:       "archive.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"archive"},
			},
			{
				Name:       "preview.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"audio-low"},
			},
		},
	}
	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs), WithBufferPolicy(policy)).New().
		Transcode(plan)
	return builder, demuxer, muxers, decoder, encoderFactory
}

func streamIDsUnorderedEqual(got []av.StreamID, want []av.StreamID) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[av.StreamID]int, len(want))
	for i := range got {
		seen[got[i]]++
	}
	for i := range want {
		if seen[want[i]] == 0 {
			return false
		}
		seen[want[i]]--
	}
	return true
}

func TestRuntimeBuilderTranscodeRequiresTransformFactory(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
	)
	codecs := codec.NewRegistry(
		codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		codec.WithEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{{
			Name:     "audio-low",
			Selector: SelectAudio(),
			Resample: &filter.ResampleConfig{
				SampleRate: 16000,
				Channels:   1,
			},
			Encode: pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Transcode(plan).
		Build(context.Background())
	if err != filter.ErrNotFound {
		t.Fatalf("err = %v, want filter.ErrNotFound", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after missing filter factory")
	}
}

func TestRuntimeBuilderTranscodeRequiresMatchingOutputSelection(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{{
			Name:     "audio-main",
			Selector: SelectAudio(),
			Encode:   pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{
			Name:       "preview.ogg",
			Renditions: []string{"missing"},
		}},
	}

	_, err := New().New().Transcode(plan).Describe()
	if err != ErrUnsupportedBuild {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestTranscodeVideoFilterResultPreallocatesI420Planes(t *testing.T) {
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:        av.MediaVideo,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}

	result := filterResultForStream(stream)
	if len(result.Frames) != 0 || cap(result.Frames) != 1 {
		t.Fatalf("frames len=%d cap=%d", len(result.Frames), cap(result.Frames))
	}
	frame := result.Frames[:1][0]
	if len(frame.Planes) != 3 {
		t.Fatalf("planes = %d, want 3", len(frame.Planes))
	}
	if cap(frame.Planes[0].Buffer.Bytes) != 640*360 ||
		cap(frame.Planes[1].Buffer.Bytes) != 640*360/4 ||
		cap(frame.Planes[2].Buffer.Bytes) != 640*360/4 {
		t.Fatalf("plane caps = %d %d %d", cap(frame.Planes[0].Buffer.Bytes), cap(frame.Planes[1].Buffer.Bytes), cap(frame.Planes[2].Buffer.Bytes))
	}
}

func TestApplyResizeConfigToStreamFit(t *testing.T) {
	stream := av.Stream{
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:   av.MediaVideo,
			Width:  1920,
			Height: 1080,
		},
	}

	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Width: 1280, Height: 720, Mode: filter.ResizeFit}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 1280 || stream.Codec.Height != 720 {
		t.Fatalf("geometry = %dx%d, want 1280x720", stream.Codec.Width, stream.Codec.Height)
	}
}

func encoderAt(encoders []*encodeTestEncoder, index int) *encodeTestEncoder {
	if index >= len(encoders) {
		return nil
	}
	return encoders[index]
}

type transcodeTestFilterFactory struct {
	filter *transcodeTestFilter
	config filter.Config
}

func (f *transcodeTestFilterFactory) NewFilter(_ context.Context, config filter.Config) (filter.FrameFilter, error) {
	f.config = config
	if f.filter == nil {
		f.filter = &transcodeTestFilter{}
	}
	return f.filter, nil
}

type transcodeTestFilter struct {
	frames  int
	flushes int
	closed  bool
}

func (f *transcodeTestFilter) Descriptor() filter.Descriptor {
	return filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}
}

func (f *transcodeTestFilter) Open(context.Context, filter.Config) error {
	return nil
}

func (f *transcodeTestFilter) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
	if frame == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	outFrame := &out.Frames[index]
	outFrame.Reset()
	*outFrame = *frame
	f.frames++
	return nil
}

func (f *transcodeTestFilter) FlushInto(context.Context, *filter.Result) error {
	f.flushes++
	return nil
}

func (f *transcodeTestFilter) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (f *transcodeTestFilter) Close() error {
	f.closed = true
	return nil
}
