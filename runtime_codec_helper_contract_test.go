package goav

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

func TestPrepareEncodeConfigCarriesCustomSettingsAndStreamOverrides(t *testing.T) {
	control := func(any) error { return nil }
	input := audioOpusTestStream()
	input.Index = 2
	input.Language = "en"
	input.Name = "microphone"
	input.Epoch = 3
	input.Metadata = av.Metadata{"room": "studio"}
	input.Codec.Profile = "input-profile"
	input.Codec.Level = "input-level"
	input.Codec.ExtraData = av.Buffer{Bytes: []byte{1, 2, 3}, Ownership: av.BufferImmutable}
	input.Codec.Attributes = av.Metadata{"preserve": "input"}

	overrideMetadata := av.Metadata{"role": "voice"}
	request := encodeRequest{
		selector: testSelectAudio(),
		config: codec.EncodeConfig{
			Stream: av.Stream{
				ID:       "encoded-audio",
				Index:    7,
				Type:     av.MediaAudio,
				TimeBase: av.TimeBase{Num: 1, Den: 44100},
				Language: "pt",
				Name:     "voice",
				Epoch:    9,
				Metadata: overrideMetadata,
				Codec: av.CodecParameters{
					SampleRate:   44100,
					SampleFormat: av.SampleFormatS16,
				},
			},
			Parameters: av.CodecParameters{
				ID:         av.CodecPCM,
				Type:       av.MediaAudio,
				ClockRate:  44100,
				Channels:   1,
				Attributes: av.Metadata{"target": "pcm"},
			},
			Settings: codec.CodecSettings{
				Bitrate: 64_000,
				Profile: "speech",
				Level:   "l1",
				Control: control,
			},
		},
	}

	config, stream, err := prepareEncodeConfig(input, request, true)
	if err != nil {
		t.Fatal(err)
	}
	if stream.ID != "encoded-audio" || stream.Index != 7 || stream.Language != "pt" ||
		stream.Name != "voice" || stream.Epoch != 9 || !reflect.DeepEqual(stream.Metadata, overrideMetadata) {
		t.Fatalf("stream overrides not applied: %+v", stream)
	}
	if stream.Codec.ID != av.CodecPCM || stream.Codec.Type != av.MediaAudio ||
		stream.Codec.SampleRate != 44100 || stream.Codec.Channels != 1 ||
		stream.Codec.Profile != "speech" || stream.Codec.Level != "l1" {
		t.Fatalf("stream codec = %+v, want custom PCM parameters with settings profile/level", stream.Codec)
	}
	if stream.Codec.ExtraData.Bytes != nil || stream.Codec.Attributes["target"] != "pcm" {
		t.Fatalf("stream codec should reset incompatible input metadata and keep target attributes: %+v", stream.Codec)
	}
	if !config.Realtime || !config.LowLatency || config.Settings.Bitrate != 64_000 || config.Settings.Control == nil {
		t.Fatalf("config settings/realtime not preserved: %+v", config)
	}
}

func TestNewEncodeStageNamedPassesCustomEncoderSettings(t *testing.T) {
	factory := &encodeTestEncoderFactory{}
	rt := MustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, factory)))
	request := encodeRequest{
		name:     "voice",
		selector: testSelectAudio(),
		config:   pcmEncodeConfig(),
	}
	config := pcmEncodeConfig()
	config.Stream = av.Stream{
		ID:    "encoded-audio",
		Type:  av.MediaAudio,
		Epoch: 4,
		Codec: av.CodecParameters{
			ID:   av.CodecPCM,
			Type: av.MediaAudio,
		},
	}
	config.Settings = codec.CodecSettings{Bitrate: 96_000, Profile: "wideband"}

	stage, err := (&builder{runtime: rt}).newEncodeStageNamed(context.Background(), "encode-custom", request, config)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if stage.Name() != "encode-custom" {
		t.Fatalf("stage name = %q, want encode-custom", stage.Name())
	}
	if got := stage.DescribeNode().Detail; !strings.Contains(got, "codec=pcm") {
		t.Fatalf("stage detail = %q, want codec detail", got)
	}
	if factory.config.Settings.Bitrate != 96_000 || factory.config.Settings.Profile != "wideband" ||
		factory.config.Stream.ID != "encoded-audio" || factory.config.Stream.Epoch != 4 {
		t.Fatalf("factory config = %+v, want custom settings and output stream", factory.config)
	}
}

func TestNewEncodeStageNamedSurfacesCustomEncoderFactoryErrors(t *testing.T) {
	config := pcmEncodeConfig()
	rt := MustNew()
	if _, err := (&builder{runtime: rt}).newEncodeStageNamed(context.Background(), "encode-custom", encodeRequest{selector: testSelectAudio(), config: config}, config); err == nil {
		t.Fatal("unregistered encoder error = nil, want lookup error")
	}

	factoryErr := errors.New("encoder factory failed")
	rt = MustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, errorEncodeFactory{err: factoryErr})))
	if _, err := (&builder{runtime: rt}).newEncodeStageNamed(context.Background(), "encode-custom", encodeRequest{selector: testSelectAudio(), config: config}, config); !errors.Is(err, factoryErr) {
		t.Fatalf("factory error = %v, want %v", err, factoryErr)
	}

	rt = MustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, nilEncodeFactory{})))
	if _, err := (&builder{runtime: rt}).newEncodeStageNamed(context.Background(), "encode-custom", encodeRequest{selector: testSelectAudio(), config: config}, config); !errors.Is(err, codec.ErrNilEncoder) {
		t.Fatalf("nil encoder error = %v, want codec.ErrNilEncoder", err)
	}
}

func TestCompileEncodeStageNamedConnectsNamedStage(t *testing.T) {
	factory := &encodeTestEncoderFactory{}
	rt := MustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, factory)))
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "encode-contract"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	sourceRef, err := graph.AddSource(&runtimeTestSource{name: "source"}, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	config := pcmEncodeConfig()
	config.Stream = av.Stream{ID: "encoded", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecPCM, Type: av.MediaAudio}}
	ref, err := compileEncodeStageNamed(context.Background(), rt, graph, "encode-contract", sourceRef, encodeRequest{selector: testSelectAudio(), config: config}, config)
	if err != nil {
		t.Fatal(err)
	}
	if ref != "encode-contract" {
		t.Fatalf("encode ref = %q, want encode-contract", ref)
	}
	spec := graph.Spec()
	if len(spec.Edges) != 1 || spec.Edges[0].From != sourceRef || spec.Edges[0].To != ref {
		t.Fatalf("graph edges = %+v, want source -> encode-contract", spec.Edges)
	}
}

func TestCompileEncodeStageNamedClosesStageWhenGraphRefusesIt(t *testing.T) {
	encoder := &encodeTestEncoder{}
	factory := &encodeTestEncoderFactory{encoder: encoder}
	rt := MustNew(
		WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1}),
		withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, factory)),
	)
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "direct-only"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	sourceRef, err := graph.AddSource(&runtimeTestSource{name: "source"}, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	config := pcmEncodeConfig()
	config.Stream = av.Stream{ID: "encoded", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecPCM, Type: av.MediaAudio}}
	_, err = compileEncodeStageNamed(context.Background(), rt, graph, "encode-buffered", sourceRef, encodeRequest{selector: testSelectAudio(), config: config}, config)
	if !errors.Is(err, pipeline.ErrBufferedEdgesUnsupported) {
		t.Fatalf("compileEncodeStageNamed() error = %v, want buffered-edge refusal", err)
	}
	if !encoder.closed {
		t.Fatal("encoder was not closed after graph AddStage refusal")
	}
}

func TestEncodeDecodeBufferSizingContracts(t *testing.T) {
	if got := audioDecodeBufferSize(av.Stream{}); got != 48000*120/1000*2*2 {
		t.Fatalf("default audio decode buffer = %d", got)
	}
	if got := audioDecodeBufferSize(av.Stream{Codec: av.CodecParameters{ClockRate: 8000, Channels: 1}}); got != 960*1*2 {
		t.Fatalf("low-rate audio decode buffer = %d, want one 960-sample mono frame", got)
	}
	if got := audioDecodeBufferSize(av.Stream{Codec: av.CodecParameters{SampleRate: 4000, Channels: 2}}); got != 960*2*2 {
		t.Fatalf("clamped audio decode buffer = %d, want 960-sample stereo frame", got)
	}
	if got := audioDecodeBufferSize(av.Stream{Codec: av.CodecParameters{SampleRate: 48000, Channels: 1}}); got != 48000*120/1000*1*2 {
		t.Fatalf("sample-rate audio decode buffer = %d", got)
	}
	if got := videoEncodePacketBufferSize(320, 180); got != 4*1024*1024 {
		t.Fatalf("small video encode buffer = %d, want 4 MiB minimum", got)
	}
	if got := videoEncodePacketBufferSize(2048, 1024); got != 2048*1024*3 {
		t.Fatalf("sized video encode buffer = %d", got)
	}
	if got := videoEncodePacketBufferSize(8192, 8192); got != 32*1024*1024 {
		t.Fatalf("large video encode buffer = %d, want 32 MiB cap", got)
	}
}

type errorEncodeFactory struct {
	err error
}

func (f errorEncodeFactory) NewEncoder(context.Context, codec.EncodeConfig) (codec.Encoder, error) {
	return nil, f.err
}

type nilEncodeFactory struct{}

func (nilEncodeFactory) NewEncoder(context.Context, codec.EncodeConfig) (codec.Encoder, error) {
	return nil, nil
}
