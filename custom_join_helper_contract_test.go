package goav

import (
	"context"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type customJoinContractStage struct {
	name    string
	inputs  shape.Set
	outputs shape.Set
}

func (s customJoinContractStage) Name() string { return s.name }

func (s customJoinContractStage) InputShapes() shape.Set { return s.inputs }

func (s customJoinContractStage) OutputShapes(shape.Spec) shape.Set { return s.outputs }

func (s customJoinContractStage) Handle(context.Context, *pipeline.Message, pipeline.Emitter) error {
	return nil
}

func (s customJoinContractStage) Close() error { return nil }

type customJoinBareStage struct{ name string }

func (s customJoinBareStage) Name() string { return s.name }

func (s customJoinBareStage) Handle(context.Context, *pipeline.Message, pipeline.Emitter) error {
	return nil
}

func (s customJoinBareStage) Close() error { return nil }

func TestCustomJoinShapeContractHelpers(t *testing.T) {
	audioFrame := shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))
	videoFrame := shape.Frame(av.MediaVideo, shape.Video(640, 360, av.PixelFormatI420))

	if customJoinDecodesArms(nil) {
		t.Fatal("empty input contract decoded arms")
	}
	if customJoinDecodesArms(shape.Set{shape.Packet(av.MediaAudio, av.CodecOpus)}) {
		t.Fatal("packet input contract decoded arms")
	}
	if customJoinDecodesArms(shape.Set{audioFrame, shape.Packet(av.MediaAudio, av.CodecOpus)}) {
		t.Fatal("mixed-domain input contract decoded arms")
	}
	if !customJoinDecodesArms(shape.Set{audioFrame, videoFrame}) {
		t.Fatal("all frame-domain input contract did not require decoded arms")
	}

	if got := customJoinMedia(shape.Set{audioFrame, shape.Frame(av.MediaAudio)}); got != av.MediaAudio {
		t.Fatalf("customJoinMedia uniform audio = %s, want audio", got)
	}
	if got := customJoinMedia(shape.Set{shape.Frame("")}); got != "" {
		t.Fatalf("customJoinMedia missing media = %s, want empty", got)
	}
	if got := customJoinMedia(shape.Set{audioFrame, videoFrame}); got != "" {
		t.Fatalf("customJoinMedia mixed media = %s, want empty", got)
	}
}

func TestCustomJoinArmExpectedContracts(t *testing.T) {
	if got, ok := customJoinArmExpected(nil); ok || got != (shape.Spec{}) {
		t.Fatalf("empty expected = %+v %v, want none", got, ok)
	}
	if got, ok := customJoinArmExpected(shape.Set{
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, "")),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, "")),
	}); ok || got != (shape.Spec{}) {
		t.Fatalf("multi-shape expected = %+v %v, want none", got, ok)
	}
	if got, ok := customJoinArmExpected(shape.Set{shape.Packet(av.MediaAudio, av.CodecOpus)}); ok || got != (shape.Spec{}) {
		t.Fatalf("packet expected = %+v %v, want none", got, ok)
	}
	if got, ok := customJoinArmExpected(shape.Set{shape.Frame(av.MediaAudio)}); ok || got != (shape.Spec{}) {
		t.Fatalf("formatless frame expected = %+v %v, want none", got, ok)
	}

	audioExpected, ok := customJoinArmExpected(shape.Set{
		shape.Frame(
			av.MediaAudio,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatF32),
			shape.Stream("ignored"),
			shape.Codec(av.CodecOpus),
			shape.Format(av.FormatOgg),
		),
	})
	wantAudio := shape.Spec{
		MediaKind:    av.MediaAudio,
		SampleRate:   48_000,
		Channels:     codec.Stereo,
		SampleFormat: av.SampleFormatF32,
	}
	if !ok || !reflect.DeepEqual(audioExpected, wantAudio) {
		t.Fatalf("audio expected = %+v %v, want %+v true", audioExpected, ok, wantAudio)
	}

	videoExpected, ok := customJoinArmExpected(shape.Set{
		shape.Frame(
			av.MediaVideo,
			shape.Video(1280, 720, av.PixelFormatI420),
			shape.Stream("ignored"),
			shape.Codec(av.CodecVP8),
		),
	})
	wantVideo := shape.Spec{
		MediaKind:   av.MediaVideo,
		Width:       1280,
		Height:      720,
		PixelFormat: av.PixelFormatI420,
	}
	if !ok || !reflect.DeepEqual(videoExpected, wantVideo) {
		t.Fatalf("video expected = %+v %v, want %+v true", videoExpected, ok, wantVideo)
	}
}

func TestCustomJoinBaseStreamContracts(t *testing.T) {
	frameInput := Source("voice",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(context.Context, SourcePush) error { return nil },
	)
	framePlan := &joinPlan{
		name: "funnel",
		arms: []joinArmPlan{{input: frameInput}},
	}
	stream, base := framePlan.customJoinBaseStream(true)
	if stream.ID != "funnel" ||
		stream.Type != av.MediaAudio ||
		stream.Codec.Type != av.MediaAudio ||
		stream.Codec.SampleRate != 48_000 ||
		stream.Codec.Channels != codec.Stereo ||
		stream.Codec.SampleFormat != av.SampleFormatS16 ||
		stream.Codec.ClockRate != 48_000 {
		t.Fatalf("decoded custom join stream = %+v", stream)
	}
	if base.Domain != shape.DomainFrame || base.StreamID != "voice" || base.MediaKind != av.MediaAudio {
		t.Fatalf("decoded custom join base = %+v", base)
	}

	packetArm := av.Stream{
		ID:       "upstream",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48_000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			SampleRate: 48_000,
			Channels:   codec.Stereo,
		},
	}
	packetPlan := &joinPlan{
		name: "switcher",
		arms: []joinArmPlan{{
			stream: packetArm,
			domain: shape.DomainPacket,
		}},
	}
	stream, base = packetPlan.customJoinBaseStream(false)
	if stream.ID != "switcher" ||
		stream.Codec.ID != av.CodecOpus ||
		stream.TimeBase != packetArm.TimeBase ||
		base.Domain != shape.DomainPacket ||
		base.StreamID != "switcher" ||
		base.Codec != av.CodecOpus {
		t.Fatalf("passthrough custom join stream=%+v base=%+v", stream, base)
	}
	if packetArm.ID != "upstream" {
		t.Fatalf("customJoinBaseStream mutated source arm stream id to %s", packetArm.ID)
	}
}

func TestCustomJoinedStreamAndDomainContracts(t *testing.T) {
	plan := &joinPlan{
		name: "funnel",
		arms: []joinArmPlan{{
			input: Source("voice",
				shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
				func(context.Context, SourcePush) error { return nil },
			),
		}},
	}

	withoutContract := customJoinedStream(plan, nil, true)
	if withoutContract.ID != "funnel" || withoutContract.Codec.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("customJoinedStream without contract = %+v", withoutContract)
	}

	stage := customJoinContractStage{
		name: "funnel",
		outputs: shape.Set{
			shape.Frame(av.MediaAudio, shape.Audio(24_000, codec.Mono, av.SampleFormatF32), shape.Codec(av.CodecOpus)),
		},
	}
	withContract := customJoinedStream(plan, stage, true)
	if withContract.ID != "funnel" ||
		withContract.Type != av.MediaAudio ||
		withContract.Codec.ID != av.CodecOpus ||
		withContract.Codec.SampleRate != 24_000 ||
		withContract.Codec.Channels != codec.Mono ||
		withContract.Codec.SampleFormat != av.SampleFormatF32 ||
		withContract.Codec.ClockRate != 24_000 {
		t.Fatalf("customJoinedStream with contract = %+v", withContract)
	}

	emptyOutput := customJoinedStream(plan, customJoinContractStage{name: "funnel"}, true)
	if !reflect.DeepEqual(emptyOutput, withoutContract) {
		t.Fatalf("customJoinedStream empty output = %+v, want fallback %+v", emptyOutput, withoutContract)
	}

	if got := (&joinPlan{tree: &joinTreeSnapshot{}}).customJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain non-custom plan = %s, want empty", got)
	}
	if got := (&joinPlan{tree: &joinTreeSnapshot{custom: &customJoinSpec{name: "empty", stage: stage}}}).customJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain without arms = %s, want empty", got)
	}
	if got := (&joinPlan{
		name: "bare",
		tree: &joinTreeSnapshot{custom: &customJoinSpec{name: "bare", stage: customJoinBareStage{name: "bare"}}},
		arms: plan.arms,
	}).customJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain bare stage = %s, want empty", got)
	}
	if got := (&joinPlan{
		name: "empty",
		tree: &joinTreeSnapshot{custom: &customJoinSpec{name: "empty", stage: customJoinContractStage{name: "empty"}}},
		arms: plan.arms,
	}).customJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain empty outputs = %s, want empty", got)
	}

	plan.tree = &joinTreeSnapshot{custom: &customJoinSpec{name: "funnel", stage: stage}}
	plan.profile.decodeArms = true
	if got := plan.customJoinedOutputDomain(); got != shape.DomainFrame {
		t.Fatalf("customJoinedOutputDomain = %s, want frame", got)
	}
}

func TestApplyShapeToStreamContracts(t *testing.T) {
	stream := av.Stream{
		ID:   "joined",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			ClockRate:    48_000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}
	if got := applyShapeToStream(stream, shape.Spec{}); !reflect.DeepEqual(got, stream) {
		t.Fatalf("empty overlay = %+v, want unchanged %+v", got, stream)
	}

	overlay := shape.Spec{
		MediaKind:    av.MediaVideo,
		Codec:        av.CodecVP8,
		Width:        640,
		Height:       360,
		PixelFormat:  av.PixelFormatI420,
		SampleRate:   24_000,
		Channels:     codec.Mono,
		SampleFormat: av.SampleFormatF32,
	}
	got := applyShapeToStream(stream, overlay)
	if got.ID != "joined" ||
		got.Type != av.MediaVideo ||
		got.Codec.Type != av.MediaVideo ||
		got.Codec.ID != av.CodecVP8 ||
		got.Codec.Width != 640 ||
		got.Codec.Height != 360 ||
		got.Codec.PixelFormat != av.PixelFormatI420 ||
		got.Codec.SampleRate != 24_000 ||
		got.Codec.ClockRate != 24_000 ||
		got.Codec.Channels != codec.Mono ||
		got.Codec.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("overlay stream = %+v", got)
	}
}
