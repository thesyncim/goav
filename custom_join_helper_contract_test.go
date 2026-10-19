package goav

import (
	"context"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
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

func TestCustomJoinBaseStreamContracts(t *testing.T) {
	frameInput := Source("voice",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(context.Context, SourcePush) error { return nil },
	)
	framePlan := &compile.JoinPlan{
		Name: "funnel",
		Arms: []compile.JoinArmPlan{{SourceShape: compileInputFromSpec(frameInput, "").Shape}},
	}
	stream, base := framePlan.CustomJoinBaseStream(true)
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
	packetPlan := &compile.JoinPlan{
		Name: "switcher",
		Arms: []compile.JoinArmPlan{{
			Stream: packetArm,
			Domain: shape.DomainPacket,
		}},
	}
	stream, base = packetPlan.CustomJoinBaseStream(false)
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
	plan := &compile.JoinPlan{
		Name: "funnel",
		Arms: []compile.JoinArmPlan{{
			SourceShape: compileInputFromSpec(Source("voice",
				shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
				func(context.Context, SourcePush) error { return nil },
			), "").Shape,
		}},
	}

	withoutContract := compile.CustomJoinedStream(plan, nil, true)
	if withoutContract.ID != "funnel" || withoutContract.Codec.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("customJoinedStream without contract = %+v", withoutContract)
	}

	stage := customJoinContractStage{
		name: "funnel",
		outputs: shape.Set{
			shape.Frame(av.MediaAudio, shape.Audio(24_000, codec.Mono, av.SampleFormatF32), shape.Codec(av.CodecOpus)),
		},
	}
	withContract := compile.CustomJoinedStream(plan, stage, true)
	if withContract.ID != "funnel" ||
		withContract.Type != av.MediaAudio ||
		withContract.Codec.ID != av.CodecOpus ||
		withContract.Codec.SampleRate != 24_000 ||
		withContract.Codec.Channels != codec.Mono ||
		withContract.Codec.SampleFormat != av.SampleFormatF32 ||
		withContract.Codec.ClockRate != 24_000 {
		t.Fatalf("customJoinedStream with contract = %+v", withContract)
	}

	emptyOutput := compile.CustomJoinedStream(plan, customJoinContractStage{name: "funnel"}, true)
	if !reflect.DeepEqual(emptyOutput, withoutContract) {
		t.Fatalf("customJoinedStream empty output = %+v, want fallback %+v", emptyOutput, withoutContract)
	}

	if got := (&compile.JoinPlan{Tree: &compile.JoinTreeSnapshot{}}).CustomJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain non-custom plan = %s, want empty", got)
	}
	if got := (&compile.JoinPlan{Tree: &compile.JoinTreeSnapshot{Custom: &compile.CustomJoinSpec{Name: "empty", Stage: stage}}}).CustomJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain without arms = %s, want empty", got)
	}
	if got := (&compile.JoinPlan{
		Name: "bare",
		Tree: &compile.JoinTreeSnapshot{Custom: &compile.CustomJoinSpec{Name: "bare", Stage: customJoinBareStage{name: "bare"}}},
		Arms: plan.Arms,
	}).CustomJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain bare stage = %s, want empty", got)
	}
	if got := (&compile.JoinPlan{
		Name: "empty",
		Tree: &compile.JoinTreeSnapshot{Custom: &compile.CustomJoinSpec{Name: "empty", Stage: customJoinContractStage{name: "empty"}}},
		Arms: plan.Arms,
	}).CustomJoinedOutputDomain(); got != "" {
		t.Fatalf("customJoinedOutputDomain empty outputs = %s, want empty", got)
	}

	plan.Tree = &compile.JoinTreeSnapshot{Custom: &compile.CustomJoinSpec{Name: "funnel", Stage: stage}}
	plan.Profile.DecodeArms = true
	if got := plan.CustomJoinedOutputDomain(); got != shape.DomainFrame {
		t.Fatalf("customJoinedOutputDomain = %s, want frame", got)
	}
}
