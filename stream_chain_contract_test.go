package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/shape"
)

func TestStreamSelectorOptionsCarryStableMetadata(t *testing.T) {
	config := newStreamSelectConfig(
		av.MediaAudio,
		StreamID("eng"),
		StreamName("English"),
		StreamIndex(2),
		InputName("mic"),
		nil,
	)
	if config.selector.Type != av.MediaAudio ||
		config.selector.ID != "eng" ||
		config.selector.Name != "English" ||
		config.selector.Index != 2 ||
		!config.selector.UseIndex ||
		config.input != "mic" {
		t.Fatalf("stream select config = %+v", config)
	}
}

func TestJobStreamBuilderJoinArmAndCurrentContracts(t *testing.T) {
	var nilBuilder *jobStreamBuilder
	if arm := nilBuilder.joinArm(); arm.chain != nil || arm.join != nil || arm.tap != nil || arm.region != nil {
		t.Fatalf("nil stream join arm = %+v, want zero", arm)
	}

	job := &Job{}
	builder := &jobStreamBuilder{job: job}
	first := builder.current()
	if first == nil || len(job.streams) != 1 || job.streams[0] != first {
		t.Fatalf("first current stream = %+v job streams=%d", first, len(job.streams))
	}
	if second := builder.current(); second != first || len(job.streams) != 1 {
		t.Fatalf("second current stream = %+v len=%d, want same stream without append", second, len(job.streams))
	}

	existing := &jobStreamBuild{name: "existing"}
	job = &Job{streams: []*jobStreamBuild{existing}}
	builder = &jobStreamBuilder{job: job}
	created := builder.current()
	if created == nil || created == existing || len(job.streams) != 1 || job.streams[0] != existing {
		t.Fatalf("current with existing job stream = created %+v job streams=%+v", created, job.streams)
	}
}

func TestFrameDomainSourceRejectsDecodeAndCopy(t *testing.T) {
	ctx := context.Background()
	source := mixTestAudioSource("frames", 100)
	sink := Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))
	tests := []struct {
		name string
		job  *Job
		code errcode.Code
	}{
		{
			name: "direct decode",
			job: From(source).Audio().
				Decode().
				To(sink),
			code: errcode.SourceShapeMismatch,
		},
		{
			name: "direct copy",
			job: From(source).Audio().
				Copy().
				To(sink),
			code: errcode.SourceShapeMismatch,
		},
		{
			name: "flow decode",
			job: From(source).Audio().
				Apply(Flow("decode").Audio().Decode()).
				To(sink),
			code: errcode.SourceShapeMismatch,
		},
		{
			name: "flow copy",
			job: From(source).Audio().
				Apply(Flow("copy").Audio().Copy()).
				To(sink),
			code: errcode.SourceShapeMismatch,
		},
		{
			name: "copy through encode helper",
			job: From(source).Audio().
				Encode(codec.Copy()).
				To(sink),
			code: errcode.SourceShapeMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Build(ctx)
			assertBuildErrorCode(t, err, tt.code)
		})
	}
}

func TestStreamChainRejectsInvalidPostEncodeAndTapContracts(t *testing.T) {
	ctx := context.Background()
	source := mixTestAudioSource("frames", 100)
	sink := Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))
	meter := FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		job  *Job
		code errcode.Code
	}{
		{
			name: "nil stage",
			job: From(source).Audio().
				Do(nil).
				To(sink),
			code: errcode.StageMissing,
		},
		{
			name: "shape after encode",
			job: From(source).Audio().
				Encode(codec.Opus()).
				Shape(shape.Frame(av.MediaAudio)).
				To(sink),
			code: errcode.StreamStepAfterEncode,
		},
		{
			name: "stage after encode",
			job: From(source).Audio().
				Encode(codec.Opus()).
				Do(meter).
				To(sink),
			code: errcode.StreamStepAfterEncode,
		},
		{
			name: "empty tap",
			job: From(source).Audio().
				Tap(FrameTap("")).
				To(sink),
			code: errcode.TapInvalid,
		},
		{
			name: "frame tap after encode",
			job: From(source).Audio().
				Encode(codec.Opus()).
				Tap(FrameTap("encoded.frames")).
				To(sink),
			code: errcode.TapDomainMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Build(ctx)
			assertBuildErrorCode(t, err, tt.code)
		})
	}
}
