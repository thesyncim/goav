package goav

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// solverTestAudioSource declares an audio frame source with explicit format
// facts and pushes one frame matching them.
func solverTestAudioSource(id av.StreamID, rate int, channels int, sampleFormat string) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(rate, channels, sampleFormat), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			samples := rate / 100 // one 10ms frame, valid for real encoders
			b := make([]byte, samples*channels*2)
			for i := 0; i < samples*channels; i++ {
				binary.LittleEndian.PutUint16(b[i*2:], uint16(int16(100)))
			}
			frame := &av.Frame{
				StreamID: id, Type: av.MediaAudio,
				Audio:  &av.AudioFrame{SampleRate: rate, Channels: channels, SampleFormat: sampleFormat, Samples: samples},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
			}
			if _, err := push.Frame(frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

// shapeContractTestStage is a pass-through stage that declares its shape
// contract, so the solver can negotiate its input format.
type shapeContractTestStage struct {
	name   string
	inputs shape.Set
}

func (s *shapeContractTestStage) Name() string { return s.name }

func (s *shapeContractTestStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	return emitter.Emit(ctx, msg)
}

func (s *shapeContractTestStage) Close() error { return nil }

func (s *shapeContractTestStage) InputShapes() shape.Set { return s.inputs }

func (s *shapeContractTestStage) OutputShapes(input shape.Spec) shape.Set { return shape.Set{input} }

// shapeSolverTestFilterFactory backs custom conversion adapters registered for
// solver selection tests.
type shapeSolverTestFilterFactory struct {
	configs []filter.Config
}

func (f *shapeSolverTestFilterFactory) NewFilter(_ context.Context, config filter.Config) (filter.FrameFilter, error) {
	f.configs = append(f.configs, config)
	return &shapeSolverTestFilter{}, nil
}

type shapeSolverTestFilter struct{}

func (f *shapeSolverTestFilter) Descriptor() filter.Descriptor { return filter.Descriptor{} }

func (f *shapeSolverTestFilter) Open(context.Context, filter.Config) error { return nil }

func (f *shapeSolverTestFilter) FilterInto(context.Context, *av.Frame, *filter.Result) error {
	return nil
}

func (f *shapeSolverTestFilter) FlushInto(context.Context, *filter.Result) error { return nil }

func (f *shapeSolverTestFilter) HandleEvent(context.Context, *av.Event) error { return nil }

func (f *shapeSolverTestFilter) Close() error { return nil }

func solverTestOpusRuntime(options ...Option) *Runtime {
	options = append([]Option{
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	}, options...)
	return MustNew(options...)
}

// TestAutoInsertsResampleBeforeEncode is the end-to-end solver path: a 44.1kHz
// source feeding a 48kHz Opus encoder under .Auto(shape.AllowResample()) gets a
// REAL resample operation inserted — visible in Describe, equal between
// Describe and Build, carried as an Explain diagnostic, and live at run time.
func TestAutoInsertsResampleBeforeEncode(t *testing.T) {
	ctx := context.Background()
	var packets int
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessagePacket && m.Packet != nil {
			packets++
		}
		return nil
	}))

	job := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(sink).
		UseRuntime(solverTestOpusRuntime(testStdFilters()))

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resample-audio") {
		t.Fatalf("planned spec should show the inserted resample node:\n%s", specText(planned))
	}

	report, err := job.Explain(ctx)
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	found := false
	for _, warning := range report.Warnings {
		if warning.Code != "shape_conversion_inserted" {
			continue
		}
		found = true
		if want := "inserted resample 44.1kHz→48kHz before encode-opus (AllowResample)"; warning.Message != want {
			t.Fatalf("diagnostic message = %q, want %q", warning.Message, want)
		}
	}
	if !found {
		t.Fatalf("Explain should carry the insertion diagnostic, warnings = %+v", report.Warnings)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if packets < 1 {
		t.Fatalf("packets at sink = %d, want >=1 (44.1kHz resampled to 48kHz then encoded)", packets)
	}
}

// TestAutoEmptyPolicyRefusesConversion pins the refusal contract: .Auto() with
// no policies activates solving but allows nothing, so a needed conversion
// fails with the source shape, the expected shape, and the exact policy to add.
func TestAutoEmptyPolicyRefusesConversion(t *testing.T) {
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testStdFilters())).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_conversion_refused" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want shape_conversion_refused wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"encode-opus needs resample 44.1kHz→48kHz",
		"source=frame audio 44.1kHz 2ch s16",
		"expected_shape=domain=frame media=audio sample_rate=48000 channels=2",
		"add .Auto(shape.AllowResample()) to the chain",
		"insert .Resample(48000, 2) explicitly before encode-opus",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

// TestAutoInsufficientPolicyRefusesStageConversion pins the refusal on the
// hard-contract path: a stage whose InputShapes pin a different sample rate
// needs a resample, the chain's .Auto(...) is active but allows only resize,
// so the solver must refuse instead of inserting the disallowed conversion.
func TestAutoInsufficientPolicyRefusesStageConversion(t *testing.T) {
	stage := &shapeContractTestStage{
		name:   "voice",
		inputs: shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))},
	}
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResize()).
		Do(stage).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(MustNew(testStdFilters())).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_conversion_refused" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want shape_conversion_refused wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"stage-voice needs resample 44.1kHz→48kHz",
		"add .Auto(shape.AllowResample()) to the chain",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

// TestShapeMismatchSuggestsExactAutoFix pins the solver-aware mismatch error:
// without any .Auto(...) the chain still fails validation, but when a
// conversion would fix it the error names the exact policy to add.
func TestShapeMismatchSuggestsExactAutoFix(t *testing.T) {
	stage := &shapeContractTestStage{
		name:   "voice",
		inputs: shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))},
	}
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Do(stage).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(MustNew(testStdFilters())).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want operation_shape_mismatch wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"actual_shape=",
		"expected_shape=",
		"add .Auto(shape.AllowResample()) to the chain",
		"insert .Resample(48000, 2) explicitly before stage-voice",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

// TestAutoInsertsResizeBeforeEncode covers the resize class: an encoder with
// pinned geometry pulls a resize from the registry under shape.AllowResize().
func TestAutoInsertsResizeBeforeEncode(t *testing.T) {
	target := codec.VP9(codec.Bitrate(600_000))
	target.Parameters.Width = 640
	target.Parameters.Height = 360
	rt := MustNew(
		testStdFilters(),
		WithEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	job := From(compositeTestVideoSource("cam", 8, 8, 100, 10, 20)).
		Video().
		Auto(shape.AllowResize()).
		Encode(target).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt)

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resize-video") {
		t.Fatalf("planned spec should show the inserted resize node:\n%s", specText(planned))
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	found := false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_conversion_inserted" && strings.Contains(warning.Message, "resize 8x8→640x360 before encode-vp9 (AllowResize)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Explain should report the inserted resize, warnings = %+v", report.Warnings)
	}
}

// TestAutoInsertsFormatConvertThroughRegisteredAdapter covers the convert
// class and external adapters: a stage contract pins s16, the source is f32,
// and the only registered audio conversion filter is selected by capability.
func TestAutoInsertsFormatConvertThroughRegisteredAdapter(t *testing.T) {
	stage := &shapeContractTestStage{
		name:   "meter",
		inputs: shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))},
	}
	factory := &shapeSolverTestFilterFactory{}
	rt := MustNew(WithFilter(filter.Descriptor{
		Name:          "sampleconv",
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16},
		Realtime:      true,
		Stateless:     true,
	}, factory))

	job := From(solverTestAudioSource("mic", 48_000, codec.Mono, "f32")).
		Audio().
		Auto(shape.AllowConvert()).
		Do(stage).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt)

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "sampleconv-audio") {
		t.Fatalf("planned spec should show the selected adapter node:\n%s", specText(planned))
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if len(factory.configs) != 1 || factory.configs[0].Audio == nil || factory.configs[0].Audio.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("adapter configs = %+v, want one audio config converting to s16", factory.configs)
	}
}

// TestAutoFailsWithoutRegisteredAdapter pins the no-candidate error: the
// policy allows the conversion but no registered filter can perform it.
func TestAutoFailsWithoutRegisteredAdapter(t *testing.T) {
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime()).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_adapter_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want shape_adapter_missing wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"no registered filter can perform the resample conversion before encode-opus",
		"goav.WithFilter(",
		"bundle.MustNewFilters",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

// TestAutoFailsOnAmbiguousAdapters pins the ambiguity error: two registered
// filters can both perform the conversion, so the solver refuses to guess and
// lists them.
func TestAutoFailsOnAmbiguousAdapters(t *testing.T) {
	desc := filter.Descriptor{Input: av.MediaAudio, Output: av.MediaAudio, Realtime: true, Stateless: true}
	first := desc
	first.Name = "conv-a"
	second := desc
	second.Name = "conv-b"
	rt := solverTestOpusRuntime(
		WithFilter(first, &shapeSolverTestFilterFactory{}),
		WithFilter(second, &shapeSolverTestFilterFactory{}),
	)

	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_adapter_ambiguous" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want shape_adapter_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "conv-a, conv-b") {
		t.Fatalf("err = %v, want candidate list conv-a, conv-b", err)
	}
}

// TestAutoSolvesBranchChains covers the branch builder's .Auto(...): a planned
// branch encoding at a different rate gets its conversion inserted on the
// branch's own operation list.
func TestAutoSolvesBranchChains(t *testing.T) {
	pcm := av.CodecID("x_pcm_s16")
	source := Source("mic",
		shape.Packet(av.MediaAudio, pcm, shape.Audio(44_100, codec.Stereo, av.SampleFormatS16), shape.Stream("mic")),
		func(_ context.Context, push SourcePush) error {
			if _, err := push.Packet(&av.Packet{StreamID: "mic", Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{0, 0}, Ownership: av.BufferImmutable}}); err != nil {
				return err
			}
			return push.EOS()
		})
	rt := solverTestOpusRuntime(
		testStdFilters(),
		WithDecoder(codec.Descriptor{ID: pcm, Name: "PCM", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}}, recipePCMDecoderFactory{decoder: &recipePCMDecoder{}}),
	)
	job := From(source).
		Audio().
		Decode().
		Branches(
			Branch("voice").
				Auto(shape.AllowResample()).
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))),
		).
		UseRuntime(rt)

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resample-voice") {
		t.Fatalf("planned spec should show the inserted branch resample node:\n%s", specText(planned))
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestAutoWorksInFlows covers the flow chains' .Auto(...): a conversion policy
// declared on a reusable Flow enables the solver wherever the flow is applied,
// so the chain gets the needed conversion inserted as a real planned operation.
func TestAutoWorksInFlows(t *testing.T) {
	normalize := Flow("normalize").Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000)))
	job := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Apply(normalize).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testStdFilters()))

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resample-audio") {
		t.Fatalf("planned spec should show the flow-enabled resample node:\n%s", specText(planned))
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestReadmeAutoResampleExampleBuilds verifies the README Shape Solving
// example against the default runtime: the inserted resample shows up in the
// plan and the planned spec equals the built graph.
func TestReadmeAutoResampleExampleBuilds(t *testing.T) {
	mic := solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)
	job := From(mic).
		Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("voice.webm", io.Discard)).
		UseRuntime(testStdRuntime())
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resample-audio") {
		t.Fatalf("planned spec should show the inserted resample node:\n%s", specText(planned))
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestMixArmSolvingReportsDiagnostic pins the join side of the solver: mix
// arms keep auto-solving with zero opt-in (the implicit arm policy) and the
// insertion is reported through Explain like every other conversion.
func TestMixArmSolvingReportsDiagnostic(t *testing.T) {
	job := Mix(
		From(mixTestAudioSourceRate("a", 48_000)).Audio(),
		From(mixTestAudioSourceRate("b", 24_000)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(MustNew(testStdFilters()))

	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	found := false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_conversion_inserted" && strings.Contains(warning.Message, `inserted resample 24kHz→48kHz on mix arm "b" (join arm policy)`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("Explain should report the mix arm insertion, warnings = %+v", report.Warnings)
	}
}
