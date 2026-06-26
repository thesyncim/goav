package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/shape"
)

// --- .Require(...): hard shape assertions ---

// TestRequireMetIsNoOp pins the satisfied assertion: a requirement the stream
// already meets changes nothing — the chain builds, runs, and the planned spec
// equals the built graph (the assertion lowers to no runtime node).
func TestRequireMetIsNoOp(t *testing.T) {
	ctx := context.Background()
	job := From(solverTestAudioSource("mic", 48_000, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testBundleFilters()))

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	// No-op means no-op: a satisfied requirement must not insert anything.
	if strings.Contains(specText(planned), "resample") {
		t.Fatalf("satisfied .Require(...) inserted a conversion:\n%s", specText(planned))
	}
	report, err := job.Explain(ctx)
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	for _, warning := range report.Warnings {
		if warning.Code == "shape_conversion_inserted" {
			t.Fatalf("satisfied .Require(...) reported an insertion: %+v", warning)
		}
	}
	task, err := job.BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != BuildLive():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run(): %v", err)
	}
}

// TestRequireViolatedFailsWithFix pins the hard refusal: the build fails
// before any resource opens with the actual and required shapes, and — since a
// conversion could satisfy the requirement — the exact .Auto(...) policy and
// the explicit operation to write instead.
func TestRequireViolatedFailsWithFix(t *testing.T) {
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testBundleFilters())).
		BuildLive(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_requirement_unmet" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want shape_requirement_unmet with matching BuildError code", err)
	}
	for _, want := range []string{
		".Require(...) is not satisfied",
		"the stream is frame audio 44.1kHz 2ch s16",
		"required frame audio 48kHz 2ch",
		"operation=require",
		"actual_shape=",
		"expected_shape=",
		"add .Auto(shape.AllowResample()) to the chain",
		"insert .Resample(48000, 2) explicitly before require",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

// TestRequireSatisfiedByAutoConversion pins the solver interplay: under an
// .Auto(...) policy that covers the delta, the planner inserts the conversion
// before the assertion, the requirement holds by construction, and the
// insertion is reported like any other solver conversion.
func TestRequireSatisfiedByAutoConversion(t *testing.T) {
	ctx := context.Background()
	job := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample()).
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testBundleFilters()))

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
		if warning.Code == "shape_conversion_inserted" && strings.Contains(warning.Message, "inserted resample 44.1kHz→48kHz before require (AllowResample)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Explain should report the insertion before the requirement, warnings = %+v", report.Warnings)
	}
	task, err := job.BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != BuildLive():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestRequireWorksInBranches pins the branch surface: a violated requirement
// on a planned branch fails the build naming the branch.
func TestRequireWorksInBranches(t *testing.T) {
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
		testBundleFilters(),
		WithDecoder(codec.Descriptor{ID: pcm, Name: "PCM", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}}, recipePCMDecoderFactory{decoder: &recipePCMDecoder{}}),
	)
	_, err := From(source).
		Audio().
		Decode().
		Branches(
			Branch("voice").
				Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, ""))).
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))),
		).
		UseRuntime(rt).
		BuildLive(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_requirement_unmet" {
		t.Fatalf("err = %v, want shape_requirement_unmet", err)
	}
	if buildErr.Node != "voice" {
		t.Fatalf("node = %q, want the branch name voice", buildErr.Node)
	}
}

// TestRequireWorksInFlows pins the flow surface: a requirement declared on a
// reusable Flow is enforced wherever the flow is applied.
func TestRequireWorksInFlows(t *testing.T) {
	normalize := Flow("normalize").Audio().
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, "")))
	_, err := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Apply(normalize).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(testBundleFilters())).
		BuildLive(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_requirement_unmet" {
		t.Fatalf("err = %v, want shape_requirement_unmet", err)
	}
}

// --- .Prefer(...): soft solver preferences ---

// TestPreferResolvesAdapterAmbiguity pins the headline preference win: on an
// offline runtime two registered filters can both perform the conversion — a
// hard shape_adapter_ambiguous without help — and the preference's capability
// facts (here: realtime) narrow the choice to one, recorded as a diagnostic.
func TestPreferResolvesAdapterAmbiguity(t *testing.T) {
	desc := filter.Descriptor{Input: av.MediaAudio, Output: av.MediaAudio, Stateless: true}
	rtCapable := desc
	rtCapable.Name = "conv-rt"
	rtCapable.Realtime = true
	offline := desc
	offline.Name = "conv-offline"
	rt := solverTestOpusRuntime(
		WithRealtime(false),
		WithFilter(rtCapable, &shapeSolverTestFilterFactory{}),
		WithFilter(offline, &shapeSolverTestFilterFactory{}),
	)
	chain := func() *jobStreamBuilder {
		return From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
			Audio().
			Auto(shape.AllowResample())
	}

	// Without a preference the selection is ambiguous and refuses to guess.
	_, err := chain().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt).
		BuildLive(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_adapter_ambiguous" {
		t.Fatalf("err = %v, want shape_adapter_ambiguous without a preference", err)
	}

	// The preference resolves it: only conv-rt declares realtime capability.
	job := chain().
		Prefer(shape.New(shape.Realtime(true))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt)
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "conv-rt") {
		t.Fatalf("planned spec should show the preferred adapter node:\n%s", specText(planned))
	}
	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	found := false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_preference_applied" && strings.Contains(warning.Message, "resolved the adapter choice") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Explain should record the preference resolution, warnings = %+v", report.Warnings)
	}
	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != BuildLive():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestPreferBiasesOpenConversionTarget pins the open-fact bias: the encoder
// pins rate and channels but leaves the sample format open, and the
// preference sets it on the planned conversion — visible in the adapter's
// constructed config and recorded as a diagnostic.
func TestPreferBiasesOpenConversionTarget(t *testing.T) {
	factory := &shapeSolverTestFilterFactory{}
	rt := solverTestOpusRuntime(WithFilter(filter.Descriptor{
		Name:          "sampleconv",
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16, "f32"},
		Realtime:      true,
		Stateless:     true,
	}, factory))

	job := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample(), shape.AllowConvert()).
		Prefer(shape.New(shape.Audio(0, 0, "f32"))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt)

	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	applied, inserted := false, false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_preference_applied" && strings.Contains(warning.Message, "set the open conversion target facts") {
			applied = true
		}
		if warning.Code == "shape_conversion_inserted" && strings.Contains(warning.Message, "s16→f32") {
			inserted = true
		}
	}
	if !applied || !inserted {
		t.Fatalf("Explain should record the biased target (applied=%v inserted=%v), warnings = %+v", applied, inserted, report.Warnings)
	}
	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if len(factory.configs) != 1 || factory.configs[0].Audio == nil || factory.configs[0].Audio.SampleFormat != "f32" {
		t.Fatalf("adapter configs = %+v, want one audio config targeting the preferred f32", factory.configs)
	}
}

// TestPreferUnsatisfiableIgnoredWithDiagnostic pins the soft contract: a
// preference the chain policy cannot cover never fails the build — it is
// dropped with a diagnostic and the plain plan proceeds.
func TestPreferUnsatisfiableIgnoredWithDiagnostic(t *testing.T) {
	factory := &shapeSolverTestFilterFactory{}
	rt := solverTestOpusRuntime(WithFilter(filter.Descriptor{
		Name:          "sampleconv",
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16, "f32"},
		Realtime:      true,
		Stateless:     true,
	}, factory))

	job := From(solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)).
		Audio().
		Auto(shape.AllowResample()). // no AllowConvert: the f32 preference is not honorable
		Prefer(shape.New(shape.Audio(0, 0, "f32"))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt)

	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	ignored := false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_preference_ignored" && strings.Contains(warning.Message, "does not allow the preferred conversion") {
			ignored = true
		}
	}
	if !ignored {
		t.Fatalf("Explain should record the dropped preference, warnings = %+v", report.Warnings)
	}
	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if len(factory.configs) != 1 || factory.configs[0].Audio == nil || factory.configs[0].Audio.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("adapter configs = %+v, want the plain s16 plan (preference dropped)", factory.configs)
	}
}

// TestPreferWorksInBranches pins the branch surface of .Prefer(...): inside a
// planned branch the same two-adapter ambiguity is a hard refusal without a
// preference, and the branch's .Prefer(realtime) narrows the choice — the
// preferred adapter shows up in the planned spec and Explain records the
// resolution against the branch.
func TestPreferWorksInBranches(t *testing.T) {
	pcm := av.CodecID("x_pcm_s16")
	source := func() InputSpec {
		return Source("mic",
			shape.Packet(av.MediaAudio, pcm, shape.Audio(44_100, codec.Stereo, av.SampleFormatS16), shape.Stream("mic")),
			func(_ context.Context, push SourcePush) error {
				if _, err := push.Packet(&av.Packet{StreamID: "mic", Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{0, 0}, Ownership: av.BufferImmutable}}); err != nil {
					return err
				}
				return push.EOS()
			})
	}
	desc := filter.Descriptor{Input: av.MediaAudio, Output: av.MediaAudio, Stateless: true}
	rtCapable := desc
	rtCapable.Name = "conv-rt"
	rtCapable.Realtime = true
	offline := desc
	offline.Name = "conv-offline"
	rt := solverTestOpusRuntime(
		WithRealtime(false),
		WithFilter(rtCapable, &shapeSolverTestFilterFactory{}),
		WithFilter(offline, &shapeSolverTestFilterFactory{}),
		WithDecoder(codec.Descriptor{ID: pcm, Name: "PCM", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}}, recipePCMDecoderFactory{decoder: &recipePCMDecoder{}}),
	)
	branch := func(prefer bool) BranchSpec {
		b := Branch("voice").Auto(shape.AllowResample())
		if prefer {
			b = b.Prefer(shape.New(shape.Realtime(true)))
		}
		return b.Encode(codec.Opus(codec.Bitrate(96_000))).
			To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))
	}

	// Without the preference the adapter choice is ambiguous and refused.
	_, err := From(source()).Audio().Decode().
		Branches(branch(false)).
		UseRuntime(rt).
		BuildLive(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "shape_adapter_ambiguous" {
		t.Fatalf("err = %v, want shape_adapter_ambiguous without a preference", err)
	}

	// The branch's .Prefer(realtime) resolves it to conv-rt.
	job := From(source()).Audio().Decode().
		Branches(branch(true)).
		UseRuntime(rt)
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "conv-rt") {
		t.Fatalf("planned spec should show the preferred adapter on the branch:\n%s", specText(planned))
	}
	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain(): %v", err)
	}
	found := false
	for _, warning := range report.Warnings {
		if warning.Code == "shape_preference_applied" && strings.Contains(warning.Message, "resolved the adapter choice") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Explain should record the branch preference resolution, warnings = %+v", report.Warnings)
	}
	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != specText(planned) {
		t.Fatalf("Describe() != BuildLive():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
}

// TestReadmeRequireExampleBuilds verifies the documented shape-error recipe:
// .Require(...) under .Auto(shape.AllowResample()) builds against the default
// runtime with the conversion inserted before the assertion.
func TestReadmeRequireExampleBuilds(t *testing.T) {
	mic := solverTestAudioSource("mic", 44_100, codec.Stereo, av.SampleFormatS16)
	job := From(mic).
		Audio().
		Auto(shape.AllowResample()).
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, 2, ""))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Write("voice.webm", io.Discard)).
		UseRuntime(testBundleRuntime())
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if !strings.Contains(specText(planned), "resample-audio") {
		t.Fatalf("planned spec should show the inserted resample node:\n%s", specText(planned))
	}
	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
}
