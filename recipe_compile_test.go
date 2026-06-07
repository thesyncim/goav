package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	transcodepkg "github.com/thesyncim/goav/transcode"
)

type noCapabilityBuilder struct{}

func (b noCapabilityBuilder) Input(format.Input) builderAPI { return b }

func (b noCapabilityBuilder) RTP(rtpav.PacketReader, ...rtpOption) builderAPI { return b }

func (b noCapabilityBuilder) Mux(format.Output) builderAPI { return b }

func (b noCapabilityBuilder) Decode(av.StreamSelector) builderAPI { return b }

func (b noCapabilityBuilder) Encode(av.StreamSelector, codec.EncodeConfig) builderAPI { return b }

func (b noCapabilityBuilder) Filter(av.StreamSelector, pipeline.Stage) builderAPI { return b }

func (b noCapabilityBuilder) Transcode(transcodepkg.Plan) builderAPI { return b }

func (b noCapabilityBuilder) Source(pipeline.Source) builderAPI { return b }

func (b noCapabilityBuilder) Stage(pipeline.Stage) builderAPI { return b }

func (b noCapabilityBuilder) Sink(pipeline.Sink) builderAPI { return b }

func (b noCapabilityBuilder) Routes(...pipeline.Route) builderAPI { return b }

func (b noCapabilityBuilder) Describe() (pipeline.Spec, error) { return pipeline.Spec{}, nil }

func (b noCapabilityBuilder) Build(context.Context) (Task, error) { return nil, ErrUnsupportedBuild }

func TestRuntimeBuilderUsesMuxVerbNotOutput(t *testing.T) {
	builder := reflect.TypeOf((*builderAPI)(nil)).Elem()
	if _, ok := builder.MethodByName("Output"); ok {
		t.Fatal("private runtime builder should not expose Output; use Mux for mux endpoints")
	}
	if _, ok := builder.MethodByName("Mux"); !ok {
		t.Fatal("private runtime builder should expose Mux for mux endpoints")
	}
}

func TestRecipeCompileStateDoesNotCarryRecipeBuilders(t *testing.T) {
	stateType := reflect.TypeOf(recipeCompileState{})
	forbidden := map[reflect.Type]string{
		reflect.TypeOf((*Job)(nil)):                  "*Job",
		reflect.TypeOf((*branchCompositionJob)(nil)): "*branchCompositionJob",
		reflect.TypeOf((*jobStreamBuild)(nil)):       "*jobStreamBuild",
		reflect.TypeOf([]streamBuild(nil)):           "[]streamBuild",
	}
	for i := 0; i < stateType.NumField(); i++ {
		field := stateType.Field(i)
		if name, ok := forbidden[field.Type]; ok {
			t.Fatalf("recipeCompileState field %s carries %s; compiler passes should use captured intent attachments", field.Name, name)
		}
		switch field.Name {
		case "inputs", "outputs", "jobOutputs", "streamOutputs":
			t.Fatalf("recipeCompileState field %s uses builder-shaped attachment naming", field.Name)
		}
	}
}

func TestRecipeAttachmentConsistencyRejectsMismatches(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		want  string
	}{
		{
			name: "job inputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            Intent{Inputs: []InputIntent{{Name: "input.ivf"}}},
				outputAttachments: []EndpointSpec{FileOutput("recording.ivf", io.Discard)},
			},
			want: "inputs",
		},
		{
			name: "job outputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Targets: []TargetIntent{{Name: "recording.ivf"}}},
				inputAttachments:  []InputSpec{FileInput("input.ivf", strings.NewReader(""))},
				outputAttachments: nil,
			},
			want: "targets",
		},
		{
			name: "branch targets",
			state: recipeCompileState{
				operation:                branchCompositionOperation,
				branchCompositionPresent: true,
				intent:                   Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Targets: []TargetIntent{{Name: "web.ivf"}}},
				branchInputAttachment:    FileInput("input.ivf", strings.NewReader("")),
				branchTargetAttachments:  nil,
			},
			want: "targets",
		},
	}
	pass := validateRecipeAttachmentConsistencyPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "intent") ||
				!strings.Contains(err.Error(), "attached") ||
				!strings.Contains(err.Error(), "custom compiler passes") ||
				!strings.Contains(err.Error(), "goav.From") {
				t.Fatalf("err = %v, want attachment mismatch guidance", err)
			}
			if !errors.As(err, &buildErr) || buildErr.Code != "recipe_attachment_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want recipe_attachment_mismatch wrapping ErrUnsupportedBuild", err)
			}
		})
	}
}

func TestJobIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Targets: []TargetIntent{{Name: "recording.ivf"}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "duplicate stream intent",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{
						{Name: "audio", Decode: true, Targets: []string{"audio"}},
						{Name: "video", Decode: true, Targets: []string{"video"}},
					},
					Targets: []TargetIntent{{Name: "audio"}, {Name: "video"}},
				},
			},
			code: "stream_duplicate",
			want: "ordinary stream recipes select one audio or video stream",
		},
		{
			name: "mixed output scope",
			state: recipeCompileState{
				operation:      "build job",
				jobOutputCount: 1,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Decode:  true,
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "archive.ivf"}, {Name: "frames"}},
				},
			},
			code: "output_scope_mixed",
			want: "stream recipes use stream-local outputs",
		},
		{
			name: "stream operation missing",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
			},
			code: "stream_operation_missing",
			want: "no decode, processing stage, or encoder was requested",
		},
	}
	pass := validateJobIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestJobOutputBindingsPassRejectsUndefinedStreamRoutes(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ogg"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"missing"},
			}},
			Targets: []TargetIntent{{Name: "archive.ogg"}},
		},
		outputAttachments: []EndpointSpec{
			FileOutput("archive.ogg", io.Discard),
		},
	}

	err := validateJobOutputBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "stream route output missing is not attached") ||
		!strings.Contains(err.Error(), "selected stream chain") {
		t.Fatalf("err = %v, want stream output binding guidance", err)
	}
}

func TestOutputFormatAdapterPassesRejectMissingMuxers(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  string
	}{
		{
			name: "job probed format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   Default(),
				outputAttachments: []EndpointSpec{
					FileOutput("recording.webm", io.Discard),
				},
			},
			want: `format "matroska"`,
		},
		{
			name: "job explicit format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   Default(),
				outputAttachments: []EndpointSpec{
					FileOutput("", io.Discard).Format(av.FormatOgg),
				},
			},
			want: `format "ogg"`,
		},
		{
			name: "transcode probed format",
			pass: validateBranchTargetFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   Default(),
				branchTargetAttachments: []namedTargetSpec{{
					name:   "web",
					output: FileOutput("web.webm", io.Discard),
				}},
			},
			want: `format "matroska"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "target_muxer_missing" || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want target_muxer_missing wrapping format.ErrNotFound", err)
			}
			if buildErr.Operation != "open target" {
				t.Fatalf("operation = %q, want open target", buildErr.Operation)
			}
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "no muxer is registered") ||
				!strings.Contains(err.Error(), "WithFormatAdapter") {
				t.Fatalf("err = %v, want muxer guidance with %q", err, tt.want)
			}
		})
	}
}

func TestOutputFormatAdapterPassesStoreResolvedFormats(t *testing.T) {
	tests := []struct {
		name     string
		pass     recipeCompilePass
		state    recipeCompileState
		validate func(*testing.T, recipeCompileState)
	}{
		{
			name: "job probed output format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				outputAttachments: []EndpointSpec{
					FileOutput("recording.ogg", io.Discard),
				},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.outputAttachments) != 1 ||
					state.outputAttachments[0].format != "" ||
					state.outputAttachments[0].resolvedFormat != av.FormatOgg {
					t.Fatalf("output attachments = %+v, want resolved Ogg format", state.outputAttachments)
				}
			},
		},
		{
			name: "transcode probed output format",
			pass: validateBranchTargetFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				branchTargetAttachments: []namedTargetSpec{{
					name:   "web",
					output: FileOutput("web.ogg", io.Discard),
				}},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.branchTargetAttachments) != 1 ||
					state.branchTargetAttachments[0].output.format != "" ||
					state.branchTargetAttachments[0].output.resolvedFormat != av.FormatOgg ||
					state.branchTargetAttachments[0].output.output.Name != "web.ogg" {
					t.Fatalf("branch target attachments = %+v, want resolved Ogg format", state.branchTargetAttachments)
				}
			},
		},
		{
			name: "explicit output format stays explicit",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
					testFormatMuxer(av.FormatIVF, &remuxTestMuxerFactory{}),
				)),
				outputAttachments: []EndpointSpec{
					FileOutput("recording.media", io.Discard).Format(av.FormatIVF),
				},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.outputAttachments) != 1 || state.outputAttachments[0].format != av.FormatIVF {
					t.Fatalf("output attachments = %+v, want explicit IVF format preserved", state.outputAttachments)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.pass.Apply(&tt.state); err != nil {
				t.Fatalf("err = %v, want resolved output formats", err)
			}
			tt.validate(t, tt.state)
		})
	}
}

func TestResolvedJobOutputFormatsEnterMediaPlanBuild(t *testing.T) {
	runtime := New(
		WithDefaults(),
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)
	job := From(
		RTP(&runtimeRTPReceiver{
			streams: []Stream{{
				ID:   "audio",
				Type: av.MediaAudio,
				Codec: av.CodecParameters{
					ID:   av.CodecOpus,
					Type: av.MediaAudio,
				},
			}},
		}).Name("audio").Codec(Opus()),
	).Copy().To(FileOutput("recording.ogg", io.Discard)).UseRuntime(runtime)

	resolved, err := compileJobRecipeForBuild(job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuild() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindPacketCopy {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindPacketCopy)
	}
	if len(resolved.outputAttachments) != 1 {
		t.Fatalf("resolved output attachments = %d, want 1", len(resolved.outputAttachments))
	}
	if got := endpointSpecOpenFormat(resolved.outputAttachments[0]); got != av.FormatOgg {
		t.Fatalf("open output format = %q, want resolved Ogg format", got)
	}
	if got := endpointSpecGraphFormat(resolved.outputAttachments[0]); got != "" {
		t.Fatalf("graph detail output format = %q, want inferred format hidden from graph detail", got)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if len(spec.Nodes) != 2 || spec.Nodes[1].Name != "recording.ogg" || spec.Nodes[1].Detail != "mux, protocol=file" {
		t.Fatalf("built spec = %+v, want inferred format hidden from mux detail", spec)
	}
}

func TestResolvedTranscodeOutputFormatsEnterPlan(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		options:   recipeCompileOptions{preflightOutputAdapters: true},
		runtime: New(withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		)),
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Select:  StreamSelect{Type: av.MediaAudio},
				Encode:  Opus(Bitrate(96_000)),
				Targets: []string{"archive"},
			}},
			Targets: []TargetIntent{{Name: "archive"}},
		},
		branchInputAttachment: FileInput("input.ivf", strings.NewReader("")),
		branchTargetAttachments: []namedTargetSpec{{
			name:   "archive",
			output: FileOutput("archive.ogg", io.Discard),
		}},
	}

	if err := validateBranchTargetFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetFormatAdaptersPass() error = %v", err)
	}
	if err := planBranchCompositionIntentPass().Apply(&state); err != nil {
		t.Fatalf("planBranchCompositionIntentPass() error = %v", err)
	}
	if len(state.plan.Targets) != 1 ||
		state.plan.Targets[0].Format != "" ||
		state.plan.Targets[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("plan targets = %+v, want resolved Ogg open format without graph detail format", state.plan.Targets)
	}
}

func TestResolvedBranchRecipeOutputFormatsRefreshPreplannedTargets(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(Branch("main").Opus(96_000).To(Target("archive", FileOutput("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(context.Background(), job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if len(resolved.plan.Targets) != 1 ||
		resolved.plan.Targets[0].Format != "" ||
		resolved.plan.Targets[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("resolved plan targets = %+v, want resolved Ogg open format", resolved.plan.Targets)
	}
}

func TestInputFormatAdapterPassesRejectMissingDemuxers(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		want  []string
	}{
		{
			name: "job probed format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   Default(),
				inputAttachments: []InputSpec{
					FileInput("input.ogg", strings.NewReader("")),
				},
			},
			code: "input_demuxer_missing",
			want: []string{`format "ogg"`, "no demuxer is registered", "WithFormatAdapter"},
		},
		{
			name: "transcode probed format",
			pass: validateBranchInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation:             branchCompositionOperation,
				options:               recipeCompileOptions{preflightInputAdapters: true},
				runtime:               Default(),
				branchInputAttachment: FileInput("input.webm", strings.NewReader("")),
			},
			code: "input_demuxer_missing",
			want: []string{`format "matroska"`, "no demuxer is registered", "WithFormatAdapter"},
		},
		{
			name: "unknown input format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   Default(),
				inputAttachments: []InputSpec{
					FileInput("input.unknown", strings.NewReader("")),
				},
			},
			code: "input_format_unknown",
			want: []string{"could not be detected", "name=input.unknown", "goav.RTP"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want %s wrapping format.ErrNotFound", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestInputFormatAdapterPassSkipsLiveReceiveInputs(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightInputAdapters: true},
		runtime:   Default(),
		inputAttachments: []InputSpec{
			RTP(nil).Codec(Opus()),
		},
	}
	if err := validateJobInputFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want RTP input format preflight skipped", err)
	}
}

func TestInputFormatAdapterPassStoresProbeStreams(t *testing.T) {
	streams := []av.Stream{{
		Index: 0,
		ID:    "eng",
		Name:  "English",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightInputAdapters: true},
		runtime: New(withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{}),
		)),
		inputAttachments: []InputSpec{
			FileInput("input.ogg", strings.NewReader("")),
		},
	}

	if err := validateJobInputFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want input probe stored", err)
	}
	if len(state.inputProbes) != 1 || len(state.inputProbes[0].Streams) != 1 {
		t.Fatalf("input probes = %+v, want one probed stream", state.inputProbes)
	}
	if got := state.inputProbes[0].Streams[0]; got.ID != "eng" || got.Codec.ID != av.CodecOpus {
		t.Fatalf("probed stream = %+v, want English Opus stream", got)
	}
}

func TestKnownInputStreamSelectionPassRejectsProbedAmbiguousAndMissingStreams(t *testing.T) {
	streams := []av.Stream{
		{
			Index: 0,
			ID:    "eng",
			Name:  "English",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
		{
			Index: 1,
			ID:    "spa",
			Name:  "Spanish",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
	}
	tests := []struct {
		name   string
		stream StreamIntent
		code   string
		want   []string
	}{
		{
			name: "ambiguous probed audio",
			stream: StreamIntent{
				Name:   "audio",
				Select: StreamSelect{Type: av.MediaAudio},
				Decode: true,
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=audio", "id=eng", "id=spa", `.Audio(goav.StreamID("eng"))`, ".Audio(goav.StreamIndex(0))"},
		},
		{
			name: "missing probed video",
			stream: StreamIntent{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo},
				Decode: true,
			},
			code: "stream_missing",
			want: []string{"no stream matches type=video", "audio[0]", "id=eng", "codec=opus"},
		},
	}
	pass := validateJobKnownInputStreamSelectionPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: Intent{Streams: []StreamIntent{
					tt.stream,
				}},
				inputProbes: []format.ProbeResult{{
					Format:  av.FormatOgg,
					Streams: streams,
				}},
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestLiveStreamSelectionPassRejectsAmbiguousAndMissingStreams(t *testing.T) {
	tests := []struct {
		name   string
		intent Intent
		code   string
		want   []string
	}{
		{
			name: "ambiguous live video",
			intent: Intent{
				Inputs: []InputIntent{
					{Name: "front", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
					{Name: "screen", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
				},
				Streams: []StreamIntent{{
					Name:   "video",
					Select: StreamSelect{Type: av.MediaVideo},
					Decode: true,
				}},
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=video", "id=front", "id=screen", `.Video(goav.StreamID("front"))`, ".Video(goav.StreamIndex(0))"},
		},
		{
			name: "missing live audio",
			intent: Intent{
				Inputs: []InputIntent{
					{Name: "camera", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
				},
				Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
				}},
			},
			code: "stream_missing",
			want: []string{"no stream matches type=audio", "video[0]", "id=camera", "codec=vp8"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightLiveStreams: true},
				intent:    tt.intent,
			}
			err := validateJobLiveStreamSelectionPass().Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestLiveStreamSelectionPassSkipsPacketOnlyJobs(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightLiveStreams: true},
		intent: Intent{Inputs: []InputIntent{{
			Name:     "video",
			Protocol: av.ProtocolRTP,
			Codec:    VP8(),
			Realtime: true,
		}}},
	}
	if err := validateJobLiveStreamSelectionPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want packet-only record recipe skipped", err)
	}
}

func TestDecodeAdapterPassRejectsKnownLiveMissingDecoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecH264,
		Name:  "h264",
		Modes: []codec.Mode{codec.ModeDecode},
		Capabilities: codec.Capabilities{
			BuildTags: []string{"goav_goh264"},
		},
		Backend: codec.Backend{
			Name:   "goh264",
			Status: "planned-build-tagged",
		},
	})
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "missing decoder",
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    Opus(),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "audio",
						Select: StreamSelect{Type: av.MediaAudio},
						Decode: true,
					}},
				},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=opus", "goav.From"},
		},
		{
			name: "descriptor-only decoder",
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "video",
						Protocol: av.ProtocolRTP,
						Codec:    H264(),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "video",
						Select: StreamSelect{Type: av.MediaVideo},
						Decode: true,
					}},
				},
			},
			code:  "decode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=h264", "backend=goh264", "build_tags=goav_goh264"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJobDecodeAdaptersPass().Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestDecodeAdapterPassDefersAmbiguousLiveSelection(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightDecodeAdapters: true},
		runtime:   New(),
		intent: Intent{
			Inputs: []InputIntent{
				{Name: "front", Protocol: av.ProtocolRTP, Codec: H264(), Realtime: true},
				{Name: "screen", Protocol: av.ProtocolRTP, Codec: H264(), Realtime: true},
			},
			Streams: []StreamIntent{{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo},
				Decode: true,
			}},
		},
	}
	if err := validateJobDecodeAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want ambiguity to stay with stream resolution", err)
	}
}

func TestKnownInputDecodeAdapterPassesRejectMissingDecoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecH264,
		Name:  "h264",
		Modes: []codec.Mode{codec.ModeDecode},
		Capabilities: codec.Capabilities{
			BuildTags: []string{"goav_goh264"},
		},
		Backend: codec.Backend{
			Name:   "goh264",
			Status: "planned-build-tagged",
		},
	})
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "job probed decoder",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatOgg,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "audio",
						Type:  av.MediaAudio,
						Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
					}},
				}},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=opus", "goav.From"},
		},
		{
			name: "job probed descriptor-only decoder",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: Intent{Streams: []StreamIntent{{
					Name:   "video",
					Select: StreamSelect{Type: av.MediaVideo},
					Decode: true,
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo},
					}},
				}},
			},
			code:  "decode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=h264", "backend=goh264", "build_tags=goav_goh264"},
		},
		{
			name: "transcode probed decoder",
			pass: validateKnownBranchInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:    "360p",
					Select:  StreamSelect{Type: av.MediaVideo},
					Encode:  VP9(Bitrate(600_000)),
					Targets: []string{"web"},
				}}},
				branchInputProbeReady: true,
				branchInputProbe: format.ProbeResult{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo},
					}},
				},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=vp9", "goav.From"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestKnownInputDecodeAdapterPassDefersAmbiguousSelection(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightDecodeAdapters: true},
		runtime:   New(),
		intent: Intent{Streams: []StreamIntent{{
			Name:   "audio",
			Select: StreamSelect{Type: av.MediaAudio},
			Decode: true,
		}}},
		inputProbes: []format.ProbeResult{{
			Format: av.FormatOgg,
			Streams: []av.Stream{
				{Index: 0, ID: "eng", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "spa", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			},
		}},
	}
	if err := validateJobKnownInputDecodeAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want ambiguity to stay with stream resolution", err)
	}
}

func TestDecodeAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	audioCodec := av.CodecID("x_audio")
	videoCodec := av.CodecID("x_video")
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job decoder advertises video for audio live stream",
			pass: validateJobDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    Codec(audioCodec, av.MediaAudio),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "audio",
						Select: StreamSelect{Type: av.MediaAudio},
						Decode: true,
					}},
				},
			},
			want: []string{"decoder adapter does not support the requested media", "codec=x_audio", "field=media", "requested=audio", "supported=video"},
		},
		{
			name: "job decoder rejects probed sample format",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatOgg,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "audio",
						Type:  av.MediaAudio,
						Codec: av.CodecParameters{
							ID:           audioCodec,
							Type:         av.MediaAudio,
							SampleFormat: av.SampleFormatF32,
						},
					}},
				}},
			},
			want: []string{"decoder adapter does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
		{
			name: "branch decoder rejects probed pixel format",
			pass: validateKnownBranchInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:    "preview",
					Select:  StreamSelect{Type: av.MediaVideo},
					Encode:  VP9(Bitrate(600_000)),
					Targets: []string{"web"},
				}}},
				branchInputProbeReady: true,
				branchInputProbe: format.ProbeResult{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{
							ID:          videoCodec,
							Type:        av.MediaVideo,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
				},
			},
			want: []string{"decoder adapter does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want decode_adapter_incompatible wrapping ErrUnsupportedBuild", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestEncodeAdapterPassesRejectMissingEncoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecVP9,
		Name:  "vp9",
		Modes: []codec.Mode{codec.ModeEncode},
		Capabilities: codec.Capabilities{
			BuildTags: []string{"goav_govpx"},
		},
		Backend: codec.Backend{
			Name:   "govpx",
			Status: "planned-build-tagged",
		},
	})
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "job missing encoder",
			pass: validateJobEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime:   Default(),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Encode: Opus(Bitrate(96_000)),
				}}},
			},
			code:  "encode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no encoder adapter", "codec=opus", "SinkEndpoint"},
		},
		{
			name: "transcode descriptor-only encoder",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: Intent{Streams: []StreamIntent{{
					Name:   "360p",
					Encode: VP9(Bitrate(600_000)),
				}}},
			},
			code:  "encode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=vp9", "backend=govpx", "build_tags=goav_govpx"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestEncodeAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	audioCodec := av.CodecID("x_audio")
	videoCodec := av.CodecID("x_video")
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job encoder advertises video for audio stream",
			pass: validateJobEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Encode: Codec(audioCodec, av.MediaAudio),
				}}},
			},
			want: []string{"encoder adapter does not support the requested media", "codec=x_audio", "field=media", "requested=audio", "supported=video"},
		},
		{
			name: "branch encoder rejects sample format",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "voice",
					Select: StreamSelect{Type: av.MediaAudio},
					Encode: Codec(audioCodec, av.MediaAudio, Parameters(av.CodecParameters{
						ID:           audioCodec,
						Type:         av.MediaAudio,
						SampleFormat: av.SampleFormatF32,
					})),
				}}},
			},
			want: []string{"encoder adapter does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
		{
			name: "branch encoder rejects transformed pixel format",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "preview",
					Select: StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{{
						Resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
					Encode: Codec(videoCodec, av.MediaVideo),
				}}},
			},
			want: []string{"encoder adapter does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want encode_adapter_incompatible wrapping ErrUnsupportedBuild", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestTransformAdapterPassesRejectMissingFilters(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job missing resample filter",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "audio",
					Select:     StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{Resample(16_000, Mono)},
				}}},
			},
			want: []string{"no resample filter adapter", "transform=resample", "goav.Default", ".Resample"},
		},
		{
			name: "transcode missing resize filter",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "720p",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
				}}},
			},
			want: []string{"no resize filter adapter", "transform=resize", "goav.Default", ".Resize"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
				t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestTransformAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job resample filter advertises video",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResample,
					Input:  av.MediaVideo,
					Output: av.MediaVideo,
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "audio",
					Select:     StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{Resample(16_000, Mono)},
				}}},
			},
			want: []string{"resample filter adapter declares incompatible media", "expected_input=audio", "actual_input=video", "Audio().Resample"},
		},
		{
			name: "branch resize filter advertises audio",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResize,
					Input:  av.MediaAudio,
					Output: av.MediaAudio,
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "720p",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
				}}},
			},
			want: []string{"resize filter adapter declares incompatible media", "expected_input=video", "actual_input=audio", "Video().Resize"},
		},
		{
			name: "job resize mode unsupported by descriptor",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:        filter.FactoryResize,
					Input:       av.MediaVideo,
					Output:      av.MediaVideo,
					ResizeModes: []filter.ResizeMode{filter.ResizeFill},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "video",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
				}}},
			},
			want: []string{"does not support the requested resize mode", "field=resize_mode", "requested=exact", "supported=fill"},
		},
		{
			name: "branch resize pixel format unsupported by descriptor",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:         filter.FactoryResize,
					Input:        av.MediaVideo,
					Output:       av.MediaVideo,
					PixelFormats: []string{av.PixelFormatI420},
					ResizeModes:  []filter.ResizeMode{filter.ResizeFit},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "preview",
					Select: StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{{
						Resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							Mode:        filter.ResizeFit,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
				}}},
			},
			want: []string{"does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
		{
			name: "job resample sample format unsupported by descriptor",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:          filter.FactoryResample,
					Input:         av.MediaAudio,
					Output:        av.MediaAudio,
					SampleFormats: []string{av.SampleFormatS16},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{{
						Resample: &filter.ResampleConfig{
							SampleRate:   16_000,
							Channels:     Mono,
							SampleFormat: av.SampleFormatF32,
						},
					}},
				}}},
			},
			want: []string{"does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want transform_adapter_incompatible wrapping ErrUnsupportedBuild", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestJobStreamOutputKindsPassRejectsInvalidOutputShapes(t *testing.T) {
	frameSink := SinkEndpoint(SinkFunc("frames", func(context.Context, Message) error { return nil }))
	fileOutput := FileOutput("archive.ogg", io.Discard)
	tests := []struct {
		name    string
		stream  StreamIntent
		outputs []EndpointSpec
		code    string
		want    []string
	}{
		{
			name: "mixed frame and mux outputs",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"frames", "archive.ogg"},
			},
			outputs: []EndpointSpec{frameSink, fileOutput},
			code:    "output_kind_mixed",
			want:    []string{"cannot mix sink endpoints and muxed outputs", ".Branches(...)"},
		},
		{
			name: "mux output without encoder",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"archive.ogg"},
			},
			outputs: []EndpointSpec{fileOutput},
			code:    "encode_missing",
			want:    []string{"decoded frames cannot be written", ".Opus"},
		},
	}
	pass := validateJobStreamOutputKindsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "unused"}},
				},
				outputAttachments: tt.outputs,
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestJobStreamOutputKindsPassAllowsEncodedPacketFanout(t *testing.T) {
	packetSink := SinkEndpoint(SinkFunc("packets", func(context.Context, Message) error { return nil }))
	fileOutput := FileOutput("archive.ogg", io.Discard)
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ogg"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Decode:  true,
				Encode:  Opus(Bitrate(96_000)),
				Targets: []string{"packets", "archive.ogg"},
			}},
			Targets: []TargetIntent{{Name: "packets"}, {Name: "archive.ogg"}},
		},
		outputAttachments: []EndpointSpec{packetSink, fileOutput},
	}
	if err := validateJobStreamOutputKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateJobStreamOutputKindsPass() error = %v", err)
	}
}

func TestJobStreamRuntimeCapabilitiesPassRejectsUnsupportedBuilder(t *testing.T) {
	tests := []struct {
		name   string
		stream StreamIntent
		code   string
		want   []string
	}{
		{
			name: "codec change policy",
			stream: StreamIntent{
				Name:        "video",
				Select:      StreamSelect{Type: av.MediaVideo},
				Decode:      true,
				CodecChange: RealtimeCodecChangePolicy(),
				Targets:     []string{"frames"},
			},
			code: "codec_change_runtime_unsupported",
			want: []string{"codec-change policy requires the standard runtime builder", "goav.Default"},
		},
		{
			name: "stream transform",
			stream: StreamIntent{
				Name:       "audio",
				Select:     StreamSelect{Type: av.MediaAudio},
				Decode:     true,
				Transforms: []TransformSpec{Resample(48_000, Stereo)},
				Targets:    []string{"frames"},
			},
			code: "transform_runtime_unsupported",
			want: []string{"stream transforms require the standard runtime builder", ".Do(stage)"},
		},
	}
	pass := validateJobStreamRuntimeCapabilitiesPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "frames"}},
				},
				builder: noCapabilityBuilder{},
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestRequireMediaPlanGraphSpecPassWrapsUnsupportedRecipeShape(t *testing.T) {
	runtime := New().(*runtime)
	builder := (&builder{runtime: runtime}).Input(format.Input{Name: "input.ivf"})
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Name:   "record",
			Inputs: []InputIntent{{Name: "input.ivf"}},
		},
		builder: builder,
	}

	err := requireMediaPlanGraphSpecPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "recipe_graph_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want recipe_graph_unsupported wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{"recipe intent", "inputs: 1", "targets: 0", "goav.From", ".Copy().To", ".Branches"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if state.specReady || state.mediaBuildKind != "" {
		t.Fatalf("state specReady=%v mediaBuildKind=%q, want unset after unsupported selection", state.specReady, state.mediaBuildKind)
	}
}

func TestJobStreamAttachmentsPassRejectsInvalidConcreteSteps(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "nil custom stage",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Decode:  true,
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
				streamSteps: []jobStreamStepAttachment{{stepIndex: 0}},
			},
			code:  "stage_missing",
			cause: ErrNilStage,
			want:  []string{".Do(stage)", "goav.FrameFunc"},
		},
		{
			name: "transform attachment mismatch",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{{
						Name:       "audio",
						Select:     StreamSelect{Type: av.MediaAudio},
						Decode:     true,
						Transforms: []TransformSpec{Resample(48_000, Stereo)},
						Targets:    []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
				streamSteps: []jobStreamStepAttachment{{
					hasTransform:   true,
					transformIndex: 1,
					stepIndex:      0,
				}},
			},
			code:  "recipe_attachment_mismatch",
			cause: ErrUnsupportedBuild,
			want:  []string{"transform attachment", "transform index: 1", "intent transforms: 1", "Intent.Transforms"},
		},
	}
	pass := validateJobStreamAttachmentsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestJobIntentShapePassRejectsStreamTransforms(t *testing.T) {
	tests := []struct {
		name   string
		stream StreamIntent
		code   string
		want   string
	}{
		{
			name: "invalid resize",
			stream: StreamIntent{
				Name:       "video",
				Select:     StreamSelect{Type: av.MediaVideo},
				Decode:     true,
				Transforms: []TransformSpec{Resize(0, 720)},
				Targets:    []string{"frames"},
			},
			code: "transform_invalid",
			want: "positive width and height",
		},
		{
			name: "wrong media",
			stream: StreamIntent{
				Name:       "audio",
				Select:     StreamSelect{Type: av.MediaAudio},
				Decode:     true,
				Transforms: []TransformSpec{Resize(320, 180)},
				Targets:    []string{"frames"},
			},
			code: "transform_media_mismatch",
			want: "resize applies to video streams",
		},
		{
			name: "empty transform",
			stream: StreamIntent{
				Name:       "video",
				Select:     StreamSelect{Type: av.MediaVideo},
				Decode:     true,
				Transforms: []TransformSpec{{}},
				Targets:    []string{"frames"},
			},
			code: "transform_invalid",
			want: "empty stream transform",
		},
	}
	pass := validateJobIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "frames"}},
				},
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code {
				t.Fatalf("err = %v, want %s", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web"},
					}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "stream missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
				},
			},
			code: "stream_missing",
			want: "no audio or video branches are configured",
		},
		{
			name: "branch name missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web"},
					}},
				},
			},
			code: "stream_name_missing",
			want: "branches need stable names",
		},
		{
			name: "copy after decode unsupported",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Decode:  true,
						Encode:  Copy(),
						Targets: []string{"web"},
					}},
				},
			},
			code: "copy_unsupported",
			want: "packet-domain stream point",
		},
		{
			name: "auto unresolved",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  Auto(),
						Targets: []string{"web"},
					}},
				},
			},
			code: "encode_auto_unresolved",
			want: "automatic codec selection",
		},
		{
			name: "duplicate branch target",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web", "web"},
					}},
				},
			},
			code: "target_duplicate",
			want: "more than once",
		},
	}
	pass := validateBranchCompositionIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeAttachmentsPassRejectsInvalidConcreteAttachments(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		want  string
	}{
		{
			name: "rtp input",
			state: recipeCompileState{
				branchInputAttachment: RTP(&runtimeRTPReceiver{}).Name("video").Codec(VP8()),
				branchTargetAttachments: []namedTargetSpec{{
					name:   "web",
					output: FileOutput("web.ivf", io.Discard),
				}},
			},
			code: "unsupported_input",
			want: "RTP transcode recipes",
		},
		{
			name: "duplicate targets",
			state: recipeCompileState{
				branchInputAttachment: FileInput("input.ivf", strings.NewReader("")),
				branchTargetAttachments: []namedTargetSpec{
					{name: "web", output: FileOutput("web.ivf", io.Discard)},
					{name: "web", output: FileOutput("preview.ivf", io.Discard)},
				},
			},
			code: "target_duplicate",
			want: "defined more than once",
		},
	}
	pass := validateBranchCompositionAttachmentsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeBranchTargetKindsPassAllowsCopyMuxBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "archive",
				Select:  StreamSelect{Type: av.MediaVideo},
				Encode:  Copy(),
				Targets: []string{"web"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: FileOutput("web.ivf", io.Discard),
		}},
	}

	if err := validateBranchCompositionIntentShapePass().Apply(&state); err != nil {
		t.Fatalf("validateBranchCompositionIntentShapePass() error = %v", err)
	}
	if err := validateBranchTargetKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassAllowsRawSinkBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "preview",
				Select:  StreamSelect{Type: av.MediaVideo},
				Targets: []string{"frames"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "frames",
			output: SinkEndpoint(SinkFunc("frames", func(context.Context, Message) error { return nil })),
		}},
	}

	if err := validateBranchTargetKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassRejectsRawMuxBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "preview",
				Select:  StreamSelect{Type: av.MediaVideo},
				Targets: []string{"web"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: FileOutput("web.ivf", io.Discard),
		}},
	}

	err := validateBranchTargetKindsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "muxed target") || !strings.Contains(err.Error(), "SinkEndpoint") {
		t.Fatalf("err = %v, want mux and sink guidance", err)
	}
}

func TestTranscodeOutputBindingsPassRejectsUndefinedRoutes(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "360p",
				Select:  StreamSelect{Type: av.MediaVideo},
				Encode:  VP9(Bitrate(600_000)),
				Targets: []string{"missing"},
			}},
			Targets: []TargetIntent{{Name: "web.ivf"}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: FileOutput("web.ivf", io.Discard),
		}},
	}

	err := validateBranchTargetBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "target missing is referenced but not defined") ||
		!strings.Contains(err.Error(), "typed target values") {
		t.Fatalf("err = %v, want target binding guidance", err)
	}
}

func TestTranscodeKnownInputStreamSelectionPassRejectsProbedBranchAmbiguity(t *testing.T) {
	streams := []av.Stream{
		{
			Index: 0,
			ID:    "camera",
			Type:  av.MediaVideo,
			Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo},
		},
		{
			Index: 1,
			ID:    "screen",
			Type:  av.MediaVideo,
			Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo},
		},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{Streams: []StreamIntent{{
			Name:    "720p",
			Select:  StreamSelect{Type: av.MediaVideo},
			Encode:  VP9(Bitrate(2_000_000)),
			Targets: []string{"web"},
		}}},
		branchInputProbeReady: true,
		branchInputProbe: format.ProbeResult{
			Format:  av.FormatMatroska,
			Streams: streams,
		},
	}

	err := validateKnownBranchInputStreamSelectionPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "multiple streams match type=video") ||
		!strings.Contains(err.Error(), "id=camera") ||
		!strings.Contains(err.Error(), "id=screen") ||
		!strings.Contains(err.Error(), `.Video(goav.StreamID("camera"))`) {
		t.Fatalf("err = %v, want probed transcode stream-selection guidance", err)
	}
}

func TestCompileJobRecipeCarriesIntentAndMediaPlanBuild(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(FileOutput("recording.ivf", io.Discard))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.builder == nil {
		t.Fatal("compileJobRecipe() produced nil builder")
	}
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.specOrigin != graphSpecOriginMediaPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginMediaPlan)
	}
	if resolved.mediaBuildKind != mediaBuildKindPacketCopy {
		t.Fatalf("resolved media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindPacketCopy)
	}
	if resolved.intent.Name != "from" {
		t.Fatalf("intent name = %q, want from", resolved.intent.Name)
	}
	if len(resolved.intent.Inputs) != 1 || resolved.intent.Inputs[0].Name != "input.ivf" {
		t.Fatalf("intent inputs = %+v", resolved.intent.Inputs)
	}
	if len(resolved.intent.Targets) != 1 || resolved.intent.Targets[0].Name != "recording.ivf" {
		t.Fatalf("intent targets = %+v", resolved.intent.Targets)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if len(spec.Nodes) == 0 || len(spec.Edges) == 0 {
		t.Fatalf("resolved spec = %+v, want planned graph nodes and edges", spec)
	}
	if got, want := spec, resolved.spec; got.Name != want.Name || len(got.Nodes) != len(want.Nodes) || len(got.Edges) != len(want.Edges) {
		t.Fatalf("resolved.Describe() = %+v, want stored spec %+v", got, want)
	}
}

func TestMediaPlanGraphSpecPassPlansFileCopy(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(FileOutput("recording.ivf", io.Discard).Format(av.FormatIVF))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.specOrigin != graphSpecOriginMediaPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginMediaPlan)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	want := pipeline.Spec{
		Name:     "goav",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "input.ivf", Kind: pipeline.NodeSource, Detail: "demux, protocol=file"},
			{Name: "recording.ivf", Kind: pipeline.NodeStage, Detail: "mux, format=ivf, protocol=file"},
		},
		Edges: []pipeline.EdgeSpec{{
			From:   pipeline.NodeRef("input.ivf"),
			To:     pipeline.NodeRef("recording.ivf"),
			Policy: pipeline.RouteAll,
		}},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("spec = %+v, want %+v", spec, want)
	}
}

func TestMediaPlanGraphSpecPassPlansRTPCopy(t *testing.T) {
	job := From(
		RTP(&runtimeRTPReceiver{streams: []Stream{{
			ID:   "video",
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				ID:   av.CodecVP8,
				Type: av.MediaVideo,
			},
		}}}).Name("video").Codec(VP8()),
	).Copy().To(FileOutput("recording.ivf", io.Discard).Format(av.FormatIVF))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.specOrigin != graphSpecOriginMediaPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginMediaPlan)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	want := pipeline.Spec{
		Name:     "goav",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "video", Kind: pipeline.NodeSource, Detail: "rtp receive, codec=vp8"},
			{Name: "recording.ivf", Kind: pipeline.NodeStage, Detail: "mux, format=ivf, protocol=file"},
		},
		Edges: []pipeline.EdgeSpec{{
			From:   pipeline.NodeRef("video"),
			To:     pipeline.NodeRef("recording.ivf"),
			Policy: pipeline.RouteAll,
		}},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("spec = %+v, want %+v", spec, want)
	}
}

func TestCompileBranchCompositionRecipeCarriesIntentAndPlan(t *testing.T) {
	web := Target("web", FileOutput("web.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap("video.decoded").
		Branches(
			Branch("360p").
				Resize(640, 360).
				VP9(600_000).
				To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	builder, ok := resolved.builder.(*builder)
	if !ok {
		t.Fatalf("resolved builder type = %T, want *builder", resolved.builder)
	}
	if len(builder.transcodes) != 0 {
		t.Fatalf("builder transcodes = %d, want recipe plan kept off builder", len(builder.transcodes))
	}
	if len(resolved.plan.Branches) != 1 || resolved.plan.Branches[0].Name != "360p" {
		t.Fatalf("resolved plan branches = %+v, want 360p branch", resolved.plan.Branches)
	}
	if resolved.mediaBuildKind != mediaBuildKindBranch {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindBranch)
	}
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "from" {
		t.Fatalf("intent name = %q, want from", resolved.intent.Name)
	}
	if len(resolved.intent.Streams) != 1 || resolved.intent.Streams[0].Name != "360p" {
		t.Fatalf("intent streams = %+v", resolved.intent.Streams)
	}
	if got := resolved.intent.Streams[0].Targets; len(got) != 1 || got[0] != "web" {
		t.Fatalf("intent route targets = %+v, want [web]", got)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if len(spec.Nodes) == 0 || len(spec.Edges) == 0 {
		t.Fatalf("resolved spec = %+v, want planned graph nodes and edges", spec)
	}
}

func TestCompileLiveFlowBranchesRecipeUsesMediaPlanBranchComposer(t *testing.T) {
	voice := Target("voice", FileOutput("voice.ogg", io.Discard).Format(av.FormatOgg))
	archive := Target("archive", FileOutput("archive.ogg", io.Discard).Format(av.FormatOgg))
	job := From(RTP(&runtimeRTPReceiver{
		streams: []Stream{audioOpusTestStream()},
	}).Name("audio").Codec(Opus())).
		Audio().
		Branches(
			Branch("voice").Apply(AudioFlow("voice").OpusVoice()).To(voice),
			Branch("archive").Apply(AudioFlow("archive").OpusMusic()).To(archive),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	builder, ok := resolved.builder.(*builder)
	if !ok {
		t.Fatalf("resolved builder type = %T, want *builder", resolved.builder)
	}
	if len(builder.transcodes) != 0 || len(builder.rtpInputs) != 0 {
		t.Fatalf("builder transcodes=%d rtp=%d, want live branch composer kept off builder", len(builder.transcodes), len(builder.rtpInputs))
	}
	if resolved.branchInputAttachment.rtp == nil {
		t.Fatal("resolved branch input = nil RTP, want live branch composer input carried on resolved plan")
	}
	if len(resolved.plan.Branches) != 2 {
		t.Fatalf("resolved plan branches = %+v, want two live flow branches", resolved.plan.Branches)
	}
	if resolved.mediaBuildKind != mediaBuildKindBranch {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindBranch)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if !specHasNode(spec, "encode-voice") || !specHasNode(spec, "encode-archive") {
		t.Fatalf("spec = %+v, want flow branch encoders", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanBranchComposer(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Tap("audio.decoded").
		Branches(Branch("main").Opus(96_000).To(Target("archive", FileOutput("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindBranch {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindBranch)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanPacketCopy(t *testing.T) {
	job := From(
		RTP(&runtimeRTPReceiver{
			streams: []Stream{{
				ID:   "video",
				Type: av.MediaVideo,
				Codec: av.CodecParameters{
					ID:   av.CodecVP8,
					Type: av.MediaVideo,
				},
			}},
		}).Name("video").Codec(VP8()),
	).Copy().To(FileOutput("recording.ivf", io.Discard))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindPacketCopy {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindPacketCopy)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if len(planned.Nodes) != len(built.Nodes) || len(planned.Edges) != len(built.Edges) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileSinkEndpoint(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		To(SinkEndpoint(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindSinkEndpoint {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindSinkEndpoint)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedMediaPlanSinkEndpointPreservesCustomStage(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		To(SinkEndpoint(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindSinkEndpoint {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindSinkEndpoint)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if !specHasNode(spec, "meter") {
		t.Fatalf("built spec = %+v, want custom stage node", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPSinkEndpoint(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{streams: []Stream{stream}}
	runtime := New(withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})))
	job := From(RTP(receiver).Name("audio").Codec(Opus())).UseRuntime(runtime).
		Audio().
		Decode().
		To(SinkEndpoint(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindSinkEndpoint {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindSinkEndpoint)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanSelectedPacketSinkEndpoint(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(SinkEndpoint(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindPacketCopy {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindPacketCopy)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeOutput(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		Opus(96_000).
		To(FileOutput("archive.ogg", io.Discard))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindEncode {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindEncode)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "meter") || !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") {
		t.Fatalf("built spec = %+v, want meter, encode, and mux nodes", built)
	}
}

func TestMediaPlanDirectStreamUsesResolvedAttachments(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Tap("audio.decoded").
		Do(&runtimeTestStage{name: "meter"}).
		Opus(96_000).
		To(FileOutput("archive.ogg", io.Discard))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	stream, ok := resolved.singleStreamIntent()
	if !ok {
		t.Fatalf("resolved intent streams = %+v, want one stream", resolved.intent.Streams)
	}
	plan, ok, err := newMediaPlanSingleStreamGraph(resolved.runtime, resolved.inputAttachments, resolved.outputAttachments, stream)
	if err != nil || !ok {
		t.Fatalf("newMediaPlanSingleStreamGraph ok=%v err=%v", ok, err)
	}
	spec, err := plan.encodeOutputSpec()
	if err != nil {
		t.Fatalf("encodeOutputSpec() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if !reflect.DeepEqual(spec, planned) {
		t.Fatalf("attachment-built spec = %+v, resolved spec = %+v", spec, planned)
	}
	if !specHasNode(spec, "meter") || !specHasNode(spec, "encode-audio") || !specHasNode(spec, "archive.ogg") {
		t.Fatalf("spec = %+v, want stage, encoder, and target from resolved attachments", spec)
	}
	if len(resolved.streamAttachments) == 0 {
		t.Fatalf("resolved stream attachments are empty; taps and custom stages should be carried on the resolved recipe")
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeSinkEndpoint(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Opus(96_000).
		To(SinkEndpoint(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindSinkEndpoint {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindSinkEndpoint)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode and sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanEncodeMuxAndSinkEndpoints(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Opus(96_000).
		To(
			FileOutput("archive.ogg", io.Discard),
			SinkEndpoint(&runtimeTestSink{name: "packets"}),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindEncode {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindEncode)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode, mux, and packet sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPEncodeOutput(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{streams: []Stream{stream}}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(RTP(receiver).Name("audio").Codec(Opus())).UseRuntime(runtime).
		Audio().
		Decode().
		Opus(96_000).
		To(FileOutput("archive.ogg", io.Discard))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if resolved.mediaBuildKind != mediaBuildKindEncode {
		t.Fatalf("media build kind = %q, want %q", resolved.mediaBuildKind, mediaBuildKindEncode)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}
