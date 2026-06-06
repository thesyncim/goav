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

func (b noCapabilityBuilder) Output(format.Output) builderAPI { return b }

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

func TestRecipeCompileStateDoesNotCarryRecipeBuilders(t *testing.T) {
	stateType := reflect.TypeOf(recipeCompileState{})
	forbidden := map[reflect.Type]string{
		reflect.TypeOf((*Job)(nil)):            "*Job",
		reflect.TypeOf((*TranscodeJob)(nil)):   "*TranscodeJob",
		reflect.TypeOf((*jobStreamBuild)(nil)): "*jobStreamBuild",
		reflect.TypeOf([]streamBuild(nil)):     "[]streamBuild",
	}
	for i := 0; i < stateType.NumField(); i++ {
		field := stateType.Field(i)
		if name, ok := forbidden[field.Type]; ok {
			t.Fatalf("recipeCompileState field %s carries %s; compiler passes should use captured intent attachments", field.Name, name)
		}
		switch field.Name {
		case "inputs", "outputs", "jobOutputs", "streamOutputs", "transcodeInput", "transcodeOutputs":
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
				outputAttachments: []OutputSpec{FileOutput("recording.ivf", io.Discard)},
			},
			want: "inputs",
		},
		{
			name: "job outputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Outputs: []OutputIntent{{Name: "recording.ivf"}}},
				inputAttachments:  []InputSpec{FileInput("input.ivf", strings.NewReader(""))},
				outputAttachments: nil,
			},
			want: "outputs",
		},
		{
			name: "transcode outputs",
			state: recipeCompileState{
				operation:                  transcodeRecipeOperation,
				transcodePresent:           true,
				intent:                     Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Outputs: []OutputIntent{{Name: "web.ivf"}}},
				transcodeInputAttachment:   FileInput("input.ivf", strings.NewReader("")),
				transcodeOutputAttachments: nil,
			},
			want: "outputs",
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
				!strings.Contains(err.Error(), "goav.Record") {
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
					Outputs: []OutputIntent{{Name: "recording.ivf"}},
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
						{Name: "audio", Decode: true, RouteTo: []string{"audio"}},
						{Name: "video", Decode: true, RouteTo: []string{"video"}},
					},
					Outputs: []OutputIntent{{Name: "audio"}, {Name: "video"}},
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
						RouteTo: []string{"frames"},
					}},
					Outputs: []OutputIntent{{Name: "archive.ivf"}, {Name: "frames"}},
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
						RouteTo: []string{"frames"},
					}},
					Outputs: []OutputIntent{{Name: "frames"}},
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
				RouteTo: []string{"missing"},
			}},
			Outputs: []OutputIntent{{Name: "archive.ogg"}},
		},
		outputAttachments: []OutputSpec{
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
				outputAttachments: []OutputSpec{
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
				outputAttachments: []OutputSpec{
					FileOutput("", io.Discard).Format(av.FormatOgg),
				},
			},
			want: `format "ogg"`,
		},
		{
			name: "transcode probed format",
			pass: validateTranscodeOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   Default(),
				transcodeOutputAttachments: []namedOutputSpec{{
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
			if !errors.As(err, &buildErr) || buildErr.Code != "output_muxer_missing" || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want output_muxer_missing wrapping format.ErrNotFound", err)
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
				outputAttachments: []OutputSpec{
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
			pass: validateTranscodeOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				transcodeOutputAttachments: []namedOutputSpec{{
					name:   "web",
					output: FileOutput("web.ogg", io.Discard),
				}},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.transcodeOutputAttachments) != 1 ||
					state.transcodeOutputAttachments[0].output.format != "" ||
					state.transcodeOutputAttachments[0].output.resolvedFormat != av.FormatOgg ||
					state.transcodeOutputAttachments[0].output.output.Name != "web.ogg" {
					t.Fatalf("transcode output attachments = %+v, want resolved Ogg format", state.transcodeOutputAttachments)
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
				outputAttachments: []OutputSpec{
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

func TestResolvedJobOutputFormatsLowerIntoBuilder(t *testing.T) {
	runtime := New(
		WithDefaults(),
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)
	job := Record(
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
		FileOutput("recording.ogg", io.Discard),
	).UseRuntime(runtime)

	resolved, err := compileJobRecipeForBuild(job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuild() error = %v", err)
	}
	builder := resolved.migration
	if builder == nil {
		t.Fatal("migration builder is nil")
	}
	if got := builder.outputOpenFormat(0); got != av.FormatOgg {
		t.Fatalf("builder open output format = %q, want resolved Ogg format", got)
	}
	if got := builder.outputFormat(0); got != "" {
		t.Fatalf("builder graph detail output format = %q, want inferred format hidden from graph detail", got)
	}
}

func TestResolvedTranscodeOutputFormatsEnterPlan(t *testing.T) {
	state := recipeCompileState{
		operation: transcodeRecipeOperation,
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
				RouteTo: []string{"archive"},
			}},
			Outputs: []OutputIntent{{Name: "archive"}},
		},
		transcodeInputAttachment: FileInput("input.ivf", strings.NewReader("")),
		transcodeOutputAttachments: []namedOutputSpec{{
			name:   "archive",
			output: FileOutput("archive.ogg", io.Discard),
		}},
	}

	if err := validateTranscodeOutputFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("validateTranscodeOutputFormatAdaptersPass() error = %v", err)
	}
	if err := planTranscodeIntentPass().Apply(&state); err != nil {
		t.Fatalf("planTranscodeIntentPass() error = %v", err)
	}
	if len(state.plan.Outputs) != 1 ||
		state.plan.Outputs[0].Format != "" ||
		state.plan.Outputs[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("plan outputs = %+v, want resolved Ogg open format without graph detail format", state.plan.Outputs)
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
			pass: validateTranscodeInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation:                transcodeRecipeOperation,
				options:                  recipeCompileOptions{preflightInputAdapters: true},
				runtime:                  Default(),
				transcodeInputAttachment: FileInput("input.webm", strings.NewReader("")),
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
			want:  []string{"no decoder adapter", "codec=opus", "goav.Record"},
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
			want:  []string{"no decoder adapter", "codec=opus", "goav.Record"},
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
			pass: validateTranscodeKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:    "360p",
					Select:  StreamSelect{Type: av.MediaVideo},
					Encode:  VP9(Bitrate(600_000)),
					RouteTo: []string{"web"},
				}}},
				transcodeInputProbeReady: true,
				transcodeInputProbe: format.ProbeResult{
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
			want:  []string{"no decoder adapter", "codec=vp9", "goav.Record"},
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
			want:  []string{"no encoder adapter", "codec=opus", "FrameSink"},
		},
		{
			name: "transcode descriptor-only encoder",
			pass: validateTranscodeEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
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
			pass: validateTranscodeTransformAdaptersPass(),
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
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

func TestJobStreamOutputKindsPassRejectsInvalidOutputShapes(t *testing.T) {
	frameSink := FrameSink(SinkFunc("frames", func(context.Context, Message) error { return nil }))
	fileOutput := FileOutput("archive.ogg", io.Discard)
	tests := []struct {
		name    string
		stream  StreamIntent
		outputs []OutputSpec
		code    string
		want    []string
	}{
		{
			name: "mixed frame and mux outputs",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				RouteTo: []string{"frames", "archive.ogg"},
			},
			outputs: []OutputSpec{frameSink, fileOutput},
			code:    "output_kind_mixed",
			want:    []string{"cannot mix frame sinks and muxed outputs", "goav.Transcode"},
		},
		{
			name: "mux output without encoder",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				RouteTo: []string{"archive.ogg"},
			},
			outputs: []OutputSpec{fileOutput},
			code:    "encode_missing",
			want:    []string{"decoded frames cannot be written", ".Opus"},
		},
		{
			name: "encoded output to frame sink",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				Encode:  Opus(Bitrate(96_000)),
				RouteTo: []string{"frames"},
			},
			outputs: []OutputSpec{frameSink},
			code:    "encoded_sink_unsupported",
			want:    []string{"encoded packets", "FileOutput"},
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
					Outputs: []OutputIntent{{Name: "unused"}},
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
				RouteTo:     []string{"frames"},
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
				RouteTo:    []string{"frames"},
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
					Outputs: []OutputIntent{{Name: "frames"}},
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

func TestMigrationGraphCompilerPassWrapsUnsupportedRecipeShape(t *testing.T) {
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

	err := selectMigrationGraphCompilerPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "recipe_graph_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want recipe_graph_unsupported wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{"recipe intent", "inputs: 1", "outputs: 0", "goav.Record", "goav.Transcode"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if state.compiler != nil || state.migration != nil {
		t.Fatalf("state compiler=%T migration=%T, want unset after unsupported selection", state.compiler, state.migration)
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
						RouteTo: []string{"frames"},
					}},
					Outputs: []OutputIntent{{Name: "frames"}},
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
						RouteTo:    []string{"frames"},
					}},
					Outputs: []OutputIntent{{Name: "frames"}},
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
				RouteTo:    []string{"frames"},
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
				RouteTo:    []string{"frames"},
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
				RouteTo:    []string{"frames"},
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
					Outputs: []OutputIntent{{Name: "frames"}},
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
				operation: transcodeRecipeOperation,
				intent: Intent{
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						RouteTo: []string{"web"},
					}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "stream missing",
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
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
				operation: transcodeRecipeOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						RouteTo: []string{"web"},
					}},
				},
			},
			code: "stream_name_missing",
			want: "transcode branches need stable names",
		},
		{
			name: "encode missing",
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						RouteTo: []string{"web"},
					}},
				},
			},
			code: "encode_missing",
			want: "stream has no codec target",
		},
		{
			name: "duplicate branch output",
			state: recipeCompileState{
				operation: transcodeRecipeOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						RouteTo: []string{"web", "web"},
					}},
				},
			},
			code: "output_duplicate",
			want: "more than once",
		},
	}
	pass := validateTranscodeIntentShapePass()
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
				transcodeInputAttachment: RTP(&runtimeRTPReceiver{}).Name("video").Codec(VP8()),
				transcodeOutputAttachments: []namedOutputSpec{{
					name:   "web",
					output: FileOutput("web.ivf", io.Discard),
				}},
			},
			code: "unsupported_input",
			want: "RTP transcode recipes",
		},
		{
			name: "frame sink output",
			state: recipeCompileState{
				transcodeInputAttachment: FileInput("input.ivf", strings.NewReader("")),
				transcodeOutputAttachments: []namedOutputSpec{{
					name:   "frames",
					output: FrameSink(SinkFunc("frames", func(context.Context, Message) error { return nil })),
				}},
			},
			code: "output_kind_invalid",
			want: "transcode outputs are muxed output groups",
		},
		{
			name: "duplicate output labels",
			state: recipeCompileState{
				transcodeInputAttachment: FileInput("input.ivf", strings.NewReader("")),
				transcodeOutputAttachments: []namedOutputSpec{
					{name: "web", output: FileOutput("web.ivf", io.Discard)},
					{name: "web", output: FileOutput("preview.ivf", io.Discard)},
				},
			},
			code: "output_duplicate",
			want: "defined more than once",
		},
	}
	pass := validateTranscodeAttachmentsPass()
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

func TestTranscodeOutputBindingsPassRejectsUndefinedRoutes(t *testing.T) {
	state := recipeCompileState{
		operation: transcodeRecipeOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "360p",
				Select:  StreamSelect{Type: av.MediaVideo},
				Encode:  VP9(Bitrate(600_000)),
				RouteTo: []string{"missing"},
			}},
			Outputs: []OutputIntent{{Name: "web.ivf"}},
		},
		transcodeOutputAttachments: []namedOutputSpec{{
			name:   "web",
			output: FileOutput("web.ivf", io.Discard),
		}},
	}

	err := validateTranscodeOutputBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "output missing is referenced but not defined") ||
		!strings.Contains(err.Error(), "define shared outputs once") {
		t.Fatalf("err = %v, want output binding guidance", err)
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
		operation: transcodeRecipeOperation,
		intent: Intent{Streams: []StreamIntent{{
			Name:    "720p",
			Select:  StreamSelect{Type: av.MediaVideo},
			Encode:  VP9(Bitrate(2_000_000)),
			RouteTo: []string{"web"},
		}}},
		transcodeInputProbeReady: true,
		transcodeInputProbe: format.ProbeResult{
			Format:  av.FormatMatroska,
			Streams: streams,
		},
	}

	err := validateTranscodeKnownInputStreamSelectionPass().Apply(&state)
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

func TestCompileJobRecipeCarriesIntentAndBuilder(t *testing.T) {
	job := Record(
		FileInput("input.ivf", strings.NewReader("")),
		FileOutput("recording.ivf", io.Discard),
	)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.builder == nil {
		t.Fatal("compileJobRecipe() produced nil builder")
	}
	if resolved.compiler == nil || resolved.migration == nil {
		t.Fatal("compileJobRecipe() did not select a migration graph compiler")
	}
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "record" {
		t.Fatalf("intent name = %q, want record", resolved.intent.Name)
	}
	if len(resolved.intent.Inputs) != 1 || resolved.intent.Inputs[0].Name != "input.ivf" {
		t.Fatalf("intent inputs = %+v", resolved.intent.Inputs)
	}
	if len(resolved.intent.Outputs) != 1 || resolved.intent.Outputs[0].Name != "recording.ivf" {
		t.Fatalf("intent outputs = %+v", resolved.intent.Outputs)
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

func TestCompileTranscodeRecipeCarriesIntentAndPlan(t *testing.T) {
	job := Transcode(FileInput("input.ivf", strings.NewReader(""))).
		Video("360p").Resize(640, 360).VP9(600_000).To("web").
		Output("web", FileOutput("web.ivf", io.Discard))

	resolved, err := compileTranscodeRecipe(job)
	if err != nil {
		t.Fatalf("compileTranscodeRecipe() error = %v", err)
	}
	builder, ok := resolved.builder.(*builder)
	if !ok {
		t.Fatalf("resolved builder type = %T, want *builder", resolved.builder)
	}
	if len(builder.transcodes) != 1 {
		t.Fatalf("builder transcodes = %d, want 1", len(builder.transcodes))
	}
	if resolved.compiler == nil || resolved.migration == nil {
		t.Fatal("compileTranscodeRecipe() did not select a migration graph compiler")
	}
	if !resolved.specReady {
		t.Fatal("compileTranscodeRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "transcode" {
		t.Fatalf("intent name = %q, want transcode", resolved.intent.Name)
	}
	if len(resolved.intent.Streams) != 1 || resolved.intent.Streams[0].Name != "360p" {
		t.Fatalf("intent streams = %+v", resolved.intent.Streams)
	}
	if got := resolved.intent.Streams[0].RouteTo; len(got) != 1 || got[0] != "web" {
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

func TestRecipeResolvedBuildUsesPlannedCompiler(t *testing.T) {
	job := Record(
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
		FileOutput("recording.ivf", io.Discard),
	)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
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
