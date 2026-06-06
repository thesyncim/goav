package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
)

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
