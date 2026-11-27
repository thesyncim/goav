package goav

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/internal/stage"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// --- Consolidated from naming_timing_contract_test.go ---

func TestFrameDurationNSContracts(t *testing.T) {
	if got := stage.FrameDurationNS(&av.Frame{Duration: av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}}); got != int64(20*time.Millisecond) {
		t.Fatalf("explicit frame duration = %d", got)
	}
	if got := stage.FrameDurationNS(&av.Frame{Audio: &av.AudioFrame{SampleRate: 48000, Samples: 960}}); got != int64(20*time.Millisecond) {
		t.Fatalf("audio-derived frame duration = %d", got)
	}
	if got := stage.FrameDurationNS(&av.Frame{}); got != 0 {
		t.Fatalf("empty frame duration = %d, want 0", got)
	}
}

// --- Consolidated from recipe_wrapper_contract_test.go ---

func TestRecipeResolvedUnsupportedContracts(t *testing.T) {
	var resolved recipeResolved
	if _, err := resolved.Describe(); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "describe recipe") {
		t.Fatalf("Describe() error = %v, want recipe_graph_unsupported describe error", err)
	}
	if _, err := resolved.Build(context.Background()); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "build recipe") {
		t.Fatalf("Build() error = %v, want recipe_graph_unsupported build error", err)
	}
	if work := resolved.workIR(); work.Name != "" || len(work.Operations) != 0 || len(work.Branches) != 0 {
		t.Fatalf("workIR() = %+v, want empty work plan", work)
	}
}

// --- Consolidated from join_select_helper_contract_test.go ---

func TestJoinStagePreallocDepthContracts(t *testing.T) {
	if got := joinStagePreallocDepth(nil); got != 1 {
		t.Fatalf("nil runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew()); got != 1 {
		t.Fatalf("direct runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Drop: pipeline.DropOldest}))); got != 1 {
		t.Fatalf("zero-capacity buffered prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 128, Drop: pipeline.DropOldest}))); got != joinStagePreallocDepthLimit {
		t.Fatalf("large buffered prealloc depth = %d, want cap %d", got, joinStagePreallocDepthLimit)
	}
}

// --- Consolidated from tap_work_helper_contract_test.go ---

func TestTaskTapsDoNotInferFromGraphNodePrefixes(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "tap-prefix-regression"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(&runtimeTestSource{name: "select-audio"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(&runtimeTestStage{name: "decode-audio"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(&runtimeTestSink{name: "out"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}

	task := &Task{graph: graph}
	if taps := task.Taps(); len(taps) != 0 {
		t.Fatalf("Taps() = %+v, want no prefix-inferred taps", taps)
	}
}

// --- Consolidated from multi_input_contract_test.go ---

func TestDuplicateInputNameErrorContract(t *testing.T) {
	err := duplicateInputNameError("mic", 0, 2)
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want errUnsupportedBuild cause", err)
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Node != "mic" {
		t.Fatalf("node = %q, want mic", buildErr.Node)
	}
	if !reflect.DeepEqual(buildErr.DetailLines(), []string{"first input index: 0", "second input index: 2"}) {
		t.Fatalf("details = %#v", buildErr.DetailLines())
	}

	source := func(context.Context, SourcePush) error { return nil }
	err = validateInputNames([]Input{
		Source("mic", shape.Frame(av.MediaAudio), source),
		Source("camera", shape.Frame(av.MediaVideo), source),
		Source("mic", shape.Frame(av.MediaAudio), source),
	})
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !strings.Contains(err.Error(), "input name \"mic\" is defined more than once") {
		t.Fatalf("err = %v, want duplicate name reason", err)
	}
}

// --- Consolidated from shape_solver_helper_contract_test.go ---

func TestShapeSolverDiagnosticHelperContracts(t *testing.T) {
	labelTests := []struct {
		name      string
		operation recipeir.Operation
		want      string
	}{
		{name: "encode id", operation: opEncode(codec.Opus()), want: "encode-opus"},
		{name: "encode component", operation: recipeir.Operation{Kind: plan.OpEncode, Component: "custom-encoder"}, want: "encode-custom-encoder"},
		{name: "encode fallback", operation: recipeir.Operation{Kind: plan.OpEncode}, want: "encode-encoder"},
		{name: "decode id", operation: recipeir.Operation{Kind: plan.OpDecode, Component: "vp8-decoder", Decode: codec.VP8()}, want: "decode-vp8"},
		{name: "decode component", operation: recipeir.Operation{Kind: plan.OpDecode, Component: "custom-decoder"}, want: "decode-custom-decoder"},
		{name: "stage component", operation: recipeir.Operation{Kind: plan.OpStage, Component: "meter"}, want: "stage-meter"},
		{name: "stage fallback", operation: recipeir.Operation{Kind: plan.OpStage}, want: "stage-custom"},
		{name: "transform", operation: opTransform(resampleSpec(48_000, codec.Stereo)), want: "resample"},
		{name: "default component", operation: recipeir.Operation{Kind: plan.OpTap, Component: "tap"}, want: "tap"},
		{name: "default kind", operation: recipeir.Operation{Kind: plan.OpTap}, want: "tap"},
		{name: "default fallback", operation: recipeir.Operation{}, want: "operation"},
	}
	for _, tt := range labelTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compile.RecipeIROperationLabel(tt.operation); got != tt.want {
				t.Fatalf("recipeIROperationLabel = %q, want %q", got, tt.want)
			}
		})
	}

	if got, want := compile.RecipeIRExplicitConversionSuggestion(resampleSpec(16_000, codec.Mono), opEncode(codec.Opus())),
		[]string{"insert .Resample(16000, 1) explicitly before encode-opus"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resample suggestion = %#v, want %#v", got, want)
	}
	if got, want := compile.RecipeIRExplicitConversionSuggestion(resizeSpec(640, 360), recipeir.Operation{}),
		[]string{"insert .Resize(640, 360) explicitly"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resize suggestion = %#v, want %#v", got, want)
	}
	if got := compile.RecipeIRExplicitConversionSuggestion(recipeir.Transform{}, opDecode(codec.Opus())); got != nil {
		t.Fatalf("empty transform suggestion = %#v, want nil", got)
	}
}

// --- Consolidated from package_name_contract_test.go ---

func TestRuntimeAndControlHostPackageNamesStayClear(t *testing.T) {
	modulePath := "github.com/thesyncim/goav"
	forbidImport := map[string]string{
		modulePath + "/runtime": "use github.com/thesyncim/goav/runconfig",
		modulePath + "/ctl":     "use github.com/thesyncim/goav/ctlserver",
	}
	forbidPackage := map[string]string{
		"runtime": "use package runconfig for runtime options",
		"ctl":     "use package ctlserver for the socket control-plane host",
	}

	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".idea", ".claude", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		if fix, ok := forbidPackage[file.Name.Name]; ok {
			t.Errorf("%s declares package %q; %s", path, file.Name.Name, fix)
		}
		for _, imported := range file.Imports {
			pathValue := strings.Trim(imported.Path.Value, "\"")
			if fix, ok := forbidImport[pathValue]; ok {
				t.Errorf("%s imports %s; %s", path, pathValue, fix)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- Consolidated from transform_helper_contract_test.go ---

func TestTransformSpecConstructorsApplyOptions(t *testing.T) {
	resize := resizeSpec(640, 360, nil, func(config *filter.ResizeConfig) {
		config.PixelFormat = av.PixelFormatI420
		config.Mode = filter.ResizeFit
	})
	if resize.Kind != recipeir.TransformResize ||
		resize.Resize.Width != 640 ||
		resize.Resize.Height != 360 ||
		resize.Resize.PixelFormat != av.PixelFormatI420 ||
		resize.Resize.Mode != filter.ResizeFit {
		t.Fatalf("Resize spec = %+v", resize)
	}

	resample := resampleSpec(48_000, codec.Stereo, nil, func(config *filter.ResampleConfig) {
		config.ChannelLayout = "stereo"
		config.SampleFormat = av.SampleFormatF32
	})
	if resample.Kind != recipeir.TransformResample ||
		resample.Resample.SampleRate != 48_000 ||
		resample.Resample.Channels != codec.Stereo ||
		resample.Resample.ChannelLayout != "stereo" ||
		resample.Resample.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("Resample spec = %+v", resample)
	}
}

func TestTransformHelperErrorContracts(t *testing.T) {
	err := validateTransformSpec("build stream", "empty", recipeir.Transform{})
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != transformInvalidCode ||
		!strings.Contains(buildErr.Reason, "empty stream transform") {
		t.Fatalf("empty transform validation = %v, want transform_invalid", err)
	}

	cause := errors.New("registry unavailable")
	stream := recipeir.Stream{Name: "voice", Selector: compile.StreamSelectFromAV(av.StreamSelector{Type: av.MediaAudio})}
	if err := publicError(compile.RecipeTransformAdapterError("build stream", stream, filter.FactoryResample, cause)); !errors.Is(err, cause) {
		t.Fatalf("non-not-found error = %v, want original cause", err)
	}
	err = publicError(compile.RecipeTransformAdapterError("build stream", stream, filter.FactoryResample, filter.ErrNotFound))
	var missingErr *BuildError
	if !errors.As(err, &missingErr) ||
		missingErr.Code != transformAdapterMissingCode ||
		missingErr.Node != "voice" ||
		!strings.Contains(missingErr.Reason, "no resample filter adapter is registered") ||
		!strings.Contains(strings.Join(missingErr.FixLines(), "\n"), ".Resample") {
		t.Fatalf("missing adapter error = %#v", err)
	}

	mediaErr := transformMediaError("voice", "resize", av.MediaVideo, av.MediaAudio)
	if !strings.Contains(mediaErr.Error(), shape.Frame(av.MediaVideo).String()) ||
		!strings.Contains(mediaErr.Error(), shape.Frame(av.MediaAudio).String()) {
		t.Fatalf("media error = %v", mediaErr)
	}
}
