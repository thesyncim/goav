package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
)

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
