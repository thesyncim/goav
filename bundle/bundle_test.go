package bundle

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func TestNoOutputsIsRefused(t *testing.T) {
	var buildErr *goav.BuildError
	if _, err := Describe(); !errors.As(err, &buildErr) || buildErr.Code != "stream_invalid" {
		t.Fatalf("Describe() error = %v, want stream_invalid", err)
	}
	if err := Run(context.Background()); !errors.As(err, &buildErr) || buildErr.Code != "stream_invalid" {
		t.Fatalf("Run() error = %v, want stream_invalid", err)
	}
}

func TestBuildReturnsLiveTask(t *testing.T) {
	var _ func(context.Context, ...goav.Output) (goav.LiveTask, error) = Build
}

func TestDefaultIsShared(t *testing.T) {
	if Default() != Default() {
		t.Fatal("Default() built a second runtime")
	}
}

func TestDescribeUsesBundledRuntimeForStructuralPlan(t *testing.T) {
	spec, err := Describe(goavtest.Audio(48_000, 1, []int16{1, 2}).Audio().To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatalf("Describe() error = %v", err)
	}
	if spec.Name == "" || len(spec.Nodes) == 0 {
		t.Fatalf("Describe() spec = %+v, want planned graph nodes", spec)
	}
}

// TestFamilyOptionsCompose pins the partial-runtime model: each family is one
// option, they compose with goav.New and with each other, and Options is the
// three together.
func TestFamilyOptionsCompose(t *testing.T) {
	for name, opts := range map[string][]runconfig.Option{
		"formats":  {Formats()},
		"codecs":   {Codecs()},
		"filters":  {Filters()},
		"all":      Options(),
		"repeated": {Filters(), Filters()},
	} {
		if _, err := goav.New(opts...); err != nil {
			t.Fatalf("%s: goav.New: %v", name, err)
		}
	}
	if got := len(Options()); got != 3 {
		t.Fatalf("Options() = %d options, want the three families", got)
	}
}
