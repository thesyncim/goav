package bundle

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func TestNilJobReturnsErrNilJob(t *testing.T) {
	if _, err := Describe(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("Describe(nil) error = %v, want ErrNilJob", err)
	}
	if _, err := Build(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("Build(nil) error = %v, want ErrNilJob", err)
	}
	if err := Run(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("Run(nil) error = %v, want ErrNilJob", err)
	}
}

func TestBuildSignaturesSplitNarrowAndLiveTask(t *testing.T) {
	var _ func(context.Context, *goav.Job, ...runconfig.Option) (goav.Task, error) = Build
}

func TestDescribeUsesBundledRuntimeForStructuralPlan(t *testing.T) {
	spec, err := Describe(context.Background(), goav.From(goavtest.Audio(48_000, 1, []int16{1, 2})).
		Audio().
		To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatalf("Describe() error = %v", err)
	}
	if spec.Name == "" || len(spec.Nodes) == 0 {
		t.Fatalf("Describe() spec = %+v, want planned graph nodes", spec)
	}
}

func TestDescribeHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Describe(ctx, goav.From(goavtest.Audio(48_000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Describe() error = %v, want context.Canceled", err)
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
