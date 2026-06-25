package bundle

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/goavtest"
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
