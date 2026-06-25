package bundle

import (
	"context"
	"errors"
	"testing"
)

func TestNilJobReturnsErrNilJob(t *testing.T) {
	if _, err := Build(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("Build(nil) error = %v, want ErrNilJob", err)
	}
	if err := Run(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("Run(nil) error = %v, want ErrNilJob", err)
	}
}
