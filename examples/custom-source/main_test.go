package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/errcode"
)

func TestRunCustomSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	frames, stats, err := runCustomSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]int16{{10, 20}, {30, 40}}
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %v, want %v", frames, want)
	}
	if stats.Accepted != 2 || stats.Dropped != 0 {
		t.Fatalf("stats = %+v, want accepted=2 dropped=0", stats)
	}
	if output := fmt.Sprintf("frames: %v\naccepted: %d dropped: %d\n", frames, stats.Accepted, stats.Dropped); output != expectedOutput(t) {
		t.Fatalf("output = %q, want %q", output, expectedOutput(t))
	}
}

func TestBrokenCustomSourceFailsBeforeRun(t *testing.T) {
	err := buildBrokenCustomSource(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want *goav.BuildError", err)
	}
	if buildErr.Code != errcode.SourceCallbackMissing {
		t.Fatalf("code = %s, want %s", buildErr.Code, errcode.SourceCallbackMissing)
	}
}

func expectedOutput(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("testdata/expected.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
