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

func TestRunProviderSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	frames, src, err := runProviderSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]int16{{101, 102}, {103, 104}}
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %v, want %v", frames, want)
	}
	if src.opens != 1 || src.source.starts != 1 || src.source.closes != 1 {
		t.Fatalf("provider opens=%d starts=%d closes=%d", src.opens, src.source.starts, src.source.closes)
	}
	output := fmt.Sprintf("provider: %s\nframes: %v\nopened: %d started: %d closed: %d\n",
		src.Name(), frames, src.opens, src.source.starts, src.source.closes)
	if output != expectedOutput(t) {
		t.Fatalf("output = %q, want %q", output, expectedOutput(t))
	}
}

func TestBrokenProviderFailsBeforeRun(t *testing.T) {
	err := buildBrokenProvider(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want *goav.BuildError", err)
	}
	if buildErr.Code != errcode.InputInvalid {
		t.Fatalf("code = %s, want %s", buildErr.Code, errcode.InputInvalid)
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
