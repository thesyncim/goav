package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest/expect"
)

func TestRunCustomSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	frames, stats, err := runCustomSource(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "frames", frames, [][]int16{{10, 20}, {30, 40}})
	expect.Equal(t, "accepted", stats.Accepted, 2)
	expect.Equal(t, "dropped", stats.Dropped, 0)
	output := fmt.Sprintf("frames: %v\naccepted: %d dropped: %d\n", frames, stats.Accepted, stats.Dropped)
	expect.GoldenString(t, "testdata/expected.txt", output)
}

func TestBrokenCustomSourceFailsBeforeRun(t *testing.T) {
	err := buildBrokenCustomSource(context.Background())
	expect.BuildError(t, err, errcode.SourceCallbackMissing,
		expect.Operation("build input"),
		expect.Cause(goav.ErrNilSource),
	)
}
