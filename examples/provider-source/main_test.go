package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest/expect"
)

func TestRunProviderSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	frames, src, err := runProviderSource(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "frames", frames, [][]int16{{101, 102}, {103, 104}})
	expect.Equal(t, "provider opens", src.opens, 1)
	expect.Equal(t, "source starts", src.source.starts, 1)
	expect.Equal(t, "source closes", src.source.closes, 1)
	output := fmt.Sprintf("provider: %s\nframes: %v\nopened: %d started: %d closed: %d\n",
		src.Name(), frames, src.opens, src.source.starts, src.source.closes)
	expect.GoldenString(t, "testdata/expected.txt", output)
}

func TestBrokenProviderFailsBeforeRun(t *testing.T) {
	err := buildBrokenProvider(context.Background())
	expect.BuildError(t, err, errcode.Code("input_invalid"),
		expect.Operation("build input"),
	)
}
