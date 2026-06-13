package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/goavtest/expect"
)

func TestRunCustomFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runCustomFilter(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "frames", got, [][]int16{{1, 1, 2, 2}, {3, 3}})
	expect.GoldenString(t, "testdata/expected.txt", fmt.Sprintln("frames:", got))
}
