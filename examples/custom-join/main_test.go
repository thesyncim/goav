package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/goavtest/expect"
)

func TestRunCustomJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runCustomJoin(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "joined", got, [][]int16{{1}, {2}, {3}, {4}})
	expect.GoldenString(t, "testdata/expected.txt", fmt.Sprintln("joined:", got))
}
