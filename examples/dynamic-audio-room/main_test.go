package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/goavtest/expect"
)

func TestRunDynamicRoomDemo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mixed, events, err := runDynamicRoomDemo(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "mixed frames", mixed, [][]int16{
		{100, 100},
		{125, 50},
		{115, 70},
		{90, 120},
		{100, 100},
	})
	expect.DeepEqual(t, "events", summarizeRoomEvents(events), []string{
		"stream_added:host",
		"stream_added:music",
		"stream_added:guest",
		"stream_removed:music",
		"stream_removed:guest",
		"stream_removed:host",
		"end_of_stream:room.mix",
	})
	expect.GoldenString(t, "testdata/expected.txt",
		fmt.Sprintf("mixed: %v\nevents: %v\n", mixed, summarizeRoomEvents(events)))
}

func TestRoomRejectsFramesForInactiveParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := runRoomScript(ctx, func(ctx context.Context, room *Room) error {
		if err := room.Join(ctx, "host"); err != nil {
			return err
		}
		return room.Push(ctx, map[string][]int16{
			"ghost": []int16{1},
		})
	})
	expect.Error(t, err)
	expect.Contains(t, "error", err.Error(), `unknown participant frame "ghost"`)
}

func TestRoomClampsMixedOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mixed, _, err := runRoomScript(ctx, func(ctx context.Context, room *Room) error {
		if err := room.Join(ctx, "host"); err != nil {
			return err
		}
		if err := room.Join(ctx, "music"); err != nil {
			return err
		}
		return room.Push(ctx, map[string][]int16{
			"host":  []int16{30000, -30000},
			"music": []int16{30000, -30000},
		})
	})
	expect.NoError(t, err)
	expect.DeepEqual(t, "mixed frames", mixed, [][]int16{{32767, -32768}})
}
