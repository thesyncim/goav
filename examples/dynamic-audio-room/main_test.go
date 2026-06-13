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

	result, err := runDynamicRoomDemo(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "per-track frames", result.PerTrack, map[string][][]int16{
		"host": {
			{100, 100},
			{100, 100},
			{100, 100},
			{100, 100},
			{100, 100},
		},
		"music": {
			{25, -50},
			{25, -50},
		},
		"guest": {
			{-10, 20},
			{-10, 20},
		},
	})
	expect.DeepEqual(t, "track meter counts", result.Meter, map[string]int{
		"host":  10,
		"music": 4,
		"guest": 4,
	})
	expect.DeepEqual(t, "mixed frames", result.Mixed, [][]int16{
		{100, 100},
		{125, 50},
		{115, 70},
		{90, 120},
		{100, 100},
	})
	expect.DeepEqual(t, "events", result.Events, []string{
		"stream_added:host",
		"stream_added:music",
		"stream_added:guest",
		"stream_removed:music",
		"stream_removed:guest",
		"stream_removed:host",
		"end_of_stream:room.control",
	})
	expect.GoldenString(t, "testdata/expected.txt",
		fmt.Sprintf("per_track: %v\nmeter: %v\nmixed: %v\nevents: %v\n",
			result.PerTrackSummary(), result.Meter, result.Mixed, result.Events))
}

func TestRoomRejectsFramesForInactiveParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := runRoomScript(ctx, func(ctx context.Context, room *RoomPipeline) error {
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

func TestOutputMixerClampsMixedOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := runRoomScript(ctx, func(ctx context.Context, room *RoomPipeline) error {
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
	expect.DeepEqual(t, "mixed frames", result.Mixed, [][]int16{{32767, -32768}})
}
