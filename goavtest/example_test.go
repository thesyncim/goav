package goavtest_test

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/goavtest"
)

// ExampleAudio runs the smallest possible pipeline test: a deterministic
// audio source straight into a collector, asserting on the samples.
func ExampleAudio() {
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{1, 2}, []int16{3})).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(out.S16())
	// Output: [[1 2] [3]]
}

// ExampleCollector mixes two deterministic inputs and reads the summed
// samples back — test code is pipeline code.
func ExampleCollector() {
	ctx := context.Background()
	out := goavtest.NewCollector()
	task, err := goav.Mix(
		goav.From(goavtest.Audio(48000, 1, []int16{100}, []int16{200})).Audio(),
		goav.From(goavtest.Audio(48000, 1, []int16{50}, []int16{-50})).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(out.S16())
	// Output: [[150] [150]]
}
