package goavtest_test

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
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

// ExampleCollector_WaitFrames shows the common count-based wait helper used by
// live and control-driven pipeline tests.
func ExampleCollector_WaitFrames() {
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{1}, []int16{2})).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	frames, err := out.WaitFrames(ctx, 2)
	fmt.Println(len(frames), err)
	// Output: 2 <nil>
}

// ExampleTestSourceScript shows a provider-shaped fixture with a mixed script:
// frame data plus an in-band event, both delivered through the source seam.
func ExampleTestSourceScript() {
	ctx := context.Background()
	frame := &av.Frame{
		Type: av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate:   8000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
			Samples:      1,
		},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: []byte{7, 0}, Ownership: av.BufferImmutable},
		}},
	}
	source := goavtest.NewTestSource("fixture",
		shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16), shape.Codec(av.CodecPCM)),
		goavtest.TestSourceScript(
			goavtest.TestSourceFrame(frame),
			goavtest.TestSourceEvent(av.Event{Type: av.EventStats, Reason: "ready"}),
		),
	)
	opened, _, err := source.OpenSource(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	emitter := &exampleScriptEmitter{}
	err = opened.Start(ctx, emitter)
	fmt.Println("err:", err)
	fmt.Println("frames:", emitter.frames)
	fmt.Println("events:", emitter.events, emitter.firstReason)
	// Output:
	// err: <nil>
	// frames: 1
	// events: 2 ready
}

type exampleScriptEmitter struct {
	frames      int
	events      int
	firstReason string
}

func (e *exampleScriptEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	if msg.Frame != nil {
		e.frames++
	}
	if msg.Event != nil {
		e.events++
		if e.firstReason == "" {
			e.firstReason = msg.Event.Reason
		}
	}
	return nil
}
