package goavtest

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

func TestTestSourceRecordsSourceControlsThroughRealTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
	)
	task, err := goav.From(source.Input()).
		Audio().Copy().
		To(NewCollector().Sink()).
		UseRuntime(Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if err := task.Control(ctx, goav.Rate(0.5).At("fixture")); err != nil {
		t.Fatal(err)
	}
	event, err := source.WaitControl(ctx, av.EventRate)
	if err != nil {
		t.Fatal(err)
	}
	if rate, ok := av.EventRateValue(&event); !ok || rate != 0.5 {
		t.Fatalf("rate control = %+v, parsed=%v ok=%v", event, rate, ok)
	}

	if err := task.Control(ctx, goav.Seek(12*time.Second).At("fixture")); err != nil {
		t.Fatal(err)
	}
	event, err = source.WaitControl(ctx, av.EventSeek)
	if err != nil {
		t.Fatal(err)
	}
	if position, ok := event.Timestamp.ToDuration(); !ok || position != 12*time.Second {
		t.Fatalf("seek control = %+v, parsed=%v ok=%v", event, position, ok)
	}
}
