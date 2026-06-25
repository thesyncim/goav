package component

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestStrictStageConstructorsRejectNilCallbacks(t *testing.T) {
	for _, tt := range []struct {
		name string
		make func() (pipeline.Stage, error)
	}{
		{
			name: "packet",
			make: func() (pipeline.Stage, error) {
				return NewPacketStage("packet", nil)
			},
		},
		{
			name: "frame",
			make: func() (pipeline.Stage, error) {
				return NewFrameStage("frame", nil)
			},
		},
		{
			name: "event",
			make: func() (pipeline.Stage, error) {
				return NewEventStage("event", nil)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stage, err := tt.make()
			if stage != nil {
				t.Fatalf("stage = %T, want nil", stage)
			}
			if !errors.Is(err, ErrNilStageCallback) {
				t.Fatalf("err = %v, want ErrNilStageCallback", err)
			}
		})
	}
}

func TestStrictSinkConstructorRejectsNilCallback(t *testing.T) {
	sink, err := NewSink("sink", nil)
	if sink != nil {
		t.Fatalf("sink = %T, want nil", sink)
	}
	if !errors.Is(err, ErrNilSinkCallback) {
		t.Fatalf("err = %v, want ErrNilSinkCallback", err)
	}
}

func TestStrictConstructorsReturnValidComponents(t *testing.T) {
	packetStage, err := NewPacketStage("packet", func(context.Context, *av.Packet, Emit) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if packetStage.Name() != "packet" {
		t.Fatalf("packet stage name = %q, want packet", packetStage.Name())
	}

	sink, err := NewSink("sink", func(context.Context, Message) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.Name() != "sink" {
		t.Fatalf("sink name = %q, want sink", sink.Name())
	}
}

func TestMustConstructorsPanicOnInvalidComponent(t *testing.T) {
	assertPanicIs(t, ErrNilStageCallback, func() {
		MustStage(NewPacketStage("packet", nil))
	})
	assertPanicIs(t, ErrNilSinkCallback, func() {
		MustSink(NewSink("sink", nil))
	})
}

func assertPanicIs(t *testing.T, want error, fn func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("panic = nil, want %v", want)
		}
		err, ok := recovered.(error)
		if !ok || !errors.Is(err, want) {
			t.Fatalf("panic = %v, want %v", recovered, want)
		}
	}()
	fn()
}
