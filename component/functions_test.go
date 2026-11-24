package component

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestFuncStagesRejectNilCallbacksOnValidate(t *testing.T) {
	for _, tt := range []struct {
		name  string
		stage pipeline.Stage
	}{
		{name: "packet", stage: PacketFunc("packet", nil)},
		{name: "frame", stage: FrameFunc("frame", nil)},
		{name: "event", stage: EventFunc("event", nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := tt.stage.(interface{ ValidateComponent() error })
			if !ok {
				t.Fatalf("stage %T does not implement ValidateComponent", tt.stage)
			}
			if err := v.ValidateComponent(); !errors.Is(err, ErrNilStageCallback) {
				t.Fatalf("err = %v, want ErrNilStageCallback", err)
			}
		})
	}
}

func TestSinkFuncRejectsNilCallbackOnValidate(t *testing.T) {
	sink := SinkFunc("sink", nil)
	v, ok := sink.(interface{ ValidateComponent() error })
	if !ok {
		t.Fatalf("sink %T does not implement ValidateComponent", sink)
	}
	if err := v.ValidateComponent(); !errors.Is(err, ErrNilSinkCallback) {
		t.Fatalf("err = %v, want ErrNilSinkCallback", err)
	}
}

func TestFuncConstructorsReturnValidComponents(t *testing.T) {
	packetStage := PacketFunc("packet", func(context.Context, *av.Packet, Emit) error {
		return nil
	})
	if packetStage.Name() != "packet" {
		t.Fatalf("packet stage name = %q, want packet", packetStage.Name())
	}

	sink := SinkFunc("sink", func(context.Context, Message) error {
		return nil
	})
	if sink.Name() != "sink" {
		t.Fatalf("sink name = %q, want sink", sink.Name())
	}
}
