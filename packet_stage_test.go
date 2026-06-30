package goav_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/goavtest"
)

func packetStageInput() goav.InputSpec {
	return goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})
}

func passthroughPacketSink() goav.Destination {
	return goav.Sink(component.SinkFunc("out", func(context.Context, component.Message) error { return nil }))
}

// TestPacketCustomStageRunsOnPacketDomain pins that a packet-domain custom stage
// (PacketFunc) runs on a packet stream without a decode, and a later .Copy()
// stays valid — the operation contract says only frame stages need .Decode().
// A runtime branch attached from a packet tap is the exercised path.
func TestPacketCustomStageRunsOnPacketDomain(t *testing.T) {
	ctx := context.Background()
	meter := component.PacketFunc("meter", func(_ context.Context, p *av.Packet, emit component.Emit) error {
		return emit.Packet(p)
	})

	task, err := goav.From(packetStageInput()).
		Audio().
		Copy().
		Tap(goav.PacketTap("audio.packets")).
		To(passthroughPacketSink()).
		BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive: %v", err)
	}
	defer task.Close()

	if _, err := task.Attach(ctx,
		goav.Branch("meter").
			From(goav.PacketTap("audio.packets")).
			Do(meter).
			Copy().
			To(passthroughPacketSink()),
	); err != nil {
		t.Fatalf("attach packet .Do().Copy() branch: %v", err)
	}
}

// TestFrameCustomStageStillRequiresDecode pins that the fix did not weaken the
// helpful refusal: a frame stage (FrameFunc) on a packet stream still demands an
// explicit .Decode() first.
func TestFrameCustomStageStillRequiresDecode(t *testing.T) {
	frames := component.FrameFunc("frames", func(_ context.Context, f *av.Frame, emit component.Emit) error {
		return emit.Frame(f)
	})
	_, err := goav.From(packetStageInput()).
		Audio().
		Do(frames).
		To(passthroughPacketSink()).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError requiring Decode", err)
	}
	if !strings.Contains(strings.Join(buildErr.FixLines(), "\n"), ".Decode()") {
		t.Fatalf("fixes do not mention .Decode(): %v", buildErr.FixLines())
	}
}
