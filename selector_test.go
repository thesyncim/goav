package goav

import (
	"context"
	"sync"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func selectorTestFrame(id av.StreamID, sample int16) *av.Frame {
	b := []byte{byte(sample), byte(sample >> 8)}
	return &av.Frame{
		StreamID: id,
		Type:     av.MediaAudio,
		Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
	}
}

func selectorTestEOS(id av.StreamID) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: id}}
}

func TestSelectorForwardsActiveInputOnly(t *testing.T) {
	sel := newSelectorStage("sel", []av.StreamID{"a", "b"}, "sel")
	emit := &mixTestEmitter{}
	ctx := context.Background()

	// Default active is inputs[0] == "a": "a" passes, "b" is dropped.
	if err := sel.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("a", 100)}, emit); err != nil {
		t.Fatal(err)
	}
	if err := sel.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("b", 200)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1 (only active 'a')", len(emit.frames))
	}
	if emit.frames[0].StreamID != "sel" {
		t.Fatalf("output stream=%s, want sel (rewritten)", emit.frames[0].StreamID)
	}
	if got := mixTestReadS16(emit.frames[0]); len(got) != 1 || got[0] != 100 {
		t.Fatalf("payload=%v, want [100] (the 'a' frame)", got)
	}

	// Switch to "b": now "b" passes and "a" is dropped.
	if err := sel.SetActive("b"); err != nil {
		t.Fatal(err)
	}
	if err := sel.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("a", 100)}, emit); err != nil {
		t.Fatal(err)
	}
	if err := sel.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("b", 200)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 2 {
		t.Fatalf("frames=%d, want 2 (active switched to 'b')", len(emit.frames))
	}
	if emit.frames[1].StreamID != "sel" {
		t.Fatalf("output stream=%s, want sel (rewritten)", emit.frames[1].StreamID)
	}
	if got := mixTestReadS16(emit.frames[1]); len(got) != 1 || got[0] != 200 {
		t.Fatalf("payload=%v, want [200] (the 'b' frame)", got)
	}
}

func TestSelectorForwardsActivePacketsOnly(t *testing.T) {
	sel := newSelectorStage("sel", []av.StreamID{"a", "b"}, "sel")
	emit := &mixTestEmitter{}
	ctx := context.Background()

	pkt := func(id av.StreamID) *pipeline.Message {
		return &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{
			StreamID: id, Type: av.MediaAudio,
			Payload: av.Buffer{Bytes: []byte{1, 2}, Ownership: av.BufferImmutable},
		}}
	}

	captured := &selectorPacketEmitter{}
	if err := sel.Handle(ctx, pkt("a"), captured); err != nil {
		t.Fatal(err)
	}
	if err := sel.Handle(ctx, pkt("b"), captured); err != nil {
		t.Fatal(err)
	}
	if len(captured.packets) != 1 {
		t.Fatalf("packets=%d, want 1 (only active 'a')", len(captured.packets))
	}
	if captured.packets[0].StreamID != "sel" {
		t.Fatalf("packet stream=%s, want sel (rewritten)", captured.packets[0].StreamID)
	}
	_ = emit
}

func TestSelectorRejectsUnknownActive(t *testing.T) {
	sel := newSelectorStage("sel", []av.StreamID{"a", "b"}, "sel")
	if err := sel.SetActive("c"); err == nil {
		t.Fatal("SetActive(c) accepted an unknown input, want error")
	}
	// A rejected SetActive must not change the active input.
	if sel.activeID() != "a" {
		t.Fatalf("active=%s after rejected SetActive, want a", sel.activeID())
	}
}

func TestSelectorEmitsEOSAfterAllInputsEnd(t *testing.T) {
	sel := newSelectorStage("sel", []av.StreamID{"a", "b"}, "sel")
	emit := &mixTestEmitter{}
	ctx := context.Background()

	if err := sel.Handle(ctx, selectorTestEOS("a"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 0 {
		t.Fatal("emitted output EOS before all inputs ended")
	}
	if err := sel.Handle(ctx, selectorTestEOS("b"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 1 || emit.events[0].Type != av.EventEndOfStream || emit.events[0].StreamID != "sel" {
		t.Fatalf("events=%+v, want one sel EOS", emit.events)
	}
}

func TestSelectorActiveStoreIsRaceClean(t *testing.T) {
	sel := newSelectorStage("sel", []av.StreamID{"a", "b"}, "sel")
	emit := &mixTestEmitter{}
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				_ = sel.SetActive("a")
			} else {
				_ = sel.SetActive("b")
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		if err := sel.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("a", 1)}, emit); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

type selectorPacketEmitter struct {
	packets []*av.Packet
}

func (c *selectorPacketEmitter) Emit(_ context.Context, m *pipeline.Message) error {
	if m.Kind == pipeline.MessagePacket {
		c.packets = append(c.packets, m.Packet)
	}
	return nil
}
