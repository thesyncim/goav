// Cold-path pins for the branch grammar: the refusals and route-narrowing
// methods no headline test exercises. Each refusal is pinned through the
// public grammar (the construction a user would actually write), and the
// .Stream/.Event narrowing is proven on a live runtime attach — routed
// delivery, not just spec plumbing.

package goav

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// coldPathPacketSource pushes one opus packet per call and ends.
func coldPathPacketSource(id av.StreamID, payloads ...[]byte) InputSpec {
	return Source(string(id),
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			for i := range payloads {
				p := av.Packet{StreamID: id, Type: av.MediaAudio, Payload: av.Buffer{Bytes: payloads[i], Ownership: av.BufferImmutable}}
				if _, err := push.Packet(&p); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func requireColdPathBuildError(t *testing.T, err error, code errcode.Code) *BuildError {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *BuildError", err)
	}
	if buildErr.Code != code {
		t.Fatalf("code = %q, want %q\nerr = %v", buildErr.Code, code, err)
	}
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want it to wrap ErrUnsupportedBuild", err)
	}
	return buildErr
}

// TestBranchesRefuseCopyParentAfterDecode pins the packet-domain rule on the
// parent: .Decode().Copy().Branches(...) asks branches to preserve packets
// from a frame-domain point and is refused at the Branches call.
func TestBranchesRefuseCopyParentAfterDecode(t *testing.T) {
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Decode().Copy().
		Branches(Branch("rec").To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))).
		Describe()
	requireColdPathBuildError(t, err, errcode.CopyBranchSourceInvalid)
}

// TestBranchRefusesSecondDecode pins the duplicate-decode refusal on the
// branch builder itself.
func TestBranchRefusesSecondDecode(t *testing.T) {
	spec := Branch("voice").Decode().Decode().
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Copy().
		Branches(spec).
		Describe()
	requireColdPathBuildError(t, err, errcode.BranchDecodeDuplicate)
}

// TestBranchesRefusePacketBranchEncode pins the packet-domain encode rule:
// a planned branch on a .Copy() parent cannot encode without decoding first.
func TestBranchesRefusePacketBranchEncode(t *testing.T) {
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Copy().
		Branches(Branch("web").Encode(codec.Opus(codec.Bitrate(96_000))).
			To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))).
		Describe()
	requireColdPathBuildError(t, err, errcode.PacketBranchEncodeUnsupported)
}

// TestNilBranchBuilderToIsRefused pins the nil-builder refusal: a nil
// *branchBuilder (a zeroed variable of the inferred builder type) still
// finishes into a spec that fails the build instead of panicking.
func TestNilBranchBuilderToIsRefused(t *testing.T) {
	var builder *branchBuilder // what `b := Branch("late"); b = nil` leaves behind
	spec := builder.To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Copy().
		Branches(spec).
		Describe()
	requireColdPathBuildError(t, err, errcode.BranchInvalid)
}

// TestBranchToRefusesEmptyDestination pins the empty-handle refusal on the
// branch: .To(goav.Destination{}) names the invalid destination.
func TestBranchToRefusesEmptyDestination(t *testing.T) {
	spec := Branch("rec").To(Destination{})
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Copy().
		Branches(spec).
		Describe()
	buildErr := requireColdPathBuildError(t, err, errcode.DestinationInvalid)
	if buildErr.Operation != "build branch" {
		t.Fatalf("operation = %q, want build branch", buildErr.Operation)
	}
}

// TestStreamToRefusesEmptyDestination pins the same refusal on the stream
// chain: .To(goav.Destination{}) is refused naming the stream.
func TestStreamToRefusesEmptyDestination(t *testing.T) {
	_, err := From(coldPathPacketSource("mic", []byte{1})).
		Audio().Copy().
		To(Destination{}).
		Describe()
	buildErr := requireColdPathBuildError(t, err, errcode.DestinationInvalid)
	if buildErr.Operation != "build stream" {
		t.Fatalf("operation = %q, want build stream", buildErr.Operation)
	}
}

// TestBranchesAnchorOnDeclaredAndDefaultPacketTaps pins the packet-anchor
// resolution for planned branches on a .Copy() parent: one branch anchors on
// the declared post-copy tap, one on the default packet tap name, and both
// receive the parent's packets end to end.
func TestBranchesAnchorOnDeclaredAndDefaultPacketTaps(t *testing.T) {
	ctx := context.Background()
	var declared, fallback int
	task, err := From(coldPathPacketSource("mic", []byte{1}, []byte{2})).
		Audio().Copy().Tap(PacketTap("recorded")).
		Branches(
			Branch("a").From(PacketTap("recorded")).
				To(Sink(SinkFunc("a", func(_ context.Context, msg Message) error {
					if msg.Kind == pipeline.MessagePacket {
						declared++
					}
					return nil
				}))),
			Branch("b").From(PacketTap("audio.packets")).
				To(Sink(SinkFunc("b", func(_ context.Context, msg Message) error {
					if msg.Kind == pipeline.MessagePacket {
						fallback++
					}
					return nil
				}))),
		).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if declared != 2 || fallback != 2 {
		t.Fatalf("declared tap branch=%d default tap branch=%d, want 2 packets each", declared, fallback)
	}
}

// coldPathRoutedSource emits one warm-up packet, waits for resume (the test
// attaches its branches in that window), then emits the routed phase:
// packets on streams a and b plus stats and packet-loss events.
type coldPathRoutedSource struct {
	name   string
	ready  chan struct{}
	resume chan struct{}
}

func (s *coldPathRoutedSource) Name() string { return s.name }

func (s *coldPathRoutedSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	warmup := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "a", Payload: av.Buffer{Bytes: []byte{0}}}}
	if err := emitter.Emit(ctx, &warmup); err != nil {
		return err
	}
	close(s.ready)
	select {
	case <-s.resume:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, msg := range []pipeline.Message{
		{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "a", Payload: av.Buffer{Bytes: []byte{1}}}},
		{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "b", Payload: av.Buffer{Bytes: []byte{2}}}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats, StreamID: "a"}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventPacketLoss, StreamID: "a"}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats, StreamID: "b"}},
	} {
		msg := msg
		if err := emitter.Emit(ctx, &msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *coldPathRoutedSource) Close() error { return nil }

// coldPathCountSink counts packets per stream id and events per type.
type coldPathCountSink struct {
	name    string
	packets map[av.StreamID]int
	events  map[av.EventType]int
}

func newColdPathCountSink(name string) *coldPathCountSink {
	return &coldPathCountSink{name: name, packets: map[av.StreamID]int{}, events: map[av.EventType]int{}}
}

func (s *coldPathCountSink) Name() string { return s.name }

func (s *coldPathCountSink) Handle(_ context.Context, msg *pipeline.Message) error {
	switch {
	case msg.Kind == pipeline.MessagePacket && msg.Packet != nil:
		s.packets[msg.Packet.StreamID]++
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil:
		s.events[msg.Event.Type]++
	}
	return nil
}

func (s *coldPathCountSink) Close() error { return nil }

// TestAttachedBranchStreamAndEventNarrowRoutes pins .Stream(...) and
// .Event(...) on the branch builder: a branch attached from a graph node with
// .Stream(id) receives only that stream's traffic — its packets AND its
// events, since events carry the stream they describe — and .Event(type)
// receives only that event type from any stream, while the base sink keeps
// receiving everything.
func TestAttachedBranchStreamAndEventNarrowRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	source := &coldPathRoutedSource{name: "feed", ready: make(chan struct{}), resume: make(chan struct{})}
	base := newColdPathCountSink("base")

	graph := expertGraph(MustNew())
	src := graph.Source("feed", source)
	baseNode := graph.Sink("base", base)
	graph.Connect(src.Out(), baseNode.In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	onlyA := newColdPathCountSink("only-a")
	stats := newColdPathCountSink("stats")
	if _, err := task.Attach(ctx,
		Branch("only-a").From(src).Stream("a").To(Sink(onlyA)),
		Branch("stats").From(src).Event(av.EventStats).To(Sink(stats)),
	); err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	if base.packets["a"] != 2 || base.packets["b"] != 1 {
		t.Fatalf("base packets = %+v, want a:2 b:1", base.packets)
	}
	if onlyA.packets["a"] != 1 || onlyA.packets["b"] != 0 {
		t.Fatalf("stream-narrowed branch packets = %+v, want only the post-attach a packet", onlyA.packets)
	}
	if onlyA.events[av.EventStats] != 1 || onlyA.events[av.EventPacketLoss] != 1 {
		t.Fatalf("stream-narrowed branch events = %+v, want stream a's stats and loss events only", onlyA.events)
	}
	if stats.events[av.EventStats] != 2 || stats.events[av.EventPacketLoss] != 0 || len(stats.packets) != 0 {
		t.Fatalf("event-narrowed branch = packets %+v events %+v, want both stats events and nothing else", stats.packets, stats.events)
	}
}
