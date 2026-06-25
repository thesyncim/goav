// The join symmetry proof: goav.Join makes N→1 convergence an extension seam,
// and this file is the executable claim. Two genuinely external convergence
// stages — an S16 frame interleaver and a first-arm passthrough that
// re-expresses Select's data-plane semantics — are defined entirely in this
// test package, lowered through the public grammar (decode arms, taps,
// branches, nesting inside a built-in Mix), and run end to end. Like the
// adapter proof, the compile-time claim IS the proof: only public goav
// packages are imported, so a third party can ship Crossfade(arms...) from
// another module with exactly this code shape. The stage contract each toy
// follows is documented on goav.Join (audio mix is the in-tree reference).
package adapterproof_test

import (
	"context"
	"encoding/binary"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// --- external join stage 1: an S16 frame interleaver ------------------------
//
// One frame per arm per round, in arm order — a zipper, not a sum, so the
// output proves a convergence behavior no built-in offers. Its shape.Contract
// declares frame-domain S16 mono 8kHz inputs, which is ALL the planner needs:
// packet arms get decode stages inserted (like Mix) and mismatched arms get
// solver conversions, with zero core knowledge of this type.

type interleaveStage struct {
	name    string
	arms    []av.StreamID
	pending map[av.StreamID][]*av.Frame
	eos     map[av.StreamID]struct{}
}

func newInterleaveStage(name string, arms ...av.StreamID) *interleaveStage {
	return &interleaveStage{
		name:    name,
		arms:    arms,
		pending: make(map[av.StreamID][]*av.Frame, len(arms)),
		eos:     make(map[av.StreamID]struct{}, len(arms)),
	}
}

func (s *interleaveStage) Name() string { return s.name }

// DescribeNode contributes the node detail; the planner mirrors it on the
// planned join node, so Describe() ≡ Build() includes the external detail.
func (s *interleaveStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: "external interleaver"}
}

// InputShapes/OutputShapes is the stage's shape.Contract — the seam the join
// planner derives every per-kind behavior from (decode arms, media kind, arm
// format solving, joined output stream).
func (s *interleaveStage) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *interleaveStage) OutputShapes(shape.Spec) shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *interleaveStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Audio == nil {
			return nil
		}
		s.pending[msg.Frame.StreamID] = append(s.pending[msg.Frame.StreamID], cloneProofFrame(msg.Frame))
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event == nil || msg.Event.Type != av.EventEndOfStream {
			return nil
		}
		s.eos[msg.Event.StreamID] = struct{}{}
		if err := s.drain(ctx, emitter); err != nil {
			return err
		}
		if len(s.eos) >= len(s.arms) {
			out := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.name)}
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		return nil
	default:
		return nil
	}
}

// drain emits one frame per arm per round in arm order; an ended arm stops
// gating, so the remaining arms keep zipping without it.
func (s *interleaveStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for s.roundReady() {
		for _, arm := range s.arms {
			queue := s.pending[arm]
			if len(queue) == 0 {
				continue
			}
			frame := queue[0]
			s.pending[arm] = queue[1:]
			frame.StreamID = av.StreamID(s.name)
			if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *interleaveStage) roundReady() bool {
	any := false
	for _, arm := range s.arms {
		if len(s.pending[arm]) != 0 {
			any = true
			continue
		}
		if _, ended := s.eos[arm]; !ended {
			return false
		}
	}
	return any
}

func (s *interleaveStage) Close() error { return nil }

// cloneProofFrame deep-copies a buffered frame: the join stage queues frames
// across Handle calls, so it must not retain upstream-owned payloads.
func cloneProofFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		bytes := append([]byte(nil), frame.Planes[i].Buffer.Bytes...)
		clone.Planes[i].Buffer = av.Buffer{Bytes: bytes, Ownership: av.BufferOwned}
	}
	return &clone
}

// --- external join stage 2: first-arm passthrough (Select re-expressed) -----
//
// No shape.Contract: the join is a media-agnostic passthrough exactly like
// Select — packets and frames forward as-is, only the StreamID rewritten.
// This is the no-private-power proof: Select's data-plane semantics rebuilt
// from the outside through the same seam.

type firstArmStage struct {
	name   string
	active av.StreamID
	arms   int
	eos    map[av.StreamID]struct{}
}

func newFirstArmStage(name string, active av.StreamID, arms int) *firstArmStage {
	return &firstArmStage{name: name, active: active, arms: arms, eos: make(map[av.StreamID]struct{}, arms)}
}

func (s *firstArmStage) Name() string { return s.name }

func (s *firstArmStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil || msg.Packet.StreamID != s.active {
			return nil
		}
		clone := *msg.Packet
		clone.StreamID = av.StreamID(s.name)
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &clone})
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.StreamID != s.active {
			return nil
		}
		clone := *msg.Frame
		clone.StreamID = av.StreamID(s.name)
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &clone})
	case pipeline.MessageEvent:
		if msg.Event == nil || msg.Event.Type != av.EventEndOfStream {
			return nil
		}
		s.eos[msg.Event.StreamID] = struct{}{}
		if len(s.eos) >= s.arms {
			out := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.name)}
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		return nil
	default:
		return nil
	}
}

func (s *firstArmStage) Close() error { return nil }

// --- fixtures ----------------------------------------------------------------

// toyPacketArm declares a packet-domain source pushing toy-codec packets —
// the arm shape that forces the join planner to insert decode arms.
func toyPacketArm(id av.StreamID, frames ...[]int16) goav.InputSpec {
	return goav.Source(string(id),
		shape.Packet(av.MediaAudio, toyCodecID, shape.Audio(8000, 1, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push source.Push) error {
			var elapsed int64
			for _, samples := range frames {
				payload := make([]byte, len(samples)*2)
				for i, sample := range samples {
					binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
				}
				packet := &av.Packet{
					StreamID: id,
					Type:     av.MediaAudio,
					PTS:      av.Timestamp{Value: elapsed, Base: av.TimeBase{Num: 1, Den: 8000}},
					Duration: av.Duration{Value: int64(len(samples)), Base: av.TimeBase{Num: 1, Den: 8000}},
					Keyframe: true,
					Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
				}
				elapsed += int64(len(samples))
				if _, err := push.Packet(packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// s16FrameArm declares a frame-domain S16 source — the arm shape that joins
// without a decode stage.
func s16FrameArm(id av.StreamID, frames ...[]int16) goav.InputSpec {
	return goav.Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push source.Push) error {
			var elapsed int64
			for _, samples := range frames {
				payload := make([]byte, len(samples)*2)
				for i, sample := range samples {
					binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
				}
				frame := &av.Frame{
					StreamID: id,
					Type:     av.MediaAudio,
					PTS:      av.Timestamp{Value: elapsed, Base: av.TimeBase{Num: 1, Den: 8000}},
					Duration: av.Duration{Value: int64(len(samples)), Base: av.TimeBase{Num: 1, Den: 8000}},
					Audio:    &av.AudioFrame{SampleRate: 8000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
					Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
				}
				elapsed += int64(len(samples))
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// s16CollectSink collects S16 frame payloads with their stream ids.
func s16CollectSink(name string, mu *sync.Mutex, ids *[]av.StreamID, frames *[][]int16) goav.Destination {
	return goav.Sink(component.SinkFunc(name, func(_ context.Context, msg component.Message) error {
		if msg.Kind != pipeline.MessageFrame || msg.Frame == nil || len(msg.Frame.Planes) == 0 {
			return nil
		}
		data := msg.Frame.Planes[0].Buffer.Bytes
		samples := make([]int16, len(data)/2)
		for i := range samples {
			samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
		}
		mu.Lock()
		*ids = append(*ids, msg.Frame.StreamID)
		*frames = append(*frames, samples)
		mu.Unlock()
		return nil
	}))
}

// --- the proof -----------------------------------------------------------------

// TestExternalJoinComposesThroughPublicGrammar runs the external interleaver
// end to end: two toy-codec PACKET arms force planner-inserted decode arms
// (derived from the stage's frame-domain contract — like Mix, with zero core
// knowledge of the stage), the joined stream is tapped, and Describe() equals
// the built graph node for node — the planned join node carries the custom
// stage's detail.
func TestExternalJoinComposesThroughPublicGrammar(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var ids []av.StreamID
	var frames [][]int16

	job := goav.Join("interleave", newInterleaveStage("interleave", "left", "right"),
		goav.From(toyPacketArm("left", []int16{10}, []int16{30})).Audio(),
		goav.From(toyPacketArm("right", []int16{20}, []int16{40})).Audio(),
	).Tap(goav.FrameTap("interleaved")).
		To(s16CollectSink("out", &mu, &ids, &frames)).
		UseRuntime(toyRuntime())

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// Describe() ≡ Build(): the one-plan guard, held for an external stage.
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("Describe() != Build():\nplanned: %+v\nbuilt:   %+v", planned, built)
	}
	var joinDetail string
	decodes := 0
	for _, node := range planned.Nodes {
		if node.Name == "interleave" {
			joinDetail = node.Detail
		}
		if strings.HasPrefix(node.Name, "interleave-decode-") {
			decodes++
		}
	}
	if joinDetail != "external interleaver" {
		t.Fatalf("planned join node detail = %q, want the external stage's detail", joinDetail)
	}
	if decodes != 2 {
		t.Fatalf("planned decode arms = %d, want 2 (derived from the stage's frame-domain contract)", decodes)
	}

	// The join tap is a first-class task tap anchored on the custom join node.
	tapped := false
	for _, tap := range task.Taps() {
		if tap.Name == "interleaved" && tap.Node == "interleave" && tap.Domain == shape.DomainFrame {
			tapped = true
		}
	}
	if !tapped {
		t.Fatalf("taps = %+v, want frame tap interleaved on the interleave node", task.Taps())
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := [][]int16{{10}, {20}, {30}, {40}}; !reflect.DeepEqual(frames, want) {
		t.Fatalf("interleaved frames = %v, want %v", frames, want)
	}
	for _, id := range ids {
		if id != "interleave" {
			t.Fatalf("joined stream ids = %v, want every frame under the join's output id", ids)
		}
	}
}

// TestExternalJoinFansOutThroughBranches proves the joined stream of a custom
// join is a normal stream point: .Branches fans it out to independent planned
// branches with their own destinations, through the same branch-compose
// lowering every chain uses.
func TestExternalJoinFansOutThroughBranches(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var xIDs, yIDs []av.StreamID
	var xs, ys [][]int16

	err := goav.Join("zipper", newInterleaveStage("zipper", "one", "two"),
		goav.From(s16FrameArm("one", []int16{1})).Audio(),
		goav.From(s16FrameArm("two", []int16{2})).Audio(),
	).Branches(
		goav.Branch("x").To(s16CollectSink("x-sink", &mu, &xIDs, &xs)),
		goav.Branch("y").To(s16CollectSink("y-sink", &mu, &yIDs, &ys)),
	).UseRuntime(toyRuntime()).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := [][]int16{{1}, {2}}; !reflect.DeepEqual(xs, want) || !reflect.DeepEqual(ys, want) {
		t.Fatalf("branch x = %v, branch y = %v, want %v on both", xs, ys, want)
	}
}

// TestExternalJoinNestsInsideBuiltinMix proves a custom join is a full
// JoinArm: the external interleaver stands as an arm of the built-in Mix, the
// outer mix consumes its joined output under the join's output id, and the
// contract-derived output facts (S16 8kHz mono) feed the mix's first-arm
// format reference.
func TestExternalJoinNestsInsideBuiltinMix(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var ids []av.StreamID
	var frames [][]int16

	err := goav.Mix(
		goav.Join("pair", newInterleaveStage("pair", "left", "right"),
			goav.From(s16FrameArm("left", []int16{10}, []int16{30})).Audio(),
			goav.From(s16FrameArm("right", []int16{20}, []int16{40})).Audio(),
		),
		goav.From(s16FrameArm("bed", []int16{1}, []int16{2}, []int16{3}, []int16{4})).Audio(),
	).To(s16CollectSink("out", &mu, &ids, &frames)).
		UseRuntime(toyRuntime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// The interleaver zips 10,20,30,40; the mix sums it with the 1,2,3,4 bed.
	if want := [][]int16{{11}, {22}, {33}, {44}}; !reflect.DeepEqual(frames, want) {
		t.Fatalf("mixed frames = %v, want %v (interleaved sub-join summed with the bed)", frames, want)
	}
}

// TestExternalJoinReproducesSelectPassthrough is the no-private-power proof:
// Select's data-plane semantics — media-agnostic passthrough of one arm,
// packets forwarded as-is with only the StreamID rewritten, one joined EOS
// after every arm ends — re-expressed through goav.Join with a stage defined
// in this package. No contract is declared, so the planner derives the same
// passthrough profile Select uses (no decode arms, sink-only delivery).
func TestExternalJoinReproducesSelectPassthrough(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var payloads [][]byte
	var ids []av.StreamID
	sink := goav.Sink(component.SinkFunc("out", func(_ context.Context, msg component.Message) error {
		if msg.Kind != pipeline.MessagePacket || msg.Packet == nil {
			return nil
		}
		mu.Lock()
		payloads = append(payloads, append([]byte(nil), msg.Packet.Payload.Bytes...))
		ids = append(ids, msg.Packet.StreamID)
		mu.Unlock()
		return nil
	}))

	job := goav.Join("firstof", newFirstArmStage("firstof", "left", 2),
		goav.From(toyPacketArm("left", []int16{100}, []int16{200})).Audio(),
		goav.From(toyPacketArm("right", []int16{900})).Audio(),
	).To(sink).UseRuntime(toyRuntime())

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range planned.Nodes {
		if strings.Contains(node.Name, "decode") {
			t.Fatalf("passthrough join planned a decode node %q; packets must forward as-is", node.Name)
		}
	}
	if err := job.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := [][]byte{{100, 0}, {200, 0}}
	if !reflect.DeepEqual(payloads, want) {
		t.Fatalf("forwarded payloads = %v, want only the active arm's packets %v", payloads, want)
	}
	for _, id := range ids {
		if id != "firstof" {
			t.Fatalf("forwarded ids = %v, want every packet under the join's output id", ids)
		}
	}
}
