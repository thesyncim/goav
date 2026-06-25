package expert_test

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/expert"
	"github.com/thesyncim/goav/pipeline"
)

type expertTestSource struct {
	name     string
	messages []pipeline.Message
}

func (s *expertTestSource) Name() string { return s.name }

func (s *expertTestSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.messages {
		if err := emitter.Emit(ctx, &s.messages[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *expertTestSource) Close() error { return nil }

type expertTestStage struct {
	name  string
	count int
}

func (s *expertTestStage) Name() string { return s.name }

func (s *expertTestStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	s.count++
	return emitter.Emit(ctx, msg)
}

func (s *expertTestStage) Close() error { return nil }

type expertTestSink struct {
	name  string
	count int
}

func (s *expertTestSink) Name() string { return s.name }

func (s *expertTestSink) Handle(context.Context, *pipeline.Message) error {
	s.count++
	return nil
}

func (s *expertTestSink) Close() error { return nil }

func packetMessage(stream av.StreamID) pipeline.Message {
	packet := av.Packet{StreamID: stream}
	return pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
}

// TestGraphWiresAndRunsThroughPublicSeam pins the whole expert door: handles
// from Source/Stage/Sink, Connect routes (full and stream-narrowed), Describe
// parity, Build, and a run that delivers messages along the wired routes.
func TestGraphWiresAndRunsThroughPublicSeam(t *testing.T) {
	graph := expert.Graph(goav.MustRuntime())
	source := graph.Source("source", &expertTestSource{
		name:     "source",
		messages: []pipeline.Message{packetMessage("audio"), packetMessage("video")},
	})
	stage := &expertTestStage{name: "stage"}
	stageNode := graph.Stage("stage", stage)
	all := &expertTestSink{name: "all"}
	audioOnly := &expertTestSink{name: "audio-only"}
	allNode := graph.Sink("all", all)
	audioNode := graph.Sink("audio-only", audioOnly)
	graph.Connect(source.Out(), stageNode.In())
	graph.Connect(stageNode.Out(), allNode.In())
	graph.Connect(stageNode.Stream("audio"), audioNode.In())

	spec, err := graph.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Nodes) != 4 || len(spec.Edges) != 3 {
		t.Fatalf("spec nodes = %d edges = %d, want 4 and 3", len(spec.Nodes), len(spec.Edges))
	}
	if source.Name() != "source" || stageNode.Name() != "stage" {
		t.Fatalf("handle names = %q, %q", source.Name(), stageNode.Name())
	}

	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count != 2 || all.count != 2 || audioOnly.count != 1 {
		t.Fatalf("counts stage=%d all=%d audio=%d, want 2, 2, 1", stage.count, all.count, audioOnly.count)
	}
}

// TestGraphHandlesAnchorRuntimeBranches pins the branch-anchoring capability:
// a GraphNode passed to goav.Branch(...).From anchors a runtime attachment at
// that node through the structural Route seam.
func TestGraphHandlesAnchorRuntimeBranches(t *testing.T) {
	graph := expert.Graph(goav.MustRuntime())
	source := graph.Source("source", &expertTestSource{
		name:     "source",
		messages: []pipeline.Message{packetMessage("audio")},
	})
	base := graph.Sink("base", &expertTestSink{name: "base"})
	graph.Connect(source.Out(), base.In())
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	late := &expertTestSink{name: "late"}
	attachment, err := task.Attach(context.Background(),
		goav.Branch("late").From(source).To(goav.Sink(late)))
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name() != "late" {
		t.Fatalf("attachment name = %q", attachment.Name())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if late.count != 1 {
		t.Fatalf("late sink count = %d, want 1", late.count)
	}
	if route := source.Route(); route.From != "source" || route.Policy != pipeline.RouteAll {
		t.Fatalf("node route = %+v", route)
	}
	if route := source.Stream("audio").Route(); route.Policy != pipeline.RouteByStream || route.Label != "audio" {
		t.Fatalf("outlet route = %+v", route)
	}
}

// TestGraphRequiresGoavRuntime pins the refusal: a nil runtime yields a
// builder whose Describe and Build fail with ErrRuntimeRequired (node and
// Connect calls stay safe no-ops).
func TestGraphRequiresGoavRuntime(t *testing.T) {
	graph := expert.Graph(nil)
	node := graph.Source("source", &expertTestSource{name: "source"})
	graph.Connect(node.Out(), node.In())
	if _, err := graph.Describe(); !errors.Is(err, expert.ErrRuntimeRequired) {
		t.Fatalf("Describe err = %v, want ErrRuntimeRequired", err)
	}
	if _, err := graph.Build(context.Background()); !errors.Is(err, expert.ErrRuntimeRequired) {
		t.Fatalf("Build err = %v, want ErrRuntimeRequired", err)
	}
}

// TestGraphRefusesNilNodesAndEmptyRoutes pins the wiring validation sentinels
// surfacing through the bridge: nil nodes and empty Connect calls latch the
// goav and pipeline sentinels reported by Describe.
func TestGraphRefusesNilNodesAndEmptyRoutes(t *testing.T) {
	graph := expert.Graph(goav.MustRuntime())
	graph.Source("source", nil)
	if _, err := graph.Describe(); !errors.Is(err, goav.ErrNilSource) {
		t.Fatalf("Describe err = %v, want ErrNilSource", err)
	}

	graph = expert.Graph(goav.MustRuntime())
	node := graph.Source("source", &expertTestSource{name: "source"})
	graph.Connect(node.Out())
	if _, err := graph.Describe(); !errors.Is(err, pipeline.ErrInvalidLink) {
		t.Fatalf("Describe err = %v, want ErrInvalidLink", err)
	}
}
