package goav

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestExpertGraphHandleAccessorsAndDefaultPolicies(t *testing.T) {
	node := graphNode{name: "camera"}
	if got := node.Name(); got != "camera" {
		t.Fatalf("node Name() = %q, want camera", got)
	}
	if got := node.In(); got.name != "camera" {
		t.Fatalf("In() = %#v, want camera inlet", got)
	}

	all := node.Out()
	if got := all.Name(); got != "camera" {
		t.Fatalf("outlet Name() = %q, want camera", got)
	}
	if got := all.branchSource(); got != (branchSourceBinding{from: "camera", policy: pipeline.RouteAll}) {
		t.Fatalf("Out().branchSource() = %#v, want all-route source", got)
	}
	if got := (graphOutlet{name: "raw"}).branchSource(); got != (branchSourceBinding{from: "raw", policy: pipeline.RouteAll}) {
		t.Fatalf("zero-policy outlet branchSource() = %#v, want all-route source", got)
	}

	stream := node.Stream("video")
	if got := stream.branchSource(); got != (branchSourceBinding{from: "camera", policy: pipeline.RouteByStream, label: "video"}) {
		t.Fatalf("Stream().branchSource() = %#v, want stream route", got)
	}
	event := node.Event(av.EventStats)
	if got := event.branchSource(); got != (branchSourceBinding{from: "camera", policy: pipeline.RouteByEvent, label: string(av.EventStats)}) {
		t.Fatalf("Event().branchSource() = %#v, want event route", got)
	}
}

func TestExpertGraphBuilderValidationContracts(t *testing.T) {
	graph := expertGraph(New())
	if got := graph.Stage("missing-stage", nil).Name(); got != "missing-stage" {
		t.Fatalf("nil stage handle name = %q, want missing-stage", got)
	}
	if got := graph.Sink("missing-sink", nil).Name(); got != "missing-sink" {
		t.Fatalf("nil sink handle name = %q, want missing-sink", got)
	}
	if _, err := graph.Describe(); !errors.Is(err, ErrNilStage) {
		t.Fatalf("Describe() err = %v, want first latched ErrNilStage", err)
	}
	if _, err := graph.Build(context.Background()); !errors.Is(err, ErrNilStage) {
		t.Fatalf("Build() err = %v, want first latched ErrNilStage", err)
	}

	graph = expertGraph(New())
	if got := graph.Sink("missing-sink", nil).Name(); got != "missing-sink" {
		t.Fatalf("nil sink handle name = %q, want missing-sink", got)
	}
	if _, err := graph.Describe(); !errors.Is(err, ErrNilSink) {
		t.Fatalf("Describe() err = %v, want ErrNilSink", err)
	}

	for _, tt := range []struct {
		name string
		run  func(*graphBuilder)
		want error
	}{
		{
			name: "missing destination",
			run: func(graph *graphBuilder) {
				graph.Connect(graphOutlet{name: "source"})
			},
			want: pipeline.ErrInvalidLink,
		},
		{
			name: "missing source",
			run: func(graph *graphBuilder) {
				graph.Connect(graphOutlet{}, graphInlet{name: "sink"})
			},
			want: pipeline.ErrUnknownNode,
		},
		{
			name: "missing destination name",
			run: func(graph *graphBuilder) {
				graph.Connect(graphOutlet{name: "source"}, graphInlet{})
			},
			want: pipeline.ErrUnknownNode,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			graph := expertGraph(New())
			tt.run(graph)
			if _, err := graph.Describe(); !errors.Is(err, tt.want) {
				t.Fatalf("Describe() err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestExpertGraphBridgeFallbackNamesAndRoutes(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	source := &runtimeTestSource{
		name:    "source-self",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	stage := &runtimeTestStage{name: "stage-self"}
	sink := &runtimeTestSink{name: "sink-self"}

	graph := expertGraph(New())
	sourceName := graph.AddSource("", source)
	stageName := graph.AddStage("", stage)
	sinkName := graph.AddSink("", sink)
	if got, want := []string{sourceName, stageName, sinkName}, []string{"source-self", "stage-self", "sink-self"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback names = %v, want %v", got, want)
	}
	graph.AddRoute(sourceName, "", "", stageName)
	graph.AddRoute(stageName, pipeline.RouteByStream, "video", sinkName)

	spec, err := graph.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
		t.Fatalf("spec = %#v, want three nodes and two edges", spec)
	}
	if spec.Edges[0].Policy != pipeline.RouteAll ||
		spec.Edges[1].Policy != pipeline.RouteByStream ||
		spec.Edges[1].Label != "video" {
		t.Fatalf("edges = %#v, want default all then stream route", spec.Edges)
	}

	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count != 1 || sink.count != 1 {
		t.Fatalf("delivered counts stage=%d sink=%d, want 1/1", stage.count, sink.count)
	}
}
