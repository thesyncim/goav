package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestRuntimeGraphHandleRoutes(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "raw-source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	stage := &runtimeTestStage{name: "raw-stage"}
	left := &runtimeTestSink{name: "raw-left"}
	right := &runtimeTestSink{name: "raw-right"}

	graph := New().Graph()
	src := graph.Source("source", source)
	dec := graph.Stage("decode", stage)
	record := graph.Sink("record", left)
	preview := graph.Sink("preview", right)

	graph.Connect(src.Stream("audio"), dec.In())
	graph.Connect(dec.Out(), record.In(), preview.In())

	planned, err := graph.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "source -> decode [stream=audio]") ||
		!strings.Contains(specText(planned), "decode -> record") ||
		!strings.Contains(specMermaid(planned), "-- \"stream=audio\" -->") {
		t.Fatalf("planned:\n%s\nmermaid:\n%s", specText(planned), specMermaid(planned))
	}

	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count != 1 || left.count != 1 || right.count != 1 {
		t.Fatalf("stage=%d left=%d right=%d", stage.count, left.count, right.count)
	}
}

func TestRuntimeGraphHandleEventRoute(t *testing.T) {
	event := av.Event{Type: av.EventStats}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageEvent, Event: &event},
	}
	stats := &runtimeTestSink{name: "stats"}
	loss := &runtimeTestSink{name: "loss"}

	graph := New().Graph()
	src := graph.Source("source", source)
	statsNode := graph.Sink("stats", stats)
	lossNode := graph.Sink("loss", loss)
	graph.Connect(src.Event(av.EventStats), statsNode.In())
	graph.Connect(src.Event(av.EventPacketLoss), lossNode.In())

	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats.count != 1 || loss.count != 0 {
		t.Fatalf("stats=%d loss=%d", stats.count, loss.count)
	}
	taskStats := task.Stats()
	if taskStats.Messages != 1 || taskStats.Events != 1 ||
		taskStats.EventsByType[av.EventStats] != 1 ||
		taskStats.Delivered != 1 ||
		!taskStats.LastEventPresent ||
		taskStats.LastEvent.Type != av.EventStats {
		t.Fatalf("stats=%+v", taskStats)
	}
}

func TestRuntimeGraphHandlesRejectNilNode(t *testing.T) {
	graph := New().Graph()
	graph.Source("source", nil)
	if _, err := graph.Build(context.Background()); !errors.Is(err, ErrNilSource) {
		t.Fatalf("err = %v, want ErrNilSource", err)
	}
}
