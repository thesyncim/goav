package inspect

import (
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

type helperTask struct {
	sub  Subscription
	snap snapshot.Task
	stat pipeline.GraphStats
}

func (t helperTask) Watch(filters ...EventFilter) Subscription {
	event := av.Event{Type: av.EventStats, StreamID: "audio"}
	for i := range filters {
		if !filters[i](event) {
			return helperSubscription{events: closedEventChannel()}
		}
	}
	return t.sub
}

func (t helperTask) Snapshot() snapshot.Task { return t.snap }

func (t helperTask) Stats() pipeline.GraphStats { return t.stat }

type helperSubscription struct {
	events <-chan av.Event
}

func (s helperSubscription) Events() <-chan av.Event { return s.events }

func (helperSubscription) Close() error { return nil }

func TestHelpersBridgeStructuralTaskCapabilities(t *testing.T) {
	events := make(chan av.Event, 1)
	events <- av.Event{Type: av.EventStats, StreamID: "audio"}
	close(events)
	task := helperTask{
		sub: helperSubscription{events: events},
		snap: snapshot.Task{Spec: pipeline.Spec{
			Name: "inspect-test",
			Nodes: []pipeline.NodeSpec{{
				Name: "source",
				Kind: pipeline.NodeSource,
			}},
		}},
		stat: pipeline.GraphStats{Messages: 3},
	}

	subscription := Subscribe(task, WatchTypes(av.EventStats), WatchStream("audio"))
	if event, ok := <-subscription.Events(); !ok || event.Type != av.EventStats || event.StreamID != "audio" {
		t.Fatalf("Subscribe event = %+v, %v; want audio stats event", event, ok)
	}
	if got := Snapshot(task).Spec.Name; got != "inspect-test" {
		t.Fatalf("Snapshot().Spec.Name = %q, want inspect-test", got)
	}
	if got := Stats(task).Messages; got != 3 {
		t.Fatalf("Stats().Messages = %d, want 3", got)
	}
	rendered, err := Render(task)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if !strings.Contains(rendered, "flowchart") || !strings.Contains(rendered, "source") {
		t.Fatalf("Render() = %q, want Mermaid flowchart with source node", rendered)
	}
}

func TestSubscribeNilTaskReturnsClosedSubscription(t *testing.T) {
	subscription := Subscribe(nil)
	if err := subscription.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, ok := <-subscription.Events(); ok {
		t.Fatal("nil Subscribe events channel is open")
	}
}

func closedEventChannel() <-chan av.Event {
	events := make(chan av.Event)
	close(events)
	return events
}
