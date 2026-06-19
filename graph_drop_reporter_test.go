package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestNamedGraphWrappersForwardDropReporter(t *testing.T) {
	source := namedSource{name: "source", source: graphDropReportingSource{drops: 3}}
	stage := namedStage{name: "stage", stage: graphDropReportingStage{drops: 5}}
	sink := namedSink{name: "sink", sink: graphDropReportingSink{drops: 7}}

	if got := source.DroppedMessages(); got != 3 {
		t.Fatalf("namedSource.DroppedMessages() = %d, want 3", got)
	}
	if got := stage.DroppedMessages(); got != 5 {
		t.Fatalf("namedStage.DroppedMessages() = %d, want 5", got)
	}
	if got := sink.DroppedMessages(); got != 7 {
		t.Fatalf("namedSink.DroppedMessages() = %d, want 7", got)
	}
}

type graphDropReportingSource struct {
	drops uint64
}

func (s graphDropReportingSource) Name() string { return "source" }

func (s graphDropReportingSource) Start(context.Context, pipeline.Emitter) error {
	return nil
}

func (s graphDropReportingSource) Close() error { return nil }

func (s graphDropReportingSource) DroppedMessages() uint64 { return s.drops }

type graphDropReportingStage struct {
	drops uint64
}

func (s graphDropReportingStage) Name() string { return "stage" }

func (s graphDropReportingStage) Handle(context.Context, *pipeline.Message, pipeline.Emitter) error {
	return nil
}

func (s graphDropReportingStage) Close() error { return nil }

func (s graphDropReportingStage) DroppedMessages() uint64 { return s.drops }

type graphDropReportingSink struct {
	drops uint64
}

func (s graphDropReportingSink) Name() string { return "sink" }

func (s graphDropReportingSink) Handle(context.Context, *pipeline.Message) error {
	return nil
}

func (s graphDropReportingSink) Close() error { return nil }

func (s graphDropReportingSink) DroppedMessages() uint64 { return s.drops }
