package goav

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestAttachOperationComponentContracts(t *testing.T) {
	stage := &runtimeTestStage{name: "meter"}
	tests := []struct {
		name      string
		operation operationSpec
		stage     pipeline.Stage
		want      string
	}{
		{name: "decode codec", operation: operationSpec{Kind: plan.OpDecode, Decode: codec.Codec(av.CodecID("x_decode"), av.MediaAudio)}, want: "x_decode"},
		{name: "transform factory", operation: operationSpec{Kind: plan.OpTransform, Transform: Resize(320, 180)}, want: "resize"},
		{name: "encode codec", operation: operationSpec{Kind: plan.OpEncode, Encode: codec.Codec(av.CodecID("x_encode"), av.MediaVideo)}, want: "x_encode"},
		{name: "copy", operation: operationSpec{Kind: plan.OpCopy}, want: "packet-copy"},
		{name: "explicit component", operation: operationSpec{Kind: plan.OpStage, Component: "custom-stage"}, stage: stage, want: "custom-stage"},
		{name: "stage fallback", operation: operationSpec{Kind: plan.OpStage}, stage: stage, want: "meter"},
		{name: "shape fallback", operation: operationSpec{Kind: plan.OpShape, Shape: shape.Frame(av.MediaAudio)}, want: "shape"},
		{name: "empty", operation: operationSpec{Kind: plan.OpStage}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := attachOperationComponent(tt.operation, tt.stage); got != tt.want {
				t.Fatalf("component = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAttachPatchHelperContracts(t *testing.T) {
	stage := &runtimeTestStage{name: "stage"}
	sink := SinkFunc("out", func(context.Context, Message) error { return nil })
	if got := attachComponentName(attachComponent{stage: stage}); got != "stage" {
		t.Fatalf("stage component name = %q", got)
	}
	if got := attachComponentName(attachComponent{sink: sink}); got != "out" {
		t.Fatalf("sink component name = %q", got)
	}
	if got := attachComponentName(attachComponent{}); got != "" {
		t.Fatalf("empty component name = %q", got)
	}

	patchShape := shape.Frame(av.MediaAudio)
	currentShape := shape.Packet(av.MediaAudio, av.CodecOpus)
	if got := attachStepShape(shape.Spec{}, patchShape); !reflect.DeepEqual(got, patchShape) {
		t.Fatalf("empty current shape = %#v, want patch %#v", got, patchShape)
	}
	if got := attachStepShape(currentShape, patchShape); !reflect.DeepEqual(got, currentShape) {
		t.Fatalf("non-empty current shape = %#v, want current %#v", got, currentShape)
	}

	if got := attachOperationDetail(plan.OpDecode); got != "decode" {
		t.Fatalf("decode detail = %q", got)
	}
	if got := attachOperationDetail(plan.OperationKind("custom")); got != "operation" {
		t.Fatalf("unknown detail = %q", got)
	}

	fileDest := File("archive.webm", io.Discard, Format(av.FormatWebM)).spec
	destinations := []attachDestination{
		{name: "live", sink: sink},
		{dest: fileDest},
	}
	if got := attachDestinationOperationKind(destinations[0]); got != plan.OpSink {
		t.Fatalf("sink destination kind = %s", got)
	}
	if got := attachDestinationOperationKind(destinations[1]); got != plan.OpMux {
		t.Fatalf("mux destination kind = %s", got)
	}
	if got := attachDestinationComponent(destinations[0]); got != "sink" {
		t.Fatalf("sink destination component = %q", got)
	}
	if got := attachDestinationComponent(destinations[1]); got != string(av.FormatWebM) {
		t.Fatalf("mux destination component = %q", got)
	}
	if got := attachDestinationIDs(destinations); !reflect.DeepEqual(got, []string{"destination/live", "destination/archive.webm"}) {
		t.Fatalf("destination ids = %#v", got)
	}
	if !attachDestinationsHaveMux(destinations) {
		t.Fatal("attachDestinationsHaveMux = false, want true")
	}
	if attachDestinationsHaveMux(destinations[:1]) {
		t.Fatal("sink-only attachDestinationsHaveMux = true, want false")
	}
}
