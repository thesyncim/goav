package goav_test

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

func TestExecutionContractExplainsCustomCodeAndDeclaredQueues(t *testing.T) {
	for _, mode := range []string{"automatic", "custom", "buffered"} {
		t.Run(mode, func(t *testing.T) {
			rt := goavtest.Runtime()
			v := goavtest.Video(64, 48, 1).Video()
			low := v.Resize(32, 24)
			wantMode, wantReason := "buffered", "automatic_encode_ladder"
			if mode != "automatic" {
				low = low.Do(component.FrameFunc("inspect", func(_ context.Context, frame *goav.Frame, emit component.Emit) error { return emit.Frame(frame) }))
				wantMode, wantReason = "direct", "custom_stage_requires_explicit_buffer"
			}
			if mode == "buffered" {
				low = low.Buffer(flow.Blocking(3))
				wantMode, wantReason = "buffered", "declared_branch_buffer"
			}
			outs := []goav.Output{
				v.Encode(codec.VP8()).As("high").To(goav.Write("high.webm", io.Discard)),
				low.Encode(codec.VP8()).As("low").To(goav.Write("low.webm", io.Discard)),
			}
			report, err := rt.Explain(t.Context(), outs...)
			if err != nil {
				t.Fatal(err)
			}
			if report.Graph.Execution.Mode != wantMode || report.Graph.Execution.Fanout != "shared_producer" {
				t.Fatalf("execution = %+v", report.Graph.Execution)
			}
			reason := false
			for _, decision := range report.Decisions {
				reason = reason || decision.Code == "execution_mode" && strings.Contains(decision.Message, wantReason)
			}
			if !reason {
				t.Fatalf("missing execution reason %q: %+v", wantReason, report.Decisions)
			}
			task, err := rt.Build(t.Context(), outs...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			if !reflect.DeepEqual(report.Graph, task.Spec()) {
				t.Fatalf("planned and running scheduling differ:\n%+v\n%+v", report.Graph, task.Spec())
			}
			sawDeclared := false
			for _, node := range report.Graph.Nodes {
				if node.Kind == pipeline.NodeSource {
					continue
				}
				if wantMode == "direct" && (node.Execution.Kind != "inline" || !node.Execution.BlocksProducer || !node.Execution.Queue.IsDirect()) {
					t.Fatalf("inline contract = %+v", node)
				}
				if wantMode == "buffered" && (node.Execution.Kind != "worker" || node.Execution.Queue.Capacity <= 0) {
					t.Fatalf("worker contract = %+v", node)
				}
				sawDeclared = sawDeclared || node.Execution.Queue.Capacity == 3
			}
			if mode == "buffered" && !sawDeclared {
				t.Fatal("declared queue absent from execution plan")
			}
		})
	}
}

func TestExecutionContractOracleWithLossyDefault(t *testing.T) {
	rt := bundle.MustNew(runconfig.WithBufferPolicy(pipeline.BufferPolicy{Capacity: 64, Drop: pipeline.DropOldest}))
	for name, build := range oracleCorpus(t) {
		t.Run(name, func(t *testing.T) {
			oracleSeq = 0
			spec, describeErr := rt.Describe(build()...)
			oracleSeq = 0
			task, err := rt.Build(t.Context(), build()...)
			if err != nil {
				return
			} // refusal cases belong to the compiler oracle
			defer task.Close()
			if describeErr != nil {
				t.Fatal(describeErr)
			}
			if !reflect.DeepEqual(spec, task.Spec()) {
				t.Fatalf("planned and live execution differ:\n%+v\n%+v", spec, task.Spec())
			}
		})
	}
}

type executionAdmittingJoin struct{}

func (executionAdmittingJoin) Name() string { return "blockingjoin" }
func (executionAdmittingJoin) Close() error { return nil }
func (executionAdmittingJoin) Handle(ctx context.Context, msg *pipeline.Message, e pipeline.Emitter) error {
	return e.Emit(ctx, msg)
}
func (executionAdmittingJoin) Admit(context.Context, *pipeline.Message) error { return nil }
func (executionAdmittingJoin) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatS16))}
}
func (executionAdmittingJoin) OutputShapes(shape.Spec) shape.Set {
	return executionAdmittingJoin{}.InputShapes()
}

func TestExecutionContractTimestampAdmissionMatchesRuntime(t *testing.T) {
	data := mfWebM(t, 0, 0, 0, 48000, 2, 1)
	for _, mode := range []string{"mixed_sources", "blocking_admitter", "wrapped_file"} {
		t.Run(mode, func(t *testing.T) {
			a := goav.FileInput("a.webm", bytes.NewReader(data))
			b := goav.FileInput("b.webm", bytes.NewReader(data))
			wantPlan, wantLive := "timestamp", "timestamp"
			var outputs []goav.Output
			switch mode {
			case "mixed_sources":
				outputs = []goav.Output{a.Audio().To(oracleSink("a")), b.Audio().To(oracleSink("b")), goavtest.Audio(48000, 2, []int16{1, 2}).Audio().To(oracleSink("free"))}
			case "blocking_admitter":
				outputs = []goav.Output{goav.Join("blockingjoin", executionAdmittingJoin{}, a.Audio().Decode(), b.Audio().Decode()).To(oracleSink("joined"))}
				wantPlan, wantLive = "arrival", "arrival"
			case "wrapped_file":
				a = goav.WrapSource(a, func(s pipeline.Source) pipeline.Source { return s })
				outputs = []goav.Output{a.Audio().To(oracleSink("a")), b.Audio().To(oracleSink("b"))}
				wantPlan = "runtime"
			}
			rt := offlineRuntime()
			report, err := rt.Explain(t.Context(), outputs...)
			if err != nil {
				t.Fatal(err)
			}
			if report.Graph.Execution.Admission != wantPlan {
				t.Fatalf("planned admission = %q, want %q", report.Graph.Execution.Admission, wantPlan)
			}
			task, err := rt.Build(t.Context(), outputs...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			if actual := task.Spec().Execution.Admission; actual != wantLive {
				t.Fatalf("live admission = %q, want %q", actual, wantLive)
			}
			ordered := false
			for _, decision := range report.Decisions {
				ordered = ordered || decision.Code == "time_ordered"
			}
			if ordered != (wantPlan == "timestamp") {
				t.Fatalf("time_ordered decision = %v, plan = %q", ordered, wantPlan)
			}
		})
	}
}
