package goav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
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

	graph := expertGraph(mustNew())
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

	graph := expertGraph(mustNew())
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
	graph := expertGraph(mustNew())
	graph.Source("source", nil)
	if _, err := graph.Build(context.Background()); !errors.Is(err, errNilSource) {
		t.Fatalf("err = %v, want errNilSource", err)
	}
}

func TestTaskAttachBranchesAndStopsWhileDirectGraphRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{1}}}},
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{2}}}},
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{3}}}},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := &runtimeTestSink{name: "base"}
	stage := &runtimeTestStage{name: "sample"}
	late := &runtimeTestSink{name: "out"}

	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	baseNode := graph.Sink("base", base)
	graph.Connect(src.Out(), baseNode.In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		runErr <- task.Run(ctx)
	}()

	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	attachment, err := task.Attach(ctx, Branch("screenshots").From(src).Do(stage).To(Sink(late)))
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name() != "screenshots" {
		t.Fatalf("attachment name = %q", attachment.Name())
	}
	runtimeAttachment, ok := attachment.(*runtimeAttachment)
	if !ok {
		t.Fatalf("attachment = %T, want runtimeAttachment", attachment)
	}
	work := runtimeAttachment.work
	if work.Name != "screenshots" {
		t.Fatalf("work patch name = %q, want screenshots", work.Name)
	}
	if got, want := workPatchOperationKindsForBranch(work.Operations, "screenshots"), []plan.OperationKind{plan.OpStage, plan.OpSink}; !reflect.DeepEqual(got, want) {
		t.Fatalf("work patch operations = %+v, want %+v", got, want)
	}
	if len(work.Branches) != 1 || work.Branches[0].Name != "screenshots" || len(work.Branches[0].Operations) != 2 {
		t.Fatalf("work patch branches = %+v, want screenshots with stage and sink operation ids", work.Branches)
	}
	if len(work.Destinations) != 1 || work.Destinations[0].Name != "out" || !reflect.DeepEqual(work.Destinations[0].Branches, []string{"screenshots"}) {
		t.Fatalf("work patch destinations = %+v, want out bound to screenshots", work.Destinations)
	}
	if len(work.Edges) != 2 ||
		work.Edges[0].From != "source" ||
		work.Edges[0].To != "screenshots/sample" ||
		work.Edges[1].To != "screenshots/out" {
		t.Fatalf("work patch edges = %+v, want source -> stage -> sink", work.Edges)
	}
	close(source.resume[0])
	select {
	case <-source.emitted[1]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := attachment.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Close(ctx); err != nil {
		t.Fatal(err)
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 3 || stage.count != 1 || late.count != 1 || late.lastPacketValue.Payload.Bytes[0] != 2 {
		t.Fatalf("base=%d stage=%d late=%d packet=%+v", base.count, stage.count, late.count, late.lastPacketValue)
	}
	branchStats := attachment.Stats()
	if _, ok := branchStats.Nodes["base"]; ok {
		t.Fatalf("branch stats leaked base node: %+v", branchStats.Nodes)
	}
	if got := branchStats.Nodes["screenshots/sample"]; got.InPackets != 1 || got.OutPackets != 1 {
		t.Fatalf("sample branch stats = %+v", got)
	}
	if got := branchStats.Nodes["screenshots/out"]; got.InPackets != 1 || got.OutPackets != 0 {
		t.Fatalf("out branch stats = %+v", got)
	}
	if branchStats.Delivered != 2 || branchStats.Packets != 1 {
		t.Fatalf("branch stats = %+v", branchStats)
	}
	text := specText(task.Describe())
	if strings.Contains(text, "screenshots/sample") || strings.Contains(text, "screenshots/out") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestTaskDetachClosesRuntimeBranches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{1}}}},
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{2}}}},
			{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{3}}}},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := &runtimeTestSink{name: "base"}
	leftStage := &runtimeTestStage{name: "left-stage"}
	leftSink := &runtimeTestSink{name: "left-sink"}
	rightStage := &runtimeTestStage{name: "right-stage"}
	rightSink := &runtimeTestSink{name: "right-sink"}

	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		runErr <- task.Run(ctx)
	}()

	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	left, err := task.Attach(ctx, Branch("left").From(src).Do(leftStage).To(Sink(leftSink)))
	if err != nil {
		t.Fatal(err)
	}
	right, err := task.Attach(ctx, Branch("right").From(src).Do(rightStage).To(Sink(rightSink)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-source.emitted[1]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := task.Detach(ctx, left); err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, right); err != nil {
		t.Fatal(err)
	}
	if err := left.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := right.Close(ctx); err != nil {
		t.Fatal(err)
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 3 || leftStage.count != 1 || rightStage.count != 1 || leftSink.count != 1 || rightSink.count != 1 {
		t.Fatalf("base=%d leftStage=%d rightStage=%d leftSink=%d rightSink=%d", base.count, leftStage.count, rightStage.count, leftSink.count, rightSink.count)
	}
	text := specText(task.Describe())
	if strings.Contains(text, "left/") || strings.Contains(text, "right/") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestTaskAttachRuntimeBranchGroup(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{9}}}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	leftStage := &runtimeTestStage{name: "left-stage"}
	leftSink := &runtimeTestSink{name: "left-sink"}
	rightStage := &runtimeTestStage{name: "right-stage"}
	rightSink := &runtimeTestSink{name: "right-sink"}

	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx,
		Branch("left").From(src).Do(leftStage).To(Sink(leftSink)),
		Branch("right").From(src).Do(rightStage).To(Sink(rightSink)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name() != "left+right" {
		t.Fatalf("attachment name = %q, want left+right", attachment.Name())
	}
	attached := specText(attachment.Spec())
	for _, want := range []string{
		"source -> left/left-stage",
		"source -> right/right-stage",
		"left/left-stage -> left/left-sink",
		"right/right-stage -> right/right-sink",
	} {
		if !strings.Contains(attached, want) {
			t.Fatalf("attachment spec missing %q:\n%s", want, attached)
		}
	}

	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || leftStage.count != 1 || rightStage.count != 1 || leftSink.count != 1 || rightSink.count != 1 {
		t.Fatalf("base=%d leftStage=%d rightStage=%d leftSink=%d rightSink=%d", base.count, leftStage.count, rightStage.count, leftSink.count, rightSink.count)
	}
	stats := attachment.Stats()
	if len(stats.Nodes) != 4 || stats.Nodes["left/left-stage"].InPackets != 1 || stats.Nodes["right/right-stage"].InPackets != 1 {
		t.Fatalf("branch stats = %+v", stats)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	text := specText(builtTask.Describe())
	if strings.Contains(text, "left/") || strings.Contains(text, "right/") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestTaskAttachRuntimeBranchGroupCanUsePendingTap(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{7}}}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	parentStage := &runtimeTestStage{name: "sample"}
	parentSink := &runtimeTestSink{name: "sampled"}
	childSink := &runtimeTestSink{name: "shots"}

	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx,
		Branch("sampler").
			From(src).
			Do(parentStage).
			Tap(PacketTap("video.sampled")).
			To(Sink(parentSink)),
		Branch("screenshots").
			From(PacketTap("video.sampled")).
			To(Sink(childSink)),
	)
	if err != nil {
		t.Fatal(err)
	}
	tap, ok := findTap(builtTask.Taps(), "video.sampled")
	if !ok || tap.Node != "sampler/sample" {
		t.Fatalf("tap = %+v, ok=%v, want sampler/sample", tap, ok)
	}
	runtimeTask, ok := builtTask.(*task)
	if !ok {
		t.Fatalf("task = %T, want *task", builtTask)
	}
	runtimeTask.attachMu.Lock()
	branchTapCount := len(runtimeTask.branchTaps)
	runtimeTask.attachMu.Unlock()
	if branchTapCount != 1 {
		t.Fatalf("runtime branch tap count = %d, want 1", branchTapCount)
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || parentStage.count != 1 || parentSink.count != 1 || childSink.count != 1 {
		t.Fatalf("base=%d stage=%d parent=%d child=%d", base.count, parentStage.count, parentSink.count, childSink.count)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if _, ok := findTap(builtTask.Taps(), "video.sampled"); ok {
		t.Fatalf("runtime tap still present after grouped detach: %+v", builtTask.Taps())
	}
	text := specText(builtTask.Describe())
	if strings.Contains(text, "sampler/") || strings.Contains(text, "screenshots/") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestRuntimeAttachUsesGraphPatchBoundary(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"runtime_attach.go", "work_patch.go"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := body.String()
	for _, required := range []string{
		"type runtimeGraphPatch struct",
		"type runtimeAttachInput struct",
		"type runtimeAttachBranchInput struct",
		"type runtimeAttachBranchPlanInput struct",
		"func runtimeAttachInputFromBranchSpecs",
		"func (t *task) runtimeAttachBranchPlanInput",
		"func runtimeAttachBranchIntent",
		"func (input runtimeAttachBranchPlanInput) intentForOperations",
		"func (t *task) attachRuntimeBranches",
		"func (p *runtimeGraphPatch) addAnchor",
		"func (p *runtimeGraphPatch) addApplied",
		"func (p *runtimeGraphPatch) setWork",
		"func (p runtimeGraphPatch) rollback",
		"func (p runtimeGraphPatch) attachment",
		"type workPatch struct",
		"func (t *task) planAttachBranchSteps",
		"func (p *attachPlan) finalizeBranch",
		"func (t *task) applyAttachBranch",
		"planInput, err := t.runtimeAttachBranchPlanInput",
		"func (p *attachPlan) registerBranch(input runtimeAttachBranchPlanInput",
		"func (p *attachPlan) finalizeBranch(index int, input runtimeAttachBranchPlanInput",
		"intent:    runtimeAttachBranchIntent(branch, anchor)",
		"range spec.operations",
		"operationSpecOutputShape",
		"for i := range input.branches",
		"patch.resetPlannedTaps()",
		"patch.setWork(",
		"patch.attachment(t, name)",
		"return t.attachRuntimeBranches(ctx, input)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("runtime attach should lower through runtimeGraphPatch; missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"type runtimeBranch struct",
		"workPatchFromRuntimeBranches",
		"runtimeBranchWorkSteps",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime attach should plan the workPatch directly from BranchSpec, not a parallel %q model", forbidden)
		}
	}
	for name, body := range map[string]string{
		"planAttachBranchSteps":      sourceFunctionBody(t, text, "planAttachBranchSteps"),
		"planAttachEncode":           sourceFunctionBody(t, text, "planAttachEncode"),
		"prepareRuntimeBranchDecode": sourceFunctionBody(t, text, "prepareRuntimeBranchDecode"),
	} {
		if strings.Contains(body, "streamIntent{") {
			t.Fatalf("%s should use the captured runtimeAttachBranchPlanInput intent, not rebuild streamIntent inline", name)
		}
	}
}

func TestRuntimeAttachInputCapturesBranchSpecs(t *testing.T) {
	spec := Branch("watch").
		From(PacketTap("pkts")).
		Copy().
		To(Sink(SinkFunc("late", func(context.Context, Message) error { return nil })))

	input, err := runtimeAttachInputFromBranchSpecs([]BranchSpec{spec})
	if err != nil {
		t.Fatal(err)
	}
	spec.name = "mutated"
	spec.operations = nil
	spec.destinations = nil
	spec.source.tap = "other"

	if input.name != "watch" ||
		len(input.branches) != 1 ||
		input.branches[0].spec.name != "watch" ||
		input.branches[0].spec.source.tap != "pkts" ||
		len(input.branches[0].spec.operations) != 1 ||
		len(input.branches[0].spec.destinations) != 1 ||
		len(input.branches[0].destinations) != 1 {
		t.Fatalf("runtime attach input = %+v, want captured branch spec and destinations", input)
	}
}

func TestRuntimeRebranchUsesInputBoundary(t *testing.T) {
	body, err := os.ReadFile("runtime_attach.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"type runtimeRebranchInput struct",
		"func runtimeRebranchInputFromArgs",
		"func (a *runtimeAttachment) rebranchRuntimeAttachment",
		"return a.rebranchRuntimeAttachment(ctx, input)",
		"a.owner.attachRuntimeBranches(ctx, input.attach)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("runtime rebranch should pass captured input to the mutation executor; missing %q", required)
		}
	}
	if strings.Contains(text, "return a.owner.Attach(ctx, specs...)") {
		t.Fatal("runtime rebranch should not recapture replacement specs through Attach")
	}
}

func TestRuntimeRebranchInputCapturesReplacementSpecsAndPolicy(t *testing.T) {
	spec := Branch("next").
		From(PacketTap("pkts")).
		Copy().
		To(Sink(SinkFunc("late", func(context.Context, Message) error { return nil })))

	input, err := runtimeRebranchInputFromArgs(context.Background(), []lifecycle.RebranchArg{
		spec,
		lifecycle.SwitchAt(lifecycle.NextKeyframe()),
		lifecycle.DrainOldBranch(),
	})
	if err != nil {
		t.Fatal(err)
	}
	spec.name = "mutated"
	spec.operations = nil
	spec.destinations = nil
	spec.source.tap = "other"

	if input.group == nil ||
		input.group.boundary != switchNextKeyframe ||
		input.disposition != oldBranchDrain ||
		input.attach.name != "next" ||
		len(input.attach.branches) != 1 ||
		input.attach.branches[0].spec.name != "next" ||
		input.attach.branches[0].spec.source.tap != "pkts" ||
		len(input.attach.branches[0].destinations) != 1 {
		t.Fatalf("runtime rebranch input = %+v, want captured switch-gated replacement", input)
	}
	operations := input.attach.branches[0].spec.operations
	if len(operations) != 2 ||
		operations[0].Kind != plan.OpStage ||
		operations[1].Kind != plan.OpCopy {
		t.Fatalf("runtime rebranch operations = %+v, want switch gate then copy", operations)
	}
	gate, ok := operations[0].Stage.(*switchGate)
	if !ok || gate.group != input.group {
		t.Fatalf("runtime rebranch gate = %#v, %v; want switch gate tied to input group", operations[0].Stage, ok)
	}
}

func workPatchOperationKindsForBranch(operations []workOperation, branch string) []plan.OperationKind {
	out := make([]plan.OperationKind, 0)
	for i := range operations {
		if operations[i].Branch == branch {
			out = append(out, operations[i].Kind)
		}
	}
	return out
}

func TestRuntimeAttachUsesSharedMuxDestinationPreparation(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"runtime_attach.go", "work_patch.go"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := body.String()
	for _, required := range []string{
		"func runtimeMuxDestinationFormat",
		"func runtimeMuxCompatibilityIssue",
		"func (t *task) planAttachMuxDestinationStep",
		"func attachSinkDestinationStep",
		"func attachSharedMuxDestinationStep",
		"runtimeMuxDestinationFormat(ctx, rt, target.dest, i)",
		"runtimeMuxCompatibilityIssue(target.name",
		"runtimeMuxDestinationFormat(ctx, t.runtime, destination.dest, index)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("runtime attach target prep should share mux helpers; missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"func runtimeSharedMuxFormat",
		"func (t *task) runtimeBranchMuxFormat",
		"func runtimeSharedMuxCompatibilityIssue",
		"func runtimeBranchMuxCompatibilityIssue",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime attach should not keep split mux format helper %q", forbidden)
		}
	}
}

func TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure(t *testing.T) {
	ctx := context.Background()
	graph := newRuntimeRollbackGraph()
	graph.failConnectAt = 2
	task := newTask(graph, nil)
	left := &runtimeTestSink{name: "left-sink"}
	right := &runtimeTestSink{name: "right-sink"}

	_, err := task.Attach(ctx,
		Branch("left").From(graphNode{name: "source"}).To(Sink(left)),
		Branch("right").From(graphNode{name: "source"}).To(Sink(right)),
	)
	if !errors.Is(err, errRuntimeRollbackConnect) {
		t.Fatalf("err = %v, want rollback connect failure", err)
	}
	text := specText(task.Describe())
	if strings.Contains(text, "left/") || strings.Contains(text, "right/") {
		t.Fatalf("spec retained failed attachment nodes:\n%s", text)
	}
	if !left.closed || !right.closed {
		t.Fatalf("left closed=%v right closed=%v, want both closed", left.closed, right.closed)
	}
	if len(graph.removed) != 2 ||
		graph.removed[0] != "right/right-sink" ||
		graph.removed[1] != "left/left-sink" {
		t.Fatalf("removed = %+v, want rollback of right then left", graph.removed)
	}
	if _, ok := findTap(task.Taps(), "video.sampled"); ok {
		t.Fatalf("unexpected tap after rollback: %+v", task.Taps())
	}
}

func TestTaskAttachRuntimeBranchGroupRejectsSharedSinkDestinationWithoutMux(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	shared := SinkFunc("shared", func(context.Context, Message) error {
		return nil
	})
	destination := Sink(shared)

	_, err = task.Attach(ctx,
		Branch("left").From(src).To(destination),
		Branch("right").From(src).To(destination),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
	if !strings.Contains(err.Error(), "goav.Mux(name, destination)") {
		t.Fatalf("err = %v, want mux guidance", err)
	}
}

func TestTaskAttachRuntimeBranchGroupSharesMuxSinkDestination(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	sharedCount := 0
	left := Mux("diagnostics", Sink(SinkFunc("shared", func(context.Context, Message) error {
		sharedCount++
		return nil
	})))
	right := Mux("diagnostics", Sink(SinkFunc("shared", func(context.Context, Message) error {
		return fmt.Errorf("explicit mux destination should open one shared sink")
	})))

	attachment, err := task.Attach(ctx,
		Branch("left").From(src).To(left),
		Branch("right").From(src).To(right),
	)
	if err != nil {
		t.Fatal(err)
	}
	spec := attachment.Spec()
	sharedNodes := 0
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == "shared" {
			sharedNodes++
		}
		if strings.Contains(spec.Nodes[i].Name, "left/") || strings.Contains(spec.Nodes[i].Name, "right/") {
			t.Fatalf("grouped destination created branch-owned sink node: %+v", spec.Nodes[i])
		}
	}
	if sharedNodes != 1 {
		t.Fatalf("grouped sink nodes = %d, want 1 in %+v", sharedNodes, spec.Nodes)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sharedCount != 2 {
		t.Fatalf("grouped sink count = %d, want one message per branch", sharedCount)
	}
}

func TestTaskAttachRuntimeBranchGroupRejectsDuplicateSinkDestinationNames(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	left := Sink(SinkFunc("shared", func(context.Context, Message) error {
		return nil
	}))
	right := Sink(SinkFunc("shared", func(context.Context, Message) error {
		return nil
	}))

	_, err = task.Attach(ctx,
		Branch("left").From(src).To(left),
		Branch("right").From(src).To(right),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "goav.Mux(name, destination)") {
		t.Fatalf("err = %v, want shared mux guidance", err)
	}
	text := specText(task.Describe())
	if strings.Contains(text, "left/") || strings.Contains(text, "right/") || strings.Contains(text, "shared") {
		t.Fatalf("spec mutated after duplicate target rejection:\n%s", text)
	}
}

func TestTaskAttachRuntimeBranchGroupRejectsDuplicateMuxDestinations(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	left := Write("shared.ogg", io.Discard, Format(av.FormatOgg))
	right := Write("shared.ogg", io.Discard, Format(av.FormatOgg))

	_, err = task.Attach(ctx,
		Branch("left").From(src).To(left),
		Branch("right").From(src).To(right),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "runtime branch group reuses one destination name") ||
		!strings.Contains(err.Error(), "runtime destination group") ||
		!strings.Contains(err.Error(), "mux destination") {
		t.Fatalf("err = %v, want grouped destination guidance", err)
	}
	text := specText(task.Describe())
	if strings.Contains(text, "left/") || strings.Contains(text, "right/") {
		t.Fatalf("spec mutated after duplicate target rejection:\n%s", text)
	}
}

func TestTaskAttachRuntimeBranchGroupSharesMuxDestination(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(testFormatMuxer(av.FormatOgg, muxers))
	audioPacket := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
	}
	videoPacket := av.Packet{
		StreamID: "video",
		Payload:  av.Buffer{Bytes: []byte{2}, Ownership: av.BufferImmutable},
	}
	audioBase := &runtimeTestSink{name: "audio-base"}
	videoBase := &runtimeTestSink{name: "video-base"}
	graph := expertGraph(mustNew(formats))
	audioSource := graph.Source("audio-source", &runtimeTestSource{name: "audio-source", message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &audioPacket}})
	videoSource := graph.Source("video-source", &runtimeTestSource{name: "video-source", message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &videoPacket}})
	graph.Connect(audioSource.Out(), graph.Sink("audio-base", audioBase).In())
	graph.Connect(videoSource.Out(), graph.Sink("video-base", videoBase).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := builtTask.(*task)
	task.taps = []snapshot.Tap{
		{
			Name:      "audio.packets",
			MediaKind: av.MediaAudio,
			Domain:    shape.DomainPacket,
			Shape: shape.Spec{
				Domain:     shape.DomainPacket,
				MediaKind:  av.MediaAudio,
				StreamID:   "audio",
				Codec:      av.CodecOpus,
				SampleRate: 48000,
				Channels:   codec.Stereo,
			},
			Node: "audio-source",
		},
		{
			Name:      "video.packets",
			MediaKind: av.MediaVideo,
			Domain:    shape.DomainPacket,
			Shape: shape.Spec{
				Domain:    shape.DomainPacket,
				MediaKind: av.MediaVideo,
				StreamID:  "video",
				Codec:     av.CodecVP8,
				Width:     640,
				Height:    360,
			},
			Node: "video-source",
		},
	}
	defer builtTask.Close()
	destination := Mux("recording", Write("recording.ogg", io.Discard, Format(av.FormatOgg)))

	attachment, err := builtTask.Attach(ctx,
		Branch("audio").From(PacketTap("audio.packets")).Copy().To(destination),
		Branch("video").From(PacketTap("video.packets")).Copy().To(destination),
	)
	if err != nil {
		t.Fatal(err)
	}
	spec := attachment.Spec()
	text := specText(spec)
	if !strings.Contains(text, "audio-source -> recording") ||
		!strings.Contains(text, "video-source -> recording") ||
		strings.Contains(text, "audio/recording") ||
		strings.Contains(text, "video/recording") {
		t.Fatalf("shared mux destination spec:\n%s", text)
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if audioBase.count != 1 || videoBase.count != 1 {
		t.Fatalf("base counts audio=%d video=%d, want both source packets delivered", audioBase.count, videoBase.count)
	}
	if len(muxers.muxers) != 1 ||
		muxers.muxers[0].writes != 2 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio", "video"}) ||
		!streamIDsEqual(muxers.muxers[0].writtenStreams, []av.StreamID{"audio", "video"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	stats := attachment.Stats()
	if len(stats.Nodes) != 1 || stats.Nodes["recording.ogg"].InMessages != 2 || stats.Nodes["recording.ogg"].OutMessages != 0 {
		t.Fatalf("shared mux destination stats = %+v", stats)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !muxers.muxers[0].closed {
		t.Fatal("shared runtime mux destination was not closed by detach")
	}
	if strings.Contains(specText(builtTask.Describe()), "recording") {
		t.Fatalf("spec retained shared mux destination after detach:\n%s", specText(builtTask.Describe()))
	}
}

func TestTaskAttachRuntimeBranchGroupRequiresBranch(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	_, err = task.Attach(ctx)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_branch_invalid" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_branch_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "no runtime branches to attach") ||
		!strings.Contains(err.Error(), "goav.Branch") {
		t.Fatalf("err = %v, want branch guidance", err)
	}
}

func TestTaskCloseStopsRuntimeAttachments(t *testing.T) {
	graph := expertGraph(mustNew())
	source := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(source.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stage := &runtimeTestStage{name: "stage"}
	sink := &runtimeTestSink{name: "sink"}
	attachment, err := task.Attach(context.Background(), Branch("close").From(source).Do(stage).To(Sink(sink)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !stage.closed || !sink.closed {
		t.Fatalf("closed stage=%v sink=%v", stage.closed, sink.closed)
	}
	if err := attachment.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachRuntimeBranchExposesNestedTap(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{7}}}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	parentStage := &runtimeTestStage{name: "sample"}
	parentSink := &runtimeTestSink{name: "sampled"}
	childSink := &runtimeTestSink{name: "shots"}

	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	parent, err := task.Attach(ctx,
		Branch("sampler").
			From(src).
			Do(parentStage).
			Tap(PacketTap("video.sampled")).
			To(Sink(parentSink)),
	)
	if err != nil {
		t.Fatal(err)
	}
	tap, ok := findTap(task.Taps(), "video.sampled")
	if !ok || tap.Node != "sampler/sample" {
		t.Fatalf("tap = %+v, ok=%v, want sampler/sample", tap, ok)
	}

	child, err := task.Attach(ctx, Branch("screenshots").From(PacketTap("video.sampled")).To(Sink(childSink)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || parentStage.count != 1 || parentSink.count != 1 || childSink.count != 1 {
		t.Fatalf("base=%d stage=%d parent=%d child=%d", base.count, parentStage.count, parentSink.count, childSink.count)
	}

	if err := task.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, ok := findTap(task.Taps(), "video.sampled"); ok {
		t.Fatalf("runtime tap still present after detach: %+v", task.Taps())
	}
	text := specText(task.Describe())
	if strings.Contains(text, "sampler/") || strings.Contains(text, "screenshots/") {
		t.Fatalf("spec:\n%s", text)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachRejectsDuplicateRuntimeTap(t *testing.T) {
	ctx := context.Background()
	graph := expertGraph(mustNew())
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	first, err := task.Attach(ctx,
		Branch("first").
			From(src).
			Do(&runtimeTestStage{name: "stage"}).
			Tap(FrameTap("sampled")).
			To(Sink(&runtimeTestSink{name: "first"})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(ctx)

	_, err = task.Attach(ctx,
		Branch("second").
			From(src).
			Do(&runtimeTestStage{name: "stage"}).
			Tap(FrameTap("sampled")).
			To(Sink(&runtimeTestSink{name: "second"})),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_branch_tap_duplicate" {
		t.Fatalf("err = %v, want runtime_branch_tap_duplicate", err)
	}
}

func TestTaskAttachRuntimeResizeBranchRunsFromFrameTap(t *testing.T) {
	ctx := context.Background()
	resizer := &transcodeTestFilter{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, &transcodeTestFilterFactory{filter: resizer}))
	frame := av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	resized := &runtimeTestSink{name: "resized"}

	graph := expertGraph(mustNew(filters))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Width:       1280,
			Height:      720,
			PixelFormat: av.PixelFormatI420,
		},
		Node: "source",
	}}
	defer mediaTask.Close()

	attachment, err := mediaTask.Attach(ctx, Branch("small").
		From(FrameTap("video.frames")).
		Resize(320, 180).
		To(Sink(resized)))
	if err != nil {
		t.Fatal(err)
	}
	if err := mediaTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.frames != 1 || resized.frames != 1 || resizer.frames != 1 {
		t.Fatalf("base=%d resized=%d filter=%d", base.frames, resized.frames, resizer.frames)
	}
	if err := mediaTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachBufferedBranchAfterRuntimeResizeTapWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resizer := &transcodeTestFilter{}
	resizeFactory := &transcodeTestFilterFactory{filter: resizer}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	frame := av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420},
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	thumbs := &runtimeTestSink{name: "thumbs"}
	inspect := &runtimeTestSink{name: "inspect"}

	graph := expertGraph(mustNew(filters, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Width:       1280,
			Height:      720,
			PixelFormat: av.PixelFormatI420,
		},
		Node: "source",
	}}
	defer mediaTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- mediaTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	parent, err := mediaTask.Attach(ctx, Branch("thumb").
		From(FrameTap("video.frames")).
		Resize(320, 180).
		Tap(FrameTap("video.320.frames")).
		To(Sink(thumbs)))
	if err != nil {
		t.Fatal(err)
	}
	if resizeFactory.config.Video == nil ||
		resizeFactory.config.Video.Width != 320 ||
		resizeFactory.config.Video.Height != 180 {
		t.Fatalf("runtime resize config = %+v, want 320x180", resizeFactory.config.Video)
	}
	resizedTap, ok := findTap(mediaTask.Taps(), "video.320.frames")
	if !ok ||
		resizedTap.Domain != shape.DomainFrame ||
		resizedTap.MediaKind != av.MediaVideo ||
		resizedTap.Shape.Width != 320 ||
		resizedTap.Shape.Height != 180 ||
		resizedTap.Node != "thumb/resize-thumb" {
		t.Fatalf("resized tap = %+v ok=%v, want frame video 320x180 tap on thumb/resize-thumb", resizedTap, ok)
	}
	child, err := mediaTask.Attach(ctx, Branch("inspect").
		From(FrameTap("video.320.frames")).
		To(Sink(inspect)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.frames != 1 || thumbs.frames != 1 || inspect.frames != 1 || resizer.frames != 1 {
		t.Fatalf("base=%d thumbs=%d inspect=%d filter=%d", base.frames, thumbs.frames, inspect.frames, resizer.frames)
	}
	if err := mediaTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !thumbs.closed || !inspect.closed || !resizer.closed {
		t.Fatalf("closed thumbs=%v inspect=%v filter=%v", thumbs.closed, inspect.closed, resizer.closed)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskDetachBufferedRuntimeResizeTapSubtreeStopsFutureMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resizer := &transcodeTestFilter{}
	resizeFactory := &transcodeTestFilterFactory{filter: resizer}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	frames := []av.Frame{
		{StreamID: "video", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420}},
		{StreamID: "video", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420}},
		{StreamID: "video", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420}},
	}
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessageFrame, Frame: &frames[0]},
			{Kind: pipeline.MessageFrame, Frame: &frames[1]},
			{Kind: pipeline.MessageFrame, Frame: &frames[2]},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := newRuntimeObservedSink("base", 3)
	thumbs := newRuntimeObservedSink("thumbs", 1)
	inspect := newRuntimeObservedSink("inspect", 1)

	graph := expertGraph(mustNew(filters, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Width:       1280,
			Height:      720,
			PixelFormat: av.PixelFormatI420,
		},
		Node: "source",
	}}
	defer mediaTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- mediaTask.Run(ctx)
	}()
	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	parent, err := mediaTask.Attach(ctx, Branch("thumb").
		From(FrameTap("video.frames")).
		Buffer(flow.Blocking(2)).
		Resize(320, 180).
		Tap(FrameTap("video.320.frames")).
		To(Sink(thumbs)))
	if err != nil {
		t.Fatal(err)
	}
	resizedTap, ok := findTap(mediaTask.Taps(), "video.320.frames")
	if !ok ||
		resizedTap.Domain != shape.DomainFrame ||
		resizedTap.MediaKind != av.MediaVideo ||
		resizedTap.Shape.Width != 320 ||
		resizedTap.Shape.Height != 180 ||
		resizedTap.Node != "thumb/resize-thumb" {
		t.Fatalf("resized tap = %+v ok=%v, want frame video 320x180 tap on thumb/resize-thumb", resizedTap, ok)
	}
	child, err := mediaTask.Attach(ctx, Branch("inspect").
		From(FrameTap("video.320.frames")).
		Buffer(flow.Blocking(2)).
		To(Sink(inspect)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-thumbs.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-inspect.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	if err := mediaTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !thumbs.closedValue() || !inspect.closedValue() || !resizer.closed {
		t.Fatalf("closed thumbs=%v inspect=%v resize=%v", thumbs.closedValue(), inspect.closedValue(), resizer.closed)
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := base.countValue(); got != 3 {
		t.Fatalf("base count = %d, want all three frames", got)
	}
	if resizer.frames != 1 {
		t.Fatalf("resize frames = %d, want only the second frame before detach", resizer.frames)
	}
	if got := thumbs.countValue(); got != 1 {
		t.Fatalf("thumbs count = %d, want only second resized frame", got)
	}
	if got := inspect.countValue(); got != 1 {
		t.Fatalf("inspect count = %d, want only second resized frame", got)
	}
	if _, ok := findTap(mediaTask.Taps(), "video.320.frames"); ok {
		t.Fatalf("video.320.frames tap still visible after parent detach: %+v", mediaTask.Taps())
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskDetachBufferedRuntimeResampleTapSubtreeStopsFutureMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	frames := []av.Frame{
		{StreamID: "audio", Type: av.MediaAudio, Audio: &av.AudioFrame{SampleRate: 48000, Channels: codec.Stereo, SampleFormat: av.SampleFormatS16}},
		{StreamID: "audio", Type: av.MediaAudio, Audio: &av.AudioFrame{SampleRate: 48000, Channels: codec.Stereo, SampleFormat: av.SampleFormatS16}},
		{StreamID: "audio", Type: av.MediaAudio, Audio: &av.AudioFrame{SampleRate: 48000, Channels: codec.Stereo, SampleFormat: av.SampleFormatS16}},
	}
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessageFrame, Frame: &frames[0]},
			{Kind: pipeline.MessageFrame, Frame: &frames[1]},
			{Kind: pipeline.MessageFrame, Frame: &frames[2]},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := newRuntimeObservedSink("base", 3)
	voice := newRuntimeObservedSink("voice", 1)
	monitor := newRuntimeObservedSink("monitor", 1)

	graph := expertGraph(mustNew(filters, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer mediaTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- mediaTask.Run(ctx)
	}()
	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	parent, err := mediaTask.Attach(ctx, Branch("voice").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2)).
		Resample(16_000, codec.Mono).
		Tap(FrameTap("audio.16k")).
		To(Sink(voice)))
	if err != nil {
		t.Fatal(err)
	}
	resampledTap, ok := findTap(mediaTask.Taps(), "audio.16k")
	if !ok ||
		resampledTap.Domain != shape.DomainFrame ||
		resampledTap.MediaKind != av.MediaAudio ||
		resampledTap.Shape.SampleRate != 16_000 ||
		resampledTap.Shape.Channels != codec.Mono ||
		resampledTap.Shape.SampleFormat != av.SampleFormatS16 ||
		resampledTap.Node != "voice/resample-voice" {
		t.Fatalf("resampled tap = %+v ok=%v, want frame audio 16k mono tap on voice/resample-voice", resampledTap, ok)
	}
	child, err := mediaTask.Attach(ctx, Branch("monitor").
		From(FrameTap("audio.16k")).
		Buffer(flow.Blocking(2)).
		To(Sink(monitor)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-voice.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-monitor.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	if err := mediaTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !voice.closedValue() || !monitor.closedValue() || !resampler.closed {
		t.Fatalf("closed voice=%v monitor=%v resample=%v", voice.closedValue(), monitor.closedValue(), resampler.closed)
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := base.countValue(); got != 3 {
		t.Fatalf("base count = %d, want all three frames", got)
	}
	if resampler.frames != 1 {
		t.Fatalf("resample frames = %d, want only the second frame before detach", resampler.frames)
	}
	if got := voice.countValue(); got != 1 {
		t.Fatalf("voice count = %d, want only second resampled frame", got)
	}
	if got := monitor.countValue(); got != 1 {
		t.Fatalf("monitor count = %d, want only second resampled frame", got)
	}
	if _, ok := findTap(mediaTask.Taps(), "audio.16k"); ok {
		t.Fatalf("audio.16k tap still visible after parent detach: %+v", mediaTask.Taps())
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachRejectsDuplicateTapAfterRuntimeFilterOpenAndClosesFilter(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	source := &runtimeTestSource{
		name: "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			StreamID: "audio",
			Type:     av.MediaAudio,
			Audio:    &av.AudioFrame{SampleRate: 48000, Channels: codec.Stereo, SampleFormat: av.SampleFormatS16},
		}},
	}
	graph := expertGraph(mustNew(filters))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}, {
		Name:      "audio.16k",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   16000,
			Channels:     codec.Mono,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer mediaTask.Close()
	before := mediaTask.Describe()

	_, err = mediaTask.Attach(ctx, Branch("voice").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		Tap(FrameTap("audio.16k")).
		To(Sink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_branch_tap_duplicate" {
		t.Fatalf("err = %v, want runtime_branch_tap_duplicate", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before duplicate-tap rejection", resampleFactory.config.Audio)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after duplicate-tap rejection")
	}
	if after := mediaTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	taps := mediaTask.Taps()
	count := 0
	for _, tap := range taps {
		if tap.Name == "audio.16k" {
			count++
		}
		if strings.Contains(tap.Node.String(), "voice") {
			t.Fatalf("runtime branch tap registered after rejected attach: %+v", tap)
		}
	}
	if count != 1 {
		t.Fatalf("audio.16k tap count = %d in %+v, want existing tap only", count, taps)
	}
}

func TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	rt := runtimeValue(t, mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))))
	graph := newRuntimeRollbackGraph()
	mediaTask := &task{
		graph:   graph,
		runtime: rt,
		taps: []snapshot.Tap{{
			Name:      "audio.frames",
			MediaKind: av.MediaAudio,
			Domain:    shape.DomainFrame,
			Shape: shape.Spec{
				Domain:       shape.DomainFrame,
				MediaKind:    av.MediaAudio,
				StreamID:     "audio",
				Codec:        av.CodecOpus,
				SampleRate:   48000,
				Channels:     codec.Stereo,
				SampleFormat: av.SampleFormatS16,
			},
			Node: "source",
		}},
	}
	before := mediaTask.Describe()

	_, err := mediaTask.Attach(ctx, Branch("voice").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		Tap(FrameTap("audio.16k")).
		To(Sink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "runtime_branch_graph_error" ||
		buildErr.Operation != "connect branch" ||
		!errors.Is(err, errRuntimeRollbackConnect) {
		t.Fatalf("err = %v, want runtime_branch_graph_error wrapping connect failure", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before graph rollback", resampleFactory.config.Audio)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after graph connect rollback")
	}
	if graph.connects != 1 {
		t.Fatalf("connects = %d, want one failed connect", graph.connects)
	}
	if !reflect.DeepEqual(graph.removed, []string{"voice/resample-voice"}) {
		t.Fatalf("removed = %v, want resample stage rolled back", graph.removed)
	}
	if after := mediaTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range mediaTask.Taps() {
		if strings.Contains(tap.Node.String(), "voice") || tap.Name == "audio.16k" {
			t.Fatalf("runtime branch tap registered after graph rollback: %+v", tap)
		}
	}
}

func TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	muxers := &remuxTestMuxerFactory{}
	rt := runtimeValue(t, mustNew(
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, resampleFactory)),
		withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory)),
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
	))
	graph := newRuntimeRollbackGraph()
	graph.failConnectAt = 3
	mediaTask := &task{
		graph:   graph,
		runtime: rt,
		taps: []snapshot.Tap{{
			Name:      "audio.frames",
			MediaKind: av.MediaAudio,
			Domain:    shape.DomainFrame,
			Shape: shape.Spec{
				Domain:       shape.DomainFrame,
				MediaKind:    av.MediaAudio,
				StreamID:     "audio",
				Codec:        av.CodecOpus,
				SampleRate:   48000,
				Channels:     codec.Stereo,
				SampleFormat: av.SampleFormatS16,
			},
			Node: "source",
		}},
	}
	before := mediaTask.Describe()

	_, err := mediaTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Write("archive.ogg", io.Discard, Format(av.FormatOgg))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "runtime_branch_graph_error" ||
		buildErr.Operation != "connect branch target" ||
		!errors.Is(err, errRuntimeRollbackConnect) {
		t.Fatalf("err = %v, want runtime_branch_graph_error wrapping terminal connect failure", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before graph rollback", resampleFactory.config.Audio)
	}
	if encoderFactory.config.Parameters.ID != av.CodecOpus || encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("runtime encode config = %+v, want Opus 96k before graph rollback", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || !muxers.muxers[0].opened {
		t.Fatalf("muxers = %+v, want one opened Ogg muxer before graph rollback", muxers.muxers)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after terminal graph rollback")
	}
	if !encoder.closed {
		t.Fatal("runtime encoder was not closed after terminal graph rollback")
	}
	if !muxers.muxers[0].closed {
		t.Fatal("runtime muxer was not closed after terminal graph rollback")
	}
	if graph.connects != 3 {
		t.Fatalf("connects = %d, want terminal connect failure after two successful branch connects", graph.connects)
	}
	wantRemoved := []string{"archive/archive.ogg", "archive/encode-archive", "archive/resample-archive"}
	if !reflect.DeepEqual(graph.removed, wantRemoved) {
		t.Fatalf("removed = %v, want %v", graph.removed, wantRemoved)
	}
	if after := mediaTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected terminal attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range mediaTask.Taps() {
		if strings.Contains(tap.Node.String(), "archive") {
			t.Fatalf("runtime branch tap registered after terminal graph rollback: %+v", tap)
		}
	}
}

func TestTaskAttachRollsBackRuntimeSinkDestinationWhenGraphConnectFails(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	rt := runtimeValue(t, mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))))
	graph := newRuntimeRollbackGraph()
	graph.failConnectAt = 2
	sink := &runtimeTestSink{name: "monitor"}
	mediaTask := &task{
		graph:   graph,
		runtime: rt,
		taps: []snapshot.Tap{{
			Name:      "audio.frames",
			MediaKind: av.MediaAudio,
			Domain:    shape.DomainFrame,
			Shape: shape.Spec{
				Domain:       shape.DomainFrame,
				MediaKind:    av.MediaAudio,
				StreamID:     "audio",
				Codec:        av.CodecOpus,
				SampleRate:   48000,
				Channels:     codec.Stereo,
				SampleFormat: av.SampleFormatS16,
			},
			Node: "source",
		}},
	}
	before := mediaTask.Describe()

	_, err := mediaTask.Attach(ctx, Branch("monitor").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		To(Sink(sink)))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "runtime_branch_graph_error" ||
		buildErr.Operation != "connect branch target" ||
		!errors.Is(err, errRuntimeRollbackConnect) {
		t.Fatalf("err = %v, want runtime_branch_graph_error wrapping sink connect failure", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before graph rollback", resampleFactory.config.Audio)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after sink destination graph rollback")
	}
	if !sink.closed {
		t.Fatal("runtime sink destination was not closed after graph rollback")
	}
	if graph.connects != 2 {
		t.Fatalf("connects = %d, want sink connect failure after one successful branch connect", graph.connects)
	}
	wantRemoved := []string{"monitor/monitor", "monitor/resample-monitor"}
	if !reflect.DeepEqual(graph.removed, wantRemoved) {
		t.Fatalf("removed = %v, want %v", graph.removed, wantRemoved)
	}
	if after := mediaTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected sink attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range mediaTask.Taps() {
		if strings.Contains(tap.Node.String(), "monitor") {
			t.Fatalf("runtime branch tap registered after sink graph rollback: %+v", tap)
		}
	}
}

func TestTaskAttachAfterCloseClosesPreparedRuntimeComponents(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, resampleFactory)),
		withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory)),
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
	)
	graph := expertGraph(rt)
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mediaTask := builtTask.(*task)
	mediaTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	if err := builtTask.Close(); err != nil {
		t.Fatal(err)
	}
	before := builtTask.Describe()

	_, err = builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Write("archive.ogg", io.Discard, Format(av.FormatOgg))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "runtime_branch_graph_error" ||
		buildErr.Operation != "add stage" ||
		!errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("err = %v, want runtime_branch_graph_error wrapping closed graph", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before closed-graph rejection", resampleFactory.config.Audio)
	}
	if encoderFactory.config.Parameters.ID != av.CodecOpus || encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("runtime encode config = %+v, want Opus 96k before closed-graph rejection", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || !muxers.muxers[0].opened {
		t.Fatalf("muxers = %+v, want one opened Ogg muxer before closed-graph rejection", muxers.muxers)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after attach to closed graph")
	}
	if !encoder.closed {
		t.Fatal("runtime encoder was not closed after attach to closed graph")
	}
	if !muxers.muxers[0].closed {
		t.Fatal("runtime muxer was not closed after attach to closed graph")
	}
	if after := builtTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after closed-graph attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range builtTask.Taps() {
		if strings.Contains(tap.Node.String(), "archive") {
			t.Fatalf("runtime branch tap registered after closed-graph attach: %+v", tap)
		}
	}
}

func TestTaskAttachClosesPreparedComponentsWhenRuntimeNodeNameExists(t *testing.T) {
	ctx := context.Background()
	resampler := &transcodeTestFilter{}
	resampleFactory := &transcodeTestFilterFactory{filter: resampler}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, resampleFactory)),
		withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory)),
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
	)
	graph := expertGraph(rt)
	graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Stage("archive/archive.ogg", &runtimeTestStage{name: "existing-target"})
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer builtTask.Close()
	mediaTask := builtTask.(*task)
	mediaTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	before := builtTask.Describe()

	_, err = builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Write("archive.ogg", io.Discard, Format(av.FormatOgg))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "runtime_branch_node_duplicate" ||
		buildErr.Node != "archive/archive.ogg" ||
		!errors.Is(err, pipeline.ErrNodeExists) {
		t.Fatalf("err = %v, want duplicate terminal node error", err)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want opened 16k mono filter before duplicate-node rejection", resampleFactory.config.Audio)
	}
	if encoderFactory.config.Parameters.ID != av.CodecOpus || encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("runtime encode config = %+v, want Opus 96k before duplicate-node rejection", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || !muxers.muxers[0].opened {
		t.Fatalf("muxers = %+v, want one opened Ogg muxer before duplicate-node rejection", muxers.muxers)
	}
	if !resampler.closed {
		t.Fatal("runtime filter was not closed after duplicate-node rejection")
	}
	if !encoder.closed {
		t.Fatal("runtime encoder was not closed after duplicate-node rejection")
	}
	if !muxers.muxers[0].closed {
		t.Fatal("runtime muxer was not closed after duplicate-node rejection")
	}
	if after := builtTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after duplicate-node attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range builtTask.Taps() {
		if strings.Contains(tap.Node.String(), "archive") {
			t.Fatalf("runtime branch tap registered after duplicate-node rejection: %+v", tap)
		}
	}
}

func TestTaskAttachRejectsUnknownAnchor(t *testing.T) {
	graph := expertGraph(mustNew())
	graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Sink("sink", &runtimeTestSink{name: "sink"})
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = task.Attach(context.Background(), Branch("late").From(graphNode{name: "missing"}).To(Sink(&runtimeTestSink{name: "late"})))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_branch_anchor_missing" || !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("err = %v, want runtime_branch_anchor_missing wrapping ErrUnknownNode", err)
	}
}

func TestTaskAttachBranchesWhileBufferedGraphRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats}},
	}
	base := &runtimeTestSink{name: "sink"}
	late := &runtimeTestSink{name: "late"}
	graph := expertGraph(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1})))
	src := graph.Source("source", source)
	sink := graph.Sink("sink", base)
	graph.Connect(src.Out(), sink.In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		runErr <- task.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	attachment, err := task.Attach(ctx, Branch("late").From(src).To(Sink(late)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || late.count != 1 {
		t.Fatalf("base=%d late=%d, want both to receive future event", base.count, late.count)
	}
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !late.closed {
		t.Fatal("late sink was not closed by detach")
	}
}

func TestTaskDetachBufferedBranchStopsFutureMessagesAndKeepsStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	packets := []av.Packet{
		{StreamID: "video", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}},
		{StreamID: "video", Payload: av.Buffer{Bytes: []byte{2}, Ownership: av.BufferImmutable}},
		{StreamID: "video", Payload: av.Buffer{Bytes: []byte{3}, Ownership: av.BufferImmutable}},
	}
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessagePacket, Packet: &packets[0]},
			{Kind: pipeline.MessagePacket, Packet: &packets[1]},
			{Kind: pipeline.MessagePacket, Packet: &packets[2]},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := newRuntimeObservedSink("base", 3)
	late := newRuntimeObservedSink("late", 2)

	graph := expertGraph(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- task.Run(ctx)
	}()
	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	attachment, err := task.Attach(ctx,
		Branch("late").
			From(src).
			Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
			To(Sink(late)),
	)
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-late.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !late.closedValue() {
		t.Fatal("late sink was not closed by detach")
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := base.countValue(); got != 3 {
		t.Fatalf("base count = %d, want all three packets", got)
	}
	if got := late.packetValues(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("late packet values = %v, want only second packet", got)
	}
	branchStats := attachment.Stats()
	if _, ok := branchStats.Nodes["base"]; ok {
		t.Fatalf("branch stats leaked base node: %+v", branchStats.Nodes)
	}
	if got := branchStats.Nodes["late/late"]; got.InPackets != 1 || got.OutPackets != 0 || got.Dropped != 0 {
		t.Fatalf("late branch stats = %+v", got)
	}
	if branchStats.Delivered != 1 || branchStats.Dropped != 0 {
		t.Fatalf("branch stats = %+v", branchStats)
	}
}

// TestTaskAttachDetachBufferedBranchStress hammers the live attach -> traffic ->
// detach window on the buffered runner: a free-running source keeps emitting
// while a branch is attached and detached 100 times, so emits holding a routing
// snapshot from before a detach race the branch teardown on every iteration.
// It pins that the race never tears the pipeline down, never delivers to a
// branch sink after its Close, and never loses a message on the DropBlock base
// path.
func TestTaskAttachDetachBufferedBranchStress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	source := &runtimeFloodSource{
		name: "source",
		msg:  pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
		stop: make(chan struct{}),
	}
	base := newRuntimeObservedSink("base", 1)

	graph := expertGraph(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropBlock})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- task.Run(ctx)
	}()
	for i := 0; i < 100; i++ {
		late := &runtimeStressSink{name: "late", received: make(chan struct{}, 1)}
		attachment, err := task.Attach(ctx, Branch(fmt.Sprintf("late-%d", i)).
			From(src).
			Buffer(flow.Blocking(2)).
			To(Sink(late)))
		if err != nil {
			t.Fatalf("iteration %d: attach: %v", i, err)
		}
		select {
		case <-late.received:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatalf("iteration %d: detach: %v", i, err)
		}
		if !late.closed.Load() {
			t.Fatalf("iteration %d: late sink was not closed by detach", i)
		}
		if got := late.afterClosed.Load(); got != 0 {
			t.Fatalf("iteration %d: deliveries after close = %d, want 0", i, got)
		}
	}
	close(source.stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got, want := base.countValue(), source.emitted; got != want {
		t.Fatalf("base count = %d, want %d (DropBlock base path must stay lossless across attach/detach)", got, want)
	}
}

func TestTaskAttachBufferedPacketCopyMuxBranchWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	packet := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{9}, Ownership: av.BufferImmutable},
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	graph := expertGraph(mustNew(formats, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:     shape.DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	attachment, err := builtTask.Attach(ctx, Branch("record").
		From(PacketTap("audio.packets")).
		Copy().
		To(Write("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "source -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 {
		t.Fatalf("base=%d, want packet delivered to base sink", base.count)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "audio" || muxers.muxers[0].streamCount != 1 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !muxers.muxers[0].closed {
		t.Fatal("runtime recording muxer was not closed by detach")
	}
}

func TestTaskAttachBufferedCopyBranchPublishesPacketTapWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	packet := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{9}, Ownership: av.BufferImmutable},
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	copied := &runtimeTestSink{name: "copied"}
	graph := expertGraph(mustNew(formats, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:     shape.DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	parent, err := builtTask.Attach(ctx, Branch("copy").
		From(PacketTap("audio.packets")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Copy().
		Tap(PacketTap("audio.copied")).
		To(Sink(copied)))
	if err != nil {
		t.Fatal(err)
	}
	copiedTap, ok := findTap(builtTask.Taps(), "audio.copied")
	if !ok ||
		copiedTap.Domain != shape.DomainPacket ||
		copiedTap.MediaKind != av.MediaAudio ||
		copiedTap.Shape.Codec != av.CodecOpus ||
		copiedTap.Node != "source" {
		t.Fatalf("copied tap = %+v ok=%v, want packet Opus tap on source", copiedTap, ok)
	}
	child, err := builtTask.Attach(ctx, Branch("record").
		From(PacketTap("audio.copied")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Copy().
		To(Write("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || copied.count != 1 {
		t.Fatalf("base=%d copied=%d", base.count, copied.count)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "audio" ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !copied.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed copied=%v muxer=%v", copied.closed, muxers.muxers[0].closed)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachBufferedEncodeMuxBranchWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := expertGraph(mustNew(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	attachment, err := builtTask.Attach(ctx, Branch("record").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Write("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "record/encode-record -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 {
		t.Fatalf("base=%d, want frame delivered to base sink", base.count)
	}
	if encoder.encodes != 1 {
		t.Fatalf("encodes=%d", encoder.encodes)
	}
	if encoderFactory.config.Stream.ID != "record" ||
		encoderFactory.config.Parameters.ID != av.CodecOpus ||
		encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "record" || muxers.muxers[0].streamCount != 1 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"record"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed {
		t.Fatal("runtime recording encoder was not closed by detach")
	}
	if !muxers.muxers[0].closed {
		t.Fatal("runtime recording muxer was not closed by detach")
	}
}

func TestTaskAttachBufferedFlowEncodeMuxBranchWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := expertGraph(mustNew(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	meter := &runtimeTestStage{name: "meter"}
	archive := Flow("archive").Audio().Do(meter).Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))
	attachment, err := builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Apply(archive).
		To(Write("archive.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "archive/meter -> archive/encode-archive") ||
		!strings.Contains(attachmentText, "archive/encode-archive -> archive/archive.ogg") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || meter.count != 1 || encoder.encodes != 1 {
		t.Fatalf("base=%d meter=%d encodes=%d", base.count, meter.count, encoder.encodes)
	}
	if encoderFactory.config.Stream.ID != "archive" ||
		encoderFactory.config.Parameters.ID != av.CodecOpus ||
		encoderFactory.config.Parameters.Channels != codec.Stereo ||
		encoderFactory.config.Settings.Bitrate != 128_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "archive" ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"archive"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !meter.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed meter=%v encoder=%v muxer=%v", meter.closed, encoder.closed, muxers.muxers[0].closed)
	}
}

func TestTaskAttachBufferedBranchPublishesPostEncodeTapWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
	}
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	encoded := &runtimeTestSink{name: "encoded"}
	graph := expertGraph(mustNew(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	parent, err := builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Tap(PacketTap("audio.encoded")).
		To(Sink(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	encodedTap, ok := findTap(builtTask.Taps(), "audio.encoded")
	if !ok ||
		encodedTap.Domain != shape.DomainPacket ||
		encodedTap.MediaKind != av.MediaAudio ||
		encodedTap.Shape.Codec != av.CodecOpus ||
		encodedTap.Node != "archive/encode-archive" {
		t.Fatalf("encoded tap = %+v ok=%v, want packet Opus tap on archive/encode-archive", encodedTap, ok)
	}
	child, err := builtTask.Attach(ctx, Branch("record").
		From(PacketTap("audio.encoded")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Copy().
		To(Write("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || encoded.count != 1 || encoder.encodes != 1 {
		t.Fatalf("base=%d encoded=%d encodes=%d", base.count, encoded.count, encoder.encodes)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "archive" ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"archive"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !encoded.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed encoded=%v encoder=%v muxer=%v", encoded.closed, encoder.closed, muxers.muxers[0].closed)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskDetachBufferedPostEncodeTapSubtreeStopsFutureMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frames := []av.Frame{
		{StreamID: "audio", Type: av.MediaAudio},
		{StreamID: "audio", Type: av.MediaAudio},
		{StreamID: "audio", Type: av.MediaAudio},
	}
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessageFrame, Frame: &frames[0]},
			{Kind: pipeline.MessageFrame, Frame: &frames[1]},
			{Kind: pipeline.MessageFrame, Frame: &frames[2]},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := newRuntimeObservedSink("base", 3)
	encoded := newRuntimeObservedSink("encoded", 1)
	copied := newRuntimeObservedSink("copied", 1)

	graph := expertGraph(mustNew(codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	parent, err := builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Tap(PacketTap("audio.encoded")).
		To(Sink(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	child, err := builtTask.Attach(ctx, Branch("copy").
		From(PacketTap("audio.encoded")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(1, 0))).
		Copy().
		To(Sink(copied)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-encoded.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-copied.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	if err := builtTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !encoded.closedValue() || !copied.closedValue() {
		t.Fatalf("closed encoded=%v copied=%v", encoded.closedValue(), copied.closedValue())
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := base.countValue(); got != 3 {
		t.Fatalf("base count = %d, want all three frames", got)
	}
	if encoder.encodes != 1 {
		t.Fatalf("encodes = %d, want only the second frame before detach", encoder.encodes)
	}
	if got := encoded.packetValues(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("encoded packet values = %v, want only second encoded packet", got)
	}
	if got := copied.packetValues(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("copied packet values = %v, want only second copied packet", got)
	}
	if _, ok := findTap(builtTask.Taps(), "audio.encoded"); ok {
		t.Fatalf("audio.encoded tap still visible after parent detach: %+v", builtTask.Taps())
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskDetachBufferedCustomStageTapSubtreeStopsFutureMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	frames := []av.Frame{
		{StreamID: "audio", Type: av.MediaAudio},
		{StreamID: "audio", Type: av.MediaAudio},
		{StreamID: "audio", Type: av.MediaAudio},
	}
	source := &runtimeBranchStepSource{
		name: "source",
		messages: []pipeline.Message{
			{Kind: pipeline.MessageFrame, Frame: &frames[0]},
			{Kind: pipeline.MessageFrame, Frame: &frames[1]},
			{Kind: pipeline.MessageFrame, Frame: &frames[2]},
		},
		emitted: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
			make(chan struct{}),
		},
		resume: []chan struct{}{
			make(chan struct{}),
			make(chan struct{}),
		},
	}
	base := newRuntimeObservedSink("base", 3)
	analysis := newRuntimeObservedSink("analysis", 1)
	dependent := newRuntimeObservedSink("dependent", 1)
	meter := &runtimeTestStage{name: "meter"}

	graph := expertGraph(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	runErr := make(chan error, 1)
	go func() {
		runErr <- builtTask.Run(ctx)
	}()
	select {
	case <-source.emitted[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	parent, err := builtTask.Attach(ctx, Branch("analysis").
		From(FrameTap("audio.frames")).
		Buffer(flow.Blocking(2)).
		Do(meter).
		Tap(FrameTap("audio.metered")).
		To(Sink(analysis)))
	if err != nil {
		t.Fatal(err)
	}
	child, err := builtTask.Attach(ctx, Branch("dependent").
		From(FrameTap("audio.metered")).
		Buffer(flow.Blocking(2)).
		To(Sink(dependent)))
	if err != nil {
		t.Fatal(err)
	}
	close(source.resume[0])
	select {
	case <-analysis.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-dependent.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitDelivered(t, ctx, base)
	if err := builtTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !analysis.closedValue() || !dependent.closedValue() || !meter.closed {
		t.Fatalf("closed analysis=%v dependent=%v meter=%v", analysis.closedValue(), dependent.closedValue(), meter.closed)
	}
	close(source.resume[1])
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := base.countValue(); got != 3 {
		t.Fatalf("base count = %d, want all three frames", got)
	}
	if meter.count != 1 {
		t.Fatalf("meter count = %d, want only the second frame before detach", meter.count)
	}
	if got := analysis.countValue(); got != 1 {
		t.Fatalf("analysis count = %d, want only second metered frame", got)
	}
	if got := dependent.countValue(); got != 1 {
		t.Fatalf("dependent count = %d, want only second metered frame", got)
	}
	if _, ok := findTap(builtTask.Taps(), "audio.metered"); ok {
		t.Fatalf("audio.metered tap still visible after parent detach: %+v", builtTask.Taps())
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBranchTapAnchorsUseStableNames(t *testing.T) {
	audio := Branch("levels").From(FrameTap("audio.decoded")).To(Sink(&runtimeTestSink{name: "levels"}))
	if audio.source.tap != "audio.decoded" || audio.source.from != "" {
		t.Fatalf("audio anchor tap=%q from=%q, want tap only", audio.source.tap, audio.source.from)
	}
	expert := Branch("expert").From(graphNode{name: "decode-audio"}).To(Sink(&runtimeTestSink{name: "expert"}))
	if expert.source.from != "decode-audio" || expert.source.tap != "" {
		t.Fatalf("expert anchor tap=%q from=%q, want node only", expert.source.tap, expert.source.from)
	}
}

func findTap(taps []snapshot.Tap, name string) (snapshot.Tap, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return snapshot.Tap{}, false
}

var errRuntimeRollbackConnect = errors.New("runtime rollback connect failure")

type runtimeRollbackGraph struct {
	spec          pipeline.Spec
	events        chan av.Event
	connects      int
	failConnectAt int
	removed       []string
	closers       map[string]func() error
}

func newRuntimeRollbackGraph() *runtimeRollbackGraph {
	return &runtimeRollbackGraph{
		spec: pipeline.Spec{
			Name: "rollback",
			Nodes: []pipeline.NodeSpec{{
				Name: "source",
				Kind: pipeline.NodeSource,
			}},
		},
		events:        make(chan av.Event),
		failConnectAt: 1,
		closers:       make(map[string]func() error),
	}
}

func (g *runtimeRollbackGraph) AddSource(source pipeline.Source, _ pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	name := source.Name()
	g.spec.Nodes = append(g.spec.Nodes, pipeline.NodeSpec{Name: name, Kind: pipeline.NodeSource})
	return pipeline.NodeRef(name), nil
}

func (g *runtimeRollbackGraph) AddStage(stage pipeline.Stage, _ pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	name := stage.Name()
	g.spec.Nodes = append(g.spec.Nodes, pipeline.NodeSpec{Name: name, Kind: pipeline.NodeStage})
	g.closers[name] = stage.Close
	return pipeline.NodeRef(name), nil
}

func (g *runtimeRollbackGraph) AddSink(sink pipeline.Sink, _ pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	name := sink.Name()
	g.spec.Nodes = append(g.spec.Nodes, pipeline.NodeSpec{Name: name, Kind: pipeline.NodeSink})
	g.closers[name] = sink.Close
	return pipeline.NodeRef(name), nil
}

func (g *runtimeRollbackGraph) Connect(route pipeline.Route) error {
	g.connects++
	if g.failConnectAt <= 0 || g.connects == g.failConnectAt {
		return errRuntimeRollbackConnect
	}
	for i := range route.To {
		g.spec.Edges = append(g.spec.Edges, pipeline.EdgeSpec{
			From:   pipeline.NodeRef(route.From),
			To:     pipeline.NodeRef(route.To[i]),
			Policy: route.Policy,
			Label:  route.Label,
		})
	}
	return nil
}

func (g *runtimeRollbackGraph) Disconnect(pipeline.Route) error {
	return nil
}

func (g *runtimeRollbackGraph) Remove(ref pipeline.NodeRef) error {
	name := ref.String()
	g.removed = append(g.removed, name)
	if closeNode := g.closers[name]; closeNode != nil {
		_ = closeNode()
		delete(g.closers, name)
	}
	nodes := g.spec.Nodes[:0]
	for i := range g.spec.Nodes {
		if g.spec.Nodes[i].Name == name {
			continue
		}
		nodes = append(nodes, g.spec.Nodes[i])
	}
	g.spec.Nodes = nodes
	edges := g.spec.Edges[:0]
	for i := range g.spec.Edges {
		if g.spec.Edges[i].From.String() == name || g.spec.Edges[i].To.String() == name {
			continue
		}
		edges = append(edges, g.spec.Edges[i])
	}
	g.spec.Edges = edges
	return nil
}

func (g *runtimeRollbackGraph) Spec() pipeline.Spec {
	out := g.spec
	out.Nodes = append([]pipeline.NodeSpec(nil), g.spec.Nodes...)
	out.Edges = append([]pipeline.EdgeSpec(nil), g.spec.Edges...)
	return out
}

func (g *runtimeRollbackGraph) Run(context.Context) error {
	return nil
}

func (g *runtimeRollbackGraph) Events() <-chan av.Event {
	return g.events
}

func (g *runtimeRollbackGraph) Stats() pipeline.GraphStats {
	return pipeline.GraphStats{}
}

func (g *runtimeRollbackGraph) Close() error {
	close(g.events)
	return nil
}

type runtimeObservedSink struct {
	name     string
	received chan struct{}
	mu       sync.Mutex
	count    int
	packets  []byte
	closed   bool
}

func newRuntimeObservedSink(name string, capacity int) *runtimeObservedSink {
	if capacity < 1 {
		capacity = 1
	}
	return &runtimeObservedSink{name: name, received: make(chan struct{}, capacity)}
}

func (s *runtimeObservedSink) Name() string {
	return s.name
}

func (s *runtimeObservedSink) Handle(_ context.Context, msg *pipeline.Message) error {
	s.mu.Lock()
	s.count++
	if msg != nil && msg.Packet != nil && len(msg.Packet.Payload.Bytes) != 0 {
		s.packets = append(s.packets, msg.Packet.Payload.Bytes[0])
	}
	s.mu.Unlock()
	select {
	case s.received <- struct{}{}:
	default:
	}
	return nil
}

func (s *runtimeObservedSink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *runtimeObservedSink) countValue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *runtimeObservedSink) packetValues() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.packets...)
}

func (s *runtimeObservedSink) closedValue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// waitDelivered blocks until sink has handled one more message. Stepped tests
// on a buffered graph must call it once per emitted message before resuming
// the source: the sink's queue is drained by an independent worker, so without
// this wait a late-scheduled worker can leave a capacity-bounded DropNever
// queue full at the next emit, which surfaces ErrBackpressure and tears the
// pipeline down.
func waitDelivered(t *testing.T, ctx context.Context, sink *runtimeObservedSink) {
	t.Helper()
	select {
	case <-sink.received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type runtimeBranchStepSource struct {
	name     string
	messages []pipeline.Message
	emitted  []chan struct{}
	resume   []chan struct{}
}

func (s *runtimeBranchStepSource) Name() string {
	return s.name
}

func (s *runtimeBranchStepSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.messages {
		if err := emitter.Emit(ctx, &s.messages[i]); err != nil {
			return err
		}
		close(s.emitted[i])
		if i >= len(s.resume) {
			continue
		}
		select {
		case <-s.resume[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *runtimeBranchStepSource) Close() error {
	return nil
}

type runtimeBranchWaitingSource struct {
	name   string
	msg    pipeline.Message
	ready  chan struct{}
	resume chan struct{}
}

func (s *runtimeBranchWaitingSource) Name() string {
	return s.name
}

func (s *runtimeBranchWaitingSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	close(s.ready)
	select {
	case <-s.resume:
	case <-ctx.Done():
		return ctx.Err()
	}
	return emitter.Emit(ctx, &s.msg)
}

func (s *runtimeBranchWaitingSource) Close() error {
	return nil
}

// runtimeFloodSource emits the same message in a tight loop until stop closes,
// pacing only on downstream backpressure, and counts its successful emits.
type runtimeFloodSource struct {
	name    string
	msg     pipeline.Message
	stop    chan struct{}
	emitted int
}

func (s *runtimeFloodSource) Name() string {
	return s.name
}

func (s *runtimeFloodSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
		s.emitted++
	}
}

func (s *runtimeFloodSource) Close() error {
	return nil
}

// runtimeStressSink is a sink double for attach/detach stress: it signals the
// first delivery and counts any delivery that arrives after Close, which the
// detach contract forbids.
type runtimeStressSink struct {
	name        string
	received    chan struct{}
	closed      atomic.Bool
	afterClosed atomic.Int64
}

func (s *runtimeStressSink) Name() string {
	return s.name
}

func (s *runtimeStressSink) Handle(context.Context, *pipeline.Message) error {
	if s.closed.Load() {
		s.afterClosed.Add(1)
	}
	select {
	case s.received <- struct{}{}:
	default:
	}
	return nil
}

func (s *runtimeStressSink) Close() error {
	s.closed.Store(true)
	return nil
}
