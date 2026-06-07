package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
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

	graph := New().Graph()
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
	attachment, err := task.Attach(ctx, Branch("screenshots").From("source").Do(stage).To(SinkEndpoint(late)))
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name() != "screenshots" {
		t.Fatalf("attachment name = %q", attachment.Name())
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

	graph := New().Graph()
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
	left, err := task.Attach(ctx, Branch("left").From("source").Do(leftStage).To(SinkEndpoint(leftSink)))
	if err != nil {
		t.Fatal(err)
	}
	right, err := task.Attach(ctx, Branch("right").From("source").Do(rightStage).To(SinkEndpoint(rightSink)))
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

func TestTaskCloseStopsRuntimeAttachments(t *testing.T) {
	graph := New().Graph()
	source := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(source.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stage := &runtimeTestStage{name: "stage"}
	sink := &runtimeTestSink{name: "sink"}
	attachment, err := task.Attach(context.Background(), Branch("close").From("source").Do(stage).To(SinkEndpoint(sink)))
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

	graph := New().Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	parent, err := task.Attach(ctx,
		Branch("sampler").
			From("source").
			Do(parentStage).
			Tap("video.sampled").
			To(SinkEndpoint(parentSink)),
	)
	if err != nil {
		t.Fatal(err)
	}
	tap, ok := findTap(task.Taps(), "video.sampled")
	if !ok || tap.Node != "sampler/sample" {
		t.Fatalf("tap = %+v, ok=%v, want sampler/sample", tap, ok)
	}

	child, err := task.Attach(ctx, Branch("screenshots").FromTap("video.sampled").To(SinkEndpoint(childSink)))
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
	graph := New().Graph()
	src := graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	task, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	first, err := task.Attach(ctx,
		Branch("first").
			From("source").
			Do(&runtimeTestStage{name: "stage"}).
			Tap("sampled").
			To(SinkEndpoint(&runtimeTestSink{name: "first"})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(ctx)

	_, err = task.Attach(ctx,
		Branch("second").
			From("source").
			Do(&runtimeTestStage{name: "stage"}).
			Tap("sampled").
			To(SinkEndpoint(&runtimeTestSink{name: "second"})),
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

	graph := New(filters).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:      DomainFrame,
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
		FromTap("video.frames").
		Resize(320, 180).
		To(SinkEndpoint(resized)))
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

	graph := New(filters, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	mediaTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := mediaTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:      DomainFrame,
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
		FromTap("video.frames").
		Resize(320, 180).
		Tap("video.320.frames").
		To(SinkEndpoint(thumbs)))
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
		resizedTap.Domain != DomainFrame ||
		resizedTap.MediaKind != av.MediaVideo ||
		resizedTap.Caps.Width != 320 ||
		resizedTap.Caps.Height != 180 ||
		resizedTap.Node != "thumb/resize-thumb" {
		t.Fatalf("resized tap = %+v ok=%v, want frame video 320x180 tap on thumb/resize-thumb", resizedTap, ok)
	}
	child, err := mediaTask.Attach(ctx, Branch("inspect").
		FromTap("video.320.frames").
		To(SinkEndpoint(inspect)))
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

func TestTaskAttachRejectsUnknownAnchor(t *testing.T) {
	graph := New().Graph()
	graph.Source("source", &runtimeTestSource{name: "source"})
	graph.Sink("sink", &runtimeTestSink{name: "sink"})
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = task.Attach(context.Background(), Branch("late").From("missing").To(SinkEndpoint(&runtimeTestSink{name: "late"})))
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
	graph := New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1})).Graph()
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
	attachment, err := task.Attach(ctx, Branch("late").From("source").To(SinkEndpoint(late)))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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

	graph := New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1})).Graph()
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
	attachment, err := task.Attach(ctx,
		Branch("late").
			From("source").
			Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
			To(SinkEndpoint(late)),
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
	graph := New(formats, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:     DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
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
		FromTap("audio.packets").
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
	graph := New(formats, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:     DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
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
		FromTap("audio.packets").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Copy().
		Tap("audio.copied").
		To(SinkEndpoint(copied)))
	if err != nil {
		t.Fatal(err)
	}
	copiedTap, ok := findTap(builtTask.Taps(), "audio.copied")
	if !ok ||
		copiedTap.Domain != DomainPacket ||
		copiedTap.MediaKind != av.MediaAudio ||
		copiedTap.Caps.Codec != av.CodecOpus ||
		copiedTap.Node != "source" {
		t.Fatalf("copied tap = %+v ok=%v, want packet Opus tap on source", copiedTap, ok)
	}
	child, err := builtTask.Attach(ctx, Branch("record").
		FromTap("audio.copied").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
	graph := New(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:     DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
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
		FromTap("audio.frames").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Opus(96_000).
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
		encoderFactory.config.Bitrate != 96_000 {
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
	graph := New(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:     DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
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
	archive := AudioFlow("archive").Do(meter).OpusMusic()
	attachment, err := builtTask.Attach(ctx, Branch("archive").
		FromTap("audio.frames").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Apply(archive).
		To(Target("archive", FileOutput("archive.ogg", io.Discard))))
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
		encoderFactory.config.Parameters.Channels != Stereo ||
		encoderFactory.config.Bitrate != 128_000 {
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
	graph := New(formats, codecs, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:     DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
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
		FromTap("audio.frames").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Opus(96_000).
		Tap("audio.encoded").
		To(SinkEndpoint(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	encodedTap, ok := findTap(builtTask.Taps(), "audio.encoded")
	if !ok ||
		encodedTap.Domain != DomainPacket ||
		encodedTap.MediaKind != av.MediaAudio ||
		encodedTap.Caps.Codec != av.CodecOpus ||
		encodedTap.Node != "archive/encode-archive" {
		t.Fatalf("encoded tap = %+v ok=%v, want packet Opus tap on archive/encode-archive", encodedTap, ok)
	}
	child, err := builtTask.Attach(ctx, Branch("record").
		FromTap("audio.encoded").
		Buffer(pipeline.BufferPolicy{Capacity: 2, CopyPacketBytes: 1}).
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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

func TestRuntimeBranchTapAnchorsUseStableNames(t *testing.T) {
	audio := Branch("levels").FromTap("audio.decoded").To(SinkEndpoint(&runtimeTestSink{name: "levels"}))
	if audio.tap != "audio.decoded" || audio.from != "" {
		t.Fatalf("audio anchor tap=%q from=%q, want tap only", audio.tap, audio.from)
	}
	expert := Branch("expert").From("decode-audio").To(SinkEndpoint(&runtimeTestSink{name: "expert"}))
	if expert.from != "decode-audio" || expert.tap != "" {
		t.Fatalf("expert anchor tap=%q from=%q, want node only", expert.tap, expert.from)
	}
}

func findTap(taps []TapInfo, name string) (TapInfo, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return TapInfo{}, false
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
