package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestTapIsPostEncodeAnchorContracts(t *testing.T) {
	if tapIsPostEncodeAnchor(nil, "encoded") || tapIsPostEncodeAnchor(&jobStreamBuild{}, "") {
		t.Fatal("empty post-encode tap lookup = true, want false")
	}

	stream := &jobStreamBuild{operations: []operationSpec{
		operationSpecForTap(PacketTap("encoded"), av.MediaAudio, plan.OpEncode),
		operationSpecForTap(FrameTap("frames"), av.MediaAudio, plan.OpDecode),
	}}
	if !tapIsPostEncodeAnchor(stream, "encoded") {
		t.Fatal("encoded packet tap not recognized as post-encode anchor")
	}
	if tapIsPostEncodeAnchor(stream, "frames") || tapIsPostEncodeAnchor(stream, "missing") {
		t.Fatal("non-terminal or missing tap recognized as post-encode anchor")
	}
}

func TestStreamRuleAttachmentTrackingContracts(t *testing.T) {
	task := &task{rules: &taskStreamRules{attached: make(map[av.StreamID][]streamRuleAttachment)}}
	task.trackStreamRuleAttachment("audio", 1, nil)
	if task.streamRuleAttached("audio", 1) {
		t.Fatal("nil attachment tracked, want ignored")
	}

	task.trackStreamRuleAttachment("audio", 1, &runtimeAttachment{id: "att-1", name: "late"})
	if !task.streamRuleAttached("audio", 1) {
		t.Fatal("runtime attachment was not tracked")
	}
	if task.streamRuleAttached("audio", 2) || task.streamRuleAttached("video", 1) {
		t.Fatal("streamRuleAttached matched wrong stream or rule")
	}
}

func TestInstallAndRunStreamRulesContracts(t *testing.T) {
	var nilTask *task
	nilTask.installStreamRules("demux", shape.DomainPacket, []streamRule{{match: MatchMedia(av.MediaAudio)}})

	graph := newWatchTestGraph(4)
	task := newTask(graph, nil)
	defer task.Close()

	task.installStreamRules("demux", shape.DomainPacket, nil)
	if task.rules != nil {
		t.Fatal("empty rule set installed runtime stream rules")
	}

	task.installStreamRules("demux", shape.DomainFrame, []streamRule{{match: MatchMedia(av.MediaAudio)}})
	if task.rules == nil ||
		task.rules.source != "demux" ||
		task.rules.domain != shape.DomainFrame ||
		len(task.rules.rules) != 1 ||
		len(task.rules.attached) != 0 {
		t.Fatalf("installed rules = %+v", task.rules)
	}

	graph.events <- av.Event{Type: av.EventStats, StreamID: "ignored"}
	graph.events <- av.Event{Type: av.EventStreamAdded}
	graph.events <- av.Event{Type: av.EventStreamRemoved}
}

func TestHandleStreamAddedGuardContracts(t *testing.T) {
	source := make(chan av.Event)
	defer close(source)
	task := &task{}
	events := task.watch.subscribe(source, 4, []inspect.EventFilter{inspect.WatchTypes(av.EventAttachError)}).Events()
	task.rules = &taskStreamRules{
		source: "demux",
		domain: shape.DomainPacket,
		rules:  []streamRule{{match: MatchMedia(av.MediaAudio), branches: []BranchSpec{{name: "late"}}}},
		attached: map[av.StreamID][]streamRuleAttachment{
			"from-event": {{rule: 0, attachment: &runtimeAttachment{id: "att-1", name: "late"}}},
		},
	}

	task.handleStreamAdded(av.Event{Type: av.EventStreamAdded})
	task.handleStreamAdded(av.Event{
		Type:   av.EventStreamAdded,
		Stream: &av.Stream{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{Type: av.MediaVideo}},
	})
	task.handleStreamAdded(av.Event{
		Type:     av.EventStreamAdded,
		StreamID: "from-event",
		Stream:   &av.Stream{Type: av.MediaAudio, Codec: av.CodecParameters{Type: av.MediaAudio}},
	})
	select {
	case event := <-events:
		t.Fatalf("unexpected stream-rule error for guarded add paths: %+v", event)
	default:
	}

	task.handleStreamAdded(av.Event{
		Type:   av.EventStreamAdded,
		Stream: &av.Stream{Type: av.MediaAudio, Codec: av.CodecParameters{Type: av.MediaAudio}},
	})
	event := <-events
	if event.Type != av.EventAttachError || event.StreamID != "" {
		t.Fatalf("missing-id error event = %+v", event)
	}
	var buildErr *BuildError
	if !errors.As(event.Cause, &buildErr) ||
		buildErr.Code != errcode.StreamRuleInvalid ||
		buildErr.Reason != "discovered stream has no id" {
		t.Fatalf("missing-id cause = %v", event.Cause)
	}
}

func TestStreamRuleAttachInputCapturesTemplatedBranch(t *testing.T) {
	task := &task{rules: &taskStreamRules{
		source: "demux",
		domain: shape.DomainPacket,
	}}
	rule := streamRule{
		branches: []BranchSpec{
			Branch("late").
				Copy().
				To(Sink(SinkFunc("late-sink", func(context.Context, Message) error { return nil }))),
		},
	}
	stream := av.Stream{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecOpus,
			Type: av.MediaAudio,
		},
	}

	input, err := task.streamRuleAttachInput(rule, stream)
	if err != nil {
		t.Fatal(err)
	}
	rule.branches[0].name = "mutated"
	rule.branches[0].operations = nil
	rule.branches[0].destinations = nil

	if input.branchNames != "late" ||
		input.attach.name != "late-audio" ||
		len(input.attach.branches) != 1 ||
		len(input.attach.branches[0].destinations) != 1 {
		t.Fatalf("stream rule attach input = %+v, want captured templated branch", input)
	}
	spec := input.attach.branches[0].spec
	if spec.name != "late-audio" ||
		spec.source.from != "demux" ||
		spec.source.policy != pipeline.RouteByStream ||
		spec.source.stream == nil ||
		spec.source.stream.ID != "audio" ||
		len(spec.operations) != 1 ||
		spec.operations[0].Kind != plan.OpCopy {
		t.Fatalf("stream rule attach spec = %+v, want discovered-stream source and copy operation", spec)
	}
}

func TestStreamRuleRemoveInputCapturesTrackedBranches(t *testing.T) {
	attachment := &runtimeAttachment{id: "att-1", name: "late"}
	task := &task{rules: &taskStreamRules{
		rules: []streamRule{{removeDisposition: oldBranchAbort}},
		attached: map[av.StreamID][]streamRuleAttachment{
			"audio": {
				{rule: 0, attachment: attachment},
				{rule: 1, attachment: nil},
			},
		},
	}}

	input := task.streamRuleRemoveInput(av.Event{Type: av.EventStreamRemoved, StreamID: "audio"})
	if input.streamID != "audio" ||
		len(input.attachments) != 1 ||
		input.attachments[0].attachment != attachment ||
		input.attachments[0].branchName != "late" ||
		input.attachments[0].disposition != oldBranchAbort {
		t.Fatalf("stream rule remove input = %+v, want captured tracked attachment", input)
	}
	if _, ok := task.rules.attached["audio"]; ok {
		t.Fatalf("removed stream still tracked: %+v", task.rules.attached)
	}
}

func TestHandleStreamRemovedDetachesTrackedBranchesWithDrain(t *testing.T) {
	graph := newWatchTestGraph(1)
	task := newTask(graph, nil)
	defer task.Close()
	attachment := &runtimeAttachment{
		id:    "att-1",
		name:  "late",
		owner: task,
		nodes: []pipeline.NodeRef{"late-node"},
	}
	task.attachments = map[*runtimeAttachment]struct{}{attachment: {}}
	task.rules = &taskStreamRules{
		attached: map[av.StreamID][]streamRuleAttachment{
			"audio": {
				{rule: 0, attachment: nil},
				{rule: 1, attachment: attachment},
			},
		},
	}

	task.handleStreamRemoved(av.Event{Type: av.EventStreamRemoved})
	if len(task.rules.attached) != 1 {
		t.Fatalf("empty stream removal changed attachments: %+v", task.rules.attached)
	}

	task.handleStreamRemoved(av.Event{Type: av.EventStreamRemoved, StreamID: "audio"})
	if _, ok := task.rules.attached["audio"]; ok {
		t.Fatalf("removed stream still tracked: %+v", task.rules.attached)
	}
	if !attachment.stopped {
		t.Fatal("tracked runtime attachment was not detached")
	}
	if _, ok := task.attachments[attachment]; ok {
		t.Fatal("detached stream-rule attachment still registered on task")
	}
	outcome, ok := attachment.detachOutcome.Load().(lifecycle.DestinationState)
	if !ok || outcome != lifecycle.DestinationCommitted {
		t.Fatalf("detach outcome = %v, %t; want committed", outcome, ok)
	}
}

func TestStreamRuleBranchNamesContract(t *testing.T) {
	rule := streamRule{branches: []BranchSpec{{name: "preview"}, {name: "archive"}}}
	if got := streamRuleBranchNames(rule); got != "preview+archive" {
		t.Fatalf("streamRuleBranchNames() = %q, want preview+archive", got)
	}
	if got := streamRuleBranchNames(streamRule{}); got != "" {
		t.Fatalf("empty streamRuleBranchNames() = %q, want empty", got)
	}
}

func TestPublishStreamRuleErrorContracts(t *testing.T) {
	source := make(chan av.Event)
	defer close(source)
	task := &task{}
	events := task.watch.subscribe(source, 2, []inspect.EventFilter{inspect.WatchTypes(av.EventAttachError)}).Events()
	cause := errors.New("attach failed")

	task.publishStreamRuleError("audio", "preview", cause)
	task.publishStreamRuleError("video", "", cause)

	first := <-events
	if first.Type != av.EventAttachError || first.StreamID != "audio" ||
		first.Reason != "stream rule branch preview failed" || !errors.Is(first.Cause, cause) {
		t.Fatalf("first stream-rule event = %+v", first)
	}
	second := <-events
	if second.Type != av.EventAttachError || second.StreamID != "video" ||
		second.Reason != "stream rule reaction failed" || !errors.Is(second.Cause, cause) {
		t.Fatalf("second stream-rule event = %+v", second)
	}
}
