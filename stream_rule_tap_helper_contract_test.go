package goav

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/plan"
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
	events := task.watch.subscribe(source, 2, []EventFilter{WatchTypes(av.EventAttachError)})
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
