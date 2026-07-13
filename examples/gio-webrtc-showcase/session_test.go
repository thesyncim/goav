package main

import (
	"context"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/snapshot"
)

func TestSessionPublishesStateForBranchChanges(t *testing.T) {
	session, err := newSession(context.Background(), bundle.MustNew(), "http://localhost:8080")
	if err != nil {
		t.Fatalf("newSession() error = %v", err)
	}
	defer session.Close()

	updates, unsubscribe := session.subscribe()
	defer unsubscribe()

	initial := awaitState(t, updates)
	if initial.ID == "" || initial.Revision == 0 {
		t.Fatalf("initial state = %+v, want session id and revision", initial)
	}
	if len(initial.Branches) != len(defaultBranches()) {
		t.Fatalf("branches = %d, want defaults", len(initial.Branches))
	}
	if len(initial.Debug.Tasks) != 2 {
		t.Fatalf("debug tasks = %d, want video and audio", len(initial.Debug.Tasks))
	}
	if len(initial.Events) == 0 {
		t.Fatalf("events empty, want event history")
	}

	_, err = session.addBranch(context.Background(), branchSpec{
		Kind:    "video",
		Codec:   "vp8",
		Width:   160,
		Height:  90,
		Bitrate: 120_000,
	})
	if err != nil {
		t.Fatalf("addBranch() error = %v", err)
	}

	updated := awaitState(t, updates)
	for len(updates) > 0 {
		updated = <-updates
	}
	if updated.Revision <= initial.Revision {
		t.Fatalf("revision = %d, want > %d", updated.Revision, initial.Revision)
	}
	if len(updated.Branches) != len(defaultBranches())+1 {
		t.Fatalf("branches = %d, want added branch", len(updated.Branches))
	}
	if !hasEvent(updated.Events, "branch", "branch track added") {
		t.Fatalf("events = %+v, want branch add event", updated.Events)
	}
}

func TestSessionRequestsBrowserKeyframeOnVideoSyncLoss(t *testing.T) {
	var packets []rtcp.Packet
	session := &session{
		videoSSRC:          42,
		branches:           make(map[string]*branch),
		listeners:          make(map[string]chan stateResponse),
		inboundPLIInterval: time.Minute,
		writeRTCP: func(next []rtcp.Packet) error {
			packets = append(packets, next...)
			return nil
		},
	}

	session.handleTaskEvent("video", av.Event{
		Type:   av.EventKeyframeRequired,
		Reason: "video depacketizer needs sync",
	})
	if len(packets) != 1 {
		t.Fatalf("packets = %d, want one PLI", len(packets))
	}
	pli, ok := packets[0].(*rtcp.PictureLossIndication)
	if !ok {
		t.Fatalf("packet type = %T, want PictureLossIndication", packets[0])
	}
	if pli.MediaSSRC != 42 {
		t.Fatalf("MediaSSRC = %d, want 42", pli.MediaSSRC)
	}
	if !hasEvent(session.events, "feedback", "browser keyframe requested") {
		t.Fatalf("events = %+v, want feedback event", session.events)
	}

	session.handleTaskEvent("video", av.Event{Type: av.EventKeyframeRequired})
	if len(packets) != 1 {
		t.Fatalf("packets after throttle = %d, want one PLI", len(packets))
	}
}

func TestSessionRequestsOutputKeyframeFromReceiverFeedback(t *testing.T) {
	task := &controlCaptureTask{}
	session := &session{
		videoTask:          task,
		branches:           make(map[string]*branch),
		listeners:          make(map[string]chan stateResponse),
		inboundPLIInterval: time.Minute,
	}
	session.branches["video-vp9"] = &branch{
		Spec: branchSpec{ID: "video-vp9", Kind: "video", Codec: "vp9", Width: 320, Height: 180, Bitrate: 320_000},
	}

	session.requestOutputVideoKeyframe(context.Background(), "video-vp9", "pli")
	if task.controls != 1 {
		t.Fatalf("controls = %d, want one keyframe control", task.controls)
	}
	if task.last.Type() != control.KeyframeType || task.last.Tap() != videoTapName {
		t.Fatalf("control = %+v, want keyframe at decoded video tap", task.last)
	}
	if !hasEvent(session.events, "feedback", "browser output keyframe requested") {
		t.Fatalf("events = %+v, want output feedback event", session.events)
	}

	session.requestOutputVideoKeyframe(context.Background(), "video-vp9", "pli")
	if task.controls != 1 {
		t.Fatalf("controls after throttle = %d, want one keyframe control", task.controls)
	}
}

func TestRTCPKeyframeFeedbackClassification(t *testing.T) {
	for _, packet := range []rtcp.Packet{
		&rtcp.PictureLossIndication{},
		&rtcp.FullIntraRequest{},
	} {
		if name, ok := rtcpKeyframeFeedback(packet); !ok || name == "" {
			t.Fatalf("rtcpKeyframeFeedback(%T) = %q, %v; want keyframe feedback", packet, name, ok)
		}
	}
	if name, ok := rtcpKeyframeFeedback(&rtcp.ReceiverReport{}); ok || name != "" {
		t.Fatalf("receiver report classified as %q, %v; want ignored", name, ok)
	}
}

type controlCaptureTask struct {
	last     control.Control
	controls int
}

func (t *controlCaptureTask) Describe() pipeline.Spec { return pipeline.Spec{} }

func (t *controlCaptureTask) Explain(context.Context) (plan.Report, error) {
	return plan.Report{}, nil
}

func (t *controlCaptureTask) Attach(context.Context, ...goav.BranchSpec) (goav.Attachment, error) {
	return nil, nil
}

func (t *controlCaptureTask) Detach(context.Context, goav.Attachment, ...lifecycle.DetachOption) error {
	return nil
}

func (t *controlCaptureTask) Taps() []snapshot.Tap {
	return []snapshot.Tap{{Name: videoTapName, Node: pipeline.NodeRef("video.decoded")}}
}

func (t *controlCaptureTask) Snapshot() snapshot.Task { return snapshot.Task{} }

func (t *controlCaptureTask) Control(_ context.Context, ctrl control.Control) error {
	t.last = ctrl
	t.controls++
	return nil
}

func (t *controlCaptureTask) Run(context.Context) error { return nil }

func (t *controlCaptureTask) Pause(context.Context) error { return nil }

func (t *controlCaptureTask) Resume(context.Context) error { return nil }

func (t *controlCaptureTask) Events() <-chan av.Event { return nil }

func (t *controlCaptureTask) Watch(...inspect.EventFilter) inspect.Subscription { return nil }

func (t *controlCaptureTask) Stats() pipeline.GraphStats { return pipeline.GraphStats{} }

func (t *controlCaptureTask) Close() error { return nil }

var _ goav.LiveTask = (*controlCaptureTask)(nil)

func awaitState(t *testing.T, updates <-chan stateResponse) stateResponse {
	t.Helper()
	select {
	case state := <-updates:
		return state
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for state")
		return stateResponse{}
	}
}

func hasEvent(events []debugEvent, kind, message string) bool {
	for _, event := range events {
		if event.Kind == kind && event.Message == message {
			return true
		}
	}
	return false
}
