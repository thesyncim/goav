package goav

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// SyncPolicy names one shared media timeline. Reuse the returned value across
// audio/video branches when those branches should align with each other.
type SyncPolicy struct {
	name      string
	tolerance time.Duration
	mode      syncMode
	scheduler *syncScheduler
	flags     syncPolicyFlags
}

type syncMode uint8

const (
	syncHoldLate syncMode = iota
	syncDropLate
)

type syncPolicyFlags uint8

const (
	syncPolicyHasTolerance syncPolicyFlags = 1 << iota
	syncPolicyHasMode
)

// Sync creates a shared media timeline policy. The default holds early media
// until slower streams catch up; use SyncDropLate for preview-style branches
// that should shed stale media instead.
func Sync(name string, opts ...SyncPolicy) SyncPolicy {
	policy := SyncPolicy{
		name:      firstNonEmpty(strings.TrimSpace(name), "sync"),
		mode:      syncHoldLate,
		scheduler: newSyncScheduler(),
	}
	for i := range opts {
		if opts[i].flags&syncPolicyHasTolerance != 0 {
			policy.tolerance = opts[i].tolerance
		}
		if opts[i].flags&syncPolicyHasMode != 0 {
			policy.mode = opts[i].mode
		}
	}
	return policy
}

// SyncTolerance sets the allowed media-time skew before a sync gate holds or
// drops a message.
func SyncTolerance(tolerance time.Duration) SyncPolicy {
	if tolerance <= 0 {
		return SyncPolicy{}
	}
	return SyncPolicy{tolerance: tolerance, flags: syncPolicyHasTolerance}
}

// SyncDropLate makes a sync gate drop messages that arrive behind the shared
// timeline by more than the tolerance.
func SyncDropLate() SyncPolicy {
	return SyncPolicy{mode: syncDropLate, flags: syncPolicyHasMode}
}

func operationSpecForSync(policy SyncPolicy) operationSpec {
	gate := newSyncGate(policy)
	return operationSpec{Kind: plan.OpStage, Component: gate.Name(), Stage: gate}
}

func newSyncGate(policy SyncPolicy) *syncGate {
	if policy.scheduler == nil {
		policy.scheduler = newSyncScheduler()
	}
	return &syncGate{policy: policy}
}

type syncGate struct {
	policy  SyncPolicy
	dropped atomic.Uint64
}

func (g *syncGate) Name() string {
	return syncStageName(g.policy.name)
}

func (g *syncGate) DescribeNode() pipeline.NodeSpec {
	detail := "sync"
	if g.policy.tolerance > 0 {
		detail += " tolerance=" + g.policy.tolerance.String()
	}
	if g.policy.mode == syncDropLate {
		detail += " drop-late"
	} else {
		detail += " hold-late"
	}
	return pipeline.NodeSpec{Name: g.Name(), Kind: pipeline.NodeStage, Detail: detail}
}

func (g *syncGate) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if msg.Kind == pipeline.MessageEvent {
		if msg.Event != nil && g.policy.scheduler != nil {
			g.policy.scheduler.observeEvent(msg.Event)
		}
		return emit.Emit(ctx, msg)
	}
	stream, pts, ok := syncMessageTime(msg)
	if !ok {
		return syncTimebaseError(g.policy, msg)
	}
	drop, err := g.policy.scheduler.admit(ctx, stream, pts, g.policy.tolerance, g.policy.mode)
	if err != nil || drop {
		if drop {
			g.dropped.Add(1)
		}
		return err
	}
	return emit.Emit(ctx, msg)
}

func (g *syncGate) Close() error {
	return nil
}

func (g *syncGate) DroppedMessages() uint64 {
	return g.dropped.Load()
}

func (g *syncGate) InputShapes() shape.Set {
	return nil
}

func (g *syncGate) OutputShapes(input shape.Spec) shape.Set {
	return shape.Set{input}
}

type syncScheduler struct {
	mu     sync.Mutex
	latest map[av.StreamID]time.Duration
	wait   func(context.Context) error
	closed bool
}

func newSyncScheduler() *syncScheduler {
	return &syncScheduler{latest: make(map[av.StreamID]time.Duration), wait: syncSchedulerWait}
}

func syncSchedulerWait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func (s *syncScheduler) admit(ctx context.Context, stream av.StreamID, pts time.Duration, tolerance time.Duration, mode syncMode) (bool, error) {
	if s == nil {
		return false, nil
	}
	if stream == "" {
		stream = "_"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		s.latest = make(map[av.StreamID]time.Duration)
	}
	if s.closed {
		return false, pipeline.ErrClosed
	}
	if previous, ok := s.latest[stream]; ok && pts+tolerance < previous {
		return true, nil
	}
	if mode == syncDropLate {
		if max, ok := s.maxLocked(); ok && pts+tolerance < max {
			return true, nil
		}
		s.latest[stream] = pts
		return false, nil
	}
	s.latest[stream] = pts
	for s.tooEarlyLocked(stream, pts, tolerance) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		wait := s.wait
		if wait == nil {
			wait = syncSchedulerWait
		}
		s.mu.Unlock()
		err := wait(ctx)
		s.mu.Lock()
		if err != nil {
			return false, err
		}
		if s.closed {
			return false, pipeline.ErrClosed
		}
	}
	return false, nil
}

func (s *syncScheduler) observeEvent(event *av.Event) {
	if s == nil || event == nil {
		return
	}
	switch event.Type {
	case av.EventDiscontinuity, av.EventCodecChanged, av.EventStreamRemoved:
		s.mu.Lock()
		if event.StreamID != "" {
			delete(s.latest, event.StreamID)
		} else {
			clear(s.latest)
		}
		s.mu.Unlock()
	}
}

func (s *syncScheduler) maxLocked() (time.Duration, bool) {
	if len(s.latest) == 0 {
		return 0, false
	}
	first := true
	var max time.Duration
	for _, pts := range s.latest {
		if first || pts > max {
			max = pts
			first = false
		}
	}
	return max, true
}

func (s *syncScheduler) minLocked() (time.Duration, bool) {
	if len(s.latest) == 0 {
		return 0, false
	}
	first := true
	var min time.Duration
	for _, pts := range s.latest {
		if first || pts < min {
			min = pts
			first = false
		}
	}
	return min, true
}

func (s *syncScheduler) tooEarlyLocked(stream av.StreamID, pts time.Duration, tolerance time.Duration) bool {
	if len(s.latest) <= 1 {
		return false
	}
	min, ok := s.minLocked()
	if !ok {
		return false
	}
	return pts > min+tolerance
}

func syncMessageTime(msg *pipeline.Message) (av.StreamID, time.Duration, bool) {
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil || !msg.Packet.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Packet.PTS.ToDuration()
		return msg.Packet.StreamID, pts, ok
	case pipeline.MessageFrame:
		if msg.Frame == nil || !msg.Frame.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Frame.PTS.ToDuration()
		return msg.Frame.StreamID, pts, ok
	default:
		return "", 0, false
	}
}

func syncStageName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "sync"
	}
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", "\t", "-", "\n", "-")
	return "sync-" + replacer.Replace(name)
}

func syncTimebaseError(policy SyncPolicy, msg *pipeline.Message) error {
	node := syncStageName(policy.name)
	kind := ""
	switch {
	case msg == nil:
	case msg.Packet != nil:
		kind = "packet"
	case msg.Frame != nil:
		kind = "frame"
	}
	return &BuildError{
		Code:      errcode.RuntimeBranchInvalid,
		Operation: "sync media timeline",
		Node:      node,
		Reason:    "media message has no valid PTS timebase",
		Details: []string{
			"message=" + firstNonEmpty(kind, "unknown"),
		},
		Suggestions: []string{
			"set Packet.PTS or Frame.PTS with a valid av.TimeBase before the sync gate",
			"declare live stream TimeBase facts on the input or discovered stream",
			"remove .Sync(...) from branches that cannot carry media timestamps",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateSyncPolicyForStream(operation string, branchName string, stream av.Stream, operations []operationSpec) error {
	if !operationSpecsContainSync(operations) {
		return nil
	}
	if stream.TimeBase.Valid() {
		return nil
	}
	if stream.Codec.ClockRate != 0 {
		return nil
	}
	return &BuildError{
		Code:      errcode.RuntimeBranchInvalid,
		Operation: operation,
		Node:      firstNonEmpty(branchName, string(stream.ID), "branch"),
		Reason:    "sync branches need stream timebase facts before graph mutation",
		Suggestions: []string{
			"set av.Stream.TimeBase on the InputStream or discovered stream",
			"set Codec.ClockRate so RTP-style timestamps can derive a timebase",
			"remove .Sync(...) from dynamic branches whose source has no media timeline",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func operationSpecsContainSync(operations []operationSpec) bool {
	for i := range operations {
		if _, ok := operations[i].Stage.(*syncGate); ok {
			return true
		}
	}
	return false
}

func syncStageOperationName(operation operationSpec, branchName string) string {
	if _, ok := operation.Stage.(*syncGate); !ok {
		if operation.Stage != nil {
			return operation.Stage.Name()
		}
		return operation.Component
	}
	return syncStageName(firstNonEmpty(branchName, operation.Component, "branch"))
}

var _ pipeline.Stage = (*syncGate)(nil)
var _ pipeline.NodeDescriber = (*syncGate)(nil)
var _ pipeline.DropReporter = (*syncGate)(nil)
var _ shape.Contract = (*syncGate)(nil)
