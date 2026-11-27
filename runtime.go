package goav

import (
	"context"
	"errors"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/stage"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/provider"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// Internal build-refusal causes. Public build-shape matching lives on
// BuildError.Family and BuildError.Code; these only keep package-internal
// errors.Is checks and low-level unwrap chains stable.
var (
	// errNilSource reports a nil source handed to a builder or constructor.
	errNilSource = errors.New("goav: nil source")
	// errNilReader reports a nil reader handed to FileInput.
	errNilReader = errors.New("goav: nil input reader")
	// errNilStage reports a nil stage handed to a builder or .Do(...).
	errNilStage = builderr.ErrNilStage
	// errNilSink reports a nil sink handed to a builder or goav.Sink(...).
	errNilSink = errors.New("goav: nil sink")
	// errNilWriter reports a nil writer handed to a byte destination.
	errNilWriter = errors.New("goav: nil writer")
	// errNilDestinationProvider reports a nil provider.Destination handed to Custom.
	errNilDestinationProvider = errors.New("goav: nil destination provider")
)

// Runtime flow-control sentinels, surfaced on the front door so source.Func,
// component.PacketFunc, component.FrameFunc, or component.SinkFunc can react to
// backpressure and shutdown with errors.Is — without importing the pipeline
// package. They share identity with the values the runtime returns, so errors.Is
// matches either name.
var (
	// ErrBackpressure is returned by source.Push/component.Emit when a
	// downstream buffer is full and the message was not delivered. On a Blocking
	// branch the producer is paced instead; on a dropping branch the message is
	// shed and the producer may keep going. A source that can pace itself should
	// slow down on this.
	ErrBackpressure = pipeline.ErrBackpressure
	// ErrClosed is returned when emitting into a graph or stage that has shut
	// down (the task stopped, or its context was cancelled). A source should
	// treat it as a signal to return cleanly.
	ErrClosed = pipeline.ErrClosed
)

// New builds a bare runtime: per-runtime registries with no adapters beyond
// content sniffing, realtime pacing off (live inputs pace automatically; see
// runconfig.WithRealtime). Import github.com/thesyncim/goav/bundle for a
// runtime with the bundled adapters already registered. Runtime options live
// in github.com/thesyncim/goav/runconfig.
func New(options ...runconfig.Option) (*Runtime, error) {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return nil, err
	}
	var pools *stage.MediaPools
	if config.MediaPools {
		pools = stage.NewMediaPools()
	}
	return &Runtime{
		codecs:           config.Codecs,
		filters:          config.Filters,
		formats:          config.Formats,
		buffer:           config.Buffer,
		realtime:         config.Realtime,
		clock:            config.Clock,
		eventCapacity:    config.EventCapacity,
		closeWaitTimeout: config.CloseWaitTimeout,
		mediaPools:       pools,
		shapeDeltas:      append([]runconfig.ShapeDeltaContributor(nil), config.ShapeDeltas...),
		qosPolicy:        config.QoSPolicy,
		schemes:          maps.Clone(config.Schemes),
	}, nil
}

func mustNew(options ...runconfig.Option) *Runtime {
	runtime, err := New(options...)
	if err != nil {
		panic(err)
	}
	return runtime
}

// Runtime is the composition root for applications embedding goav: it holds
// the codec, format, and filter registries, the buffer and pool settings, and
// the clock that tasks run with. Build it with New, or import
// github.com/thesyncim/goav/bundle for the bundled adapter set. Applications
// run Outputs on it with Run (one shot) or Build (a Task), and inspect
// them without opening anything with Describe and Explain. Its fields are
// unexported: construction and adapter registration go through runconfig
// options.
type Runtime struct {
	codecs           *codec.Registry
	filters          *filter.Registry
	formats          *format.Registry
	buffer           pipeline.BufferPolicy
	realtime         bool
	clock            av.Clock
	eventCapacity    int
	closeWaitTimeout time.Duration
	mediaPools       *stage.MediaPools
	shapeDeltas      []runconfig.ShapeDeltaContributor
	// qosPolicy is the opt-in per-runtime QoS policy started on every task
	// this runtime builds (runconfig.WithQoSPolicy).
	qosPolicy func(av.Event) []control.Control
	// schemes resolve URIInput and URI values (runconfig.WithScheme).
	schemes map[string]provider.Scheme
}

// Probe asks the runtime's registered probers which container format a
// request names, the same lookup inputs and destinations resolve through.
func (r *Runtime) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	return r.formats.Probe(ctx, request)
}

// taskLifecycle groups the task's execution and shutdown state guarded by
// lifecycleMu. lifecycleMu is never held while waiting for graph work.
// Lock order: attachMu may be acquired before lifecycleMu; the reverse is forbidden.
type taskLifecycle struct {
	lifecycleMu            sync.Mutex
	closeGate              chan struct{}
	phase                  taskPhase
	runActive              bool
	closeAbandoned         bool
	attachmentClosePending bool
	runErr                 error
	closeErr               error
	activeAttachments      map[*runtimeAttachment]struct{}
}

// taskAttachments groups live branch attachment state guarded by attachMu.
type taskAttachments struct {
	attachMu    contextMutex
	attachments map[*runtimeAttachment]struct{}
	branchTaps  []TapSnapshot
	isolated    []*isolatedBranch
}

// Task is a built media composition produced by Runtime.Build: it runs and
// closes the graph, reports plans/specs/snapshots/stats, and supports live
// branch attach/detach, out-of-band controls, pause/resume, and event watches.
type Task struct {
	graph   pipeline.Graph
	runtime *Runtime
	// realtime and buffer hold the task's resolved execution policy so the
	// shared Runtime stays immutable across tasks.
	realtime  bool
	buffer    pipeline.BufferPolicy
	bufferSet bool
	qos       *qosReporter
	controls  *taskControls
	// timeline is the task-wide clock service every paced source sleeps on;
	// control.Rate scales it so all paced delivery re-anchors together. Nil
	// only for tasks assembled outside the build paths (unit-test graphs).
	timeline         *stage.Timeline
	destinations     []*destinationTransaction
	rootDestinations []compile.WorkDestination
	// encoders are the encode stages the build opened; stream-addressed
	// encoder controls go to the one that produces the stream.
	encoders           []*codec.EncoderStage
	taps               []TapSnapshot
	explainReport      plan.Report
	explainReportReady bool

	// watch fans the graph's event stream out to filtered Watch subscribers.
	watch eventWatch

	// rules is the dynamic-stream rule engine (OnStream grammar), installed at
	// build before the task escapes; nil when the job declared no rules.
	rules *taskStreamRules

	taskAttachments
	taskLifecycle
}

type taskPhase uint8

const (
	taskBuilt taskPhase = iota
	taskRunning
	taskRunDone
	taskClosing
	taskFinalizing
	taskClosed
)

func newTask(graph pipeline.Graph, runtime *Runtime, destinations ...*destinationTransaction) *Task {
	return newTaskWithRootDestinations(graph, runtime, nil, destinations...)
}

func newTaskWithRootDestinations(graph pipeline.Graph, runtime *Runtime, rootDestinations []compile.WorkDestination, destinations ...*destinationTransaction) *Task {
	t := &Task{
		graph:            graph,
		runtime:          runtime,
		destinations:     destinations,
		rootDestinations: compile.CloneWorkDestinations(rootDestinations),
	}
	for _, tx := range destinations {
		t.bindDestination(tx, nil)
	}
	return t
}

func (t *Task) Spec() pipeline.Spec {
	return t.graph.Spec()
}

func (t *Task) Explain(context.Context) (plan.Report, error) {
	if t.explainReportReady {
		report := clonePlanReport(t.explainReport)
		report.Graph = t.Spec()
		report.Taps = planTapRows(t.Taps())
		return report, nil
	}
	return plan.Report{
		Summary: "running media task",
		Graph:   t.Spec(),
		Taps:    planTapRows(t.Taps()),
	}, nil
}

// EncoderDescriptors lists encoder factories registered on the task runtime.
func (t *Task) EncoderDescriptors() []codec.Descriptor {
	if t == nil || t.runtime == nil || t.runtime.codecs == nil {
		return nil
	}
	descriptors, err := t.runtime.codecs.Find("", codec.ModeEncode)
	if err != nil {
		return nil
	}
	out := make([]codec.Descriptor, 0, len(descriptors))
	seen := make(map[av.CodecID]struct{}, len(descriptors))
	for _, desc := range descriptors {
		if desc.ID == "" {
			continue
		}
		if _, ok := seen[desc.ID]; ok {
			continue
		}
		if _, err := t.runtime.codecs.EncoderFactory(desc.ID); err != nil {
			continue
		}
		out = append(out, desc)
		seen[desc.ID] = struct{}{}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Type < out[j].Type
	})
	return out
}

// MuxerDescriptors lists muxer factories registered on the task runtime.
func (t *Task) MuxerDescriptors() []format.Descriptor {
	if t == nil || t.runtime == nil || t.runtime.formats == nil {
		return nil
	}
	return t.runtime.formats.MuxerDescriptors()
}

func (t *Task) Run(ctx context.Context) error {
	t.lifecycleMu.Lock()
	switch t.phase {
	case taskBuilt:
		t.phase = taskRunning
		t.runActive = true
		t.lifecycleMu.Unlock()
	case taskRunning, taskRunDone:
		t.lifecycleMu.Unlock()
		return errors.New("goav: task already started")
	default:
		t.lifecycleMu.Unlock()
		return pipeline.ErrClosed
	}
	err := t.graph.Run(ctx)
	t.lifecycleMu.Lock()
	if t.phase >= taskClosing {
		err = withoutShutdownError(err)
	}
	// Caller cancellation remains observable even when the graph's own
	// shutdown sentinel was the source's return value.
	if ctxErr := ctx.Err(); ctxErr != nil && (err == nil || errors.Is(err, ctxErr)) {
		err = ctxErr
	}
	t.finishDestinationsLocked(err)
	t.runActive = false
	t.runErr = err
	if t.phase == taskRunning {
		t.phase = taskRunDone
	}
	t.lifecycleMu.Unlock()
	return err
}

func (t *Task) Stats() pipeline.GraphStats {
	return t.graph.Stats()
}

func (t *Task) Taps() []TapSnapshot {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.tapsLocked()
}

func (t *Task) Snapshot() TaskSnapshot {
	if t == nil {
		return TaskSnapshot{}
	}
	stats := t.Stats()
	cleanupErr := taskGraphCleanupError(t.graph)
	t.lifecycleMu.Lock()
	t.closeErr = cleanupErr
	t.promoteClosedLocked(stats.PendingCleanup)
	outcome := errors.Join(t.runErr, t.closeErr)
	state, destinationState := t.lifecycleStatesLocked(outcome)
	t.lifecycleMu.Unlock()
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	t.pruneIsolatedLocked()
	branches := make([]BranchSnapshot, 0, len(t.attachments))
	for attachment := range t.attachments {
		state := attachment.branchSnapshotLocked(stats)
		if state.ID == "" && state.Name == "" {
			continue
		}
		branches = append(branches, state)
	}
	for _, branch := range t.isolated {
		if branch.attachment != nil {
			if _, active := t.attachments[branch.attachment]; active || !branch.domain.Failed() {
				continue
			}
		}
		branches = append(branches, t.isolatedSnapshot(branch, stats))
	}
	return TaskSnapshot{
		State:        state,
		Err:          outcome,
		Error:        errorText(outcome),
		Paused:       t.timeline != nil && t.timeline.PausedNow(),
		Spec:         t.Spec(),
		Stats:        stats,
		Taps:         t.tapsLocked(),
		Branches:     branches,
		Destinations: taskSnapshotDestinations(t.destinationSnapshots(t.rootDestinations, t.destinations, destinationState), branches),
	}
}

func (t *Task) promoteClosedLocked(pendingCleanup int) {
	if t.phase != taskFinalizing {
		return
	}
	if !t.attachmentClosePending && pendingCleanup == 0 {
		t.phase = taskClosed
	}
}

func (t *Task) lifecycleStates() (TaskState, DestinationState) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	return t.lifecycleStatesLocked(errors.Join(t.runErr, t.closeErr))
}

// lifecycleStatesLocked derives the typed task state and the matching state of
// the task's still-open destinations from the explicit taskPhase under
// lifecycleMu. Run only determines whether finalization may commit; Close and
// the individual destination ledgers record what actually happened.
func (t *Task) lifecycleStatesLocked(outcome error) (TaskState, DestinationState) {
	switch t.phase {
	case taskBuilt:
		return TaskBuilt, DestinationOpen
	case taskRunning:
		return TaskRunning, DestinationOpen
	case taskRunDone, taskClosing, taskFinalizing:
		if outcome != nil {
			return TaskFailed, DestinationFinalizing
		}
		return TaskFinalizing, DestinationFinalizing
	case taskClosed:
		if t.runActive {
			if outcome != nil {
				return TaskFailed, DestinationFinalizing
			}
			return TaskFinalizing, DestinationFinalizing
		}
		if outcome != nil {
			return TaskFailed, DestinationClosed
		}
		return TaskClosed, DestinationClosed
	default:
		return TaskBuilt, DestinationOpen
	}
}

func taskSnapshotDestinations(root []DestinationSnapshot, branches []BranchSnapshot) []DestinationSnapshot {
	if len(root) == 0 && len(branches) == 0 {
		return nil
	}
	out := make([]DestinationSnapshot, 0)
	seen := make(map[string]struct{})
	for i := range root {
		destination := root[i]
		key := destination.Name + "\x00" + string(destination.Operation) + "\x00" + destination.Component
		if destination.ID != "" {
			key = destination.ID
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, destination)
	}
	for i := range branches {
		for j := range branches[i].Destinations {
			destination := branches[i].Destinations[j]
			key := destination.Name + "\x00" + string(destination.Operation) + "\x00" + destination.Component
			if destination.ID != "" {
				key = destination.ID
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, destination)
		}
	}
	return out
}

func (t *Task) Detach(ctx context.Context, attachment Attachment, options ...DetachOption) error {
	input, err := runtimeDetachInputFromArgs(ctx, attachment, options)
	if err != nil {
		return err
	}
	return t.detachRuntimeAttachment(ctx, input)
}

type runtimeDetachInput struct {
	attachment  Attachment
	runtime     *runtimeAttachment
	disposition oldBranchDisposition
}

func runtimeDetachInputForRuntimeAttachment(attachment *runtimeAttachment, disposition oldBranchDisposition) runtimeDetachInput {
	return runtimeDetachInput{
		attachment:  attachment,
		runtime:     attachment,
		disposition: disposition,
	}
}

func runtimeDetachInputFromArgs(ctx context.Context, attachment Attachment, options []DetachOption) (runtimeDetachInput, error) {
	if err := ctx.Err(); err != nil {
		return runtimeDetachInput{}, err
	}
	if attachment == nil {
		return runtimeDetachInput{}, nilAttachmentDetachError()
	}
	policy := detachPolicyFromOptions(options)
	input := runtimeDetachInput{
		attachment:  attachment,
		disposition: policy.disposition,
	}
	if runtimeAttachment, ok := attachment.(*runtimeAttachment); ok {
		input = runtimeDetachInputForRuntimeAttachment(runtimeAttachment, policy.disposition)
	}
	return input, nil
}

func (t *Task) detachRuntimeAttachment(ctx context.Context, input runtimeDetachInput) error {
	if input.runtime != nil {
		return t.stopAttachment(ctx, input)
	}
	return input.attachment.Close(ctx)
}

func nilAttachmentDetachError() error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: "detach runtime branch",
		Reason:    "attachment is nil",
		fixes: buildErrorFixes([]string{
			"keep the Attachment returned by Task.Attach and pass it to Task.Detach",
			"skip Detach when no branch is attached",
		}),
		cause: errUnsupportedBuild,
	}
}

// stopAttachments keeps removal's upstream-first queue drain and final stats.
// A deadline bounds both the attachment lock and each built-in node wait.
func (t *Task) stopAttachments(ctx context.Context) (bool, error) {
	if err := t.attachMu.LockContext(ctx); err != nil {
		return false, err
	}
	defer t.attachMu.Unlock()
	err := t.stopAttachmentsLocked(ctx)
	// This caller may have completed the bookkeeping before its previously
	// scheduled shutdown pass acquired attachMu. That pass is now a no-op.
	t.lifecycleMu.Lock()
	t.attachmentClosePending = false
	t.lifecycleMu.Unlock()
	return true, err
}

func (t *Task) stopAttachmentsLocked(ctx context.Context) error {
	t.lifecycleMu.Lock()
	disposition := oldBranchDetach
	if t.closeAbandoned {
		disposition = oldBranchAbort
	}
	t.lifecycleMu.Unlock()
	var first error
	for attachment := range t.attachments {
		input := runtimeDetachInputForRuntimeAttachment(attachment, disposition)
		first = errorsJoinUnique(first, t.stopAttachmentLocked(ctx, input))
	}
	return first
}

// A concurrent Attach or Detach may own attachMu past this Close's deadline.
// The graph is already shutting down; this pass retires the remaining handles
// after that operation releases the lock. It never waits for handler cleanup.
func (t *Task) finishAttachmentShutdown() {
	t.lifecycleMu.Lock()
	if t.attachmentClosePending {
		t.lifecycleMu.Unlock()
		return
	}
	t.attachmentClosePending = true
	t.lifecycleMu.Unlock()
	go func() {
		t.attachMu.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = t.stopAttachmentsLocked(ctx)
		t.attachMu.Unlock()
		stats := t.graph.Stats()
		t.lifecycleMu.Lock()
		t.attachmentClosePending = false
		if t.phase == taskFinalizing && stats.PendingCleanup == 0 {
			t.phase = taskClosed
		}
		t.lifecycleMu.Unlock()
	}()
}

func (t *Task) Close() error {
	return t.CloseContext(context.Background())
}

func (t *Task) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return errorsJoinUnique(taskGraphCleanupError(t.graph), err)
	}
	t.lifecycleMu.Lock()
	if t.closeGate == nil {
		t.closeGate = make(chan struct{}, 1)
	}
	gate := t.closeGate
	t.lifecycleMu.Unlock()
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return errorsJoinUnique(taskGraphCleanupError(t.graph), ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return errorsJoinUnique(taskGraphCleanupError(t.graph), err)
	}
	t.lifecycleMu.Lock()
	if t.phase < taskClosing {
		t.closeAbandoned = t.phase != taskRunDone
		t.phase = taskClosing
		// One ordered decision covers roots and every still-owned attachment,
		// including a patch being installed or a drain still waiting for
		// queued work. Completed detachments have relinquished ownership.
		if t.closeAbandoned {
			for _, tx := range t.destinations {
				tx.Fail()
			}
		}
		for attachment := range t.activeAttachments {
			if t.closeAbandoned || attachment.phase == attachDraining {
				attachment.phase = attachAborting
				attachment.applyDetachDisposition(oldBranchAbort)
			}
		}
	}
	t.lifecycleMu.Unlock()
	stopped, first := t.stopAttachments(ctx)
	if contextGraph, ok := t.graph.(pipeline.ContextCloser); ok {
		first = errors.Join(first, contextGraph.CloseContext(ctx))
	} else {
		first = errors.Join(first, ctx.Err(), t.graph.Close())
	}
	if !stopped {
		t.finishAttachmentShutdown()
	}
	stats := t.graph.Stats()
	cleanupErr := taskGraphCleanupError(t.graph)
	t.lifecycleMu.Lock()
	if !t.attachmentClosePending && stats.PendingCleanup == 0 {
		t.phase = taskClosed
	} else if t.phase == taskClosing {
		t.phase = taskFinalizing
	}
	t.closeErr = cleanupErr
	outcome := errorsJoinUnique(t.closeErr, closeCallError(first, t.closeErr))
	outcome = errorsJoinUnique(outcome, ctx.Err())
	t.lifecycleMu.Unlock()
	return outcome
}

// compileEnv is the view of the runtime the compiler reads.
func (r *Runtime) compileEnv() compile.Env {
	if r == nil {
		return compile.Env{}
	}
	return compile.Env{
		Present:     true,
		Codecs:      r.codecs,
		Filters:     r.filters,
		Formats:     r.formats,
		Buffer:      r.buffer,
		Realtime:    r.realtime,
		ShapeDeltas: r.shapeDeltas,
	}
}

func (t *Task) effectiveRealtime() bool {
	if t == nil {
		return false
	}
	return t.realtime || (t.runtime != nil && t.runtime.realtime)
}

func (t *Task) effectiveBuffer() pipeline.BufferPolicy {
	if t == nil {
		return pipeline.BufferPolicy{}
	}
	if t.bufferSet {
		return t.buffer
	}
	if t.runtime != nil {
		return t.runtime.buffer
	}
	return pipeline.BufferPolicy{}
}

func (t *Task) compileEnv() compile.Env {
	if t == nil || t.runtime == nil {
		return compile.Env{}
	}
	env := t.runtime.compileEnv()
	env.Realtime = t.effectiveRealtime()
	env.Buffer = t.effectiveBuffer()
	return env
}

// newLoweringBuilder constructs a per-operation builder carrying this task's
// resolved execution environment (clock, realtime, buffer, QoS, controls) so
// runtime attach shares the exact lowering setup of initial build.
func (t *Task) newLoweringBuilder() *builder {
	return &builder{
		runtime:   t.runtime,
		clock:     t.playoutClock(),
		realtime:  t.effectiveRealtime(),
		buffer:    t.effectiveBuffer(),
		bufferSet: true,
		qos:       t.qos,
		controls:  t.controls,
	}
}
