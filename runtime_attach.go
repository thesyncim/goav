package goav

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

var runtimeAttachmentSeq atomic.Uint64

// attachDestination is one validated destination of an attaching branch: the
// cloned spec, its sink, and the share key that groups reused destination
// values or explicit destination groups inside one Mutable.Attach call.
type attachDestination struct {
	name     string
	dest     destinationSpec
	sink     pipeline.Sink
	shareKey string
}

type runtimeBranchGroupDestinations struct {
	sharedSinkKeys map[string]struct{}
	sharedMuxKeys  map[string]struct{}
}

type runtimeAttachGroup struct {
	destinations runtimeBranchGroupDestinations
	reserved     map[string]struct{}
	sharedSinks  map[string]*runtimeSharedSinkDestination
	sharedMuxes  map[string]*runtimeSharedMuxDestination
	muxOrder     []string
}

type runtimeSharedSinkDestination struct {
	name   string
	sink   pipeline.Sink
	ref    pipeline.NodeRef
	buffer pipeline.BufferPolicy
}

type runtimeSharedMuxDestination struct {
	name     string
	dest     destinationSpec
	streams  []av.Stream
	branches []string
	stage    *format.MuxStage
	ref      pipeline.NodeRef
	routes   []pipeline.Route
	buffer   pipeline.BufferPolicy
}

type runtimeGraphPatch struct {
	nodes             []pipeline.NodeRef
	routes            []pipeline.Route
	taps              []snapshot.Tap
	anchorTaps        []string
	anchorNodes       []string
	stages            []pipeline.Stage
	copyNeverBranches []string
	work              workPatch
}

func (p *runtimeGraphPatch) addAnchor(tap string, from string) {
	if tap != "" {
		p.anchorTaps = append(p.anchorTaps, tap)
	}
	if from != "" {
		p.anchorNodes = append(p.anchorNodes, from)
	}
}

func (p *runtimeGraphPatch) addApplied(nodes []pipeline.NodeRef, routes []pipeline.Route, taps []snapshot.Tap) {
	p.nodes = append(p.nodes, nodes...)
	p.routes = append(p.routes, routes...)
	p.taps = append(p.taps, taps...)
}

func (p *runtimeGraphPatch) addPlannedTaps(taps []snapshot.Tap) {
	p.taps = append(p.taps, taps...)
}

func (p *runtimeGraphPatch) setWork(work workPatch) {
	p.work = cloneWorkPatch(work)
}

func (p *runtimeGraphPatch) resetPlannedTaps() {
	p.taps = p.taps[:0]
}

func (p runtimeGraphPatch) rollback(task *task) {
	if task != nil {
		task.rollbackRuntimeBranch(p.nodes)
	}
}

func (p runtimeGraphPatch) attachment(owner *task, name string) *runtimeAttachment {
	return &runtimeAttachment{
		id:          nextRuntimeAttachmentID(name),
		name:        name,
		owner:       owner,
		anchorTap:   firstNonEmpty(p.anchorTaps...),
		anchorNode:  firstNonEmpty(p.anchorNodes...),
		anchorTaps:  uniqueStrings(p.anchorTaps),
		anchorNodes: uniqueStrings(p.anchorNodes),
		nodes:       append([]pipeline.NodeRef(nil), p.nodes...),
		routes:      append([]pipeline.Route(nil), p.routes...),
		taps:        append([]snapshot.Tap(nil), p.taps...),
		stages:      append([]pipeline.Stage(nil), p.stages...),
		copyNever:   uniqueStrings(p.copyNeverBranches),
		work:        cloneWorkPatch(p.work),
	}
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	// ID is the task-unique attachment identifier; Name is the branch name
	// the BranchSpec declared (a name may be reused across attachments).
	ID() string
	Name() string
	// Spec returns the branch's private graph patch as a structured spec.
	Spec() pipeline.Spec
	// Stats returns branch-scoped counters; Snapshot returns the branch-owned
	// point-in-time view (lifecycle state, stats, published taps).
	Stats() pipeline.GraphStats
	Snapshot() snapshot.Branch
	// Pause stops delivery to this branch without touching the source or its
	// siblings; messages are skipped while paused. Resume restores delivery.
	Pause(context.Context) error
	Resume(context.Context) error
	// Rebranch replaces this branch with new ones in place: the replacements are
	// attached and start receiving before this branch is detached (no gap), so a
	// live subscriber can be switched (e.g. to a different simulcast layer)
	// without rebuilding the task. On attach failure this branch is left intact
	// by default. Pass replacement BranchSpec values plus lifecycle policies:
	// lifecycle.SwitchAt(lifecycle.NextFrame()/lifecycle.NextKeyframe()) delays
	// the switch to that stream boundary — the replacements shed media until the
	// boundary and this branch detaches at it — and
	// lifecycle.DrainOldBranch/lifecycle.AbortOldBranch select whether this
	// branch's destinations commit or abort on detach.
	// Without options the switch is immediate, exactly like Detach after Attach.
	Rebranch(context.Context, ...lifecycle.RebranchArg) (Attachment, error)
	// Close detaches this branch and any dependent branches anchored on its
	// taps; equivalent to Mutable.Detach.
	Close(context.Context) error
}

// Attach compiles each BranchSpec through the shared operation-chain lowering
// into a workPatch — the plan — opens its destinations, and then applies the
// patch atomically to the running graph: a failed attach rolls every applied
// node back and leaves the task unchanged.
func (t *task) Attach(ctx context.Context, specs ...BranchSpec) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, runtimeBranchInvalidError("no runtime branches to attach", "pass one or more goav.Branch(name)...To(destination) values")
	}
	destinations := make([][]attachDestination, len(specs))
	for i := range specs {
		branchDestinations, err := attachBranchDestinations(specs[i])
		if err != nil {
			return nil, err
		}
		if err := validateAttachBranchSpec(specs[i], branchDestinations); err != nil {
			return nil, err
		}
		destinations[i] = branchDestinations
	}
	groupDestinations, err := validateRuntimeBranchGroupDestinations(specs, destinations)
	if err != nil {
		return nil, err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	group := newRuntimeAttachGroup(groupDestinations)
	ap := newAttachPlan()
	var patch runtimeGraphPatch
	rollback := func(err error) (Attachment, error) {
		ap.closeOwned()
		group.failSharedMuxStages()
		group.closeSharedMuxStages()
		patch.rollback(t)
		return nil, err
	}
	for i := range specs {
		graphSpec := t.graph.Spec()
		from, anchor, err := t.resolveRuntimeBranchAnchor(specs[i], graphSpec, patch.taps)
		if err != nil {
			return rollback(err)
		}
		patch.addAnchor(specs[i].source.tap, from)
		steps, err := t.planAttachBranchSteps(ctx, specs[i], destinations[i], anchor, group)
		if err != nil {
			return rollback(err)
		}
		index := ap.registerBranch(specs[i], from, steps)
		if err := t.validateAttachBranchTapsLocked(steps, patch.taps); err != nil {
			return rollback(err)
		}
		if err := t.configureAttachBranchBuffer(&ap.branches[index], specs[i], steps); err != nil {
			return rollback(err)
		}
		if specs[i].branchBuffer.CopyMode == flow.CopyNever {
			patch.copyNeverBranches = append(patch.copyNeverBranches, firstNonEmpty(specs[i].name, "branch"))
		}
		if err := ap.finalizeBranch(index, specs[i], destinations[i], anchor, graphSpec, group, steps); err != nil {
			return rollback(err)
		}
		patch.addPlannedTaps(ap.branches[index].taps)
	}
	if err := group.prepareSharedMuxStages(ctx, t.runtime); err != nil {
		return rollback(err)
	}
	name := runtimeAttachmentName(specs)
	ap.work.Name = firstNonEmpty(name, "runtime-attach")
	ap.work.Rollback = workPatchRollbackFromBranches(ap.work.Operations, ap.work.Destinations)
	patch.setWork(ap.work)
	patch.resetPlannedTaps()
	for i := range ap.branches {
		branchRefs, branchRoutes, branchTaps, err := t.applyAttachBranch(ap, ap.branches[i], group)
		if err != nil {
			patch.addApplied(branchRefs, nil, nil)
			return rollback(err)
		}
		patch.addApplied(branchRefs, branchRoutes, branchTaps)
	}
	sharedRefs, sharedRoutes, err := group.attachSharedMuxDestinations(t.graph)
	if err != nil {
		patch.addApplied(sharedRefs, sharedRoutes, nil)
		return rollback(err)
	}
	patch.addApplied(sharedRefs, sharedRoutes, nil)
	patch.stages = attachmentStages(ap, group, patch.nodes)
	attachment := patch.attachment(t, name)
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(patch.taps)
	t.publishBranchLifecycleEvent(av.EventBranchAttached, attachment, oldBranchDetach)
	return attachment, nil
}

func (t *task) configureAttachBranchBuffer(branch *attachPlanBranch, spec BranchSpec, steps []attachStep) error {
	if branch == nil {
		return nil
	}
	if _, ok := t.graph.(pipeline.NodeInjector); !ok {
		return nil
	}
	if spec.branchBuffer.CopyMode == flow.CopyNever {
		return nil
	}
	policy := branch.buffer
	var runtimeBuffer pipeline.BufferPolicy
	if t.runtime != nil {
		runtimeBuffer = t.runtime.buffer
	}
	if policy.IsDirect() {
		policy = realtimeRecipeBufferPolicy(runtimeBuffer)
	} else {
		// An explicit branch buffer (flow.DropOldest/Latest/...) still needs the
		// copy bounds the runtime already sized for this graph's shapes. A direct
		// branch inherits them through realtimeRecipeBufferPolicy above; an explicit
		// one must inherit them too, or a buffered edge would refuse mutable
		// payloads at runtime (ErrBufferedMessageUnsafe) unless the caller redundantly
		// passed flow.BufferCopyBounds. Only fill bounds the caller left unset — a
		// caller that sized a bound deliberately (even smaller) keeps it.
		if policy.CopyPacketBytes == 0 {
			policy.CopyPacketBytes = runtimeBuffer.CopyPacketBytes
		}
		if policy.CopyFrameBytes == 0 {
			policy.CopyFrameBytes = runtimeBuffer.CopyFrameBytes
		}
	}
	if policy.IsDirect() {
		return nil
	}
	var err error
	policy, err = bufferPolicyWithShapeBudgetsForSteps(policy, steps)
	if err != nil {
		return err
	}
	branch.buffer = policy
	return nil
}

// attachmentStages binds the applied nodes back to their prepared stages so a
// later disposition detach (DrainOldBranch/AbortOldBranch) can finalize the
// branch's destinations explicitly instead of relying on the default close.
func attachmentStages(ap *attachPlan, group *runtimeAttachGroup, nodes []pipeline.NodeRef) []pipeline.Stage {
	if ap == nil {
		return nil
	}
	out := make([]pipeline.Stage, 0, len(nodes))
	for _, node := range nodes {
		if component, ok := ap.components[node]; ok && component.stage != nil {
			out = append(out, component.stage)
			continue
		}
		if group == nil {
			continue
		}
		if key, ok := group.sharedMuxKeyForNode(node); ok {
			if target := group.sharedMuxes[key]; target != nil && target.stage != nil {
				out = append(out, target.stage)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// attachBranchDestinations validates and clones the branch's destinations,
// carrying the share keys that group reused destination values or explicit
// destination groups.
func attachBranchDestinations(spec BranchSpec) ([]attachDestination, error) {
	if spec.err != nil {
		return nil, spec.err
	}
	if len(spec.destinations) == 0 {
		return nil, runtimeBranchInvalidError("branch destination is missing", "finish the branch with .To(goav.Sink(sink)) or .To(goav.File(name, writer))")
	}
	destinations := make([]attachDestination, 0, len(spec.destinations))
	for i := range spec.destinations {
		ref := cloneDestinationRef(spec.destinations[i])
		destination := cloneDestinationSpec(ref.dest)
		name := firstNonEmpty(ref.name, spec.name, "branch")
		if err := destination.validate("attach runtime branch", name); err != nil {
			return nil, err
		}
		destinations = append(destinations, attachDestination{
			name:     name,
			dest:     destination,
			sink:     destination.sink,
			shareKey: runtimeBranchSharedDestinationKey(ref),
		})
	}
	return destinations, nil
}

func validateAttachBranchSpec(spec BranchSpec, destinations []attachDestination) error {
	if spec.name == "" {
		return runtimeBranchInvalidError("branch name is empty", "start with goav.Branch(\"name\")")
	}
	if spec.source.from == "" && spec.source.tap == "" {
		return runtimeBranchInvalidError("branch source is empty", "call .From(input.Stream(stream)) for an input stream, .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) with a tap from Inspectable.Taps(), or .From(graphNode) with an expert graph handle")
	}
	if spec.source.stream != nil && spec.source.label == "" {
		return runtimeBranchInvalidError("branch source stream id is empty", "set av.Stream.ID before passing the stream to input.Stream(stream)")
	}
	seen := make(map[string]int, len(destinations))
	for i := range destinations {
		name := firstNonEmpty(destinations[i].name, destinations[i].dest.label(fmt.Sprintf("target%d", i+1)))
		if firstIndex, ok := seen[name]; ok {
			return duplicateRuntimeBranchDestinationRefError(spec.name, name, firstIndex, i)
		}
		seen[name] = i
	}
	return nil
}

func validateRuntimeBranchGroupDestinations(specs []BranchSpec, destinations [][]attachDestination) (runtimeBranchGroupDestinations, error) {
	var group runtimeBranchGroupDestinations
	seen := make(map[string]attachDestination)
	seenBranch := make(map[string]string)
	for i := range destinations {
		branchName := firstNonEmpty(specs[i].name, fmt.Sprintf("branch-%d", i+1))
		for j := range destinations[i] {
			destination := destinations[i][j]
			label := destination.name
			if label == "" {
				continue
			}
			if first, ok := seen[label]; ok {
				if first.shareKey != "" && first.shareKey == destination.shareKey {
					switch {
					case runtimeBranchDestinationCanShareSink(first) && runtimeBranchDestinationCanShareSink(destination):
						if group.sharedSinkKeys == nil {
							group.sharedSinkKeys = make(map[string]struct{})
						}
						group.sharedSinkKeys[first.shareKey] = struct{}{}
						continue
					case runtimeBranchDestinationCanShareMux(first) && runtimeBranchDestinationCanShareMux(destination):
						if group.sharedMuxKeys == nil {
							group.sharedMuxKeys = make(map[string]struct{})
						}
						group.sharedMuxKeys[first.shareKey] = struct{}{}
						continue
					}
				}
				return group, &BuildError{
					Family:    errcode.FamilyForCode(errcode.DestinationDuplicate),
					Code:      errcode.DestinationDuplicate,
					Operation: "attach runtime branches",
					Node:      firstNonEmpty(specs[i].name, "branch"),
					Reason:    "runtime branch group reuses one destination name",
					Fields: buildErrorFields([]string{
						"destination: " + label,
						"first branch: " + seenBranch[label],
						"second branch: " + branchName,
					}),
					Fixes: buildErrorFixes([]string{
						"reuse one destination value or wrap each branch destination with goav.Mux(name, destination) for a shared runtime destination group",
						"create distinct destination values with distinct names for independent runtime destinations",
						"use a sink destination for runtime diagnostic groups or a mux destination for runtime recording groups",
					}),
					Cause: ErrUnsupportedBuild,
				}
			}
			seen[label] = destination
			seenBranch[label] = branchName
		}
	}
	return group, nil
}

func runtimeBranchDestinationCanShareSink(destination attachDestination) bool {
	return destination.sink != nil && !destinationSpecHasOutput(destination.dest)
}

func runtimeBranchDestinationCanShareMux(destination attachDestination) bool {
	return destination.sink == nil && destinationSpecHasOutput(destination.dest)
}

func runtimeBranchSharedDestinationKey(target destinationRef) string {
	return destinationShareKey(target.dest, target.id)
}

func newRuntimeAttachGroup(destinations runtimeBranchGroupDestinations) *runtimeAttachGroup {
	return &runtimeAttachGroup{
		destinations: destinations,
		reserved:     make(map[string]struct{}),
		sharedSinks:  make(map[string]*runtimeSharedSinkDestination),
		sharedMuxes:  make(map[string]*runtimeSharedMuxDestination),
	}
}

func (g *runtimeAttachGroup) isSharedSink(key string) bool {
	if g == nil || key == "" || len(g.destinations.sharedSinkKeys) == 0 {
		return false
	}
	_, ok := g.destinations.sharedSinkKeys[key]
	return ok
}

func (g *runtimeAttachGroup) isSharedMux(key string) bool {
	if g == nil || key == "" || len(g.destinations.sharedMuxKeys) == 0 {
		return false
	}
	_, ok := g.destinations.sharedMuxKeys[key]
	return ok
}

func (g *runtimeAttachGroup) reserveNode(spec pipeline.Spec, name string) error {
	if g == nil {
		return validateRuntimeBranchNodeName(spec, make(map[string]struct{}), name)
	}
	if _, ok := g.reserved[name]; ok {
		return runtimeBranchNodeDuplicateError(name)
	}
	if specHasNode(spec, name) {
		return runtimeBranchNodeDuplicateError(name)
	}
	g.reserved[name] = struct{}{}
	return nil
}

func (g *runtimeAttachGroup) reserveSharedSink(spec pipeline.Spec, key string, destName string, sink pipeline.Sink, buffer pipeline.BufferPolicy) error {
	if g == nil || !g.isSharedSink(key) {
		return nil
	}
	if target, ok := g.sharedSinks[key]; ok {
		target.buffer = mergeBufferCopyBounds(target.buffer, buffer)
		return nil
	}
	name := destName
	if name == "" && sink != nil {
		name = sink.Name()
	}
	name = firstNonEmpty(name, "sink")
	if err := g.reserveNode(spec, name); err != nil {
		return err
	}
	g.sharedSinks[key] = &runtimeSharedSinkDestination{name: name, sink: sink, buffer: buffer}
	return nil
}

func (g *runtimeAttachGroup) sharedSinkRef(graph pipeline.Graph, key string) (pipeline.NodeRef, bool, error) {
	if g == nil || !g.isSharedSink(key) {
		return "", false, runtimeBranchInvalidError("shared sink destination is not registered", "reuse one goav.Sink(sink) destination value inside one Mutable.Attach call")
	}
	target := g.sharedSinks[key]
	if target == nil {
		return "", false, runtimeBranchInvalidError("shared sink destination is not reserved", "reuse one goav.Sink(sink) destination value inside one Mutable.Attach call")
	}
	if target.ref != "" {
		return target.ref, false, nil
	}
	ref, err := graph.AddSink(namedSink{name: target.name, sink: target.sink}, target.buffer)
	if err != nil {
		return "", false, runtimeBranchGraphError("add sink", target.name, err)
	}
	target.ref = ref
	return ref, true, nil
}

func (g *runtimeAttachGroup) reserveSharedMux(spec pipeline.Spec, branchName string, buffer pipeline.BufferPolicy, key string, destName string, dest destinationSpec, stream av.Stream) error {
	if g == nil || !g.isSharedMux(key) {
		return nil
	}
	target, ok := g.sharedMuxes[key]
	if !ok {
		name := firstNonEmpty(destName, dest.label(firstNonEmpty(branchName, "target")), "target")
		if err := g.reserveNode(spec, name); err != nil {
			return err
		}
		target = &runtimeSharedMuxDestination{
			name:   name,
			dest:   dest,
			buffer: buffer,
		}
		g.sharedMuxes[key] = target
		g.muxOrder = append(g.muxOrder, key)
	} else {
		target.buffer = mergeBufferCopyBounds(target.buffer, buffer)
	}
	target.streams = append(target.streams, stream)
	target.branches = append(target.branches, firstNonEmpty(branchName, "branch"))
	return nil
}

func (g *runtimeAttachGroup) sharedSinkKeyForNode(node pipeline.NodeRef) (string, bool) {
	if g == nil {
		return "", false
	}
	for key, destination := range g.sharedSinks {
		if destination != nil && destination.name == node.String() {
			return key, true
		}
	}
	return "", false
}

func (g *runtimeAttachGroup) sharedMuxKeyForNode(node pipeline.NodeRef) (string, bool) {
	if g == nil {
		return "", false
	}
	for key, destination := range g.sharedMuxes {
		if destination != nil && destination.name == node.String() {
			return key, true
		}
	}
	return "", false
}

func (g *runtimeAttachGroup) addSharedMuxRoute(key string, route pipeline.Route) error {
	if g == nil || !g.isSharedMux(key) {
		return runtimeBranchInvalidError("shared mux destination is not registered", "reuse one goav.File(name, writer) destination value inside one Mutable.Attach call")
	}
	target := g.sharedMuxes[key]
	if target == nil {
		return runtimeBranchInvalidError("shared mux destination is not reserved", "reuse one goav.File(name, writer) destination value inside one Mutable.Attach call")
	}
	route.To = []string{target.name}
	target.routes = append(target.routes, route)
	return nil
}

func (g *runtimeAttachGroup) prepareSharedMuxStages(ctx context.Context, rt *runtime) error {
	if g == nil || len(g.muxOrder) == 0 {
		return nil
	}
	if rt == nil {
		return runtimeBranchInvalidError(
			"runtime branch mux destination groups require the bundled runtime",
			"build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching grouped file or URI branches",
		)
	}
	service := &builder{runtime: rt}
	for i, key := range g.muxOrder {
		target := g.sharedMuxes[key]
		if target == nil {
			continue
		}
		formatID, err := runtimeMuxDestinationFormat(ctx, rt, target.dest, i)
		if err != nil {
			return err
		}
		if issue, ok := runtimeMuxCompatibilityIssue(target.name, formatID, target.branches, target.streams, rt); ok {
			return muxCompatibilityBuildError("attach runtime branches", issue)
		}
		stage, err := service.openMuxDestinationStage(
			ctx,
			target.dest,
			i,
			target.streams,
			formatID,
			destinationGraphFormat(target.dest),
		)
		if err != nil {
			return err
		}
		target.stage = stage
	}
	return nil
}

func (g *runtimeAttachGroup) attachSharedMuxDestinations(graph pipeline.Graph) ([]pipeline.NodeRef, []pipeline.Route, error) {
	if g == nil || len(g.muxOrder) == 0 {
		return nil, nil, nil
	}
	refs := make([]pipeline.NodeRef, 0, len(g.muxOrder))
	routes := make([]pipeline.Route, 0)
	for _, key := range g.muxOrder {
		target := g.sharedMuxes[key]
		if target == nil || target.stage == nil {
			continue
		}
		ref, err := graph.AddStage(namedStage{name: target.name, stage: target.stage}, target.buffer)
		if err != nil {
			return refs, routes, runtimeBranchGraphError("add mux destination", target.name, err)
		}
		target.ref = ref
		refs = append(refs, ref)
		for i := range target.routes {
			if err := graph.Connect(target.routes[i]); err != nil {
				return refs, routes, runtimeBranchGraphError("connect mux destination", target.name, err)
			}
			routes = append(routes, target.routes[i])
		}
	}
	return refs, routes, nil
}

func (g *runtimeAttachGroup) closeSharedMuxStages() {
	if g == nil {
		return
	}
	for _, key := range g.muxOrder {
		target := g.sharedMuxes[key]
		if target == nil || target.ref != "" || target.stage == nil {
			continue
		}
		_ = target.stage.Close()
		target.stage = nil
	}
}

func (g *runtimeAttachGroup) failSharedMuxStages() {
	if g == nil {
		return
	}
	for _, key := range g.muxOrder {
		target := g.sharedMuxes[key]
		if target == nil || target.stage == nil {
			continue
		}
		markPipelineStageFailed(target.stage)
	}
}

func runtimeMuxDestinationFormat(ctx context.Context, rt *runtime, dest destinationSpec, index int) (av.FormatID, error) {
	formatID := destinationOpenFormat(dest)
	if formatID != "" {
		return formatID, nil
	}
	if rt == nil {
		return "", runtimeBranchInvalidError(
			"runtime branch mux destinations require the bundled runtime",
			"build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching file or URI branches",
		)
	}
	result, err := rt.formats.Probe(ctx, outputProbeRequest(dest.output))
	if err != nil {
		return "", outputFormatProbeError(dest.output, index, err)
	}
	return result.Format, nil
}

func runtimeMuxCompatibilityIssue(destinationName string, formatID av.FormatID, branches []string, streams []av.Stream, rt *Runtime) (muxCompatibilityIssue, bool) {
	output := workDestination{
		Name:      destinationName,
		Operation: plan.OpMux,
		Format:    formatID,
		Branches:  append([]string(nil), branches...),
	}
	plannedStreams := make([]plannedMuxStream, 0, len(streams))
	for i := range streams {
		branch := "branch"
		if i < len(branches) && branches[i] != "" {
			branch = branches[i]
		}
		stream := streams[i]
		plannedStreams = append(plannedStreams, plannedMuxStream{
			Branch:   branch,
			Codec:    stream.Codec.ID,
			Media:    firstNonEmptyMedia(stream.Type, stream.Codec.Type, codecMedia(stream.Codec.ID)),
			TimeBase: muxStreamTimeBase(stream),
		})
	}
	return checkKnownMuxCompatibility(output, plannedStreams, rt)
}

func runtimeAttachmentName(specs []BranchSpec) string {
	if len(specs) == 1 {
		return specs[0].name
	}
	names := make([]string, 0, len(specs))
	for i := range specs {
		if specs[i].name != "" {
			names = append(names, specs[i].name)
		}
	}
	if len(names) == 0 {
		return "branches"
	}
	return strings.Join(names, "+")
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		value := values[i]
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (t *task) resolveRuntimeBranchAnchor(spec BranchSpec, graphSpec pipeline.Spec, pending []snapshot.Tap) (string, snapshot.Tap, error) {
	if spec.source.tap != "" {
		taps := append([]snapshot.Tap(nil), t.tapsLocked()...)
		taps = append(taps, pending...)
		for _, tap := range taps {
			if tap.Name == spec.source.tap {
				if err := validateTapDomain("attach runtime branch", firstNonEmpty(spec.name, "branch"), TapRef{name: spec.source.tap, domain: spec.source.tapDomain}, tap.Domain); err != nil {
					return "", snapshot.Tap{}, err
				}
				return tap.Node.String(), tap, nil
			}
		}
		return "", snapshot.Tap{}, runtimeBranchTapMissingError(spec.source.tap, taps)
	}
	if !specHasNode(graphSpec, spec.source.from) {
		return "", snapshot.Tap{}, runtimeBranchAnchorMissingError(spec.source.from)
	}
	if spec.source.stream != nil {
		return spec.source.from, discoveredStreamAnchorTap(spec.source), nil
	}
	return spec.source.from, snapshot.Tap{Node: pipeline.NodeRef(spec.source.from)}, nil
}

// discoveredStreamAnchorTap synthesizes the anchor tap for a source+stream
// anchor (a branch attached to a stream the source discovered at runtime):
// the source node is the anchor and the announced av.Stream supplies the
// shape facts a tap would normally carry.
func discoveredStreamAnchorTap(source branchSourceBinding) snapshot.Tap {
	domain := source.streamDomain
	if domain == "" {
		domain = shape.DomainPacket
	}
	spec := shape.FromStream(*source.stream, domain)
	return snapshot.Tap{
		Node:      pipeline.NodeRef(source.from),
		Domain:    domain,
		MediaKind: spec.MediaKind,
		Shape:     spec,
	}
}

// validateAttachBranchShapeContract validates the branch's operation list
// against the live anchor shape with the same shape algebra the build path
// uses, and rejects mux destinations whose final shape is not packet-domain.
func validateAttachBranchShapeContract(spec BranchSpec, destinations []attachDestination, initial shape.Spec) error {
	stream := streamIntent{
		Name: spec.name,
		Select: plan.StreamSelect{
			Type:  firstNonEmptyMedia(spec.media, initial.MediaKind),
			Codec: initial.Codec,
		},
		Operations: cloneOperationSpecs(spec.operations),
	}
	if err := validateOperationSpecShapes("attach runtime branch", stream, initial); err != nil {
		return err
	}
	final := normalizeTapShape(initial)
	for _, operation := range spec.operations {
		final = operationSpecOutputShape(final, operation)
	}
	for i := range destinations {
		destination := destinations[i]
		if destination.sink != nil {
			continue
		}
		if !destinationSpecHasOutput(destination.dest) {
			continue
		}
		if final.Domain == shape.DomainPacket {
			continue
		}
		destinationName := firstNonEmpty(destination.name, destination.dest.label(fmt.Sprintf("destination%d", i+1)))
		return destinationShapeMismatchError("attach runtime branch", spec.name, destinationName, destination.dest, final)
	}
	return nil
}

func (t *task) validateAttachBranchTapsLocked(steps []attachStep, pending []snapshot.Tap) error {
	seen := make(map[string]struct{}, len(steps))
	current := append([]snapshot.Tap(nil), t.tapsLocked()...)
	current = append(current, pending...)
	existing := make(map[string]struct{}, len(current))
	for i := range current {
		existing[current[i].Name] = struct{}{}
	}
	for i := range steps {
		if steps[i].tap == nil || !steps[i].install {
			continue
		}
		name := steps[i].tap.Name
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return runtimeBranchTapDuplicateError(name)
		}
		seen[name] = struct{}{}
		if _, ok := existing[name]; ok {
			return runtimeBranchTapDuplicateError(name)
		}
	}
	return nil
}

// applyAttachBranch executes one branch's slice of the workPatch against the
// running graph: each planned node is installed under its patch name and
// connected along its planned edge, with shared destinations resolved through
// the attach group. The returned refs/routes/taps feed the graph patch for
// bookkeeping and rollback.
func (t *task) applyAttachBranch(ap *attachPlan, branch attachPlanBranch, group *runtimeAttachGroup) ([]pipeline.NodeRef, []pipeline.Route, []snapshot.Tap, error) {
	refs := make([]pipeline.NodeRef, 0, branch.operations[1]-branch.operations[0])
	routes := make([]pipeline.Route, 0, branch.edges[1]-branch.edges[0])
	taps := append([]snapshot.Tap(nil), branch.taps...)
	edgeIndex := branch.edges[0]
	nextRoute := func() (pipeline.Route, bool) {
		if edgeIndex >= branch.edges[1] {
			return pipeline.Route{}, false
		}
		edge := ap.work.Edges[edgeIndex]
		edgeIndex++
		return pipeline.Route{
			From:   edge.From.String(),
			To:     []string{edge.To.String()},
			Policy: edge.Policy,
			Label:  edge.Label,
		}, true
	}
	for i := branch.operations[0]; i < branch.operations[1]; i++ {
		operation := ap.work.Operations[i]
		if operation.Kind == plan.OpTap || operation.Node == "" {
			continue
		}
		if key, ok := group.sharedSinkKeyForNode(operation.Node); ok {
			ref, added, err := group.sharedSinkRef(t.graph, key)
			if err != nil {
				return refs, routes, taps, err
			}
			if added {
				refs = append(refs, ref)
			}
			route, ok := nextRoute()
			if !ok {
				continue
			}
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch target", ref.String(), err)
			}
			routes = append(routes, route)
			continue
		}
		if key, ok := group.sharedMuxKeyForNode(operation.Node); ok {
			route, ok := nextRoute()
			if !ok {
				continue
			}
			if err := group.addSharedMuxRoute(key, route); err != nil {
				return refs, routes, taps, err
			}
			continue
		}
		component, ok := ap.components[operation.Node]
		if !ok {
			continue
		}
		nodeName := operation.Node.String()
		var (
			ref pipeline.NodeRef
			err error
		)
		switch {
		case component.stage != nil:
			ref, err = t.graph.AddStage(namedStage{name: nodeName, stage: component.stage}, branch.buffer)
			if err != nil {
				return refs, routes, taps, runtimeBranchGraphError("add stage", nodeName, err)
			}
		case component.sink != nil:
			ref, err = t.graph.AddSink(namedSink{name: nodeName, sink: component.sink}, branch.buffer)
			if err != nil {
				return refs, routes, taps, runtimeBranchGraphError("add sink", nodeName, err)
			}
		default:
			continue
		}
		refs = append(refs, ref)
		route, hasRoute := nextRoute()
		if !hasRoute {
			continue
		}
		if err := t.graph.Connect(route); err != nil {
			return refs, routes, taps, runtimeBranchGraphError(attachConnectOperation(operation, route, branch.from), nodeName, err)
		}
		routes = append(routes, route)
	}
	return refs, routes, taps, nil
}

// attachConnectOperation names the failing connect for diagnostics: branches
// connect from their anchor first, then stage to stage, then into targets.
func attachConnectOperation(operation workOperation, route pipeline.Route, from pipeline.NodeRef) string {
	if route.From == from.String() {
		return "connect branch"
	}
	if operation.Kind == plan.OpSink || operation.Kind == plan.OpMux {
		return "connect branch target"
	}
	return "connect branch stage"
}

func (t *task) rollbackRuntimeBranch(nodes []pipeline.NodeRef) {
	for i := len(nodes) - 1; i >= 0; i-- {
		_ = t.graph.Remove(nodes[i])
	}
}

func (t *task) trackAttachmentLocked(attachment *runtimeAttachment) {
	if t.attachments == nil {
		t.attachments = make(map[*runtimeAttachment]struct{})
	}
	t.attachments[attachment] = struct{}{}
}

func (t *task) tapsLocked() []snapshot.Tap {
	var base []snapshot.Tap
	if len(t.taps) != 0 {
		base = t.taps
	} else {
		base = inferSpecTaps(t.graph.Spec())
	}
	out := make([]snapshot.Tap, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap snapshot.Tap) {
		if tap.Name == "" {
			return
		}
		if _, ok := seen[tap.Name]; ok {
			return
		}
		seen[tap.Name] = struct{}{}
		out = append(out, tap)
	}
	for i := range base {
		appendTap(base[i])
	}
	for i := range t.branchTaps {
		appendTap(t.branchTaps[i])
	}
	return out
}

func (t *task) addAttachmentTapsLocked(taps []snapshot.Tap) {
	if len(taps) == 0 {
		return
	}
	t.branchTaps = append(t.branchTaps, taps...)
}

func (t *task) removeAttachmentTapsLocked(attachment *runtimeAttachment) {
	if attachment == nil || len(attachment.taps) == 0 || len(t.branchTaps) == 0 {
		return
	}
	remove := make(map[string]struct{}, len(attachment.taps))
	for i := range attachment.taps {
		remove[attachment.taps[i].Name] = struct{}{}
	}
	out := t.branchTaps[:0]
	for i := range t.branchTaps {
		if _, ok := remove[t.branchTaps[i].Name]; ok {
			continue
		}
		out = append(out, t.branchTaps[i])
	}
	t.branchTaps = out
}

func (t *task) stopAttachment(ctx context.Context, attachment *runtimeAttachment, disposition oldBranchDisposition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, attachment, disposition)
}

func (t *task) stopAttachmentLocked(ctx context.Context, attachment *runtimeAttachment, disposition oldBranchDisposition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attachment == nil {
		return nil
	}
	first := t.stopAttachmentChildrenLocked(ctx, attachment, disposition)
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		return first
	}
	attachment.applyDetachDisposition(disposition)
	err := attachment.stopLocked(t.graph)
	attachment.stopped = true
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	if err == nil {
		t.publishAttachmentDestinationLifecycleEvents(attachment, disposition)
		t.publishBranchLifecycleEvent(av.EventBranchDetached, attachment, disposition)
	} else {
		t.publishAttachmentDestinationCommitErrors(attachment, err)
	}
	if first != nil {
		return first
	}
	return err
}

func (t *task) publishAttachmentDestinationLifecycleEvents(attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if attachment == nil {
		return
	}
	var kind av.EventType
	switch disposition {
	case oldBranchDrain:
		kind = av.EventDestinationCommitted
	case oldBranchAbort:
		kind = av.EventDestinationAborted
	default:
		return
	}
	for i := range attachment.work.Destinations {
		t.publishDestinationLifecycleEvent(kind, attachment.work.Destinations[i].Name, attachment, nil)
	}
}

func (t *task) publishAttachmentDestinationCommitErrors(attachment *runtimeAttachment, cause error) {
	if attachment == nil || cause == nil {
		return
	}
	for i := range attachment.work.Destinations {
		t.publishDestinationLifecycleEvent(av.EventDestinationCommitError, attachment.work.Destinations[i].Name, attachment, cause)
	}
}

func (t *task) stopAttachmentChildrenLocked(ctx context.Context, attachment *runtimeAttachment, disposition oldBranchDisposition) error {
	if attachment == nil {
		return nil
	}
	var first error
	for {
		child := t.childAttachmentLocked(attachment)
		if child == nil {
			return first
		}
		if err := t.stopAttachmentLocked(ctx, child, disposition); first == nil && err != nil {
			first = err
		}
	}
}

func (t *task) publishBranchLifecycleEvent(kind av.EventType, attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if t == nil || attachment == nil {
		return
	}
	reason := "runtime branch attached"
	if kind == av.EventBranchDetached {
		reason = "runtime branch detached"
	}
	event := av.Event{
		Type:   kind,
		Reason: reason,
		Metadata: av.Metadata{
			av.MetadataAttachmentID:   attachment.ID(),
			av.MetadataAttachmentName: attachment.Name(),
		},
	}
	if kind == av.EventBranchDetached {
		event.Metadata[av.MetadataDetachDisposition] = detachDispositionLabel(disposition)
	}
	t.watch.publish(event)
}

func detachDispositionLabel(disposition oldBranchDisposition) string {
	switch disposition {
	case oldBranchDrain:
		return "drain"
	case oldBranchAbort:
		return "abort"
	default:
		return "detach"
	}
}

func (t *task) childAttachmentLocked(parent *runtimeAttachment) *runtimeAttachment {
	if parent == nil {
		return nil
	}
	taps := make(map[string]struct{}, len(parent.taps))
	for i := range parent.taps {
		if parent.taps[i].Name != "" {
			taps[parent.taps[i].Name] = struct{}{}
		}
	}
	nodes := make(map[string]struct{}, len(parent.nodes))
	for i := range parent.nodes {
		nodes[parent.nodes[i].String()] = struct{}{}
	}
	for attachment := range t.attachments {
		if attachment == nil || attachment == parent || attachment.stopped {
			continue
		}
		for _, anchor := range attachment.allAnchorTaps() {
			if _, ok := taps[anchor]; ok && anchor != "" {
				return attachment
			}
		}
		for _, anchor := range attachment.allAnchorNodes() {
			if _, ok := nodes[anchor]; ok && anchor != "" {
				return attachment
			}
		}
	}
	return nil
}

type runtimeAttachment struct {
	id          string
	name        string
	owner       *task
	anchorTap   string
	anchorNode  string
	anchorTaps  []string
	anchorNodes []string
	nodes       []pipeline.NodeRef
	routes      []pipeline.Route
	taps        []snapshot.Tap
	stages      []pipeline.Stage
	copyNever   []string
	work        workPatch
	stopMu      sync.Mutex
	stopped     bool
	// detachOutcome records the lifecycle.DestinationState a disposition detach
	// (DrainOldBranch/AbortOldBranch) chose for this branch's destinations;
	// snapshots report it instead of the plain lifecycle.DestinationClosed.
	detachOutcome atomic.Value
}

func (a *runtimeAttachment) allAnchorTaps() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorTaps) == 0 {
		if a.anchorTap == "" {
			return nil
		}
		return []string{a.anchorTap}
	}
	return a.anchorTaps
}

func (a *runtimeAttachment) allAnchorNodes() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorNodes) == 0 {
		if a.anchorNode == "" {
			return nil
		}
		return []string{a.anchorNode}
	}
	return a.anchorNodes
}

func (a *runtimeAttachment) ID() string {
	if a == nil {
		return ""
	}
	return a.id
}

func (a *runtimeAttachment) Name() string {
	if a == nil {
		return ""
	}
	return a.name
}

func (a *runtimeAttachment) Spec() pipeline.Spec {
	if a == nil || a.owner == nil {
		return pipeline.Spec{}
	}
	return a.specFromGraph(a.owner.Describe())
}

func (a *runtimeAttachment) specFromGraph(graph pipeline.Spec) pipeline.Spec {
	if a == nil {
		return pipeline.Spec{}
	}
	nodes := make(map[string]struct{}, len(a.nodes))
	for i := range a.nodes {
		nodes[a.nodes[i].String()] = struct{}{}
	}
	spec := pipeline.Spec{Name: a.name, Realtime: graph.Realtime}
	for i := range graph.Nodes {
		if _, ok := nodes[graph.Nodes[i].Name]; ok {
			spec.Nodes = append(spec.Nodes, graph.Nodes[i])
		}
	}
	for i := range graph.Edges {
		if _, ok := nodes[graph.Edges[i].To.String()]; ok {
			spec.Edges = append(spec.Edges, graph.Edges[i])
		}
	}
	return spec
}

func (a *runtimeAttachment) Stats() pipeline.GraphStats {
	if a == nil || a.owner == nil {
		return pipeline.GraphStats{}
	}
	return branchStatsForNodes(a.owner.Stats(), a.nodes)
}

func (a *runtimeAttachment) Snapshot() snapshot.Branch {
	if a == nil {
		return snapshot.Branch{}
	}
	stats := pipeline.GraphStats{}
	if a.owner == nil {
		return a.branchSnapshotLocked(stats)
	}
	stats = a.owner.Stats()
	a.owner.attachMu.Lock()
	defer a.owner.attachMu.Unlock()
	return a.branchSnapshotLocked(stats)
}

func (a *runtimeAttachment) branchSnapshotLocked(taskStats pipeline.GraphStats) snapshot.Branch {
	if a == nil {
		return snapshot.Branch{}
	}
	state := lifecycle.BranchAttached
	destinationState := lifecycle.DestinationOpen
	spec := pipeline.Spec{}
	if a.owner != nil {
		_, destinationState = a.owner.lifecycleStates()
		spec = a.specFromGraph(a.owner.Describe())
	}
	if a.stopped {
		state = lifecycle.BranchDetached
		destinationState = lifecycle.DestinationClosed
		if outcome, ok := a.detachOutcome.Load().(lifecycle.DestinationState); ok && outcome != "" {
			destinationState = outcome
		}
	}
	return snapshot.Branch{
		ID:           a.id,
		Name:         a.name,
		State:        state,
		AnchorTaps:   append([]string(nil), a.allAnchorTaps()...),
		AnchorNodes:  append([]string(nil), a.allAnchorNodes()...),
		Nodes:        append([]pipeline.NodeRef(nil), a.nodes...),
		Taps:         append([]snapshot.Tap(nil), a.taps...),
		Destinations: destinationSnapshotsFromWork(a.work.Destinations, destinationState),
		Spec:         spec,
		Stats:        branchStatsForNodes(taskStats, a.nodes),
	}
}

func (a *runtimeAttachment) Pause(ctx context.Context) error {
	return a.setPaused(ctx, true)
}

func (a *runtimeAttachment) Resume(ctx context.Context) error {
	return a.setPaused(ctx, false)
}

func (a *runtimeAttachment) setPaused(ctx context.Context, paused bool) error {
	if a == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	if a.stopped {
		return runtimeBranchInvalidError("pause runtime branch", "branch is already detached")
	}
	if a.owner == nil {
		return nil
	}
	pauser, ok := a.owner.graph.(pipeline.NodePauser)
	if !ok {
		return runtimeBranchInvalidError("pause runtime branch", "this task graph does not support pausing branches")
	}
	var first error
	for i := range a.nodes {
		if err := pauser.SetNodePaused(a.nodes[i], paused); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	return first
}

func (a *runtimeAttachment) Rebranch(ctx context.Context, options ...lifecycle.RebranchArg) (Attachment, error) {
	if a == nil || a.owner == nil {
		return nil, runtimeBranchInvalidError("rebranch runtime branch", "attachment has no owning task")
	}
	policy := rebranchPolicyFromOptions(options)
	if len(policy.specs) == 0 {
		return nil, runtimeBranchInvalidError("rebranch runtime branch", "pass one or more replacement goav.Branch(name)...To(destination) specs")
	}
	if policy.invalid != "" {
		return nil, runtimeBranchInvalidError(policy.invalid, "pass lifecycle.SwitchAt(lifecycle.AtMediaTime(position)) with position >= 0")
	}
	specs := policy.specs
	var group *switchGroup
	if policy.boundary != switchImmediate {
		group = newSwitchGroup(policy.boundary, policy.mediaTime)
		specs = gatedBranchSpecs(specs, group)
	}
	// Attach the replacements first so they are live before the old branch goes
	// away (no delivery gap); if that fails, leave the old branch untouched.
	next, err := a.owner.Attach(ctx, specs...)
	if err != nil {
		return nil, err
	}
	if group == nil {
		if err := a.detachReplaced(ctx, policy.disposition); err != nil {
			return next, err
		}
		return next, nil
	}
	go a.detachReplacedAtBoundary(group, policy.disposition)
	return next, nil
}

// detachReplacedAtBoundary waits for the switch boundary — the first
// replacement gate opening — and detaches this branch with the chosen
// disposition. When every gate closes before any opened (teardown reached the
// replacements first), the switch never happened and this branch's own
// teardown finalizes it instead.
func (a *runtimeAttachment) detachReplacedAtBoundary(group *switchGroup, disposition oldBranchDisposition) {
	select {
	case <-group.opened:
	case <-group.abandoned:
		select {
		case <-group.opened:
		default:
			return
		}
	}
	_ = a.detachReplaced(context.Background(), disposition)
}

// detachReplaced detaches this branch as the replaced side of a rebranch:
// drain commits its destinations, abort marks its prepared stages failed so
// closing aborts them, and the default stays a plain detach.
func (a *runtimeAttachment) detachReplaced(ctx context.Context, disposition oldBranchDisposition) error {
	switch disposition {
	case oldBranchDrain:
		return a.owner.Detach(ctx, a, lifecycle.DrainBranch())
	case oldBranchAbort:
		return a.owner.Detach(ctx, a, lifecycle.AbortBranch())
	default:
		return a.owner.Detach(ctx, a)
	}
}

func (a *runtimeAttachment) applyDetachDisposition(disposition oldBranchDisposition) {
	switch disposition {
	case oldBranchDrain:
		a.detachOutcome.Store(lifecycle.DestinationCommitted)
	case oldBranchAbort:
		for _, stage := range a.stages {
			markPipelineStageFailed(stage)
		}
		a.detachOutcome.Store(lifecycle.DestinationAborted)
	case oldBranchDetach:
	}
}

func (a *runtimeAttachment) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.owner != nil {
		return a.owner.stopAttachment(ctx, a, oldBranchDetach)
	}
	return nil
}

func (a *runtimeAttachment) stopLocked(graph pipeline.Graph) error {
	var first error
	for i := range a.routes {
		if err := graph.Disconnect(a.routes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	for i := len(a.nodes) - 1; i >= 0; i-- {
		if err := graph.Remove(a.nodes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	return first
}

func branchStatsForNodes(stats pipeline.GraphStats, nodes []pipeline.NodeRef) pipeline.GraphStats {
	if len(nodes) == 0 || len(stats.Nodes) == 0 {
		return pipeline.GraphStats{}
	}
	out := pipeline.GraphStats{
		DropReasons: make(map[pipeline.DropPolicy]uint64),
		Nodes:       make(map[string]pipeline.NodeStats, len(nodes)),
	}
	for i := range nodes {
		name := nodes[i].String()
		nodeStats, ok := stats.Nodes[name]
		if !ok {
			continue
		}
		out.Nodes[name] = clonePublicNodeStats(nodeStats)
		out.Messages += nodeStats.OutMessages
		out.Packets += nodeStats.OutPackets
		out.Frames += nodeStats.OutFrames
		out.Events += nodeStats.OutEvents
		out.Delivered += nodeStats.InMessages
		out.Dropped += nodeStats.Dropped
		for reason, count := range nodeStats.DropReasons {
			out.DropReasons[reason] += count
		}
		if nodeStats.LastEventPresent {
			out.LastEvent = nodeStats.LastEvent
			out.LastEventPresent = true
		}
	}
	if len(out.DropReasons) == 0 {
		out.DropReasons = nil
	}
	if len(out.Nodes) == 0 {
		out.Nodes = nil
	}
	return out
}

func clonePublicNodeStats(stats pipeline.NodeStats) pipeline.NodeStats {
	cloned := stats
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[pipeline.DropPolicy]uint64, len(stats.DropReasons))
		for reason, count := range stats.DropReasons {
			cloned.DropReasons[reason] = count
		}
	}
	return cloned
}

func isStoppedAttachmentError(err error) bool {
	return errors.Is(err, pipeline.ErrInvalidLink) ||
		errors.Is(err, pipeline.ErrUnknownNode) ||
		errors.Is(err, pipeline.ErrClosed)
}

func destinationSpecHasOutput(dest destinationSpec) bool {
	return dest.output.Name != "" ||
		dest.output.URI != "" ||
		dest.output.Writer != nil ||
		dest.format != "" ||
		dest.resolvedFormat != ""
}

func (t *task) prepareRuntimeBranchDecode(ctx context.Context, branchName string, currentStream av.Stream, currentShape shape.Spec, spec codec.CodecSpec) (pipeline.Stage, error) {
	if t.runtime == nil {
		return nil, runtimeBranchInvalidError(
			"runtime branch decoding requires the bundled runtime",
			"build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching decode branches",
		)
	}
	if currentShape.Domain != shape.DomainPacket {
		return nil, runtimeBranchDecodeDomainError(branchName, currentShape)
	}
	if currentStream.Codec.ID == "" {
		return nil, runtimeBranchDecodeCodecMissingError(branchName, currentShape)
	}
	request := runtimeBranchDecodeRequest(branchName, currentStream, spec)
	if _, err := t.runtime.codecs.DecoderFactory(currentStream.Codec.ID); err != nil {
		stream := streamIntent{
			Name:       branchName,
			Operations: []operationSpec{operationSpecForDecode(spec, string(currentStream.Codec.ID))},
		}
		return nil, recipeDecodeAdapterError("attach runtime branch", stream, currentStream.Codec.ID, t.runtime.codecs, err)
	}
	stream := streamIntent{
		Name:       branchName,
		Select:     streamSelectFromAV(request.selector),
		Operations: []operationSpec{operationSpecForDecode(spec, string(currentStream.Codec.ID))},
	}
	if err := validateDecodeAdapterDescriptors("attach runtime branch", stream, t.runtime.codecs, decodeAdapterRequestFromStream(currentStream, stream)); err != nil {
		return nil, err
	}
	stage, err := (&builder{runtime: t.runtime}).newDecodeStage(ctx, request, currentStream, t.runtime.realtime, true, codec.DecodeBounds{})
	if err != nil {
		return nil, err
	}
	return stage, nil
}

func runtimeBranchTransform(branchName string, stream av.Stream, spec TransformSpec, index int) (mediaTransform, error) {
	base := firstNonEmpty(branchName, "branch")
	if err := validateTransformSpec("attach runtime branch", base, spec); err != nil {
		return mediaTransform{}, err
	}
	suffix := ""
	if index > 0 {
		suffix = "-" + strconv.Itoa(index+1)
	}
	switch {
	case spec.Resize != nil && spec.Resample != nil:
		return mediaTransform{}, &BuildError{
			Family:    errcode.FamilyForCode(errcode.TransformInvalid),
			Code:      errcode.TransformInvalid,
			Operation: "attach runtime branch",
			Node:      base,
			Reason:    "one runtime branch transform cannot be both resize and resample",
			Fixes:     buildErrorFixes([]string{"declare two separate steps instead: .Resize(width, height).Resample(rate, channels)"}),
			Cause:     ErrUnsupportedBuild,
		}
	case spec.Resize != nil:
		if stream.Type != av.MediaVideo && stream.Codec.Type != av.MediaVideo {
			return mediaTransform{}, runtimeBranchTransformMediaError(base, "resize", av.MediaVideo, runtimeBranchStreamMedia(stream))
		}
		resize := *spec.Resize
		return mediaTransform{
			name:    "resize-" + base + suffix,
			factory: transformFactoryName(spec),
			video:   &resize,
		}, nil
	case spec.Resample != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return mediaTransform{}, runtimeBranchTransformMediaError(base, "resample", av.MediaAudio, runtimeBranchStreamMedia(stream))
		}
		resample := *spec.Resample
		return mediaTransform{
			name:    "resample-" + base + suffix,
			factory: transformFactoryName(spec),
			audio:   &resample,
		}, nil
	default:
		return mediaTransform{}, &BuildError{
			Family:    errcode.FamilyForCode(errcode.TransformInvalid),
			Code:      errcode.TransformInvalid,
			Operation: "attach runtime branch",
			Node:      base,
			Reason:    "empty runtime branch transform",
			Fixes: buildErrorFixes([]string{
				"call .Resize(width, height) for video frame taps",
				"call .Resample(sampleRate, channels) for audio frame taps",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
}

func runtimeBranchStreamMedia(stream av.Stream) av.MediaType {
	if stream.Type != "" {
		return stream.Type
	}
	return stream.Codec.Type
}

func runtimeBranchAnchorShape(anchor snapshot.Tap) shape.Spec {
	shape := anchor.Shape
	if shape.Domain == "" {
		shape.Domain = anchor.Domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = anchor.MediaKind
	}
	return shape
}

func streamFromRuntimeBranchShape(name string, shape shape.Spec) av.Stream {
	stream := av.Stream{
		ID:   av.StreamID(firstNonEmpty(string(shape.StreamID), name, "runtime-branch")),
		Name: firstNonEmpty(name, "runtime-branch"),
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:            shape.Codec,
			Type:          shape.MediaKind,
			SampleRate:    shape.SampleRate,
			Channels:      shape.Channels,
			ChannelLayout: "",
			Width:         shape.Width,
			Height:        shape.Height,
			PixelFormat:   shape.PixelFormat,
			SampleFormat:  shape.SampleFormat,
		},
	}
	if shape.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(shape.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(shape.SampleRate)}
	}
	return stream
}

func runtimeBranchEncodeRequest(name string, encode codec.CodecSpec, stream av.Stream) encodeRequest {
	config := encodeConfigFromSpec(encode)
	if config.Stream.ID == "" {
		config.Stream.ID = av.StreamID(firstNonEmpty(name, string(stream.ID), "branch"))
	}
	if config.Stream.Name == "" {
		config.Stream.Name = firstNonEmpty(name, stream.Name, string(stream.ID), "branch")
	}
	selector := av.StreamSelector{Type: stream.Type}
	if selector.Type == "" {
		selector.Type = stream.Codec.Type
	}
	if stream.ID != "" {
		selector.ID = stream.ID
	}
	if stream.Codec.ID != "" {
		selector.Codec = stream.Codec.ID
	}
	return encodeRequest{
		name:     firstNonEmpty(name, string(stream.ID), "branch"),
		selector: selector,
		config:   config,
	}
}

func runtimeBranchDecodeRequest(branchName string, stream av.Stream, spec codec.CodecSpec) decodeRequest {
	selector := av.StreamSelector{
		Name:  firstNonEmpty(branchName, stream.Name),
		Type:  stream.Type,
		ID:    stream.ID,
		Codec: stream.Codec.ID,
	}
	if selector.Type == "" {
		selector.Type = stream.Codec.Type
	}
	return decodeRequest{selector: selector, config: cloneCodecSpec(spec)}
}

func mediaShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := previous
	spec.Domain = shape.DomainFrame
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		spec.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		spec.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		spec.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		spec.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		spec.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		spec.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		spec.SampleFormat = stream.Codec.SampleFormat
	}
	return spec
}

func streamPacketShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := mediaShapeFromRuntimeBranchStream(stream, previous)
	spec.Domain = shape.DomainPacket
	return spec
}

func runtimeBranchTap(name string, node pipeline.NodeRef, spec shape.Spec, after plan.OperationKind) snapshot.Tap {
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	media := spec.MediaKind
	if spec.Domain == "" {
		spec.Domain = domain
	}
	if spec.MediaKind == "" {
		spec.MediaKind = media
	}
	return snapshot.Tap{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		After:     after,
		Shape:     spec,
		Node:      node,
	}
}

func validateRuntimeBranchNodeName(spec pipeline.Spec, seen map[string]struct{}, name string) error {
	if _, ok := seen[name]; ok {
		return runtimeBranchNodeDuplicateError(name)
	}
	seen[name] = struct{}{}
	if specHasNode(spec, name) {
		return runtimeBranchNodeDuplicateError(name)
	}
	return nil
}

func runtimeBranchNodeName(branchName string, localName string, fallback string) string {
	localName = firstNonEmpty(localName, fallback)
	return branchName + "/" + localName
}

func nextRuntimeAttachmentID(name string) string {
	seq := runtimeAttachmentSeq.Add(1)
	if name == "" {
		return "attachment-" + strconv.FormatUint(seq, 10)
	}
	return name + "-" + strconv.FormatUint(seq, 10)
}

func specHasNode(spec pipeline.Spec, name string) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == name {
			return true
		}
	}
	return false
}

func runtimeBranchInvalidError(reason string, suggestion string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchInvalid),
		Code:      errcode.RuntimeBranchInvalid,
		Operation: "attach runtime branch",
		Reason:    reason,
		Fixes: buildErrorFixes([]string{
			suggestion,
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchAnchorMissingError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchAnchorMissing),
		Code:      errcode.RuntimeBranchAnchorMissing,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch source node does not exist in the running task graph",
		Fixes: buildErrorFixes([]string{
			"call Inspectable.Taps() and use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) for stable media outlets",
			"keep the GraphNode returned by expert.Graph(runtime).Source/Stage/Sink for expert graph attachments",
			"attach from a stable decoded-frame tap when the branch needs raw frames",
		}),
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchTapMissingError(name string, taps []snapshot.Tap) error {
	details := make([]string, 0, len(taps))
	for i := range taps {
		details = append(details, taps[i].Name+": "+string(taps[i].Domain)+" "+string(taps[i].MediaKind))
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchTapMissing),
		Code:      errcode.RuntimeBranchTapMissing,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch source tap does not exist in the running task",
		Fields:    buildErrorFields(details),
		Fixes: buildErrorFixes([]string{
			"add .Tap(goav.FrameTap(" + strconv.Quote(name) + ")) or .Tap(goav.PacketTap(" + strconv.Quote(name) + ")) at the point you want to attach",
			"call Inspectable.Taps() before attaching runtime branches",
			"use .From(graphNode) only for expert graph-node attachments",
		}),
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchNodeDuplicateError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchNodeDuplicate),
		Code:      errcode.RuntimeBranchNodeDuplicate,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch node name already exists in the task graph",
		Fixes: buildErrorFixes([]string{
			"use a unique goav.Branch(name)",
			"use distinct stage and sink names inside repeated runtime branches",
		}),
		Cause: pipeline.ErrNodeExists,
	}
}

func duplicateRuntimeBranchDestinationRefError(branch string, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.DestinationDuplicate),
		Code:      errcode.DestinationDuplicate,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", label),
		Fields: buildErrorFields([]string{
			fmt.Sprintf("first destination index: %d", firstIndex),
			fmt.Sprintf("second destination index: %d", secondIndex),
		}),
		Fixes: buildErrorFixes([]string{
			"list each destination once in .To(...)",
			"route one runtime branch to multiple destinations with distinct values such as .To(archive, monitor)",
			"reuse destination values or wrap each grouped attachment destination with goav.Mux(name, destination)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTapDuplicateError(name string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchTapDuplicate),
		Code:      errcode.RuntimeBranchTapDuplicate,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch tap name already exists in the task",
		Fixes: buildErrorFixes([]string{
			"use a unique tap name for each runtime branch outlet",
			"call Inspectable.Taps() before attaching to inspect the current tap names",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTransformMediaError(branch string, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.TransformMediaMismatch),
		Code:      errcode.TransformMediaMismatch,
		Operation: "attach runtime branch",
		Node:      branch,
		Reason:    transform + " applies to " + string(expected) + " frame taps",
		Fields: buildErrorFields([]string{
			"expected_shape=" + shape.Frame(expected).String(),
			"actual_shape=" + shape.Frame(actual).String(),
		}),
		Fixes: buildErrorFixes([]string{
			"use .Video().Decode().Tap(goav.FrameTap(name)) or a video transform tap before attaching .Resize(...)",
			"use .Audio().Decode().Tap(goav.FrameTap(name)) or an audio transform tap before attaching .Resample(...)",
			"call Inspectable.Taps() and choose a tap with matching media kind",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTransformError(node string, cause error) error {
	if cause == nil {
		return nil
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchTransformError),
		Code:      errcode.RuntimeBranchTransformError,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "runtime branch transform could not be opened",
		Fixes: buildErrorFixes([]string{
			"register a matching resize or resample filter adapter",
			"import github.com/thesyncim/goav/bundle and use bundle.MustNewFilters(...) for bundled filters",
			"attach from a frame tap with media shape that match the requested transform",
		}),
		Cause: cause,
	}
}

func runtimeBranchEncodeMissingError(branch string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchEncodeMissing),
		Code:      errcode.RuntimeBranchEncodeMissing,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "muxed runtime branches need packet copy or an encoder",
		Fixes: buildErrorFixes([]string{
			"call .Copy() when attaching from a packet tap",
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) when attaching from a frame tap",
			"use .To(goav.Sink(...)) when the runtime branch should receive raw frames",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchEncodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchEncodeDomainMismatch),
		Code:      errcode.RuntimeBranchEncodeDomainMismatch,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch encoding requires a frame tap",
		Fields:    buildErrorFields(runtimeBranchShapeDetails(shape)),
		Fixes: buildErrorFixes([]string{
			"attach from a tap declared after Decode, Resize, Resample, or a frame-stage .Do(...)",
			"use .Copy() from a packet tap when no re-encode is intended",
			"call Inspectable.Taps() and choose a tap with domain=frame",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchDecodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchDecodeDomainMismatch),
		Code:      errcode.RuntimeBranchDecodeDomainMismatch,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decoding requires a packet tap",
		Fields:    buildErrorFields(runtimeBranchShapeDetails(shape)),
		Fixes: buildErrorFixes([]string{
			"attach from a tap declared after Copy, packet receive, or Encode",
			"omit .Decode() when attaching from a frame tap",
			"call Inspectable.Taps() and choose a tap with domain=packet",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchDecodeCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchDecodeCodecMissing),
		Code:      errcode.RuntimeBranchDecodeCodecMissing,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decode needs packet codec metadata",
		Fields:    buildErrorFields(runtimeBranchShapeDetails(shape)),
		Fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"declare the input codec (provider codec intent or file metadata) before building the task",
			"call Inspectable.Taps() and choose a packet tap that reports codec=...",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchCopyDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchCopyDomainMismatch),
		Code:      errcode.RuntimeBranchCopyDomainMismatch,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch packet copy requires a packet tap",
		Fields:    buildErrorFields(runtimeBranchShapeDetails(shape)),
		Fixes: buildErrorFixes([]string{
			"attach from a tap declared after Copy or Encode",
			"encode frame taps with .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before writing a muxed destination",
			"call Inspectable.Taps() and choose a tap with domain=packet",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchMuxCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchMuxCodecMissing),
		Code:      errcode.RuntimeBranchMuxCodecMissing,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch mux destination needs codec metadata",
		Fields:    buildErrorFields(runtimeBranchShapeDetails(shape)),
		Fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"set an explicit encoder with .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...))",
			"use .To(goav.Sink(...)) when the branch should stay raw",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchShapeDetails(shape shape.Spec) []string {
	details := []string{
		"domain=" + string(shape.Domain),
		"media=" + string(shape.MediaKind),
	}
	if shape.Codec != "" {
		details = append(details, "codec="+string(shape.Codec))
	}
	if shape.StreamID != "" {
		details = append(details, "stream="+string(shape.StreamID))
	}
	return details
}

func runtimeBranchGraphError(operation string, node string, cause error) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeBranchGraphError),
		Code:      errcode.RuntimeBranchGraphError,
		Operation: operation,
		Node:      node,
		Reason:    "runtime graph rejected the branch attachment",
		Fixes: buildErrorFixes([]string{
			"runtime branches are supported on direct task graphs today",
			"use a direct buffer policy for live runtime branch experiments",
			"build the branch before Run when using buffered execution",
		}),
		Cause: cause,
	}
}
