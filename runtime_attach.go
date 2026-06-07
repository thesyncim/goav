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
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

var runtimeAttachmentSeq atomic.Uint64

// runtimeBranch is the internal graph mutation plan for a branch attached to an
// already-built task.
type runtimeBranch struct {
	name           string
	media          av.MediaType
	from           string
	tap            string
	tapDomain      MediaDomain
	anchor         TapInfo
	operations     []OperationSpec
	steps          []runtimeBranchStep
	postEncodeTaps []string
	encode         CodecSpec
	destinations   []runtimeBranchDestination
	terminals      []runtimeBranchTerminal
	policy         pipeline.RoutePolicy
	label          string
	buffer         pipeline.BufferPolicy
	err            error
}

type runtimeBranchStep struct {
	stage       pipeline.Stage
	kind        OperationKind
	decode      bool
	codec       CodecSpec
	transform   TransformSpec
	tap         string
	tapDomain   MediaDomain
	after       OperationKind
	shapeUpdate MediaShape
	shape       MediaShape
	owned       bool
}

type runtimeBranchDestination struct {
	name     string
	dest     destinationSpec
	sink     pipeline.Sink
	shareKey string
}

type runtimeBranchTerminal struct {
	name     string
	shareKey string
	dest     destinationSpec
	stream   av.Stream
	stage    pipeline.Stage
	sink     pipeline.Sink
	shape    MediaShape
	owned    bool
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
	name string
	sink pipeline.Sink
	ref  pipeline.NodeRef
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
	nodes       []pipeline.NodeRef
	routes      []pipeline.Route
	taps        []TapInfo
	anchorTaps  []string
	anchorNodes []string
	work        workPatch
}

func (p *runtimeGraphPatch) addAnchor(branch runtimeBranch) {
	if branch.tap != "" {
		p.anchorTaps = append(p.anchorTaps, branch.tap)
	}
	if branch.from != "" {
		p.anchorNodes = append(p.anchorNodes, branch.from)
	}
}

func (p *runtimeGraphPatch) addApplied(nodes []pipeline.NodeRef, routes []pipeline.Route, taps []TapInfo) {
	p.nodes = append(p.nodes, nodes...)
	p.routes = append(p.routes, routes...)
	p.taps = append(p.taps, taps...)
}

func (p *runtimeGraphPatch) addPlannedTaps(taps []TapInfo) {
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
		taps:        append([]TapInfo(nil), p.taps...),
		work:        cloneWorkPatch(p.work),
	}
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	ID() string
	Name() string
	Spec() pipeline.Spec
	Stats() BranchStats
	Snapshot() BranchSnapshot
	Close(context.Context) error
}

func (t *task) Attach(ctx context.Context, specs ...BranchSpec) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, runtimeBranchInvalidError("no runtime branches to attach", "pass one or more goav.Branch(name)...To(destination) values")
	}
	branches := make([]runtimeBranch, len(specs))
	for i := range specs {
		branch, err := runtimeBranchFromSpec(specs[i])
		if err != nil {
			return nil, err
		}
		if err := validateRuntimeBranch(branch); err != nil {
			return nil, err
		}
		branches[i] = branch
	}
	destinations, err := validateRuntimeBranchGroupDestinations(branches)
	if err != nil {
		return nil, err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	group := newRuntimeAttachGroup(destinations)
	var patch runtimeGraphPatch
	nodeNamesByBranch := make([][]string, len(branches))
	preparedBranches := 0
	rollback := func(err error) (Attachment, error) {
		for i := 0; i < preparedBranches; i++ {
			closeRuntimeBranchOwnedStages(branches[i])
		}
		group.failSharedMuxStages()
		group.closeSharedMuxStages()
		patch.rollback(t)
		return nil, err
	}
	for i := range branches {
		branch := &branches[i]
		graphSpec := t.graph.Spec()
		var err error
		branch.from, branch.anchor, err = t.resolveRuntimeBranchAnchor(branch, graphSpec, patch.taps)
		if err != nil {
			return rollback(err)
		}
		patch.addAnchor(*branch)
		if err := t.prepareRuntimeBranch(ctx, branch, group); err != nil {
			preparedBranches = i + 1
			return rollback(err)
		}
		preparedBranches = i + 1
		if err := t.validateRuntimeBranchTapsLocked(*branch, patch.taps); err != nil {
			return rollback(err)
		}
		nodeNames, err := runtimeBranchNodeNames(*branch, graphSpec, group)
		if err != nil {
			return rollback(err)
		}
		nodeNamesByBranch[i] = nodeNames
		patch.addPlannedTaps(runtimeBranchPlannedTaps(*branch, nodeNames))
	}
	if err := group.prepareSharedMuxStages(ctx, t.runtime); err != nil {
		return rollback(err)
	}
	name := runtimeAttachmentName(branches)
	patch.setWork(workPatchFromRuntimeBranches(name, branches, nodeNamesByBranch, group))
	patch.resetPlannedTaps()
	for i := range branches {
		branch := &branches[i]
		nodeNames := nodeNamesByBranch[i]
		branchRefs, branchRoutes, branchTaps, err := t.attachRuntimeBranch(*branch, nodeNames, group)
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
	attachment := patch.attachment(t, name)
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(patch.taps)
	return attachment, nil
}

func runtimeBranchFromSpec(spec BranchSpec) (runtimeBranch, error) {
	if spec.err != nil {
		return runtimeBranch{err: spec.err}, spec.err
	}
	operations := runtimeBranchOperationSpecsFromSpec(spec)
	branch := runtimeBranch{
		name:       spec.name,
		media:      spec.media,
		from:       spec.from,
		tap:        spec.tap,
		tapDomain:  spec.tapDomain,
		operations: operations,
		encode:     cloneCodecSpec(spec.encode),
		policy:     spec.policy,
		label:      spec.label,
		buffer:     spec.buffer,
	}
	branch.steps = runtimeBranchStepsFromOperationSpecs(operations)
	branch.postEncodeTaps = runtimeBranchPostEncodeTapsFromOperations(operations, spec.postEncodeTaps)
	if len(spec.destinations) == 0 {
		return branch, runtimeBranchInvalidError("branch destination is missing", "finish the branch with .To(goav.Sink(sink)) or .To(goav.File(name, writer))")
	}
	for i := range spec.destinations {
		ref := cloneDestinationRef(spec.destinations[i])
		if ref.err != nil {
			return branch, ref.err
		}
		destination := cloneDestinationSpec(ref.dest)
		name := firstNonEmpty(ref.name, spec.name, "branch")
		if err := destination.validate("attach runtime branch", name); err != nil {
			return branch, err
		}
		branch.destinations = append(branch.destinations, runtimeBranchDestination{
			name:     name,
			dest:     destination,
			sink:     destination.sink,
			shareKey: runtimeBranchSharedDestinationKey(ref),
		})
	}
	return branch, nil
}

func runtimeBranchOperationSpecsFromSpec(spec BranchSpec) []OperationSpec {
	operations := cloneOperationSpecs(spec.operations)
	if spec.decode && !operationSpecsContainKind(operations, OpDecode) {
		operation := operationSpecForDecode(spec.decodeCodec, string(spec.decodeCodec.ID))
		operations = append([]OperationSpec{operation}, operations...)
	}
	switch {
	case spec.encode.Copy && !operationSpecsContainKind(operations, OpCopy):
		operations = append(operations, operationSpecForCopy(spec.encode))
	case codecIntentSet(spec.encode) && !spec.encode.Copy && !operationSpecsContainKind(operations, OpEncode):
		operations = append(operations, operationSpecForEncode(spec.encode))
	}
	after := OpEncode
	if spec.encode.Copy {
		after = OpCopy
	}
	for i := range spec.postEncodeTaps {
		tapName := spec.postEncodeTaps[i]
		if operationSpecsContainTap(operations, tapName) {
			continue
		}
		tap := PacketTap(tapName)
		operations = append(operations, operationSpecForTap(tap, spec.media, after))
	}
	return operations
}

func operationSpecsContainTap(operations []OperationSpec, name string) bool {
	if name == "" {
		return false
	}
	for i := range operations {
		if operations[i].Kind == OpTap && (operations[i].Component == name || operations[i].Tap.Name == name) {
			return true
		}
	}
	return false
}

func runtimeBranchPostEncodeTapsFromOperations(operations []OperationSpec, explicit []string) []string {
	if len(operations) == 0 && len(explicit) == 0 {
		return nil
	}
	taps := make([]string, 0, len(explicit))
	seen := make(map[string]struct{}, len(explicit))
	for i := range explicit {
		name := explicit[i]
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		taps = append(taps, name)
	}
	for i := range operations {
		operation := operations[i]
		if !operationSpecTapIsTerminalPacket(operation) || operation.Tap.Name == "" {
			continue
		}
		name := operation.Tap.Name
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		taps = append(taps, name)
	}
	return taps
}

func runtimeBranchStepsFromOperationSpecs(operations []OperationSpec) []runtimeBranchStep {
	if len(operations) == 0 {
		return nil
	}
	out := make([]runtimeBranchStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case OpDecode:
			out = append(out, runtimeBranchStep{kind: OpDecode, decode: true, codec: cloneCodecSpec(operation.Decode)})
		case OpStage:
			if operation.Stage != nil {
				out = append(out, runtimeBranchStep{kind: OpStage, stage: operation.Stage})
			}
		case OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				out = append(out, runtimeBranchStep{kind: OpShape, shapeUpdate: operation.Shape})
			}
		case OpTransform:
			if operation.Transform.Resize != nil || operation.Transform.Resample != nil {
				out = append(out, runtimeBranchStep{kind: OpTransform, transform: cloneTransformSpec(operation.Transform)})
			}
		case OpTap:
			if operation.Tap.Name != "" && !operationSpecTapIsTerminalPacket(operation) {
				out = append(out, runtimeBranchStep{
					kind:      OpTap,
					tap:       operation.Tap.Name,
					tapDomain: operation.Tap.Domain,
					after:     operation.Tap.After,
				})
			}
		}
	}
	return out
}

func validateRuntimeBranchGroupDestinations(branches []runtimeBranch) (runtimeBranchGroupDestinations, error) {
	var destinations runtimeBranchGroupDestinations
	seen := make(map[string]runtimeBranchDestination)
	seenBranch := make(map[string]string)
	for i := range branches {
		branch := branches[i]
		for j := range branch.destinations {
			label := branch.destinations[j].name
			if label == "" {
				continue
			}
			if first, ok := seen[label]; ok {
				if first.shareKey != "" && first.shareKey == branch.destinations[j].shareKey {
					switch {
					case runtimeBranchDestinationCanShareSink(first) && runtimeBranchDestinationCanShareSink(branch.destinations[j]):
						if destinations.sharedSinkKeys == nil {
							destinations.sharedSinkKeys = make(map[string]struct{})
						}
						destinations.sharedSinkKeys[first.shareKey] = struct{}{}
						continue
					case runtimeBranchDestinationCanShareMux(first) && runtimeBranchDestinationCanShareMux(branch.destinations[j]):
						if destinations.sharedMuxKeys == nil {
							destinations.sharedMuxKeys = make(map[string]struct{})
						}
						destinations.sharedMuxKeys[first.shareKey] = struct{}{}
						continue
					}
				}
				return destinations, &BuildError{
					Code:      "destination_duplicate",
					Operation: "attach runtime branches",
					Node:      firstNonEmpty(branch.name, "branch"),
					Reason:    "runtime branch group reuses one destination name",
					Details: []string{
						"destination: " + label,
						"first branch: " + seenBranch[label],
						"second branch: " + firstNonEmpty(branch.name, fmt.Sprintf("branch-%d", i+1)),
					},
					Suggestions: []string{
						"reuse one destination value when branches should share a runtime destination group",
						"create distinct destination values with distinct names for independent runtime destinations",
						"use a sink destination for runtime diagnostic groups or a mux destination for runtime recording groups",
					},
					Cause: ErrUnsupportedBuild,
				}
			}
			seen[label] = branch.destinations[j]
			seenBranch[label] = firstNonEmpty(branch.name, fmt.Sprintf("branch-%d", i+1))
		}
	}
	return destinations, nil
}

func runtimeBranchDestinationCanShareSink(destination runtimeBranchDestination) bool {
	return destination.sink != nil && !destinationSpecHasOutput(destination.dest)
}

func runtimeBranchDestinationCanShareMux(destination runtimeBranchDestination) bool {
	return destination.sink == nil && destinationSpecHasOutput(destination.dest)
}

func runtimeBranchSharedDestinationKey(target destinationRef) string {
	if target.id == 0 {
		return ""
	}
	return strconv.FormatUint(target.id, 10)
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

func (g *runtimeAttachGroup) reserveSharedSink(spec pipeline.Spec, terminal runtimeBranchTerminal) error {
	if g == nil || !g.isSharedSink(terminal.shareKey) {
		return nil
	}
	if _, ok := g.sharedSinks[terminal.shareKey]; ok {
		return nil
	}
	name := firstNonEmpty(terminal.name, terminal.sink.Name(), "sink")
	if err := g.reserveNode(spec, name); err != nil {
		return err
	}
	g.sharedSinks[terminal.shareKey] = &runtimeSharedSinkDestination{name: name, sink: terminal.sink}
	return nil
}

func (g *runtimeAttachGroup) sharedSinkRef(graph pipeline.Graph, terminal runtimeBranchTerminal, buffer pipeline.BufferPolicy) (pipeline.NodeRef, bool, error) {
	if g == nil || !g.isSharedSink(terminal.shareKey) {
		return "", false, runtimeBranchInvalidError("shared sink destination is not registered", "reuse one goav.Sink(sink) destination value inside one Task.Attach call")
	}
	target := g.sharedSinks[terminal.shareKey]
	if target == nil {
		return "", false, runtimeBranchInvalidError("shared sink destination is not reserved", "reuse one goav.Sink(sink) destination value inside one Task.Attach call")
	}
	if target.ref != "" {
		return target.ref, false, nil
	}
	ref, err := graph.AddSink(namedSink{name: target.name, sink: target.sink}, buffer)
	if err != nil {
		return "", false, runtimeBranchGraphError("add sink", target.name, err)
	}
	target.ref = ref
	return ref, true, nil
}

func (g *runtimeAttachGroup) reserveSharedMux(spec pipeline.Spec, branch runtimeBranch, terminal runtimeBranchTerminal) error {
	if g == nil || !g.isSharedMux(terminal.shareKey) {
		return nil
	}
	target, ok := g.sharedMuxes[terminal.shareKey]
	if !ok {
		name := firstNonEmpty(terminal.name, terminal.dest.label(firstNonEmpty(branch.name, "target")), "target")
		if err := g.reserveNode(spec, name); err != nil {
			return err
		}
		target = &runtimeSharedMuxDestination{
			name:   name,
			dest:   terminal.dest,
			buffer: branch.buffer,
		}
		g.sharedMuxes[terminal.shareKey] = target
		g.muxOrder = append(g.muxOrder, terminal.shareKey)
	}
	target.streams = append(target.streams, terminal.stream)
	target.branches = append(target.branches, firstNonEmpty(branch.name, "branch"))
	return nil
}

func (g *runtimeAttachGroup) addSharedMuxRoute(key string, route pipeline.Route) error {
	if g == nil || !g.isSharedMux(key) {
		return runtimeBranchInvalidError("shared mux destination is not registered", "reuse one goav.File(name, writer) destination value inside one Task.Attach call")
	}
	target := g.sharedMuxes[key]
	if target == nil {
		return runtimeBranchInvalidError("shared mux destination is not reserved", "reuse one goav.File(name, writer) destination value inside one Task.Attach call")
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
			"runtime branch mux destination groups require the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching grouped file or URI branches",
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
			"runtime branch mux destinations require the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching file or URI branches",
		)
	}
	result, err := rt.formats.Probe(ctx, outputProbeRequest(dest.output))
	if err != nil {
		return "", outputFormatProbeError(dest.output, index, err)
	}
	return result.Format, nil
}

func runtimeMuxCompatibilityIssue(destinationName string, formatID av.FormatID, branches []string, streams []av.Stream, rt Runtime) (muxCompatibilityIssue, bool) {
	output := planOutput{
		Name:       destinationName,
		Operation:  OpMux,
		Format:     formatID,
		BranchRefs: append([]string(nil), branches...),
	}
	plannedStreams := make([]plannedMuxStream, 0, len(streams))
	for i := range streams {
		branch := "branch"
		if i < len(branches) && branches[i] != "" {
			branch = branches[i]
		}
		stream := streams[i]
		plannedStreams = append(plannedStreams, plannedMuxStream{
			Branch: branch,
			Codec:  stream.Codec.ID,
			Media:  firstNonEmptyMedia(stream.Type, stream.Codec.Type, codecMedia(stream.Codec.ID)),
		})
	}
	return checkKnownMuxCompatibility(output, plannedStreams, rt)
}

func runtimeAttachmentName(branches []runtimeBranch) string {
	if len(branches) == 1 {
		return branches[0].name
	}
	names := make([]string, 0, len(branches))
	for i := range branches {
		if branches[i].name != "" {
			names = append(names, branches[i].name)
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

func (t *task) resolveRuntimeBranchAnchor(branch *runtimeBranch, spec pipeline.Spec, pending []TapInfo) (string, TapInfo, error) {
	if branch == nil {
		return "", TapInfo{}, runtimeBranchInvalidError("branch is nil", "build branches with goav.Branch(name)")
	}
	if branch.tap != "" {
		taps := append([]TapInfo(nil), t.tapsLocked()...)
		taps = append(taps, pending...)
		for _, tap := range taps {
			if tap.Name == branch.tap {
				if err := validateTapDomain("attach runtime branch", firstNonEmpty(branch.name, "branch"), TapRef{name: branch.tap, domain: branch.tapDomain}, tap.Domain); err != nil {
					return "", TapInfo{}, err
				}
				return tap.Node.String(), tap, nil
			}
		}
		return "", TapInfo{}, runtimeBranchTapMissingError(branch.tap, taps)
	}
	if !specHasNode(spec, branch.from) {
		return "", TapInfo{}, runtimeBranchAnchorMissingError(branch.from)
	}
	return branch.from, TapInfo{Node: pipeline.NodeRef(branch.from)}, nil
}

func (t *task) prepareRuntimeBranch(ctx context.Context, branch *runtimeBranch, group *runtimeAttachGroup) error {
	if branch == nil {
		return nil
	}
	currentShape := runtimeBranchAnchorShape(branch.anchor)
	if branch.media != "" && currentShape.MediaKind != "" {
		if err := validateChainMedia("attach runtime branch", firstNonEmpty(branch.name, "branch"), currentShape.MediaKind, chainSpec{name: branch.name, media: branch.media}); err != nil {
			return err
		}
	}
	if err := validateRuntimeBranchShapeContract(*branch, currentShape); err != nil {
		return err
	}
	currentStream := streamFromRuntimeBranchShape(branch.name, currentShape)
	transformIndex := 0
	for i := range branch.steps {
		step := &branch.steps[i]
		switch {
		case step.decode:
			decodedStage, err := t.prepareRuntimeBranchDecode(ctx, branch.name, currentStream, currentShape, step.codec)
			if err != nil {
				closeRuntimeBranchOwnedStages(*branch)
				return err
			}
			step.stage = decodedStage
			step.decode = false
			step.owned = true
			currentShape.Domain = DomainFrame
			step.shape = currentShape
		case step.stage != nil:
			step.shape = currentShape
		case !mediaShapeEmpty(step.shapeUpdate):
			currentShape = mergeMediaShape(currentShape, step.shapeUpdate)
			step.shape = currentShape
		case runtimeBranchStepHasTransform(*step):
			if t.runtime == nil {
				closeRuntimeBranchOwnedStages(*branch)
				return runtimeBranchInvalidError(
					"runtime branch transforms require the standard runtime",
					"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching resize/resample branches",
				)
			}
			if currentShape.Domain != DomainFrame {
				closeRuntimeBranchOwnedStages(*branch)
				return runtimeBranchInvalidError(
					"runtime branch transforms require a frame tap",
					"attach transform branches with .From(goav.FrameTap(name)) where name is declared after Decode, Resize, Resample, or a frame-stage Tap",
				)
			}
			transformName := transformFactoryName(step.transform)
			streamIntent := StreamIntent{
				Name:       firstNonEmpty(branch.name, "branch"),
				Select:     StreamSelect{Type: currentStream.Type},
				Transforms: []TransformSpec{step.transform},
			}
			if _, err := t.runtime.filters.Factory(transformName); err != nil {
				closeRuntimeBranchOwnedStages(*branch)
				return recipeTransformAdapterError("attach runtime branch", streamIntent, transformName, err)
			}
			if desc, err := t.runtime.filters.Descriptor(transformName); err == nil {
				if err := validateTransformAdapterDescriptor("attach runtime branch", streamIntent, step.transform, transformName, desc); err != nil {
					closeRuntimeBranchOwnedStages(*branch)
					return err
				}
			}
			transform, err := runtimeBranchTransform(branch.name, currentStream, step.transform, transformIndex)
			if err != nil {
				closeRuntimeBranchOwnedStages(*branch)
				return err
			}
			stage, outputStream, err := (&builder{runtime: t.runtime}).newMediaTransformStage(ctx, transform, currentStream, t.runtime.realtime)
			if err != nil {
				closeRuntimeBranchOwnedStages(*branch)
				return runtimeBranchTransformError(transform.name, err)
			}
			step.stage = stage
			step.transform = TransformSpec{}
			step.owned = true
			currentStream = outputStream
			currentShape = mediaShapeFromRuntimeBranchStream(outputStream, currentShape)
			step.shape = currentShape
			transformIndex++
		case step.tap != "":
			if err := validateTapDomain("attach runtime branch", firstNonEmpty(branch.name, "branch"), TapRef{name: step.tap, domain: step.tapDomain}, currentShape.Domain); err != nil {
				closeRuntimeBranchOwnedStages(*branch)
				return err
			}
			step.shape = currentShape
		}
	}
	if err := t.prepareRuntimeBranchDestinations(ctx, branch, currentStream, currentShape, group); err != nil {
		return err
	}
	return nil
}

func validateRuntimeBranchShapeContract(branch runtimeBranch, initial MediaShape) error {
	stream := StreamIntent{
		Name: branch.name,
		Select: StreamSelect{
			Type:  firstNonEmptyMedia(branch.media, initial.MediaKind),
			Codec: initial.Codec,
		},
		Operations: runtimeBranchShapeOperationSpecs(branch),
	}
	if err := validateOperationSpecShapes("attach runtime branch", stream, initial); err != nil {
		return err
	}
	shape := runtimeBranchFinalShape(branch, initial)
	for i := range branch.destinations {
		destination := branch.destinations[i]
		if destination.sink != nil {
			continue
		}
		if !destinationSpecHasOutput(destination.dest) {
			continue
		}
		if shape.Domain == DomainPacket {
			continue
		}
		destinationName := firstNonEmpty(destination.name, destination.dest.label(fmt.Sprintf("destination%d", i+1)))
		return destinationShapeMismatchError("attach runtime branch", branch.name, destinationName, destination.dest, shape)
	}
	return nil
}

func runtimeBranchShapeOperationSpecs(branch runtimeBranch) []OperationSpec {
	operations := cloneOperationSpecs(branch.operations)
	if branch.encode.Copy && !operationSpecsContainKind(operations, OpCopy) {
		operations = append(operations, OperationSpec{Kind: OpCopy, Component: "packet-copy", Encode: cloneCodecSpec(branch.encode)})
	} else if codecIntentSet(branch.encode) && !branch.encode.Copy && !operationSpecsContainKind(operations, OpEncode) {
		operations = append(operations, OperationSpec{Kind: OpEncode, Component: string(branch.encode.ID), Encode: cloneCodecSpec(branch.encode)})
	}
	return operations
}

func runtimeBranchFinalShape(branch runtimeBranch, initial MediaShape) MediaShape {
	shape := normalizeTapShape(initial)
	for _, operation := range runtimeBranchShapeOperationSpecs(branch) {
		shape = operationSpecOutputShape(shape, operation)
	}
	return shape
}

func (t *task) prepareRuntimeBranchDestinations(ctx context.Context, branch *runtimeBranch, currentStream av.Stream, currentShape MediaShape, group *runtimeAttachGroup) error {
	if branch == nil {
		return nil
	}
	if len(branch.destinations) == 0 {
		return runtimeBranchInvalidError("branch destination is missing", "finish the branch with .To(goav.Sink(sink)) or .To(goav.File(name, writer))")
	}
	stream := currentStream
	shape := currentShape
	hasMuxDestination := runtimeBranchHasMuxDestination(*branch)
	if branch.encode.Copy {
		if currentShape.Domain != DomainPacket {
			closeRuntimeBranchOwnedStages(*branch)
			return runtimeBranchCopyDomainError(branch.name, currentShape)
		}
		appendRuntimeBranchPostEncodeTaps(branch, shape, OpCopy)
	} else if codecIntentSet(branch.encode) {
		encodedStream, err := t.prepareRuntimeBranchEncode(ctx, branch, currentStream, currentShape)
		if err != nil {
			closeRuntimeBranchOwnedStages(*branch)
			return err
		}
		stream = encodedStream
		shape = streamPacketShapeFromRuntimeBranchStream(encodedStream, currentShape)
		appendRuntimeBranchPostEncodeTaps(branch, shape, OpEncode)
	} else if hasMuxDestination {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchEncodeMissingError(branch.name)
	}
	if !hasMuxDestination {
		for i := range branch.destinations {
			branch.terminals = append(branch.terminals, runtimeBranchSinkTerminal(branch.destinations[i], stream, shape))
		}
		return nil
	}
	if t.runtime == nil {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchInvalidError(
			"runtime branch mux destinations require the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching file or URI branches",
		)
	}
	if stream.Codec.ID == "" {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchMuxCodecMissingError(branch.name, shape)
	}
	for i := range branch.destinations {
		destination := branch.destinations[i]
		if destination.sink != nil {
			branch.terminals = append(branch.terminals, runtimeBranchSinkTerminal(destination, stream, shape))
			continue
		}
		if group != nil && group.isSharedMux(destination.shareKey) {
			branch.terminals = append(branch.terminals, runtimeBranchSharedMuxTerminal(destination, stream, shape))
			continue
		}
		terminal, err := prepareRuntimeBranchMuxTerminal(ctx, t.runtime, *branch, destination, stream, shape, i)
		if err != nil {
			closeRuntimeBranchOwnedStages(*branch)
			return err
		}
		branch.terminals = append(branch.terminals, terminal)
	}
	return nil
}

func runtimeBranchSinkTerminal(destination runtimeBranchDestination, stream av.Stream, shape MediaShape) runtimeBranchTerminal {
	return runtimeBranchTerminal{
		name:     destination.name,
		shareKey: destination.shareKey,
		dest:     destination.dest,
		stream:   stream,
		sink:     destination.sink,
		shape:    shape,
	}
}

func runtimeBranchSharedMuxTerminal(destination runtimeBranchDestination, stream av.Stream, shape MediaShape) runtimeBranchTerminal {
	return runtimeBranchTerminal{
		name:     destination.name,
		shareKey: destination.shareKey,
		dest:     destination.dest,
		stream:   stream,
		shape:    shape,
	}
}

func prepareRuntimeBranchMuxTerminal(ctx context.Context, rt *runtime, branch runtimeBranch, destination runtimeBranchDestination, stream av.Stream, shape MediaShape, index int) (runtimeBranchTerminal, error) {
	formatID, err := runtimeMuxDestinationFormat(ctx, rt, destination.dest, index)
	if err != nil {
		return runtimeBranchTerminal{}, err
	}
	branchName := firstNonEmpty(branch.name, destination.name, "branch")
	destinationName := firstNonEmpty(destination.name, destination.dest.label(branchName))
	if issue, ok := runtimeMuxCompatibilityIssue(destinationName, formatID, []string{branchName}, []av.Stream{stream}, rt); ok {
		return runtimeBranchTerminal{}, muxCompatibilityBuildError("attach runtime branch", issue)
	}
	muxStage, err := (&builder{runtime: rt}).openMuxDestinationStage(
		ctx,
		destination.dest,
		index,
		[]av.Stream{stream},
		formatID,
		destinationGraphFormat(destination.dest),
	)
	if err != nil {
		return runtimeBranchTerminal{}, err
	}
	return runtimeBranchTerminal{
		name:   destination.name,
		dest:   destination.dest,
		stream: stream,
		stage:  muxStage,
		shape:  shape,
		owned:  true,
	}, nil
}

func (t *task) prepareRuntimeBranchDecode(ctx context.Context, branchName string, currentStream av.Stream, currentShape MediaShape, spec CodecSpec) (pipeline.Stage, error) {
	if t.runtime == nil {
		return nil, runtimeBranchInvalidError(
			"runtime branch decoding requires the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching decode branches",
		)
	}
	if currentShape.Domain != DomainPacket {
		return nil, runtimeBranchDecodeDomainError(branchName, currentShape)
	}
	if currentStream.Codec.ID == "" {
		return nil, runtimeBranchDecodeCodecMissingError(branchName, currentShape)
	}
	request := runtimeBranchDecodeRequest(branchName, currentStream, spec)
	if _, err := t.runtime.codecs.DecoderFactory(currentStream.Codec.ID); err != nil {
		stream := StreamIntent{Name: branchName, Decode: true}
		return nil, recipeDecodeAdapterError("attach runtime branch", stream, currentStream.Codec.ID, t.runtime.codecs, err)
	}
	stream := StreamIntent{
		Name:   branchName,
		Select: streamSelectFromAV(request.selector),
		Decode: true,
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

func runtimeBranchHasMuxDestination(branch runtimeBranch) bool {
	for i := range branch.destinations {
		if branch.destinations[i].sink == nil && destinationSpecHasOutput(branch.destinations[i].dest) {
			return true
		}
	}
	return false
}

func (t *task) prepareRuntimeBranchEncode(ctx context.Context, branch *runtimeBranch, currentStream av.Stream, currentShape MediaShape) (av.Stream, error) {
	if t.runtime == nil {
		return av.Stream{}, runtimeBranchInvalidError(
			"runtime branch encoding requires the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching encode branches",
		)
	}
	if currentShape.Domain != DomainFrame {
		return av.Stream{}, runtimeBranchEncodeDomainError(branch.name, currentShape)
	}
	if err := validateRecipeEncode(branch.encode, "attach runtime branch", firstNonEmpty(branch.name, "branch")); err != nil {
		return av.Stream{}, err
	}
	if _, err := t.runtime.codecs.EncoderFactory(branch.encode.ID); err != nil {
		stream := StreamIntent{Name: branch.name, Encode: branch.encode}
		return av.Stream{}, recipeEncodeAdapterError("attach runtime branch", stream, t.runtime.codecs, err)
	}
	request := runtimeBranchEncodeRequest(*branch, currentStream)
	config, encodedStream, err := prepareEncodeConfig(currentStream, request, t.runtime.realtime)
	if err != nil {
		return av.Stream{}, err
	}
	stream := StreamIntent{Name: branch.name, Select: StreamSelect{Type: currentStream.Type}, Encode: branch.encode}
	if err := validateEncodeAdapterDescriptors("attach runtime branch", stream, t.runtime.codecs, encodeAdapterRequestFromPreparedStream(branch.encode, encodedStream)); err != nil {
		return av.Stream{}, err
	}
	stage, err := (&builder{runtime: t.runtime}).newEncodeStage(ctx, request, config)
	if err != nil {
		return av.Stream{}, err
	}
	branch.steps = append(branch.steps, runtimeBranchStep{
		stage: stage,
		kind:  OpEncode,
		shape: streamPacketShapeFromRuntimeBranchStream(encodedStream, currentShape),
		owned: true,
	})
	return encodedStream, nil
}

func appendRuntimeBranchPostEncodeTaps(branch *runtimeBranch, shape MediaShape, after OperationKind) {
	if branch == nil || len(branch.postEncodeTaps) == 0 {
		return
	}
	for i := range branch.postEncodeTaps {
		branch.steps = append(branch.steps, runtimeBranchStep{
			tap:       branch.postEncodeTaps[i],
			kind:      OpTap,
			tapDomain: DomainPacket,
			after:     after,
			shape:     shape,
		})
	}
	branch.postEncodeTaps = nil
}

func (t *task) attachRuntimeBranch(branch runtimeBranch, nodeNames []string, group *runtimeAttachGroup) ([]pipeline.NodeRef, []pipeline.Route, []TapInfo, error) {
	refs := make([]pipeline.NodeRef, 0, len(nodeNames))
	routes := make([]pipeline.Route, 0, len(nodeNames))
	taps := make([]TapInfo, 0, runtimeBranchTapCount(branch))
	previous := pipeline.NodeRef(branch.from)
	stageIndex := 0
	connectedFromAnchor := false
	for i := range branch.steps {
		step := branch.steps[i]
		if step.tap != "" {
			taps = append(taps, runtimeBranchTapInfo(step.tap, previous, step.shape, step.after))
			continue
		}
		if step.stage == nil {
			continue
		}
		ref, err := t.graph.AddStage(namedStage{name: nodeNames[stageIndex], stage: step.stage}, branch.buffer)
		if err != nil {
			return refs, routes, taps, runtimeBranchGraphError("add stage", nodeNames[stageIndex], err)
		}
		refs = append(refs, ref)
		if !connectedFromAnchor {
			route := runtimeBranchRoute(pipeline.NodeRef(branch.from), ref, branch)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch", nodeNames[stageIndex], err)
			}
			routes = append(routes, route)
			connectedFromAnchor = true
		} else {
			route := routeBetween(previous, ref)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch stage", nodeNames[stageIndex], err)
			}
			routes = append(routes, route)
		}
		previous = ref
		stageIndex++
	}

	terminalNameIndex := 0
	for i := range branch.terminals {
		terminal := branch.terminals[i]
		if group != nil && group.isSharedSink(terminal.shareKey) {
			ref, added, err := group.sharedSinkRef(t.graph, terminal, branch.buffer)
			if err != nil {
				return refs, routes, taps, err
			}
			if added {
				refs = append(refs, ref)
			}
			var route pipeline.Route
			if !connectedFromAnchor {
				route = runtimeBranchRoute(pipeline.NodeRef(branch.from), ref, branch)
			} else {
				route = routeBetween(previous, ref)
			}
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch target", ref.String(), err)
			}
			routes = append(routes, route)
			continue
		}
		if group != nil && group.isSharedMux(terminal.shareKey) {
			var route pipeline.Route
			if !connectedFromAnchor {
				route = runtimeBranchRoute(pipeline.NodeRef(branch.from), pipeline.NodeRef(terminal.name), branch)
			} else {
				route = routeBetween(previous, pipeline.NodeRef(terminal.name))
			}
			if err := group.addSharedMuxRoute(terminal.shareKey, route); err != nil {
				return refs, routes, taps, err
			}
			continue
		}
		nodeName := nodeNames[stageIndex+terminalNameIndex]
		terminalNameIndex++
		var (
			ref pipeline.NodeRef
			err error
		)
		switch {
		case terminal.stage != nil:
			ref, err = t.graph.AddStage(namedStage{name: nodeName, stage: terminal.stage}, branch.buffer)
			if err != nil {
				return refs, routes, taps, runtimeBranchGraphError("add stage", nodeName, err)
			}
		case terminal.sink != nil:
			ref, err = t.graph.AddSink(namedSink{name: nodeName, sink: terminal.sink}, branch.buffer)
			if err != nil {
				return refs, routes, taps, runtimeBranchGraphError("add sink", nodeName, err)
			}
		default:
			continue
		}
		refs = append(refs, ref)
		if !connectedFromAnchor {
			route := runtimeBranchRoute(pipeline.NodeRef(branch.from), ref, branch)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch", nodeName, err)
			}
			routes = append(routes, route)
			continue
		}
		route := routeBetween(previous, ref)
		if err := t.graph.Connect(route); err != nil {
			return refs, routes, taps, runtimeBranchGraphError("connect branch target", nodeName, err)
		}
		routes = append(routes, route)
	}
	return refs, routes, taps, nil
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

func (t *task) tapsLocked() []TapInfo {
	var base []TapInfo
	if len(t.taps) != 0 {
		base = t.taps
	} else {
		base = inferSpecTaps(t.graph.Spec())
	}
	out := make([]TapInfo, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap TapInfo) {
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

func (t *task) addAttachmentTapsLocked(taps []TapInfo) {
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

func (t *task) stopAttachment(ctx context.Context, attachment *runtimeAttachment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, attachment)
}

func (t *task) stopAttachmentLocked(ctx context.Context, attachment *runtimeAttachment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attachment == nil {
		return nil
	}
	first := t.stopAttachmentChildrenLocked(ctx, attachment)
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		return first
	}
	err := attachment.stopLocked(t.graph)
	attachment.stopped = true
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	if first != nil {
		return first
	}
	return err
}

func (t *task) stopAttachmentChildrenLocked(ctx context.Context, attachment *runtimeAttachment) error {
	if attachment == nil {
		return nil
	}
	var first error
	for {
		child := t.childAttachmentLocked(attachment)
		if child == nil {
			return first
		}
		if err := t.stopAttachmentLocked(ctx, child); first == nil && err != nil {
			first = err
		}
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
	taps        []TapInfo
	work        workPatch
	stopMu      sync.Mutex
	stopped     bool
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

func (a *runtimeAttachment) Stats() BranchStats {
	if a == nil || a.owner == nil {
		return BranchStats{}
	}
	return branchStatsForNodes(a.owner.Stats(), a.nodes)
}

func (a *runtimeAttachment) Snapshot() BranchSnapshot {
	if a == nil {
		return BranchSnapshot{}
	}
	stats := TaskStats{}
	if a.owner == nil {
		return a.branchSnapshotLocked(stats)
	}
	stats = a.owner.Stats()
	a.owner.attachMu.Lock()
	defer a.owner.attachMu.Unlock()
	return a.branchSnapshotLocked(stats)
}

func (a *runtimeAttachment) branchSnapshotLocked(taskStats TaskStats) BranchSnapshot {
	if a == nil {
		return BranchSnapshot{}
	}
	state := "attached"
	if a.stopped {
		state = "detached"
	}
	spec := pipeline.Spec{}
	if a.owner != nil {
		spec = a.specFromGraph(a.owner.Describe())
	}
	return BranchSnapshot{
		ID:           a.id,
		Name:         a.name,
		State:        state,
		AnchorTaps:   append([]string(nil), a.allAnchorTaps()...),
		AnchorNodes:  append([]string(nil), a.allAnchorNodes()...),
		Nodes:        append([]pipeline.NodeRef(nil), a.nodes...),
		Taps:         append([]TapInfo(nil), a.taps...),
		Destinations: destinationSnapshotsFromWork(a.work.Destinations, !a.stopped),
		Spec:         spec,
		Stats:        branchStatsForNodes(taskStats, a.nodes),
	}
}

func (a *runtimeAttachment) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.owner != nil {
		return a.owner.stopAttachment(ctx, a)
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

func validateRuntimeBranch(branch runtimeBranch) error {
	if branch.err != nil {
		return branch.err
	}
	if branch.name == "" {
		return runtimeBranchInvalidError("branch name is empty", "start with goav.Branch(\"name\")")
	}
	if branch.from == "" && branch.tap == "" {
		return runtimeBranchInvalidError("branch source is empty", "call .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) with a tap from Task.Taps(), or .From(graphNode) with an expert graph handle")
	}
	if len(branch.destinations) == 0 {
		return runtimeBranchInvalidError("branch destination is missing", "finish the branch with .To(goav.Sink(sink)) or .To(goav.File(name, writer))")
	}
	if err := validateRuntimeBranchDestinations(branch); err != nil {
		return err
	}
	return nil
}

func validateRuntimeBranchDestinations(branch runtimeBranch) error {
	seen := make(map[string]int, len(branch.destinations))
	for i := range branch.destinations {
		name := firstNonEmpty(branch.destinations[i].name, branch.destinations[i].dest.label(fmt.Sprintf("target%d", i+1)))
		if firstIndex, ok := seen[name]; ok {
			return duplicateRuntimeBranchDestinationRefError(branch.name, name, firstIndex, i)
		}
		seen[name] = i
	}
	return nil
}

func destinationSpecHasOutput(dest destinationSpec) bool {
	return dest.output.Name != "" ||
		dest.output.URI != "" ||
		dest.output.Writer != nil ||
		dest.format != "" ||
		dest.resolvedFormat != ""
}

func runtimeBranchNodeNames(branch runtimeBranch, spec pipeline.Spec, group *runtimeAttachGroup) ([]string, error) {
	capacity := runtimeBranchStageCount(branch) + len(branch.terminals)
	names := make([]string, 0, capacity)
	seen := make(map[string]struct{}, capacity)
	stageIndex := 0
	for i := range branch.steps {
		if branch.steps[i].stage == nil {
			continue
		}
		name := runtimeBranchNodeName(branch.name, branch.steps[i].stage.Name(), fmt.Sprintf("stage%d", stageIndex+1))
		if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
			return nil, err
		}
		if group != nil {
			if err := group.reserveNode(spec, name); err != nil {
				return nil, err
			}
		}
		names = append(names, name)
		stageIndex++
	}
	for i := range branch.terminals {
		terminal := branch.terminals[i]
		if group != nil && group.isSharedSink(terminal.shareKey) {
			if err := group.reserveSharedSink(spec, terminal); err != nil {
				return nil, err
			}
			continue
		}
		if group != nil && group.isSharedMux(terminal.shareKey) {
			if err := group.reserveSharedMux(spec, branch, terminal); err != nil {
				return nil, err
			}
			continue
		}
		name := runtimeBranchNodeName(branch.name, runtimeBranchTerminalName(terminal), fmt.Sprintf("target%d", i+1))
		if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
			return nil, err
		}
		if group != nil {
			if err := group.reserveNode(spec, name); err != nil {
				return nil, err
			}
		}
		names = append(names, name)
	}
	return names, nil
}

func runtimeBranchPlannedTaps(branch runtimeBranch, nodeNames []string) []TapInfo {
	if runtimeBranchTapCount(branch) == 0 {
		return nil
	}
	taps := make([]TapInfo, 0, runtimeBranchTapCount(branch))
	previous := pipeline.NodeRef(branch.from)
	stageIndex := 0
	for i := range branch.steps {
		step := branch.steps[i]
		if step.tap != "" {
			taps = append(taps, runtimeBranchTapInfo(step.tap, previous, step.shape, step.after))
			continue
		}
		if step.stage == nil {
			continue
		}
		if stageIndex >= len(nodeNames) {
			continue
		}
		previous = pipeline.NodeRef(nodeNames[stageIndex])
		stageIndex++
	}
	return taps
}

func runtimeBranchTerminalName(terminal runtimeBranchTerminal) string {
	switch {
	case terminal.stage != nil:
		return terminal.stage.Name()
	case terminal.sink != nil:
		return terminal.sink.Name()
	default:
		return ""
	}
}

func runtimeBranchStageCount(branch runtimeBranch) int {
	count := 0
	for i := range branch.steps {
		if branch.steps[i].stage != nil {
			count++
		}
	}
	return count
}

func runtimeBranchStepHasTransform(step runtimeBranchStep) bool {
	return step.transform.Resize != nil || step.transform.Resample != nil
}

func runtimeBranchTapCount(branch runtimeBranch) int {
	count := 0
	for i := range branch.steps {
		if branch.steps[i].tap != "" {
			count++
		}
	}
	return count
}

func (t *task) validateRuntimeBranchTapsLocked(branch runtimeBranch, pending []TapInfo) error {
	seen := make(map[string]struct{}, runtimeBranchTapCount(branch))
	current := append([]TapInfo(nil), t.tapsLocked()...)
	current = append(current, pending...)
	existing := make(map[string]struct{}, len(current))
	for i := range current {
		existing[current[i].Name] = struct{}{}
	}
	for i := range branch.steps {
		name := branch.steps[i].tap
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
			Code:      "transform_invalid",
			Operation: "attach runtime branch",
			Node:      base,
			Reason:    "one runtime branch transform cannot be both resize and resample",
			Cause:     ErrUnsupportedBuild,
		}
	case spec.Resize != nil:
		if stream.Type != av.MediaVideo && stream.Codec.Type != av.MediaVideo {
			return mediaTransform{}, runtimeBranchTransformMediaError(base, "resize", "video")
		}
		resize := *spec.Resize
		return mediaTransform{
			name:    "resize-" + base + suffix,
			factory: transformFactoryName(spec),
			video:   &resize,
		}, nil
	case spec.Resample != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return mediaTransform{}, runtimeBranchTransformMediaError(base, "resample", "audio")
		}
		resample := *spec.Resample
		return mediaTransform{
			name:    "resample-" + base + suffix,
			factory: transformFactoryName(spec),
			audio:   &resample,
		}, nil
	default:
		return mediaTransform{}, &BuildError{
			Code:      "transform_invalid",
			Operation: "attach runtime branch",
			Node:      base,
			Reason:    "empty runtime branch transform",
			Suggestions: []string{
				"call .Resize(width, height) for video frame taps",
				"call .Resample(sampleRate, channels) for audio frame taps",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
}

func runtimeBranchAnchorShape(anchor TapInfo) MediaShape {
	shape := anchor.Shape
	if shape.Domain == "" {
		shape.Domain = anchor.Domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = anchor.MediaKind
	}
	return shape
}

func streamFromRuntimeBranchShape(name string, shape MediaShape) av.Stream {
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

func runtimeBranchEncodeRequest(branch runtimeBranch, stream av.Stream) encodeRequest {
	config := encodeConfigFromSpec(branch.encode)
	if config.Stream.ID == "" {
		config.Stream.ID = av.StreamID(firstNonEmpty(branch.name, string(stream.ID), "branch"))
	}
	if config.Stream.Name == "" {
		config.Stream.Name = firstNonEmpty(branch.name, stream.Name, string(stream.ID), "branch")
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
		name:     firstNonEmpty(branch.name, string(stream.ID), "branch"),
		selector: selector,
		config:   config,
	}
}

func runtimeBranchDecodeRequest(branchName string, stream av.Stream, spec CodecSpec) decodeRequest {
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

func mediaShapeFromRuntimeBranchStream(stream av.Stream, previous MediaShape) MediaShape {
	shape := previous
	shape.Domain = DomainFrame
	if stream.Type != "" {
		shape.MediaKind = stream.Type
	}
	if stream.ID != "" {
		shape.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		shape.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		shape.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		shape.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		shape.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		shape.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		shape.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		shape.SampleFormat = stream.Codec.SampleFormat
	}
	return shape
}

func streamPacketShapeFromRuntimeBranchStream(stream av.Stream, previous MediaShape) MediaShape {
	shape := mediaShapeFromRuntimeBranchStream(stream, previous)
	shape.Domain = DomainPacket
	return shape
}

func runtimeBranchTapInfo(name string, node pipeline.NodeRef, shape MediaShape, after OperationKind) TapInfo {
	domain := shape.Domain
	if domain == "" {
		domain = DomainPacket
	}
	media := shape.MediaKind
	if shape.Domain == "" {
		shape.Domain = domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = media
	}
	return TapInfo{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		After:     after,
		Shape:     shape,
		Node:      node,
	}
}

func closeRuntimeBranchOwnedStages(branch runtimeBranch) {
	for i := range branch.steps {
		if branch.steps[i].owned && branch.steps[i].stage != nil {
			markPipelineStageFailed(branch.steps[i].stage)
			_ = branch.steps[i].stage.Close()
		}
	}
	for i := range branch.terminals {
		if branch.terminals[i].owned && branch.terminals[i].stage != nil {
			markPipelineStageFailed(branch.terminals[i].stage)
			_ = branch.terminals[i].stage.Close()
		}
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

func runtimeBranchRoute(from pipeline.NodeRef, to pipeline.NodeRef, branch runtimeBranch) pipeline.Route {
	policy := branch.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	return pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: policy,
		Label:  branch.label,
	}
}

func routeBetween(from pipeline.NodeRef, to pipeline.NodeRef) pipeline.Route {
	return pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: pipeline.RouteAll,
	}
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
		Code:      "runtime_branch_invalid",
		Operation: "attach runtime branch",
		Reason:    reason,
		Suggestions: []string{
			suggestion,
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchAnchorMissingError(node string) error {
	return &BuildError{
		Code:      "runtime_branch_anchor_missing",
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch source node does not exist in the running task graph",
		Suggestions: []string{
			"call task.Taps() and use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) for stable media outlets",
			"keep the GraphNode returned by goav.Expert(runtime).Graph().Source/Stage/Sink for expert graph attachments",
			"attach from a stable decoded-frame tap when the branch needs raw frames",
		},
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchTapMissingError(name string, taps []TapInfo) error {
	details := make([]string, 0, len(taps))
	for i := range taps {
		details = append(details, taps[i].Name+": "+string(taps[i].Domain)+" "+string(taps[i].MediaKind))
	}
	return &BuildError{
		Code:      "runtime_branch_tap_missing",
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch source tap does not exist in the running task",
		Details:   details,
		Suggestions: []string{
			"add .Tap(goav.FrameTap(" + strconv.Quote(name) + ")) or .Tap(goav.PacketTap(" + strconv.Quote(name) + ")) at the point you want to attach",
			"call task.Taps() before attaching runtime branches",
			"use .From(graphNode) only for expert graph-node attachments",
		},
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchNodeDuplicateError(node string) error {
	return &BuildError{
		Code:      "runtime_branch_node_duplicate",
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch node name already exists in the task graph",
		Suggestions: []string{
			"use a unique goav.Branch(name)",
			"use distinct stage and sink names inside repeated runtime branches",
		},
		Cause: pipeline.ErrNodeExists,
	}
}

func duplicateRuntimeBranchDestinationRefError(branch string, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "destination_duplicate",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", label),
		Details: []string{
			fmt.Sprintf("first destination index: %d", firstIndex),
			fmt.Sprintf("second destination index: %d", secondIndex),
		},
		Suggestions: []string{
			"list each destination once in .To(...)",
			"route one runtime branch to multiple destinations with distinct values such as .To(archive, monitor)",
			"reuse destination values across separate Branch(...) attachments when they should share a logical destination",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTapDuplicateError(name string) error {
	return &BuildError{
		Code:      "runtime_branch_tap_duplicate",
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch tap name already exists in the task",
		Suggestions: []string{
			"use a unique tap name for each runtime branch outlet",
			"call task.Taps() before attaching to inspect the current tap names",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTransformMediaError(branch string, transform string, media string) error {
	return &BuildError{
		Code:      "runtime_branch_transform_media_mismatch",
		Operation: "attach runtime branch",
		Node:      branch,
		Reason:    transform + " applies to " + media + " frame taps",
		Suggestions: []string{
			"use .Video().Decode().Tap(goav.FrameTap(name)) or a video transform tap before attaching .Resize(...)",
			"use .Audio().Decode().Tap(goav.FrameTap(name)) or an audio transform tap before attaching .Resample(...)",
			"call task.Taps() and choose a tap with matching media kind",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchTransformError(node string, cause error) error {
	if cause == nil {
		return nil
	}
	return &BuildError{
		Code:      "runtime_branch_transform_error",
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "runtime branch transform could not be opened",
		Suggestions: []string{
			"register a matching resize or resample filter adapter",
			"use goav.Default() or goav.New(goav.WithDefaults()) for standard filters",
			"attach from a frame tap with media shape that match the requested transform",
		},
		Cause: cause,
	}
}

func runtimeBranchEncodeMissingError(branch string) error {
	return &BuildError{
		Code:      "runtime_branch_encode_missing",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "muxed runtime branches need packet copy or an encoder",
		Suggestions: []string{
			"call .Copy() when attaching from a packet tap",
			"call .Encode(goav.Opus(...)), .Encode(goav.VP8(...)), or .Encode(goav.VP9(...)) when attaching from a frame tap",
			"use .To(goav.Sink(...)) when the runtime branch should receive raw frames",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchEncodeDomainError(branch string, shape MediaShape) error {
	return &BuildError{
		Code:      "runtime_branch_encode_domain_mismatch",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch encoding requires a frame tap",
		Details:   runtimeBranchShapeDetails(shape),
		Suggestions: []string{
			"attach from a tap declared after Decode, Resize, Resample, or a frame-stage .Do(...)",
			"use .Copy() from a packet tap when no re-encode is intended",
			"call task.Taps() and choose a tap with domain=frame",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchDecodeDomainError(branch string, shape MediaShape) error {
	return &BuildError{
		Code:      "runtime_branch_decode_domain_mismatch",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decoding requires a packet tap",
		Details:   runtimeBranchShapeDetails(shape),
		Suggestions: []string{
			"attach from a tap declared after Copy, packet receive, or Encode",
			"omit .Decode() when attaching from a frame tap",
			"call task.Taps() and choose a tap with domain=packet",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchDecodeCodecMissingError(branch string, shape MediaShape) error {
	return &BuildError{
		Code:      "runtime_branch_decode_codec_missing",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decode needs packet codec metadata",
		Details:   runtimeBranchShapeDetails(shape),
		Suggestions: []string{
			"attach from a recipe tap with codec shape",
			"declare the RTP/WebRTC/File input codec before building the task",
			"call task.Taps() and choose a packet tap that reports codec=...",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchCopyDomainError(branch string, shape MediaShape) error {
	return &BuildError{
		Code:      "runtime_branch_copy_domain_mismatch",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch packet copy requires a packet tap",
		Details:   runtimeBranchShapeDetails(shape),
		Suggestions: []string{
			"attach from a tap declared after Copy or Encode",
			"encode frame taps with .Encode(goav.Opus(...)), .Encode(goav.VP8(...)), or .Encode(goav.VP9(...)) before writing a muxed destination",
			"call task.Taps() and choose a tap with domain=packet",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchMuxCodecMissingError(branch string, shape MediaShape) error {
	return &BuildError{
		Code:      "runtime_branch_mux_codec_missing",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch mux destination needs codec metadata",
		Details:   runtimeBranchShapeDetails(shape),
		Suggestions: []string{
			"attach from a recipe tap with codec shape",
			"set an explicit encoder with .Encode(goav.Opus(...)), .Encode(goav.VP8(...)), or .Encode(goav.VP9(...))",
			"use .To(goav.Sink(...)) when the branch should stay raw",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchShapeDetails(shape MediaShape) []string {
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
		Code:      "runtime_branch_graph_error",
		Operation: operation,
		Node:      node,
		Reason:    "runtime graph rejected the branch attachment",
		Suggestions: []string{
			"runtime branches are supported on direct task graphs today",
			"use a direct buffer policy for live runtime branch experiments",
			"build the branch before Run when using buffered execution",
		},
		Cause: cause,
	}
}
