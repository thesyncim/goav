package goav

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

var runtimeAttachmentSeq atomic.Uint64

// runtimeBranch is the internal graph mutation plan for a branch attached to an
// already-built task.
type runtimeBranch struct {
	name           string
	from           string
	tap            string
	anchor         TapInfo
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
	stage     pipeline.Stage
	transform TransformSpec
	tap       string
	caps      StreamCaps
	owned     bool
}

type runtimeBranchDestination struct {
	name     string
	endpoint EndpointSpec
	sink     pipeline.Sink
}

type runtimeBranchTerminal struct {
	stage pipeline.Stage
	sink  pipeline.Sink
	caps  StreamCaps
	owned bool
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	ID() string
	Name() string
	Spec() pipeline.Spec
	Stats() BranchStats
	Close(context.Context) error
}

func (t *task) Attach(ctx context.Context, spec BranchSpec) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	branch, err := runtimeBranchFromSpec(spec)
	if err != nil {
		return nil, err
	}
	if err := validateRuntimeBranch(branch); err != nil {
		return nil, err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()

	graphSpec := t.graph.Spec()
	branch.from, branch.anchor, err = t.resolveRuntimeBranchAnchor(branch, graphSpec)
	if err != nil {
		return nil, err
	}
	if err := t.prepareRuntimeBranch(ctx, &branch); err != nil {
		return nil, err
	}
	if err := t.validateRuntimeBranchTapsLocked(branch); err != nil {
		closeRuntimeBranchOwnedStages(branch)
		return nil, err
	}

	nodeNames, err := runtimeBranchNodeNames(branch, graphSpec)
	if err != nil {
		closeRuntimeBranchOwnedStages(branch)
		return nil, err
	}
	refs, routes, taps, err := t.attachRuntimeBranch(branch, nodeNames)
	if err != nil {
		t.rollbackRuntimeBranch(refs)
		closeRuntimeBranchOwnedStages(branch)
		return nil, err
	}
	attachment := &runtimeAttachment{
		id:         nextRuntimeAttachmentID(branch.name),
		name:       branch.name,
		owner:      t,
		anchorTap:  branch.tap,
		anchorNode: branch.from,
		nodes:      refs,
		routes:     routes,
		taps:       taps,
	}
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(taps)
	return attachment, nil
}

func runtimeBranchFromSpec(spec BranchSpec) (runtimeBranch, error) {
	if spec.err != nil {
		return runtimeBranch{err: spec.err}, spec.err
	}
	branch := runtimeBranch{
		name:   spec.name,
		from:   spec.from,
		tap:    spec.tap,
		encode: spec.encode,
		policy: spec.policy,
		label:  spec.label,
		buffer: spec.buffer,
	}
	for i := range spec.steps {
		step := spec.steps[i]
		switch {
		case step.stage != nil:
			branch.steps = append(branch.steps, runtimeBranchStep{stage: step.stage})
		case step.transform.Resize != nil || step.transform.Resample != nil:
			branch.steps = append(branch.steps, runtimeBranchStep{transform: cloneTransformSpec(step.transform)})
		case step.tap != "":
			branch.steps = append(branch.steps, runtimeBranchStep{tap: step.tap})
		}
	}
	branch.postEncodeTaps = append([]string(nil), spec.postEncodeTaps...)
	if len(spec.targets) == 0 {
		return branch, runtimeBranchInvalidError("branch endpoint is missing", "finish the branch with .To(goav.SinkEndpoint(sink)) or .To(goav.Target(name, endpoint))")
	}
	for i := range spec.targets {
		target := cloneTargetSpec(spec.targets[i])
		if target.err != nil {
			return branch, target.err
		}
		endpoint := cloneEndpointSpec(target.endpoint)
		name := firstNonEmpty(target.name, spec.name, "branch")
		if err := endpoint.validate("attach runtime branch", name); err != nil {
			return branch, err
		}
		branch.destinations = append(branch.destinations, runtimeBranchDestination{
			name:     name,
			endpoint: endpoint,
			sink:     endpoint.sink,
		})
	}
	return branch, nil
}

func (t *task) resolveRuntimeBranchAnchor(branch runtimeBranch, spec pipeline.Spec) (string, TapInfo, error) {
	if branch.tap != "" {
		taps := t.tapsLocked()
		for _, tap := range taps {
			if tap.Name == branch.tap {
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

func (t *task) prepareRuntimeBranch(ctx context.Context, branch *runtimeBranch) error {
	if branch == nil {
		return nil
	}
	currentCaps := runtimeBranchAnchorCaps(branch.anchor)
	currentStream := streamFromRuntimeBranchCaps(branch.name, currentCaps)
	transformIndex := 0
	for i := range branch.steps {
		step := &branch.steps[i]
		switch {
		case step.stage != nil:
			step.caps = currentCaps
		case runtimeBranchStepHasTransform(*step):
			if t.runtime == nil {
				closeRuntimeBranchOwnedStages(*branch)
				return runtimeBranchInvalidError(
					"runtime branch transforms require the standard runtime",
					"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching resize/resample branches",
				)
			}
			if currentCaps.Domain != DomainFrame {
				closeRuntimeBranchOwnedStages(*branch)
				return runtimeBranchInvalidError(
					"runtime branch transforms require a frame tap",
					"attach transform branches with .FromTap(name) where name is declared after Decode, Resize, Resample, or a frame-stage Tap",
				)
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
			currentCaps = streamCapsFromRuntimeBranchStream(outputStream, currentCaps)
			step.caps = currentCaps
			transformIndex++
		case step.tap != "":
			step.caps = currentCaps
		}
	}
	if err := t.prepareRuntimeBranchDestination(ctx, branch, currentStream, currentCaps); err != nil {
		return err
	}
	return nil
}

func (t *task) prepareRuntimeBranchDestination(ctx context.Context, branch *runtimeBranch, currentStream av.Stream, currentCaps StreamCaps) error {
	if branch == nil {
		return nil
	}
	if len(branch.destinations) == 0 {
		return runtimeBranchInvalidError("branch endpoint is missing", "finish the branch with .To(goav.SinkEndpoint(sink)) or .To(goav.FileOutput(name, writer))")
	}
	stream := currentStream
	caps := currentCaps
	hasMuxEndpoint := runtimeBranchHasMuxEndpoint(*branch)
	if branch.encode.Copy {
		if currentCaps.Domain != DomainPacket {
			closeRuntimeBranchOwnedStages(*branch)
			return runtimeBranchCopyDomainError(branch.name, currentCaps)
		}
		appendRuntimeBranchPostEncodeTaps(branch, caps)
	} else if codecIntentSet(branch.encode) {
		encodedStream, err := t.prepareRuntimeBranchEncode(ctx, branch, currentStream, currentCaps)
		if err != nil {
			closeRuntimeBranchOwnedStages(*branch)
			return err
		}
		stream = encodedStream
		caps = streamPacketCapsFromRuntimeBranchStream(encodedStream, currentCaps)
		appendRuntimeBranchPostEncodeTaps(branch, caps)
	} else if hasMuxEndpoint {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchEncodeMissingError(branch.name)
	}
	if !hasMuxEndpoint {
		for i := range branch.destinations {
			branch.terminals = append(branch.terminals, runtimeBranchTerminal{sink: branch.destinations[i].sink, caps: caps})
		}
		return nil
	}
	if t.runtime == nil {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchInvalidError(
			"runtime branch mux endpoints require the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching file or URI branches",
		)
	}
	if stream.Codec.ID == "" {
		closeRuntimeBranchOwnedStages(*branch)
		return runtimeBranchMuxCodecMissingError(branch.name, caps)
	}
	for i := range branch.destinations {
		destination := branch.destinations[i]
		if destination.sink != nil {
			branch.terminals = append(branch.terminals, runtimeBranchTerminal{sink: destination.sink, caps: caps})
			continue
		}
		muxStage, err := (&builder{runtime: t.runtime}).openMuxStageWithFormat(
			ctx,
			destination.endpoint.output,
			i,
			[]av.Stream{stream},
			endpointSpecOpenFormat(destination.endpoint),
			endpointSpecGraphFormat(destination.endpoint),
		)
		if err != nil {
			closeRuntimeBranchOwnedStages(*branch)
			return err
		}
		branch.terminals = append(branch.terminals, runtimeBranchTerminal{
			stage: muxStage,
			caps:  caps,
			owned: true,
		})
	}
	return nil
}

func runtimeBranchHasMuxEndpoint(branch runtimeBranch) bool {
	for i := range branch.destinations {
		if branch.destinations[i].sink == nil && endpointSpecHasOutput(branch.destinations[i].endpoint) {
			return true
		}
	}
	return false
}

func (t *task) prepareRuntimeBranchEncode(ctx context.Context, branch *runtimeBranch, currentStream av.Stream, currentCaps StreamCaps) (av.Stream, error) {
	if t.runtime == nil {
		return av.Stream{}, runtimeBranchInvalidError(
			"runtime branch encoding requires the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching encode branches",
		)
	}
	if currentCaps.Domain != DomainFrame {
		return av.Stream{}, runtimeBranchEncodeDomainError(branch.name, currentCaps)
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
	stage, err := (&builder{runtime: t.runtime}).newEncodeStage(ctx, request, config)
	if err != nil {
		return av.Stream{}, err
	}
	branch.steps = append(branch.steps, runtimeBranchStep{
		stage: stage,
		caps:  streamPacketCapsFromRuntimeBranchStream(encodedStream, currentCaps),
		owned: true,
	})
	return encodedStream, nil
}

func appendRuntimeBranchPostEncodeTaps(branch *runtimeBranch, caps StreamCaps) {
	if branch == nil || len(branch.postEncodeTaps) == 0 {
		return
	}
	for i := range branch.postEncodeTaps {
		branch.steps = append(branch.steps, runtimeBranchStep{
			tap:  branch.postEncodeTaps[i],
			caps: caps,
		})
	}
	branch.postEncodeTaps = nil
}

func (t *task) attachRuntimeBranch(branch runtimeBranch, nodeNames []string) ([]pipeline.NodeRef, []pipeline.Route, []TapInfo, error) {
	refs := make([]pipeline.NodeRef, 0, len(nodeNames))
	routes := make([]pipeline.Route, 0, len(nodeNames))
	taps := make([]TapInfo, 0, runtimeBranchTapCount(branch))
	previous := pipeline.NodeRef(branch.from)
	stageIndex := 0
	connectedFromAnchor := false
	for i := range branch.steps {
		step := branch.steps[i]
		if step.tap != "" {
			taps = append(taps, runtimeBranchTapInfo(step.tap, previous, step.caps))
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

	for i := range branch.terminals {
		terminal := branch.terminals[i]
		nodeName := nodeNames[stageIndex+i]
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
		if _, ok := taps[attachment.anchorTap]; ok && attachment.anchorTap != "" {
			return attachment
		}
		if _, ok := nodes[attachment.anchorNode]; ok && attachment.anchorNode != "" {
			return attachment
		}
	}
	return nil
}

type runtimeAttachment struct {
	id         string
	name       string
	owner      *task
	anchorTap  string
	anchorNode string
	nodes      []pipeline.NodeRef
	routes     []pipeline.Route
	taps       []TapInfo
	stopMu     sync.Mutex
	stopped    bool
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
	graph := a.owner.Describe()
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
		return runtimeBranchInvalidError("branch source is empty", "call .FromTap(name) with a tap from Task.Taps() or .From(node) with an expert graph node")
	}
	if len(branch.destinations) == 0 {
		return runtimeBranchInvalidError("branch endpoint is missing", "finish the branch with .To(goav.SinkEndpoint(sink)) or .To(goav.FileOutput(name, writer))")
	}
	return nil
}

func endpointSpecHasOutput(endpoint EndpointSpec) bool {
	return endpoint.output.Name != "" ||
		endpoint.output.URI != "" ||
		endpoint.output.Writer != nil ||
		endpoint.format != "" ||
		endpoint.resolvedFormat != ""
}

func runtimeBranchNodeNames(branch runtimeBranch, spec pipeline.Spec) ([]string, error) {
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
		names = append(names, name)
		stageIndex++
	}
	for i := range branch.terminals {
		name := runtimeBranchNodeName(branch.name, runtimeBranchTerminalName(branch.terminals[i]), fmt.Sprintf("target%d", i+1))
		if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
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

func (t *task) validateRuntimeBranchTapsLocked(branch runtimeBranch) error {
	seen := make(map[string]struct{}, runtimeBranchTapCount(branch))
	current := t.tapsLocked()
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

func runtimeBranchAnchorCaps(anchor TapInfo) StreamCaps {
	caps := anchor.Caps
	if caps.Domain == "" {
		caps.Domain = anchor.Domain
	}
	if caps.MediaKind == "" {
		caps.MediaKind = anchor.MediaKind
	}
	return caps
}

func streamFromRuntimeBranchCaps(name string, caps StreamCaps) av.Stream {
	stream := av.Stream{
		ID:   av.StreamID(firstNonEmpty(string(caps.StreamID), name, "runtime-branch")),
		Name: firstNonEmpty(name, "runtime-branch"),
		Type: caps.MediaKind,
		Codec: av.CodecParameters{
			ID:            caps.Codec,
			Type:          caps.MediaKind,
			SampleRate:    caps.SampleRate,
			Channels:      caps.Channels,
			ChannelLayout: "",
			Width:         caps.Width,
			Height:        caps.Height,
			PixelFormat:   caps.PixelFormat,
			SampleFormat:  caps.SampleFormat,
		},
	}
	if caps.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(caps.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(caps.SampleRate)}
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

func streamCapsFromRuntimeBranchStream(stream av.Stream, previous StreamCaps) StreamCaps {
	caps := previous
	caps.Domain = DomainFrame
	if stream.Type != "" {
		caps.MediaKind = stream.Type
	}
	if stream.ID != "" {
		caps.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		caps.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		caps.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		caps.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		caps.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		caps.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		caps.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		caps.SampleFormat = stream.Codec.SampleFormat
	}
	return caps
}

func streamPacketCapsFromRuntimeBranchStream(stream av.Stream, previous StreamCaps) StreamCaps {
	caps := streamCapsFromRuntimeBranchStream(stream, previous)
	caps.Domain = DomainPacket
	return caps
}

func runtimeBranchTapInfo(name string, node pipeline.NodeRef, caps StreamCaps) TapInfo {
	domain := caps.Domain
	if domain == "" {
		domain = DomainPacket
	}
	media := caps.MediaKind
	if caps.Domain == "" {
		caps.Domain = domain
	}
	if caps.MediaKind == "" {
		caps.MediaKind = media
	}
	return TapInfo{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		Caps:      caps,
		Node:      node,
	}
}

func closeRuntimeBranchOwnedStages(branch runtimeBranch) {
	for i := range branch.steps {
		if branch.steps[i].owned && branch.steps[i].stage != nil {
			_ = branch.steps[i].stage.Close()
		}
	}
	for i := range branch.terminals {
		if branch.terminals[i].owned && branch.terminals[i].stage != nil {
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
			"call task.Taps() and use .FromTap(name) for stable media outlets",
			"call task.Describe() and use a node name from the graph spec for expert graph attachments",
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
			"add .Tap(" + strconv.Quote(name) + ") at the point you want to attach",
			"call task.Taps() before attaching runtime branches",
			"use .From(node) only for expert graph-node attachments",
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
			"use .Video().Decode().Tap(name) or a video transform tap before attaching .Resize(...)",
			"use .Audio().Decode().Tap(name) or an audio transform tap before attaching .Resample(...)",
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
			"attach from a frame tap with media caps that match the requested transform",
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
			"call .Opus(...), .VP8(...), or .VP9(...) when attaching from a frame tap",
			"use .To(goav.SinkEndpoint(...)) when the runtime branch should receive raw frames",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchEncodeDomainError(branch string, caps StreamCaps) error {
	return &BuildError{
		Code:      "runtime_branch_encode_domain_mismatch",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch encoding requires a frame tap",
		Details:   runtimeBranchCapsDetails(caps),
		Suggestions: []string{
			"attach from a tap declared after Decode, Resize, Resample, or a frame-stage .Do(...)",
			"use .Copy() from a packet tap when no re-encode is intended",
			"call task.Taps() and choose a tap with domain=frame",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchCopyDomainError(branch string, caps StreamCaps) error {
	return &BuildError{
		Code:      "runtime_branch_copy_domain_mismatch",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch packet copy requires a packet tap",
		Details:   runtimeBranchCapsDetails(caps),
		Suggestions: []string{
			"attach from a tap declared after Copy or Encode",
			"encode frame taps with .Opus(...), .VP8(...), or .VP9(...) before writing a muxed endpoint",
			"call task.Taps() and choose a tap with domain=packet",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchMuxCodecMissingError(branch string, caps StreamCaps) error {
	return &BuildError{
		Code:      "runtime_branch_mux_codec_missing",
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch mux endpoint needs codec metadata",
		Details:   runtimeBranchCapsDetails(caps),
		Suggestions: []string{
			"attach from a recipe tap with codec caps",
			"set an explicit encoder such as .Opus(...), .VP8(...), or .VP9(...)",
			"use .To(goav.SinkEndpoint(...)) when the branch should stay raw",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchCapsDetails(caps StreamCaps) []string {
	details := []string{
		"domain=" + string(caps.Domain),
		"media=" + string(caps.MediaKind),
	}
	if caps.Codec != "" {
		details = append(details, "codec="+string(caps.Codec))
	}
	if caps.StreamID != "" {
		details = append(details, "stream="+string(caps.StreamID))
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
