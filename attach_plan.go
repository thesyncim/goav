package goav

import (
	"context"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

// An attach group lowers as a build does. Its branches hang below anchors on
// the running task — a tap, an input's track, a graph route, or a tap an
// earlier branch of the group declares — and the build's lowering plans
// them node for node: work the branches share is one node, a destination
// several branches route to is one node, and every node a branch alone reads
// is named under the branch. The attach then checks the branches against
// what only the running task knows (the anchors' live shapes and the
// adapters they need), opens every planned node through the build's opener,
// and only then mutates the graph: each node is added in schedule order and
// connected to its inputs, and a failure removes what it added and closes
// what it opened.

// attachPlan is one attach group planned against a running task.
type attachPlan struct {
	task     *task
	name     string
	state    *recipeCompileState
	lowering *recipeLowering
	branches []attachPlanBranch
	// anchors are the live streams the group's anchor nodes carry.
	anchors map[pipeline.NodeRef]av.Stream
	// opened are the components the planned nodes opened as, and shapes
	// the shapes they emit, aligned with the schedule.
	opened  []openedNode
	shapes  map[pipeline.NodeRef]shape.Spec
	buffers []pipeline.BufferPolicy
	// taps are the taps the group installs; declared every named tap its
	// branches declare, which later branches may anchor on.
	taps     []snapshot.Tap
	declared map[string]attachDeclaredTap
}

// attachPlanBranch is one branch of an attach group: its anchor and the
// destinations it routes to.
type attachPlanBranch struct {
	name         string
	source       recipeir.RuntimeBranchSource
	anchor       attachAnchor
	destinations []attachDestination
	dests        []int
	buffer       pipeline.BufferPolicy
	copyNever    bool
	// stream is the stream the branch delivers, known once it is checked.
	stream av.Stream
}

// attachAnchor is where a branch reads the running task: a live node, or a
// tap an earlier branch of the group declares.
type attachAnchor struct {
	key     string
	ref     pipeline.NodeRef
	pending recipeir.NodeID
	route   anchorRoute
	tap     string
	shape   shape.Spec
	stream  av.Stream
	present bool
}

// attachDeclaredTap is a tap a branch of the group declares: its operation,
// and the shape and stream at it once its branch is checked.
type attachDeclaredTap struct {
	node   recipeir.NodeID
	domain shape.MediaDomain
	shape  shape.Spec
	stream av.Stream
	ok     bool
}

// planAttach plans an attach group below its anchors through the build's
// lowering, checks it against the live anchors, and opens its nodes. On
// failure it closes everything it opened.
func (t *task) planAttach(ctx context.Context, input runtimeAttachInput) (*attachPlan, error) {
	p := &attachPlan{
		task:     t,
		name:     firstNonEmpty(input.name, "runtime-attach"),
		anchors:  map[pipeline.NodeRef]av.Stream{},
		shapes:   map[pipeline.NodeRef]shape.Spec{},
		declared: map[string]attachDeclaredTap{},
	}
	if err := p.lower(input); err != nil {
		return nil, err
	}
	for i := range p.branches {
		if err := p.checkBranch(i); err != nil {
			return nil, err
		}
	}
	if err := p.checkMuxes(ctx); err != nil {
		return nil, err
	}
	if err := p.open(ctx); err != nil {
		p.close()
		return nil, err
	}
	if err := p.reserve(); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

// lower resolves each branch's anchor and lowers the group: the branches
// become the streams of one recipe whose roots are the anchors.
func (p *attachPlan) lower(input runtimeAttachInput) error {
	t := p.task
	graphSpec := t.graph.Spec()
	live := t.tapsLocked()
	s := &recipeCompileState{operation: "attach runtime branch", runtime: t.runtime, inserted: map[string]recipeir.NodeID{}}
	attach := &attachLowering{
		name:    p.name,
		anchors: map[recipeir.NodeID]pipeline.NodeRef{},
		routes:  map[recipeir.NodeID]anchorRoute{},
		pending: map[recipeir.NodeID]recipeir.NodeID{},
	}
	// Operations keep the identities of the Streams they came from, so work
	// several branches share is one node; an operation without one (a
	// planner-inserted copy, a rebranch switch gate) is its branch's own.
	next := recipeir.NodeID(0)
	for _, branch := range input.branches {
		for _, operation := range branch.recipe.branch.Operations {
			next = max(next, operation.Node+1)
		}
	}
	roots := map[string]recipeir.NodeID{}
	shared := map[string]int{}
	for _, branch := range input.branches {
		recipe := branch.recipe.branch
		anchor, err := p.resolveAnchor(recipe, graphSpec, live)
		if err != nil {
			return err
		}
		root, ok := roots[anchor.key]
		if !ok {
			root = next
			next++
			roots[anchor.key] = root
			if anchor.ref != "" {
				attach.anchors[root] = anchor.ref
				attach.routes[root] = anchor.route
			} else {
				attach.pending[root] = anchor.pending
			}
		}
		operations := cloneOperations(recipe.Operations)
		for j := range operations {
			if operations[j].Node < 0 {
				operations[j].Node = next
				next++
			}
			if tap := operations[j].Tap; operations[j].Kind == plan.OpTap && tap.Name != "" {
				if _, ok := p.declared[tap.Name]; !ok {
					p.declared[tap.Name] = attachDeclaredTap{node: operations[j].Node, domain: tap.Domain}
				}
			}
		}
		name := firstNonEmpty(recipe.Name, "branch")
		stream := recipeir.Stream{
			Name:       name,
			Named:      true,
			Root:       root,
			Input:      -1,
			Selector:   plan.StreamSelect{Type: firstNonEmptyMedia(recipe.Media, anchor.shape.MediaKind), Codec: anchor.shape.Codec},
			Operations: operations,
			Buffer:     recipe.Buffer,
		}
		planned := attachPlanBranch{
			name:         name,
			source:       recipe.Source,
			anchor:       anchor,
			destinations: branch.destinations,
			copyNever:    recipe.Buffer.CopyMode == flow.CopyNever,
		}
		facts := branchFacts{frames: anchor.shape.Domain == shape.DomainFrame}
		for _, destination := range branch.destinations {
			key := destination.recipe.ShareKey
			if index, ok := shared[key]; ok && (input.groupDestinations.sharesSink(key) || input.groupDestinations.sharesMux(key)) {
				facts.dests = append(facts.dests, index)
				continue
			}
			shared[key] = len(s.outputAttachments)
			facts.dests = append(facts.dests, len(s.outputAttachments))
			s.outputAttachments = append(s.outputAttachments, destination.dest)
			s.outputDestinationNames = append(s.outputDestinationNames, destination.recipe.Name)
		}
		planned.dests = facts.dests
		s.recipe.Streams = append(s.recipe.Streams, stream)
		s.facts = append(s.facts, facts)
		p.branches = append(p.branches, planned)
	}
	l := newRecipeLowering(s)
	l.attach = attach
	if err := l.plan(); err != nil {
		return err
	}
	p.state, p.lowering = s, l
	return nil
}

// resolveAnchor finds where a branch reads the running task: a live tap, a
// node of its graph, or a tap an earlier branch of the group declares.
func (p *attachPlan) resolveAnchor(recipe recipeir.RuntimeBranch, graphSpec pipeline.Spec, live []snapshot.Tap) (attachAnchor, error) {
	source := recipe.Source
	policy := source.Policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	route := anchorRoute{policy: policy, label: source.Label}
	if name := source.Tap.Name; name != "" {
		for _, tap := range live {
			if tap.Name != name {
				continue
			}
			if err := validateTapDomain("attach runtime branch", firstNonEmpty(recipe.Name, "branch"), tapRef{name: name, domain: source.Tap.Domain}, tap.Domain); err != nil {
				return attachAnchor{}, err
			}
			return attachAnchor{key: "tap\x00" + name, ref: tap.Node, route: route, tap: name, shape: runtimeBranchAnchorShape(tap)}, nil
		}
		if declared, ok := p.declared[name]; ok {
			return attachAnchor{key: "tap\x00" + name, pending: declared.node, tap: name}, nil
		}
		taps := append([]snapshot.Tap(nil), live...)
		for name, declared := range p.declared {
			taps = append(taps, snapshot.Tap{Name: name, Domain: declared.domain})
		}
		return attachAnchor{}, runtimeBranchTapMissingError(name, taps)
	}
	if !specHasNode(graphSpec, source.From) {
		return attachAnchor{}, runtimeBranchAnchorMissingError(source.From)
	}
	anchor := attachAnchor{
		key:   "node\x00" + source.From + "\x00" + string(policy) + "\x00" + source.Label,
		ref:   pipeline.NodeRef(source.From),
		route: route,
	}
	if source.StreamPresent {
		tap := discoveredStreamAnchorTap(source)
		anchor.shape = tap.Shape
		anchor.stream, anchor.present = source.Stream, true
	}
	return anchor, nil
}

// node is the planned node an operation of the group lowers to.
func (p *attachPlan) node(operation recipeir.Operation) (int, bool) {
	ref, ok := p.lowering.refs[operation.Node]
	if !ok || ref == "" {
		return 0, false
	}
	return p.nodeIndex(ref)
}

func (p *attachPlan) nodeIndex(ref pipeline.NodeRef) (int, bool) {
	for i := range p.lowering.g.nodes {
		if p.lowering.g.nodes[i].name == ref.String() {
			return i, true
		}
	}
	return 0, false
}

// checkBranch checks one branch against its anchor's live shape — what only
// the running task knows: the anchor's domain, codec, and media, and the
// adapters its work needs there — and records the taps it declares.
func (p *attachPlan) checkBranch(index int) error {
	t := p.task
	branch := &p.branches[index]
	stream := p.state.recipe.Streams[index]
	name := branch.name
	current, live := branch.anchor.shape, branch.anchor.stream
	if branch.anchor.ref == "" {
		// A tap an earlier branch declares: its shape is what that branch
		// carries there.
		declared := p.declared[branch.anchor.tap]
		if !declared.ok {
			return runtimeBranchTapMissingError(branch.anchor.tap, t.tapsLocked())
		}
		if err := validateTapDomain("attach runtime branch", name, tapRef{name: branch.anchor.tap, domain: branch.source.Tap.Domain}, declared.shape.Domain); err != nil {
			return err
		}
		current, live = declared.shape, declared.stream
	} else if !branch.anchor.present {
		live = streamFromRuntimeBranchShape(name, current)
	}
	operations := stream.Operations
	recipeStream := stream
	recipeStream.Selector.Type = firstNonEmptyMedia(live.Type, live.Codec.Type, stream.Selector.Type, codecMedia(live.Codec.ID))
	recipeStream.Selector.Codec = firstNonEmptyCodec(live.Codec.ID, stream.Selector.Codec)
	if err := validateAttachBranchShapeContract(recipeStream, operations, branch.destinations, current); err != nil {
		return err
	}
	if err := validateSyncPolicyForRecipeIROperations("attach runtime branch", name, live, operations); err != nil {
		return err
	}
	if err := validatePlayoutPolicyForRecipeIROperations("attach runtime branch", name, live, operations); err != nil {
		return err
	}
	realtime := t.runtime != nil && t.runtime.realtime
	needsRuntime := func(work string, fix string) error {
		if t.runtime != nil {
			return nil
		}
		return runtimeBranchInvalidError("runtime branch "+work+" requires the bundled runtime", "build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching "+fix)
	}
	encoded, copied := false, false
	for i, operation := range operations {
		switch operation.Kind {
		case plan.OpDecode:
			if err := needsRuntime("decoding", "decode branches"); err != nil {
				return err
			}
			if current.Domain != shape.DomainPacket {
				return runtimeBranchDecodeDomainError(name, current)
			}
			if live.Codec.ID == "" {
				return runtimeBranchDecodeCodecMissingError(name, current)
			}
			planned := recipeStream
			planned.Selector = streamSelectFromStream(live)
			if _, err := t.runtime.codecs.DecoderFactory(live.Codec.ID); err != nil {
				return recipeDecodeAdapterError("attach runtime branch", planned, live.Codec.ID, t.runtime.codecs, err)
			}
			if err := validateDecodeAdapterDescriptors("attach runtime branch", planned, t.runtime.codecs, decodeAdapterRequestFromStream(live, planned)); err != nil {
				return err
			}
			current.Domain = shape.DomainFrame
		case plan.OpShape:
			if mediaShapeEmpty(operation.Shape) {
				break
			}
			if err := validateShapeAnnotationDomain("attach runtime branch", name, i, current, operation.Shape); err != nil {
				return err
			}
			current = shape.Merge(current, operation.Shape)
		case plan.OpTransform:
			if recipeIRTransformEmpty(operation.Transform) {
				break
			}
			if err := needsRuntime("transforms", "resize/resample branches"); err != nil {
				return err
			}
			if current.Domain != shape.DomainFrame {
				return runtimeBranchInvalidError(
					"runtime branch transforms require a frame tap",
					"attach transform branches from task.Tap(name) where name is declared after Decode, Resize, Resample, or a frame stage",
				)
			}
			if err := validateTransformSpec("attach runtime branch", name, operation.Transform); err != nil {
				return err
			}
			node, _ := p.node(operation)
			transform := p.lowering.g.nodes[node].transform
			planned := recipeStream
			planned.Selector.Type = firstNonEmptyMedia(live.Type, live.Codec.Type, planned.Selector.Type)
			if _, err := t.runtime.filters.Factory(transform.factory); err != nil {
				return recipeTransformAdapterError("attach runtime branch", planned, transform.factory, err)
			}
			if desc, err := t.runtime.filters.Descriptor(transform.factory); err == nil {
				if err := validateRecipeIRTransformAdapterDescriptor("attach runtime branch", planned, operation.Transform, transform.factory, desc); err != nil {
					return err
				}
			}
			want, kind := av.MediaVideo, "resize"
			if operation.Transform.Kind == recipeir.TransformResample {
				want, kind = av.MediaAudio, "resample"
			}
			if live.Type != want && live.Codec.Type != want {
				return runtimeBranchTransformMediaError(name, kind, want, runtimeBranchStreamMedia(live))
			}
			out, err := applyMediaTransformToStream(live, transform)
			if err != nil {
				return runtimeBranchTransformError(transform.name, err)
			}
			live = out
			current = mediaShapeFromRuntimeBranchStream(live, current)
		case plan.OpTap:
			if operation.Tap.Name == "" {
				break
			}
			terminal := recipeIROperationTapIsTerminalPacket(operation)
			if !terminal {
				if err := validateTapDomain("attach runtime branch", name, tapRef{name: operation.Tap.Name, domain: operation.Tap.Domain}, current.Domain); err != nil {
					return err
				}
			}
			if declared := p.declared[operation.Tap.Name]; declared.node == operation.Node {
				declared.shape, declared.stream, declared.ok = current, live, true
				p.declared[operation.Tap.Name] = declared
			}
			if terminal && !encoded && !copied {
				// A packet tap after work the branch does not do names no
				// point of it.
				break
			}
			p.taps = append(p.taps, runtimeBranchTap(operation.Tap.Name, p.lowering.parentRef(index, operation.Node), current, operation.Tap.After))
		case plan.OpEncode:
			if err := needsRuntime("encoding", "encode branches"); err != nil {
				return err
			}
			if current.Domain != shape.DomainFrame {
				return runtimeBranchEncodeDomainError(name, current)
			}
			if err := validateRecipeEncode(operation.Encode, "attach runtime branch", name); err != nil {
				return err
			}
			planned := recipeStream
			planned.Selector.Type = firstNonEmptyMedia(live.Type, live.Codec.Type, planned.Selector.Type)
			if _, err := t.runtime.codecs.EncoderFactory(operation.Encode.ID); err != nil {
				return recipeEncodeAdapterCodecError("attach runtime branch", planned, operation.Encode.ID, t.runtime.codecs, err)
			}
			node, _ := p.node(operation)
			_, out, err := prepareEncodeConfig(live, p.lowering.g.nodes[node].request, realtime)
			if err != nil {
				return err
			}
			if err := validateEncodeAdapterDescriptors("attach runtime branch", planned, t.runtime.codecs, encodeAdapterRequestFromPreparedStream(operation.Encode, out)); err != nil {
				return err
			}
			live = out
			current = streamPacketShapeFromRuntimeBranchStream(live, current)
			encoded = true
		case plan.OpCopy:
			if current.Domain != shape.DomainPacket {
				return runtimeBranchCopyDomainError(name, current)
			}
			copied = true
		}
	}
	if attachDestinationsHaveMux(branch.destinations) {
		if !encoded && !copied {
			return runtimeBranchEncodeMissingError(name)
		}
		if t.runtime == nil {
			return runtimeBranchInvalidError(
				"runtime branch mux destinations require the bundled runtime",
				"build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching file or URI branches",
			)
		}
		if live.Codec.ID == "" {
			return runtimeBranchMuxCodecMissingError(name, current)
		}
	}
	branch.stream = live
	if branch.anchor.ref != "" {
		if _, ok := p.anchors[branch.anchor.ref]; !ok {
			p.anchors[branch.anchor.ref] = streamOrAnchor(branch.anchor, name)
		}
	}
	return nil
}

// streamOrAnchor is the live stream an anchor carries: the stream a source
// announced, else the one its shape describes.
func streamOrAnchor(anchor attachAnchor, name string) av.Stream {
	if anchor.present {
		return anchor.stream
	}
	return streamFromRuntimeBranchShape(name, anchor.shape)
}

// checkMuxes probes the format of every mux destination and checks the
// streams its branches deliver fit it, before anything opens.
func (p *attachPlan) checkMuxes(ctx context.Context) error {
	s := p.state
	for dest := range s.outputAttachments {
		destination := s.outputAttachments[dest]
		if destination.sink != nil {
			continue
		}
		var streams []av.Stream
		var branches []string
		for i := range p.branches {
			if slices.Contains(p.branches[i].dests, dest) {
				streams = append(streams, p.branches[i].stream)
				branches = append(branches, p.branches[i].name)
			}
		}
		formatID, err := runtimeMuxDestinationFormat(ctx, p.task.runtime, destination, dest)
		if err != nil {
			return err
		}
		name := s.outputDestinationNames[dest]
		if issue, ok := runtimeMuxCompatibilityIssue(name, formatID, branches, streams, p.task.runtime); ok {
			return muxCompatibilityBuildError("attach runtime branch", issue)
		}
		s.outputAttachments[dest].resolvedFormat = formatID
	}
	return nil
}

// open opens every planned node through the build's opener, in schedule
// order, against the live streams its inputs carry.
func (p *attachPlan) open(ctx context.Context) error {
	t := p.task
	service := &builder{runtime: t.runtime}
	realtime := t.runtime != nil && t.runtime.realtime
	streams := map[pipeline.NodeRef]av.Stream{}
	maps := func(ref pipeline.NodeRef) (av.Stream, shape.Spec) {
		if stream, ok := streams[ref]; ok {
			return stream, p.shapes[ref]
		}
		return p.anchors[ref], p.anchorShape(ref)
	}
	for _, node := range p.lowering.g.nodes {
		ref := pipeline.NodeRef(node.name)
		in := make([]av.Stream, 0, len(node.inputs))
		var inShape shape.Spec
		for i, input := range node.inputs {
			stream, spec := maps(input)
			in = append(in, stream)
			if i == 0 {
				inShape = spec
			}
		}
		opened, err := openComposeNode(ctx, service, node, in, p.state.outputAttachments, realtime, codec.DecodeBounds{})
		if err != nil {
			if node.kind == composeStep && node.transform.stage == nil {
				return runtimeBranchTransformError(node.transform.name, err)
			}
			return err
		}
		if node.kind == composeStep && node.transform.stage != nil {
			// Attached gates pace on the running task's timeline and report
			// QoS to its Watch surface.
			bindGateDeps(node.transform.stage, t.playoutClock(), t.qosReportFunc())
		}
		p.opened = append(p.opened, opened)
		streams[ref] = opened.stream
		switch node.kind {
		case composeDecode:
			inShape.Domain = shape.DomainFrame
		case composeStep:
			if node.transform.stage == nil {
				inShape = mediaShapeFromRuntimeBranchStream(opened.stream, inShape)
			}
		case composeEncode:
			inShape = streamPacketShapeFromRuntimeBranchStream(opened.stream, inShape)
		}
		p.shapes[ref] = inShape
	}
	return nil
}

// anchorShape is the live shape at a group's anchor node.
func (p *attachPlan) anchorShape(ref pipeline.NodeRef) shape.Spec {
	for i := range p.branches {
		if p.branches[i].anchor.ref == ref {
			return p.branches[i].anchor.shape
		}
	}
	return shape.Spec{}
}

// reserve claims the group's taps and node names against the running task,
// and sizes each node's queue.
func (p *attachPlan) reserve() error {
	t := p.task
	existing := map[string]bool{}
	for _, tap := range t.tapsLocked() {
		existing[tap.Name] = true
	}
	for _, tap := range p.taps {
		if existing[tap.Name] {
			return runtimeBranchTapDuplicateError(tap.Name)
		}
		existing[tap.Name] = true
	}
	graphSpec := t.graph.Spec()
	seen := map[string]bool{}
	for _, node := range p.lowering.g.nodes {
		if seen[node.name] || specHasNode(graphSpec, node.name) {
			return runtimeBranchNodeDuplicateError(node.name)
		}
		seen[node.name] = true
	}
	for i := range p.branches {
		p.branches[i].buffer = t.attachBufferPolicy(p.state.recipe.Streams[i].Buffer)
	}
	p.buffers = make([]pipeline.BufferPolicy, len(p.lowering.g.nodes))
	for n, node := range p.lowering.g.nodes {
		fed := false
		for i := range p.branches {
			if !p.feeds(i, node) {
				continue
			}
			if !fed {
				p.buffers[n], fed = p.branches[i].buffer, true
				continue
			}
			p.buffers[n] = mergeBufferCopyBounds(p.buffers[n], p.branches[i].buffer)
		}
	}
	return nil
}

// feeds reports whether the branch at index reads node.
func (p *attachPlan) feeds(index int, node composeNode) bool {
	switch node.kind {
	case composeSink, composeMux:
		return slices.Contains(p.branches[index].dests, node.output)
	}
	for _, operation := range p.state.recipe.Streams[index].Operations {
		if ref, ok := p.lowering.refs[operation.Node]; ok && ref.String() == node.name {
			return true
		}
	}
	return false
}

// attachBufferPolicy is the queue an attached branch's nodes run behind: the
// branch's own, or the runtime's on a buffered task, either carrying the
// copy bounds the runtime sized for the task's shapes.
func (t *task) attachBufferPolicy(buffer flow.BranchBuffer) pipeline.BufferPolicy {
	policy := buffer.PipelinePolicy()
	if _, ok := t.graph.(pipeline.NodeInjector); !ok || buffer.CopyMode == flow.CopyNever {
		return policy
	}
	var runtimeBuffer pipeline.BufferPolicy
	if t.runtime != nil {
		runtimeBuffer = t.runtime.buffer
	}
	if policy.IsDirect() {
		return realtimeRecipeBufferPolicy(runtimeBuffer)
	}
	// An explicit branch buffer still needs the copy bounds the runtime
	// sized, or a buffered edge would refuse mutable payloads at runtime;
	// a bound the caller set stays.
	if policy.CopyPacketBytes == 0 {
		policy.CopyPacketBytes = runtimeBuffer.CopyPacketBytes
	}
	if policy.CopyFrameBytes == 0 {
		policy.CopyFrameBytes = runtimeBuffer.CopyFrameBytes
	}
	return policy
}

// close closes what the plan opened: an attach that failed aborts its
// muxers' writes.
func (p *attachPlan) close() {
	for _, opened := range p.opened {
		if opened.owned && opened.stage != nil {
			markPipelineStageFailed(opened.stage)
			_ = opened.stage.Close()
		}
	}
}

// apply installs the planned nodes in schedule order, each connected to its
// inputs as it is added — a live anchor by the anchor's own route — and
// returns the attachment. On failure it removes what it added and closes
// what the plan opened.
func (p *attachPlan) apply() (*runtimeAttachment, error) {
	t := p.task
	var nodes []pipeline.NodeRef
	var routes []pipeline.Route
	fail := func(err error) (*runtimeAttachment, error) {
		p.close()
		t.rollbackRuntimeBranch(nodes)
		return nil, err
	}
	for n, node := range p.lowering.g.nodes {
		opened := p.opened[n]
		var err error
		var ref pipeline.NodeRef
		if opened.sink != nil {
			ref, err = t.graph.AddSink(opened.sink, p.buffers[n])
			if err != nil {
				return fail(runtimeBranchGraphError("add sink", node.name, err))
			}
		} else {
			ref, err = t.graph.AddStage(opened.stage, p.buffers[n])
			if err != nil {
				return fail(runtimeBranchGraphError("add stage", node.name, err))
			}
		}
		nodes = append(nodes, ref)
		for _, input := range node.inputs {
			if !slices.Contains(nodes, input) {
				continue
			}
			next := pipeline.Route{From: input.String(), To: []string{node.name}, Policy: pipeline.RouteAll}
			if err := t.graph.Connect(next); err != nil {
				return fail(runtimeBranchGraphError(p.connectOperation(node, input), node.name, err))
			}
			routes = append(routes, next)
		}
	}
	// The branch is whole before anything reaches it: the edges from the
	// running graph connect last, one route per anchor and route, so the
	// first message the anchor sends — a switch's keyframe — finds every
	// node downstream of it in place.
	for _, ingress := range p.ingress(nodes) {
		if err := t.graph.Connect(ingress); err != nil {
			return fail(runtimeBranchGraphError("connect branch", ingress.To[0], err))
		}
		routes = append(routes, ingress)
	}
	attachment := &runtimeAttachment{
		id:     nextRuntimeAttachmentID(p.name),
		name:   p.name,
		owner:  t,
		nodes:  nodes,
		routes: routes,
		taps:   p.taps,
	}
	for i := range p.branches {
		branch := p.branches[i]
		attachment.anchorTaps = append(attachment.anchorTaps, branch.anchor.tap)
		if branch.anchor.ref != "" {
			attachment.anchorNodes = append(attachment.anchorNodes, branch.anchor.ref.String())
		} else {
			attachment.anchorNodes = append(attachment.anchorNodes, p.lowering.refs[p.state.recipe.Streams[i].Root].String())
		}
		if branch.copyNever {
			attachment.copyNever = append(attachment.copyNever, branch.name)
		}
		for _, destination := range branch.destinations {
			attachment.destinations = append(attachment.destinations, workDestination{
				ID:        workDestinationID(destination.recipe.Name),
				Name:      destination.recipe.Name,
				Operation: attachDestinationOperationKind(destination),
				Component: attachDestinationComponent(destination),
				Format:    destination.recipe.Format,
				Branches:  []string{branch.name},
			})
		}
	}
	attachment.anchorTap = firstNonEmpty(attachment.anchorTaps...)
	attachment.anchorNode = firstNonEmpty(attachment.anchorNodes...)
	attachment.anchorTaps = uniqueStrings(attachment.anchorTaps)
	attachment.anchorNodes = uniqueStrings(attachment.anchorNodes)
	attachment.copyNever = uniqueStrings(attachment.copyNever)
	for _, opened := range p.opened {
		if opened.stage != nil {
			attachment.stages = append(attachment.stages, opened.stage)
		}
	}
	return attachment, nil
}

// ingress groups the edges from the running graph into the patch: one route
// per anchor node, policy, and label, into every node it feeds.
func (p *attachPlan) ingress(planned []pipeline.NodeRef) []pipeline.Route {
	var out []pipeline.Route
	for _, node := range p.lowering.g.nodes {
		for i, input := range node.inputs {
			if slices.Contains(planned, input) {
				continue
			}
			route := node.route(i)
			at := slices.IndexFunc(out, func(r pipeline.Route) bool {
				return r.From == input.String() && r.Policy == route.policy && r.Label == route.label
			})
			if at < 0 {
				out = append(out, pipeline.Route{From: input.String(), Policy: route.policy, Label: route.label})
				at = len(out) - 1
			}
			out[at].To = append(out[at].To, node.name)
		}
	}
	return out
}

// connectOperation names a failed connect inside the patch: stage to stage,
// or into a destination.
func (p *attachPlan) connectOperation(node composeNode, input pipeline.NodeRef) string {
	if node.kind == composeSink || node.kind == composeMux {
		return "connect branch target"
	}
	return "connect branch stage"
}

// spec is the graph patch the plan describes: the nodes it adds and the
// edges into them, as the running graph reports them once applied.
func (p *attachPlan) spec() (pipeline.Spec, error) {
	return p.lowering.g.emit(pipeline.Spec{Name: p.name, Realtime: p.task.graph.Spec().Realtime})
}

func attachDestinationOperationKind(destination attachDestination) plan.OperationKind {
	if destination.recipe.Kind == recipeir.DestinationKindByteStream {
		return plan.OpMux
	}
	return plan.OpSink
}

func attachDestinationComponent(destination attachDestination) string {
	if destination.recipe.Kind == recipeir.DestinationKindSink {
		return "sink"
	}
	if formatID := destination.recipe.Format; formatID != "" {
		return string(formatID)
	}
	return "mux"
}

func attachDestinationsHaveMux(destinations []attachDestination) bool {
	for i := range destinations {
		if destinations[i].recipe.Kind == recipeir.DestinationKindByteStream {
			return true
		}
	}
	return false
}

func destinationSnapshotsFromWork(destinations []workDestination, state lifecycle.DestinationState) []snapshot.Destination {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]snapshot.Destination, 0, len(destinations))
	for i := range destinations {
		out = append(out, snapshot.Destination{
			Name:      destinations[i].Name,
			Operation: destinations[i].Operation,
			Component: destinations[i].Component,
			Format:    destinations[i].Format,
			Branches:  append([]string(nil), destinations[i].Branches...),
			State:     state,
			Open:      state == lifecycle.DestinationOpen,
		})
	}
	return out
}
