package goav

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// The lowering turns a recipe's DAG into one composeGraph: the schedule
// Describe emits and Build builds, node for node. Every DAG node lowers once,
// however many branches read it: a node one branch reads takes that branch's
// name, a node several read takes its stream's. The schedule is
//
//   - the sources of the inputs the recipe's streams read, in input order;
//   - per stream root, in branch order: an input's select (shared by every
//     root of one selector), its decode, and the work all its branches
//     share; or a join with its arms, depth first;
//   - each branch's own work, in branch order (work only some branches
//     share runs where the first of them reaches it);
//   - the destinations, in order.
//
// A node's inputs that are not scheduled yet (a join arm fed by a stream's
// node) are scheduled first.

// composeNodeKind is what a planned node lowers to.
type composeNodeKind uint8

const (
	composeSource composeNodeKind = iota + 1
	composeSelect
	composeDecode
	composeStep
	composeEncode
	composeSink
	composeMux
	// composeRemuxSink and composeRemuxMux are whole-input packet copies:
	// every source feeds them with all its streams.
	composeRemuxSink
	composeRemuxMux
	// Join trees: the bind of an arm whose streams are known once it opens,
	// its event-keeping decode, its run-time conversion, a planned arm
	// conversion, and the convergence stage.
	composeArmBind
	composeArmDecode
	composeArmConform
	composeTransform
	composeJoin
)

// composeNode is one planned node: its name and describe detail, the nodes it
// consumes in connect order, and what the build makes it from.
type composeNode struct {
	kind   composeNodeKind
	name   string
	detail string
	inputs []pipeline.NodeRef
	// input is a source's or select's recipe input; selector what a select or
	// decode picks.
	input    int
	selector av.StreamSelector
	// decode is a decode's codec options; dropEvents keeps a lone decode to
	// a sink from forwarding stream events.
	decode     codec.Spec
	dropEvents bool
	// transform is a step's work; request an encoder's.
	transform mediaTransform
	request   encodeRequest
	// owner is the branch that alone reads the node, or -1.
	owner int
	// output indexes a destination.
	output int
	// join is the join a join-tree node belongs to, arm the arm it serves,
	// and stream the planned stream a transform is built against.
	join     *joinPlan
	arm      int
	stream   av.Stream
	resolved bool
	target   pipeline.NodeRef
}

// composeGraph is the one schedule a recipe is described and built from.
type composeGraph struct {
	name  string
	nodes []composeNode
}

func (g *composeGraph) add(node composeNode) pipeline.NodeRef {
	g.nodes = append(g.nodes, node)
	return pipeline.NodeRef(node.name)
}

// emit renders the schedule as a spec: the nodes in schedule order, then the
// edges grouped by producer in schedule order, each producer's edges in
// consumer order — the order a built graph reports them in.
func (g composeGraph) emit(spec pipeline.Spec) (pipeline.Spec, error) {
	planned := make(map[string]plannedNode, len(g.nodes))
	for _, node := range g.nodes {
		kind := pipeline.NodeStage
		switch node.kind {
		case composeSink, composeRemuxSink:
			kind = pipeline.NodeSink
		case composeSource:
			kind = pipeline.NodeSource
		}
		if err := addPlannedNode(planned, &spec, node.name, kind, pipeline.NodeRef(node.name), node.detail); err != nil {
			return pipeline.Spec{}, err
		}
	}
	consumers := make(map[pipeline.NodeRef][]pipeline.NodeRef, len(g.nodes))
	for _, node := range g.nodes {
		for _, input := range node.inputs {
			consumers[input] = append(consumers[input], pipeline.NodeRef(node.name))
		}
	}
	for _, node := range g.nodes {
		from := pipeline.NodeRef(node.name)
		for _, to := range consumers[from] {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: from, To: to, Policy: pipeline.RouteAll})
		}
	}
	return spec, nil
}

// recipeLowering plans one recipe's composeGraph.
type recipeLowering struct {
	s *recipeCompileState
	g composeGraph
	// refs are the planned nodes of DAG nodes; sources of inputs.
	refs    map[recipeir.NodeID]pipeline.NodeRef
	sources map[int]pipeline.NodeRef
	// groups are the selects (and their decodes) of selector groups.
	groups       map[string]pipeline.NodeRef
	groupDecodes map[string]pipeline.NodeRef
	names        map[string]bool
	// counts number transforms per scope and per branch for their names.
	counts map[string]int
	// owners are the branches reading each node.
	owners map[recipeir.NodeID][]int
	// leaves are the planned last node of each branch.
	leaves []pipeline.NodeRef
	// nodeNames are the planned names of DAG nodes, read by the work plan.
	nodeNames map[recipeir.NodeID]string
	// sourceNames are the planned source node names, by input.
	sourceNames []string
}

func (s *recipeCompileState) lower() error {
	if s.runtime == nil && s.options.preflight {
		return nil
	}
	l := &recipeLowering{
		s:            s,
		refs:         map[recipeir.NodeID]pipeline.NodeRef{},
		sources:      map[int]pipeline.NodeRef{},
		groups:       map[string]pipeline.NodeRef{},
		groupDecodes: map[string]pipeline.NodeRef{},
		names:        map[string]bool{},
		counts:       map[string]int{},
		owners:       map[recipeir.NodeID][]int{},
		nodeNames:    map[recipeir.NodeID]string{},
		sourceNames:  graphSourceNodeNames(s.inputAttachments),
	}
	if err := l.plan(); err != nil {
		return err
	}
	spec := pipeline.Spec{Name: s.graphName(), Realtime: runtimeRealtime(s.runtime)}
	spec, err := l.g.emit(spec)
	if err != nil {
		return err
	}
	l.g.name = spec.Name
	work := buildRecipeWorkPlan(s, l, spec)
	s.graphPlan = newGraphPlan(s.runtime, spec, work, &recipeLowerer{state: s, graph: l.g, lowering: l})
	return nil
}

// graphName names the task's graph: a recipe that is one join is named after
// it.
func (s *recipeCompileState) graphName() string {
	if root, ok := s.soleJoinRoot(); ok {
		return "goav-" + s.joinNames[root]
	}
	return "goav"
}

func (l *recipeLowering) plan() error {
	s := l.s
	for i := range s.recipe.Streams {
		for _, operation := range s.recipe.Streams[i].Operations {
			l.owners[operation.Node] = append(l.owners[operation.Node], i)
		}
	}
	if len(s.remux) != 0 {
		return l.planRemux()
	}
	// The sources of the inputs streams read come first.
	streamInputs := map[int]bool{}
	for i := range s.recipe.Streams {
		if s.recipe.Streams[i].Input >= 0 {
			streamInputs[s.recipe.Streams[i].Input] = true
		}
	}
	for input := range s.inputAttachments {
		if streamInputs[input] || s.ruleInput(input) {
			l.source(input)
		}
	}
	// Then each root: its select or join, and the work all its branches
	// share.
	seen := map[recipeir.NodeID]bool{}
	for i := range s.recipe.Streams {
		root := s.recipe.Streams[i].Root
		if seen[root] {
			continue
		}
		seen[root] = true
		if err := l.root(root, i); err != nil {
			return err
		}
	}
	// Then each branch's own work.
	l.leaves = make([]pipeline.NodeRef, len(s.recipe.Streams))
	for i := range s.recipe.Streams {
		leaf, err := l.branch(i)
		if err != nil {
			return err
		}
		l.leaves[i] = leaf
	}
	// Then the destinations.
	for dest := range s.outputAttachments {
		l.destination(dest)
	}
	return nil
}

func (s *recipeCompileState) ruleInput(input int) bool {
	return len(s.streamRules) != 0 && input == 0
}

func (l *recipeLowering) unique(name string) string {
	out := name
	for n := 2; l.names[out]; n++ {
		out = name + "-" + strconv.Itoa(n)
	}
	l.names[out] = true
	return out
}

func (l *recipeLowering) add(node composeNode) pipeline.NodeRef {
	l.names[node.name] = true
	return l.g.add(node)
}

// source plans one input's source node, once.
func (l *recipeLowering) source(input int) pipeline.NodeRef {
	if ref, ok := l.sources[input]; ok {
		return ref
	}
	name := l.sourceNames[input]
	ref := l.add(composeNode{
		kind:   composeSource,
		name:   name,
		detail: l.s.inputAttachments[input].graphSourceNodeDetail(),
		input:  input,
	})
	l.sources[input] = ref
	return ref
}

// scope is the name a root's shared nodes are named in: an input stream's
// kind, qualified by its input when the recipe has several, or a join's name.
func (l *recipeLowering) scope(stream recipeir.Stream) string {
	if stream.Input < 0 {
		return l.s.joinNames[stream.Root]
	}
	selector := recipeIRStreamSelector(stream)
	return branchComposeInputNodeName(firstNonEmpty(string(selector.Type), string(selector.ID), selector.Name, "stream"), stream.Selector.Input)
}

func selectGroupKey(stream recipeir.Stream) string {
	return branchComposeSelectorGroupKey(recipeIRStreamSelector(stream), strconv.Itoa(stream.Input))
}

// root plans a root: an input stream's select, the decode right after it,
// and the work every branch of the root shares; or a join.
func (l *recipeLowering) root(root recipeir.NodeID, first int) error {
	s := l.s
	stream := s.recipe.Streams[first]
	if stream.Input < 0 {
		ref, err := l.join(root)
		if err != nil {
			return err
		}
		l.refs[root] = ref
		return l.trunk(root, first)
	}
	key := selectGroupKey(stream)
	selectRef, ok := l.groups[key]
	if !ok {
		selector := recipeIRStreamSelector(stream)
		selectRef = l.add(composeNode{
			kind:     composeSelect,
			name:     l.unique(branchComposeInputNodeName(selectNodeName(selector), stream.Selector.Input)),
			detail:   selectNodeDetail(selector),
			inputs:   []pipeline.NodeRef{l.source(stream.Input)},
			input:    stream.Input,
			selector: selector,
		})
		l.groups[key] = selectRef
		// The decode right after a select serves every branch of the selector
		// that decodes there.
		decode, ok, err := l.groupDecode(key)
		if err != nil {
			return err
		}
		if ok {
			decodeRef := l.add(composeNode{
				kind:       composeDecode,
				name:       l.unique(branchComposeInputNodeName(decodeNodeName(selector), stream.Selector.Input)),
				detail:     decodeRequestDetail(decodeRequest{selector: selector}),
				inputs:     []pipeline.NodeRef{selectRef},
				selector:   selector,
				decode:     decode,
				dropEvents: l.dropDecodeEvents(),
				owner:      -1,
			})
			l.groupDecodes[key] = decodeRef
		}
	}
	l.refs[root] = selectRef
	return l.trunk(root, first)
}

// groupDecode reports the decode config of a selector group whose branches
// decode right after the select: they share one decoder, so two branches
// that declare different configs are refused.
func (l *recipeLowering) groupDecode(key string) (codec.Spec, bool, error) {
	s := l.s
	found := false
	var config codec.Spec
	owner := ""
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		if stream.Input < 0 || selectGroupKey(stream) != key {
			continue
		}
		operation, ok := firstWorkOperation(stream.Operations)
		if !ok || operation.Kind != plan.OpDecode {
			continue
		}
		if found && codecSpecHasDecodeIntent(operation.Decode) && codecSpecHasDecodeIntent(config) && !codecSpecEqual(config, operation.Decode) {
			return codec.Spec{}, false, branchComposeDecodeConfigConflictError(owner, stream.Name)
		}
		if !found || codecSpecHasDecodeIntent(operation.Decode) {
			config, owner = cloneCodecSpec(operation.Decode), stream.Name
		}
		found = true
	}
	return config, found, nil
}

// dropDecodeEvents reports a recipe that is one lone branch decoding to
// sinks without an encoder: its decode forwards no stream events.
func (l *recipeLowering) dropDecodeEvents() bool {
	s := l.s
	if len(s.recipe.Streams) != 1 || !s.facts[0].lone {
		return false
	}
	return recipeIRStreamEncodeSpec(s.recipe.Streams[0]).ID == ""
}

// firstWorkOperation is a branch's first operation that is not an
// annotation or a tap.
func firstWorkOperation(operations []recipeir.Operation) (recipeir.Operation, bool) {
	for _, operation := range operations {
		if recipeIROperationIsAnnotation(operation) || operation.Kind == plan.OpTap || operation.Kind == plan.OpCopy {
			continue
		}
		return operation, true
	}
	return recipeir.Operation{}, false
}

// trunk plans the work every branch of a root shares, in order.
func (l *recipeLowering) trunk(root recipeir.NodeID, first int) error {
	s := l.s
	var branches []int
	for i := range s.recipe.Streams {
		if s.recipe.Streams[i].Root == root {
			branches = append(branches, i)
		}
	}
	if len(branches) < 2 {
		return nil
	}
	for _, operation := range s.recipe.Streams[first].Operations {
		if len(l.owners[operation.Node]) < len(branches) {
			break
		}
		if _, err := l.operation(first, operation); err != nil {
			return err
		}
	}
	return nil
}

// branch plans a branch's own work and returns its last planned node.
func (l *recipeLowering) branch(index int) (pipeline.NodeRef, error) {
	stream := l.s.recipe.Streams[index]
	upstream := l.refs[stream.Root]
	for _, operation := range stream.Operations {
		ref, err := l.operation(index, operation)
		if err != nil {
			return "", err
		}
		if ref != "" {
			upstream = ref
		}
	}
	return upstream, nil
}

// parentRef is the planned node a branch's operation reads: the nearest
// planned node before it on the branch.
func (l *recipeLowering) parentRef(index int, node recipeir.NodeID) pipeline.NodeRef {
	stream := l.s.recipe.Streams[index]
	ref := l.refs[stream.Root]
	for _, operation := range stream.Operations {
		if operation.Node == node {
			return ref
		}
		if planned, ok := l.refs[operation.Node]; ok && planned != "" {
			ref = planned
		}
	}
	return ref
}

// operation plans one operation of a branch, once: it returns the planned
// node, or "" for an operation that plans none.
func (l *recipeLowering) operation(index int, operation recipeir.Operation) (pipeline.NodeRef, error) {
	if ref, ok := l.refs[operation.Node]; ok {
		return ref, nil
	}
	s := l.s
	stream := s.recipe.Streams[index]
	owners := l.owners[operation.Node]
	owner := -1
	name := l.scope(stream)
	if len(owners) <= 1 {
		owner = index
		name = stream.Name
	}
	input := l.parentRef(index, operation.Node)
	var node composeNode
	switch operation.Kind {
	case plan.OpDecode:
		if ref, ok := l.groupDecodes[selectGroupKey(stream)]; ok && stream.Input >= 0 && input == l.refs[stream.Root] {
			l.refs[operation.Node] = ref
			l.nodeNames[operation.Node] = string(ref)
			return ref, nil
		}
		selector := recipeIRStreamSelector(stream)
		node = composeNode{
			kind:     composeDecode,
			name:     l.unique("decode-" + l.scope(stream)),
			detail:   decodeRequestDetail(decodeRequest{selector: selector}),
			selector: selector,
			decode:   cloneCodecSpec(operation.Decode),
		}
	case plan.OpTransform, plan.OpStage:
		transform, err := l.transform(name, operation)
		if err != nil {
			return "", err
		}
		node = composeNode{kind: composeStep, name: transform.name, detail: mediaTransformDetail(transform), transform: transform}
	case plan.OpEncode:
		request := s.encodeRequest(index, operation, owner < 0)
		if owner < 0 {
			request.name = name
		}
		node = composeNode{kind: composeEncode, name: l.unique(encodeNodeName(request)), detail: encodeNodeDetail(request), request: request}
	default:
		// Copies, taps, annotations, names, and queues plan no node.
		return "", nil
	}
	node.inputs = []pipeline.NodeRef{input}
	node.owner = owner
	ref := l.add(node)
	l.refs[operation.Node] = ref
	l.nodeNames[operation.Node] = node.name
	return ref, nil
}

// transform names and configures one step: a stage keeps its own name (a
// sync or playout gate is named for its stream and policy), a transform is
// named for its adapter and its stream, numbered from its second.
func (l *recipeLowering) transform(name string, operation recipeir.Operation) (mediaTransform, error) {
	if operation.Kind == plan.OpStage {
		if operation.Stage == nil {
			return mediaTransform{}, branchChainStepError(name, "branch stage operation has no stage")
		}
		switch gate := operation.Stage.(type) {
		case *syncGate:
			return mediaTransform{name: l.unique(syncStageName(name + "-" + gate.policy.Name())), stage: operation.Stage}, nil
		case *playoutGate:
			return mediaTransform{name: l.unique(playoutStageName(name + "-" + gate.policy.Name())), stage: operation.Stage}, nil
		}
		return mediaTransform{name: operation.Stage.Name(), stage: operation.Stage}, nil
	}
	l.counts[name]++
	suffix := ""
	if n := l.counts[name]; n > 1 {
		suffix = "-" + strconv.Itoa(n)
	}
	switch operation.Transform.Kind {
	case recipeir.TransformResize:
		resize := operation.Transform.Resize
		factory := firstNonEmpty(operation.Component, filter.FactoryResize)
		return mediaTransform{name: l.unique(factory + "-" + name + suffix), factory: factory, video: &resize}, nil
	case recipeir.TransformResample:
		resample := operation.Transform.Resample
		factory := firstNonEmpty(operation.Component, filter.FactoryResample)
		return mediaTransform{name: l.unique(factory + "-" + name + suffix), factory: factory, audio: &resample}, nil
	default:
		return mediaTransform{}, branchChainStepError(name, "branch transform operation is empty")
	}
}

// encodeRequest is a branch encoder's request. An encoder one branch owns
// encodes under the branch's name, unless the branch is the recipe's only
// one and names no fork; an encoder several branches share keeps the
// identity of the stream it encodes, as As names only a branch's own work.
func (s *recipeCompileState) encodeRequest(index int, operation recipeir.Operation, shared bool) encodeRequest {
	stream := s.recipe.Streams[index]
	config := encodeConfigFromSpec(operation.Encode)
	if !shared && (len(s.recipe.Streams) != 1 || stream.Named) {
		if config.Stream.ID == "" {
			config.Stream.ID = av.StreamID(stream.Name)
		}
		if config.Stream.Name == "" {
			config.Stream.Name = stream.Name
		}
	}
	return encodeRequest{name: stream.Name, selector: recipeIRStreamSelector(stream), config: config}
}

// destination plans one destination fed by every branch that routes to it,
// in branch order.
func (l *recipeLowering) destination(dest int) {
	s := l.s
	var inputs []pipeline.NodeRef
	var feeds []int
	for i := range s.recipe.Streams {
		for _, d := range s.facts[i].dests {
			if d == dest {
				inputs = append(inputs, l.leaves[i])
				feeds = append(feeds, i)
			}
		}
	}
	spec := s.outputAttachments[dest]
	owner := -1
	if len(feeds) == 1 {
		owner = feeds[0]
	}
	if spec.sink != nil {
		l.add(composeNode{kind: composeSink, name: destinationSinkNodeName(spec, s.outputDestinationNames[dest], dest), detail: describedNodeDetail(spec.sink), inputs: inputs, output: dest, owner: owner})
		return
	}
	output := destinationSpecOutput(spec)
	if output.Name == "" {
		output.Name = s.outputDestinationNames[dest]
	}
	l.add(composeNode{kind: composeMux, name: muxNodeName(output, dest), detail: outputNodeDetailWithFormat(output, destinationGraphFormat(spec)), inputs: inputs, output: dest, owner: owner})
}

// destinationSinkNodeName names a sink node: the sink's own name, else the
// destination's.
func destinationSinkNodeName(spec destinationSpec, name string, index int) string {
	if spec.sink != nil && spec.sink.Name() != "" {
		return spec.sink.Name()
	}
	if name != "" {
		return name
	}
	if index == 0 {
		return "sink"
	}
	return "sink-" + strconv.Itoa(index)
}

// planRemux plans whole-input copies: every input's source, then each
// destination fed by all of them.
func (l *recipeLowering) planRemux() error {
	s := l.s
	for _, remux := range s.remux {
		spec, ok := declaredSourceShape(s.inputAttachments[remux.input])
		if !ok || spec.Domain != shape.DomainEvent {
			continue
		}
		for _, dest := range remux.dests {
			if s.outputAttachments[dest].sink == nil {
				// Events have no packets to mux.
				return graphPlanInvalidError("event source destination must be a sink", nil)
			}
		}
	}
	sources := make([]pipeline.NodeRef, len(s.inputAttachments))
	for input := range s.inputAttachments {
		sources[input] = l.source(input)
	}
	for dest := range s.outputAttachments {
		spec := s.outputAttachments[dest]
		// A destination reads the inputs whose copies its Outputs route to it.
		var inputs []pipeline.NodeRef
		for input := range s.inputAttachments {
			for _, remux := range s.remux {
				if remux.input == input && slices.Contains(remux.dests, dest) {
					inputs = append(inputs, sources[input])
					break
				}
			}
		}
		if spec.sink != nil {
			l.add(composeNode{kind: composeRemuxSink, name: firstNonEmpty(spec.sink.Name(), s.outputDestinationNames[dest], "sink"), detail: describedNodeDetail(spec.sink), inputs: inputs, output: dest})
			continue
		}
		output := destinationSpecOutput(spec)
		if output.Name == "" {
			output.Name = s.outputDestinationNames[dest]
		}
		l.add(composeNode{kind: composeRemuxMux, name: muxNodeName(output, dest), detail: outputNodeDetailWithFormat(output, destinationGraphFormat(spec)), inputs: inputs, output: dest})
	}
	return nil
}

// --- the build ---

// recipeLowerer builds a recipe's planned graph.
type recipeLowerer struct {
	state    *recipeCompileState
	graph    composeGraph
	lowering *recipeLowering
}

func (r *recipeLowerer) withRuntime(rt *Runtime) *recipeLowerer {
	state := *r.state
	state.runtime = rt
	joins := make(map[recipeir.NodeID]*joinPlan, len(state.joins))
	for id, join := range state.joins {
		joins[id] = joinPlanWithRuntime(join, rt)
	}
	state.joins = joins
	graph := r.graph
	graph.nodes = append([]composeNode(nil), r.graph.nodes...)
	for i := range graph.nodes {
		if graph.nodes[i].join != nil {
			graph.nodes[i].join = joins[graph.nodes[i].join.node]
		}
	}
	return &recipeLowerer{state: &state, graph: graph, lowering: r.lowering}
}

// lower builds the plan into graph: each node in schedule order, connected
// to its inputs as it is added, so the built graph is the described one.
func (r *recipeLowerer) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	s := r.state
	rt := service.runtime
	realtime := rt.realtime
	streams := make(map[pipeline.NodeRef]av.Stream, len(r.graph.nodes))
	sourceStreams := map[int][]av.Stream{}
	qualifiers := map[int]string{}
	sourceInputs := map[pipeline.NodeRef]int{}
	var bounds []decodeBoundsProvider
	buffers, err := r.branchBuffers(gp.work)
	if err != nil {
		return err
	}
	bufferOf := func(node composeNode) pipeline.BufferPolicy {
		if node.owner >= 0 {
			if buffer, ok := buffers[node.owner]; ok && !buffer.IsDirect() {
				return buffer
			}
		}
		return rt.buffer
	}
	in := composeLowering{realtime: realtime, work: gp.work, admission: !realtime}
	for _, node := range r.graph.nodes {
		ref := pipeline.NodeRef(node.name)
		switch node.kind {
		case composeSource:
			sourceInputs[ref] = node.input
			input := s.inputAttachments[node.input]
			build, err := input.openGraphSourceBuild(ctx, service, node.name, len(s.inputAttachments) > 1)
			if err != nil {
				return err
			}
			if _, err := graph.AddSource(build.source, rt.buffer); err != nil {
				build.source.Close()
				return err
			}
			sourceStreams[node.input] = append([]av.Stream(nil), build.streams...)
			qualifiers[node.input] = build.qualifier
			realtime = realtime || build.realtime
			in.realtime = realtime
			if build.bounds != nil {
				bounds = append(bounds, build.bounds)
			}
			if len(build.streams) == 1 {
				streams[ref] = qualifyStream(build.streams[0], build.qualifier)
			}
		case composeSelect:
			selected, err := r.selectStream(node, sourceStreams[node.input], qualifiers[node.input])
			if err != nil {
				return err
			}
			stage := newStreamSelectStage(node.name, selected, node.selector, node.detail)
			if err := addComposeStage(graph, stage, rt.buffer, node.inputs); err != nil {
				return err
			}
			streams[ref] = selected
		case composeDecode:
			selected := streams[node.inputs[0]]
			decodeBounds := codec.DecodeBounds{}
			if len(bounds) != 0 {
				decodeBounds = providerDecodeBoundsForStream(selected, bounds)
			}
			stage, err := service.newDecodeStageNamed(ctx, node.name, decodeRequest{selector: node.selector, config: node.decode}, selected, realtime, node.dropEvents, decodeBounds)
			if err != nil {
				return err
			}
			if err := addComposeStage(graph, stage, bufferOf(node), node.inputs); err != nil {
				return err
			}
			streams[ref] = selected
		case composeStep:
			stage, out, err := service.newBranchComposeStepStageNamed(ctx, node.name, node.transform, streams[node.inputs[0]], realtime)
			if err != nil {
				return err
			}
			if err := addComposeStage(graph, stage, bufferOf(node), node.inputs); err != nil {
				return err
			}
			streams[ref] = out
		case composeEncode:
			config, _, err := prepareEncodeConfig(streams[node.inputs[0]], node.request, realtime)
			if err != nil {
				return err
			}
			_, encoded, err := service.compileEncodeStageNamed(ctx, graph, node.name, node.inputs[0], node.request, config, bufferOf(node))
			if err != nil {
				return err
			}
			streams[ref] = encoded
		case composeSink, composeRemuxSink:
			sink := s.outputAttachments[node.output].sink
			if node.name != sink.Name() {
				sink = namedSink{name: node.name, sink: sink}
			}
			sinkRef, err := graph.AddSink(sink, bufferOf(node))
			if err != nil {
				return err
			}
			if err := connectComposeInputs(graph, node.inputs, sinkRef); err != nil {
				return err
			}
		case composeMux, composeRemuxMux:
			var muxed []av.Stream
			if node.kind == composeRemuxMux {
				for _, from := range node.inputs {
					input := sourceInputs[from]
					muxed = append(muxed, qualifyStreams(sourceStreams[input], qualifiers[input])...)
				}
			} else {
				for _, input := range node.inputs {
					muxed = append(muxed, streams[input])
				}
			}
			destination := s.outputAttachments[node.output]
			muxStage, err := service.openMuxDestinationStage(ctx, destination, node.output, muxed, destinationOpenFormat(destination), destinationGraphFormat(destination))
			if err != nil {
				return err
			}
			stage := pipeline.Stage(muxStage)
			if node.name != muxStage.Name() {
				stage = namedStage{name: node.name, stage: muxStage}
			}
			if err := addComposeStage(graph, stage, bufferOf(node), node.inputs); err != nil {
				return err
			}
		default:
			in.sourceStreams = sourceStreams
			in.qualifiers = qualifiers
			stream, err := lowerJoinNode(ctx, service, graph, node, streams, in)
			if err != nil {
				return err
			}
			streams[ref] = stream
		}
	}
	return nil
}

// selectStream is the stream a select picks from its input's streams.
func (r *recipeLowerer) selectStream(node composeNode, streams []av.Stream, qualifier string) (av.Stream, error) {
	input := r.state.inputAttachments[node.input]
	selected, err := selectDecodeStream(streams, node.selector)
	if inputStartsFrameDomain(input) {
		selected, err = selectStream(streams, node.selector)
	}
	if err != nil {
		return av.Stream{}, err
	}
	return qualifyStream(selected, qualifier), nil
}

// branchBuffers sizes each branch's declared queue from the shapes the work
// plan records for it.
func (r *recipeLowerer) branchBuffers(work workPlan) (map[int]pipeline.BufferPolicy, error) {
	operations := graphPlanOperationsByBranch(work.Operations)
	out := map[int]pipeline.BufferPolicy{}
	for i, stream := range r.state.recipe.Streams {
		buffer, err := branchComposeBufferPolicy(stream.Buffer, operations[stream.Name])
		if err != nil {
			return nil, err
		}
		out[i] = buffer
	}
	return out, nil
}

// composeLowering is what building join nodes needs beyond the plan.
type composeLowering struct {
	realtime bool
	// admission enables per-arm backpressure on join stages; work sizes a
	// pinned join queue.
	admission     bool
	work          workPlan
	sourceStreams map[int][]av.Stream
	qualifiers    map[int]string
}

// addComposeStage adds stage and connects its inputs, closing the stage if
// the graph refuses it.
func addComposeStage(graph pipeline.Graph, stage pipeline.Stage, buffer pipeline.BufferPolicy, inputs []pipeline.NodeRef) error {
	ref, err := graph.AddStage(stage, buffer)
	if err != nil {
		stage.Close()
		return err
	}
	return connectComposeInputs(graph, inputs, ref)
}

func connectComposeInputs(graph pipeline.Graph, inputs []pipeline.NodeRef, to pipeline.NodeRef) error {
	for _, from := range inputs {
		if err := connectRefs(graph, from, to); err != nil {
			return err
		}
	}
	return nil
}

// graphConfig runs the graph buffered when the runtime asks for it, when a
// realtime graph decodes or encodes, when a join needs concurrent arms or a
// control-plane queue, when a branch declared a queue, or when a fanout of
// goav-owned encode work can run its branches in parallel.
func (r *recipeLowerer) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	s := r.state
	rt := s.runtime
	if rt == nil {
		return pipeline.GraphConfig{}, recipeGraphUnsupportedRecipeError("configure graph", s.recipe)
	}
	buffer := rt.buffer
	if buffer.IsDirect() {
		for _, id := range s.joinOrder {
			if pinned := s.joins[id].treeGraphBuffer(rt.realtime); pinned != nil {
				buffer = *pinned
				break
			}
		}
	}
	if buffer.IsDirect() && rt.realtime && r.realtimeCodecWork() {
		buffer = realtimeRecipeBufferPolicy(buffer)
	}
	declared := false
	for i := range s.recipe.Streams {
		declared = declared || !s.recipe.Streams[i].Buffer.PipelinePolicy().IsDirect()
	}
	promoted := false
	if buffer.IsDirect() && (declared || r.runsParallel()) {
		buffer = pipeline.BufferPolicy{Capacity: promotedGraphBufferCapacity, Drop: pipeline.DropBlock}
		promoted = true
	}
	if !buffer.IsDirect() {
		budgeted, err := bufferPolicyWithShapeBudgets(buffer, gp.work)
		if err != nil {
			if promoted && !declared {
				// Parallelism is an optimization: stay direct.
				budgeted = rt.buffer
			} else {
				return pipeline.GraphConfig{}, err
			}
		}
		buffer = budgeted
	}
	return pipeline.GraphConfig{
		Name:             firstNonEmpty(gp.name, "goav"),
		Realtime:         rt.realtime,
		Buffer:           buffer,
		EventCapacity:    rt.eventCapacity,
		CloseWaitTimeout: rt.closeWaitTimeout,
	}, nil
}

// realtimeCodecWork reports a graph that decodes or encodes, so a realtime
// run puts codec work on node workers instead of the source's call.
func (r *recipeLowerer) realtimeCodecWork() bool {
	for _, node := range r.graph.nodes {
		switch node.kind {
		case composeDecode, composeEncode, composeArmDecode:
			return true
		}
	}
	return false
}

// runsParallel reports whether a fanout runs its branches on their own
// workers by default: one source fans out to at least two encoders, every
// operation is built in (no custom .Do stage), and every output is a
// goav-owned muxer fed by exactly one branch. Such a graph runs no caller
// code inside a branch and each output still receives one branch's packets
// in order, so concurrency changes only the wall time.
func (r *recipeLowerer) runsParallel() bool {
	s := r.state
	if len(s.joins) != 0 {
		return false
	}
	encoding := map[int]int{}
	shared := false
	for _, node := range r.graph.nodes {
		switch node.kind {
		case composeStep:
			if node.transform.stage != nil {
				return false
			}
		case composeEncode:
			input := s.streamOfNode(node)
			encoding[input]++
			shared = shared || encoding[input] >= 2
		case composeSink:
			return false
		case composeMux:
			destination := s.outputAttachments[node.output]
			if destination.custom != nil || len(node.inputs) != 1 {
				return false
			}
		}
	}
	return shared
}

// streamOfNode is the recipe input a branch-owned node reads.
func (s *recipeCompileState) streamOfNode(node composeNode) int {
	if node.owner >= 0 {
		return s.recipe.Streams[node.owner].Input
	}
	return -1
}

func (s *recipeCompileState) String() string {
	return fmt.Sprintf("recipe(%d streams)", len(s.recipe.Streams))
}
