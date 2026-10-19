package compile

import (
	"cmp"
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

// composeNodeKind is what a planned node lowers to.
type composeNodeKind uint8

const (
	ComposeSource composeNodeKind = iota + 1
	ComposeSelect
	ComposeDecode
	ComposeStep
	ComposeEncode
	ComposeSink
	ComposeMux
	// composeRemuxSink and composeRemuxMux are whole-input packet copies:
	// every source feeds them with all its streams.
	ComposeRemuxSink
	ComposeRemuxMux
	// Join trees: the bind of an arm whose streams are known once it opens,
	// its event-keeping decode, its run-time conversion, a planned arm
	// conversion, and the convergence stage.
	ComposeArmBind
	ComposeArmDecode
	ComposeArmConform
	ComposeTransform
	ComposeJoin
)

// ComposeNode is one planned node: its name and describe detail, the nodes it
// consumes in connect order, and what the build makes it from.
type ComposeNode struct {
	Kind   composeNodeKind
	Name   string
	Detail string
	Inputs []pipeline.NodeRef
	// input is a source's or select's recipe input; selector what a select or
	// decode picks.
	Input    int
	Selector av.StreamSelector
	// decode is a decode's codec options; dropEvents keeps a lone decode to
	// a sink from forwarding stream events.
	Decode     codec.Spec
	DropEvents bool
	// transform is a step's work; request an encoder's.
	Transform MediaTransform
	Request   EncodeRequest
	// owner is the branch that alone reads the node, or -1.
	Owner int
	// routes are the routes an attached node reads its inputs by, aligned
	// with inputs; empty for a build, which reads every input whole.
	routes []AnchorRoute
	// output indexes a destination.
	Output int
	// join is the join a join-tree node belongs to, arm the arm it serves,
	// and stream the planned stream a transform is built against.
	Join     *JoinPlan
	Arm      int
	Stream   av.Stream
	Resolved bool
	Target   pipeline.NodeRef
}

// ComposeGraph is the one schedule a recipe is described and built from.
type ComposeGraph struct {
	name  string
	Nodes []ComposeNode
}

func (g *ComposeGraph) add(node ComposeNode) pipeline.NodeRef {
	g.Nodes = append(g.Nodes, node)
	return pipeline.NodeRef(node.Name)
}

// emit renders the schedule as a spec: the nodes in schedule order, then the
// edges grouped by producer in schedule order, each producer's edges in
// consumer order — the order a built graph reports them in.
func (g ComposeGraph) emit(spec pipeline.Spec) (pipeline.Spec, error) {
	planned := make(map[string]PlannedNode, len(g.Nodes))
	for _, node := range g.Nodes {
		kind := pipeline.NodeStage
		switch node.Kind {
		case ComposeSink, ComposeRemuxSink:
			kind = pipeline.NodeSink
		case ComposeSource:
			kind = pipeline.NodeSource
		}
		if err := AddPlannedNode(planned, &spec, node.Name, kind, pipeline.NodeRef(node.Name), node.Detail); err != nil {
			return pipeline.Spec{}, err
		}
	}
	consumers := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(g.Nodes))
	// An attached group's anchors are nodes of the running graph, not of
	// the schedule: their edges lead.
	var producers []pipeline.NodeRef
	for _, node := range g.Nodes {
		for i, input := range node.Inputs {
			if _, ok := planned[input.String()]; !ok && !slices.Contains(producers, input) {
				producers = append(producers, input)
			}
			route := node.Route(i)
			consumers[input] = append(consumers[input], pipeline.EdgeSpec{From: input, To: pipeline.NodeRef(node.Name), Policy: route.Policy, Label: route.Label})
		}
	}
	for _, node := range g.Nodes {
		producers = append(producers, pipeline.NodeRef(node.Name))
	}
	for _, from := range producers {
		spec.Edges = append(spec.Edges, consumers[from]...)
	}
	return spec, nil
}

// AnchorRoute is the route a node reads an input by.
type AnchorRoute struct {
	Policy pipeline.RoutePolicy
	Label  string
}

// Route is the route the node reads its input at index by: an attached
// node reads a live anchor by the anchor's own policy and label, and every
// other input whole.
func (n ComposeNode) Route(index int) AnchorRoute {
	if index < len(n.routes) && n.routes[index].Policy != "" {
		return n.routes[index]
	}
	return AnchorRoute{Policy: pipeline.RouteAll}
}

// recipeLowering plans one recipe's composeGraph.
type lowering struct {
	s *recipeState
	g ComposeGraph
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
	// attach is set when the recipe is an attach group.
	attach *attachLowering
}

// attachLowering is what lowering an attach group adds to a build's: its
// roots are anchors on the running task — a live node, or a tap an earlier
// branch of the group declares — and its nodes are named under the branch
// that alone reads them, or the attachment when several do.
type attachLowering struct {
	name string
	// anchors are the live nodes anchor roots read, and routes the routes
	// they read them by.
	anchors map[recipeir.NodeID]pipeline.NodeRef
	routes  map[recipeir.NodeID]AnchorRoute
	// pending are the tap operations anchor roots on a tap the group itself
	// declares read.
	pending map[recipeir.NodeID]recipeir.NodeID
}

func newRecipeLowering(s *recipeState) *lowering {
	return &lowering{
		s:            s,
		refs:         map[recipeir.NodeID]pipeline.NodeRef{},
		sources:      map[int]pipeline.NodeRef{},
		groups:       map[string]pipeline.NodeRef{},
		groupDecodes: map[string]pipeline.NodeRef{},
		names:        map[string]bool{},
		counts:       map[string]int{},
		owners:       map[recipeir.NodeID][]int{},
		nodeNames:    map[recipeir.NodeID]string{},
		sourceNames:  sourceNodeNames(s.req.Inputs),
	}
}

func (s *recipeState) lower() error {
	if !s.env.Present && s.req.Preflight {
		return nil
	}
	l := newRecipeLowering(s)
	if err := l.plan(); err != nil {
		return err
	}
	spec := pipeline.Spec{Name: s.graphName(), Realtime: !s.env.Present || s.env.Realtime}
	spec, err := l.g.emit(spec)
	if err != nil {
		return err
	}
	l.g.name = spec.Name
	s.work = buildRecipeWorkPlan(s, l, spec)
	s.lowered, s.composed, s.spec = true, l.g, spec
	return nil
}

// graphName names the task's graph: a recipe that is one join is named after
// it.
func (s *recipeState) graphName() string {
	if root, ok := s.soleJoinRoot(); ok {
		return "goav-" + s.joinNames[root]
	}
	return "goav"
}

func (l *lowering) plan() error {
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
	for input := range s.req.Inputs {
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
	for dest := range s.outputs {
		l.destination(dest)
	}
	return nil
}

func (s *recipeState) ruleInput(input int) bool {
	return len(s.streamRules) != 0 && input == 0
}

func (l *lowering) unique(name string) string {
	out := name
	for n := 2; l.names[out]; n++ {
		out = name + "-" + strconv.Itoa(n)
	}
	l.names[out] = true
	return out
}

func (l *lowering) add(node ComposeNode) pipeline.NodeRef {
	l.names[node.Name] = true
	return l.g.add(node)
}

// source plans one input's source node, once.
func (l *lowering) source(input int) pipeline.NodeRef {
	if ref, ok := l.sources[input]; ok {
		return ref
	}
	name := l.sourceNames[input]
	ref := l.add(ComposeNode{
		Kind:   ComposeSource,
		Name:   name,
		Detail: l.s.req.Inputs[input].SourceDetail,
		Input:  input,
	})
	l.sources[input] = ref
	return ref
}

// named is the name of a planned node owner alone reads (or -1): an attach
// group names a branch's own nodes under the branch, and the nodes several
// of its branches share under the attachment.
func (l *lowering) named(owner int, name string) string {
	switch {
	case l.attach == nil:
		return name
	case owner >= 0:
		return runtimeBranchNodeName(l.s.recipe.Streams[owner].Name, name)
	default:
		return runtimeBranchNodeName(l.attach.name, name)
	}
}

// scope is the name a root's shared nodes are named in: an input stream's
// kind, qualified by its input when the recipe has several, a join's name,
// or an attached stream's own name.
func (l *lowering) scope(stream recipeir.Stream) string {
	if l.attach != nil {
		return stream.Name
	}
	if stream.Input < 0 {
		return l.s.joinNames[stream.Root]
	}
	selector := recipeIRStreamSelector(stream)
	return branchComposeInputNodeName(cmp.Or(string(selector.Type), string(selector.ID), selector.Name, "stream"), stream.Selector.Input)
}

func selectGroupKey(stream recipeir.Stream) string {
	return branchComposeSelectorGroupKey(recipeIRStreamSelector(stream), strconv.Itoa(stream.Input))
}

// root plans a root: an input stream's select, the decode right after it,
// and the work every branch of the root shares; or a join.
func (l *lowering) root(root recipeir.NodeID, first int) error {
	if l.attach != nil {
		// An attached root reads a live node, or the node before a tap an
		// earlier branch of the group declares.
		ref, ok := l.attach.anchors[root]
		if !ok {
			var err error
			if ref, err = l.ensure(l.attach.pending[root]); err != nil {
				return err
			}
		}
		l.refs[root] = ref
		return l.trunk(root, first)
	}
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
		selectRef = l.add(ComposeNode{
			Kind:     ComposeSelect,
			Name:     l.unique(branchComposeInputNodeName(selectNodeName(selector), stream.Selector.Input)),
			Detail:   selectNodeDetail(selector),
			Inputs:   []pipeline.NodeRef{l.source(stream.Input)},
			Input:    stream.Input,
			Selector: selector,
		})
		l.groups[key] = selectRef
		// The decode right after a select serves every branch of the selector
		// that decodes there.
		decode, ok, err := l.groupDecode(key)
		if err != nil {
			return err
		}
		if ok {
			decodeRef := l.add(ComposeNode{
				Kind:       ComposeDecode,
				Name:       l.unique(branchComposeInputNodeName(DecodeNodeName(selector), stream.Selector.Input)),
				Detail:     DecodeRequestDetail(DecodeRequest{Selector: selector}),
				Inputs:     []pipeline.NodeRef{selectRef},
				Selector:   selector,
				Decode:     decode,
				DropEvents: l.dropDecodeEvents(),
				Owner:      -1,
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
func (l *lowering) groupDecode(key string) (codec.Spec, bool, error) {
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
		if found && codecSpecHasDecodeIntent(operation.Decode) && codecSpecHasDecodeIntent(config) && !CodecSpecEqual(config, operation.Decode) {
			return codec.Spec{}, false, branchComposeDecodeConfigConflictError(owner, stream.Name)
		}
		if !found || codecSpecHasDecodeIntent(operation.Decode) {
			config, owner = CloneCodecSpec(operation.Decode), stream.Name
		}
		found = true
	}
	return config, found, nil
}

// dropDecodeEvents reports a recipe that is one lone branch decoding to
// sinks without an encoder: its decode forwards no stream events.
func (l *lowering) dropDecodeEvents() bool {
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
func (l *lowering) trunk(root recipeir.NodeID, first int) error {
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
func (l *lowering) branch(index int) (pipeline.NodeRef, error) {
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
func (l *lowering) parentRef(index int, node recipeir.NodeID) pipeline.NodeRef {
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
func (l *lowering) operation(index int, operation recipeir.Operation) (pipeline.NodeRef, error) {
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
	var node ComposeNode
	switch operation.Kind {
	case plan.OpDecode:
		if ref, ok := l.groupDecodes[selectGroupKey(stream)]; ok && stream.Input >= 0 && input == l.refs[stream.Root] {
			l.refs[operation.Node] = ref
			l.nodeNames[operation.Node] = string(ref)
			return ref, nil
		}
		selector := recipeIRStreamSelector(stream)
		node = ComposeNode{
			Kind:     ComposeDecode,
			Name:     l.unique(l.named(owner, "decode-"+l.scope(stream))),
			Detail:   DecodeRequestDetail(DecodeRequest{Selector: selector}),
			Selector: selector,
			Decode:   CloneCodecSpec(operation.Decode),
			// An attached decoder forwards no stream events: the anchor's
			// own consumers saw them.
			DropEvents: l.attach != nil,
		}
	case plan.OpTransform, plan.OpStage:
		transform, err := l.transform(name, owner, operation)
		if err != nil {
			return "", err
		}
		node = ComposeNode{Kind: ComposeStep, Name: transform.Name, Detail: MediaTransformDetail(transform), Transform: transform}
	case plan.OpEncode:
		request := s.encodeRequest(index, operation, owner < 0)
		if owner < 0 {
			request.Name = name
		}
		backend := EncodeAdapterRequestFromRecipeIR(operation.Encode, stream)
		backend.Realtime = s.env.Realtime || inputsLive(s.req.Inputs)
		detail := JoinSpecDetail(EncodeNodeDetail(request), EncodeBackendDetail(s.env.Codecs, backend))
		node = ComposeNode{Kind: ComposeEncode, Name: l.unique(l.named(owner, EncodeNodeName(request))), Detail: detail, Request: request}
	default:
		// Copies, taps, annotations, names, and queues plan no node.
		return "", nil
	}
	node.Inputs = []pipeline.NodeRef{input}
	if l.attach != nil {
		node.routes = []AnchorRoute{l.anchorRoute(index, input)}
	}
	node.Owner = owner
	ref := l.add(node)
	l.refs[operation.Node] = ref
	l.nodeNames[operation.Node] = node.Name
	return ref, nil
}

// transform names and configures one step: a stage keeps its own name (a
// sync or playout gate is named for its stream and policy), a transform is
// named for its adapter and its stream, numbered from its second.
func (l *lowering) transform(name string, owner int, operation recipeir.Operation) (MediaTransform, error) {
	if operation.Kind == plan.OpStage {
		if operation.Stage == nil {
			return MediaTransform{}, branchChainStepError(name, "branch stage operation has no stage")
		}
		if gate, ok := operation.Stage.(gateStage); ok {
			return MediaTransform{Name: l.unique(l.named(owner, gate.GateName(name))), Stage: operation.Stage}, nil
		}
		return MediaTransform{Name: l.named(owner, operation.Stage.Name()), Stage: operation.Stage}, nil
	}
	l.counts[name]++
	suffix := ""
	if n := l.counts[name]; n > 1 {
		suffix = "-" + strconv.Itoa(n)
	}
	switch operation.Transform.Kind {
	case recipeir.TransformResize:
		resize := operation.Transform.Resize
		factory := cmp.Or(operation.Component, filter.FactoryResize)
		return MediaTransform{Name: l.unique(l.named(owner, factory+"-"+name+suffix)), Factory: factory, Video: &resize}, nil
	case recipeir.TransformResample:
		resample := operation.Transform.Resample
		factory := cmp.Or(operation.Component, filter.FactoryResample)
		return MediaTransform{Name: l.unique(l.named(owner, factory+"-"+name+suffix)), Factory: factory, Audio: &resample}, nil
	default:
		return MediaTransform{}, branchChainStepError(name, "branch transform operation is empty")
	}
}

// encodeRequest is a branch encoder's request. An encoder one branch owns
// encodes under the branch's name, unless the branch is the recipe's only
// one and names no fork; an encoder several branches share keeps the
// identity of the stream it encodes, as As names only a branch's own work.
func (s *recipeState) encodeRequest(index int, operation recipeir.Operation, shared bool) EncodeRequest {
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
	return EncodeRequest{Name: stream.Name, Selector: recipeIRStreamSelector(stream), Config: config}
}

// destination plans one destination fed by every branch that routes to it,
// in branch order.
func (l *lowering) destination(dest int) {
	s := l.s
	var inputs []pipeline.NodeRef
	var routes []AnchorRoute
	var feeds []int
	for i := range s.recipe.Streams {
		for _, d := range s.facts[i].dests {
			if d == dest {
				inputs = append(inputs, l.leaves[i])
				routes = append(routes, l.anchorRoute(i, l.leaves[i]))
				feeds = append(feeds, i)
			}
		}
	}
	spec := s.outputs[dest]
	owner := -1
	if len(feeds) == 1 {
		owner = feeds[0]
	}
	if spec.Sink {
		l.add(ComposeNode{Kind: ComposeSink, Name: l.destinationNamed(owner, destinationSinkNodeName(spec, s.outputDestinationNames[dest], dest)), Detail: spec.SinkDetail, Inputs: inputs, routes: routes, Output: dest, Owner: owner})
		return
	}
	output := spec.Output
	if output.Name == "" {
		output.Name = s.outputDestinationNames[dest]
	}
	l.add(ComposeNode{Kind: ComposeMux, Name: l.destinationNamed(owner, MuxNodeName(output, dest)), Detail: OutputNodeDetailWithFormat(output, spec.Format), Inputs: inputs, routes: routes, Output: dest, Owner: owner})
}

// destinationNamed names a destination node: an attach group names a
// destination one branch alone feeds under the branch, and keeps the name
// of one several branches share.
func (l *lowering) destinationNamed(owner int, name string) string {
	if owner < 0 {
		return name
	}
	return l.named(owner, name)
}

// anchorRoute is the route the branch at index reads input by: its live
// anchor's own policy and label when input is that anchor.
func (l *lowering) anchorRoute(index int, input pipeline.NodeRef) AnchorRoute {
	if l.attach == nil {
		return AnchorRoute{}
	}
	root := l.s.recipe.Streams[index].Root
	if ref, ok := l.attach.anchors[root]; ok && ref == input {
		return l.attach.routes[root]
	}
	return AnchorRoute{}
}

// destinationSinkNodeName names a sink node: the sink's own name, else the
// destination's.
func destinationSinkNodeName(spec Destination, name string, index int) string {
	if spec.SinkName != "" {
		return spec.SinkName
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
func (l *lowering) planRemux() error {
	s := l.s
	for _, remux := range s.remux {
		if input := s.req.Inputs[remux.input]; !input.HasShape || input.Shape.Domain != shape.DomainEvent {
			continue
		}
		for _, dest := range remux.dests {
			if !s.outputs[dest].Sink {
				// Events have no packets to mux.
				return GraphPlanInvalidError("event source destination must be a sink", nil)
			}
		}
	}
	sources := make([]pipeline.NodeRef, len(s.req.Inputs))
	for input := range s.req.Inputs {
		sources[input] = l.source(input)
	}
	for dest := range s.outputs {
		spec := s.outputs[dest]
		// A destination reads the inputs whose copies its Outputs route to it.
		var inputs []pipeline.NodeRef
		for input := range s.req.Inputs {
			for _, remux := range s.remux {
				if remux.input == input && slices.Contains(remux.dests, dest) {
					inputs = append(inputs, sources[input])
					break
				}
			}
		}
		if spec.Sink {
			l.add(ComposeNode{Kind: ComposeRemuxSink, Name: cmp.Or(spec.SinkName, s.outputDestinationNames[dest], "sink"), Detail: spec.SinkDetail, Inputs: inputs, Output: dest})
			continue
		}
		output := spec.Output
		if output.Name == "" {
			output.Name = s.outputDestinationNames[dest]
		}
		l.add(ComposeNode{Kind: ComposeRemuxMux, Name: MuxNodeName(output, dest), Detail: OutputNodeDetailWithFormat(output, spec.Format), Inputs: inputs, Output: dest})
	}
	return nil
}

func (s *recipeState) String() string {
	return fmt.Sprintf("recipe(%d streams)", len(s.recipe.Streams))
}

// sourceNodeNames are the planned source node names of inputs.
func sourceNodeNames(inputs []Input) []string {
	names := make([]string, len(inputs))
	for i := range inputs {
		names[i] = inputs[i].SourceNode
	}
	return names
}
