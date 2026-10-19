package compile

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// planJoins plans every join of the recipe, nested ones first.
func (s *recipeState) planJoins() error {
	s.joins = map[recipeir.NodeID]*JoinPlan{}
	s.armDecodes = map[recipeir.NodeID]string{}
	s.armTaps = map[recipeir.NodeID]bool{}
	sets := s.inputStreamSets()
	for _, id := range s.joinOrder {
		if _, err := s.planJoin(id, sets); err != nil {
			return err
		}
	}
	return nil
}

func (s *recipeState) planJoin(id recipeir.NodeID, sets []InputStreamSet) (*JoinPlan, error) {
	if p, ok := s.joins[id]; ok {
		return p, nil
	}
	node := s.req.Graph.Nodes[id]
	kind := node.Join.Kind
	tree := &JoinTreeSnapshot{Kind: JoinKind(kind)}
	if node.Join.Sync {
		tree.Sync = JoinSyncPTS
	}
	if _, builtin := joinProfiles[JoinKind(kind)]; !builtin || node.Join.Stage != nil {
		tree.Custom = &CustomJoinSpec{Name: kind, Stage: node.Join.Stage}
	}
	if node.Join.Sync && (JoinKind(kind) == JoinSelect || tree.Custom != nil) {
		return nil, StreamSyncByPTSError()
	}
	profile, err := resolveJoinProfile(tree)
	if err != nil {
		return nil, err
	}
	name := s.joinNames[id]
	if tree.Custom != nil && name != kind {
		return nil, customJoinNameCollisionError(kind, name)
	}
	p := &JoinPlan{env: s.env, Tree: tree, Profile: profile, Name: name, node: id}
	if encode, ok := s.joinEncode(id); ok {
		// An encoder's explicit format is the target a join over files
		// converts to; otherwise its first input's format is.
		p.target = explicitEncodeSoftInputShape(encode)
	}
	if len(node.Parents) < 2 {
		return nil, joinInputsError(kind, name)
	}
	seen := make(map[av.StreamID]struct{}, len(node.Parents))
	for i, armID := range node.Parents {
		armPlan, region, armOps, err := s.planJoinArm(p, armID, node.Join.At[i], sets)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[armPlan.Stream.ID]; dup {
			return nil, JoinArmError(name, string(armPlan.Stream.ID), kind+" arms must have distinct stream ids",
				"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
		}
		seen[armPlan.Stream.ID] = struct{}{}
		if err := validateJoinArmDomain(name, armPlan.inputName, profile, armPlan.Domain, armOps); err != nil {
			return nil, err
		}
		// Explicit .Decode() on a packet arm becomes the arm decode node.
		// Frame arms feed frame-consuming joins directly, and passthrough
		// joins without .Decode() forward packets as-is.
		if armPlan.Domain == shape.DomainPacket && recipeIROperationsContainKind(armOps, plan.OpDecode) {
			armPlan.decodeNode = name + "-decode-" + string(armPlan.Stream.ID)
			// An arm path other joins' arms read decodes once: the later arms
			// read the decoder the first planned.
			if first, ok := s.armDecodes[armPlan.decodeGraph]; ok && armPlan.hasDecodeGraph {
				armPlan.decodeNode, armPlan.Bind = first, nil
			} else if armPlan.hasDecodeGraph {
				s.armDecodes[armPlan.decodeGraph] = armPlan.decodeNode
			}
		}
		if profile.planArm != nil {
			stagePlan, err := profile.planArm(p, joinArmSnapshot{region: region}, armPlan.Stream)
			if err != nil {
				return nil, err
			}
			armPlan.stage = stagePlan
		}
		if len(p.Arms) == 0 && armPlan.late && profile.conformFollowsFirstArm {
			// The join's format follows its first arm, known only once it
			// opens: every arm then converts at run time.
			p.lateTarget = true
		}
		if profile.ConformTarget != nil && (armPlan.late || p.lateTarget) {
			armPlan.conform = p.planArmConform(armPlan)
		} else if armPlan.stage == nil && profile.armExpected != nil {
			// The solver converts a nested arm's output like any arm: an
			// outer mix resamples a sub-mix that produced another rate.
			stagePlan, err := p.solveArmConversion(s.env, armPlan.Stream, armPlan.inputName)
			if err != nil {
				return nil, err
			}
			armPlan.stage = stagePlan
		}
		p.Arms = append(p.Arms, armPlan)
	}
	for i := range p.Arms {
		p.Late = p.Late || p.Arms[i].late
	}
	p.Joined = profile.joinedStream(p)
	p.joinedDomain = p.deriveJoinedDomain()
	s.joins[id] = p
	return p, nil
}

// joinEncode is the first encoder a branch of the join's output declares.
func (s *recipeState) joinEncode(id recipeir.NodeID) (codec.Spec, bool) {
	for i := range s.recipe.Streams {
		if s.recipe.Streams[i].Root != id {
			continue
		}
		for _, operation := range s.recipe.Streams[i].Operations {
			if operation.Kind == plan.OpEncode {
				return operation.Encode, true
			}
		}
	}
	return codec.Spec{}, false
}

// planJoinArm resolves one arm: its plan, its canvas position, and the arm's
// own operations after what streams already lower.
func (s *recipeState) planJoinArm(p *JoinPlan, armID recipeir.NodeID, at recipeir.Point, sets []InputStreamSet) (JoinArmPlan, *compositeRegion, []recipeir.Operation, error) {
	g := s.req.Graph
	name, kind, profile := p.Name, string(p.Tree.Kind), p.Profile
	var region *compositeRegion
	if at.Set {
		region = &compositeRegion{x: at.X, y: at.Y}
	}
	path := g.Path(armID)
	root := path[0]
	rootNode := g.Nodes[root]
	feed, fed := s.armFeed(path)
	start := 1
	if fed {
		for i := range path {
			if path[i] == feed {
				start = i + 1
			}
		}
	}
	var armOps []recipeir.Operation
	for _, id := range path[start:] {
		operation := g.Nodes[id].Op
		operation.Node = id
		if operation.Kind == recipeir.OpName {
			continue
		}
		armOps = append(armOps, operation)
	}
	switch rootNode.Kind {
	case recipeir.NodeJoin:
		if fed {
			break
		}
		sub, err := s.planJoin(root, sets)
		if err != nil {
			return JoinArmPlan{}, nil, nil, err
		}
		for _, operation := range armOps {
			switch {
			case operation.Kind == plan.OpEncode:
				return JoinArmPlan{}, nil, nil, JoinArmError(name, sub.Name,
					"a nested "+string(sub.Tree.Kind)+" arm cannot carry .Encode(...); encode the outer join instead",
					"move .Encode(...) to the outer "+name+": encode once after the final convergence")
			case recipeIROperationIsAnnotation(operation), operation.Kind == plan.OpTap:
			default:
				return JoinArmPlan{}, nil, nil, StreamInvalidError("", "a join used as another join's input takes only .Tap(...), and for Mix and Composite .Auto, .Require, .Prefer, and .Encode; apply other operations to its inputs or after the outer join")
			}
		}
		if profile.media != "" && sub.Joined.Type != profile.media {
			return JoinArmPlan{}, nil, nil, JoinArmError(name, sub.Name,
				fmt.Sprintf("%s arm %q produces %s, want %s", kind, sub.Name, sub.Joined.Type, profile.media),
				"feed the "+kind+" arms that produce "+string(profile.media)+" media (mix joins audio, composite joins video)")
		}
		arm := JoinArmPlan{sub: sub, inputName: sub.Name, Stream: sub.Joined, Domain: sub.joinedDomain, late: sub.Late}
		for _, operation := range armOps {
			if operation.Kind == plan.OpTap && operation.Tap.Name == "" {
				return JoinArmPlan{}, nil, nil, tapNameEmptyError("build "+name, sub.Name)
			}
			if operation.Kind == plan.OpTap {
				// A tap on a nested join anchors on the nested join's node.
				arm.taps = append(arm.taps, joinArmTap{ref: TapRef{Name: operation.Tap.Name, Domain: sub.joinedDomain}, after: plan.OpJoin})
			}
		}
		return arm, region, nil, nil
	case recipeir.NodeSelect:
	default:
		return JoinArmPlan{}, nil, nil, JoinArmError(name, name,
			"each "+kind+" arm must be one input's stream or a nested join",
			"pass input.Audio()/.Video() streams or nested goav.Mix/Composite/Select/Join streams as arms")
	}
	input := rootNode.Input
	set := InputStreamSet{Name: name, Domain: shape.DomainPacket}
	if input < len(sets) {
		set = sets[input]
	}
	var armPlan JoinArmPlan
	switch {
	case fed:
		armPlan = s.fedArm(p, feed, input, set, rootNode.Select)
	case joinArmBindsAtOpen(s.req.Inputs[input], set):
		bound, err := planJoinBindArm(name, kind, profile, recipeIRSelector(rootNode.Select), set)
		if err != nil {
			return JoinArmPlan{}, nil, nil, err
		}
		armPlan = bound
	default:
		selector := recipeIRSelector(rootNode.Select)
		if selector.Type == "" {
			selector.Type = profile.media
		}
		stream, err := SelectStream(set.Streams, selector)
		if err != nil {
			return JoinArmPlan{}, nil, nil, err
		}
		armPlan = JoinArmPlan{inputName: set.Name, Stream: stream, Domain: set.Domain}
	}
	armPlan.Input = input
	armPlan.SourceShape = s.req.Inputs[input].Shape
	if err := validateJoinArmOperations(name, armPlan.inputName, armOps); err != nil {
		return JoinArmPlan{}, nil, nil, err
	}
	for _, operation := range armOps {
		if operation.Kind == plan.OpDecode {
			armPlan.decodeGraph, armPlan.hasDecodeGraph = operation.Node, true
		}
	}
	// A tap on a path another join's arm also reads installs once.
	var ownOps []recipeir.Operation
	for _, operation := range armOps {
		if operation.Kind == plan.OpTap && s.armTaps[operation.Node] {
			continue
		}
		if operation.Kind == plan.OpTap {
			s.armTaps[operation.Node] = true
		}
		ownOps = append(ownOps, operation)
	}
	taps, err := joinChainArmTaps(name, armPlan.inputName, armPlan.Domain, ownOps)
	if err != nil {
		return JoinArmPlan{}, nil, nil, err
	}
	armPlan.taps = taps
	return armPlan, region, armOps, nil
}

// armFeed is the deepest node of an arm's path that a branch of the recipe
// lowers too: the arm reads it instead of opening its own path.
func (s *recipeState) armFeed(path []recipeir.NodeID) (recipeir.NodeID, bool) {
	for i := len(path) - 1; i >= 0; i-- {
		id := path[i]
		for j := range s.recipe.Streams {
			stream := s.recipe.Streams[j]
			if stream.Root == id {
				return id, true
			}
			for _, operation := range stream.Operations {
				if operation.Node == id {
					return id, true
				}
			}
		}
	}
	return 0, false
}

// fedArm plans an arm that reads a node a branch lowers: its stream is what
// that node carries, known statically when the input's streams are.
func (s *recipeState) fedArm(p *JoinPlan, feed recipeir.NodeID, input int, set InputStreamSet, selector plan.StreamSelect) JoinArmPlan {
	domain := set.Domain
	for _, id := range s.req.Graph.Path(feed)[1:] {
		switch s.req.Graph.Nodes[id].Op.Kind {
		case plan.OpDecode:
			domain = shape.DomainFrame
		case plan.OpEncode:
			domain = shape.DomainPacket
		}
	}
	arm := JoinArmPlan{inputName: set.Name, Domain: domain, feed: feed, fed: true}
	if set.Known && len(set.Streams) != 0 {
		if stream, err := SelectStream(set.Streams, recipeIRSelector(selector)); err == nil {
			arm.Stream = stream
			return arm
		}
	}
	media := FirstNonEmptyMedia(selector.Type, p.Profile.media)
	arm.Stream = av.Stream{ID: av.StreamID(set.Name), Type: media, Codec: av.CodecParameters{Type: media}}
	arm.late = true
	return arm
}

func recipeIROperationsContainKind(operations []recipeir.Operation, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

// join plans a join's tree depth first: each arm's chain (the input's source,
// a nested join, or the node a fed arm reads), its bind, decode, and
// conversion, then the join node, whose convergence edges come after every
// arm's chain.
func (l *lowering) join(id recipeir.NodeID) (pipeline.NodeRef, error) {
	if ref, ok := l.refs[id]; ok {
		return ref, nil
	}
	p := l.s.joins[id]
	armRefs := make([]pipeline.NodeRef, 0, len(p.Arms))
	for i := range p.Arms {
		arm := &p.Arms[i]
		var upstream pipeline.NodeRef
		shared := false
		switch {
		case arm.sub != nil:
			ref, err := l.join(arm.sub.node)
			if err != nil {
				return "", err
			}
			upstream = ref
		case arm.fed:
			ref, err := l.ensure(arm.feed)
			if err != nil {
				return "", err
			}
			upstream = ref
		default:
			if ref, ok := l.refs[arm.decodeGraph]; ok && arm.hasDecodeGraph {
				// Another join's arm planned this path's decoder: read it.
				upstream, shared = ref, true
				break
			}
			upstream = l.source(arm.Input)
		}
		arm.sourceNode = string(upstream)
		if arm.Bind != nil && !shared {
			upstream = l.add(ComposeNode{Kind: ComposeArmBind, Name: arm.Bind.node, Detail: BindArmNodeDetail(arm.Bind.Selector), Inputs: []pipeline.NodeRef{upstream}, Join: p, Arm: i})
		}
		if arm.decodeNode != "" && !shared {
			upstream = l.add(ComposeNode{
				Kind:   ComposeArmDecode,
				Name:   arm.decodeNode,
				Detail: DecodeRequestDetail(DecodeRequest{Selector: av.StreamSelector{Type: arm.Stream.Type}}),
				Inputs: []pipeline.NodeRef{upstream},
				Join:   p,
				Arm:    i,
			})
			if arm.hasDecodeGraph {
				l.refs[arm.decodeGraph] = upstream
			}
		}
		if arm.stage != nil {
			upstream = l.add(ComposeNode{
				Kind:      ComposeTransform,
				Name:      arm.stage.transform.Name,
				Detail:    MediaTransformDetail(arm.stage.transform),
				Inputs:    []pipeline.NodeRef{upstream},
				Transform: arm.stage.transform,
				Stream:    arm.stage.stream,
				Join:      p,
			})
		}
		if arm.conform != nil {
			node := ComposeNode{
				Kind:   ComposeArmConform,
				Name:   arm.conform.node,
				Detail: conformNodeDetail(arm.conform.target),
				Inputs: []pipeline.NodeRef{upstream},
				Join:   p,
				Arm:    i,
			}
			if i > 0 && p.Profile.conformFollowsFirstArm {
				node.Target = armRefs[0]
			}
			upstream = l.add(node)
		}
		armRefs = append(armRefs, upstream)
	}
	ref := l.add(ComposeNode{Kind: ComposeJoin, Name: p.Name, Detail: p.joinNodeDetail(), Inputs: armRefs, Join: p})
	l.refs[id] = ref
	l.nodeNames[id] = p.Name
	return ref, nil
}

// ensure plans the node a fed arm reads before the join that reads it: a
// select root, or a branch's operation and the work before it.
func (l *lowering) ensure(id recipeir.NodeID) (pipeline.NodeRef, error) {
	if ref, ok := l.refs[id]; ok {
		return ref, nil
	}
	s := l.s
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		if stream.Root == id {
			if err := l.root(id, i); err != nil {
				return "", err
			}
			return l.refs[id], nil
		}
		for _, operation := range stream.Operations {
			if operation.Node != id {
				continue
			}
			if _, ok := l.refs[stream.Root]; !ok {
				if err := l.root(stream.Root, i); err != nil {
					return "", err
				}
			}
			for _, before := range stream.Operations {
				if _, err := l.operation(i, before); err != nil {
					return "", err
				}
				if before.Node == id {
					break
				}
			}
			return l.parentRefThrough(i, id), nil
		}
	}
	return "", GraphPlanInvalidError("join arm reads a node no branch plans", nil)
}

// parentRefThrough is the planned node a branch carries at node: node's own
// when it plans one, else the nearest before it.
func (l *lowering) parentRefThrough(index int, node recipeir.NodeID) pipeline.NodeRef {
	if ref, ok := l.refs[node]; ok && ref != "" {
		return ref
	}
	return l.parentRef(index, node)
}
