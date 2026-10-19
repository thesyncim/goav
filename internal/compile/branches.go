package compile

import (
	"cmp"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// streamPath is one Output's chain: the graph nodes from its root to the
// Stream it routes, and the destination nodes it goes to.
type streamPath struct {
	nodes []recipeir.NodeID
	dests []recipeir.NodeID
}

// leafGroup is one distinct Stream routed by one or more Outputs.
type leafGroup struct {
	nodes []recipeir.NodeID
	dests []recipeir.NodeID
}

// derivedBranch is one branch before the recipe orders them.
type derivedBranch struct {
	root  recipeir.NodeID
	leaf  leafGroup
	name  string
	named bool
	lone  bool
}

func (s *recipeState) deriveBranches() error {
	g := s.req
	var paths []streamPath
	for _, out := range g.Outputs {
		if g.Graph.Nodes[out.From].Kind == recipeir.NodeInput {
			s.remux = append(s.remux, remuxOutput{input: g.Graph.Nodes[out.From].Input, nodes: out.Dests})
			continue
		}
		paths = append(paths, streamPath{nodes: g.Graph.Path(out.From), dests: out.Dests})
	}
	if len(s.remux) != 0 && len(paths) != 0 {
		return StreamInvalidError("", "input.To(...) copies every stream; route single streams with input.Video().To(...) instead")
	}
	var roots []recipeir.NodeID
	byRoot := map[recipeir.NodeID][]streamPath{}
	for _, path := range paths {
		root := path.nodes[0]
		switch g.Graph.Nodes[root].Kind {
		case recipeir.NodeAnchor:
			return StreamInvalidError(g.Graph.Nodes[root].Anchor.Tap, "a stream from task.Tap, task.Track, or task.Route attaches to that running task: task.Attach(ctx, outputs...)")
		}
		if _, ok := byRoot[root]; !ok {
			roots = append(roots, root)
		}
		byRoot[root] = append(byRoot[root], path)
	}
	s.claimJoinNames(roots)
	names := newStreamBranchNames()
	var forks, lone []derivedBranch
	var rules []recipeir.NodeID
	for _, root := range roots {
		if g.Graph.Nodes[root].Kind == recipeir.NodeRule {
			rules = append(rules, root)
			continue
		}
		leaves := groupLeaves(g.Graph, byRoot[root])
		if len(leaves) == 1 && !g.Graph.HasOp(leaves[0].nodes[1:], recipeir.OpBuffer) && !g.Graph.HasOp(leaves[0].nodes[1:], recipeir.OpName) {
			lone = append(lone, derivedBranch{root: root, leaf: leaves[0], lone: true})
			continue
		}
		for _, leaf := range leaves {
			forks = append(forks, derivedBranch{root: root, leaf: leaf, name: names.next(g, leaf), named: true})
		}
	}
	if names.err != nil {
		return names.err
	}
	taken := map[string]bool{}
	for _, fork := range forks {
		taken[fork.name] = true
	}
	for i := range lone {
		base := s.rootName(lone[i].root)
		name := base
		for n := 2; taken[name]; n++ {
			name = fmt.Sprintf("%s-%d", base, n)
		}
		taken[name] = true
		lone[i].name = name
	}
	branches := append(forks, lone...)
	for i := range g.Inputs {
		s.recipe.Inputs = append(s.recipe.Inputs, g.Inputs[i].IR)
	}
	if err := s.assignDestinations(branches); err != nil {
		return err
	}
	for i := range branches {
		stream, facts, err := s.branchStream(branches[i], branches)
		if err != nil {
			return err
		}
		s.recipe.Streams = append(s.recipe.Streams, stream)
		s.facts = append(s.facts, facts)
	}
	s.markShared()
	for i := range s.recipe.Streams {
		if err := s.validateStreamOperations(i); err != nil {
			return err
		}
	}
	s.recipe.Graph = g.Graph
	if s.env.Present {
		s.recipe.Policies.Realtime = s.env.Realtime || inputsLive(g.Inputs)
	}
	for _, root := range rules {
		if err := s.deriveRule(root, byRoot[root], names); err != nil {
			return err
		}
	}
	return names.err
}

// rootName names a root's lone branch: the stream kind for an input's
// stream, the join's node name for a join.
func (s *recipeState) rootName(root recipeir.NodeID) string {
	if name, ok := s.joinNames[root]; ok {
		return name
	}
	switch s.req.Graph.Nodes[root].Select.Type {
	case av.MediaVideo:
		return "video"
	case av.MediaAudio:
		return "audio"
	default:
		return "stream"
	}
}

// claimJoinNames gives every join its node name: the first join of a kind
// takes the kind (mix), later ones a suffix (mix-2), claimed depth first from
// each root in order, so a lone join keeps its historical name.
func (s *recipeState) claimJoinNames(roots []recipeir.NodeID) {
	s.joinNames = map[recipeir.NodeID]string{}
	used := map[string]struct{}{}
	var claim func(id recipeir.NodeID)
	claim = func(id recipeir.NodeID) {
		node := s.req.Graph.Nodes[id]
		if node.Kind != recipeir.NodeJoin {
			for _, parent := range node.Parents {
				claim(parent)
			}
			return
		}
		if _, ok := s.joinNames[id]; ok {
			return
		}
		s.joinNames[id] = claimJoinName(used, node.Join.Kind)
		s.joinOrder = append(s.joinOrder, id)
		for _, arm := range node.Parents {
			claim(arm)
		}
	}
	for _, root := range roots {
		claim(root)
	}
}

// assignDestinations orders the recipe's destinations by first use, scanning
// the branches in order, and checks each handle's name.
func (s *recipeState) assignDestinations(branches []derivedBranch) error {
	index := map[recipeir.NodeID]int{}
	byName := map[string]int{}
	add := func(node recipeir.NodeID, owner string, position int) error {
		if _, ok := index[node]; ok {
			return nil
		}
		slot := s.req.Graph.Nodes[node].Output
		dest := s.req.Dests[slot]
		if dest.Invalid != "" {
			return DestinationInvalidError("build stream", cmp.Or(owner, "stream"), dest.Invalid)
		}
		name := cmp.Or(dest.Label, fmt.Sprintf("%s-%d", cmp.Or(owner, "output"), position+1))
		if first, ok := byName[name]; ok && !s.outputs[first].sharesGroup(dest) {
			return DestinationNameClashError(name)
		}
		index[node] = len(s.outputs)
		byName[name] = len(s.outputs)
		s.outputs = append(s.outputs, dest)
		s.outputSlots = append(s.outputSlots, slot)
		s.outputDestinationNames = append(s.outputDestinationNames, name)
		ir := dest.IR
		ir.Name = name
		s.recipe.Destinations = append(s.recipe.Destinations, ir)
		return nil
	}
	for _, branch := range branches {
		for i, node := range branch.leaf.dests {
			if err := add(node, branch.name, i); err != nil {
				return err
			}
		}
	}
	for r := range s.remux {
		for i, node := range s.remux[r].nodes {
			if err := add(node, "", i); err != nil {
				return err
			}
			s.remux[r].dests = append(s.remux[r].dests, index[node])
		}
	}
	s.destIndex = index
	return nil
}

// branchStream is one branch's IR: its root, its operations from the root
// with the packet copy a never-decoded packet stream carries, and its
// destinations.
func (s *recipeState) branchStream(branch derivedBranch, all []derivedBranch) (recipeir.Stream, branchFacts, error) {
	g := s.req
	root := g.Graph.Nodes[branch.root]
	stream := recipeir.Stream{Name: branch.name, Named: branch.named, Root: branch.root, Input: -1}
	facts := branchFacts{lone: branch.lone}
	switch root.Kind {
	case recipeir.NodeSelect:
		stream.Input = root.Input
		stream.Selector = root.Select
		if len(g.Inputs) > 1 {
			stream.Selector.Input = g.Inputs[root.Input].Name
		}
		facts.frames = g.Inputs[root.Input].Frames
	case recipeir.NodeJoin:
		facts.frames = joinOutputFrames(g, branch.root)
	}
	ops := branch.leaf.nodes[1:]
	decodes := g.Graph.HasOp(ops, plan.OpDecode)
	if !facts.frames && !decodes && (root.Kind == recipeir.NodeSelect || !branch.lone) {
		key := fmt.Sprintf("copy@%d", branch.root)
		if !s.rootCopiesOnly(branch.root, all) {
			key = fmt.Sprintf("copy@%d/%s", branch.root, branch.name)
		}
		stream.Operations = append(stream.Operations, s.insertedOperation(key, branch.root, OpCopy()))
	}
	for _, id := range ops {
		operation := g.Graph.Nodes[id].Op
		if operation.Kind == recipeir.OpName || operation.Kind == recipeir.OpBuffer {
			// A name and a queue are facts of the branch, not work.
			continue
		}
		operation.Node = id
		stream.Operations = append(stream.Operations, operation)
	}
	if buffer, ok := pathBuffer(g.Graph, ops); ok {
		stream.Buffer = buffer.Buffer
	}
	for _, dest := range branch.leaf.dests {
		facts.dests = append(facts.dests, s.destIndex[dest])
		stream.Outputs = append(stream.Outputs, recipeir.OutputRef(s.outputDestinationNames[s.destIndex[dest]]))
	}
	return stream, facts, nil
}

// rootCopiesOnly reports a packet root none of whose branches decode: its
// branches share one packet copy.
func (s *recipeState) rootCopiesOnly(root recipeir.NodeID, branches []derivedBranch) bool {
	for _, branch := range branches {
		if branch.root == root && s.req.Graph.HasOp(branch.leaf.nodes[1:], plan.OpDecode) {
			return false
		}
	}
	return true
}

// insertedOperation is an operation the planner adds, on a node of its own:
// the same key, from any branch, is the same node.
func (s *recipeState) insertedOperation(key string, parent recipeir.NodeID, operation recipeir.Operation) recipeir.Operation {
	id, ok := s.inserted[key]
	if !ok {
		id = recipeir.NodeID(len(s.req.Graph.Nodes))
		s.req.Graph.Nodes = append(s.req.Graph.Nodes, recipeir.Node{Kind: recipeir.NodeOp, Parents: []recipeir.NodeID{parent}, Op: operation})
		s.inserted[key] = id
	}
	operation.Node = id
	return operation
}

// markShared marks each operation whose node serves several branches.
func (s *recipeState) markShared() {
	count := map[recipeir.NodeID]int{}
	for i := range s.recipe.Streams {
		for _, operation := range s.recipe.Streams[i].Operations {
			count[operation.Node]++
		}
	}
	for i := range s.recipe.Streams {
		operations := s.recipe.Streams[i].Operations
		for j := range operations {
			operations[j].Shared = count[operations[j].Node] > 1
		}
	}
}

// pathBuffer is the queue a path declares: its last Buffer.
func pathBuffer(g recipeir.Graph, nodes []recipeir.NodeID) (recipeir.Operation, bool) {
	for i := len(nodes) - 1; i >= 0; i-- {
		if op := g.Nodes[nodes[i]].Op; op.Kind == recipeir.OpBuffer {
			return op, true
		}
	}
	return recipeir.Operation{}, false
}

// groupLeaves merges Outputs that route the same work: paths ending at one
// node, or differing only by As names after it, are one branch with several
// destinations.
func groupLeaves(g recipeir.Graph, paths []streamPath) []leafGroup {
	var groups []leafGroup
	byLeaf := map[recipeir.NodeID][]int{}
	for _, path := range paths {
		// Paths through the same work are one branch unless their names after
		// it differ: a different name makes another branch over the same
		// nodes, and an unnamed path joins the first branch over its work.
		leaf, name := workLeaf(g, path.nodes), trailingName(g, path.nodes)
		placed := false
		for _, i := range byLeaf[leaf] {
			groupName := trailingName(g, groups[i].nodes)
			if name != "" && groupName != "" && name != groupName {
				continue
			}
			if groupName == "" {
				groups[i].nodes = path.nodes
			}
			groups[i].dests = append(groups[i].dests, path.dests...)
			placed = true
			break
		}
		if !placed {
			byLeaf[leaf] = append(byLeaf[leaf], len(groups))
			groups = append(groups, leafGroup{nodes: path.nodes, dests: append([]recipeir.NodeID(nil), path.dests...)})
		}
	}
	return groups
}

// trailingName is the name a path's last As after its work gives it.
func trailingName(g recipeir.Graph, nodes []recipeir.NodeID) string {
	if last := len(nodes) - 1; last > 0 && g.Nodes[nodes[last]].Op.Kind == recipeir.OpName {
		return g.Nodes[nodes[last]].Op.Name
	}
	return ""
}

// workLeaf is the last node of a path that does work: trailing As names only
// label it.
func workLeaf(g recipeir.Graph, nodes []recipeir.NodeID) recipeir.NodeID {
	i := len(nodes) - 1
	for i > 0 && g.Nodes[nodes[i]].Op.Kind == recipeir.OpName {
		i--
	}
	return nodes[i]
}

// joinOutputFrames reports whether a join emits frames: Mix and Composite do;
// Select and custom joins forward their first input's domain unless the
// custom stage declares its output.
func joinOutputFrames(req *Request, joinID recipeir.NodeID) bool {
	node := req.Graph.Nodes[joinID]
	switch JoinKind(node.Join.Kind) {
	case JoinMix, JoinComposite:
		return true
	}
	if node.Join.Stage != nil {
		if profile, err := CustomJoinProfile(&CustomJoinSpec{Name: node.Join.Kind, Stage: node.Join.Stage}); err == nil && profile.DecodeArms {
			return true
		}
	}
	if len(node.Parents) == 0 {
		return true
	}
	return endsFrames(req, node.Parents[0])
}

// endsFrames reports whether a stream carries frames at its end.
func endsFrames(req *Request, id recipeir.NodeID) bool {
	nodes := req.Graph.Path(id)
	frames := true
	switch root := req.Graph.Nodes[nodes[0]]; root.Kind {
	case recipeir.NodeJoin:
		frames = joinOutputFrames(req, nodes[0])
	case recipeir.NodeSelect:
		frames = req.Inputs[root.Input].Frames
	case recipeir.NodeRule:
		frames = req.Inputs[req.Graph.Nodes[root.Parents[0]].Input].Frames
	}
	for _, n := range nodes[1:] {
		switch req.Graph.Nodes[n].Op.Kind {
		case plan.OpDecode:
			frames = true
		case plan.OpEncode:
			frames = false
		}
	}
	return frames
}

// streamBranchNames names the forks of a recipe: a fork is named by its last
// As(...), else by its first destination without the extension. A derived
// name is kept unique with a suffix; an As name is an address live controls
// use, so naming two forks alike is refused.
type streamBranchNames struct {
	counts   map[string]int
	explicit map[string]bool
	err      error
}

func newStreamBranchNames() *streamBranchNames {
	return &streamBranchNames{counts: map[string]int{}, explicit: map[string]bool{}}
}

func (n *streamBranchNames) next(req *Request, leaf leafGroup) string {
	name := ""
	for i := len(leaf.nodes) - 1; i >= 0 && name == ""; i-- {
		if op := req.Graph.Nodes[leaf.nodes[i]].Op; op.Kind == recipeir.OpName {
			name = op.Name
		}
	}
	if name != "" {
		if n.explicit[name] && n.err == nil {
			n.err = streamNameDuplicateError(name)
		}
		n.explicit[name] = true
	}
	if name == "" && len(leaf.dests) != 0 {
		label := req.Dests[req.Graph.Nodes[leaf.dests[0]].Output].Label
		name = strings.TrimSuffix(label, filepath.Ext(label))
	}
	if name == "" {
		name = "branch"
	}
	n.counts[name]++
	if n.counts[name] > 1 && !n.explicit[name] {
		name = fmt.Sprintf("%s-%d", name, n.counts[name])
	}
	return name
}

func streamNameDuplicateError(name string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StreamInvalidCode),
		Code:      builderr.StreamInvalidCode,
		Operation: "build recipe",
		Node:      name,
		Reason:    fmt.Sprintf("two forks are named %q", name),
		Fixes: builderr.Fixes([]string{
			"give each fork its own name: .As(\"hd\") and .As(\"sd\")",
			"route one fork to several destinations with .To(a, b) instead of naming two forks alike",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func StreamInvalidError(node string, reason string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StreamInvalidCode),
		Code:      builderr.StreamInvalidCode,
		Operation: "build recipe",
		Node:      node,
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"start each stream from an input or a join: input.Video(), input.Audio(), goav.Mix(a, b)",
			"route every stream with .To(destination) and pass the Outputs to rt.Run(ctx, outputs...)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func StreamSyncByPTSError() error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StreamInvalidCode),
		Code:      builderr.StreamInvalidCode,
		Operation: "build recipe",
		Reason:    "SyncByPTS aligns a Mix or Composite's inputs; call it on goav.Mix(...) or goav.Composite(...) output",
		Fixes:     builderr.Fixes([]string{"goav.Mix(a, b).SyncByPTS()", "goav.Composite(a, goav.At(b, x, y)).SyncByPTS()"}),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

// validateStreamOperations is the one validator over a branch's operations:
// it walks them from the root tracking the stream's domain and refuses an
// operation its stream cannot feed. It also fixes each tap's domain and
// position, which follow where the tap is.
func (s *recipeState) validateStreamOperations(index int) error {
	stream := &s.recipe.Streams[index]
	frames := s.facts[index].frames
	return validateBranchOperations(stream, frames, frames && stream.Input >= 0, s.nodeShared)
}

// validateBranchOperations is the one validator over a branch's operations:
// it walks them from the root tracking the stream's domain and refuses an
// operation its stream cannot feed. It also fixes each tap's domain and
// position, which follow where the tap is.
func validateBranchOperations(stream *recipeir.Stream, frames bool, frameSource bool, shared func(recipeir.NodeID) bool) error {
	encoded := false
	var encode codec.Spec
	decodeShared := false
	copied := false
	after := plan.OperationKind("")
	node := recipeIRStreamName(*stream)
	const operation = "build stream"
	frameStep := func(step string) error {
		if encoded {
			return chainStepAfterEncodeError(operation, node, step, encode)
		}
		if frames {
			return nil
		}
		if copied {
			return chainStepOnPacketCopyError(operation, node, step)
		}
		return chainFrameInputRequiredError(operation, node, step)
	}
	for i := range stream.Operations {
		op := &stream.Operations[i]
		switch op.Kind {
		case plan.OpCopy:
			copied = true
			after = plan.OpCopy
			continue
		case plan.OpDecode:
			switch {
			case frameSource:
				return frameSourceDecodeError(operation, node)
			case encoded:
				return chainStepAfterEncodeError(operation, node, "decode", encode)
			case frames && decodeShared:
				return branchDecodeDomainError(node)
			case frames:
				return duplicateBranchDecodeError(node)
			}
			frames = true
			decodeShared = shared(op.Node)
		case plan.OpEncode:
			switch {
			case op.Encode.Copy:
				return copyEncodeSpellingError(operation, node)
			case encoded:
				return duplicateStreamEncodeError(operation, node, encode, op.Encode)
			}
			if err := frameStep("encode"); err != nil {
				return err
			}
			encoded, encode, frames = true, op.Encode, false
		case plan.OpTransform:
			step := string(op.Transform.Kind)
			if err := frameStep(cmp.Or(step, "transform")); err != nil {
				return err
			}
		case plan.OpStage:
			if err := ValidateStageComponent(op.Stage); err != nil {
				return streamStageMissingError(node, err)
			}
			if stageRequiresFrameInput(op.Stage) {
				if err := frameStep("custom stage"); err != nil {
					return err
				}
			}
		case plan.OpShape:
			if !recipeIROperationIsAnnotation(*op) && encoded {
				return chainStepAfterEncodeError(operation, node, "shape", encode)
			}
			continue
		case plan.OpTap:
			if op.Tap.Name == "" {
				return tapNameEmptyError(operation, node)
			}
			op.Tap.MediaKind = FirstNonEmptyMedia(op.Tap.MediaKind, stream.Selector.Type)
			op.Tap.Domain = shape.DomainPacket
			if frames {
				op.Tap.Domain = shape.DomainFrame
			}
			op.Tap.After = after
			if after == "" && encoded {
				op.Tap.After = plan.OpEncode
			}
			continue
		case recipeir.OpBuffer:
			if err := ValidateBranchBuffer(op.Buffer, operation, node); err != nil {
				return err
			}
			continue
		case recipeir.OpName:
			continue
		}
		after = op.Kind
	}
	return validateStreamDestinations(*stream)
}

func (s *recipeState) nodeShared(id recipeir.NodeID) bool {
	count := 0
	for i := range s.recipe.Streams {
		for _, operation := range s.recipe.Streams[i].Operations {
			if operation.Node == id {
				count++
				break
			}
		}
	}
	return count > 1
}

// validateStreamDestinations refuses a branch that lists one destination
// twice.
func validateStreamDestinations(stream recipeir.Stream) error {
	seen := make(map[recipeir.OutputRef]int, len(stream.Outputs))
	for i, output := range stream.Outputs {
		if first, ok := seen[output]; ok {
			return duplicateBranchDestinationError(stream.Name, string(output), first, i)
		}
		seen[output] = i
	}
	return nil
}

func tapNameEmptyError(operation string, node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.TapInvalid),
		Code:      errcode.TapInvalid,
		Operation: operation,
		Node:      node,
		Reason:    "tap name is empty",
		Fixes: builderr.Fixes([]string{
			"call .Tap(\"video.decoded\") or another stable, non-empty tap name",
			"omit .Tap(...) when no runtime branch should attach at that point",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func streamStageMissingError(name string, cause error) error {
	if cause == nil {
		cause = builderr.ErrNilStage
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StageMissingCode),
		Code:      builderr.StageMissingCode,
		Operation: "build stream",
		Node:      name,
		Reason:    "custom stream stage is nil",
		Fixes: builderr.Fixes([]string{
			"pass a non-nil stage to .Do(stage)",
			"use component.FrameFunc, component.PacketFunc, or component.EventFunc for small hooks",
			"remove .Do(...) when no custom processing is needed",
		}),
		Cause: cause,
	}
}

func mixedStreamOutputError(operation string, name string, media av.MediaType) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.OutputKindMixedCode),
		Code:      builderr.OutputKindMixedCode,
		Operation: operation,
		Node:      name,
		Reason:    "stream recipes cannot mix sinks and muxed outputs",
		Fixes: builderr.Fixes([]string{
			"use .Decode().To(goav.Sink(...)) for decoded frames",
			"call " + builderr.EncodeHint(media) + " before .To(goav.Write(...)) for encoded output",
			"fork the decoded Stream for separate outputs: v.To(goav.Sink(...)), v.Encode(...).To(goav.Write(...))",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func streamEncodeMissingError(operation string, name string, media av.MediaType) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.EncodeMissingCode),
		Code:      builderr.EncodeMissingCode,
		Operation: operation,
		Node:      name,
		Reason:    "decoded frames cannot be written to a muxed output without an encoder",
		Details:   builderr.Details(builderr.KV("expected_shape", shape.New(shape.Domain(shape.DomainPacket), shape.Media(media)).String()), builderr.KV("actual_shape", shape.Frame(media).String())),
		Fixes: builderr.Fixes([]string{
			"call " + builderr.EncodeHint(media) + " before .To(goav.Write(...))",
			"send decoded frames to goav.Sink(...)",
			"leave out .Decode() to copy the packets: input.Video().To(output)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func duplicateBranchDestinationError(name string, target string, firstIndex int, secondIndex int) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationDuplicateCode),
		Code:      builderr.DestinationDuplicateCode,
		Operation: "build recipe",
		Node:      name,
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", target),
		Details:   builderr.Details(builderr.Note(fmt.Sprintf("first destination index: %d", firstIndex)), builderr.Note(fmt.Sprintf("second destination index: %d", secondIndex))),
		Fixes: builderr.Fixes([]string{
			"list each destination once in .To(...)",
			"route one branch to multiple destinations with distinct values such as .To(archive, preview)",
			"reuse one destination value instead of constructing the same name twice",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// DestinationNameClashError refuses two destinations one name labels apart:
// copies of one handle that disagree, or distinct handles that share a name.
func DestinationNameClashError(name string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationDuplicateCode),
		Code:      builderr.DestinationDuplicateCode,
		Operation: "build recipe",
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is defined more than once with different destination handles", name),
		Fixes: builderr.Fixes([]string{
			"pass the same destination value to every stream that should share one output",
			"give separate destinations distinct names, for a sink with goav.Name(...)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

type ComponentValidator interface {
	ValidateComponent() error
}

func ValidateStageComponent(stage pipeline.Stage) error {
	if stage == nil {
		return builderr.ErrNilStage
	}
	if validator, ok := stage.(ComponentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}
