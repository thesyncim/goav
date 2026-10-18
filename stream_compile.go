package goav

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// streamPath is one Output's chain: the nodes from its root to the Stream it
// routes, and where it goes.
type streamPath struct {
	nodes []*streamNode
	dests []Destination
}

func (p streamPath) root() *streamRoot { return p.nodes[0].root }

// compileOutputs lowers a value recipe onto the planner's job form. A Stream
// that fans out becomes a trunk and planned branches split where the paths
// part; outputs that route the same Stream merge into one multi-destination
// chain; a join's arms become its arm chains.
func compileOutputs(outs []Output) (*Job, error) {
	if len(outs) == 0 {
		return nil, streamInvalidError("", "a recipe needs at least one Output: route a stream with stream.To(destination)")
	}
	var remux []Output
	var paths []streamPath
	for i := range outs {
		out := outs[i]
		if len(out.dests) == 0 {
			return nil, streamInvalidError("", "an Output needs a destination: stream.To(goav.Write(name, writer)) or .To(goav.Sink(sink))")
		}
		if out.remux != nil {
			remux = append(remux, out)
			continue
		}
		nodes, err := streamNodes(out.stream)
		if err != nil {
			return nil, err
		}
		paths = append(paths, streamPath{nodes: nodes, dests: out.dests})
	}
	var join *streamRoot
	for _, path := range paths {
		if path.root().join == nil {
			continue
		}
		if join != nil && path.root() != join {
			return nil, streamBesideJoinRecipeError("a recipe runs one join today; route the second join's output in its own run")
		}
		join = path.root()
	}
	if join != nil {
		if len(remux) != 0 || len(paths) != countJoinPaths(paths, join) {
			return nil, streamBesideJoinRecipeError("a join's output runs alone today; run the other streams in their own run")
		}
		return compileJoinOutputs(join, paths)
	}
	if len(remux) != 0 {
		return compileRemuxOutputs(remux, paths)
	}
	return compileInputOutputs(paths)
}

func countJoinPaths(paths []streamPath, join *streamRoot) int {
	n := 0
	for _, path := range paths {
		if path.root() == join {
			n++
		}
	}
	return n
}

// streamNodes lists a Stream's nodes root first, surfacing the first
// construction error.
func streamNodes(s Stream) ([]*streamNode, error) {
	if s.node == nil {
		return nil, streamInvalidError("", "the stream is the zero Stream: start from an input (input.Video(), input.Audio()) or a join (goav.Mix(...))")
	}
	var nodes []*streamNode
	for n := s.node; n != nil; n = n.parent {
		if n.err != nil {
			return nil, n.err
		}
		nodes = append(nodes, n)
	}
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	if nodes[0].root == nil {
		return nil, streamInvalidError("", "the stream has no input")
	}
	return nodes, nil
}

// compileRemuxOutputs copies every stream of each input.To Output. Several
// inputs copy together when they share their destinations; one input may
// route to several Outputs.
func compileRemuxOutputs(remux []Output, paths []streamPath) (*Job, error) {
	if len(paths) != 0 {
		return nil, streamInvalidError("", "input.To(...) copies every stream; route single streams with input.Video().To(...) instead")
	}
	var inputs []InputSpec
	seen := map[*inputIdentity]bool{}
	var dests []Destination
	for i := range remux {
		if !seen[remux[i].remux.id] {
			seen[remux[i].remux.id] = true
			inputs = append(inputs, *remux[i].remux)
		}
		if len(inputs) == 1 {
			dests = append(dests, remux[i].dests...)
		}
	}
	if len(inputs) > 1 {
		dests = remux[0].dests
		for i := range remux {
			if !sameDestinations(remux[i].dests, dests) {
				return nil, streamInvalidError("", "inputs copied in one run share their destinations: a.To(out), b.To(out); route single streams with input.Video().To(...) otherwise")
			}
		}
	}
	return From(inputs...).Copy().To(dests...), nil
}

func sameDestinations(a, b []Destination) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].spec.id != b[i].spec.id {
			return false
		}
	}
	return true
}

// leafGroup is one distinct Stream routed by one or more Outputs.
type leafGroup struct {
	nodes []*streamNode
	dests []Destination
}

// groupLeaves merges Outputs that route the same work: paths ending at one
// node, or differing only by As names after it, are one chain with several
// destinations.
func groupLeaves(paths []streamPath) []leafGroup {
	var groups []leafGroup
	index := map[*streamNode]int{}
	for _, path := range paths {
		leaf := workLeaf(path.nodes)
		if i, ok := index[leaf]; ok {
			groups[i].dests = append(groups[i].dests, path.dests...)
			continue
		}
		index[leaf] = len(groups)
		groups = append(groups, leafGroup{nodes: path.nodes, dests: append([]Destination(nil), path.dests...)})
	}
	return groups
}

// commonPrefix is how many nodes every group shares from the root.
func commonPrefix(groups []leafGroup) int {
	n := len(groups[0].nodes)
	for _, group := range groups[1:] {
		i := 0
		for i < n && i < len(group.nodes) && group.nodes[i] == groups[0].nodes[i] {
			i++
		}
		n = i
	}
	return n
}

func compileInputOutputs(paths []streamPath) (*Job, error) {
	var inputs []InputSpec
	seen := map[*inputIdentity]bool{}
	var roots []*streamRoot
	byRoot := map[*streamRoot][]streamPath{}
	for _, path := range paths {
		root := path.root()
		if root.tap != "" || root.track != nil {
			return nil, streamInvalidError(root.tap, "a stream from task.Tap or task.Track attaches to that running task: task.Attach(ctx, outputs...)")
		}
		if root.input.id == nil {
			return nil, streamInvalidError("", "the stream's input was not constructed: use goav.FileInput, goav.Input, or goav.Source")
		}
		if !seen[root.input.id] {
			seen[root.input.id] = true
			inputs = append(inputs, root.input)
		}
		if _, ok := byRoot[root]; !ok {
			roots = append(roots, root)
		}
		byRoot[root] = append(byRoot[root], path)
	}
	job := From(inputs...)
	names := newStreamBranchNames()
	for _, root := range roots {
		if root.rule != nil {
			job.OnStream(*root.rule, streamBranchSpecs(byRoot[root], inputStartsFrameDomain(root.input), names, nil)...)
			continue
		}
		options := append([]StreamOption(nil), root.options...)
		if len(inputs) > 1 {
			options = append(options, InputName(root.input.inputName(root.input.graphSourceNodeName())))
		}
		var chain *Chain
		switch root.media {
		case "video":
			chain = job.Video(options...)
		case "audio":
			chain = job.Audio(options...)
		default:
			chain = job.Stream(options...)
		}
		frames := inputStartsFrameDomain(root.input)
		leaves := groupLeaves(byRoot[root])
		// A named stream runs as a fork so its name reaches the nodes and the
		// stream id controls address.
		if len(leaves) == 1 && !pathHasOp(leaves[0].nodes[1:], streamOpBuffer) && !pathHasOp(leaves[0].nodes[1:], streamOpName) {
			ops := leaves[0].nodes[1:]
			if !frames && !pathHasOp(ops, streamOpDecode) {
				chain.Copy()
			}
			applyChainOps(chain, ops, frames)
			chain.To(leaves[0].dests...)
			continue
		}
		shared := 1
		if len(leaves) > 1 {
			shared = commonPrefix(leaves)
		}
		trunk := leaves[0].nodes[1:shared]
		anyDecode := pathHasOp(trunk, streamOpDecode)
		allCopy := !frames && !anyDecode
		for _, leaf := range leaves {
			if pathHasOp(leaf.nodes[shared:], streamOpDecode) {
				allCopy = false
			}
		}
		if allCopy {
			chain.Copy()
		}
		trunkFrames := applyChainOps(chain, trunk, frames)
		branches := make([]BranchSpec, 0, len(leaves))
		for _, leaf := range leaves {
			suffix := leaf.nodes[shared:]
			branch := Branch(names.next(leaf))
			if !allCopy && !trunkFrames && !pathHasOp(suffix, streamOpDecode) {
				branch.Copy()
			}
			applyBranchOps(branch, suffix, trunkFrames)
			branches = append(branches, branch.To(leaf.dests...))
		}
		chain.Branches(branches...)
	}
	return job, names.err
}

func inputStartsFrameDomain(input InputSpec) bool {
	spec, ok := declaredSourceShape(input)
	return ok && spec.Domain == shape.DomainFrame
}

func pathHasOp(nodes []*streamNode, kind streamOpKind) bool {
	for _, n := range nodes {
		if n.op.kind == kind {
			return true
		}
	}
	return false
}

// applyChainOps records the operations on a chain and reports whether the
// stream carries frames after them.
func applyChainOps(chain *Chain, nodes []*streamNode, frames bool) bool {
	for _, n := range nodes {
		op := n.op
		switch op.kind {
		case streamOpDecode:
			chain.Decode(op.codecOptions...)
			frames = true
		case streamOpEncode:
			chain.Encode(op.encode)
			frames = false
		case streamOpResize:
			chain.Resize(op.width, op.height, op.resize...)
		case streamOpResample:
			chain.Resample(op.rate, op.channels, op.audio...)
		case streamOpDo:
			chain.Do(op.stages...)
		case streamOpPlayout:
			chain.Playout(op.playout)
		case streamOpSync:
			chain.Sync(op.sync)
		case streamOpTap:
			chain.Tap(streamTapRef(op.name, frames))
		case streamOpAuto:
			chain.Auto(op.policies...)
		case streamOpRequire:
			chain.Require(op.spec)
		case streamOpPrefer:
			chain.Prefer(op.spec)
		case streamOpShape:
			chain.Shape(op.spec)
		}
	}
	return frames
}

func applyBranchOps(branch *BranchBuilder, nodes []*streamNode, frames bool) {
	for _, n := range nodes {
		op := n.op
		switch op.kind {
		case streamOpDecode:
			branch.Decode(op.codecOptions...)
			frames = true
		case streamOpEncode:
			branch.Encode(op.encode)
			frames = false
		case streamOpResize:
			branch.Resize(op.width, op.height, op.resize...)
		case streamOpResample:
			branch.Resample(op.rate, op.channels, op.audio...)
		case streamOpDo:
			branch.Do(op.stages...)
		case streamOpPlayout:
			branch.Playout(op.playout)
		case streamOpSync:
			branch.Sync(op.sync)
		case streamOpTap:
			branch.Tap(streamTapRef(op.name, frames))
		case streamOpBuffer:
			branch.Buffer(op.buffer)
		case streamOpAuto:
			branch.Auto(op.policies...)
		case streamOpRequire:
			branch.Require(op.spec)
		case streamOpPrefer:
			branch.Prefer(op.spec)
		case streamOpShape:
			branch.Shape(op.spec)
		}
	}
}

func streamTapRef(name string, frames bool) tapRef {
	if frames {
		return FrameTap(name)
	}
	return PacketTap(name)
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

func (n *streamBranchNames) next(leaf leafGroup) string {
	name := ""
	for i := len(leaf.nodes) - 1; i >= 0 && name == ""; i-- {
		if leaf.nodes[i].op.kind == streamOpName {
			name = leaf.nodes[i].op.name
		}
	}
	if name != "" {
		if n.explicit[name] && n.err == nil {
			n.err = streamNameDuplicateError(name)
		}
		n.explicit[name] = true
	}
	if name == "" && len(leaf.dests) != 0 {
		label := leaf.dests[0].spec.label("")
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
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      name,
		Reason:    fmt.Sprintf("two forks are named %q", name),
		fixes: buildErrorFixes([]string{
			"give each fork its own name: .As(\"hd\") and .As(\"sd\")",
			"route one fork to several destinations with .To(a, b) instead of naming two forks alike",
		}),
		cause: errUnsupportedBuild,
	}
}

func compileJoinOutputs(root *streamRoot, paths []streamPath) (*Job, error) {
	join, err := legacyJoinFromRoot(root)
	if err != nil {
		return nil, err
	}
	leaves := groupLeaves(paths)
	shared := len(leaves[0].nodes)
	if len(leaves) > 1 {
		shared = commonPrefix(leaves)
	}
	trunk := leaves[0].nodes[1:shared]
	// A Select forwards its active arm to a sink as-is; anything else it feeds
	// runs in a planned branch.
	selectIntoMux := root.join.kind == joinSelect && len(leaves) == 1 && !allSinks(leaves[0].dests)
	if len(leaves) == 1 && joinStreamTakes(trunk) && !selectIntoMux {
		applyJoinOps(join, trunk)
		return join.To(leaves[0].dests...), nil
	}
	if !joinStreamTakes(trunk) || selectIntoMux {
		shared, trunk = 1, nil
	}
	applyJoinOps(join, trunk)
	names := newStreamBranchNames()
	branches := make([]BranchSpec, 0, len(leaves))
	for _, leaf := range leaves {
		branch := Branch(names.next(leaf))
		applyBranchOps(branch, leaf.nodes[shared:], true)
		branches = append(branches, branch.To(leaf.dests...))
	}
	if names.err != nil {
		return nil, names.err
	}
	return join.Branches(branches...), nil
}

// legacyJoin is a join builder: its output takes taps and routes to
// destinations or planned branches.
type legacyJoin interface {
	JoinArm
	Branches(...BranchSpec) *Job
	To(...Destination) *Job
}

// joinStreamTakes reports whether a join's own builder accepts ops directly;
// any other operation runs inside a planned branch.
func joinStreamTakes(nodes []*streamNode) bool {
	for _, n := range nodes {
		switch n.op.kind {
		case streamOpAuto, streamOpRequire, streamOpPrefer, streamOpEncode, streamOpTap, streamOpName:
		default:
			return false
		}
	}
	return true
}

func applyJoinOps(join legacyJoin, nodes []*streamNode) {
	for _, n := range nodes {
		op := n.op
		switch j := join.(type) {
		case *MixStream:
			switch op.kind {
			case streamOpAuto:
				j.Auto(op.policies...)
			case streamOpRequire:
				j.Require(op.spec)
			case streamOpPrefer:
				j.Prefer(op.spec)
			case streamOpEncode:
				j.Encode(op.encode)
			case streamOpTap:
				j.Tap(FrameTap(op.name))
			}
		case *CompositeStream:
			switch op.kind {
			case streamOpAuto:
				j.Auto(op.policies...)
			case streamOpRequire:
				j.Require(op.spec)
			case streamOpPrefer:
				j.Prefer(op.spec)
			case streamOpEncode:
				j.Encode(op.encode)
			case streamOpTap:
				j.Tap(FrameTap(op.name))
			}
		case *SelectStream:
			if op.kind == streamOpTap {
				j.Tap(FrameTap(op.name))
			}
		case *JoinStream:
			if op.kind == streamOpTap {
				j.Tap(FrameTap(op.name))
			}
		}
	}
}

// legacyJoinFromRoot builds a join and its arms: each arm Stream is a chain
// from its input (or a nested join), placed at its Layer position.
func legacyJoinFromRoot(root *streamRoot) (legacyJoin, error) {
	spec := root.join
	arms := make([]JoinArm, 0, len(spec.layers))
	for _, layer := range spec.layers {
		stream, x, y, placed := layer.compositeLayer()
		arm, err := legacyJoinArm(stream, placed, x, y)
		if err != nil {
			return nil, err
		}
		arms = append(arms, arm)
	}
	switch spec.kind {
	case joinMix:
		mix := LegacyMix(arms...)
		if spec.sync {
			mix.SyncByPTS()
		}
		return mix, nil
	case joinComposite:
		composite := LegacyComposite(arms...)
		if spec.sync {
			composite.SyncByPTS()
		}
		return composite, nil
	case joinSelect:
		if spec.sync {
			return nil, streamSyncByPTSError()
		}
		return LegacySelect(arms...), nil
	default:
		if spec.sync {
			return nil, streamSyncByPTSError()
		}
		return LegacyJoin(spec.name, spec.stage, arms...), nil
	}
}

func legacyJoinArm(stream Stream, placed bool, x, y int) (JoinArm, error) {
	nodes, err := streamNodes(stream)
	if err != nil {
		return nil, err
	}
	root := nodes[0].root
	ops := nodes[1:]
	if root.join != nil {
		join, err := legacyJoinFromRoot(root)
		if err != nil {
			return nil, err
		}
		applyJoinOps(join, ops)
		if placed {
			switch j := join.(type) {
			case *CompositeStream:
				j.Region(x, y)
			case *SelectStream:
				j.Region(x, y)
			}
		}
		return join, nil
	}
	var chain *Chain
	job := From(root.input)
	switch root.media {
	case "video":
		chain = job.Video(root.options...)
	case "audio":
		chain = job.Audio(root.options...)
	default:
		chain = job.Stream(root.options...)
	}
	applyChainOps(chain, ops, inputStartsFrameDomain(root.input))
	if placed {
		chain.Region(x, y)
	}
	return chain, nil
}

func streamInvalidError(node string, reason string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      node,
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			"start each stream from an input or a join: input.Video(), input.Audio(), goav.Mix(a, b)",
			"route every stream with .To(destination) and pass the Outputs to rt.Run(ctx, outputs...)",
		}),
		cause: errUnsupportedBuild,
	}
}

func streamSyncByPTSError() error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Reason:    "SyncByPTS aligns a Mix or Composite's inputs; call it on goav.Mix(...) or goav.Composite(...) output",
		fixes:     buildErrorFixes([]string{"goav.Mix(a, b).SyncByPTS()", "goav.Composite(a, goav.At(b, x, y)).SyncByPTS()"}),
		cause:     errUnsupportedBuild,
	}
}

func streamBesideJoinRecipeError(reason string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamBesideJoinCode),
		Code:      streamBesideJoinCode,
		Operation: "build recipe",
		Reason:    reason,
		fixes:     buildErrorFixes([]string{"run the join's Outputs and the other Outputs with separate rt.Run calls"}),
		cause:     errUnsupportedBuild,
	}
}

// Run builds the Outputs into one task and runs it to completion.
func (r *runtime) Run(ctx context.Context, outs ...Output) error {
	job, err := compileOutputs(outs)
	if err != nil {
		return err
	}
	return job.UseRuntime(r).Run(ctx)
}

// Build builds the Outputs into one task without starting it.
func (r *runtime) Build(ctx context.Context, outs ...Output) (LiveTask, error) {
	job, err := compileOutputs(outs)
	if err != nil {
		return nil, err
	}
	return job.UseRuntime(r).BuildLive(ctx)
}

// Explain reports the typed plan the Outputs build, without opening them.
func (r *runtime) Explain(ctx context.Context, outs ...Output) (plan.Report, error) {
	job, err := compileOutputs(outs)
	if err != nil {
		return plan.Report{}, err
	}
	return job.UseRuntime(r).Explain(ctx)
}

// Describe returns the graph the Outputs build, without opening them.
func (r *runtime) Describe(outs ...Output) (pipeline.Spec, error) {
	job, err := compileOutputs(outs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return job.UseRuntime(r).Describe()
}

// workLeaf is the last node of a path that does work: trailing As names only
// label it.
func workLeaf(nodes []*streamNode) *streamNode {
	i := len(nodes) - 1
	for i > 0 && nodes[i].op.kind == streamOpName {
		i--
	}
	return nodes[i]
}

// streamBranchSpecs lowers the Outputs of a stream that starts at a branch
// anchor (a discovered stream, a live tap) into one branch per distinct work
// leaf. A packet branch that never decodes copies.
func streamBranchSpecs(paths []streamPath, frames bool, names *streamBranchNames, from branchSource) []BranchSpec {
	leaves := groupLeaves(paths)
	branches := make([]BranchSpec, 0, len(leaves))
	for _, leaf := range leaves {
		ops := leaf.nodes[1:]
		branch := Branch(names.next(leaf))
		if from != nil {
			branch.From(from)
		}
		if !frames && !pathHasOp(ops, streamOpDecode) {
			branch.Copy()
		}
		applyBranchOps(branch, ops, frames)
		branches = append(branches, branch.To(leaf.dests...))
	}
	return branches
}

// Tap is the stream at one of the task's taps.
func (t *task) Tap(name string) Stream {
	return Stream{node: &streamNode{root: &streamRoot{tap: name}}}
}

// Track is a stream one of the task's inputs produces.
func (t *task) Track(input InputSpec, stream av.Stream) Stream {
	return Stream{node: &streamNode{root: &streamRoot{input: input, track: &stream}}}
}

// Attach adds Outputs fed from the task's taps and tracks to the running task.
func (t *task) Attach(ctx context.Context, outs ...Output) (Attachment, error) {
	branches, err := t.outputBranchSpecs(outs)
	if err != nil {
		return nil, err
	}
	return t.LegacyAttach(ctx, branches...)
}

// outputBranchSpecs lowers Outputs rooted at the task's taps and tracks onto
// runtime branches. A tap an earlier Output of the group declares is known by
// the domain at its position.
func (t *task) outputBranchSpecs(outs []Output) ([]BranchSpec, error) {
	if len(outs) == 0 {
		return nil, streamInvalidError("", "attach Outputs built from task.Tap(name) or task.Track(input, stream), each routed with .To(destination)")
	}
	taps := map[string]bool{}
	for _, tap := range t.Taps() {
		taps[tap.Name] = tap.Domain == shape.DomainFrame
	}
	names := newStreamBranchNames()
	var branches []BranchSpec
	for i := range outs {
		if outs[i].remux != nil || len(outs[i].dests) == 0 {
			return nil, streamInvalidError("", "attach Outputs built from task.Tap(name) or task.Track(input, stream), each routed with .To(destination)")
		}
		nodes, err := streamNodes(outs[i].stream)
		if err != nil {
			return nil, err
		}
		root := nodes[0].root
		path := []streamPath{{nodes: nodes, dests: outs[i].dests}}
		var frames bool
		var from branchSource
		switch {
		case root.tap != "":
			frames = taps[root.tap]
			from = streamTapRef(root.tap, frames)
		case root.track != nil:
			frames = inputStartsFrameDomain(root.input)
			from = root.input.Track(*root.track)
		default:
			return nil, streamInvalidError("", "a running task attaches streams from its taps and tracks: task.Tap(name), task.Track(input, stream)")
		}
		recordStreamTaps(nodes[1:], frames, taps)
		branches = append(branches, streamBranchSpecs(path, frames, names, from)...)
	}
	if names.err != nil {
		return nil, names.err
	}
	return branches, nil
}

// recordStreamTaps records the domain of each tap nodes declare.
func recordStreamTaps(nodes []*streamNode, frames bool, taps map[string]bool) {
	for _, n := range nodes {
		switch n.op.kind {
		case streamOpDecode:
			frames = true
		case streamOpEncode:
			frames = false
		case streamOpTap:
			taps[n.op.name] = frames
		}
	}
}

// rebranchArgs replaces each Output among args with the branches it attaches
// as, so Rebranch takes the Outputs Attach does.
func (t *task) rebranchArgs(args []lifecycle.RebranchArg) ([]lifecycle.RebranchArg, error) {
	var outs []Output
	mapped := make([]lifecycle.RebranchArg, 0, len(args))
	for _, arg := range args {
		if out, ok := arg.(Output); ok {
			outs = append(outs, out)
			continue
		}
		mapped = append(mapped, arg)
	}
	if len(outs) == 0 {
		return args, nil
	}
	branches, err := t.outputBranchSpecs(outs)
	if err != nil {
		return nil, err
	}
	for _, branch := range branches {
		mapped = append(mapped, branch)
	}
	return mapped, nil
}

// RebranchArg lets an Output ride the lifecycle.RebranchArg list
// Attachment.Rebranch accepts, as a replacement.
func (Output) RebranchArg() {}

func allSinks(dests []Destination) bool {
	for i := range dests {
		if dests[i].spec.sink == nil {
			return false
		}
	}
	return true
}
