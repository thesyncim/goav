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
func compileOutputs(outs []Output) (*recipeJob, error) {
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
func compileRemuxOutputs(remux []Output, paths []streamPath) (*recipeJob, error) {
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
	return recipeFrom(inputs...).Copy().To(dests...), nil
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

func compileInputOutputs(paths []streamPath) (*recipeJob, error) {
	var inputs []InputSpec
	seen := map[*inputIdentity]bool{}
	var roots []*streamRoot
	byRoot := map[*streamRoot][]streamPath{}
	for _, path := range paths {
		root := path.root()
		if root.tap != "" || root.track != nil || root.route != nil {
			return nil, streamInvalidError(root.tap, "a stream from task.Tap, task.Track, or task.Route attaches to that running task: task.Attach(ctx, outputs...)")
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
	job := recipeFrom(inputs...)
	names := newStreamBranchNames()
	for _, root := range roots {
		if root.rule != nil {
			job.OnStream(*root.rule, streamBranchSpecs(byRoot[root], inputStartsFrameDomain(root.input), names, nil)...)
			continue
		}
		options := append([]StreamOption(nil), root.options...)
		if len(inputs) > 1 {
			options = append(options, selectInputName(root.input.inputName(root.input.graphSourceNodeName())))
		}
		var chain *streamChain
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
		branches := make([]branchRecipe, 0, len(leaves))
		for _, leaf := range leaves {
			suffix := leaf.nodes[shared:]
			branch := newBranch(names.next(leaf))
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
func applyChainOps(chain *streamChain, nodes []*streamNode, frames bool) bool {
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

func applyBranchOps(branch *branchBuilder, nodes []*streamNode, frames bool) {
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
		return frameTapRef(name)
	}
	return packetTapRef(name)
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

func compileJoinOutputs(root *streamRoot, paths []streamPath) (*recipeJob, error) {
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
	// The join's own builder takes the leading operations it supports; the
	// rest, and every fork, runs in planned branches. A Select reaches a muxed
	// destination only through a branch.
	taken := joinTakes(root.join.kind, trunk)
	if root.join.kind == joinSelect && !allSinks(leaves[0].dests) {
		taken = 0
	}
	// A join's own encoder ends its stream: it is taken only when nothing
	// follows it; otherwise the encoder runs in the planned branch.
	if len(leaves) > 1 || taken < len(trunk) {
		for i := range trunk[:taken] {
			if trunk[i].op.kind == streamOpEncode {
				taken = i
				break
			}
		}
	}
	frames := applyJoinOps(join, trunk[:taken], joinOutputFrames(root.join))
	if len(leaves) == 1 && taken == len(trunk) && (root.join.kind != joinSelect || allSinks(leaves[0].dests)) {
		return join.To(leaves[0].dests...), nil
	}
	names := newStreamBranchNames()
	branches := make([]branchRecipe, 0, len(leaves))
	for _, leaf := range leaves {
		if pathHasOp(leaf.nodes[1+taken:], streamOpTap) {
			return nil, streamInvalidError("", "a join's output takes a tap only right after the join, before its other operations: goav.Mix(a, b).Tap(name).Encode(...)")
		}
		branch := newBranch(names.next(leaf))
		applyBranchOps(branch, leaf.nodes[1+taken:], frames)
		branches = append(branches, branch.To(leaf.dests...))
	}
	if names.err != nil {
		return nil, names.err
	}
	return join.Branches(branches...), nil
}

// joinBuilder is a join builder: its output takes taps and routes to
// destinations or planned branches.
type joinBuilder interface {
	recipeArm
	Branches(...branchRecipe) *recipeJob
	To(...Destination) *recipeJob
}

// joinTakes counts the leading operations a join's own builder applies: Mix
// and Composite take shape policies, frame taps, and their encoder; Select
// and custom joins take taps. A name, and anything after what the builder
// takes, runs in a planned branch.
func joinTakes(kind joinKind, nodes []*streamNode) int {
	encoded := false
	for i, n := range nodes {
		switch n.op.kind {
		case streamOpTap:
			if encoded {
				return i
			}
		case streamOpAuto, streamOpRequire, streamOpPrefer:
			if encoded || (kind != joinMix && kind != joinComposite) {
				return i
			}
		case streamOpEncode:
			if encoded || (kind != joinMix && kind != joinComposite) {
				return i
			}
			encoded = true
		default:
			return i
		}
	}
	return len(nodes)
}

// applyJoinOps applies operations joinTakes accepted and reports whether the
// join's output still carries frames after them.
func applyJoinOps(join joinBuilder, nodes []*streamNode, frames bool) bool {
	for _, n := range nodes {
		op := n.op
		if op.kind == streamOpEncode {
			frames = false
		}
		switch j := join.(type) {
		case *mixStream:
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
				j.Tap(streamTapRef(op.name, frames))
			}
		case *compositeStream:
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
				j.Tap(streamTapRef(op.name, frames))
			}
		case *legacySelectStream:
			if op.kind == streamOpTap {
				j.Tap(streamTapRef(op.name, frames))
			}
		case *joinStreamBuilder:
			if op.kind == streamOpTap {
				j.Tap(streamTapRef(op.name, frames))
			}
		}
	}
	return frames
}

// joinOutputFrames reports whether a join emits frames: Mix and Composite do;
// Select and custom joins forward their first input's domain.
func joinOutputFrames(join *streamJoin) bool {
	if join.kind == joinMix || join.kind == joinComposite || len(join.layers) == 0 {
		return true
	}
	stream, _, _, _ := join.layers[0].compositeLayer()
	return streamEndsFrames(stream)
}

// streamEndsFrames reports whether a stream carries frames at its end.
func streamEndsFrames(s Stream) bool {
	nodes, err := streamNodes(s)
	if err != nil {
		return true
	}
	root := nodes[0].root
	frames := true
	if root.join != nil {
		frames = joinOutputFrames(root.join)
	} else if root.tap == "" && root.track == nil && root.route == nil {
		frames = inputStartsFrameDomain(root.input)
	}
	for _, n := range nodes[1:] {
		switch n.op.kind {
		case streamOpDecode:
			frames = true
		case streamOpEncode:
			frames = false
		}
	}
	return frames
}

// legacyJoinFromRoot builds a join and its arms: each arm Stream is a chain
// from its input (or a nested join), placed at its Layer position.
func legacyJoinFromRoot(root *streamRoot) (joinBuilder, error) {
	spec := root.join
	arms := make([]recipeArm, 0, len(spec.layers))
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
		mix := legacyMix(arms...)
		if spec.sync {
			mix.SyncByPTS()
		}
		return mix, nil
	case joinComposite:
		composite := legacyComposite(arms...)
		if spec.sync {
			composite.SyncByPTS()
		}
		return composite, nil
	case joinSelect:
		if spec.sync {
			return nil, streamSyncByPTSError()
		}
		return legacySelect(arms...), nil
	default:
		if spec.sync {
			return nil, streamSyncByPTSError()
		}
		return legacyJoin(spec.name, spec.stage, arms...), nil
	}
}

func legacyJoinArm(stream Stream, placed bool, x, y int) (recipeArm, error) {
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
		var armOps []*streamNode
		for _, n := range ops {
			if n.op.kind != streamOpName {
				armOps = append(armOps, n)
			}
		}
		if joinTakes(root.join.kind, armOps) < len(armOps) {
			return nil, streamInvalidError("", "a join used as another join's input takes only .Tap(...), and for Mix and Composite .Auto, .Require, .Prefer, and .Encode; apply other operations to its inputs or after the outer join")
		}
		applyJoinOps(join, armOps, joinOutputFrames(root.join))
		if placed {
			switch j := join.(type) {
			case *compositeStream:
				j.Region(x, y)
			case *legacySelectStream:
				j.Region(x, y)
			}
		}
		return join, nil
	}
	var chain *streamChain
	job := recipeFrom(root.input)
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
func streamBranchSpecs(paths []streamPath, frames bool, names *streamBranchNames, from branchSource) []branchRecipe {
	leaves := groupLeaves(paths)
	branches := make([]branchRecipe, 0, len(leaves))
	for _, leaf := range leaves {
		ops := leaf.nodes[1:]
		branch := newBranch(names.next(leaf))
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

// Route is the stream a graph route carries.
func (t *task) Route(route pipeline.Route) Stream {
	return Stream{node: &streamNode{root: &streamRoot{route: &route}}}
}

// routeAnchor anchors an attached branch on a graph route.
type routeAnchor pipeline.Route

func (r routeAnchor) Name() string { return r.From }

func (r routeAnchor) Route() pipeline.Route { return pipeline.Route(r) }

// Attach adds Outputs fed from the task's taps and tracks to the running task.
func (t *task) Attach(ctx context.Context, outs ...Output) (Attachment, error) {
	branches, err := t.outputBranchSpecs(outs)
	if err != nil {
		return nil, err
	}
	return t.attachBranches(ctx, branches...)
}

// outputBranchSpecs lowers Outputs rooted at the task's taps and tracks onto
// runtime branches. A tap an earlier Output of the group declares is known by
// the domain at its position.
func (t *task) outputBranchSpecs(outs []Output) ([]branchRecipe, error) {
	if len(outs) == 0 {
		return nil, streamInvalidError("", "attach Outputs built from task.Tap(name) or task.Track(input, stream), each routed with .To(destination)")
	}
	taps := map[string]bool{}
	for _, tap := range t.Taps() {
		taps[tap.Name] = tap.Domain == shape.DomainFrame
	}
	names := newStreamBranchNames()
	var branches []branchRecipe
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
		case root.route != nil:
			// A graph route's domain is the graph's: no packet copy is implied.
			frames = true
			from = routeAnchor(*root.route)
		default:
			return nil, streamInvalidError("", "a running task attaches streams from its taps, tracks, and graph routes: task.Tap(name), task.Track(input, stream), task.Route(route)")
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
