package compile

import (
	"cmp"
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// joinArmStagePlan is one planned per-arm stage: the transform to lower and the
// arm stream the transform stage is constructed against.
type joinArmStagePlan struct {
	transform MediaTransform
	stream    av.Stream
}

// joinArmPlan is one planned arm: its input, the statically selected stream,
// and the planned per-arm nodes (optional decode, optional arm stage).
type JoinArmPlan struct {
	// input indexes the recipe input a leaf arm reads; sourceShape is the
	// shape that input declares.
	Input       int
	SourceShape shape.Spec
	inputName   string
	// sourceNode is the leaf arm's planned source node name, resolved across
	// every leaf in the join tree so repeated inputs stay disambiguated.
	sourceNode string
	Stream     av.Stream
	Domain     shape.MediaDomain
	// decodeNode is the planned decode node name; empty means the arm feeds the
	// join directly (frame arms, passthrough kinds).
	decodeNode string
	// decodeGraph is the arm's decode node in the recipe graph.
	decodeGraph    recipeir.NodeID
	hasDecodeGraph bool
	stage          *joinArmStagePlan
	// taps are the frame-domain taps the arm chain declared (.Tap after the
	// arm's decode point), installed on the task and resolvable by tap arms.
	taps []joinArmTap
	// sub is set when the arm is itself a join: the nested join's plan. Its
	// joined output stands in for stream/domain above, and its join node is
	// the arm's upstream in the graph.
	sub *JoinPlan
	// feed is the node a fed arm reads: a stream other work reads too, which
	// the arm takes from where the branch lowering planned it.
	feed recipeir.NodeID
	fed  bool
	// bind is set when the arm's streams are known only once its input opens
	// (a file, or a provider that declares none): the arm selects its stream
	// then and republishes it under its planned id.
	Bind *joinBindArmPlan
	// conform converts the arm at run time to the join's format, for an arm
	// whose format, or whose join's format, is known only once inputs open.
	conform *joinConformPlan
	// late reports that the arm's format is known only once its input opens;
	// stream then carries the arm id and media kind alone.
	late bool
}

// armTapAnchorNode names the planned node a chain arm's taps anchor on: the
// arm's decode node when the arm decodes, otherwise its bind or source node.
func (a *JoinArmPlan) armTapAnchorNode() string {
	bind := ""
	if a.Bind != nil {
		bind = a.Bind.node
	}
	return cmp.Or(a.decodeNode, bind, a.sourceNode)
}

// joinStream is the arm's stream where it meets the join: after its
// conversion when it conforms.
func (a *JoinArmPlan) joinStream() av.Stream {
	if a.conform != nil {
		return ConformedStream(a.Stream, a.conform.target)
	}
	return a.Stream
}

// joinPlan is one planned join: its N arms (each an input's stream, a nested
// join, or a fed stream, with its optional bind, decode, and conversion)
// converging into one join node whose output — the joined stream — the
// join's branches read like any stream.
type JoinPlan struct {
	env     Env
	Tree    *JoinTreeSnapshot
	Profile joinProfile
	Name    string
	// node is the join's recipe graph node.
	node recipeir.NodeID

	Arms         []JoinArmPlan
	Joined       av.Stream
	joinedDomain shape.MediaDomain
	// diagnostics records the arm conversions the shape solver inserted, so
	// Explain reports them exactly like chain insertions.
	diagnostics []plan.Diagnostic

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample planned by the mix planArm hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its planArm hook.
	Layouts []CompositeLayout
	// late reports that the joined format is known only once arm inputs open:
	// the build derives it from the streams its arms resolved. lateTarget
	// marks a join whose target follows a late first arm, and target holds
	// the facts the downstream encoder fixes for it.
	Late       bool
	lateTarget bool
	target     shape.Spec
}

func JoinArmError(name string, node string, reason string, suggestions ...string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(joinErrorCode(name, "arm")),
		Code:      joinErrorCode(name, "arm"),
		Operation: "build " + name,
		Node:      node,
		Reason:    reason,
		Fixes:     builderr.Fixes(suggestions),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

func validateJoinArmDomain(name string, arm string, profile joinProfile, domain shape.MediaDomain, operations []recipeir.Operation) error {
	if !profile.DecodeArms || domain != shape.DomainPacket || OperationsHaveDecode(operations) {
		return nil
	}
	return JoinArmError(name, cmp.Or(arm, name),
		name+" arm is packet-domain; frame-consuming joins require explicit .Decode() on each packet arm",
		"write the arm as input.Audio().Decode() or input.Video().Decode() before passing it to the join",
		"for an already decoded source, declare a frame-domain source shape: goav.Source(name, shape.Frame(...), fn)")
}

// joinInputsError is the too-few-arms refusal, raised by the public sugar and
// again by the planner for nested joins.
func joinInputsError(kind string, node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(joinErrorCode(kind, "inputs")),
		Code:      joinErrorCode(kind, "inputs"),
		Operation: "build " + kind,
		Node:      node,
		Reason:    kind + " requires at least two source arms",
		Fixes: builderr.Fixes([]string{
			"pass at least two arms: " + joinTwoArmExample(kind),
			"route the single stream directly when nothing converges",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// joinTwoArmExample renders the minimal two-arm call for a join kind, used in
// suggestions.
func joinTwoArmExample(kind string) string {
	switch JoinKind(kind) {
	case JoinMix:
		return "goav.Mix(a.Audio(), b.Audio())"
	case JoinComposite:
		return "goav.Composite(a.Video(), b.Video())"
	case JoinSelect:
		return "goav.Select(a.Video(), b.Video())"
	default:
		return "goav.Join(" + strconv.Quote(kind) + ", stage, a.Audio(), b.Audio())"
	}
}

func ExplicitEncodeSoftInputShape(encode codec.Spec) shape.Spec {
	if encode.Copy || encode.ID == "" {
		return shape.Spec{}
	}
	parameters := encode.Parameters
	media := FirstNonEmptyMedia(encode.Type, parameters.Type, CodecMedia(encode.ID))
	var out shape.Spec
	switch media {
	case av.MediaAudio:
		if encode.Settings.SampleRateSet {
			out.SampleRate = parameters.SampleRate
		}
		if encode.Settings.ChannelsSet {
			out.Channels = parameters.Channels
		}
		if parameters.SampleFormat != "" {
			out.SampleFormat = parameters.SampleFormat
		}
		if out.SampleRate == 0 && out.Channels == 0 && out.SampleFormat == "" {
			return shape.Spec{}
		}
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaAudio
	case av.MediaVideo:
		if parameters.Width != 0 {
			out.Width = parameters.Width
		}
		if parameters.Height != 0 {
			out.Height = parameters.Height
		}
		if parameters.PixelFormat != "" {
			out.PixelFormat = parameters.PixelFormat
		}
		if out.Width == 0 && out.Height == 0 && out.PixelFormat == "" {
			return shape.Spec{}
		}
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaVideo
	default:
		return shape.Spec{}
	}
	return out
}

// claimJoinName claims a unique node name for one join in the tree: the first
// join of a kind keeps the kind name (mix), later ones get an index suffix
// (mix-2, mix-3, ...) — the root claims first, so a lone join keeps its
// historical name.
func claimJoinName(used map[string]struct{}, kind string) string {
	name := kind
	for n := 2; ; n++ {
		if _, ok := used[name]; !ok {
			used[name] = struct{}{}
			return name
		}
		name = kind + "-" + strconv.Itoa(n)
	}
}

// firstArmSourceShape derives the format facts of the first arm — the join's
// format reference: a leaf arm reports its declared custom-source shape, a
// nested arm reports its sub-join's joined stream.
func (p *JoinPlan) firstArmSourceShape() shape.Spec {
	if len(p.Arms) == 0 {
		return shape.Spec{}
	}
	arm := p.Arms[0]
	if arm.late || arm.fed {
		return shape.FromStream(arm.joinStream(), arm.Domain)
	}
	if arm.sub != nil {
		return shape.FromStream(arm.sub.Joined, arm.sub.joinedDomain)
	}
	return arm.SourceShape
}

// allTaps collects this join's taps, the taps its arm chains declared, and
// every nested join's taps — one flat list for the task: join-level taps
// anchor on their join node, arm taps on the arm's decode (or source) node.
func (p *JoinPlan) allTaps() []WorkTap {
	var taps []WorkTap
	for i := range p.Arms {
		arm := &p.Arms[i]
		for _, tap := range arm.taps {
			taps = append(taps, WorkTap{
				Name:      tap.ref.Name,
				Node:      pipeline.NodeRef(arm.armTapAnchorNode()),
				Domain:    tap.ref.Domain,
				MediaKind: arm.Stream.Type,
				After:     tap.after,
				Shape:     shape.FromStream(arm.Stream, tap.ref.Domain),
				Shared:    true,
			})
		}
		if arm.sub != nil {
			taps = append(taps, arm.sub.allTaps()...)
		}
	}
	return taps
}

// allDiagnostics collects this join's solver diagnostics and every nested
// join's, so Explain reports conversions inserted anywhere in the tree.
func (p *JoinPlan) allDiagnostics() []plan.Diagnostic {
	diagnostics := append([]plan.Diagnostic(nil), p.diagnostics...)
	for i := range p.Arms {
		if p.Arms[i].sub != nil {
			diagnostics = append(diagnostics, p.Arms[i].sub.allDiagnostics()...)
		}
	}
	return diagnostics
}

// solveArmConversion runs one arm through the shape solver: the profile's
// armExpected derives the target format, the implicit armPolicy stands in for
// a user .Auto(...), and an allowed delta plans a real per-arm transform stage
// (named <join>-<adapter>-<stream>, e.g. mix-resample-b) plus a diagnostic.
func (p *JoinPlan) solveArmConversion(env Env, stream av.Stream, armName string) (*joinArmStagePlan, error) {
	expected := p.Profile.armExpected(p, stream)
	if MediaShapeEmpty(expected) {
		return nil, nil
	}
	actual := shape.FromStream(stream, shape.DomainFrame)
	if actual.MediaKind == av.MediaAudio {
		actual.Channels = MaxInt(actual.Channels, 1)
	}
	if shape.Conversions(actual, expected).Empty() {
		return nil, nil
	}
	conversion, planned, err := PlanShapeConversion(env, actual, expected, env.Realtime)
	if err != nil {
		step := recipeir.Operation{Kind: plan.OpTransform, Component: p.Name}
		return nil, recipeIRShapeSolverAdapterError("build "+p.Name, cmp.Or(armName, string(stream.ID)), 0, step, actual, expected, err)
	}
	if !planned || !p.Profile.armPolicy.Covers(conversion.needed) {
		return nil, JoinArmError(p.Name, cmp.Or(armName, string(stream.ID)),
			fmt.Sprintf("%s arm %q cannot be converted to the join format (%s -> %s)", p.Name, cmp.Or(armName, string(stream.ID)), HumanizeShape(actual), HumanizeShape(expected)),
			"feed the arm "+HumanizeShape(expected)+" media, or align its source format with the first arm (the join's format reference)")
	}
	p.diagnostics = append(p.diagnostics, plan.Diagnostic{
		Code: DiagnosticShapeConversionInserted,
		Node: cmp.Or(armName, string(stream.ID)),
		Message: fmt.Sprintf("inserted %s on %s arm %q (join arm policy)",
			conversion.detail, p.Name, cmp.Or(armName, string(stream.ID))),
		Details: []string{
			"adapter=" + conversion.Factory,
			"source=" + HumanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + expected.String(),
		},
	})
	return &joinArmStagePlan{
		transform: MediaTransform{
			Name:    p.Name + "-" + conversion.Factory + "-" + string(stream.ID),
			Factory: conversion.Factory,
			Video:   TransformResize(conversion.Transform),
			Audio:   TransformResample(conversion.Transform),
		},
		stream: JoinArmTransformStream(stream, actual.MediaKind),
	}, nil
}

// joinArmTransformStream is the input stream description the per-arm transform
// stage opens against: the arm's selected stream with audio facts normalized
// (at least one channel, S16 when the arm declared no sample format).
func JoinArmTransformStream(stream av.Stream, media av.MediaType) av.Stream {
	if media != av.MediaAudio {
		return stream
	}
	return av.Stream{
		ID:   stream.ID,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   stream.Codec.SampleRate,
			Channels:     MaxInt(stream.Codec.Channels, 1),
			SampleFormat: cmp.Or(stream.Codec.SampleFormat, av.SampleFormatS16),
			ClockRate:    uint32(stream.Codec.SampleRate),
		},
	}
}

// joinNodeDetail renders the join node's planned graph detail: built-in kinds
// share joinSyncNodeDetail with their stages' DescribeNode; a custom join's
// planned node carries whatever the caller's stage describes, so Describe() ≡
// Build() holds for external convergence stages too.
func (p *JoinPlan) joinNodeDetail() string {
	if p.Tree.Custom != nil {
		return DescribedNodeDetail(p.Tree.Custom.Stage)
	}
	return JoinSyncNodeDetail(p.Tree.Sync)
}

// deriveJoinedDomain reports the media domain of the join's output:
// frame-consuming kinds always converge frames; passthrough kinds forward the
// first arm's domain unchanged (frames when the arm chain declared .Decode()).
// A custom join whose stage contract declares an output domain wins outright.
func (p *JoinPlan) deriveJoinedDomain() shape.MediaDomain {
	if domain := p.CustomJoinedOutputDomain(); domain != "" {
		return domain
	}
	if p.Profile.DecodeArms {
		return shape.DomainFrame
	}
	if len(p.Arms) != 0 {
		if p.Arms[0].decodeNode != "" {
			return shape.DomainFrame
		}
		if p.Arms[0].Domain != "" {
			return p.Arms[0].Domain
		}
	}
	return shape.DomainFrame
}

// treeGraphBuffer returns the first profile-pinned graph buffer in the join
// tree that applies at this realtime setting: a nested Select still needs the
// buffered per-node workers its control plane injects through, even under an
// offline root whose own realtime-only pin (Mix/Composite) does not apply.
func (p *JoinPlan) TreeGraphBuffer(realtime bool) *pipeline.BufferPolicy {
	if p.Profile.graphBuffer != nil && (!p.Profile.graphBufferRealtimeOnly || realtime) {
		return p.Profile.graphBuffer
	}
	for i := range p.Arms {
		if p.Arms[i].sub == nil {
			continue
		}
		if buffer := p.Arms[i].sub.TreeGraphBuffer(realtime); buffer != nil {
			return buffer
		}
	}
	return nil
}

// appendJoinArmWork renders this join's arms into the work IR: one branch per
// leaf arm carrying its decode/transform operations, and — for nested arms —
// the sub-join's own arm branches (recursively) plus one branch anchored on
// the sub's plan.OpJoin node carrying the outer join's per-arm operations. It
// returns the shape of the first arm at the join input (the OpJoin ShapeIn).
func (p *JoinPlan) appendJoinArmWork(operations *[]WorkOperation, branches *[]WorkBranch, base int) shape.Spec {
	armShape := NormalizeTapShape(shape.FromStream(p.Joined, p.joinedDomain))
	for i := range p.Arms {
		arm := p.Arms[i]
		branchName := cmp.Or(arm.inputName, fmt.Sprintf("arm-%d", i))
		current := NormalizeTapShape(shape.FromStream(arm.Stream, arm.Domain))
		sourceShape := current
		index := 0
		var ops []string
		appendOperation := func(operation WorkOperation) {
			*operations = append(*operations, operation)
			ops = append(ops, operation.ID)
			index++
		}
		if arm.sub != nil {
			subArmShape := arm.sub.appendJoinArmWork(operations, branches, base)
			joinDetail := "converge " + strconv.Itoa(len(arm.sub.Arms)) + " arms"
			if arm.sub.Tree.Sync == JoinSyncPTS {
				joinDetail += " by pts"
			}
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpJoin),
				Name:      arm.sub.Name,
				Kind:      plan.OpJoin,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.sub.Name),
				Component: arm.sub.Name,
				Detail:    joinDetail,
				ShapeIn:   subArmShape,
				ShapeOut:  current,
			})
		}
		if arm.Bind != nil {
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpSelect),
				Name:      arm.Bind.node,
				Kind:      plan.OpSelect,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.Bind.node),
				Component: selectorComponent(StreamSelectFromAV(arm.Bind.Selector)),
				Detail:    "select stream when the input opens",
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if arm.decodeNode != "" {
			out := current
			out.Domain = shape.DomainFrame
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpDecode),
				Name:      arm.decodeNode,
				Kind:      plan.OpDecode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.decodeNode),
				Component: codecComponent(arm.Stream.Codec.ID),
				Detail:    "packets to frames",
				Codec:     codecSpecFromStream(arm.Stream),
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		for _, tap := range arm.taps {
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpTap),
				Name:      tap.ref.Name,
				Kind:      plan.OpTap,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.armTapAnchorNode()),
				Component: tap.ref.Name,
				Detail:    "named media outlet",
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if arm.stage != nil {
			out := shape.Merge(current, mediaShapeFromMediaTransform(arm.stage.transform))
			out.Domain = shape.DomainFrame
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpTransform),
				Name:      arm.stage.transform.Name,
				Kind:      plan.OpTransform,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.stage.transform.Name),
				Component: cmp.Or(arm.stage.transform.Factory, arm.stage.transform.Name, "transform"),
				Detail:    "transform frames",
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		if arm.conform != nil {
			out := NormalizeTapShape(shape.FromStream(ConformedStream(arm.Stream, arm.conform.target), shape.DomainFrame))
			appendOperation(WorkOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpTransform),
				Name:      arm.conform.node,
				Kind:      plan.OpTransform,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.conform.node),
				Component: arm.conform.adapter,
				Detail:    "convert frames to the join format as they arrive",
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		if i == 0 {
			armShape = current
		}
		selected := StreamSelectFromStream(arm.Stream)
		if arm.Bind != nil {
			selected = StreamSelectFromAV(arm.Bind.Selector)
		}
		*branches = append(*branches, WorkBranch{
			ID:          workBranchID(branchName, base+len(*branches)),
			Name:        branchName,
			Input:       cmp.Or(arm.inputName, branchName),
			Stream:      selected,
			SourceShape: sourceShape,
			Operations:  ops,
		})
	}
	return armShape
}

// joinArmWork renders the join's arms into the work plan, nested joins
// first; armShape is the first arm's shape where it meets the join.
func (p *JoinPlan) joinArmWork(base int) (operations []WorkOperation, branches []WorkBranch, armShape shape.Spec) {
	armShape = p.appendJoinArmWork(&operations, &branches, base)
	return operations, branches, armShape
}
