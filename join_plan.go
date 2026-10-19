package goav

import (
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// joinArmStagePlan is one planned per-arm stage: the transform to lower and the
// arm stream the transform stage is constructed against.
type joinArmStagePlan struct {
	transform mediaTransform
	stream    av.Stream
}

// joinArmPlan is one planned arm: its input, the statically selected stream,
// and the planned per-arm nodes (optional decode, optional arm stage).
type joinArmPlan struct {
	// input indexes the recipe input a leaf arm reads; sourceShape is the
	// shape that input declares.
	input       int
	sourceShape shape.Spec
	inputName   string
	// sourceNode is the leaf arm's planned source node name, resolved across
	// every leaf in the join tree so repeated inputs stay disambiguated.
	sourceNode string
	stream     av.Stream
	domain     shape.MediaDomain
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
	sub *joinPlan
	// feed is the node a fed arm reads: a stream other work reads too, which
	// the arm takes from where the branch lowering planned it.
	feed recipeir.NodeID
	fed  bool
	// bind is set when the arm's streams are known only once its input opens
	// (a file, or a provider that declares none): the arm selects its stream
	// then and republishes it under its planned id.
	bind *joinBindArmPlan
	// conform converts the arm at run time to the join's format, for an arm
	// whose format, or whose join's format, is known only once inputs open.
	conform *joinConformPlan
	// late reports that the arm's format is known only once its input opens;
	// stream then carries the arm id and media kind alone.
	late bool
}

// armTapAnchorNode names the planned node a chain arm's taps anchor on: the
// arm's decode node when the arm decodes, otherwise its bind or source node.
func (a *joinArmPlan) armTapAnchorNode() string {
	bind := ""
	if a.bind != nil {
		bind = a.bind.node
	}
	return firstNonEmpty(a.decodeNode, bind, a.sourceNode)
}

// joinStream is the arm's stream where it meets the join: after its
// conversion when it conforms.
func (a *joinArmPlan) joinStream() av.Stream {
	if a.conform != nil {
		return conformedStream(a.stream, a.conform.target)
	}
	return a.stream
}

// joinPlan is one planned join: its N arms (each an input's stream, a nested
// join, or a fed stream, with its optional bind, decode, and conversion)
// converging into one join node whose output — the joined stream — the
// join's branches read like any stream.
type joinPlan struct {
	runtime *Runtime
	tree    *joinTreeSnapshot
	profile joinProfile
	name    string
	// node is the join's recipe graph node.
	node recipeir.NodeID

	arms         []joinArmPlan
	joined       av.Stream
	joinedDomain shape.MediaDomain
	// diagnostics records the arm conversions the shape solver inserted, so
	// Explain reports them exactly like chain insertions.
	diagnostics []plan.Diagnostic

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample planned by the mix planArm hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its planArm hook.
	layouts []compositeLayout
	// late reports that the joined format is known only once arm inputs open:
	// the build derives it from the streams its arms resolved. lateTarget
	// marks a join whose target follows a late first arm, and target holds
	// the facts the downstream encoder fixes for it.
	late       bool
	lateTarget bool
	target     shape.Spec
}

func joinArmError(name string, node string, reason string, suggestions ...string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(joinErrorCode(name, "arm")),
		Code:      joinErrorCode(name, "arm"),
		Operation: "build " + name,
		Node:      node,
		Reason:    reason,
		fixes:     buildErrorFixes(suggestions),
		cause:     errUnsupportedBuild,
	}
}

func validateJoinArmDomain(name string, arm string, profile joinProfile, domain shape.MediaDomain, operations []recipeir.Operation) error {
	if !profile.decodeArms || domain != shape.DomainPacket || operationsHaveDecode(operations) {
		return nil
	}
	return joinArmError(name, firstNonEmpty(arm, name),
		name+" arm is packet-domain; frame-consuming joins require explicit .Decode() on each packet arm",
		"write the arm as input.Audio().Decode() or input.Video().Decode() before passing it to the join",
		"for an already decoded source, declare a frame-domain source shape: goav.Source(name, shape.Frame(...), fn)")
}

// joinInputsError is the too-few-arms refusal, raised by the public sugar and
// again by the planner for nested joins.
func joinInputsError(kind string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(joinErrorCode(kind, "inputs")),
		Code:      joinErrorCode(kind, "inputs"),
		Operation: "build " + kind,
		Node:      node,
		Reason:    kind + " requires at least two source arms",
		fixes: buildErrorFixes([]string{
			"pass at least two arms: " + joinTwoArmExample(kind),
			"route the single stream directly when nothing converges",
		}),
		cause: errUnsupportedBuild,
	}
}

// joinTwoArmExample renders the minimal two-arm call for a join kind, used in
// suggestions.
func joinTwoArmExample(kind string) string {
	switch joinKind(kind) {
	case joinMix:
		return "goav.Mix(a.Audio(), b.Audio())"
	case joinComposite:
		return "goav.Composite(a.Video(), b.Video())"
	case joinSelect:
		return "goav.Select(a.Video(), b.Video())"
	default:
		return "goav.Join(" + strconv.Quote(kind) + ", stage, a.Audio(), b.Audio())"
	}
}

func explicitEncodeSoftInputShape(encode codec.Spec) shape.Spec {
	if encode.Copy || encode.ID == "" {
		return shape.Spec{}
	}
	parameters := encode.Parameters
	media := firstNonEmptyMedia(encode.Type, parameters.Type, codecMedia(encode.ID))
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
func (p *joinPlan) firstArmSourceShape() shape.Spec {
	if len(p.arms) == 0 {
		return shape.Spec{}
	}
	arm := p.arms[0]
	if arm.late || arm.fed {
		return shape.FromStream(arm.joinStream(), arm.domain)
	}
	if arm.sub != nil {
		return shape.FromStream(arm.sub.joined, arm.sub.joinedDomain)
	}
	return arm.sourceShape
}

// allTaps collects this join's taps, the taps its arm chains declared, and
// every nested join's taps — one flat list for the task: join-level taps
// anchor on their join node, arm taps on the arm's decode (or source) node.
func (p *joinPlan) allTaps() []workTap {
	var taps []workTap
	for i := range p.arms {
		arm := &p.arms[i]
		for _, tap := range arm.taps {
			taps = append(taps, workTap{
				Name:      tap.ref.name,
				Node:      pipeline.NodeRef(arm.armTapAnchorNode()),
				Domain:    tap.ref.domain,
				MediaKind: arm.stream.Type,
				After:     tap.after,
				Shape:     shape.FromStream(arm.stream, tap.ref.domain),
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
func (p *joinPlan) allDiagnostics() []plan.Diagnostic {
	diagnostics := append([]plan.Diagnostic(nil), p.diagnostics...)
	for i := range p.arms {
		if p.arms[i].sub != nil {
			diagnostics = append(diagnostics, p.arms[i].sub.allDiagnostics()...)
		}
	}
	return diagnostics
}

// solveArmConversion runs one arm through the shape solver: the profile's
// armExpected derives the target format, the implicit armPolicy stands in for
// a user .Auto(...), and an allowed delta plans a real per-arm transform stage
// (named <join>-<adapter>-<stream>, e.g. mix-resample-b) plus a diagnostic.
func (p *joinPlan) solveArmConversion(rt *Runtime, stream av.Stream, armName string) (*joinArmStagePlan, error) {
	expected := p.profile.armExpected(p, stream)
	if mediaShapeEmpty(expected) {
		return nil, nil
	}
	actual := shape.FromStream(stream, shape.DomainFrame)
	if actual.MediaKind == av.MediaAudio {
		actual.Channels = maxInt(actual.Channels, 1)
	}
	if shape.Conversions(actual, expected).Empty() {
		return nil, nil
	}
	conversion, planned, err := planShapeConversion(rt, actual, expected, rt.realtime)
	if err != nil {
		step := recipeir.Operation{Kind: plan.OpTransform, Component: p.name}
		return nil, recipeIRShapeSolverAdapterError("build "+p.name, firstNonEmpty(armName, string(stream.ID)), 0, step, actual, expected, err)
	}
	if !planned || !p.profile.armPolicy.Covers(conversion.needed) {
		return nil, joinArmError(p.name, firstNonEmpty(armName, string(stream.ID)),
			fmt.Sprintf("%s arm %q cannot be converted to the join format (%s -> %s)", p.name, firstNonEmpty(armName, string(stream.ID)), humanizeShape(actual), humanizeShape(expected)),
			"feed the arm "+humanizeShape(expected)+" media, or align its source format with the first arm (the join's format reference)")
	}
	p.diagnostics = append(p.diagnostics, plan.Diagnostic{
		Code: diagnosticShapeConversionInserted,
		Node: firstNonEmpty(armName, string(stream.ID)),
		Message: fmt.Sprintf("inserted %s on %s arm %q (join arm policy)",
			conversion.detail, p.name, firstNonEmpty(armName, string(stream.ID))),
		Details: []string{
			"adapter=" + conversion.factory,
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + expected.String(),
		},
	})
	return &joinArmStagePlan{
		transform: mediaTransform{
			name:    p.name + "-" + conversion.factory + "-" + string(stream.ID),
			factory: conversion.factory,
			video:   transformResize(conversion.transform),
			audio:   transformResample(conversion.transform),
		},
		stream: joinArmTransformStream(stream, actual.MediaKind),
	}, nil
}

// joinArmTransformStream is the input stream description the per-arm transform
// stage opens against: the arm's selected stream with audio facts normalized
// (at least one channel, S16 when the arm declared no sample format).
func joinArmTransformStream(stream av.Stream, media av.MediaType) av.Stream {
	if media != av.MediaAudio {
		return stream
	}
	return av.Stream{
		ID:   stream.ID,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   stream.Codec.SampleRate,
			Channels:     maxInt(stream.Codec.Channels, 1),
			SampleFormat: firstNonEmpty(stream.Codec.SampleFormat, av.SampleFormatS16),
			ClockRate:    uint32(stream.Codec.SampleRate),
		},
	}
}

// joinNodeDetail renders the join node's planned graph detail: built-in kinds
// share joinSyncNodeDetail with their stages' DescribeNode; a custom join's
// planned node carries whatever the caller's stage describes, so Describe() ≡
// Build() holds for external convergence stages too.
func (p *joinPlan) joinNodeDetail() string {
	if p.tree.custom != nil {
		return describedNodeDetail(p.tree.custom.stage)
	}
	return joinSyncNodeDetail(p.tree.sync)
}

// deriveJoinedDomain reports the media domain of the join's output:
// frame-consuming kinds always converge frames; passthrough kinds forward the
// first arm's domain unchanged (frames when the arm chain declared .Decode()).
// A custom join whose stage contract declares an output domain wins outright.
func (p *joinPlan) deriveJoinedDomain() shape.MediaDomain {
	if domain := p.customJoinedOutputDomain(); domain != "" {
		return domain
	}
	if p.profile.decodeArms {
		return shape.DomainFrame
	}
	if len(p.arms) != 0 {
		if p.arms[0].decodeNode != "" {
			return shape.DomainFrame
		}
		if p.arms[0].domain != "" {
			return p.arms[0].domain
		}
	}
	return shape.DomainFrame
}

// treeGraphBuffer returns the first profile-pinned graph buffer in the join
// tree that applies at this realtime setting: a nested Select still needs the
// buffered per-node workers its control plane injects through, even under an
// offline root whose own realtime-only pin (Mix/Composite) does not apply.
func (p *joinPlan) treeGraphBuffer(realtime bool) *pipeline.BufferPolicy {
	if p.profile.graphBuffer != nil && (!p.profile.graphBufferRealtimeOnly || realtime) {
		return p.profile.graphBuffer
	}
	for i := range p.arms {
		if p.arms[i].sub == nil {
			continue
		}
		if buffer := p.arms[i].sub.treeGraphBuffer(realtime); buffer != nil {
			return buffer
		}
	}
	return nil
}

// --- work plan ---

// appendJoinArmWork renders this join's arms into the work IR: one branch per
// leaf arm carrying its decode/transform operations, and — for nested arms —
// the sub-join's own arm branches (recursively) plus one branch anchored on
// the sub's plan.OpJoin node carrying the outer join's per-arm operations. It
// returns the shape of the first arm at the join input (the OpJoin ShapeIn).
func (p *joinPlan) appendJoinArmWork(operations *[]workOperation, branches *[]workBranch, base int) shape.Spec {
	armShape := normalizeTapShape(shape.FromStream(p.joined, p.joinedDomain))
	for i := range p.arms {
		arm := p.arms[i]
		branchName := firstNonEmpty(arm.inputName, fmt.Sprintf("arm-%d", i))
		current := normalizeTapShape(shape.FromStream(arm.stream, arm.domain))
		sourceShape := current
		index := 0
		var ops []string
		appendOperation := func(operation workOperation) {
			*operations = append(*operations, operation)
			ops = append(ops, operation.ID)
			index++
		}
		if arm.sub != nil {
			subArmShape := arm.sub.appendJoinArmWork(operations, branches, base)
			joinDetail := "converge " + strconv.Itoa(len(arm.sub.arms)) + " arms"
			if arm.sub.tree.sync == joinSyncPTS {
				joinDetail += " by pts"
			}
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpJoin),
				Name:      arm.sub.name,
				Kind:      plan.OpJoin,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.sub.name),
				Component: arm.sub.name,
				Detail:    joinDetail,
				ShapeIn:   subArmShape,
				ShapeOut:  current,
			})
		}
		if arm.bind != nil {
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpSelect),
				Name:      arm.bind.node,
				Kind:      plan.OpSelect,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.bind.node),
				Component: selectorComponent(streamSelectFromAV(arm.bind.selector)),
				Detail:    "select stream when the input opens",
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if arm.decodeNode != "" {
			out := current
			out.Domain = shape.DomainFrame
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpDecode),
				Name:      arm.decodeNode,
				Kind:      plan.OpDecode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.decodeNode),
				Component: codecComponent(arm.stream.Codec.ID),
				Detail:    "packets to frames",
				Codec:     codecSpecFromStream(arm.stream),
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		for _, tap := range arm.taps {
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpTap),
				Name:      tap.ref.name,
				Kind:      plan.OpTap,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.armTapAnchorNode()),
				Component: tap.ref.name,
				Detail:    "named media outlet",
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if arm.stage != nil {
			out := shape.Merge(current, mediaShapeFromMediaTransform(arm.stage.transform))
			out.Domain = shape.DomainFrame
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpTransform),
				Name:      arm.stage.transform.name,
				Kind:      plan.OpTransform,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.stage.transform.name),
				Component: firstNonEmpty(arm.stage.transform.factory, arm.stage.transform.name, "transform"),
				Detail:    "transform frames",
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		if arm.conform != nil {
			out := normalizeTapShape(shape.FromStream(conformedStream(arm.stream, arm.conform.target), shape.DomainFrame))
			appendOperation(workOperation{
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
		selected := streamSelectFromStream(arm.stream)
		if arm.bind != nil {
			selected = streamSelectFromAV(arm.bind.selector)
		}
		*branches = append(*branches, workBranch{
			ID:          workBranchID(branchName, base+len(*branches)),
			Name:        branchName,
			Input:       firstNonEmpty(arm.inputName, branchName),
			Stream:      selected,
			SourceShape: sourceShape,
			Operations:  ops,
		})
	}
	return armShape
}

// joinArmWork renders the join's arms into the work plan, nested joins
// first; armShape is the first arm's shape where it meets the join.
func (p *joinPlan) joinArmWork(base int) (operations []workOperation, branches []workBranch, armShape shape.Spec) {
	armShape = p.appendJoinArmWork(&operations, &branches, base)
	return operations, branches, armShape
}
