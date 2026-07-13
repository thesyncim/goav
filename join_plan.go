package goav

import (
	"context"
	"fmt"
	"sort"
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
	input     InputSpec
	inputName string
	// sourceNode is the leaf arm's planned source node name, resolved across
	// every leaf in the join tree so repeated inputs stay disambiguated.
	sourceNode string
	stream     av.Stream
	domain     shape.MediaDomain
	// decodeNode is the planned decode node name; empty means the arm feeds the
	// join directly (frame arms, passthrough kinds).
	decodeNode string
	stage      *joinArmStagePlan
	// taps are the frame-domain taps the arm chain declared (.Tap after the
	// arm's decode point), installed on the task and resolvable by tap arms.
	taps []joinArmTap
	// sub is set when the arm is itself a join: the nested join's plan. Its
	// joined output stands in for stream/domain above, and its join node is
	// the arm's upstream in the graph.
	sub *joinPlan
	// tapArm is set when the arm is a tap reference: no source opens — the
	// arm's upstream is the resolved tap's planned node, and a restamp stage
	// re-publishes the tapped stream under the tap name as the arm's id.
	tapArm *joinTapArmPlan
}

// armTapAnchorNode names the planned node a chain arm's taps anchor on: the
// arm's decode node when the arm decodes, otherwise its source node.
func (a *joinArmPlan) armTapAnchorNode() string {
	return firstNonEmpty(a.decodeNode, a.sourceNode)
}

// joinPlan is the planned multi-upstream join: N arm sub-chains (input +
// optional decode + arm stage) converging into ONE join node, with the
// downstream chain (encode/taps/branches/destination) hanging off it exactly
// like a chain hangs off any node. It is a normal graphPlanLowerer: spec()
// emits the planned nodes/edges, lower() executes them against the graph, and
// buildJoinWorkPlan renders the same plan into the workPlan IR — so Describe()
// and Build() come from the one plan.
type joinPlan struct {
	runtime *runtime
	tree    *joinTreeSnapshot
	profile joinProfile
	name    string

	arms         []joinArmPlan
	joined       av.Stream
	joinedDomain shape.MediaDomain
	taps         []workTap
	// diagnostics records the arm conversions the shape solver inserted, so
	// Explain reports them exactly like chain insertions.
	diagnostics []plan.Diagnostic

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample planned by the mix planArm hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its planArm hook.
	layouts []compositeLayout

	// direct destination delivery (optionally through one encoder): each
	// destination receives the joined stream, exactly like a chain's
	// multi-destination .To(...) fanout.
	downstream    []joinDownstreamStagePlan
	encode        *encodeRequest
	encodeConfig  codec.EncodeConfig
	encodedStream av.Stream
	destinations  []destinationSpec

	// planned branch fanout off the joined stream.
	branchRoutes  []branchComposeRoute
	branchTargets []branchComposeTargetRoute
}

type joinDownstreamStagePlan struct {
	transform mediaTransform
	stream    av.Stream
}

type joinPlanInput struct {
	runtime   *runtime
	operation string
	intent    intent
	tree      *joinTreeSnapshot
	sets      []inputStreamSet
}

func joinPlanInputFromCompileState(state *recipeCompileState) joinPlanInput {
	if state == nil {
		return joinPlanInput{}
	}
	return joinPlanInput{
		runtime:   state.runtime,
		operation: state.operation,
		intent:    state.intent,
		tree:      cloneJoinTreeSnapshot(state.joinTree),
		sets:      jobInputStreamSetsFromRecipeIR(state.intent.Inputs, state.inputFacts, state.inputProbes),
	}
}

type joinWorkPlanInput struct {
	inputs        []inputIntent
	destinations  []destinationIntent
	outputFormats map[string]av.FormatID
}

func joinTreeSnapshotFromSpec(spec *joinSpec) *joinTreeSnapshot {
	if spec == nil {
		return nil
	}
	tree := &joinTreeSnapshot{
		kind:       spec.kind,
		dests:      append([]Destination(nil), spec.dests...),
		operations: cloneOperationSpecs(spec.operations),
		taps:       append([]tapRef(nil), spec.taps...),
		branches:   joinBranchSnapshotsFromSpecs(spec.branches, joinProfiles[spec.kind].media),
		sync:       spec.sync,
		custom:     spec.custom,
	}
	if spec.encode != nil {
		encode := cloneCodecSpec(*spec.encode)
		tree.encode = &encode
	}
	for i := range spec.arms {
		tree.arms = append(tree.arms, joinArmSnapshotFromArm(spec.arms[i]))
	}
	return tree
}

func joinArmSnapshotFromArm(arm JoinArm) joinArmSnapshot {
	if arm == nil {
		return joinArmSnapshot{}
	}
	resolved := arm.joinArm()
	out := joinArmSnapshot{
		chainInput:      resolved.chainInput,
		chainInputOK:    resolved.chainInputOK,
		chainErr:        resolved.chainErr,
		chainOperations: cloneOperationSpecs(resolved.chainOperations),
	}
	if resolved.join != nil {
		out.join = joinTreeSnapshotFromSpec(resolved.join)
	}
	if resolved.tap != nil {
		tap := *resolved.tap
		out.tap = &tap
	}
	if resolved.region != nil {
		region := *resolved.region
		out.region = &region
	}
	return out
}

func cloneJoinTreeSnapshot(tree *joinTreeSnapshot) *joinTreeSnapshot {
	if tree == nil {
		return nil
	}
	out := &joinTreeSnapshot{
		kind:       tree.kind,
		dests:      append([]Destination(nil), tree.dests...),
		operations: cloneOperationSpecs(tree.operations),
		taps:       append([]tapRef(nil), tree.taps...),
		branches:   cloneJoinBranchSnapshots(tree.branches),
		sync:       tree.sync,
		custom:     tree.custom,
	}
	if tree.encode != nil {
		encode := cloneCodecSpec(*tree.encode)
		out.encode = &encode
	}
	if len(tree.arms) != 0 {
		out.arms = make([]joinArmSnapshot, len(tree.arms))
		for i := range tree.arms {
			out.arms[i] = cloneJoinArmSnapshot(tree.arms[i])
		}
	}
	return out
}

func cloneJoinArmSnapshot(arm joinArmSnapshot) joinArmSnapshot {
	out := joinArmSnapshot{
		chainInput:      arm.chainInput,
		chainInputOK:    arm.chainInputOK,
		chainErr:        arm.chainErr,
		chainOperations: cloneOperationSpecs(arm.chainOperations),
		join:            cloneJoinTreeSnapshot(arm.join),
	}
	if arm.tap != nil {
		tap := *arm.tap
		out.tap = &tap
	}
	if arm.region != nil {
		region := *arm.region
		out.region = &region
	}
	return out
}

func joinWorkPlanInputFromCompileState(state *recipeCompileState) joinWorkPlanInput {
	if state == nil {
		return joinWorkPlanInput{}
	}
	return joinWorkPlanInput{
		inputs:        append([]inputIntent(nil), state.intent.Inputs...),
		destinations:  append([]destinationIntent(nil), state.intent.Destinations...),
		outputFormats: cloneOutputFormatMap(state.outputFormatMap()),
	}
}

func cloneOutputFormatMap(formats map[string]av.FormatID) map[string]av.FormatID {
	if len(formats) == 0 {
		return nil
	}
	out := make(map[string]av.FormatID, len(formats))
	for name, formatID := range formats {
		out[name] = formatID
	}
	return out
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

func validateJoinArmDomain(name string, arm string, profile joinProfile, domain shape.MediaDomain, operations []operationSpec) error {
	if !profile.decodeArms || domain != shape.DomainPacket || chainHasDecode(operations) {
		return nil
	}
	return joinArmError(name, firstNonEmpty(arm, name),
		name+" arm is packet-domain; frame-consuming joins require explicit .Decode() on each packet arm",
		"write the arm as goav.From(input).Audio().Decode() or goav.From(input).Video().Decode() before passing it to the join",
		"for an already decoded source, declare a frame-domain source shape or pass a goav.FrameTap(...) arm")
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
			"route the single chain directly when nothing converges",
		}),
		cause: errUnsupportedBuild,
	}
}

// joinTwoArmExample renders the minimal two-arm call for a join kind, used in
// suggestions.
func joinTwoArmExample(kind string) string {
	switch joinKind(kind) {
	case joinMix:
		return "goav.Mix(goav.From(a).Audio(), goav.From(b).Audio())"
	case joinComposite:
		return "goav.Composite(goav.From(a).Video(), goav.From(b).Video())"
	case joinSelect:
		return "goav.Select(goav.From(a).Video(), goav.From(b).Video())"
	default:
		return "goav.Join(" + strconv.Quote(kind) + ", stage, goav.From(a).Audio(), goav.From(b).Audio())"
	}
}

// newJoinPlan plans a captured join tree from the compile state: the join tree is
// planned recursively (an arm is a source chain or a nested join), each arm is
// validated and resolved statically (custom-source shapes, probes, live codec
// intent, per-kind arm stages and solver conversions), and the downstream
// chain (taps + branches or encode/destination) is planned against the root's
// joined stream — all before any source opens.
func newJoinPlan(input joinPlanInput) (*joinPlan, error) {
	tree := input.tree
	if tree == nil {
		return nil, nilRecipeError(input.operation, "nil join")
	}
	name := string(tree.kind)
	anchors := newJoinTapAnchors(declaredJoinTapNames(tree))
	p, _, err := planJoinTree(input, tree, 0, make(map[string]struct{}), anchors)
	if err != nil {
		return nil, err
	}
	p.assignArmSourceNames()
	profile := p.profile
	switch {
	case len(tree.branches) != 0:
		if err := p.planJoinBranches(); err != nil {
			return nil, err
		}
	case tree.encode != nil:
		if err := p.planJoinEncode(); err != nil {
			return nil, err
		}
		destinations, err := resolveJoinDestinations(name, tree)
		if err != nil {
			return nil, err
		}
		p.destinations = destinations
	default:
		destinations, err := resolveJoinDestinations(name, tree)
		if err != nil {
			return nil, err
		}
		for i := range destinations {
			if destinations[i].sink == nil {
				return nil, &BuildError{
					Phase:     phaseBuild,
					Family:    errcode.FamilyForCode(joinErrorCode(name, "destination")),
					Code:      joinErrorCode(name, "destination"),
					Operation: "build " + name,
					Node:      name,
					Reason:    profile.sinkOnlyReason,
					fixes:     buildErrorFixes(append([]string(nil), profile.sinkOnlySuggestions...)),
					cause:     errUnsupportedBuild,
				}
			}
		}
		p.destinations = destinations
	}
	return p, nil
}

// planJoinEncode solves joined-stream annotations against the terminal encoder
// and materializes any inserted conversion operations as real stages between
// the join node and encode node.
func (p *joinPlan) planJoinEncode() error {
	if p == nil || p.tree == nil || p.tree.encode == nil {
		return nil
	}
	operations := cloneOperationSpecs(p.tree.operations)
	operations = append(operations, operationSpecForEncode(*p.tree.encode))
	operations, err := p.solveJoinedOperations(p.name, operations)
	if err != nil {
		return err
	}
	operationFacts := operationFactsFromSpecs(operations)
	stream := p.joined
	transformIndex := 0
	for i := range operations {
		operation := operations[i]
		if operationSpecIsAnnotation(operation) || operation.Kind == plan.OpTap {
			continue
		}
		switch operation.Kind {
		case plan.OpTransform, plan.OpStage:
			transform, err := branchComposeRouteOperationTransform(p.name, transformIndex, operationFacts, i)
			if err != nil {
				return err
			}
			if operation.Kind == plan.OpTransform && (operation.Transform.resize != nil || operation.Transform.resample != nil) {
				transformIndex++
			}
			if mediaTransformEmpty(transform) {
				continue
			}
			p.downstream = append(p.downstream, joinDownstreamStagePlan{transform: transform, stream: stream})
			stream, err = applyMediaTransformToStream(stream, transform)
			if err != nil {
				return err
			}
		case plan.OpEncode:
			request := encodeRequest{name: p.name + "-encode", selector: av.StreamSelector{Type: p.profile.media}, config: encodeConfigFromSpec(operation.Encode)}
			config, encodedStream, err := prepareEncodeConfig(stream, request, runtimeRealtime(p.runtime))
			if err != nil {
				return err
			}
			p.encode = &request
			p.encodeConfig = config
			p.encodedStream = encodedStream
			return nil
		default:
			return branchChainStepError(p.name, "joined output operation is unsupported before encode")
		}
	}
	return encodeTargetMissingError(encodeRequest{name: p.name + "-encode", selector: av.StreamSelector{Type: p.profile.media}}, stream)
}

func (p *joinPlan) solveJoinedOperations(node string, operations []operationSpec) ([]operationSpec, error) {
	rt := p.runtime
	intent := streamIntent{
		Name:       node,
		Select:     streamSelectFromStream(p.joined),
		Operations: cloneOperationSpecs(operations),
	}
	initial := normalizeTapShape(shape.FromStream(p.joined, p.joinedDomain))
	if err := p.validateJoinedExplicitSoftInputShapes(node, intent.Operations, initial); err != nil {
		return nil, err
	}
	solved, diagnostics, err := solveOperationSpecShapes("build "+p.name, rt, intent, initial)
	if err != nil {
		return nil, err
	}
	p.diagnostics = append(p.diagnostics, diagnostics...)
	if solved != nil {
		return solved, nil
	}
	return cloneOperationSpecs(operations), nil
}

func (p *joinPlan) validateJoinedExplicitSoftInputShapes(node string, operations []operationSpec, initial shape.Spec) error {
	if _, active := chainAutoPolicy(operations); active {
		return nil
	}
	current := normalizeTapShape(initial)
	if current.MediaKind == "" {
		current.MediaKind = p.joined.Type
	}
	if current.Codec == "" {
		current.Codec = p.joined.Codec.ID
	}
	for i := range operations {
		operation := operations[i]
		expected := explicitEncodeSoftInputShape(operation)
		if !mediaShapeEmpty(expected) {
			set := shape.Set{expected}
			if !set.Accepts(current) {
				err := operationShapeFailureError("build "+p.name, node, i, operation, set, current)
				if target, fixable := shapeConversionTargetFromSet(current, set); fixable {
					err = appendAutoFixSuggestions(err, current, target, operation)
				}
				return err
			}
		}
		current = operationSpecOutputShape(current, operation)
	}
	return nil
}

func explicitEncodeSoftInputShape(operation operationSpec) shape.Spec {
	if operation.Kind != plan.OpEncode || operation.Encode.Copy || operation.Encode.ID == "" {
		return shape.Spec{}
	}
	parameters := operation.Encode.Parameters
	media := firstNonEmptyMedia(operation.Encode.Type, parameters.Type, codecMedia(operation.Encode.ID))
	var out shape.Spec
	switch media {
	case av.MediaAudio:
		if operation.Encode.Settings.SampleRateSet {
			out.SampleRate = parameters.SampleRate
		}
		if operation.Encode.Settings.ChannelsSet {
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

// resolveJoinDestinations resolves a join's .To(...) destinations into specs
// with the chain rules: at least one destination, and the same duplicate
// detection multi-destination chain fanout applies (one handle listed twice is
// the same refusal).
func resolveJoinDestinations(name string, tree *joinTreeSnapshot) ([]destinationSpec, error) {
	if len(tree.dests) == 0 {
		return nil, &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(outputMissingCode),
			Code:      outputMissingCode,
			Operation: "build " + name,
			Node:      name,
			Reason:    "no output is configured",
			fixes: buildErrorFixes([]string{
				"route the join to one or more destinations: .To(goav.Sink(sink))",
				"fan the joined stream out with .Branches(...) when branches need their own chains",
			}),
			cause: errUnsupportedBuild,
		}
	}
	outputs := make([]destinationSpec, 0, len(tree.dests))
	for i := range tree.dests {
		outputs = append(outputs, cloneDestinationSpec(tree.dests[i].spec))
	}
	if err := validateDestinationSpecs("build "+name, outputs); err != nil {
		return nil, err
	}
	return outputs, nil
}

// planJoinTree plans one join node and its arms, recursing when an arm is
// itself a join. sets are the per-leaf input stream sets, flattened across the
// whole tree in depth-first arm order (the order joinLeafInputSpecs walks);
// cursor is the next unconsumed leaf and the updated cursor is returned. Join
// node names are claimed depth-first through used, so nested joins of the same
// kind get disambiguated names (mix, mix-2, ...). anchors collects the taps
// declared by already-planned arms, so a tap-reference arm resolves strictly to an
// earlier point of the same tree — forward references and cycles are
// unrepresentable by construction.
func planJoinTree(input joinPlanInput, tree *joinTreeSnapshot, cursor int, used map[string]struct{}, anchors *joinTapAnchors) (*joinPlan, int, error) {
	kind := string(tree.kind)
	profile, err := resolveJoinProfile(tree)
	if err != nil {
		return nil, 0, err
	}
	name := claimJoinName(used, kind)
	if tree.custom != nil && name != kind {
		return nil, 0, customJoinNameCollisionError(kind, name)
	}
	p := &joinPlan{runtime: input.runtime, tree: tree, profile: profile, name: name}
	if len(tree.arms) < 2 {
		return nil, 0, joinInputsError(kind, name)
	}
	seen := make(map[av.StreamID]struct{}, len(tree.arms))
	for i := range tree.arms {
		armSpec := tree.arms[i]
		var armPlan joinArmPlan
		switch {
		case armSpec.join != nil:
			// A nested join arm contributes its JOINED output stream under its
			// output id; the sub-join's node becomes the arm's upstream.
			if err := validateNestedJoinArm(name, armSpec.join); err != nil {
				return nil, 0, err
			}
			sub, next, err := planJoinTree(input, armSpec.join, cursor, used, anchors)
			if err != nil {
				return nil, 0, err
			}
			cursor = next
			if profile.media != "" && sub.joined.Type != profile.media {
				return nil, 0, joinArmError(name, sub.name,
					fmt.Sprintf("%s arm %q produces %s, want %s", kind, sub.name, sub.joined.Type, profile.media),
					"feed the "+kind+" arms that produce "+string(profile.media)+" media (mix joins audio, composite joins video)")
			}
			armPlan = joinArmPlan{sub: sub, inputName: sub.name, stream: sub.joined, domain: sub.joinedDomain}
		case armSpec.tap != nil:
			// A tap arm converges an already-flowing stream: it anchors on the
			// tap's planned node — declared by an earlier arm of the tree — and
			// re-stamps the tapped media under the tap name as the arm's id.
			tapPlan, err := planJoinTapArm(name, kind, profile, *armSpec.tap, anchors)
			if err != nil {
				return nil, 0, err
			}
			armPlan = tapPlan
		case armSpec.chainInputOK:
			if err := armSpec.chainErr; err != nil {
				return nil, 0, err
			}
			if cursor >= len(input.sets) || !input.sets[cursor].known || len(input.sets[cursor].streams) == 0 {
				return nil, 0, recipeGraphUnsupportedError(input.operation, input.intent)
			}
			stream, err := selectStream(input.sets[cursor].streams, av.StreamSelector{Type: profile.media})
			if err != nil {
				return nil, 0, err
			}
			armPlan = joinArmPlan{
				input:     armSpec.chainInput,
				inputName: input.sets[cursor].name,
				stream:    stream,
				domain:    input.sets[cursor].domain,
			}
			armOps := cloneOperationSpecs(armSpec.chainOperations)
			if err := validateJoinArmOperations(name, armPlan.inputName, armOps); err != nil {
				return nil, 0, err
			}
			taps, err := joinChainArmTaps(name, armPlan.inputName, armOps)
			if err != nil {
				return nil, 0, err
			}
			armPlan.taps = taps
			cursor++
		default:
			return nil, 0, joinArmError(name, name,
				"each "+kind+" arm must be a single-input source chain, a tap declared by an earlier arm, or a nested join",
				"pass goav.From(input).Audio()/.Video() chains, goav.FrameTap/PacketTap refs, or nested goav.Mix/Composite/Select joins as arms")
		}
		if _, dup := seen[armPlan.stream.ID]; dup {
			return nil, 0, joinArmError(name, string(armPlan.stream.ID), kind+" arms must have distinct stream ids",
				"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
		}
		seen[armPlan.stream.ID] = struct{}{}
		armOps := cloneOperationSpecs(armSpec.chainOperations)
		if err := validateJoinArmDomain(name, armPlan.inputName, profile, armPlan.domain, armOps); err != nil {
			return nil, 0, err
		}
		// Explicit .Decode() on a packet-domain chain arm becomes the arm decode
		// node. Frame-domain arms feed frame-consuming joins directly, and
		// passthrough joins without .Decode() forward packets as-is.
		if armPlan.domain == shape.DomainPacket && chainHasDecode(armOps) {
			armPlan.decodeNode = name + "-decode-" + string(armPlan.stream.ID)
		}
		if profile.planArm != nil {
			stagePlan, err := profile.planArm(p, armSpec, armPlan.stream)
			if err != nil {
				return nil, 0, err
			}
			armPlan.stage = stagePlan
		}
		if armPlan.stage == nil && profile.armExpected != nil {
			// The solver converts a nested arm's OUTPUT like any arm: an outer
			// mix resamples a sub-mix that produced another rate.
			stagePlan, err := p.solveArmConversion(input.runtime, armPlan.stream, armPlan.inputName)
			if err != nil {
				return nil, 0, err
			}
			armPlan.stage = stagePlan
		}
		p.arms = append(p.arms, armPlan)
		// The arm's declared taps become resolvable by every later arm of the
		// tree (the lowering walks the same depth-first order).
		for _, tap := range armPlan.taps {
			anchors.declare(tap.ref.name, joinTapAnchor{
				owner:  p,
				arm:    len(p.arms) - 1,
				domain: tap.ref.domain,
				stream: armPlan.stream,
			})
		}
	}
	// The join's output is a normal stream point from here on: derive the joined
	// stream once and let taps, branches, the optional encoder, and the sink all
	// compose against it.
	p.joined = profile.joinedStream(p)
	p.joinedDomain = p.deriveJoinedDomain()
	taps, err := joinPlanTaps(tree.taps, name, p.joined, p.joinedDomain, pipeline.NodeRef(name))
	if err != nil {
		return nil, 0, err
	}
	p.taps = taps
	// The join's own output taps anchor on the join node: when this join is a
	// nested arm, a later arm of the OUTER join may reference them (the root's
	// registration is inert — no arm of the root is planned after this point,
	// so a join can never feed itself).
	for _, tap := range p.taps {
		anchors.declare(tap.Name, joinTapAnchor{owner: p, arm: -1, domain: tap.Domain, stream: p.joined})
	}
	return p, cursor, nil
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

// validateNestedJoinArm rejects terminal state on a join used as an arm: an
// arm contributes its joined output to the outer join, so it cannot carry its
// own encoder. (.To and .Branches already return a *Job, which is not a
// JoinArm — those stay impossible at compile time.)
func validateNestedJoinArm(outer string, sub *joinTreeSnapshot) error {
	if sub.encode != nil {
		return joinArmError(outer, string(sub.kind),
			"a nested "+string(sub.kind)+" arm cannot carry .Encode(...); encode the outer join instead",
			"move .Encode(...) to the outer "+outer+": encode once after the final convergence")
	}
	return nil
}

// assignArmSourceNames resolves the source node name of every leaf arm in the
// tree through the shared input-name disambiguation, so the planned spec and
// the lowering agree even when nested joins repeat input names.
func (p *joinPlan) assignArmSourceNames() {
	leaves := p.leafArms()
	inputs := make([]InputSpec, len(leaves))
	for i := range leaves {
		inputs[i] = leaves[i].input
	}
	names := graphSourceNodeNames(inputs)
	for i := range leaves {
		leaves[i].sourceNode = names[i]
	}
}

// leafArms returns pointers to every leaf arm plan in depth-first arm order —
// the order the flattened intent inputs follow. Tap arms open no input, so
// they are not leaves.
func (p *joinPlan) leafArms() []*joinArmPlan {
	out := make([]*joinArmPlan, 0, len(p.arms))
	for i := range p.arms {
		if p.arms[i].sub != nil {
			out = append(out, p.arms[i].sub.leafArms()...)
			continue
		}
		if p.arms[i].tapArm != nil {
			continue
		}
		out = append(out, &p.arms[i])
	}
	return out
}

// firstArmSourceShape derives the format facts of the first arm — the join's
// format reference: a leaf arm reports its declared custom-source shape, a
// nested arm reports its sub-join's joined stream.
func (p *joinPlan) firstArmSourceShape() shape.Spec {
	if len(p.arms) == 0 {
		return shape.Spec{}
	}
	arm := p.arms[0]
	if arm.sub != nil {
		return shape.FromStream(arm.sub.joined, arm.sub.joinedDomain)
	}
	if arm.tapArm != nil {
		return shape.FromStream(arm.stream, arm.domain)
	}
	spec, _ := declaredSourceShape(arm.input)
	return spec
}

// allTaps collects this join's taps, the taps its arm chains declared, and
// every nested join's taps — one flat list for the task: join-level taps
// anchor on their join node, arm taps on the arm's decode (or source) node.
func (p *joinPlan) allTaps() []workTap {
	taps := append([]workTap(nil), p.taps...)
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
func (p *joinPlan) solveArmConversion(rt *runtime, stream av.Stream, armName string) (*joinArmStagePlan, error) {
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
		step := operationSpec{Kind: plan.OpTransform, Component: p.name}
		return nil, shapeSolverAdapterError("build "+p.name, firstNonEmpty(armName, string(stream.ID)), 0, step, actual, expected, err)
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
			video:   conversion.transform.resize,
			audio:   conversion.transform.resample,
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

// planJoinBranches plans the joined stream's fanout through the same
// branch-composition planner the recipe path uses: each captured join branch is
// validated, lowered to recipe stream facts, planned with
// planBranchCompositionRecipe, and prepared with prepareBranchComposePlan —
// anchored at the join node instead of a demuxed source, with no second fanout
// implementation.
func (p *joinPlan) planJoinBranches() error {
	name := p.name
	parentPacket := p.joinedDomain == shape.DomainPacket
	destinations, err := joinBranchSnapshotDestinations(p.tree.branches)
	if err != nil {
		return err
	}
	recipe := recipeir.Recipe{Kind: recipeir.KindBranchComposition, Name: name}
	for i := range p.tree.branches {
		branch := p.tree.branches[i]
		// The branch's builder-construction error was captured at the recipe
		// boundary; surface it before validating the captured domain facts.
		if branch.construction != nil {
			return branch.construction
		}
		branchOps := operationSpecsFromRecipeIROperations(branch.recipe.Operations)
		facts := branchDomainFacts{
			name:         branch.recipe.Name,
			media:        branch.recipe.Media,
			operations:   branchOps,
			destinations: branch.destinations,
		}
		if err := validateBranchDomainFacts(p.joined.Type, parentPacket, !parentPacket, i, facts.name, facts.media, facts.operations, facts.destinations); err != nil {
			return err
		}
		if err := p.validateJoinBranchAnchor(branch.recipe); err != nil {
			return err
		}
		operations := cloneOperationSpecs(p.tree.operations)
		operations = append(operations, joinBranchOperationSpecs(parentPacket, branchOps)...)
		solved, err := p.solveJoinedOperations(branch.recipe.Name, operations)
		if err != nil {
			return err
		}
		operations = solved
		recipe.Streams = append(recipe.Streams, recipeIRStreamFromIntent(streamIntent{
			Name:         branch.recipe.Name,
			Select:       plan.StreamSelect{Type: p.joined.Type},
			Operations:   operations,
			Destinations: append([]string(nil), branchDestinationNames(branch.destinations)...),
		}))
	}
	for i := range destinations {
		if err := destinations[i].output.validate("build "+name, fmt.Sprintf("output-%d", i)); err != nil {
			return err
		}
		recipe.Destinations = append(recipe.Destinations, recipeIRDestinationFromIntent(destinations[i].output.intentWithName(destinations[i].name)))
	}
	composePlan, err := planBranchCompositionRecipe(recipe, InputSpec{}, destinations)
	if err != nil {
		return err
	}
	routes, targets, err := prepareBranchComposePlan(composePlan)
	if err != nil {
		return err
	}
	for i := range routes {
		routes[i].sourceDomain = p.joinedDomain
	}
	p.branchRoutes = routes
	p.branchTargets = targets
	return nil
}

func joinBranchOperationSpecs(parentPacket bool, operations []operationSpec) []operationSpec {
	if !parentPacket || chainHasDecode(operations) || codecIntentSet(chainEncodeSpec(operations)) {
		return cloneOperationSpecs(operations)
	}
	if operationSpecsContainKind(operations, plan.OpCopy) {
		return cloneOperationSpecs(operations)
	}
	out := []operationSpec{operationSpecForCopy(codec.Copy())}
	out = append(out, cloneOperationSpecs(operations)...)
	return out
}

// validateJoinBranchAnchor resolves a branch's .From(...) against the join: the
// joined stream is the only planned anchor a join offers, so an explicit tap
// must name one of the join-level taps (which alias the join node); anything
// else raises the same branch_tap_missing error the chain path raises. It reads
// the captured recipe IR, not the mutable branch spec.
func (p *joinPlan) validateJoinBranchAnchor(branch recipeir.JoinBranch) error {
	if branch.Source.Name == "" {
		return nil
	}
	for _, tap := range p.tree.taps {
		if tap.name != branch.Source.Name {
			continue
		}
		from := tapRef{name: branch.Source.Name, domain: branch.Source.Domain}
		return validateTapDomain("build branches", firstNonEmpty(branch.Name, "branch"), from, p.joinedDomain)
	}
	return plannedBranchTapMissingError(p.name, branch.Name, branch.Source.Name)
}

// joinBranchSnapshotDestinations collects the concrete destinations declared
// across the captured join branches, deduplicated by name, for the join's
// branch-composition planner. The handles stay concrete (the IR exception).
func joinBranchSnapshotDestinations(branches []joinBranchSnapshot) ([]namedDestinationSpec, error) {
	var out []namedDestinationSpec
	var err error
	for i := range branches {
		out, err = appendNamedBranchDestinations(out, branches[i].destinations...)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// joinPlanTaps validates the join-level taps and converts them into the same
// workTap records the recipe compiler installs on its tasks, anchored at the
// join node — so a join tap shows up in task.Taps() and anchors runtime
// branches exactly like a tap declared on an ordinary stream chain.
func joinPlanTaps(joinTaps []tapRef, name string, joined av.Stream, domain shape.MediaDomain, node pipeline.NodeRef) ([]workTap, error) {
	if len(joinTaps) == 0 {
		return nil, nil
	}
	taps := make([]workTap, 0, len(joinTaps))
	for _, tap := range joinTaps {
		if tap.name == "" {
			return nil, &BuildError{
				Phase:     phaseBuild,
				Family:    errcode.FamilyForCode(errcode.TapInvalid),
				Code:      errcode.TapInvalid,
				Operation: "build " + name,
				Node:      name,
				Reason:    "tap name is empty",
				fixes: buildErrorFixes([]string{
					"call .Tap(goav.FrameTap(\"" + name + ".out\")) or another stable tap ref",
					"omit .Tap(...) when no runtime branch should attach at that point",
				}),
				cause: errUnsupportedBuild,
			}
		}
		if err := validateTapDomain("build "+name, name, tap, domain); err != nil {
			return nil, err
		}
		taps = append(taps, workTap{
			Name:      tap.name,
			Node:      node,
			Domain:    domain,
			MediaKind: joined.Type,
			After:     plan.OpStage,
			Shape:     shape.FromStream(joined, domain),
			Shared:    true,
		})
	}
	return taps, nil
}

// --- graphPlanLowerer ---

func (p *joinPlan) runtimeRef() *runtime {
	return p.runtime
}

// graphConfig keeps the join graph's identity and buffer policy: the graph is
// named after the kind and a direct runtime buffer is replaced by the first
// profile-pinned buffered policy anywhere in the join tree, so control-plane
// injection works no matter where the Select sits (root or nested arm).
func (p *joinPlan) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	buffer := p.runtime.buffer
	if pinned, realtimeOnly := p.treeGraphBuffer(); pinned != nil && buffer.IsDirect() && (!realtimeOnly || p.runtime.realtime) {
		buffer = *pinned
	}
	if !buffer.IsDirect() {
		var err error
		buffer, err = bufferPolicyWithShapeBudgets(buffer, gp.work)
		if err != nil {
			return pipeline.GraphConfig{}, err
		}
	}
	return pipeline.GraphConfig{
		Name:             "goav-" + p.name,
		Buffer:           buffer,
		Realtime:         p.runtime.realtime,
		EventCapacity:    p.runtime.eventCapacity,
		CloseWaitTimeout: p.runtime.closeWaitTimeout,
	}, nil
}

// treeGraphBuffer returns the first profile-pinned graph buffer in the join
// tree: a nested Select still needs the buffered per-node workers its control
// plane injects through.
func (p *joinPlan) treeGraphBuffer() (*pipeline.BufferPolicy, bool) {
	if p.profile.graphBuffer != nil {
		return p.profile.graphBuffer, p.profile.graphBufferRealtimeOnly
	}
	for i := range p.arms {
		if p.arms[i].sub == nil {
			continue
		}
		if buffer, realtimeOnly := p.arms[i].sub.treeGraphBuffer(); buffer != nil {
			return buffer, realtimeOnly
		}
	}
	return nil, false
}

// spec emits the planned join graph: per-arm sub-chains (a source or a
// recursively planned nested join, plus the optional decode and arm stage),
// the N-to-1 convergence into the join node, and the downstream chain —
// encode→destination, sink, or the planned branch fanout through the shared
// branch-compose planner. The lowering follows the same order, so Describe()
// equals the built graph.
func (p *joinPlan) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav-" + p.name, Realtime: runtimeRealtime(p.runtime)}
	nodes := make(map[string]plannedNode, len(p.arms)*3+4)
	if err := p.planJoinTreeSpec(&spec, nodes); err != nil {
		return pipeline.Spec{}, err
	}
	joinRef := pipeline.NodeRef(p.name)
	switch {
	case len(p.branchRoutes) != 0:
		routed, err := planBranchComposeRoutes(spec, nodes, []pipeline.NodeRef{joinRef}, p.branchRoutes, p.branchTargets)
		if err != nil {
			return pipeline.Spec{}, err
		}
		return groupSpecEdgesByNode(routed), nil
	case p.encode != nil:
		upstream := joinRef
		for i := range p.downstream {
			stageName := p.downstream[i].transform.name
			stageRef := pipeline.NodeRef(stageName)
			if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stageRef, mediaTransformDetail(p.downstream[i].transform)); err != nil {
				return pipeline.Spec{}, err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: stageRef, Policy: pipeline.RouteAll})
			upstream = stageRef
		}
		encodeName := encodeNodeName(*p.encode)
		if err := addPlannedNode(nodes, &spec, encodeName, pipeline.NodeStage, pipeline.NodeRef(encodeName), encodeNodeDetail(*p.encode)); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: pipeline.NodeRef(encodeName), Policy: pipeline.RouteAll})
		destRefs, err := p.planDestinationNodes(&spec, nodes)
		if err != nil {
			return pipeline.Spec{}, err
		}
		for i := range destRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: pipeline.NodeRef(encodeName), To: destRefs[i], Policy: pipeline.RouteAll})
		}
		return groupSpecEdgesByNode(spec), nil
	default:
		destRefs, err := p.planDestinationNodes(&spec, nodes)
		if err != nil {
			return pipeline.Spec{}, err
		}
		for i := range destRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: joinRef, To: destRefs[i], Policy: pipeline.RouteAll})
		}
		return groupSpecEdgesByNode(spec), nil
	}
}

// groupSpecEdgesByNode reorders the planned edges into the exact order the
// built graph reports them: pipeline.Graph.Spec lists every node's outgoing
// routes grouped under the node, nodes in add order, routes in connect order.
// The join planner emits edges in the lowering's CONNECT order; this stable
// group-by keeps Describe() byte-identical to Build() even when one node fans
// out to consumers connected in different lowering phases (a tap-arm restamp
// connected during the arm walk plus the arm's own convergence edge after it).
func groupSpecEdgesByNode(spec pipeline.Spec) pipeline.Spec {
	if len(spec.Edges) < 2 {
		return spec
	}
	order := make(map[pipeline.NodeRef]int, len(spec.Nodes))
	for i := range spec.Nodes {
		order[pipeline.NodeRef(spec.Nodes[i].Name)] = i
	}
	edges := append([]pipeline.EdgeSpec(nil), spec.Edges...)
	sort.SliceStable(edges, func(i, j int) bool {
		return order[edges[i].From] < order[edges[j].From]
	})
	spec.Edges = edges
	return spec
}

// planJoinTreeSpec emits this join's planned sub-graph: each arm's chain — a
// source, a recursively emitted nested join, or a tap-arm restamp hung off the
// tap's already-planned node, the optional decode, the optional arm stage —
// the arm edges into this join node, and the join node itself, in exactly the
// order the lowering adds them.
func (p *joinPlan) planJoinTreeSpec(spec *pipeline.Spec, nodes map[string]plannedNode) error {
	joinRef := pipeline.NodeRef(p.name)
	armRefs := make([]pipeline.NodeRef, 0, len(p.arms))
	for i := range p.arms {
		arm := p.arms[i]
		var upstream pipeline.NodeRef
		switch {
		case arm.sub != nil:
			if err := arm.sub.planJoinTreeSpec(spec, nodes); err != nil {
				return err
			}
			upstream = pipeline.NodeRef(arm.sub.name)
		case arm.tapArm != nil:
			// The tap's anchor node was planned by the declaring arm earlier in
			// the same depth-first walk; the restamp hangs off it as a fanout.
			anchor := pipeline.NodeRef(arm.tapArm.anchor.node())
			if err := addPlannedNode(nodes, spec, arm.tapArm.node, pipeline.NodeStage, pipeline.NodeRef(arm.tapArm.node), tapArmNodeDetail(arm.tapArm.tap.name)); err != nil {
				return err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: anchor, To: pipeline.NodeRef(arm.tapArm.node), Policy: pipeline.RouteAll})
			upstream = pipeline.NodeRef(arm.tapArm.node)
		default:
			if err := addPlannedNode(nodes, spec, arm.sourceNode, pipeline.NodeSource, pipeline.NodeRef(arm.sourceNode), arm.input.graphSourceNodeDetail()); err != nil {
				return err
			}
			upstream = pipeline.NodeRef(arm.sourceNode)
		}
		if arm.decodeNode != "" {
			detail := decodeRequestDetail(decodeRequest{selector: av.StreamSelector{Type: arm.stream.Type}})
			if err := addPlannedNode(nodes, spec, arm.decodeNode, pipeline.NodeStage, pipeline.NodeRef(arm.decodeNode), detail); err != nil {
				return err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: pipeline.NodeRef(arm.decodeNode), Policy: pipeline.RouteAll})
			upstream = pipeline.NodeRef(arm.decodeNode)
		}
		if arm.stage != nil {
			stageName := arm.stage.transform.name
			if err := addPlannedNode(nodes, spec, stageName, pipeline.NodeStage, pipeline.NodeRef(stageName), mediaTransformDetail(arm.stage.transform)); err != nil {
				return err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: pipeline.NodeRef(stageName), Policy: pipeline.RouteAll})
			upstream = pipeline.NodeRef(stageName)
		}
		armRefs = append(armRefs, upstream)
	}
	if err := addPlannedNode(nodes, spec, p.name, pipeline.NodeStage, joinRef, p.joinNodeDetail()); err != nil {
		return err
	}
	// The N-to-1 convergence edges come after every arm's chain, exactly the
	// order lowerJoinTree connects them — so a node fanning out to BOTH the
	// join and a tap-arm restamp lists its routes identically in Describe and
	// in the built graph.
	for i := range armRefs {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: armRefs[i], To: joinRef, Policy: pipeline.RouteAll})
	}
	return nil
}

// joinDestinationNodeName names one destination node exactly as the lowering
// does: sinks keep their own name, mux destinations use muxNodeName with their
// .To(...) position.
func (p *joinPlan) joinDestinationNodeName(index int) string {
	destination := p.destinations[index]
	if destination.sink != nil {
		return firstNonEmpty(destination.sink.Name(), destination.label("sink"))
	}
	return muxNodeName(destination.output, index)
}

func (p *joinPlan) planDestinationNodes(spec *pipeline.Spec, nodes map[string]plannedNode) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0, len(p.destinations))
	for i := range p.destinations {
		name := p.joinDestinationNodeName(i)
		ref := pipeline.NodeRef(name)
		if p.destinations[i].sink != nil {
			if err := addPlannedNode(nodes, spec, name, pipeline.NodeSink, ref, describedNodeDetail(p.destinations[i].sink)); err != nil {
				return nil, err
			}
			refs = append(refs, ref)
			continue
		}
		detail := outputNodeDetailWithFormat(p.destinations[i].output, destinationGraphFormat(p.destinations[i]))
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, detail); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// lower executes the planned join against the graph: the join tree is lowered
// recursively (each arm opens through the input seam or lowers its nested
// join, decodes/stages per plan, and converges into the kind's stage), then
// the root delivers through the shared downstream path. Node names come from
// the plan, so the built graph equals the planned spec.
func (p *joinPlan) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	rt := p.runtime
	joinRef, err := p.lowerJoinTree(ctx, gp, graph, service)
	if err != nil {
		return err
	}
	switch {
	case len(p.branchRoutes) != 0:
		groups, err := resolveBranchComposeStreamGroups([]av.Stream{p.joined}, p.branchRoutes)
		if err != nil {
			return err
		}
		branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, rt, graph, []pipeline.NodeRef{joinRef}, groups, nil, p.branchRoutes, nil, rt.realtime)
		if err != nil {
			return err
		}
		return compileBranchComposeRoutes(ctx, service, graph, p.branchRoutes, p.branchTargets, branchInputs, branchStreams, nil, nil, rt.realtime)
	case p.encode != nil:
		upstream := string(joinRef)
		for i := range p.downstream {
			stagePlan := p.downstream[i]
			stage, _, err := service.newMediaTransformStageNamed(ctx, stagePlan.transform.name, stagePlan.transform, stagePlan.stream, rt.realtime)
			if err != nil {
				return err
			}
			upstream, err = insertJoinArmStage(graph, rt, stage, upstream)
			if err != nil {
				return err
			}
		}
		// openMuxDestinationStage records the destination transaction (file
		// commit/abort) on service; buildGraphPlanTask carries it to the task so
		// the file finalizes. Every destination receives the encoded packets —
		// the same fanout a chain's multi-destination .To(...) lowers to.
		return compileEncodeDestinationPath(ctx, service, graph, pipeline.NodeRef(upstream), *p.encode, p.encodeConfig, p.encodedStream, p.destinations)
	default:
		for i := range p.destinations {
			sinkRef, err := graph.AddSink(p.destinations[i].sink, rt.buffer)
			if err != nil {
				return err
			}
			if err := graph.Connect(pipeline.Route{From: string(joinRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll}); err != nil {
				return err
			}
		}
		return nil
	}
}

// lowerJoinTree lowers one join of the tree: each arm opens its source (or
// recursively lowers its nested join), the planned per-arm decode/transform
// stages are inserted, and the kind's convergence stage is built and connected
// to the N arm upstreams. It returns the join node's ref — the upstream a
// nested join hands its outer arm.
func (p *joinPlan) lowerJoinTree(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) (pipeline.NodeRef, error) {
	rt := p.runtime
	armRefs := make([]string, 0, len(p.arms))
	armIDs := make([]av.StreamID, 0, len(p.arms))
	seen := make(map[av.StreamID]struct{}, len(p.arms))
	for i := range p.arms {
		arm := p.arms[i]
		var upstream string
		stream := arm.stream
		switch {
		case arm.sub != nil:
			subRef, err := arm.sub.lowerJoinTree(ctx, gp, graph, service)
			if err != nil {
				return "", err
			}
			if _, dup := seen[stream.ID]; dup {
				return "", joinArmError(p.name, string(stream.ID), p.name+" arms must have distinct stream ids",
					"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
			}
			upstream = string(subRef)
		case arm.tapArm != nil:
			// No source opens: the restamp stage hangs off the tap's node — the
			// declaring arm lowered it earlier in the same depth-first order —
			// and republishes the tapped media under the tap name.
			if _, dup := seen[stream.ID]; dup {
				return "", joinArmError(p.name, string(stream.ID), p.name+" arms must have distinct stream ids",
					"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
			}
			ref, err := graph.AddStage(newTapArmStage(arm.tapArm.node, av.StreamID(arm.tapArm.tap.name)), rt.buffer)
			if err != nil {
				return "", err
			}
			if err := graph.Connect(pipeline.Route{From: arm.tapArm.anchor.node(), To: []string{string(ref)}, Policy: pipeline.RouteAll}); err != nil {
				return "", err
			}
			upstream = string(ref)
		default:
			source, streams, _, err := arm.input.openGraphSource(ctx, service, arm.sourceNode)
			if err != nil {
				return "", err
			}
			stream, err = selectStream(streams, av.StreamSelector{Type: p.profile.media})
			if err != nil {
				source.Close()
				return "", err
			}
			if _, dup := seen[stream.ID]; dup {
				source.Close()
				return "", joinArmError(p.name, string(stream.ID), p.name+" arms must have distinct stream ids",
					"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
			}
			srcRef, err := graph.AddSource(source, rt.buffer)
			if err != nil {
				source.Close()
				return "", err
			}
			upstream = string(srcRef)
		}
		seen[stream.ID] = struct{}{}
		if arm.decodeNode != "" {
			request := decodeRequest{selector: av.StreamSelector{Type: stream.Type}}
			// Arm events MUST flow through decode (dropInputEvents=false): the
			// join stage's per-arm EOS bookkeeping (an ended arm stops gating,
			// the joined EOS fires when every arm ended) and the PTS re-sync on
			// discontinuity both read the arm's events. The join consumes them
			// — only the joined stream's own events continue downstream.
			decodeStage, err := service.newDecodeStageNamed(ctx, arm.decodeNode, request, stream, rt.realtime, false, codec.DecodeBounds{})
			if err != nil {
				return "", err
			}
			if upstream, err = insertJoinArmStage(graph, rt, decodeStage, upstream); err != nil {
				return "", err
			}
		}
		if arm.stage != nil {
			stage, _, err := service.newMediaTransformStageNamed(ctx, arm.stage.transform.name, arm.stage.transform, arm.stage.stream, rt.realtime)
			if err != nil {
				return "", err
			}
			if upstream, err = insertJoinArmStage(graph, rt, stage, upstream); err != nil {
				return "", err
			}
		}
		armRefs = append(armRefs, upstream)
		armIDs = append(armIDs, stream.ID)
	}
	stage, pinned := p.profile.newStage(p, armIDs)
	stageBuffer := rt.buffer
	if pinned != nil {
		stageBuffer = *pinned
		if !stageBuffer.IsDirect() {
			var err error
			stageBuffer, err = p.joinNodeBufferPolicy(stageBuffer, gp.work)
			if err != nil {
				return "", err
			}
		}
	}
	joinRef, err := graph.AddStage(stage, stageBuffer)
	if err != nil {
		return "", err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(joinRef)}, Policy: pipeline.RouteAll}); err != nil {
			return "", err
		}
	}
	return joinRef, nil
}

func (p *joinPlan) joinNodeBufferPolicy(policy pipeline.BufferPolicy, work workPlan) (pipeline.BufferPolicy, error) {
	if operation, ok := workOperationForNodeKind(work.Operations, pipeline.NodeRef(p.name), plan.OpJoin); ok {
		return bufferPolicyWithShapeBudgetsForOperations(policy, []workOperation{operation})
	}
	return bufferPolicyWithShapeBudgetsForOperations(policy, work.Operations)
}

// insertJoinArmStage appends a per-arm stage after upstream and returns its ref.
func insertJoinArmStage(graph pipeline.Graph, rt *runtime, stage pipeline.Stage, upstream string) (string, error) {
	bindPlayoutClock(stage, rt.clock)
	ref, err := graph.AddStage(stage, rt.buffer)
	if err != nil {
		return "", err
	}
	if err := graph.Connect(pipeline.Route{From: upstream, To: []string{string(ref)}, Policy: pipeline.RouteAll}); err != nil {
		return "", err
	}
	return string(ref), nil
}

// --- work plan ---

// buildJoinWorkPlan renders the planned join into the workPlan IR: one branch
// per arm (its decode/stage operations) recursing through nested joins, one
// joined branch anchored on the plan.OpJoin node carrying the downstream chain,
// and — for fanouts — one branch per planned branch route. The N-to-1
// convergence rides workPlan.Edges, copied from the planned spec.
func (p *joinPlan) buildJoinWorkPlan(input joinWorkPlanInput, spec pipeline.Spec) workPlan {
	outputs := planOutputs(input.destinations, input.outputFormats)
	work := workPlan{
		Name:         firstNonEmpty(spec.Name, "goav-"+p.name),
		Realtime:     spec.Realtime,
		Inputs:       workInputsFromIntent(input.inputs),
		Taps:         p.allTaps(),
		Destinations: workDestinationsFromPlan(outputs),
		Diagnostics:  clonePlanDiagnostics(p.allDiagnostics()),
	}
	ids := workDestinationIDsByName(work.Destinations)
	work.Operations, work.Branches = p.joinWorkBranches(ids, outputs)
	work.Destinations = joinWorkDestinationBranches(work.Destinations, work.Branches)
	work.Edges = workEdgesFromSpec(spec, work.Operations)
	return work
}

// joinWorkDestinationBranches fills each destination's branch refs from the
// branches that route into it, mirroring planOutputsWithBranches.
func joinWorkDestinationBranches(destinations []workDestination, branches []workBranch) []workDestination {
	for i := range destinations {
		for j := range branches {
			if stringInSlice(destinations[i].ID, branches[j].Destinations) {
				destinations[i].Branches = append(destinations[i].Branches, branches[j].Name)
			}
		}
	}
	return destinations
}

func (p *joinPlan) joinWorkBranches(ids map[string]string, outputs []planOutput) ([]workOperation, []workBranch) {
	var operations []workOperation
	var branches []workBranch
	joinedShape := normalizeTapShape(shape.FromStream(p.joined, p.joinedDomain))
	armShape := p.appendJoinArmWork(&operations, &branches)
	joinOperations, joinBranch := p.joinedWorkBranch(ids, outputs, armShape, joinedShape, len(branches))
	operations = append(operations, joinOperations...)
	branches = append(branches, joinBranch)
	if len(p.branchRoutes) != 0 {
		fanOperations, fanBranches := p.joinFanoutWorkBranches(ids, outputs, joinedShape, len(branches))
		operations = append(operations, fanOperations...)
		branches = append(branches, fanBranches...)
	}
	return operations, branches
}

// appendJoinArmWork renders this join's arms into the work IR: one branch per
// leaf arm carrying its decode/transform operations, and — for nested arms —
// the sub-join's own arm branches (recursively) plus one branch anchored on
// the sub's plan.OpJoin node carrying the outer join's per-arm operations. It
// returns the shape of the first arm at the join input (the OpJoin ShapeIn).
func (p *joinPlan) appendJoinArmWork(operations *[]workOperation, branches *[]workBranch) shape.Spec {
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
			subArmShape := arm.sub.appendJoinArmWork(operations, branches)
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
		if arm.tapArm != nil {
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpSelect),
				Name:      arm.tapArm.node,
				Kind:      plan.OpSelect,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.tapArm.node),
				Component: arm.tapArm.tap.name,
				Detail:    "attach tap arm",
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
			out := shape.Merge(current, mediaShapeFromTransform(transformSpecFromMediaTransform(arm.stage.transform)))
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
		if i == 0 {
			armShape = current
		}
		*branches = append(*branches, workBranch{
			ID:          workBranchID(branchName, len(*branches)),
			Name:        branchName,
			Input:       firstNonEmpty(arm.inputName, branchName),
			Stream:      streamSelectFromStream(arm.stream),
			SourceShape: sourceShape,
			Operations:  ops,
		})
	}
	return armShape
}

// joinedWorkBranch renders the joined stream's branch: the plan.OpJoin convergence
// node plus — in single-destination mode — the optional encode and the terminal
// destination operation.
func (p *joinPlan) joinedWorkBranch(ids map[string]string, outputs []planOutput, armShape shape.Spec, joinedShape shape.Spec, branchIndex int) ([]workOperation, workBranch) {
	branchName := p.name
	joinDetail := "converge " + strconv.Itoa(len(p.arms)) + " arms"
	if p.tree.sync == joinSyncPTS {
		joinDetail += " by pts"
	}
	operations := []workOperation{{
		ID:        workOperationIDForKind(branchName, 0, plan.OpJoin),
		Name:      p.name,
		Kind:      plan.OpJoin,
		Branch:    branchName,
		Node:      pipeline.NodeRef(p.name),
		Component: p.name,
		Detail:    joinDetail,
		ShapeIn:   armShape,
		ShapeOut:  joinedShape,
	}}
	branch := workBranch{
		ID:          workBranchID(branchName, branchIndex),
		Name:        branchName,
		Stream:      streamSelectFromStream(p.joined),
		SourceShape: joinedShape,
	}
	current := joinedShape
	index := 1
	if len(p.branchRoutes) == 0 {
		for i := range p.downstream {
			stagePlan := p.downstream[i]
			kind := plan.OpTransform
			if stagePlan.transform.stage != nil {
				kind = plan.OpStage
			}
			out := shape.Merge(current, mediaShapeFromTransform(transformSpecFromMediaTransform(stagePlan.transform)))
			out.Domain = shape.DomainFrame
			operations = append(operations, workOperation{
				ID:        workOperationIDForKind(branchName, index, kind),
				Name:      stagePlan.transform.name,
				Kind:      kind,
				Branch:    branchName,
				Node:      pipeline.NodeRef(stagePlan.transform.name),
				Component: firstNonEmpty(stagePlan.transform.factory, stagePlan.transform.name, "transform"),
				Detail:    "transform frames",
				ShapeIn:   current,
				ShapeOut:  out,
			})
			index++
			current = out
		}
		if p.encode != nil {
			encoded := normalizeTapShape(shape.FromStream(p.encodedStream, shape.DomainPacket))
			operations = append(operations, workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpEncode),
				Name:      encodeNodeName(*p.encode),
				Kind:      plan.OpEncode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(encodeNodeName(*p.encode)),
				Component: string(p.encodeConfig.Parameters.ID),
				Detail:    "frames to packets",
				Codec:     codecSpecFromEncodeConfig(p.encodeConfig),
				ShapeIn:   current,
				ShapeOut:  encoded,
			})
			index++
			current = encoded
		}
		for i := range outputs {
			output := outputs[i]
			node := pipeline.NodeRef(p.joinDestinationNodeName(i))
			operations = append(operations, workOperation{
				ID:           workOperationIDForKind(branchName, index, output.Operation),
				Name:         workOperationName(node, output.Component, "destination", index),
				Kind:         output.Operation,
				Branch:       branchName,
				Node:         node,
				Component:    output.Component,
				Detail:       "destination",
				ShapeIn:      current,
				ShapeOut:     current,
				Destinations: []string{workDestinationIDForName(ids, output.Name)},
			})
			index++
			branch.Destinations = append(branch.Destinations, workDestinationIDForName(ids, output.Name))
		}
	}
	for i := range operations {
		branch.Operations = append(branch.Operations, operations[i].ID)
	}
	return operations, branch
}

// joinFanoutWorkBranches renders the planned branch fanout exactly along the
// branch-compose routes the spec and the lowering share, so the operation node
// names match the planned graph.
func (p *joinPlan) joinFanoutWorkBranches(ids map[string]string, outputs []planOutput, joinedShape shape.Spec, branchIndexBase int) ([]workOperation, []workBranch) {
	outputsByName := planOutputsByName(outputs)
	var operations []workOperation
	var branches []workBranch
	for i := range p.branchRoutes {
		route := p.branchRoutes[i]
		branchName := route.name
		current := joinedShape
		index := 0
		var ops []string
		appendOperation := func(operation workOperation) {
			operations = append(operations, operation)
			ops = append(ops, operation.ID)
			index++
		}
		selectNode := branchComposeInputNodeName(selectNodeName(route.branch.Selector), route.branch.Input)
		appendOperation(workOperation{
			ID:        workOperationIDForKind(branchName, index, plan.OpSelect),
			Name:      selectNode,
			Kind:      plan.OpSelect,
			Branch:    branchName,
			Node:      pipeline.NodeRef(selectNode),
			Component: selectorComponent(streamSelectFromStream(p.joined)),
			Detail:    "select stream",
			ShapeIn:   current,
			ShapeOut:  current,
		})
		switch {
		case branchComposeRouteNeedsDecode(route):
			decodeNode := branchComposeInputNodeName(decodeNodeName(route.branch.Selector), route.branch.Input)
			out := current
			out.Domain = shape.DomainFrame
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpDecode),
				Name:      decodeNode,
				Kind:      plan.OpDecode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(decodeNode),
				Component: codecComponent(p.joined.Codec.ID),
				Detail:    "packets to frames",
				Codec:     codecSpecFromStream(p.joined),
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		case route.copy && p.joinedDomain == shape.DomainPacket:
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpCopy),
				Name:      "packet-copy",
				Kind:      plan.OpCopy,
				Branch:    branchName,
				Component: "packet-copy",
				Detail:    "preserve encoded packets",
				Codec:     codecSpecFromStream(p.joined),
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if transforms, err := branchComposePrivateOperationTransforms(route); err == nil {
			for j := range transforms {
				kind := plan.OpTransform
				if transforms[j].stage != nil {
					kind = plan.OpStage
				}
				out := shape.Merge(current, mediaShapeFromTransform(transformSpecFromMediaTransform(transforms[j])))
				out.Domain = shape.DomainFrame
				appendOperation(workOperation{
					ID:        workOperationIDForKind(branchName, index, kind),
					Name:      transforms[j].name,
					Kind:      kind,
					Branch:    branchName,
					Node:      pipeline.NodeRef(transforms[j].name),
					Component: firstNonEmpty(transforms[j].factory, transforms[j].name),
					Detail:    "transform frames",
					ShapeIn:   current,
					ShapeOut:  out,
				})
				current = out
			}
		}
		if branchComposeRouteNeedsEncode(route) {
			encodeNode := encodeNodeName(route.request)
			out := current
			out.Domain = shape.DomainPacket
			out.Codec = route.request.config.Parameters.ID
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, plan.OpEncode),
				Name:      encodeNode,
				Kind:      plan.OpEncode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(encodeNode),
				Component: string(route.request.config.Parameters.ID),
				Detail:    "frames to packets",
				Codec:     codecSpecFromEncodeConfig(route.request.config),
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		}
		var destinationIDs []string
		for j := range p.branchTargets {
			target := p.branchTargets[j]
			if !intInSlice(i, target.matches) {
				continue
			}
			targetName := branchComposeTargetRouteName(target)
			output := outputsByName[targetName]
			node := pipeline.NodeRef(joinFanoutTargetNodeName(target, j))
			id := workDestinationIDForName(ids, firstNonEmpty(output.Name, targetName))
			appendOperation(workOperation{
				ID:           workOperationIDForKind(branchName, index, output.Operation),
				Name:         workOperationName(node, output.Component, "destination", index),
				Kind:         output.Operation,
				Branch:       branchName,
				Node:         node,
				Component:    output.Component,
				Detail:       "destination",
				ShapeIn:      current,
				ShapeOut:     current,
				Destinations: []string{id},
			})
			destinationIDs = append(destinationIDs, id)
		}
		branches = append(branches, workBranch{
			ID:           workBranchID(branchName, branchIndexBase+i),
			Name:         branchName,
			Input:        p.name,
			Stream:       streamSelectFromStream(p.joined),
			SourceShape:  joinedShape,
			Operations:   ops,
			Destinations: destinationIDs,
		})
	}
	return operations, branches
}

// joinFanoutTargetNodeName names a fanout destination node exactly as
// planBranchComposeRoutes and compileBranchComposeRoutes do.
func joinFanoutTargetNodeName(target branchComposeTargetRoute, index int) string {
	if target.sink != nil {
		return branchComposeTargetSinkNodeName(target, index)
	}
	return muxNodeName(target.target, index)
}

func intInSlice(needle int, haystack []int) bool {
	for i := range haystack {
		if haystack[i] == needle {
			return true
		}
	}
	return false
}
