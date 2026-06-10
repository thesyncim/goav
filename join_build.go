package goav

import (
	"context"
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// joinKind names a convergence workflow. Public sugar (Mix, Composite, Select)
// lowers to one joinSpec and one planned join lowering; no composition code
// dispatches on the kind outside the joinProfiles table.
type joinKind string

const (
	joinMix       joinKind = "mix"
	joinComposite joinKind = "composite"
	joinSelect    joinKind = "select"
)

// joinSpec is the one internal N→1 model behind Mix/Composite/Select: N source
// arms converge into one stage and continue like any other stream point — to a
// single destination (optionally through an encoder), out to planned branches,
// and past named taps on the joined stream.
type joinSpec struct {
	kind   joinKind
	arms   []*jobStreamBuilder
	dest   Destination
	encode *codec.CodecSpec // mix/composite only; nil delivers the raw join output
	// taps name the joined stream as stable attach points, installed on the task
	// exactly like chain taps (visible in task.Taps(), runtime-attachable).
	taps []TapRef
	// branches fan the joined stream out to planned branch chains, each carrying
	// its own destinations; when set, dest/encode are unused.
	branches []BranchSpec
	// sync selects arm alignment for the convergence stage (Mix/Composite):
	// arrival order by default, PTS alignment via .SyncByPTS(). Select needs no
	// sync — it forwards exactly one live arm.
	sync joinSyncMode
}

// joinProfile is the per-kind configuration consulted by the join planner.
// Everything that differs between Mix, Composite and Select lives here — the
// plan skeleton is written once. The profile name is the joinKind: it doubles
// as the join node name, graph-name suffix and error-code prefix.
type joinProfile struct {
	// media selects each arm's stream; the zero value picks the arm's single
	// stream regardless of type (Select is media-agnostic passthrough).
	media av.MediaType
	// decodeArms auto-inserts a decode for packet-domain arms so the join stage
	// sees frames. Select forwards packets as-is and leaves this false.
	decodeArms bool
	// graphBuffer, when non-nil, replaces a direct runtime buffer so the graph
	// gets per-node serial workers for control-plane injection (Select).
	graphBuffer *pipeline.BufferPolicy
	// planArm runs per arm at plan time after the optional decode decision to
	// record arm state (composite layouts). Format solving does NOT live here —
	// it goes through the shape solver via armExpected/armPolicy.
	planArm func(p *joinPlan, arm *jobStreamBuilder, stream av.Stream) (*joinArmStagePlan, error)
	// armExpected derives the format facts every arm must satisfy before the
	// join stage (mix: the first arm's audio format is the target). A zero Spec
	// skips solving for that arm. The needed conversions are planned through the
	// one shape solver under armPolicy — the join's implicit always-on policy —
	// and lowered as real per-arm transform stages.
	armExpected func(p *joinPlan, stream av.Stream) shape.Spec
	armPolicy   shape.Policy
	// newStage builds the convergence stage at lowering time. A non-nil buffer
	// pins the stage's input queue (Select: non-lossy DropBlock so injected
	// controls survive).
	newStage func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy)
	// joinedStream derives the join's output stream — the normal stream point
	// that taps, branches, and the optional encoder all compose against.
	joinedStream func(p *joinPlan) av.Stream
	// sinkOnlyReason is the destination error when dest is not a frame Sink and
	// no encode applies.
	sinkOnlyReason string
}

// joinProfiles is the single per-kind table; each entry lives next to its
// public sugar (audio_mix.go, video_composite_build.go, select_build.go).
var joinProfiles = map[joinKind]joinProfile{
	joinMix:       mixJoinProfile,
	joinComposite: compositeJoinProfile,
	joinSelect:    selectJoinProfile,
}

// newJoinJob lowers public convergence sugar into the one joinSpec carried by
// Job, so .To/Build/Run are shared by every join kind.
func newJoinJob(kind joinKind, spec joinSpec) *Job {
	name := string(kind)
	job := &Job{name: name, runtime: Default()}
	if len(spec.arms) < 2 {
		job.setErr(&BuildError{Code: name + "_inputs", Operation: "build " + name, Node: name, Reason: name + " requires at least two source arms", Cause: ErrUnsupportedBuild})
		return job
	}
	spec.kind = kind
	job.join = &spec
	return job
}

// newJoinBranchesJob lowers a join that fans out to planned branches instead of
// a single destination. The branch specs are the same goav.Branch values an
// ordinary stream chain accepts; each carries its own destinations, so the Job
// needs no .To. A join-level encoder is terminal exactly like a stream encoder,
// so .Encode(...).Branches(...) is rejected with the chain's error.
func newJoinBranchesJob(kind joinKind, spec joinSpec, branches []BranchSpec) *Job {
	name := string(kind)
	job := newJoinJob(kind, spec)
	if job.err != nil {
		return job
	}
	if len(branches) == 0 {
		job.setErr(branchMissingError(name))
		return job
	}
	if spec.encode != nil {
		job.setErr(branchEncodeParentOperationError(name, *spec.encode))
		return job
	}
	job.join.branches = branches
	return job
}

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
	stream    av.Stream
	domain    shape.MediaDomain
	// decodeNode is the planned decode node name; empty means the arm feeds the
	// join directly (frame arms, passthrough kinds).
	decodeNode string
	stage      *joinArmStagePlan
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
	join    *joinSpec
	profile joinProfile
	name    string

	arms         []joinArmPlan
	joined       av.Stream
	joinedDomain shape.MediaDomain
	taps         []workTap
	// diagnostics records the arm conversions the shape solver inserted, so
	// Explain reports them exactly like chain insertions.
	diagnostics []info.Diagnostic

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample planned by the mix planArm hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its planArm hook.
	layouts []compositeLayout

	// single-destination delivery (optionally through one encoder).
	encode        *encodeRequest
	encodeConfig  codec.EncodeConfig
	encodedStream av.Stream
	destination   destinationSpec

	// planned branch fanout off the joined stream.
	branchRoutes  []branchComposeRoute
	branchTargets []branchComposeTargetRoute
}

func joinArmError(name string, node string, reason string) error {
	return &BuildError{Code: name + "_arm", Operation: "build " + name, Node: node, Reason: reason, Cause: ErrUnsupportedBuild}
}

// newJoinPlan plans a joinSpec from the compile state: arms are validated and
// resolved statically (custom-source shapes, probes, live codec intent), the
// per-kind profile plans arm stages and the joined stream, and the downstream
// chain (taps + branches or encode/destination) is planned against the joined
// stream — all before any source opens.
func newJoinPlan(rt *runtime, state *recipeCompileState) (*joinPlan, error) {
	spec := state.joinAttachment
	name := string(spec.kind)
	profile, ok := joinProfiles[spec.kind]
	if !ok {
		return nil, &BuildError{Code: name + "_kind", Operation: "build " + name, Node: name, Reason: "unknown join kind", Cause: ErrUnsupportedBuild}
	}
	p := &joinPlan{runtime: rt, join: spec, profile: profile, name: name}
	sets := jobInputStreamSets(state.intent.Inputs, state.inputAttachments, state.inputProbes)
	seen := make(map[av.StreamID]struct{}, len(spec.arms))
	for i := range spec.arms {
		arm := spec.arms[i]
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			return nil, joinArmError(name, name, "each "+name+" arm must be a single-input source chain")
		}
		if i >= len(sets) || !sets[i].known || len(sets[i].streams) == 0 {
			return nil, recipeGraphUnsupportedError(state.operation, state.intent)
		}
		stream, err := selectStream(sets[i].streams, av.StreamSelector{Type: profile.media})
		if err != nil {
			return nil, err
		}
		if _, dup := seen[stream.ID]; dup {
			return nil, joinArmError(name, string(stream.ID), name+" arms must have distinct stream ids")
		}
		seen[stream.ID] = struct{}{}
		armPlan := joinArmPlan{
			input:     arm.job.inputs[0],
			inputName: sets[i].name,
			stream:    stream,
			domain:    sets[i].domain,
		}
		// Packet-domain arms decode to frames before the join (auto-inserted —
		// the join stage works on decoded media). Frame-domain arms feed it
		// directly. Passthrough kinds skip this and forward packets as-is.
		if profile.decodeArms && armPlan.domain == shape.DomainPacket {
			armPlan.decodeNode = name + "-decode-" + string(stream.ID)
		}
		if profile.planArm != nil {
			stagePlan, err := profile.planArm(p, arm, stream)
			if err != nil {
				return nil, err
			}
			armPlan.stage = stagePlan
		}
		if armPlan.stage == nil && profile.armExpected != nil {
			stagePlan, err := p.solveArmConversion(rt, stream, sets[i].name)
			if err != nil {
				return nil, err
			}
			armPlan.stage = stagePlan
		}
		p.arms = append(p.arms, armPlan)
	}
	// The join's output is a normal stream point from here on: derive the joined
	// stream once and let taps, branches, the optional encoder, and the sink all
	// compose against it.
	p.joined = profile.joinedStream(p)
	p.joinedDomain = p.deriveJoinedDomain()
	taps, err := joinPlanTaps(spec, name, p.joined, p.joinedDomain, pipeline.NodeRef(name))
	if err != nil {
		return nil, err
	}
	p.taps = taps
	switch {
	case len(spec.branches) != 0:
		if err := p.planJoinBranches(); err != nil {
			return nil, err
		}
	case spec.encode != nil:
		request := encodeRequest{name: name + "-encode", selector: av.StreamSelector{Type: profile.media}, config: encodeConfigFromSpec(*spec.encode)}
		config, encodedStream, err := prepareEncodeConfig(p.joined, request, rt.realtime)
		if err != nil {
			return nil, err
		}
		p.encode = &request
		p.encodeConfig = config
		p.encodedStream = encodedStream
		p.destination = spec.dest.spec
	default:
		if spec.dest.spec.sink == nil {
			return nil, &BuildError{Code: name + "_destination", Operation: "build " + name, Node: name, Reason: profile.sinkOnlyReason, Cause: ErrUnsupportedBuild}
		}
		p.destination = spec.dest.spec
	}
	return p, nil
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
	plan, planned, err := planShapeConversion(rt, actual, expected, rt.realtime)
	if err != nil {
		step := OperationSpec{Kind: info.OpTransform, Component: p.name}
		return nil, shapeSolverAdapterError("build "+p.name, firstNonEmpty(armName, string(stream.ID)), 0, step, actual, expected, err)
	}
	if !planned || !p.profile.armPolicy.Covers(plan.needed) {
		return nil, joinArmError(p.name, firstNonEmpty(armName, string(stream.ID)),
			fmt.Sprintf("%s arm %q cannot be converted to the join format (%s -> %s)", p.name, firstNonEmpty(armName, string(stream.ID)), humanizeShape(actual), humanizeShape(expected)))
	}
	p.diagnostics = append(p.diagnostics, info.Diagnostic{
		Code: "shape_conversion_inserted",
		Node: firstNonEmpty(armName, string(stream.ID)),
		Message: fmt.Sprintf("inserted %s on %s arm %q (join arm policy)",
			plan.detail, p.name, firstNonEmpty(armName, string(stream.ID))),
		Details: []string{
			"adapter=" + plan.factory,
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + expected.String(),
		},
	})
	return &joinArmStagePlan{
		transform: mediaTransform{
			name:    p.name + "-" + plan.factory + "-" + string(stream.ID),
			factory: plan.factory,
			video:   plan.operation.Transform.Resize,
			audio:   plan.operation.Transform.Resample,
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

// deriveJoinedDomain reports the media domain of the join's output: kinds that
// decode their arms always converge frames; passthrough kinds forward the first
// arm's domain unchanged.
func (p *joinPlan) deriveJoinedDomain() shape.MediaDomain {
	if p.profile.decodeArms {
		return shape.DomainFrame
	}
	if len(p.arms) != 0 && p.arms[0].domain != "" {
		return p.arms[0].domain
	}
	return shape.DomainFrame
}

// planJoinBranches plans the joined stream's fanout through the same
// branch-composition planner the recipe path uses: each BranchSpec is validated
// with validateBranchSpec, lowered to a streamBuild, planned with
// planBranchCompositionRecipe, and prepared with prepareBranchComposePlan —
// anchored at the join node instead of a demuxed source, with no second fanout
// implementation.
func (p *joinPlan) planJoinBranches() error {
	name := p.name
	parentPacket := p.joinedDomain == shape.DomainPacket
	// The synthesized parent stream stands in for the joined stream point: frame
	// joins read like a decoded stream, packet joins like a .Copy() point, so the
	// shared planned-branch helpers see the shapes they already know.
	parent := &jobStreamBuild{name: name, selector: av.StreamSelector{Type: p.joined.Type}}
	if parentPacket {
		parent.operations = []OperationSpec{operationSpecForCopy(codec.Copy())}
	}
	destinations := &Job{name: name}
	builds := make([]streamBuild, 0, len(p.join.branches))
	for i := range p.join.branches {
		branch := p.join.branches[i]
		if err := validateBranchSpec(p.joined.Type, parentPacket, i, branch); err != nil {
			return err
		}
		if err := p.validateJoinBranchAnchor(branch); err != nil {
			return err
		}
		if err := destinations.addBranchDestinations(branch.destinations...); err != nil {
			return err
		}
		operations := plannedBranchPrivateOperationSpecs(parent, branch, parentPacket)
		builds = append(builds, streamBuild{
			name:             branch.name,
			selector:         av.StreamSelector{Type: p.joined.Type},
			decode:           !parentPacket || chainHasDecode(branch.operations),
			decodeCodec:      chainDecodeCodec(branch.operations),
			operations:       operations,
			privateOps:       operations,
			destinationNames: append([]string(nil), branchDestinationNames(branch.destinations)...),
		})
	}
	for i := range destinations.branchDestinations {
		if err := destinations.branchDestinations[i].output.validate("build "+name, fmt.Sprintf("output-%d", i)); err != nil {
			return err
		}
	}
	intent := Intent{Name: name}
	for i := range builds {
		intent.Streams = append(intent.Streams, branchStreamIntent(builds[i]))
	}
	plan, err := planBranchCompositionRecipe(intent, InputSpec{}, destinations.branchDestinations, builds)
	if err != nil {
		return err
	}
	routes, targets, err := prepareBranchComposePlan(plan)
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

// validateJoinBranchAnchor resolves a branch's .From(...) against the join: the
// joined stream is the only planned anchor a join offers, so an explicit tap
// must name one of the join-level taps (which alias the join node); anything
// else raises the same branch_tap_missing error the chain path raises.
func (p *joinPlan) validateJoinBranchAnchor(branch BranchSpec) error {
	if branch.source.tap == "" {
		return nil
	}
	for _, tap := range p.join.taps {
		if tap.name != branch.source.tap {
			continue
		}
		from := TapRef{name: branch.source.tap, domain: branch.source.tapDomain}
		return validateTapDomain("build branches", firstNonEmpty(branch.name, "branch"), from, p.joinedDomain)
	}
	return plannedBranchTapMissingError(p.name, branch.name, branch.source.tap)
}

// joinPlanTaps validates the join-level taps and converts them into the same
// workTap records the recipe compiler installs on its tasks, anchored at the
// join node — so a join tap shows up in task.Taps() and anchors runtime
// branches exactly like a tap declared on an ordinary stream chain.
func joinPlanTaps(spec *joinSpec, name string, joined av.Stream, domain shape.MediaDomain, node pipeline.NodeRef) ([]workTap, error) {
	if len(spec.taps) == 0 {
		return nil, nil
	}
	taps := make([]workTap, 0, len(spec.taps))
	for _, tap := range spec.taps {
		if tap.name == "" {
			return nil, &BuildError{
				Code:      "tap_invalid",
				Operation: "build " + name,
				Node:      name,
				Reason:    "tap name is empty",
				Suggestions: []string{
					"call .Tap(goav.FrameTap(\"" + name + ".out\")) or another stable tap ref",
					"omit .Tap(...) when no runtime branch should attach at that point",
				},
				Cause: ErrUnsupportedBuild,
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
			After:     info.OpStage,
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
// named after the kind and a direct runtime buffer is replaced by the profile's
// buffered policy so control-plane injection works (Select).
func (p *joinPlan) graphConfig() pipeline.GraphConfig {
	buffer := p.runtime.buffer
	if p.profile.graphBuffer != nil && buffer.IsDirect() {
		buffer = *p.profile.graphBuffer
	}
	return pipeline.GraphConfig{Name: "goav-" + p.name, Buffer: buffer, Realtime: p.runtime.realtime}
}

// spec emits the planned join graph: per-arm source (+ optional decode + arm
// stage) sub-chains, the N-to-1 convergence into the join node, and the
// downstream chain — encode→destination, sink, or the planned branch fanout
// through the shared branch-compose planner. The lowering follows the same
// order, so Describe() equals the built graph.
func (p *joinPlan) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav-" + p.name, Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, len(p.arms)*3+4)
	joinRef := pipeline.NodeRef(p.name)
	sourceNames := p.armSourceNames()
	for i := range p.arms {
		arm := p.arms[i]
		sourceName := sourceNames[i]
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, pipeline.NodeRef(sourceName), arm.input.graphSourceNodeDetail()); err != nil {
			return pipeline.Spec{}, err
		}
		upstream := pipeline.NodeRef(sourceName)
		if arm.decodeNode != "" {
			detail := decodeRequestDetail(decodeRequest{selector: av.StreamSelector{Type: arm.stream.Type}})
			if err := addPlannedNode(nodes, &spec, arm.decodeNode, pipeline.NodeStage, pipeline.NodeRef(arm.decodeNode), detail); err != nil {
				return pipeline.Spec{}, err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: pipeline.NodeRef(arm.decodeNode), Policy: pipeline.RouteAll})
			upstream = pipeline.NodeRef(arm.decodeNode)
		}
		if arm.stage != nil {
			stageName := arm.stage.transform.name
			if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, pipeline.NodeRef(stageName), mediaTransformDetail(arm.stage.transform)); err != nil {
				return pipeline.Spec{}, err
			}
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: pipeline.NodeRef(stageName), Policy: pipeline.RouteAll})
			upstream = pipeline.NodeRef(stageName)
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: upstream, To: joinRef, Policy: pipeline.RouteAll})
	}
	if err := addPlannedNode(nodes, &spec, p.name, pipeline.NodeStage, joinRef, joinSyncNodeDetail(p.join.sync)); err != nil {
		return pipeline.Spec{}, err
	}
	switch {
	case len(p.branchRoutes) != 0:
		return planBranchComposeRoutes(spec, nodes, []pipeline.NodeRef{joinRef}, p.branchRoutes, p.branchTargets)
	case p.encode != nil:
		encodeName := encodeNodeName(*p.encode)
		if err := addPlannedNode(nodes, &spec, encodeName, pipeline.NodeStage, pipeline.NodeRef(encodeName), encodeNodeDetail(*p.encode)); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: joinRef, To: pipeline.NodeRef(encodeName), Policy: pipeline.RouteAll})
		destRef, err := p.planDestinationNode(&spec, nodes)
		if err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: pipeline.NodeRef(encodeName), To: destRef, Policy: pipeline.RouteAll})
		return spec, nil
	default:
		destRef, err := p.planDestinationNode(&spec, nodes)
		if err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: joinRef, To: destRef, Policy: pipeline.RouteAll})
		return spec, nil
	}
}

// armSourceNames resolves the per-arm source node names through the shared
// input-name disambiguation, so the planned spec and the lowering agree.
func (p *joinPlan) armSourceNames() []string {
	inputs := make([]InputSpec, len(p.arms))
	for i := range p.arms {
		inputs[i] = p.arms[i].input
	}
	return graphSourceNodeNames(inputs)
}

// joinDestinationNodeName names the single-destination node exactly as the
// lowering does: sinks keep their own name, mux destinations use muxNodeName.
func (p *joinPlan) joinDestinationNodeName() string {
	if p.destination.sink != nil {
		return firstNonEmpty(p.destination.sink.Name(), p.destination.label("sink"))
	}
	return muxNodeName(p.destination.output, 0)
}

func (p *joinPlan) planDestinationNode(spec *pipeline.Spec, nodes map[string]plannedNode) (pipeline.NodeRef, error) {
	name := p.joinDestinationNodeName()
	ref := pipeline.NodeRef(name)
	if p.destination.sink != nil {
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSink, ref, describedNodeDetail(p.destination.sink)); err != nil {
			return "", err
		}
		return ref, nil
	}
	detail := outputNodeDetailWithFormat(p.destination.output, destinationGraphFormat(p.destination))
	if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, detail); err != nil {
		return "", err
	}
	return ref, nil
}

// lower executes the planned join against the graph: open each arm through the
// input seam, decode/stage per plan, converge into the kind's stage, then
// deliver through the shared downstream path. Node names come from the plan, so
// the built graph equals the planned spec.
func (p *joinPlan) lower(ctx context.Context, _ graphPlan, graph pipeline.Graph, service *builder) error {
	rt := p.runtime
	armRefs := make([]string, 0, len(p.arms))
	armIDs := make([]av.StreamID, 0, len(p.arms))
	seen := make(map[av.StreamID]struct{}, len(p.arms))
	sourceNames := p.armSourceNames()
	for i := range p.arms {
		arm := p.arms[i]
		source, streams, _, err := arm.input.openGraphSource(ctx, service, sourceNames[i])
		if err != nil {
			return err
		}
		stream, err := selectStream(streams, av.StreamSelector{Type: p.profile.media})
		if err != nil {
			source.Close()
			return err
		}
		if _, dup := seen[stream.ID]; dup {
			source.Close()
			return joinArmError(p.name, string(stream.ID), p.name+" arms must have distinct stream ids")
		}
		seen[stream.ID] = struct{}{}
		srcRef, err := graph.AddSource(source, rt.buffer)
		if err != nil {
			source.Close()
			return err
		}
		upstream := string(srcRef)
		if arm.decodeNode != "" {
			request := decodeRequest{selector: av.StreamSelector{Type: stream.Type}}
			decodeStage, err := service.newDecodeStageNamed(ctx, arm.decodeNode, request, stream, rt.realtime, true, codec.DecodeBounds{})
			if err != nil {
				return err
			}
			if upstream, err = insertJoinArmStage(graph, rt, decodeStage, upstream); err != nil {
				return err
			}
		}
		if arm.stage != nil {
			stage, _, err := service.newMediaTransformStageNamed(ctx, arm.stage.transform.name, arm.stage.transform, arm.stage.stream, rt.realtime)
			if err != nil {
				return err
			}
			if upstream, err = insertJoinArmStage(graph, rt, stage, upstream); err != nil {
				return err
			}
		}
		armRefs = append(armRefs, upstream)
		armIDs = append(armIDs, stream.ID)
	}
	stage, pinned := p.profile.newStage(p, armIDs)
	stageBuffer := rt.buffer
	if pinned != nil {
		stageBuffer = *pinned
	}
	joinRef, err := graph.AddStage(stage, stageBuffer)
	if err != nil {
		return err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(joinRef)}, Policy: pipeline.RouteAll}); err != nil {
			return err
		}
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
		// openMuxDestinationStage records the destination transaction (file
		// commit/abort) on service; buildGraphPlanTask carries it to the task so
		// the file finalizes.
		return compileEncodeDestinationPath(ctx, service, graph, joinRef, *p.encode, p.encodeConfig, p.encodedStream, []destinationSpec{p.destination})
	default:
		sinkRef, err := graph.AddSink(p.destination.sink, rt.buffer)
		if err != nil {
			return err
		}
		return graph.Connect(pipeline.Route{From: string(joinRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll})
	}
}

// insertJoinArmStage appends a per-arm stage after upstream and returns its ref.
func insertJoinArmStage(graph pipeline.Graph, rt *runtime, stage pipeline.Stage, upstream string) (string, error) {
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
// per arm (its decode/stage operations), one joined branch anchored on the
// info.OpJoin node carrying the downstream chain, and — for fanouts — one branch per
// planned branch route. The N-to-1 convergence rides workPlan.Edges, copied
// from the planned spec.
func (p *joinPlan) buildJoinWorkPlan(state *recipeCompileState, spec pipeline.Spec) workPlan {
	intent := state.intent
	outputs := planOutputs(intent.Destinations, state.outputFormatMap())
	work := workPlan{
		Name:         firstNonEmpty(spec.Name, "goav-"+p.name),
		Realtime:     spec.Realtime,
		Inputs:       workInputsFromIntent(intent.Inputs),
		Taps:         append([]workTap(nil), p.taps...),
		Destinations: workDestinationsFromPlan(outputs),
		Diagnostics:  clonePlanDiagnostics(p.diagnostics),
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
	armShape := joinedShape
	for i := range p.arms {
		arm := p.arms[i]
		branchName := firstNonEmpty(arm.inputName, fmt.Sprintf("arm-%d", i))
		current := normalizeTapShape(shape.FromStream(arm.stream, arm.domain))
		sourceShape := current
		index := 0
		var ops []string
		if arm.decodeNode != "" {
			out := current
			out.Domain = shape.DomainFrame
			operation := workOperation{
				ID:        workOperationIDForKind(branchName, index, info.OpDecode),
				Name:      arm.decodeNode,
				Kind:      info.OpDecode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.decodeNode),
				Component: codecComponent(arm.stream.Codec.ID),
				Detail:    "packets to frames",
				ShapeIn:   current,
				ShapeOut:  out,
			}
			operations = append(operations, operation)
			ops = append(ops, operation.ID)
			index++
			current = out
		}
		if arm.stage != nil {
			out := shape.Merge(current, mediaShapeFromTransform(transformSpecFromMediaTransform(arm.stage.transform)))
			out.Domain = shape.DomainFrame
			operation := workOperation{
				ID:        workOperationIDForKind(branchName, index, info.OpTransform),
				Name:      arm.stage.transform.name,
				Kind:      info.OpTransform,
				Branch:    branchName,
				Node:      pipeline.NodeRef(arm.stage.transform.name),
				Component: firstNonEmpty(arm.stage.transform.factory, arm.stage.transform.name, "transform"),
				Detail:    "transform frames",
				ShapeIn:   current,
				ShapeOut:  out,
			}
			operations = append(operations, operation)
			ops = append(ops, operation.ID)
			index++
			current = out
		}
		if i == 0 {
			armShape = current
		}
		branches = append(branches, workBranch{
			ID:          workBranchID(branchName, len(branches)),
			Name:        branchName,
			Input:       branchName,
			Stream:      streamSelectFromStream(arm.stream),
			SourceShape: sourceShape,
			Operations:  ops,
		})
	}
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

// joinedWorkBranch renders the joined stream's branch: the info.OpJoin convergence
// node plus — in single-destination mode — the optional encode and the terminal
// destination operation.
func (p *joinPlan) joinedWorkBranch(ids map[string]string, outputs []planOutput, armShape shape.Spec, joinedShape shape.Spec, branchIndex int) ([]workOperation, workBranch) {
	branchName := p.name
	joinDetail := "converge " + strconv.Itoa(len(p.arms)) + " arms"
	if p.join.sync == joinSyncPTS {
		joinDetail += " by pts"
	}
	operations := []workOperation{{
		ID:        workOperationIDForKind(branchName, 0, info.OpJoin),
		Name:      p.name,
		Kind:      info.OpJoin,
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
		if p.encode != nil {
			encoded := normalizeTapShape(shape.FromStream(p.encodedStream, shape.DomainPacket))
			operations = append(operations, workOperation{
				ID:        workOperationIDForKind(branchName, index, info.OpEncode),
				Name:      encodeNodeName(*p.encode),
				Kind:      info.OpEncode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(encodeNodeName(*p.encode)),
				Component: string(p.encodeConfig.Parameters.ID),
				Detail:    "frames to packets",
				ShapeIn:   current,
				ShapeOut:  encoded,
			})
			index++
			current = encoded
		}
		if len(outputs) != 0 {
			output := outputs[0]
			node := pipeline.NodeRef(p.joinDestinationNodeName())
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
			ID:        workOperationIDForKind(branchName, index, info.OpSelect),
			Name:      selectNode,
			Kind:      info.OpSelect,
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
				ID:        workOperationIDForKind(branchName, index, info.OpDecode),
				Name:      decodeNode,
				Kind:      info.OpDecode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(decodeNode),
				Component: codecComponent(p.joined.Codec.ID),
				Detail:    "packets to frames",
				ShapeIn:   current,
				ShapeOut:  out,
			})
			current = out
		case route.copy && p.joinedDomain == shape.DomainPacket:
			appendOperation(workOperation{
				ID:        workOperationIDForKind(branchName, index, info.OpCopy),
				Name:      "packet-copy",
				Kind:      info.OpCopy,
				Branch:    branchName,
				Component: "packet-copy",
				Detail:    "preserve encoded packets",
				ShapeIn:   current,
				ShapeOut:  current,
			})
		}
		if transforms, err := branchComposePrivateOperationTransforms(route); err == nil {
			for j := range transforms {
				kind := info.OpTransform
				if transforms[j].stage != nil {
					kind = info.OpStage
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
				ID:        workOperationIDForKind(branchName, index, info.OpEncode),
				Name:      encodeNode,
				Kind:      info.OpEncode,
				Branch:    branchName,
				Node:      pipeline.NodeRef(encodeNode),
				Component: string(route.request.config.Parameters.ID),
				Detail:    "frames to packets",
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

// --- compile-state normalization ---

// joinIntent renders the joinSpec as the recipe Intent the one compile carries:
// one input per arm and the join's destinations. Arm and destination validation
// stays in newJoinPlan; this is the descriptive surface only.
func joinIntent(job *Job) Intent {
	spec := job.join
	intent := Intent{Name: string(spec.kind)}
	if rt, ok := job.runtime.(*runtime); ok {
		intent.Policies.Realtime = rt.realtime
	}
	for _, arm := range spec.arms {
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			break
		}
		intent.Inputs = append(intent.Inputs, arm.job.inputs[0].intent())
	}
	if len(spec.branches) != 0 {
		named, _ := joinBranchNamedDestinations(string(spec.kind), spec.branches)
		for i := range named {
			intent.Destinations = append(intent.Destinations, named[i].output.intentWithName(named[i].name))
		}
		return intent
	}
	intent.Destinations = append(intent.Destinations, spec.dest.spec.intentWithName(""))
	return intent
}

// joinArmInputs captures the arm input attachments aligned with the intent's
// inputs (stopping at the first malformed arm, which newJoinPlan rejects).
func joinArmInputs(spec *joinSpec) []InputSpec {
	inputs := make([]InputSpec, 0, len(spec.arms))
	for _, arm := range spec.arms {
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			break
		}
		inputs = append(inputs, arm.job.inputs[0])
	}
	return inputs
}

// joinOutputAttachments captures the join's destination attachments for the
// compile state (format resolution); newJoinPlan re-validates them.
func joinOutputAttachments(spec *joinSpec) ([]destinationSpec, []string) {
	if len(spec.branches) == 0 {
		return []destinationSpec{spec.dest.spec}, []string{""}
	}
	named, _ := joinBranchNamedDestinations(string(spec.kind), spec.branches)
	outputs := make([]destinationSpec, 0, len(named))
	names := make([]string, 0, len(named))
	for i := range named {
		outputs = append(outputs, named[i].output)
		names = append(names, named[i].name)
	}
	return outputs, names
}

// joinBranchNamedDestinations collects the branch destinations through the same
// dedupe the planned-branch path uses.
func joinBranchNamedDestinations(name string, branches []BranchSpec) ([]namedDestinationSpec, error) {
	destinations := &Job{name: name}
	for i := range branches {
		if err := destinations.addBranchDestinations(branches[i].destinations...); err != nil {
			return nil, err
		}
	}
	return destinations.branchDestinations, nil
}
