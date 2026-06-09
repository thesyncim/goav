package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// joinKind names a convergence workflow. Public sugar (Mix, Composite, Select)
// lowers to one joinSpec and one buildJoin path; no composition code dispatches
// on the kind outside the joinProfiles table.
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
	encode *CodecSpec // mix/composite only; nil delivers the raw join output
	// taps name the joined stream as stable attach points, installed on the task
	// exactly like chain taps (visible in task.Taps(), runtime-attachable).
	taps []TapRef
	// branches fan the joined stream out to planned branch chains, each carrying
	// its own destinations; when set, dest/encode are unused.
	branches []BranchSpec
}

// joinProfile is the per-kind configuration consulted by buildJoin. Everything
// that differs between Mix, Composite and Select lives here — the build
// skeleton is written once. The profile name is the joinKind: it doubles as the
// node name, graph-name suffix and error-code prefix.
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
	// armStage runs per arm after the optional decode; it may insert per-arm
	// stages (mix resample shape-solving) or record arm state (composite
	// layouts) and returns the arm's new upstream node.
	armStage func(b *joinBuild, arm *jobStreamBuilder, stream av.Stream, upstream string) (string, error)
	// newStage builds the convergence stage. A non-nil buffer pins the stage's
	// input queue (Select: non-lossy DropBlock so injected controls survive).
	newStage func(b *joinBuild, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy)
	// joinedStream derives the join's output stream — the normal stream point
	// that taps, branches, and the optional encoder all compose against.
	joinedStream func(b *joinBuild) av.Stream
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

// joinBuild carries the in-flight build state shared between the buildJoin
// skeleton and the profile hooks.
type joinBuild struct {
	ctx     context.Context
	spec    *joinSpec
	rt      *runtime
	graph   pipeline.Graph
	service *builder

	// armStreams/armDomains record each arm's selected stream and media domain
	// in arm order, so joined-output derivation (taps, branches, select
	// passthrough) reads resolved arm state instead of re-opening inputs.
	armStreams []av.Stream
	armDomains []shape.MediaDomain

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample inserted by the mix armStage hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its armStage hook.
	layouts []compositeLayout
}

// joinedDomain reports the media domain of the join's output: kinds that decode
// their arms always converge frames; passthrough kinds forward the first arm's
// domain unchanged.
func (b *joinBuild) joinedDomain() shape.MediaDomain {
	if joinProfiles[b.spec.kind].decodeArms {
		return shape.DomainFrame
	}
	if len(b.armDomains) != 0 && b.armDomains[0] != "" {
		return b.armDomains[0]
	}
	return shape.DomainFrame
}

// insertArmStage appends a per-arm stage after upstream and returns its ref.
func (b *joinBuild) insertArmStage(stage pipeline.Stage, upstream string) (string, error) {
	ref, err := b.graph.AddStage(stage, b.rt.buffer)
	if err != nil {
		return "", err
	}
	if err := b.graph.Connect(pipeline.Route{From: upstream, To: []string{string(ref)}, Policy: pipeline.RouteAll}); err != nil {
		return "", err
	}
	return string(ref), nil
}

// buildJoin compiles a joinSpec: open each arm through the input seam, select
// its stream, optionally decode, run the per-kind arm hook, converge into the
// kind's stage, then deliver through one shared output path (encode→destination
// or frame Sink). Per-kind behavior comes only from the joinProfiles table.
func (j *Job) buildJoin(ctx context.Context) (Task, error) {
	if j.err != nil {
		return nil, j.err
	}
	rt, ok := j.runtime.(*runtime)
	if !ok {
		return nil, ErrExpertRuntimeRequired
	}
	spec := j.join
	name := string(spec.kind)
	profile, ok := joinProfiles[spec.kind]
	if !ok {
		return nil, &BuildError{Code: name + "_kind", Operation: "build " + name, Node: name, Reason: "unknown join kind", Cause: ErrUnsupportedBuild}
	}
	graphBuffer := rt.buffer
	if profile.graphBuffer != nil && graphBuffer.IsDirect() {
		graphBuffer = *profile.graphBuffer
	}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "goav-" + name, Buffer: graphBuffer, Realtime: rt.realtime})
	if err != nil {
		return nil, err
	}
	service := &builder{runtime: rt}
	b := &joinBuild{ctx: ctx, spec: spec, rt: rt, graph: graph, service: service}
	armRefs := make([]string, 0, len(spec.arms))
	armIDs := make([]av.StreamID, 0, len(spec.arms))
	seen := make(map[av.StreamID]struct{}, len(spec.arms))
	for i := range spec.arms {
		arm := spec.arms[i]
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			graph.Close()
			return nil, &BuildError{Code: name + "_arm", Operation: "build " + name, Node: name, Reason: "each " + name + " arm must be a single-input source chain", Cause: ErrUnsupportedBuild}
		}
		source, streams, domain, err := arm.job.inputs[0].openGraphSource(ctx, service, i)
		if err != nil {
			graph.Close()
			return nil, err
		}
		stream, err := selectStream(streams, av.StreamSelector{Type: profile.media})
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		id := stream.ID
		if _, dup := seen[id]; dup {
			source.Close()
			graph.Close()
			return nil, &BuildError{Code: name + "_arm", Operation: "build " + name, Node: string(id), Reason: name + " arms must have distinct stream ids", Cause: ErrUnsupportedBuild}
		}
		seen[id] = struct{}{}
		srcRef, err := graph.AddSource(source, rt.buffer)
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		upstream := string(srcRef)
		// Packet-domain arms decode to frames before the join (auto-inserted —
		// the join stage works on decoded media). Frame-domain arms feed it
		// directly. Passthrough kinds skip this and forward packets as-is.
		if profile.decodeArms && domain == shape.DomainPacket {
			request := decodeRequest{selector: av.StreamSelector{Type: stream.Type}}
			decodeStage, err := service.newDecodeStageNamed(ctx, name+"-decode-"+string(id), request, stream, rt.realtime, true, codec.DecodeBounds{})
			if err != nil {
				graph.Close()
				return nil, err
			}
			if upstream, err = b.insertArmStage(decodeStage, upstream); err != nil {
				graph.Close()
				return nil, err
			}
		}
		if profile.armStage != nil {
			if upstream, err = profile.armStage(b, arm, stream, upstream); err != nil {
				graph.Close()
				return nil, err
			}
		}
		armRefs = append(armRefs, upstream)
		armIDs = append(armIDs, id)
		b.armStreams = append(b.armStreams, stream)
		b.armDomains = append(b.armDomains, domain)
	}
	stage, pinned := profile.newStage(b, armIDs)
	stageBuffer := rt.buffer
	if pinned != nil {
		stageBuffer = *pinned
	}
	joinRef, err := graph.AddStage(stage, stageBuffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(joinRef)}, Policy: pipeline.RouteAll}); err != nil {
			graph.Close()
			return nil, err
		}
	}
	// The join's output is a normal stream point from here on: derive the joined
	// stream once and let taps, branches, the optional encoder, and the sink all
	// compose against it.
	joined := profile.joinedStream(b)
	joinedDomain := b.joinedDomain()
	taps, err := joinPlanTaps(spec, name, joined, joinedDomain, joinRef)
	if err != nil {
		graph.Close()
		return nil, err
	}
	if len(spec.branches) != 0 {
		if err := b.compileJoinBranches(joinRef, joined, joinedDomain); err != nil {
			graph.Close()
			return nil, err
		}
		joinTask := newTask(graph, rt, service.destinationTxs...)
		installTaskTaps(joinTask, taps)
		return joinTask, nil
	}
	if spec.encode != nil {
		request := encodeRequest{name: name + "-encode", selector: av.StreamSelector{Type: profile.media}, config: encodeConfigFromSpec(*spec.encode)}
		config, encodedStream, err := prepareEncodeConfig(joined, request, rt.realtime)
		if err != nil {
			graph.Close()
			return nil, err
		}
		if err := compileEncodeDestinationPath(ctx, service, graph, joinRef, request, config, encodedStream, []destinationSpec{spec.dest.spec}); err != nil {
			graph.Close()
			return nil, err
		}
		// openMuxDestinationStage records the destination transaction (file
		// commit/abort) on service; carry it to the task so the file finalizes.
		joinTask := newTask(graph, rt, service.destinationTxs...)
		installTaskTaps(joinTask, taps)
		return joinTask, nil
	}
	sink := spec.dest.spec.sink
	if sink == nil {
		graph.Close()
		return nil, &BuildError{Code: name + "_destination", Operation: "build " + name, Node: name, Reason: profile.sinkOnlyReason, Cause: ErrUnsupportedBuild}
	}
	sinkRef, err := graph.AddSink(sink, rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	if err := graph.Connect(pipeline.Route{From: string(joinRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll}); err != nil {
		graph.Close()
		return nil, err
	}
	joinTask := newTask(graph, rt)
	installTaskTaps(joinTask, taps)
	return joinTask, nil
}

// joinPlanTaps validates the join-level taps and converts them into the same
// planTap records the recipe compiler installs on its tasks, anchored at the
// join node — so a join tap shows up in task.Taps() and anchors runtime
// branches exactly like a tap declared on an ordinary stream chain.
func joinPlanTaps(spec *joinSpec, name string, joined av.Stream, domain shape.MediaDomain, node pipeline.NodeRef) ([]planTap, error) {
	if len(spec.taps) == 0 {
		return nil, nil
	}
	taps := make([]planTap, 0, len(spec.taps))
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
		taps = append(taps, planTap{
			Name:      tap.name,
			Node:      node,
			Domain:    domain,
			MediaKind: joined.Type,
			After:     OpStage,
			Shape:     shape.FromStream(joined, domain),
			Shared:    true,
		})
	}
	return taps, nil
}

// compileJoinBranches fans the joined stream out to planned branches through the
// same branch-composition lowering the recipe path uses: each BranchSpec is
// validated with validateBranchSpec, lowered to a streamBuild, planned with
// planBranchCompositionRecipe, and compiled with compileBranchComposeInputs and
// compileBranchComposeRoutes — anchored at the join node instead of a demuxed
// source, with no second fanout implementation.
func (b *joinBuild) compileJoinBranches(joinRef pipeline.NodeRef, joined av.Stream, domain shape.MediaDomain) error {
	name := string(b.spec.kind)
	parentPacket := domain == shape.DomainPacket
	// The synthesized parent stream stands in for the joined stream point: frame
	// joins read like a decoded stream, packet joins like a .Copy() point, so the
	// shared planned-branch helpers see the shapes they already know.
	parent := &jobStreamBuild{name: name, selector: av.StreamSelector{Type: joined.Type}}
	if parentPacket {
		parent.operations = []OperationSpec{operationSpecForCopy(codec.Copy())}
	}
	destinations := &Job{name: name}
	builds := make([]streamBuild, 0, len(b.spec.branches))
	for i := range b.spec.branches {
		branch := b.spec.branches[i]
		if err := validateBranchSpec(joined.Type, parentPacket, i, branch); err != nil {
			return err
		}
		if err := b.validateJoinBranchAnchor(branch, domain); err != nil {
			return err
		}
		if err := destinations.addBranchDestinations(branch.destinations...); err != nil {
			return err
		}
		operations := plannedBranchPrivateOperationSpecs(parent, branch, parentPacket)
		builds = append(builds, streamBuild{
			name:             branch.name,
			selector:         av.StreamSelector{Type: joined.Type},
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
		routes[i].sourceDomain = domain
	}
	groups, err := resolveBranchComposeStreamGroups([]av.Stream{joined}, routes)
	if err != nil {
		return err
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(b.ctx, b.rt, b.graph, []pipeline.NodeRef{joinRef}, groups, nil, routes, nil, b.rt.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(b.ctx, b.service, b.graph, routes, targets, branchInputs, branchStreams, nil, nil, b.rt.realtime)
}

// validateJoinBranchAnchor resolves a branch's .From(...) against the join: the
// joined stream is the only planned anchor a join offers, so an explicit tap
// must name one of the join-level taps (which alias the join node); anything
// else raises the same branch_tap_missing error the chain path raises.
func (b *joinBuild) validateJoinBranchAnchor(branch BranchSpec, domain shape.MediaDomain) error {
	if branch.source.tap == "" {
		return nil
	}
	for _, tap := range b.spec.taps {
		if tap.name != branch.source.tap {
			continue
		}
		from := TapRef{name: branch.source.tap, domain: branch.source.tapDomain}
		return validateTapDomain("build branches", firstNonEmpty(branch.name, "branch"), from, domain)
	}
	return plannedBranchTapMissingError(string(b.spec.kind), branch.name, branch.source.tap)
}
