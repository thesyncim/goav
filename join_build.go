package goav

import (
	"context"

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
// arms converge into one stage and continue to a single destination, optionally
// through an encoder.
type joinSpec struct {
	kind   joinKind
	arms   []*jobStreamBuilder
	dest   Destination
	encode *CodecSpec // mix/composite only; nil delivers the raw join output
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
	// encodeStream derives the joined stream fed to the encoder when the spec
	// carries a CodecSpec; nil marks the kind sink-only (no .Encode support).
	encodeStream func(b *joinBuild) av.Stream
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
func newJoinJob(kind joinKind, arms []*jobStreamBuilder, dest Destination, encode *CodecSpec) *Job {
	name := string(kind)
	job := &Job{name: name, runtime: Default()}
	if len(arms) < 2 {
		job.setErr(&BuildError{Code: name + "_inputs", Operation: "build " + name, Node: name, Reason: name + " requires at least two source arms", Cause: ErrUnsupportedBuild})
		return job
	}
	job.join = &joinSpec{kind: kind, arms: arms, dest: dest, encode: encode}
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

	// mix: the first arm's audio format is the join target; later arms that
	// differ get a resample inserted by the mix armStage hook.
	targetRate     int
	targetChannels int
	// composite: per-arm canvas placement collected by its armStage hook.
	layouts []compositeLayout
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
	if spec.encode != nil && profile.encodeStream != nil {
		request := encodeRequest{name: name + "-encode", selector: av.StreamSelector{Type: profile.media}, config: encodeConfigFromSpec(*spec.encode)}
		config, encodedStream, err := prepareEncodeConfig(profile.encodeStream(b), request, rt.realtime)
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
		return newTask(graph, rt, service.destinationTxs...), nil
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
	return newTask(graph, rt), nil
}
