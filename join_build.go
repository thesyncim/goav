package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
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

const joinStagePreallocDepthLimit = 64

func joinStagePreallocDepth(rt *Runtime) int {
	if rt == nil || rt.buffer.IsDirect() {
		return 1
	}
	depth := max(rt.buffer.Capacity, 1)
	if depth > joinStagePreallocDepthLimit {
		return joinStagePreallocDepthLimit
	}
	return depth
}

// joinProfile is the per-kind configuration consulted by the join planner.
// Everything that differs between Mix, Composite and Select lives here — the
// plan skeleton is written once. The profile name is the joinKind: it doubles
// as the join node name, graph-name suffix and error-code prefix.
type joinProfile struct {
	// media selects each arm's stream; the zero value picks the arm's single
	// stream regardless of type (Select is media-agnostic passthrough).
	media av.MediaType
	// decodeArms marks joins whose convergence stage consumes frames. Packet
	// chain arms must declare .Decode() explicitly; packet tap/nested arms are
	// rejected instead of gaining hidden decode work.
	decodeArms bool
	// placesArms marks joins that position arms on a canvas (Composite), so an
	// arm may carry .Region(x, y); every other join refuses one.
	placesArms bool
	// graphBuffer, when non-nil, replaces a direct runtime buffer so the graph
	// gets per-node serial workers for control-plane injection (Select) or for
	// realtime join arms that must start concurrently (Mix/Composite).
	graphBuffer             *pipeline.BufferPolicy
	graphBufferRealtimeOnly bool
	// planArm runs per arm at plan time after the optional decode decision to
	// record arm state (composite layouts). Format solving does NOT live here —
	// it goes through the shape solver via armExpected/armPolicy.
	planArm func(p *joinPlan, arm joinArmSnapshot, stream av.Stream) (*joinArmStagePlan, error)
	// armExpected derives the format facts every arm must satisfy before the
	// join stage (mix: the first arm's audio format is the target). A zero Spec
	// skips solving for that arm. The needed conversions are planned through the
	// one shape solver under armPolicy — the join's implicit always-on policy —
	// and lowered as real per-arm transform stages.
	armExpected func(p *joinPlan, stream av.Stream) shape.Spec
	armPolicy   shape.Policy
	// conformTarget is the frame shape an arm whose format is known only once
	// it opens converts to at run time, given the first arm's resolved stream
	// (zero facts are kept from the arriving frame). Nil joins forward arms
	// as they arrive.
	conformTarget func(p *joinPlan, first av.Stream) shape.Spec
	// conformFollowsFirstArm marks a join whose format is its first arm's
	// (Mix): when that arm is late, every arm converts at run time.
	conformFollowsFirstArm bool
	// newStage builds the convergence stage at lowering time. A non-nil buffer
	// pins the stage's input queue (Select: non-lossy DropBlock so injected
	// controls survive).
	newStage func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy)
	// joinedStream derives the join's output stream — the normal stream point
	// that taps, branches, and the optional encoder all compose against.
	joinedStream func(p *joinPlan) av.Stream
	// sinkOnlyReason is the destination error when dest is not a frame Sink and
	// no encode applies; sinkOnlySuggestions carry the kind's concrete fixes.
	sinkOnlyReason      string
	sinkOnlySuggestions []string
}

// joinProfiles is the single per-kind table; each entry lives next to its
// public sugar (audio_mix.go, video_composite_build.go, select_build.go).
var joinProfiles = map[joinKind]joinProfile{
	joinMix:       mixJoinProfile,
	joinComposite: compositeJoinProfile,
	joinSelect:    selectJoinProfile,
}

// resolveJoinProfile resolves one join spec's per-kind profile: the built-in
// table for Mix/Composite/Select, the contract-derived profile for a custom
// goav.Join (join_custom.go). The unknown-kind refusal guards the internal
// invariant that every non-custom spec was constructed by a built-in builder.
func resolveJoinProfile(tree *joinTreeSnapshot) (joinProfile, error) {
	if tree.custom != nil {
		return customJoinProfile(tree.custom)
	}
	profile, ok := joinProfiles[tree.kind]
	if !ok {
		kind := string(tree.kind)
		return joinProfile{}, &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(joinErrorCode(kind, "kind")),
			Code:      joinErrorCode(kind, "kind"),
			Operation: "build " + kind,
			Node:      kind,
			Reason:    "unknown join kind",
			fields:    errDetails(errNote("declared join kinds: mix, composite, select; custom kinds go through goav.Join")),
			fixes: buildErrorFixes([]string{
				"use Mix, Composite, or Select for the built-in convergence profiles",
				"use goav.Join(name, stage, streams...) for a custom convergence stage",
			}),
			cause: errUnsupportedBuild,
		}
	}
	return profile, nil
}

// joinErrorCode derives a join refusal code from the join kind and family:
// joinErrorCode("mix", "arm") == errcode.Code("mix_arm"). Built-in and custom
// profile codes share the same suffix families; nested joins of a repeated kind
// carry their claimed node name (mix-2_arm), and the "kind" family marks the
// internal unknown-join-kind invariant.
func joinErrorCode(kind string, family string) errcode.Code {
	return errcode.Code(kind + "_" + family)
}

// joinTreeSnapshot is the join a planner reads: its kind, arm alignment, and
// a custom join's stage.
type joinTreeSnapshot struct {
	kind   joinKind
	sync   joinSyncMode
	custom *customJoinSpec
}

// joinArmSnapshot is what a join's profile reads of one arm: its canvas
// position.
type joinArmSnapshot struct {
	region *compositeRegion
}
