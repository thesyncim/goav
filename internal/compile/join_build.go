package compile

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// joinKind names a convergence workflow. Public sugar (Mix, Composite, Select)
// lowers to one joinSpec and one planned join lowering; no composition code
// dispatches on the kind outside the joinProfiles table.
type JoinKind string

const (
	JoinMix       JoinKind = "mix"
	JoinComposite JoinKind = "composite"
	JoinSelect    JoinKind = "select"
)

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
	DecodeArms bool
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
	planArm func(p *JoinPlan, arm joinArmSnapshot, stream av.Stream) (*joinArmStagePlan, error)
	// armExpected derives the format facts every arm must satisfy before the
	// join stage (mix: the first arm's audio format is the target). A zero Spec
	// skips solving for that arm. The needed conversions are planned through the
	// one shape solver under armPolicy — the join's implicit always-on policy —
	// and lowered as real per-arm transform stages.
	armExpected func(p *JoinPlan, stream av.Stream) shape.Spec
	armPolicy   shape.Policy
	// conformTarget is the frame shape an arm whose format is known only once
	// it opens converts to at run time, given the first arm's resolved stream
	// (zero facts are kept from the arriving frame). Nil joins forward arms
	// as they arrive.
	ConformTarget func(p *JoinPlan, first av.Stream) shape.Spec
	// conformFollowsFirstArm marks a join whose format is its first arm's
	// (Mix): when that arm is late, every arm converts at run time.
	conformFollowsFirstArm bool
	// joinedStream derives the join's output stream — the normal stream point
	// that taps, branches, and the optional encoder all compose against.
	joinedStream func(p *JoinPlan) av.Stream
	// sinkOnlyReason is the destination error when dest is not a frame Sink and
	// no encode applies; sinkOnlySuggestions carry the kind's concrete fixes.
	sinkOnlyReason      string
	sinkOnlySuggestions []string
}

// joinProfiles is the single per-kind table; each entry lives next to its
// public sugar (audio_mix.go, video_composite_build.go, select_build.go).
var joinProfiles = map[JoinKind]joinProfile{
	JoinMix:       mixJoinProfile,
	JoinComposite: compositeJoinProfile,
	JoinSelect:    selectJoinProfile,
}

// resolveJoinProfile resolves one join spec's per-kind profile: the built-in
// table for Mix/Composite/Select, the contract-derived profile for a custom
// goav.Join (join_custom.go). The unknown-kind refusal guards the internal
// invariant that every non-custom spec was constructed by a built-in builder.
func resolveJoinProfile(tree *JoinTreeSnapshot) (joinProfile, error) {
	if tree.Custom != nil {
		return CustomJoinProfile(tree.Custom)
	}
	profile, ok := joinProfiles[tree.Kind]
	if !ok {
		kind := string(tree.Kind)
		return joinProfile{}, &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(joinErrorCode(kind, "kind")),
			Code:      joinErrorCode(kind, "kind"),
			Operation: "build " + kind,
			Node:      kind,
			Reason:    "unknown join kind",
			Details:   builderr.Details(builderr.Note("declared join kinds: mix, composite, select; custom kinds go through goav.Join")),
			Fixes: builderr.Fixes([]string{
				"use Mix, Composite, or Select for the built-in convergence profiles",
				"use goav.Join(name, stage, streams...) for a custom convergence stage",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	return profile, nil
}

// joinTreeSnapshot is the join a planner reads: its kind, arm alignment, and
// a custom join's stage.
type JoinTreeSnapshot struct {
	Kind   JoinKind
	Sync   JoinSyncMode
	Custom *CustomJoinSpec
}

// joinArmSnapshot is what a join's profile reads of one arm: its canvas
// position.
type joinArmSnapshot struct {
	region *compositeRegion
}

// joinErrorCode derives a join refusal code from the join kind and family:
// joinErrorCode("mix", "arm") == errcode.Code("mix_arm"). Built-in and custom
// profile codes share the same suffix families; nested joins of a repeated kind
// carry their claimed node name (mix-2_arm), and the "kind" family marks the
// internal unknown-join-kind invariant.
func joinErrorCode(kind string, family string) errcode.Code {
	return errcode.Code(kind + "_" + family)
}
