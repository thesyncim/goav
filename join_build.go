package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
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

func joinStagePreallocDepth(rt *runtime) int {
	if rt == nil || rt.buffer.IsDirect() {
		return 1
	}
	depth := rt.buffer.Capacity
	if depth < 1 {
		depth = 1
	}
	if depth > joinStagePreallocDepthLimit {
		return joinStagePreallocDepthLimit
	}
	return depth
}

// joinSpec is the one internal N→1 model behind Mix/Composite/Select: N arms
// converge into one stage and continue like any other stream point — to one or
// more destinations (optionally through an encoder), out to planned branches,
// and past named taps on the joined stream.
type joinSpec struct {
	kind   joinKind
	arms   []JoinArm
	dests  []Destination
	encode *codec.Spec // mix/composite only; nil delivers the raw join output
	// operations are joined-stream annotations (Auto/Require/Prefer) that run
	// before the terminal encode or planned branches.
	operations []operationSpec
	// taps name the joined stream as stable attach points, installed on the task
	// exactly like chain taps (visible in task.Taps(), runtime-attachable).
	taps []tapRef
	// branches fan the joined stream out to planned branch chains, each carrying
	// its own destinations; when set, dest/encode are unused.
	branches []BranchSpec
	// sync selects arm alignment for the convergence stage (Mix/Composite):
	// arrival order by default, PTS alignment via .SyncByPTS(). Select needs no
	// sync — it forwards exactly one live arm.
	sync joinSyncMode
	// custom carries a caller-supplied convergence stage (goav.Join); its
	// joinProfile is derived from the stage's declared shape.Contract instead
	// of the built-in table, and kind is the join's validated name.
	custom *customJoinSpec
}

// JoinArm is one source arm of a join: an ordinary source chain such as
// From(x).Audio(), another join whose joined output feeds the outer join —
// Mix(Mix(a, b), c) sub-mixes two arms and mixes the result with a third, and
// Select(Mix(a, b), Mix(c, d)) switches between two live mixes — or a tap reference
// naming a tap an earlier arm declared, so one decoded stream converges
// mid-graph without opening its source again. It is a sealed interface: only
// goav builders implement it.
type JoinArm interface {
	// joinArm resolves the arm to its internal spec; unexported so the set of
	// arm shapes stays closed (source chains, nested joins, and tap refs).
	joinArm() joinArmSpec
}

// joinArmSpec is the resolved arm behind the sealed JoinArm interface:
// exactly one of captured source-chain facts, join (a nested join), or tap
// (a reference to a tap declared by an earlier arm) is set. region carries the
// arm's composite placement, when declared.
type joinArmSpec struct {
	chainInput      InputSpec
	chainInputOK    bool
	chainErr        error
	chainOperations []operationSpec
	join            *joinSpec
	tap             *tapRef
	region          *compositeRegion
}

// joinTreeSnapshot is the planner-facing join tree captured at the recipe
// boundary. It preserves the recursive arm shape and concrete resource handles
// needed by the current lowerer while removing planner-time calls back into
// fluent arm builders.
type joinTreeSnapshot struct {
	kind       joinKind
	arms       []joinArmSnapshot
	dests      []Destination
	encode     *codec.Spec
	operations []operationSpec
	taps       []tapRef
	branches   []joinBranchSnapshot
	sync       joinSyncMode
	custom     *customJoinSpec
}

// joinBranchSnapshot is the planner-facing capture of one join fanout branch:
// the immutable recipe-IR facts (name, media, anchor, operations) plus the
// concrete destination handles that stay outside the IR by design, and the
// builder-construction error captured at snapshot time. The join planner reads
// it instead of the mutable BranchSpec.
type joinBranchSnapshot struct {
	recipe       recipeir.JoinBranch
	destinations []destinationRef
	construction error
}

// joinBranchSnapshotsFromSpecs captures the join fanout branches at the
// builder→IR boundary: each branch's recipe facts become immutable recipe IR,
// its concrete destinations are cloned as the IR exception, and its
// builder-construction error is validated here (the only place that reads the
// mutable BranchSpec) so the planner never has to.
func joinBranchSnapshotsFromSpecs(branches []BranchSpec, selected av.MediaType) []joinBranchSnapshot {
	if len(branches) == 0 {
		return nil
	}
	out := make([]joinBranchSnapshot, 0, len(branches))
	for i := range branches {
		out = append(out, joinBranchSnapshot{
			recipe: recipeir.JoinBranch{
				Name:       branches[i].name,
				Media:      branches[i].media,
				Source:     recipeir.TapRef{Name: branches[i].source.tap, Domain: branches[i].source.tapDomain},
				Operations: recipeIROperationsFromSpecs(branches[i].operations),
			},
			destinations: append([]destinationRef(nil), branches[i].destinations...),
			construction: validateBranchConstruction(i, selected, branches[i]),
		})
	}
	return out
}

func cloneJoinBranchSnapshots(branches []joinBranchSnapshot) []joinBranchSnapshot {
	if len(branches) == 0 {
		return nil
	}
	out := make([]joinBranchSnapshot, len(branches))
	for i := range branches {
		out[i] = joinBranchSnapshot{
			recipe: recipeir.JoinBranch{
				Name:       branches[i].recipe.Name,
				Media:      branches[i].recipe.Media,
				Source:     branches[i].recipe.Source,
				Operations: append([]recipeir.Operation(nil), branches[i].recipe.Operations...),
			},
			destinations: append([]destinationRef(nil), branches[i].destinations...),
			construction: branches[i].construction,
		}
	}
	return out
}

type joinArmSnapshot struct {
	chainInput      InputSpec
	chainInputOK    bool
	chainErr        error
	chainOperations []operationSpec
	join            *joinTreeSnapshot
	tap             *tapRef
	region          *compositeRegion
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
				"use goav.Join(name, stage, arms...) for a custom convergence stage",
			}),
			cause: errUnsupportedBuild,
		}
	}
	return profile, nil
}

// newJoinJob lowers public convergence sugar into the one joinSpec carried by
// Job, so .To/Build/Run are shared by every join kind.
func newJoinJob(kind joinKind, spec joinSpec) *Job {
	name := string(kind)
	job := newJob(name)
	if len(spec.arms) < 2 {
		job.setErr(joinInputsError(name, name))
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

// joinArmInputs captures the arm input attachments aligned with the intent's
// inputs (stopping at the first malformed arm, which newJoinPlan rejects).
func joinArmInputs(tree *joinTreeSnapshot) []InputSpec {
	return joinLeafInputSpecs(tree)
}

// joinLeafInputSpecs returns every leaf arm's InputSpec in depth-first arm
// order — the order the compile state's inputs, probes, and stream sets follow
// and planJoinTree consumes. Tap arms open no input and are skipped. It stops
// at the first malformed arm, which planJoinTree rejects with a precise error.
func joinLeafInputSpecs(tree *joinTreeSnapshot) []InputSpec {
	if tree == nil {
		return nil
	}
	out := make([]InputSpec, 0, len(tree.arms))
	var walk func(t *joinTreeSnapshot) bool
	walk = func(t *joinTreeSnapshot) bool {
		for _, resolved := range t.arms {
			switch {
			case resolved.join != nil:
				if !walk(resolved.join) {
					return false
				}
			case resolved.tap != nil:
				// A tap arm attaches to an already-open stream.
			case resolved.chainInputOK:
				out = append(out, resolved.chainInput)
			default:
				return false
			}
		}
		return true
	}
	walk(tree)
	return out
}

// joinOutputAttachments captures the join's destination attachments for the
// compile state (format resolution); newJoinPlan re-validates them.
func joinOutputAttachments(spec *joinSpec) ([]destinationSpec, []string) {
	if len(spec.branches) == 0 {
		outputs := make([]destinationSpec, 0, len(spec.dests))
		names := make([]string, 0, len(spec.dests))
		for i := range spec.dests {
			outputs = append(outputs, spec.dests[i].spec)
			names = append(names, "")
		}
		return outputs, names
	}
	named, _ := joinBranchNamedDestinations(spec.branches)
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
func joinBranchNamedDestinations(branches []BranchSpec) ([]namedDestinationSpec, error) {
	var destinations []namedDestinationSpec
	for i := range branches {
		updated, err := appendNamedBranchDestinations(destinations, branches[i].destinations...)
		if err != nil {
			return nil, err
		}
		destinations = updated
	}
	return destinations, nil
}

// joinErrorCode derives a join refusal code from the join kind and family:
// joinErrorCode("mix", "arm") == errcode.Code("mix_arm"). Built-in and custom
// profile codes share the same suffix families; nested joins of a repeated kind
// carry their claimed node name (mix-2_arm), and the "kind" family marks the
// internal unknown-join-kind invariant.
func joinErrorCode(kind string, family string) errcode.Code {
	return errcode.Code(kind + "_" + family)
}
