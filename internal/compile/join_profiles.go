package compile

import (
	"cmp"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// JoinKind names a convergence workflow. Public sugar (Mix, Composite, Select)
// lowers to one joinSpec and one planned join lowering; no composition code
// dispatches on the kind outside the joinProfiles table.
type JoinKind string

const (
	JoinMix       JoinKind = recipeir.JoinMix
	JoinComposite JoinKind = recipeir.JoinComposite
	JoinSelect    JoinKind = recipeir.JoinSelect
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
func resolveJoinProfile(tree *joinTreeSnapshot) (joinProfile, error) {
	if tree.Custom != nil {
		return customJoinProfile(tree.Custom)
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
type joinTreeSnapshot struct {
	Kind   JoinKind
	Sync   JoinSyncMode
	Custom *customJoinSpec
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

// customJoinSpec carries the caller-supplied convergence stage behind a
// joinSpec; customJoinProfile derives the per-kind profile from it.
type customJoinSpec struct {
	Name  string
	Stage pipeline.Stage
}

// customJoinProfile derives a caller-supplied join's profile — everything the
// built-in table carries, from the stage's declared shape.Contract and the
// join's validated name. It is the closure proof in code: no per-kind switch
// consults anything a third party cannot provide.
func customJoinProfile(custom *customJoinSpec) (joinProfile, error) {
	if custom == nil {
		return joinProfile{}, customJoinStageError("join", "custom join carries no stage")
	}
	name := custom.Name
	if err := validateCustomJoinName(name); err != nil {
		return joinProfile{}, err
	}
	if custom.Stage == nil {
		return joinProfile{}, customJoinStageError(name, "custom join stage is nil")
	}
	if stageName := custom.Stage.Name(); stageName != name {
		return joinProfile{}, customJoinStageError(name,
			"custom join stage reports Name() "+strconv.Quote(stageName)+", want the join name "+strconv.Quote(name))
	}
	contract, _ := custom.Stage.(shape.Contract)
	var inputs shape.Set
	if contract != nil {
		inputs = contract.InputShapes()
	}
	decodeArms := customJoinDecodesArms(inputs)
	profile := joinProfile{
		media:      customJoinMedia(inputs),
		DecodeArms: decodeArms,
		joinedStream: func(p *JoinPlan) av.Stream {
			return customJoinedStream(p, contract, decodeArms)
		},
		sinkOnlyReason: name + " to a non-Sink destination is not supported (a custom join carries no .Encode)",
		sinkOnlySuggestions: []string{
			"deliver the joined stream with .To(goav.Sink(sink))",
			"encode the joined stream in its own fork when it must reach muxed destinations: goav.Join(...).Buffer(flow.Blocking(8)).Encode(codec...).To(...)",
		},
	}
	if expected, ok := customJoinArmExpected(inputs); ok {
		profile.armExpected = func(*JoinPlan, av.Stream) shape.Spec { return expected }
		profile.ConformTarget = func(*JoinPlan, av.Stream) shape.Spec { return expected }
		profile.armPolicy = shape.AllowResample().Union(shape.AllowResize()).Union(shape.AllowConvert())
	}
	return profile, nil
}

// validateCustomJoinName enforces the join-name contract: snake-safe (it is
// the error-code family), and not a built-in kind (which would shadow the
// table entry).
func validateCustomJoinName(name string) error {
	if !joinNameSnakeSafe(name) {
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(errcode.JoinNameInvalid),
			Code:      errcode.JoinNameInvalid,
			Operation: "build join",
			Node:      cmp.Or(name, "join"),
			Reason:    "custom join name " + strconv.Quote(name) + " is not snake-safe ([a-z][a-z0-9_]*)",
			Fixes: builderr.Fixes([]string{
				"name the join with lowercase letters, digits, and underscores: goav.Join(\"crossfade\", stage, streams...)",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	if _, reserved := joinProfiles[JoinKind(name)]; reserved {
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(errcode.JoinNameInvalid),
			Code:      errcode.JoinNameInvalid,
			Operation: "build join",
			Node:      name,
			Reason:    "custom join name " + strconv.Quote(name) + " is reserved by the built-in join of that kind",
			Fixes: builderr.Fixes([]string{
				"pick a distinct name: the name is the join's node name, output stream id, and error-code family",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	return nil
}

// customJoinNameCollisionError refuses a custom join whose name is already
// claimed by another join node of the same tree: a custom stage cannot be
// auto-renamed (the caller built it with its output id), so the collision is
// rejected instead of silently suffixed.
func customJoinNameCollisionError(name string, claimed string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.JoinNameInvalid),
		Code:      errcode.JoinNameInvalid,
		Operation: "build " + name,
		Node:      claimed,
		Reason:    "custom join name " + strconv.Quote(name) + " collides with another join node in the same join tree",
		Fixes: builderr.Fixes([]string{
			"give each goav.Join its own name (the name is the node name and the joined stream id)",
			"construct a distinct stage instance per join — a stage carries run state",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// customJoinStageError is the malformed-stage refusal (nil stage, name
// mismatch).
func customJoinStageError(name string, reason string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.JoinStageInvalid),
		Code:      errcode.JoinStageInvalid,
		Operation: "build " + cmp.Or(name, "join"),
		Node:      cmp.Or(name, "join"),
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"pass a pipeline.Stage whose Name() equals the join name: goav.Join(\"crossfade\", stage, streams...)",
			"see the Join doc for the convergence-stage contract (audio mix is the reference implementation)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// joinNameSnakeSafe reports whether a join name fits the [a-z][a-z0-9_]*
// shape — the same vocabulary the errcode catalog uses, so the derived
// per-kind codes (<name>_arm, ...) stay well-formed.
func joinNameSnakeSafe(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
		case i > 0 && (c >= '0' && c <= '9' || c == '_'):
		default:
			return false
		}
	}
	return true
}

// customJoinDecodesArms derives the arm domain from the stage's input
// contract: a contract accepting only frame-domain media requires decoded arms
// (Mix-shaped); a packet or unconstrained contract — or none — passes arms
// through as-is (Select-shaped).
func customJoinDecodesArms(inputs shape.Set) bool {
	if len(inputs) == 0 {
		return false
	}
	for i := range inputs {
		if inputs[i].Domain != shape.DomainFrame {
			return false
		}
	}
	return true
}

// customJoinMedia derives the per-arm stream selector from the input
// contract: one media kind shared by every input shape selects it; mixed or
// absent kinds keep the join media-agnostic (each arm's single stream).
func customJoinMedia(inputs shape.Set) av.MediaType {
	var media av.MediaType
	for i := range inputs {
		kind := inputs[i].MediaKind
		if kind == "" {
			return ""
		}
		if media == "" {
			media = kind
			continue
		}
		if media != kind {
			return ""
		}
	}
	return media
}

// customJoinArmExpected derives the per-arm format contract the shape solver
// enforces: exactly one frame-domain input shape carrying format facts. The
// expected spec keeps only the convertible facts, so the solver plans the
// same per-arm conversions Mix plans for mismatched arms.
func customJoinArmExpected(inputs shape.Set) (shape.Spec, bool) {
	if len(inputs) != 1 || inputs[0].Domain != shape.DomainFrame {
		return shape.Spec{}, false
	}
	expected := shape.Spec{
		MediaKind:    inputs[0].MediaKind,
		Width:        inputs[0].Width,
		Height:       inputs[0].Height,
		PixelFormat:  inputs[0].PixelFormat,
		SampleRate:   inputs[0].SampleRate,
		Channels:     inputs[0].Channels,
		SampleFormat: inputs[0].SampleFormat,
	}
	if expected.Width == 0 && expected.Height == 0 && expected.PixelFormat == "" &&
		expected.SampleRate == 0 && expected.Channels == 0 && expected.SampleFormat == "" {
		return shape.Spec{}, false
	}
	return expected, true
}

// customJoinedStream derives a custom join's output stream: the first arm's
// facts under the join's name — frame joins rebuild from declared facts like
// Mix/Composite, passthrough joins forward the arm stream like Select — with
// the stage contract's declared output facts overlaid when present.
func customJoinedStream(p *JoinPlan, contract shape.Contract, decodeArms bool) av.Stream {
	stream, base := p.CustomJoinBaseStream(decodeArms)
	if contract == nil {
		return stream
	}
	outputs := contract.OutputShapes(base)
	if len(outputs) == 0 {
		return stream
	}
	return applyShapeToStream(stream, outputs[0])
}

// CustomJoinBaseStream resolves the fallback joined stream and the input spec
// the stage contract's OutputShapes is consulted with.
func (p *JoinPlan) CustomJoinBaseStream(decodeArms bool) (av.Stream, shape.Spec) {
	if decodeArms {
		base := p.firstArmSourceShape()
		return streamFromJoinShape(p.Name, base), base
	}
	stream := p.Arms[0].joinStream()
	stream.ID = av.StreamID(p.Name)
	return stream, shape.FromStream(stream, p.Arms[0].Domain)
}

// CustomJoinedOutputDomain reports the output domain a custom join's stage
// contract declares ("" without one): the joined-domain derivation honors it
// over the decode-arms default.
func (p *JoinPlan) CustomJoinedOutputDomain() shape.MediaDomain {
	if p.Tree.Custom == nil || len(p.Arms) == 0 {
		return ""
	}
	contract, ok := p.Tree.Custom.Stage.(shape.Contract)
	if !ok {
		return ""
	}
	_, base := p.CustomJoinBaseStream(p.Profile.DecodeArms)
	outputs := contract.OutputShapes(base)
	if len(outputs) == 0 {
		return ""
	}
	return outputs[0].Domain
}

// streamFromJoinShape builds the joined output stream of a frame-domain
// custom join from declared format facts — the union of the Mix and Composite
// derivations (decoded media carries no codec id).
func streamFromJoinShape(name string, spec shape.Spec) av.Stream {
	return av.Stream{
		ID:   av.StreamID(name),
		Type: spec.MediaKind,
		Codec: av.CodecParameters{
			Type:         spec.MediaKind,
			Width:        spec.Width,
			Height:       spec.Height,
			PixelFormat:  spec.PixelFormat,
			SampleRate:   spec.SampleRate,
			Channels:     spec.Channels,
			SampleFormat: spec.SampleFormat,
			ClockRate:    uint32(max(spec.SampleRate, 0)),
		},
	}
}

// applyShapeToStream overlays the non-empty facts of a contract-declared
// output spec onto the joined stream, leaving undeclared facts at their
// first-arm fallback.
func applyShapeToStream(stream av.Stream, spec shape.Spec) av.Stream {
	if spec.MediaKind != "" {
		stream.Type = spec.MediaKind
		stream.Codec.Type = spec.MediaKind
	}
	if spec.Codec != "" {
		stream.Codec.ID = spec.Codec
	}
	if spec.Width != 0 {
		stream.Codec.Width = spec.Width
	}
	if spec.Height != 0 {
		stream.Codec.Height = spec.Height
	}
	if spec.PixelFormat != "" {
		stream.Codec.PixelFormat = spec.PixelFormat
	}
	if spec.SampleRate != 0 {
		stream.Codec.SampleRate = spec.SampleRate
		stream.Codec.ClockRate = uint32(spec.SampleRate)
	}
	if spec.Channels != 0 {
		stream.Codec.Channels = spec.Channels
	}
	if spec.SampleFormat != "" {
		stream.Codec.SampleFormat = spec.SampleFormat
	}
	return stream
}

// mixJoinProfile is Mix's entry in the join table: audio arms, explicit
// packet-arm decode, first-arm-wins format solving through the shared shape
// solver (the implicit always-on arm policy — no user opt-in needed),
// audioMixStage convergence, and an encodable S16 output stream derived from
// the first arm's shape.
var mixJoinProfile = joinProfile{
	media:      av.MediaAudio,
	DecodeArms: true,
	graphBuffer: &pipeline.BufferPolicy{
		Capacity: RealtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	},
	graphBufferRealtimeOnly: true,
	// The first arm's audio format is the mix target; later arms that differ
	// get a conversion planned by the shape solver under the arm policy.
	armExpected: func(p *JoinPlan, stream av.Stream) shape.Spec {
		if stream.Codec.SampleRate <= 0 {
			return shape.Spec{}
		}
		channels := max(stream.Codec.Channels, 1)
		if p.targetRate == 0 {
			p.targetRate, p.targetChannels = stream.Codec.SampleRate, channels
			return shape.Spec{}
		}
		return shape.Frame(av.MediaAudio, shape.Audio(p.targetRate, p.targetChannels, ""))
	},
	armPolicy: shape.AllowResample().Union(shape.AllowConvert()),
	// An arm whose format is known only once it opens converts at run time to
	// the first arm's rate and channels in S16, or to the encoder's explicit
	// rate and channels when the first arm is such an arm too.
	ConformTarget: func(p *JoinPlan, first av.Stream) shape.Spec {
		rate, channels := first.Codec.SampleRate, first.Codec.Channels
		if rate > 0 {
			channels = max(channels, 1)
		}
		if p.lateTarget {
			rate = firstPositiveInt(p.target.SampleRate, rate)
			channels = firstPositiveInt(p.target.Channels, channels)
		}
		return shape.Frame(av.MediaAudio, shape.Audio(rate, channels, av.SampleFormatS16))
	},
	conformFollowsFirstArm: true,
	// The output id is the join's planned node name (mix, or mix-2 when
	// nested under another mix); the format facts come from the first arm —
	// a leaf's declared source shape or a sub-join's joined stream.
	joinedStream: func(p *JoinPlan) av.Stream {
		shape := p.firstArmSourceShape()
		return av.Stream{
			ID:   av.StreamID(p.Name),
			Type: av.MediaAudio,
			Codec: av.CodecParameters{
				Type:         av.MediaAudio,
				SampleRate:   shape.SampleRate,
				Channels:     shape.Channels,
				SampleFormat: shape.SampleFormat,
				ClockRate:    uint32(shape.SampleRate),
			},
		}
	},
	sinkOnlyReason: "mix to a non-Sink destination requires .Encode(...)",
	sinkOnlySuggestions: []string{
		"encode the mixed audio first: goav.Mix(a, b).Encode(codec.Opus(codec.Bitrate(96_000))).To(out)",
		"deliver raw mixed frames with .To(goav.Sink(sink)) instead",
	},
}

// compositeRegion is an arm's top-left placement on the composite canvas. It is
// composite-only — it has no meaning outside a Composite arm.
type compositeRegion struct {
	x, y int
}

// compositeJoinProfile is Composite's entry in the join table: video arms,
// explicit packet-arm decode, per-arm Region layout collection,
// videoCompositeStage convergence, and an encodable output stream derived from
// the Region bounding box over every arm's frame shape.
var compositeJoinProfile = joinProfile{
	placesArms: true,
	media:      av.MediaVideo,
	DecodeArms: true,
	graphBuffer: &pipeline.BufferPolicy{
		Capacity: RealtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	},
	graphBufferRealtimeOnly: true,
	// Each arm places its frame on the canvas at its declared Region — a chain
	// arm's .Region(x, y) or a nested composite's — defaulting to the top-left
	// corner {0,0}.
	planArm: func(p *JoinPlan, arm joinArmSnapshot, _ av.Stream) (*joinArmStagePlan, error) {
		layout := CompositeLayout{}
		if arm.region != nil {
			layout = CompositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		p.Layouts = append(p.Layouts, layout)
		return nil, nil
	},
	// The canvas takes each arm at its own size; an arm whose format is known
	// only once it opens converts to I420 at run time.
	ConformTarget: func(*JoinPlan, av.Stream) shape.Spec {
		return shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420))
	},
	// The output id is the join's planned node name (composite, or composite-2
	// when nested); the geometry facts are the full Region bounding box over
	// all arms, not the first arm's tile.
	joinedStream: func(p *JoinPlan) av.Stream {
		spec := compositeJoinedOutputShape(p)
		return av.Stream{
			ID:   av.StreamID(p.Name),
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				Type:        av.MediaVideo,
				Width:       spec.Width,
				Height:      spec.Height,
				PixelFormat: spec.PixelFormat,
			},
		}
	},
	sinkOnlyReason: "composite to a non-Sink destination requires .Encode(...)",
	sinkOnlySuggestions: []string{
		"encode the composited video first: goav.Composite(a, b).Encode(codec.VP8(codec.Bitrate(1_000_000))).To(out)",
		"deliver raw composited frames with .To(goav.Sink(sink)) instead",
	},
}

func compositeJoinedOutputShape(p *JoinPlan) shape.Spec {
	if p == nil || len(p.Arms) == 0 {
		return shape.Frame(av.MediaVideo)
	}
	spec := shape.Frame(av.MediaVideo, shape.Stream(av.StreamID(p.Name)))
	canvasW, canvasH := 0, 0
	missingGeometry := false
	missingPixelFormat := false
	unsupportedPixelFormat := ""
	for i := range p.Arms {
		arm := compositeArmFrameShape(p.Arms[i])
		if arm.Width <= 0 || arm.Height <= 0 {
			missingGeometry = true
		} else {
			layout := compositeArmLayout(p, i)
			canvasW = max(canvasW, layout.X+arm.Width)
			canvasH = max(canvasH, layout.Y+arm.Height)
		}
		switch arm.PixelFormat {
		case "":
			missingPixelFormat = true
		case av.PixelFormatI420:
		default:
			if unsupportedPixelFormat == "" {
				unsupportedPixelFormat = arm.PixelFormat
			}
		}
	}
	if !missingGeometry {
		spec.Width = canvasW
		spec.Height = canvasH
	}
	switch {
	case missingPixelFormat:
		spec.PixelFormat = ""
	case unsupportedPixelFormat != "":
		spec.PixelFormat = unsupportedPixelFormat
	default:
		spec.PixelFormat = av.PixelFormatI420
	}
	return spec
}

func compositeArmFrameShape(arm joinArmPlan) shape.Spec {
	if arm.sub != nil && !arm.late {
		return shape.FromStream(arm.sub.Joined, arm.sub.joinedDomain)
	}
	return shape.FromStream(arm.joinStream(), shape.DomainFrame)
}

func compositeArmLayout(p *JoinPlan, index int) CompositeLayout {
	if p == nil || index < 0 || index >= len(p.Layouts) {
		return CompositeLayout{}
	}
	return p.Layouts[index]
}

// CompositeLayout places one input's top-left corner on the output canvas. One
// layout entry pairs with each input arm, so the compositor knows where to copy
// that arm's frame.
type CompositeLayout struct {
	X, Y int
}

// SelectBufferCapacity sizes the non-lossy queues on the Select graph. It is small
// — a passthrough switch holds no real backlog — but large enough that an injected
// SelectActive control rides alongside live arm traffic without forcing a stall.
const SelectBufferCapacity = 32

// selectJoinProfile is Select's entry in the join table: media-agnostic
// passthrough arms (the zero media selector picks each arm's single stream, so a
// frame or packet arm switches as-is), no decode, selectorStage convergence,
// sink-only delivery.
var selectJoinProfile = joinProfile{
	// Select is driven live by SelectActive through the control plane, which
	// needs a per-node serial worker — i.e. a buffered graph. A direct (zero)
	// runtime buffer would build a direct graph with no injector, so default it
	// to a non-lossy buffered policy. An explicitly configured buffer is
	// honoured as-is.
	graphBuffer: &pipeline.BufferPolicy{Capacity: SelectBufferCapacity, Drop: pipeline.DropBlock},
	// The switched output is the first arm's stream forwarded as-is under the
	// selector's output id (its planned node name) — Select is passthrough, so
	// the arm's codec and format facts describe the joined stream. A nested
	// join arm contributes its joined stream the same way.
	joinedStream: func(p *JoinPlan) av.Stream {
		stream := p.Arms[0].joinStream()
		stream.ID = av.StreamID(p.Name)
		return stream
	},
	sinkOnlyReason: "select to a non-Sink destination is not supported yet",
	sinkOnlySuggestions: []string{
		"deliver the selected stream with .To(goav.Sink(sink))",
		"encode the selected stream when it must reach muxed destinations: goav.Select(a, b).Encode(codec...).To(out)",
	},
}

// JoinSyncMode selects how a convergent join (Mix/Composite) aligns its arms.
type JoinSyncMode int

const (
	// joinSyncArrival pairs arm frames by arrival: one buffered frame per arm
	// per step, no timestamp math. Right for live sources on one clock; the
	// default.
	JoinSyncArrival JoinSyncMode = iota
	// joinSyncPTS aligns arm frames by presentation time on a common
	// nanosecond clock — opt-in via LegacyMix(...).SyncByPTS() and
	// LegacyComposite(...).SyncByPTS().
	JoinSyncPTS
)

// JoinSyncNodeDetail renders the join node's planned/built graph detail.
// Arrival is the default and stays unannotated, so existing plans are
// byte-identical; the planner and the stages' DescribeNode share this one
// function, keeping Describe() ≡ Build().
func JoinSyncNodeDetail(mode JoinSyncMode) string {
	if mode == JoinSyncPTS {
		return "sync=pts"
	}
	return ""
}
