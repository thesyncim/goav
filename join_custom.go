// Custom joins: goav.Join lowers a caller-supplied convergence stage through
// the SAME joinSpec/joinProfile machinery as Mix/Composite/Select — the
// built-ins are profiles over this seam, not privileged citizens. Everything
// the per-kind profile table carries is derived here from the stage's
// declared shape.Contract (arm domain, media kind, arm format solving, joined
// output) or from the join's name (node name, output stream id, error-code
// family), so a third party ships a Crossfade(arms...) without touching core.

package goav

import (
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Join converges N arms through a caller-supplied convergence stage — the
// extension form of the built-in joins. Mix, Composite, and Select lower
// through exactly this machinery; a custom join is their equal: it plans
// through the one recipe compile (Describe() ≡ Build(), plan.OpJoin in
// Explain), its joined output is a normal stream point (.Tap, .Branches,
// .To), and the result is itself a JoinArm, so custom joins nest inside and
// around the built-ins (Mix(Join("pair", stage, a, b), c)).
//
// The stage contract (audioMixStage in audio_mix.go is the reference
// implementation the built-ins follow):
//
//   - name is the join's identity: its planned node name, the joined output
//     stream id, and its error-code family (<name>_inputs, <name>_arm, ...).
//     It must be snake-safe ([a-z][a-z0-9_]*), not a built-in kind (mix,
//     composite, select), and unique within one join tree; stage.Name() must
//     report exactly name.
//   - The stage is the convergence node: Handle receives every arm's
//     messages, identified by Frame/Packet/Event.StreamID. Handle is called
//     serially per node (the pipeline.Stage contract), so per-arm state needs
//     no locking. The caller constructs the stage with whatever it needs —
//     typically the arm stream ids it declared (source names, tap names,
//     nested join output names).
//   - The stage emits the joined output under its own output id,
//     av.StreamID(name) — taps, branches, and destinations compose against
//     that id.
//   - Per-arm events arrive on Handle: track av.EventEndOfStream per arm and
//     emit ONE joined end-of-stream (under the output id) once every arm has
//     ended. Forward control-plane events riding the data path unchanged
//     (an event whose Reason targets a downstream stage, e.g. SelectActive);
//     re-stamping them would erase their target.
//   - A stage instance carries run state: build the Job once per instance.
//
// Per-arm behavior derives from the stage's shape.Contract when implemented:
//
//   - InputShapes() all in the frame domain → packet arms decode before the
//     stage, exactly like Mix/Composite; otherwise arms pass through as-is,
//     exactly like Select. A uniform MediaKind across the input shapes
//     selects each arm's stream (.Audio()-like); mixed or absent kinds keep
//     the join media-agnostic.
//   - A single frame-domain input shape carrying format facts (sample rate /
//     channels / sample format, size / pixel format) is the arm contract:
//     arms that differ get conversions planned by the shape solver under the
//     join's implicit arm policy and lowered as real per-arm stages, the same
//     service Mix enjoys.
//   - OutputShapes(first arm's shape) describes the joined stream; absent a
//     contract (or with an empty output set) the joined stream falls back to
//     the first arm's facts under the join's name.
//
// Without a contract the join is a media-agnostic passthrough (Select-shaped).
// Custom joins carry no .Encode — deliver frames to sinks with .To, or fan
// out with .Branches where branches encode for muxed destinations.
func Join(name string, stage pipeline.Stage, arms ...JoinArm) *joinStream {
	return &joinStream{name: name, stage: stage, arms: arms}
}

// joinStream is the builder a custom Join returns: the same grammar surface
// the built-in join builders expose (Tap/Branches/To), plus JoinArm so a
// custom join nests like any other arm.
type joinStream struct {
	name  string
	stage pipeline.Stage
	arms  []JoinArm
	taps  []TapRef
}

// customJoinSpec carries the caller-supplied convergence stage behind a
// joinSpec; customJoinProfile derives the per-kind profile from it.
type customJoinSpec struct {
	name  string
	stage pipeline.Stage
}

// spec lowers the builder to the one joinSpec shared by every convergence
// builder; the join kind is the custom name.
func (s *joinStream) spec() joinSpec {
	return joinSpec{
		kind:   joinKind(s.name),
		arms:   s.arms,
		taps:   s.taps,
		custom: &customJoinSpec{name: s.name, stage: s.stage},
	}
}

// joinArm lets a custom join stand as an arm of an outer join: the outer join
// consumes the JOINED output stream under the custom join's output id (its
// name), exactly like a nested Mix contributes its mixed stream.
func (s *joinStream) joinArm() joinArmSpec {
	if s == nil {
		return joinArmSpec{}
	}
	spec := s.spec()
	return joinArmSpec{join: &spec}
}

// Tap names the joined stream as a stable attach point — the same tap a
// normal chain declares: it appears in task.Taps() and runtime branches can
// Attach from it later. Its domain follows the join's output (frame-domain
// for decode-arm joins, the arms' domain for passthrough joins).
func (s *joinStream) Tap(tap TapRef) *joinStream {
	s.taps = append(s.taps, tap)
	return s
}

// Branches fans the joined stream out to planned branch chains, each with its
// own destinations — the same goav.Branch specs an ordinary stream chain
// accepts at this stream point. Branches that encode reach muxed
// destinations; the join itself stays sink-only.
func (s *joinStream) Branches(branches ...BranchSpec) *Job {
	spec := s.spec()
	if job, refused := s.refuse(spec); refused {
		return job
	}
	return newJoinBranchesJob(spec.kind, spec, branches)
}

// To delivers the joined stream to one or more sink destinations (a fanout
// when several are given) and returns a Job, so the custom join runs through
// the same Build/Run as every other recipe. It lowers to the one joinSpec
// shared by every convergence builder.
func (s *joinStream) To(destinations ...Destination) *Job {
	spec := s.spec()
	spec.dests = destinations
	if job, refused := s.refuse(spec); refused {
		return job
	}
	return newJoinJob(spec.kind, spec)
}

// refuse pre-validates the custom join's identity (name and stage) so a bad
// Join surfaces its precise refusal (join_name_invalid / join_stage_invalid)
// instead of a derived one; nested arms hit the same validation through
// resolveJoinProfile at plan time.
func (s *joinStream) refuse(spec joinSpec) (*Job, bool) {
	if _, err := customJoinProfile(spec.custom); err != nil {
		job := newJob(firstNonEmpty(s.name, "join"))
		job.setErr(err)
		return job, true
	}
	return nil, false
}

// customJoinProfile derives a caller-supplied join's profile — everything the
// built-in table carries, from the stage's declared shape.Contract and the
// join's validated name. It is the closure proof in code: no per-kind switch
// consults anything a third party cannot provide.
func customJoinProfile(custom *customJoinSpec) (joinProfile, error) {
	if custom == nil {
		return joinProfile{}, customJoinStageError("join", "custom join carries no stage")
	}
	name := custom.name
	if err := validateCustomJoinName(name); err != nil {
		return joinProfile{}, err
	}
	if custom.stage == nil {
		return joinProfile{}, customJoinStageError(name, "custom join stage is nil")
	}
	if stageName := custom.stage.Name(); stageName != name {
		return joinProfile{}, customJoinStageError(name,
			"custom join stage reports Name() "+strconv.Quote(stageName)+", want the join name "+strconv.Quote(name))
	}
	contract, _ := custom.stage.(shape.Contract)
	var inputs shape.Set
	if contract != nil {
		inputs = contract.InputShapes()
	}
	decodeArms := customJoinDecodesArms(inputs)
	profile := joinProfile{
		media:      customJoinMedia(inputs),
		decodeArms: decodeArms,
		newStage: func(*joinPlan, []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
			return custom.stage, nil
		},
		joinedStream: func(p *joinPlan) av.Stream {
			return customJoinedStream(p, contract, decodeArms)
		},
		sinkOnlyReason: name + " to a non-Sink destination is not supported (a custom join carries no .Encode)",
		sinkOnlySuggestions: []string{
			"deliver the joined stream with .To(goav.Sink(sink))",
			"fan the joined stream out with .Branches(goav.Branch(\"out\").Encode(codec...).To(...)) when it must reach muxed destinations",
		},
	}
	if expected, ok := customJoinArmExpected(inputs); ok {
		profile.armExpected = func(*joinPlan, av.Stream) shape.Spec { return expected }
		profile.armPolicy = shape.AllowResample().Union(shape.AllowResize()).Union(shape.AllowConvert())
	}
	return profile, nil
}

// validateCustomJoinName enforces the join-name contract: snake-safe (it is
// the error-code family), and not a built-in kind (which would shadow the
// table entry).
func validateCustomJoinName(name string) error {
	if !joinNameSnakeSafe(name) {
		return &BuildError{
			Code:      errcode.JoinNameInvalid,
			Operation: "build join",
			Node:      firstNonEmpty(name, "join"),
			Reason:    "custom join name " + strconv.Quote(name) + " is not snake-safe ([a-z][a-z0-9_]*)",
			Suggestions: []string{
				"name the join with lowercase letters, digits, and underscores: goav.Join(\"crossfade\", stage, arms...)",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if _, reserved := joinProfiles[joinKind(name)]; reserved {
		return &BuildError{
			Code:      errcode.JoinNameInvalid,
			Operation: "build join",
			Node:      name,
			Reason:    "custom join name " + strconv.Quote(name) + " is reserved by the built-in join of that kind",
			Suggestions: []string{
				"pick a distinct name: the name is the join's node name, output stream id, and error-code family",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return nil
}

// customJoinNameCollisionError refuses a custom join whose name is already
// claimed by another join node of the same tree: a custom stage cannot be
// auto-renamed (the caller built it with its output id), so the collision is
// rejected instead of silently suffixed.
func customJoinNameCollisionError(name string, claimed string) error {
	return &BuildError{
		Code:      errcode.JoinNameInvalid,
		Operation: "build " + name,
		Node:      claimed,
		Reason:    "custom join name " + strconv.Quote(name) + " collides with another join node in the same join tree",
		Suggestions: []string{
			"give each goav.Join its own name (the name is the node name and the joined stream id)",
			"construct a distinct stage instance per join — a stage carries run state",
		},
		Cause: ErrUnsupportedBuild,
	}
}

// customJoinStageError is the malformed-stage refusal (nil stage, name
// mismatch).
func customJoinStageError(name string, reason string) error {
	return &BuildError{
		Code:      errcode.JoinStageInvalid,
		Operation: "build " + firstNonEmpty(name, "join"),
		Node:      firstNonEmpty(name, "join"),
		Reason:    reason,
		Suggestions: []string{
			"pass a pipeline.Stage whose Name() equals the join name: goav.Join(\"crossfade\", stage, arms...)",
			"see the Join doc for the convergence-stage contract (audio mix is the reference implementation)",
		},
		Cause: ErrUnsupportedBuild,
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
// contract: a contract accepting only frame-domain media gets decode arms
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
func customJoinedStream(p *joinPlan, contract shape.Contract, decodeArms bool) av.Stream {
	stream, base := p.customJoinBaseStream(decodeArms)
	if contract == nil {
		return stream
	}
	outputs := contract.OutputShapes(base)
	if len(outputs) == 0 {
		return stream
	}
	return applyShapeToStream(stream, outputs[0])
}

// customJoinBaseStream resolves the fallback joined stream and the input spec
// the stage contract's OutputShapes is consulted with.
func (p *joinPlan) customJoinBaseStream(decodeArms bool) (av.Stream, shape.Spec) {
	if decodeArms {
		base := p.firstArmSourceShape()
		return streamFromJoinShape(p.name, base), base
	}
	stream := p.arms[0].stream
	stream.ID = av.StreamID(p.name)
	return stream, shape.FromStream(stream, p.arms[0].domain)
}

// customJoinedOutputDomain reports the output domain a custom join's stage
// contract declares ("" without one): the joined-domain derivation honors it
// over the decode-arms default.
func (p *joinPlan) customJoinedOutputDomain() shape.MediaDomain {
	if p.join.custom == nil || len(p.arms) == 0 {
		return ""
	}
	contract, ok := p.join.custom.stage.(shape.Contract)
	if !ok {
		return ""
	}
	_, base := p.customJoinBaseStream(p.profile.decodeArms)
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
			ClockRate:    uint32(maxInt(spec.SampleRate, 0)),
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
