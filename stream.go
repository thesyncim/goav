package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// Stream is one media stream of a recipe: an input's selected stream, a join's
// output, or the result of an operation on another Stream. It is an immutable
// value: every method returns a new Stream and leaves its receiver unchanged,
// so a Stream used twice fans out and the work before the fork runs once.
//
//	v := in.Video().Decode()
//	hd := v.Resize(1280, 720).Encode(codec.VP9())
//	sd := v.Resize(640, 360).Encode(codec.VP9())
//	err := rt.Run(ctx, hd.To(hdOut), sd.To(sdOut))
//
// A Stream does nothing until a runtime runs the Outputs built from it.
type Stream struct {
	node *streamNode
}

// streamNode is one node of the recipe tree a Stream points into: a root (an
// input's selection or a join) or one operation on its parent.
type streamNode struct {
	parent *streamNode
	root   *streamRoot
	op     streamOp
	// err is a construction error, reported when the recipe compiles.
	err error
}

type streamRoot struct {
	input   InputSpec
	media   av.MediaType
	options []StreamOption
	join    *streamJoin
	// rule roots a stream the input discovers while it runs (OnStream); tap
	// and track root a stream of a running task (task.Tap, task.Track), route
	// one of a graph route (task.Route).
	rule  *sourcepkg.StreamMatch
	tap   string
	track *av.Stream
	route *pipeline.Route
}

type streamJoin struct {
	kind   string
	name   string
	stage  pipeline.Stage
	layers []joinLayer
	sync   bool
}

type streamOpKind uint8

const (
	streamOpDecode streamOpKind = iota + 1
	streamOpEncode
	streamOpResize
	streamOpResample
	streamOpDo
	streamOpPlayout
	streamOpSync
	streamOpTap
	streamOpBuffer
	streamOpAuto
	streamOpRequire
	streamOpPrefer
	streamOpShape
	streamOpName
)

type streamOp struct {
	kind         streamOpKind
	codecOptions []codec.Option
	encode       codec.Spec
	width        int
	height       int
	resize       []resizeOption
	rate         int
	channels     int
	audio        []audioOption
	stages       []pipeline.Stage
	playout      flow.PlayoutPolicy
	sync         flow.SyncPolicy
	name         string
	buffer       flow.BranchBuffer
	policies     []shape.Policy
	spec         shape.Spec
}

// Video selects the input's video stream; with several, StreamID or
// StreamIndex narrows the choice.
func (s InputSpec) Video(options ...StreamOption) Stream {
	return s.selectStream(av.MediaVideo, options)
}

// Audio selects the input's audio stream; with several, StreamID or
// StreamIndex narrows the choice.
func (s InputSpec) Audio(options ...StreamOption) Stream {
	return s.selectStream(av.MediaAudio, options)
}

// OnStream is each stream the input discovers while it runs that match
// selects: Outputs built from it attach for every such stream, named per
// stream, and detach when the stream ends. The attach happens on the source's
// emit path, as GStreamer's pad-added does: the source's Emit of the
// av.EventStreamAdded announce returns once the branch exists, so the branch
// receives the stream from its first packet. A rule destination's Open
// therefore runs on the source's goroutine, bounded by the task's context;
// keep it fast or hand slow setup to a goroutine of its own.
func (s InputSpec) OnStream(match sourcepkg.StreamMatch) Stream {
	return Stream{node: &streamNode{root: &streamRoot{input: s, rule: &match}}}
}

// Stream selects one stream of any media kind, narrowed by StreamID or
// StreamIndex when the input carries several.
func (s InputSpec) Stream(options ...StreamOption) Stream {
	return s.selectStream(av.MediaUnknown, options)
}

func (s InputSpec) selectStream(media av.MediaType, options []StreamOption) Stream {
	return Stream{node: &streamNode{root: &streamRoot{
		input:   s,
		media:   media,
		options: append([]StreamOption(nil), options...),
	}}}
}

func (s Stream) then(op streamOp) Stream {
	return Stream{node: &streamNode{parent: s.node, op: op}}
}

// Decode turns the stream's packets into frames.
func (s Stream) Decode(options ...codec.Option) Stream {
	return s.then(streamOp{kind: streamOpDecode, codecOptions: append([]codec.Option(nil), options...)})
}

// Encode turns frames into packets of the given codec.
func (s Stream) Encode(spec codec.Spec) Stream {
	return s.then(streamOp{kind: streamOpEncode, encode: spec})
}

// Resize scales decoded video frames to width x height.
func (s Stream) Resize(width int, height int, options ...resizeOption) Stream {
	return s.then(streamOp{kind: streamOpResize, width: width, height: height, resize: append([]resizeOption(nil), options...)})
}

// Resample converts decoded audio to sampleRate and channels.
func (s Stream) Resample(sampleRate int, channels int, options ...audioOption) Stream {
	return s.then(streamOp{kind: streamOpResample, rate: sampleRate, channels: channels, audio: append([]audioOption(nil), options...)})
}

// Do runs custom stages on the stream: frame stages on decoded frames, packet
// stages (a bitstream filter) on packets, before or after the encoder.
func (s Stream) Do(stages ...pipeline.Stage) Stream {
	return s.then(streamOp{kind: streamOpDo, stages: append([]pipeline.Stage(nil), stages...)})
}

// Playout holds media until its time is due on the task timeline.
func (s Stream) Playout(policy flow.PlayoutPolicy) Stream {
	return s.then(streamOp{kind: streamOpPlayout, playout: policy})
}

// Sync places the stream on a shared media timeline; reuse one policy across
// streams to align them.
func (s Stream) Sync(policy flow.SyncPolicy) Stream {
	return s.then(streamOp{kind: streamOpSync, sync: policy})
}

// Tap names this point of the stream as a stable outlet: it is listed by
// task.Taps() and runtime branches attach to it.
func (s Stream) Tap(name string) Stream {
	return s.then(streamOp{kind: streamOpTap, name: name})
}

// Buffer runs the rest of this fork behind its own queue, so a slow consumer
// backpressures or sheds alone.
func (s Stream) Buffer(buffer flow.BranchBuffer) Stream {
	return s.then(streamOp{kind: streamOpBuffer, buffer: buffer})
}

// Auto lets the planner insert the conversions the policies allow.
func (s Stream) Auto(policies ...shape.Policy) Stream {
	return s.then(streamOp{kind: streamOpAuto, policies: append([]shape.Policy(nil), policies...)})
}

// Require fails the build unless the stream has the given shape here.
func (s Stream) Require(spec shape.Spec) Stream {
	return s.then(streamOp{kind: streamOpRequire, spec: spec})
}

// Prefer asks the planner for the given shape here when it can convert.
func (s Stream) Prefer(spec shape.Spec) Stream {
	return s.then(streamOp{kind: streamOpPrefer, spec: spec})
}

// Shape declares the stream's shape after a custom stage changes it.
func (s Stream) Shape(spec shape.Spec) Stream {
	return s.then(streamOp{kind: streamOpShape, spec: spec})
}

// As names the stream from here on: a fork's name is its node prefix and the
// id of the stream it encodes, which live controls address
// (control.SetBitrate("hd", ...)).
func (s Stream) As(name string) Stream {
	return s.then(streamOp{kind: streamOpName, name: name})
}

// JoinArg is one argument of Mix or Composite: a Stream (an input of the
// join), a Stream placed [At] a position (Composite only), or a join option
// such as [ByPTS]. It is sealed: only goav values implement it.
type JoinArg interface {
	joinArg()
}

func (Stream) joinArg() {}

type positionedLayer struct {
	stream Stream
	x, y   int
}

func (positionedLayer) joinArg() {}

// joinOption is the concrete join option (ByPTS).
type joinOption struct {
	byPTS bool
}

func (joinOption) joinArg() {}

// At places a Composite input's top-left corner at (x, y) on the canvas.
func At(s Stream, x, y int) JoinArg {
	return positionedLayer{stream: s, x: x, y: y}
}

// ByPTS aligns a Mix or Composite's inputs by presentation time instead of
// arrival order: goav.Mix(a, b, goav.ByPTS()).
func ByPTS() JoinArg {
	return joinOption{byPTS: true}
}

// Mix sums audio streams into one. Options such as ByPTS go among the streams.
func Mix(args ...JoinArg) Stream {
	return newJoinStream(recipeir.JoinMix, "", nil, args)
}

// Composite draws video streams onto one canvas, in order, each at its At
// position (the canvas origin for a bare Stream). Options such as ByPTS go
// among the streams.
func Composite(args ...JoinArg) Stream {
	return newJoinStream(recipeir.JoinComposite, "", nil, args)
}

// Select forwards one of its streams at a time; control.SelectActive switches
// which.
func Select(streams ...Stream) Stream {
	return newJoinStream(recipeir.JoinSelect, "", nil, joinArgs(streams))
}

// Join converges streams into a custom stage whose Name equals name.
func Join(name string, stage pipeline.Stage, streams ...Stream) Stream {
	return newJoinStream("", name, stage, joinArgs(streams))
}

func joinArgs(streams []Stream) []JoinArg {
	args := make([]JoinArg, len(streams))
	for i := range streams {
		args[i] = streams[i]
	}
	return args
}

// joinLayer is one input of a join: a stream and, on a Composite, its
// position.
type joinLayer struct {
	stream Stream
	x, y   int
	placed bool
}

func newJoinStream(kind string, name string, stage pipeline.Stage, args []JoinArg) Stream {
	join := &streamJoin{kind: kind, name: name, stage: stage}
	label := firstNonEmpty(kind, name)
	for i, arg := range args {
		switch arg := arg.(type) {
		case Stream:
			join.layers = append(join.layers, joinLayer{stream: arg})
		case positionedLayer:
			if kind != recipeir.JoinComposite {
				return Stream{node: &streamNode{err: joinAtOutsideCompositeError(label, i)}}
			}
			join.layers = append(join.layers, joinLayer{stream: arg.stream, x: arg.x, y: arg.y, placed: true})
		case joinOption:
			join.sync = join.sync || arg.byPTS
		default:
			return Stream{node: &streamNode{err: joinLayerNilError(label, i)}}
		}
	}
	return Stream{node: &streamNode{root: &streamRoot{join: join}}}
}

// Output routes a Stream to destinations. A runtime runs Outputs together:
// streams written to one Destination value share its container.
type Output struct {
	stream Stream
	// remux is the input every stream of which copies to the destinations.
	remux   *InputSpec
	dests   []Destination
	failure BranchFailurePolicy
}

// BranchFailurePolicy determines the scope of a returned handler error.
type BranchFailurePolicy string

const (
	// FailTask is the default: the first handler error stops the task.
	FailTask BranchFailurePolicy = "fail_task"
	// IsolateBranch retires this Output's exclusive suffix on its first
	// handler error. Shared upstream errors still fail the task. Panics and
	// stalls are outside this policy; blocking queues still pace producers.
	IsolateBranch BranchFailurePolicy = "isolate_branch"
)

// OnFailure selects the failure scope for this Output at Build or Attach.
// Isolation is refused for shared destinations, joins, or unsafe egress.
func (o Output) OnFailure(policy BranchFailurePolicy) Output { o.failure = policy; return o }

// To routes the stream to destinations: packets mux into a container,
// frames or packets reach a Sink.
func (s Stream) To(destinations ...Destination) Output {
	return Output{stream: s, dests: append([]Destination(nil), destinations...)}
}

// To copies every stream of the input, without decoding, into destinations.
func (s InputSpec) To(destinations ...Destination) Output {
	input := s
	return Output{remux: &input, dests: append([]Destination(nil), destinations...)}
}
