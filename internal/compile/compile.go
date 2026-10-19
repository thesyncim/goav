package compile

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// compileRequest is everything one compile reads: the recipe DAG, its
// Outputs, the facts of the input and destination handles the DAG indexes,
// the stream matches its rules name, and the host that checks the handles.
type Request struct {
	Graph   recipeir.Graph
	Outputs []Output
	Inputs  []Input
	Dests   []Destination
	Matches []sourcepkg.StreamMatch
	Host    Host
	Env     Env
	// Preflight runs the checks a build needs before it opens anything
	// (adapters, formats, probes); Describe compiles without them. Ctx
	// bounds the host's probes.
	Preflight bool
	Ctx       context.Context
}

func (r *Request) context() context.Context {
	if r.Ctx != nil {
		return r.Ctx
	}
	return context.Background()
}

// compileEnv is the runtime settings the passes read: every field is a
// leaf type. Present is false when no runtime is configured.
type Env struct {
	Present     bool
	Codecs      *codec.SimpleRegistry
	Filters     *filter.SimpleRegistry
	Formats     *format.SimpleRegistry
	Buffer      pipeline.BufferPolicy
	Realtime    bool
	ShapeDeltas []runconfig.ShapeDeltaContributor
}

// compileOutput is one Output: the node it routes (a NodeInput for
// input.To) and its destination nodes in .To order.
type Output struct {
	From  recipeir.NodeID
	Dests []recipeir.NodeID
}

// compileInput is what the compiler knows of one input before it opens.
type Input struct {
	IR recipeir.Input
	// Name names the input in a stream selection; SourceNode and
	// SourceDetail are its planned source node.
	Name         string
	SourceNode   string
	SourceDetail string
	// Shape is the shape the input declares, if it declares one; Frames
	// reports a declared frame shape.
	Shape    shape.Spec
	HasShape bool
	Frames   bool
	// Live reports an input that delivers on its own clock.
	Live bool
	// Demuxed reports an input a demuxer opens (a file or URI).
	Demuxed bool
	// EventDomain is the domain the input's stream events arrive in.
	EventDomain shape.MediaDomain
}

// compileDestination is what the compiler knows of one destination.
type Destination struct {
	// Invalid is why the handle is unusable, or "".
	Invalid string
	// Label is the destination's own name, or "".
	Label string
	// Group identifies a destination copies of one handle share, or "".
	Group string
	IR    recipeir.Destination
	// Sink reports a sink destination: SinkName and SinkDetail name it.
	Sink       bool
	SinkName   string
	SinkDetail string
	// HasOutput reports a byte destination; Output is its address.
	HasOutput bool
	Output    format.Output
	// Format is the container asked for; ResolvedFormat the one the
	// output resolved to.
	Format         av.FormatID
	ResolvedFormat av.FormatID
}

// sharesGroup reports two destinations one handle's copies made.
func (d Destination) sharesGroup(other Destination) bool {
	return d.Group != "" && d.Group == other.Group
}

// openFormat is the container the destination opens.
func (d Destination) openFormat() av.FormatID {
	if d.ResolvedFormat != "" {
		return d.ResolvedFormat
	}
	return d.Format
}

// compileHost checks what only the concrete handles know, at the point in
// the pass order the checks have always run. order lists each recipe
// destination's index in compileRequest.Dests.
type Host interface {
	ValidateHandles(operation string, order []int, names []string) error
	ResolveOutputs(ctx context.Context, order []int, names []string) ([]av.FormatID, error)
	ProbeInputs(ctx context.Context) ([]format.ProbeResult, error)
}

// inputsLive reports whether any input delivers on its own clock.
func inputsLive(inputs []Input) bool {
	for i := range inputs {
		if inputs[i].Live {
			return true
		}
	}
	return false
}

// recipeCompileState is one recipe under compilation: its IR (the DAG, the
// branches derived from it, inputs, destinations), the concrete handles the
// IR indexes, and what the passes resolve along the way.
type State struct {
	Operation string
	Req       *Request
	Recipe    recipeir.Recipe
	Env       Env

	// facts are the lowering facts of each branch, aligned with
	// recipe.Streams.
	Facts []BranchFacts
	// remux are the whole-input copies (input.To): inputs and destinations.
	remux []remuxOutput

	// outputs are the facts of the recipe's destinations, in recipe order;
	// outputSlots index each in req.Dests.
	Outputs                []Destination
	outputSlots            []int
	OutputDestinationNames []string
	inputProbes            []format.ProbeResult
	streamRules            []StreamRulePlan

	// joins are the planned joins, by graph node.
	joins     map[recipeir.NodeID]*JoinPlan
	joinOrder []recipeir.NodeID
	// joinNames are the joins' claimed node names, by graph node.
	joinNames map[recipeir.NodeID]string
	// armDecodes are the decoders join arms planned, by decode node, and
	// armTaps the arm taps installed, so a path several joins' arms read
	// decodes and taps once.
	armDecodes map[recipeir.NodeID]string
	armTaps    map[recipeir.NodeID]bool

	// inserted are the nodes the planner inserted (packet copies and shape
	// conversions), keyed so a node shared by several branches gets one.
	Inserted map[string]recipeir.NodeID
	// destIndex indexes each output node's destination.
	destIndex map[recipeir.NodeID]int

	shapeDiagnostics []plan.Diagnostic
	// lowered reports the recipe lowered: composed is its compose graph,
	// spec the graph it describes, and work its work plan.
	lowered  bool
	composed ComposeGraph
	spec     pipeline.Spec
	work     WorkPlan
}

// branchFacts are what the lowering knows of one branch beyond its IR.
type BranchFacts struct {
	// lone marks the only branch of a root that names no fork: it lowers
	// under its root's identity.
	lone bool
	// frames reports that the branch's root carries frames.
	Frames bool
	// dests indexes the branch's destinations, in .To order.
	Dests []int
}

type remuxOutput struct {
	input int
	// nodes are the destination nodes; dests their destination indexes.
	nodes []recipeir.NodeID
	dests []int
}

type recipeCompilePass struct {
	name string
	fn   func(*State) error
}

// compileResult is what one compile resolves: the recipe IR, its planned
// joins and compose graph, the spec and work plan it lowers to, and what the
// passes learned of the inputs and destinations.
type Result struct {
	Recipe        recipeir.Recipe
	Joins         map[recipeir.NodeID]*JoinPlan
	JoinOrder     []recipeir.NodeID
	Graph         ComposeGraph
	Lowered       bool
	Spec          pipeline.Spec
	Work          WorkPlan
	InputProbes   []format.ProbeResult
	OutputFormats map[string]av.FormatID
	OutputSlots   []int
	StreamRules   []StreamRulePlan
	Name          string
	RulesSource   string
	RulesDomain   shape.MediaDomain
}

// runCompile runs the compiler passes over one recipe DAG: derive and
// validate its branches, check its inputs and destinations, preflight the
// adapters, plan its joins, solve its shapes, and lower it to the one graph
// plan Describe, Explain, and Build read.
func Compile(req *Request) (Result, error) {
	state := State{
		Operation: "build job",
		Req:       req,
		Env:       req.Env,
		Inserted:  map[string]recipeir.NodeID{},
	}
	passes := []recipeCompilePass{
		{"derive branches", (*State).deriveBranches},
		{"validate stream rules", validateStreamRulesForCompile},
		{"validate recipe shape", (*State).validateRecipeShape},
		{"validate attachments", (*State).validateAttachments},
		{"validate stream output kinds", (*State).validateStreamOutputKinds},
		{"validate live stream selection", (*State).validateLiveStreamSelection},
		{"validate output format adapters", (*State).validateOutputFormatAdapters},
		{"validate decode adapters", (*State).validateDecodeAdapters},
		{"validate encode adapters", (*State).validateEncodeAdapters},
		{"validate transform adapters", (*State).validateTransformAdapters},
		{"validate input format adapters", (*State).validateInputFormatAdapters},
		{"validate known input stream selection", (*State).validateKnownInputStreamSelection},
		{"plan joins", (*State).planJoins},
		{"solve operation shapes", (*State).solveOperationShapes},
		{"validate destination shapes", (*State).validateDestinationShapes},
		{"validate known input decode adapters", (*State).validateKnownInputDecodeAdapters},
		{"lower recipe", (*State).lower},
		{"validate mux compatibility", validateMuxCompatibility},
		{"validate recipe runtime", (*State).validateRuntime},
	}
	for _, pass := range passes {
		if err := pass.fn(&state); err != nil {
			return Result{}, CompilerPassError(state.Operation, pass.name, err)
		}
	}
	return Result{
		Recipe:        state.Recipe,
		Joins:         state.joins,
		JoinOrder:     state.joinOrder,
		Graph:         state.composed,
		Lowered:       state.lowered,
		Spec:          state.spec,
		Work:          state.work,
		InputProbes:   state.inputProbes,
		OutputFormats: state.outputFormatMap(),
		OutputSlots:   state.outputSlots,
		StreamRules:   state.streamRules,
		Name:          state.recipeName(),
		RulesSource:   state.ruleSourceNode(),
		RulesDomain:   state.ruleSourceDomain(),
	}, nil
}

func CompilerPassError(operation string, pass string, err error) error {
	var refusal *builderr.Refusal
	if !errors.As(err, &refusal) || refusal == nil {
		return err
	}
	if refusal.Code != "" || refusal.Reason != "" || len(builderr.Lines(refusal.Details)) != 0 || len(builderr.FixLines(refusal.Fixes)) != 0 {
		return err
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.CompilerPassFailedCode),
		Code:      builderr.CompilerPassFailedCode,
		Operation: cmp.Or(refusal.Operation, operation),
		Reason:    "recipe compiler pass failed without a diagnostic",
		Details:   builderr.Details(builderr.KV("pass", pass)),
		Fixes: builderr.Fixes([]string{
			"run Explain(ctx) to inspect the partial plan",
			"report the pass name with the recipe shape",
		}),
		Cause: err,
	}
}

// recipeName names the recipe in Explain's summary: a recipe that is one
// join is named after it.
func (s *State) recipeName() string {
	if root, ok := s.soleJoinRoot(); ok {
		return s.joinNames[root]
	}
	return "from"
}

// soleJoinRoot reports the join a recipe is, when every branch reads one
// join's output.
func (s *State) soleJoinRoot() (recipeir.NodeID, bool) {
	if len(s.Recipe.Streams) == 0 {
		return 0, false
	}
	root := s.Recipe.Streams[0].Root
	for i := range s.Recipe.Streams {
		if s.Recipe.Streams[i].Root != root || s.Recipe.Streams[i].Input >= 0 {
			return 0, false
		}
	}
	return root, true
}

func (s *State) outputFormatMap() map[string]av.FormatID {
	formats := make(map[string]av.FormatID)
	for i := range s.Outputs {
		formatID := s.Outputs[i].openFormat()
		if formatID == "" {
			continue
		}
		formats[s.OutputDestinationNames[i]] = formatID
	}
	if len(formats) == 0 {
		return nil
	}
	return formats
}

// validateRecipeShape checks what every branch must carry before any adapter
// is consulted: a valid selector, encoder, and transforms. The DAG already
// gives every branch an input and a destination.
func (s *State) validateRecipeShape() error {
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		node := recipeIRStreamName(stream)
		if err := validateRecipeStreamSelector(s.Operation, node, recipeIRStreamSelector(stream)); err != nil {
			return err
		}
		if err := ValidateRecipeEncode(recipeIRStreamEncodeSpec(stream), s.Operation, stream.Name); err != nil {
			return err
		}
		for _, transform := range recipeIRStreamTransforms(stream) {
			if err := validateRecipeIRTransform(s.Operation, node, transform); err != nil {
				return err
			}
		}
	}
	return nil
}

func recipeIRStreamName(stream recipeir.Stream) string {
	return cmp.Or(stream.Name, string(stream.Selector.ID), string(stream.Selector.Type), "stream")
}

func recipeIRStreamSelector(stream recipeir.Stream) av.StreamSelector {
	return recipeIRSelector(stream.Selector)
}

// recipeIRSelector is the stream selector a planned selection carries.
func recipeIRSelector(selector plan.StreamSelect) av.StreamSelector {
	return av.StreamSelector{
		ID:       selector.ID,
		Index:    selector.Index,
		UseIndex: selector.UseIndex,
		Type:     selector.Type,
		Codec:    selector.Codec,
		Name:     selector.Name,
	}
}

// recipeIROperationIsAnnotation reports an operation that constrains or
// names the stream but lowers to no node and does no work: an .Auto(...)
// policy, a .Require(...) or .Prefer(...), an As name, or a Buffer.
func recipeIROperationIsAnnotation(operation recipeir.Operation) bool {
	switch operation.Kind {
	case recipeir.OpName, recipeir.OpBuffer:
		return true
	case plan.OpShape:
		return MediaShapeEmpty(operation.Shape) &&
			(operation.Auto != nil || operation.Require != nil || operation.Prefer != nil)
	default:
		return false
	}
}

func recipeIRStreamEncodeSpec(stream recipeir.Stream) codec.Spec {
	for i := range stream.Operations {
		operation := stream.Operations[i]
		if operation.Kind == plan.OpEncode || operation.Kind == plan.OpCopy {
			return CloneCodecSpec(operation.Encode)
		}
	}
	return codec.Spec{}
}

func recipeIRStreamTransforms(stream recipeir.Stream) []recipeir.Transform {
	var transforms []recipeir.Transform
	for i := range stream.Operations {
		if stream.Operations[i].Kind == plan.OpTransform {
			transforms = append(transforms, stream.Operations[i].Transform)
		}
	}
	return transforms
}

func recipeIRStreamHasDecode(stream recipeir.Stream) bool {
	for i := range stream.Operations {
		if stream.Operations[i].Kind == plan.OpDecode {
			return true
		}
	}
	return false
}

func validateRecipeIRTransform(operation string, node string, transform recipeir.Transform) error {
	switch transform.Kind {
	case recipeir.TransformResize:
		if transform.Resize.Width > 0 && transform.Resize.Height > 0 {
			return nil
		}
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.TransformInvalidCode),
			Code:      builderr.TransformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resize requires positive width and height",
			Details:   builderr.Details(builderr.KV("width", fmt.Sprintf("%d", transform.Resize.Width)), builderr.KV("height", fmt.Sprintf("%d", transform.Resize.Height))),
			Fixes: builderr.Fixes([]string{
				"call .Resize(width, height) with positive dimensions",
				"remove .Resize(...) when no video scaling is needed",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	case recipeir.TransformResample:
		if transform.Resample.SampleRate > 0 && transform.Resample.Channels > 0 {
			return nil
		}
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.TransformInvalidCode),
			Code:      builderr.TransformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resample requires positive sample rate and channels",
			Details:   builderr.Details(builderr.KV("sample_rate", fmt.Sprintf("%d", transform.Resample.SampleRate)), builderr.KV("channels", fmt.Sprintf("%d", transform.Resample.Channels))),
			Fixes: builderr.Fixes([]string{
				"call .Resample(sampleRate, channels) with positive values",
				"remove .Resample(...) when no audio conversion is needed",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	default:
		return EmptyTransformSpecError(operation, node)
	}
}

func (s *State) validateAttachments() error {
	return s.Req.Host.ValidateHandles(s.Operation, s.outputSlots, s.OutputDestinationNames)
}

// validateStreamOutputKinds refuses a branch whose output kinds its stream
// cannot feed: decoded frames reach a mux only through an encoder, and one
// branch cannot feed sinks and muxes from frames.
func (s *State) validateStreamOutputKinds() error {
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		facts := s.Facts[i]
		sink, mux := false, false
		for _, dest := range facts.Dests {
			if s.Recipe.Destinations[dest].Kind == recipeir.DestinationKindSink {
				sink = true
			} else {
				mux = true
			}
		}
		encode := recipeIRStreamEncodeSpec(stream)
		node := recipeIRStreamName(stream)
		if sink && mux && !codecIntentSet(encode) {
			return mixedStreamOutputError(s.Operation, node, stream.Selector.Type)
		}
		if mux && encode.ID == "" && !encode.Copy && s.streamEndsFrames(i) {
			return streamEncodeMissingError(s.Operation, node, stream.Selector.Type)
		}
	}
	return nil
}

// streamEndsFrames reports whether a branch carries frames at its end.
func (s *State) streamEndsFrames(index int) bool {
	frames := s.Facts[index].Frames
	for _, operation := range s.Recipe.Streams[index].Operations {
		switch operation.Kind {
		case plan.OpDecode:
			frames = true
		case plan.OpEncode, plan.OpCopy:
			frames = false
		}
	}
	return frames
}

func (s *State) validateLiveStreamSelection() error {
	if !s.Req.Preflight {
		return nil
	}
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		if stream.Input < 0 {
			continue
		}
		if s.selectionNeedsUnion(stream) {
			if err := s.validateStreamSelectionAcrossInputs(stream); err != nil {
				return err
			}
			continue
		}
		if !s.streamNeedsDecode(i) {
			continue
		}
		if err := validateLiveRecipeStreamSelection(s.Recipe.Inputs, stream); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) validateKnownInputStreamSelection() error {
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		if stream.Input < 0 {
			continue
		}
		if s.selectionNeedsUnion(stream) {
			if err := s.validateStreamSelectionAcrossInputs(stream); err != nil {
				return err
			}
			continue
		}
		if !s.streamNeedsDecode(i) {
			continue
		}
		if stream.Input < len(s.inputProbes) {
			if err := validateKnownProbeRecipeStreamSelection(s.inputProbes[stream.Input], stream); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectionNeedsUnion reports a branch of a recipe with several inputs: its
// selection is checked against the stream sets of every input, narrowed to
// its own.
func (s *State) selectionNeedsUnion(stream recipeir.Stream) bool {
	return len(s.Recipe.Inputs) > 1 || stream.Selector.Input != ""
}

func (s *State) validateStreamSelectionAcrossInputs(stream recipeir.Stream) error {
	_, _, err := SelectStreamAcrossInputSets(s.inputStreamSets(), recipeIRStreamSelector(stream), stream.Selector.Input)
	return err
}

func (s *State) inputStreamSets() []InputStreamSet {
	return jobInputStreamSets(s.Recipe.Inputs, s.inputProbes)
}

func validateKnownProbeRecipeStreamSelection(probe format.ProbeResult, stream recipeir.Stream) error {
	if len(probe.Streams) == 0 {
		return nil
	}
	_, err := SelectDecodeStream(probe.Streams, recipeIRStreamSelector(stream))
	return err
}

// streamNeedsDecode reports a branch of an input's packets that decodes,
// transforms, or encodes them.
func (s *State) streamNeedsDecode(index int) bool {
	stream := s.Recipe.Streams[index]
	if stream.Input < 0 || s.Facts[index].Frames {
		return false
	}
	return recipeIRStreamHasDecode(stream) ||
		len(recipeIRStreamTransforms(stream)) != 0 ||
		recipeIRStreamEncodeSpec(stream).ID != ""
}

func (s *State) streamsNeedingDecode() []recipeir.Stream {
	var out []recipeir.Stream
	for i := range s.Recipe.Streams {
		if s.streamNeedsDecode(i) {
			out = append(out, s.Recipe.Streams[i])
		}
	}
	return out
}

func (s *State) validateOutputFormatAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	formats, err := s.Req.Host.ResolveOutputs(s.Req.context(), s.outputSlots, s.OutputDestinationNames)
	if err != nil {
		return err
	}
	for i := range formats {
		s.Outputs[i].ResolvedFormat = formats[i]
	}
	return nil
}

func (s *State) validateInputFormatAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	probes, err := s.Req.Host.ProbeInputs(s.Req.context())
	if err != nil {
		return err
	}
	s.inputProbes = probes
	return nil
}

func (s *State) validateDecodeAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	streams := s.streamsNeedingDecode()
	if len(streams) == 0 {
		return nil
	}
	return validateRecipeIRDecodeAdapters(s.Operation, s.Env, s.Recipe.Inputs, streams)
}

func (s *State) validateEncodeAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	return validateRecipeIREncodeAdapters(s.Operation, s.Env, s.Recipe.Streams)
}

func (s *State) validateTransformAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	return validateRecipeIRTransformAdapters(s.Operation, s.Env, s.Recipe.Streams)
}

func (s *State) validateKnownInputDecodeAdapters() error {
	if !s.Req.Preflight {
		return nil
	}
	streams := s.streamsNeedingDecode()
	if len(streams) == 0 {
		return nil
	}
	return validateKnownRecipeIRDecodeAdapters(s.Operation, s.Env, s.inputProbes, streams)
}

// validateRuntime refuses a build without a runtime: a recipe builds on one,
// which owns its adapters, buffers, and clock.
func (s *State) validateRuntime() error {
	if s.Req.Preflight && !s.Env.Present {
		return RuntimeMissingError(s.Operation)
	}
	return nil
}

// runtimeMissingError is the no-runtime refusal.
func RuntimeMissingError(operation string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.RuntimeMissing),
		Code:      errcode.RuntimeMissing,
		Operation: operation,
		Reason:    "no runtime is configured",
		Fixes: builderr.Fixes([]string{
			"run the Outputs on a runtime: rt.Run(ctx, outputs...)",
			"build a bare runtime with goav.New(...)",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), bundle.Build(ctx, outputs...), or bundle.Run(ctx, outputs...)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func RecipeGraphUnsupportedRecipeError(operation string, recipe recipeir.Recipe) error {
	details := []string{
		fmt.Sprintf("inputs: %d", len(recipe.Inputs)),
		fmt.Sprintf("streams: %d", len(recipe.Streams)),
		fmt.Sprintf("destinations: %d", len(recipe.Destinations)),
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.RecipeGraphUnsupported),
		Code:      errcode.RecipeGraphUnsupported,
		Operation: operation,
		Reason:    "recipe did not plan a graph",
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			"use input.To(output...) for packet-preserving record or remux",
			"use input.Audio().Decode().To(goav.Sink(...)) or input.Video().Decode().To(...) for decoded frames",
			"use v := input.Video().Decode(), then v.Encode(codec.VP9(...)).As(name).To(output) for named forks",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}
