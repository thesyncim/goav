package goav

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
	sourcepkg "github.com/thesyncim/goav/source"
)

// Run builds the Outputs into one task and runs it to completion. It returns
// the first build refusal or runtime error; destinations commit on success
// and abort on failure. Finalize failures surface here too: if both the run
// and close/finalization fail, Run returns errors.Join(runErr, closeErr).
func (r *Runtime) Run(ctx context.Context, outs ...Output) error {
	task, err := r.Build(ctx, outs...)
	if err != nil {
		return err
	}
	runErr := task.Run(ctx)
	closeErr := task.Close()
	return errors.Join(runErr, closeErr)
}

// Build builds the Outputs into one task without starting it.
func (r *Runtime) Build(ctx context.Context, outs ...Output) (LiveTask, error) {
	built, err := r.build(ctx, outs...)
	return built, publicError(err)
}

func (r *Runtime) build(ctx context.Context, outs ...Output) (LiveTask, error) {
	g, err := compileGraph(outs)
	if err != nil {
		return nil, err
	}
	announcer := g.withStreamAnnouncer()
	resolved, err := compileRecipe(g, r, buildCompileOptions(ctx))
	if err != nil {
		return nil, err
	}
	built, err := resolved.Build(ctx)
	if err != nil {
		return nil, err
	}
	resolved.installStreamRules(built, announcer)
	return built, nil
}

// Explain reports the typed plan the Outputs build, without opening them.
// On a refusal it returns both the structured BuildError and a partial
// report naming what is missing.
func (r *Runtime) Explain(ctx context.Context, outs ...Output) (plan.Report, error) {
	report, err := r.explain(ctx, outs...)
	return report, publicError(err)
}

func (r *Runtime) explain(ctx context.Context, outs ...Output) (plan.Report, error) {
	g, err := compileGraph(outs)
	if err != nil {
		return plan.Report{}, err
	}
	resolved, err := compileRecipe(g, r, buildCompileOptions(ctx))
	if err == nil {
		return newPlanReport("build job", resolved)
	}
	fallback, fallbackErr := compileRecipe(g, r, recipeCompileOptions{})
	if fallbackErr != nil {
		return plan.Report{}, err
	}
	report, reportErr := newPlanReport("build job", fallback)
	if reportErr != nil {
		return plan.Report{}, err
	}
	annotatePlanReportError(&report, err)
	return report, err
}

// Describe returns the graph the Outputs build, without opening them.
func (r *Runtime) Describe(outs ...Output) (pipeline.Spec, error) {
	spec, err := r.describe(outs...)
	return spec, publicError(err)
}

func (r *Runtime) describe(outs ...Output) (pipeline.Spec, error) {
	g, err := compileGraph(outs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	resolved, err := compileRecipe(g, r, recipeCompileOptions{})
	if err != nil {
		return pipeline.Spec{}, err
	}
	return resolved.Describe()
}

type recipeCompileOptions struct {
	ctx       context.Context
	preflight bool
}

func buildCompileOptions(ctx context.Context) recipeCompileOptions {
	return recipeCompileOptions{ctx: ctx, preflight: true}
}

// compileRequest is everything one compile reads: the recipe DAG, its
// Outputs, the facts of the input and destination handles the DAG indexes,
// the stream matches its rules name, and the host that checks the handles.
type compileRequest struct {
	Graph   recipeir.Graph
	Outputs []compileOutput
	Inputs  []compileInput
	Dests   []compileDestination
	Matches []sourcepkg.StreamMatch
	Host    compileHost
	Env     compileEnv
	// Preflight runs the checks a build needs before it opens anything
	// (adapters, formats, probes); Describe compiles without them. Ctx
	// bounds the host's probes.
	Preflight bool
	Ctx       context.Context
}

func (r *compileRequest) context() context.Context {
	if r.Ctx != nil {
		return r.Ctx
	}
	return context.Background()
}

// compileEnv is the runtime settings the passes read: every field is a
// leaf type. Present is false when no runtime is configured.
type compileEnv struct {
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
type compileOutput struct {
	From  recipeir.NodeID
	Dests []recipeir.NodeID
}

// compileInput is what the compiler knows of one input before it opens.
type compileInput struct {
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
type compileDestination struct {
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
func (d compileDestination) sharesGroup(other compileDestination) bool {
	return d.Group != "" && d.Group == other.Group
}

// openFormat is the container the destination opens.
func (d compileDestination) openFormat() av.FormatID {
	if d.ResolvedFormat != "" {
		return d.ResolvedFormat
	}
	return d.Format
}

// compileHost checks what only the concrete handles know, at the point in
// the pass order the checks have always run. order lists each recipe
// destination's index in compileRequest.Dests.
type compileHost interface {
	ValidateHandles(operation string, order []int, names []string) error
	ResolveOutputs(ctx context.Context, order []int, names []string) ([]av.FormatID, error)
	ProbeInputs(ctx context.Context) ([]format.ProbeResult, error)
}

// inputsLive reports whether any input delivers on its own clock.
func inputsLive(inputs []compileInput) bool {
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
type recipeCompileState struct {
	operation string
	req       *compileRequest
	recipe    recipeir.Recipe
	env       compileEnv

	// facts are the lowering facts of each branch, aligned with
	// recipe.Streams.
	facts []branchFacts
	// remux are the whole-input copies (input.To): inputs and destinations.
	remux []remuxOutput

	// outputs are the facts of the recipe's destinations, in recipe order;
	// outputSlots index each in req.Dests.
	outputs                []compileDestination
	outputSlots            []int
	outputDestinationNames []string
	inputProbes            []format.ProbeResult
	streamRules            []streamRulePlan

	// joins are the planned joins, by graph node.
	joins     map[recipeir.NodeID]*joinPlan
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
	inserted map[string]recipeir.NodeID
	// destIndex indexes each output node's destination.
	destIndex map[recipeir.NodeID]int

	shapeDiagnostics []plan.Diagnostic
	// lowered reports the recipe lowered: composed is its compose graph,
	// spec the graph it describes, and work its work plan.
	lowered  bool
	composed composeGraph
	spec     pipeline.Spec
	work     workPlan
}

// branchFacts are what the lowering knows of one branch beyond its IR.
type branchFacts struct {
	// lone marks the only branch of a root that names no fork: it lowers
	// under its root's identity.
	lone bool
	// frames reports that the branch's root carries frames.
	frames bool
	// dests indexes the branch's destinations, in .To order.
	dests []int
}

type remuxOutput struct {
	input int
	// nodes are the destination nodes; dests their destination indexes.
	nodes []recipeir.NodeID
	dests []int
}

type recipeResolved struct {
	recipe            recipeir.Recipe
	runtime           *Runtime
	graphPlan         graphPlan
	inputAttachments  []InputSpec
	outputAttachments []destinationSpec
	inputProbes       []format.ProbeResult
	outputFormats     map[string]av.FormatID
	streamRules       []streamRule
	name              string
	rulesSource       string
	rulesDomain       shape.MediaDomain
}

type recipeCompilePass struct {
	name string
	fn   func(*recipeCompileState) error
}

// compileRecipe compiles one recipe DAG against rt: it projects the DAG and
// its handles into the compile request, runs the compiler, and binds the
// result back to the handles the build opens.
func compileRecipe(g *recipeGraph, rt *Runtime, options recipeCompileOptions) (recipeResolved, error) {
	rt = g.compileRuntime(rt)
	handles := g.handles(rt)
	req := g.compileRequest(handles)
	req.Env, req.Preflight, req.Ctx = rt.compileEnv(), options.preflight, options.ctx
	result, err := runCompile(req)
	if err != nil {
		return recipeResolved{}, err
	}
	resolved := recipeResolved{
		recipe:            result.recipe,
		runtime:           rt,
		inputAttachments:  handles.inputs,
		outputAttachments: handles.outputs(result.outputSlots),
		inputProbes:       result.inputProbes,
		outputFormats:     result.outputFormats,
		streamRules:       g.handles(rt).streamRules(result.streamRules),
		name:              result.name,
		rulesSource:       result.rulesSource,
		rulesDomain:       result.rulesDomain,
	}
	if result.lowered {
		resolved.graphPlan = newGraphPlan(rt, result.spec, result.work, &recipeLowerer{
			runtime:   rt,
			recipe:    result.recipe,
			joins:     result.joins,
			joinOrder: result.joinOrder,
			graph:     result.graph,
			inputs:    resolved.inputAttachments,
			outputs:   resolved.outputAttachments,
		})
	}
	return resolved, nil
}

// compileResult is what one compile resolves: the recipe IR, its planned
// joins and compose graph, the spec and work plan it lowers to, and what the
// passes learned of the inputs and destinations.
type compileResult struct {
	recipe        recipeir.Recipe
	joins         map[recipeir.NodeID]*joinPlan
	joinOrder     []recipeir.NodeID
	graph         composeGraph
	lowered       bool
	spec          pipeline.Spec
	work          workPlan
	inputProbes   []format.ProbeResult
	outputFormats map[string]av.FormatID
	outputSlots   []int
	streamRules   []streamRulePlan
	name          string
	rulesSource   string
	rulesDomain   shape.MediaDomain
}

// runCompile runs the compiler passes over one recipe DAG: derive and
// validate its branches, check its inputs and destinations, preflight the
// adapters, plan its joins, solve its shapes, and lower it to the one graph
// plan Describe, Explain, and Build read.
func runCompile(req *compileRequest) (compileResult, error) {
	state := recipeCompileState{
		operation: "build job",
		req:       req,
		env:       req.Env,
		inserted:  map[string]recipeir.NodeID{},
	}
	passes := []recipeCompilePass{
		{"derive branches", (*recipeCompileState).deriveBranches},
		{"validate stream rules", validateStreamRulesForCompile},
		{"validate recipe shape", (*recipeCompileState).validateRecipeShape},
		{"validate attachments", (*recipeCompileState).validateAttachments},
		{"validate stream output kinds", (*recipeCompileState).validateStreamOutputKinds},
		{"validate live stream selection", (*recipeCompileState).validateLiveStreamSelection},
		{"validate output format adapters", (*recipeCompileState).validateOutputFormatAdapters},
		{"validate decode adapters", (*recipeCompileState).validateDecodeAdapters},
		{"validate encode adapters", (*recipeCompileState).validateEncodeAdapters},
		{"validate transform adapters", (*recipeCompileState).validateTransformAdapters},
		{"validate input format adapters", (*recipeCompileState).validateInputFormatAdapters},
		{"validate known input stream selection", (*recipeCompileState).validateKnownInputStreamSelection},
		{"plan joins", (*recipeCompileState).planJoins},
		{"solve operation shapes", (*recipeCompileState).solveOperationShapes},
		{"validate destination shapes", (*recipeCompileState).validateDestinationShapes},
		{"validate known input decode adapters", (*recipeCompileState).validateKnownInputDecodeAdapters},
		{"lower recipe", (*recipeCompileState).lower},
		{"validate mux compatibility", validateMuxCompatibility},
		{"validate recipe runtime", (*recipeCompileState).validateRuntime},
	}
	for _, pass := range passes {
		if err := pass.fn(&state); err != nil {
			return compileResult{}, compilerPassError(state.operation, pass.name, err)
		}
	}
	return compileResult{
		recipe:        state.recipe,
		joins:         state.joins,
		joinOrder:     state.joinOrder,
		graph:         state.composed,
		lowered:       state.lowered,
		spec:          state.spec,
		work:          state.work,
		inputProbes:   state.inputProbes,
		outputFormats: state.outputFormatMap(),
		outputSlots:   state.outputSlots,
		streamRules:   state.streamRules,
		name:          state.recipeName(),
		rulesSource:   state.ruleSourceNode(),
		rulesDomain:   state.ruleSourceDomain(),
	}, nil
}

func compilerPassError(operation string, pass string, err error) error {
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr == nil {
		return err
	}
	if buildErr.Code != "" || buildErr.Reason != "" || len(buildErr.DetailLines()) != 0 || len(buildErr.FixLines()) != 0 {
		return err
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(compilerPassFailedCode),
		Code:      compilerPassFailedCode,
		Operation: firstNonEmpty(buildErr.Operation, operation),
		Reason:    "recipe compiler pass failed without a diagnostic",
		fields:    errDetails(errDetail("pass", pass)),
		fixes: buildErrorFixes([]string{
			"run Explain(ctx) to inspect the partial plan",
			"report the pass name with the recipe shape",
		}),
		cause: err,
	}
}

// compileRuntime is the runtime a recipe plans and builds against. Realtime
// follows the inputs unless the runtime forces it with WithRealtime(true): a
// recipe with a live input (a provider or push source whose shape declares
// Realtime) compiles against a realtime view of its runtime, so its graph
// buffers between stages and encoders pick low-latency presets, while
// file-to-file recipes keep the offline runtime and run at full speed.
func (g *recipeGraph) compileRuntime(rt *Runtime) *Runtime {
	if rt == nil || rt.realtime || !inputsAreLive(g.inputs) {
		return rt
	}
	return runtimeWithRealtime(rt, true)
}

// inputsAreLive reports whether any input delivers on its own clock.
func inputsAreLive(inputs []InputSpec) bool {
	for i := range inputs {
		if inputs[i].realtime || inputs[i].input.Realtime {
			return true
		}
	}
	return false
}

// recipeName names the recipe in Explain's summary: a recipe that is one
// join is named after it.
func (s *recipeCompileState) recipeName() string {
	if root, ok := s.soleJoinRoot(); ok {
		return s.joinNames[root]
	}
	return "from"
}

// soleJoinRoot reports the join a recipe is, when every branch reads one
// join's output.
func (s *recipeCompileState) soleJoinRoot() (recipeir.NodeID, bool) {
	if len(s.recipe.Streams) == 0 {
		return 0, false
	}
	root := s.recipe.Streams[0].Root
	for i := range s.recipe.Streams {
		if s.recipe.Streams[i].Root != root || s.recipe.Streams[i].Input >= 0 {
			return 0, false
		}
	}
	return root, true
}

func (s *recipeCompileState) outputFormatMap() map[string]av.FormatID {
	formats := make(map[string]av.FormatID)
	for i := range s.outputs {
		formatID := s.outputs[i].openFormat()
		if formatID == "" {
			continue
		}
		formats[s.outputDestinationNames[i]] = formatID
	}
	if len(formats) == 0 {
		return nil
	}
	return formats
}

func destinationSpecFormat(output destinationSpec) av.FormatID {
	if output.resolvedFormat != "" {
		return output.resolvedFormat
	}
	return output.format
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if !r.graphPlan.ready() {
		return pipeline.Spec{}, recipeGraphUnsupportedRecipeError("describe recipe", r.recipe)
	}
	return r.graphPlan.Describe()
}

func (r recipeResolved) Build(ctx context.Context) (LiveTask, error) {
	if !r.graphPlan.ready() {
		return nil, recipeGraphUnsupportedRecipeError("build recipe", r.recipe)
	}
	report, err := newPlanReport("build job", r)
	if err != nil {
		return nil, err
	}
	task, err := r.graphPlan.Build(ctx)
	if err != nil {
		return nil, err
	}
	installTaskExplainReport(task, report)
	installTaskTaps(task, r.graphPlan.work.Taps)
	return task, nil
}

// workIR is the compiled work plan — the single plan views render from.
func (r recipeResolved) workIR() workPlan {
	if r.graphPlan.ready() {
		return r.graphPlan.workPlan()
	}
	return workPlan{}
}

func installTaskTaps(mediaTask Task, taps []workTap) {
	if len(taps) == 0 {
		return
	}
	runtimeTask, ok := mediaTask.(*task)
	if !ok || runtimeTask == nil {
		return
	}
	runtimeTask.taps = tapInfosFromPlan(taps)
}

func installTaskExplainReport(mediaTask Task, report plan.Report) {
	runtimeTask, ok := mediaTask.(*task)
	if !ok || runtimeTask == nil {
		return
	}
	runtimeTask.explainReport = clonePlanReport(report)
	runtimeTask.explainReportReady = true
}

func tapInfosFromPlan(taps []workTap) []snapshot.Tap {
	out := make([]snapshot.Tap, 0, len(taps))
	seen := make(map[string]struct{}, len(taps))
	for i := range taps {
		if taps[i].Name == "" {
			continue
		}
		if _, ok := seen[taps[i].Name]; ok {
			continue
		}
		seen[taps[i].Name] = struct{}{}
		out = append(out, snapshot.Tap{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			After:     taps[i].After,
			Shape:     taps[i].Shape,
			Node:      taps[i].Node,
		})
	}
	return out
}

// --- recipe shape ---

// validateRecipeShape checks what every branch must carry before any adapter
// is consulted: a valid selector, encoder, and transforms. The DAG already
// gives every branch an input and a destination.
func (s *recipeCompileState) validateRecipeShape() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		node := recipeIRStreamName(stream)
		if err := validateRecipeStreamSelector(s.operation, node, recipeIRStreamSelector(stream)); err != nil {
			return err
		}
		if err := validateRecipeEncode(recipeIRStreamEncodeSpec(stream), s.operation, stream.Name); err != nil {
			return err
		}
		for _, transform := range recipeIRStreamTransforms(stream) {
			if err := validateRecipeIRTransform(s.operation, node, transform); err != nil {
				return err
			}
		}
	}
	return nil
}

func recipeIRStreamName(stream recipeir.Stream) string {
	return firstNonEmpty(stream.Name, string(stream.Selector.ID), string(stream.Selector.Type), "stream")
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
		return mediaShapeEmpty(operation.Shape) &&
			(operation.Auto != nil || operation.Require != nil || operation.Prefer != nil)
	default:
		return false
	}
}

func recipeIRStreamEncodeSpec(stream recipeir.Stream) codec.Spec {
	for i := range stream.Operations {
		operation := stream.Operations[i]
		if operation.Kind == plan.OpEncode || operation.Kind == plan.OpCopy {
			return cloneCodecSpec(operation.Encode)
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
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resize requires positive width and height",
			fields:    errDetails(errDetail("width", fmt.Sprintf("%d", transform.Resize.Width)), errDetail("height", fmt.Sprintf("%d", transform.Resize.Height))),
			fixes: buildErrorFixes([]string{
				"call .Resize(width, height) with positive dimensions",
				"remove .Resize(...) when no video scaling is needed",
			}),
			cause: errUnsupportedBuild,
		}
	case recipeir.TransformResample:
		if transform.Resample.SampleRate > 0 && transform.Resample.Channels > 0 {
			return nil
		}
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resample requires positive sample rate and channels",
			fields:    errDetails(errDetail("sample_rate", fmt.Sprintf("%d", transform.Resample.SampleRate)), errDetail("channels", fmt.Sprintf("%d", transform.Resample.Channels))),
			fixes: buildErrorFixes([]string{
				"call .Resample(sampleRate, channels) with positive values",
				"remove .Resample(...) when no audio conversion is needed",
			}),
			cause: errUnsupportedBuild,
		}
	default:
		return emptyTransformSpecError(operation, node)
	}
}

// --- attachments ---

func (s *recipeCompileState) validateAttachments() error {
	return s.req.Host.ValidateHandles(s.operation, s.outputSlots, s.outputDestinationNames)
}

// validateStreamOutputKinds refuses a branch whose output kinds its stream
// cannot feed: decoded frames reach a mux only through an encoder, and one
// branch cannot feed sinks and muxes from frames.
func (s *recipeCompileState) validateStreamOutputKinds() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		facts := s.facts[i]
		sink, mux := false, false
		for _, dest := range facts.dests {
			if s.recipe.Destinations[dest].Kind == recipeir.DestinationKindSink {
				sink = true
			} else {
				mux = true
			}
		}
		encode := recipeIRStreamEncodeSpec(stream)
		node := recipeIRStreamName(stream)
		if sink && mux && !codecIntentSet(encode) {
			return mixedStreamOutputError(s.operation, node, stream.Selector.Type)
		}
		if mux && encode.ID == "" && !encode.Copy && s.streamEndsFrames(i) {
			return streamEncodeMissingError(s.operation, node, stream.Selector.Type)
		}
	}
	return nil
}

// streamEndsFrames reports whether a branch carries frames at its end.
func (s *recipeCompileState) streamEndsFrames(index int) bool {
	frames := s.facts[index].frames
	for _, operation := range s.recipe.Streams[index].Operations {
		switch operation.Kind {
		case plan.OpDecode:
			frames = true
		case plan.OpEncode, plan.OpCopy:
			frames = false
		}
	}
	return frames
}

func (s *recipeCompileState) validateLiveStreamSelection() error {
	if !s.req.Preflight {
		return nil
	}
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
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
		if err := validateLiveRecipeStreamSelection(s.recipe.Inputs, stream); err != nil {
			return err
		}
	}
	return nil
}

func (s *recipeCompileState) validateKnownInputStreamSelection() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
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
func (s *recipeCompileState) selectionNeedsUnion(stream recipeir.Stream) bool {
	return len(s.recipe.Inputs) > 1 || stream.Selector.Input != ""
}

func (s *recipeCompileState) validateStreamSelectionAcrossInputs(stream recipeir.Stream) error {
	_, _, err := selectStreamAcrossInputSets(s.inputStreamSets(), recipeIRStreamSelector(stream), stream.Selector.Input)
	return err
}

func (s *recipeCompileState) inputStreamSets() []inputStreamSet {
	return jobInputStreamSets(s.recipe.Inputs, s.inputProbes)
}

func validateKnownProbeRecipeStreamSelection(probe format.ProbeResult, stream recipeir.Stream) error {
	if len(probe.Streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(probe.Streams, recipeIRStreamSelector(stream))
	return err
}

// streamNeedsDecode reports a branch of an input's packets that decodes,
// transforms, or encodes them.
func (s *recipeCompileState) streamNeedsDecode(index int) bool {
	stream := s.recipe.Streams[index]
	if stream.Input < 0 || s.facts[index].frames {
		return false
	}
	return recipeIRStreamHasDecode(stream) ||
		len(recipeIRStreamTransforms(stream)) != 0 ||
		recipeIRStreamEncodeSpec(stream).ID != ""
}

func (s *recipeCompileState) streamsNeedingDecode() []recipeir.Stream {
	var out []recipeir.Stream
	for i := range s.recipe.Streams {
		if s.streamNeedsDecode(i) {
			out = append(out, s.recipe.Streams[i])
		}
	}
	return out
}

// --- adapters ---

func (s *recipeCompileState) validateOutputFormatAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	formats, err := s.req.Host.ResolveOutputs(s.req.context(), s.outputSlots, s.outputDestinationNames)
	if err != nil {
		return err
	}
	for i := range formats {
		s.outputs[i].ResolvedFormat = formats[i]
	}
	return nil
}

func (s *recipeCompileState) validateInputFormatAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	probes, err := s.req.Host.ProbeInputs(s.req.context())
	if err != nil {
		return err
	}
	s.inputProbes = probes
	return nil
}

func (s *recipeCompileState) validateDecodeAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	streams := s.streamsNeedingDecode()
	if len(streams) == 0 {
		return nil
	}
	return validateRecipeIRDecodeAdapters(s.operation, s.env, s.recipe.Inputs, streams)
}

func (s *recipeCompileState) validateEncodeAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	return validateRecipeIREncodeAdapters(s.operation, s.env, s.recipe.Streams)
}

func (s *recipeCompileState) validateTransformAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	return validateRecipeIRTransformAdapters(s.operation, s.env, s.recipe.Streams)
}

func (s *recipeCompileState) validateKnownInputDecodeAdapters() error {
	if !s.req.Preflight {
		return nil
	}
	streams := s.streamsNeedingDecode()
	if len(streams) == 0 {
		return nil
	}
	return validateKnownRecipeIRDecodeAdapters(s.operation, s.env, s.inputProbes, streams)
}

// --- runtime ---

// validateRuntime refuses a build without a runtime: a recipe builds on one,
// which owns its adapters, buffers, and clock.
func (s *recipeCompileState) validateRuntime() error {
	if s.req.Preflight && !s.env.Present {
		return runtimeMissingError(s.operation)
	}
	return nil
}

// runtimeMissingError is the no-runtime refusal.
func runtimeMissingError(operation string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(errcode.RuntimeMissing),
		Code:      errcode.RuntimeMissing,
		Operation: operation,
		Reason:    "no runtime is configured",
		fixes: buildErrorFixes([]string{
			"run the Outputs on a runtime: rt.Run(ctx, outputs...)",
			"build a bare runtime with goav.New(...)",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), bundle.Build(ctx, outputs...), or bundle.Run(ctx, outputs...)",
		}),
		cause: errUnsupportedBuild,
	}
}

func recipeGraphUnsupportedRecipeError(operation string, recipe recipeir.Recipe) error {
	details := []string{
		fmt.Sprintf("inputs: %d", len(recipe.Inputs)),
		fmt.Sprintf("streams: %d", len(recipe.Streams)),
		fmt.Sprintf("destinations: %d", len(recipe.Destinations)),
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(errcode.RecipeGraphUnsupported),
		Code:      errcode.RecipeGraphUnsupported,
		Operation: operation,
		Reason:    "recipe did not plan a graph",
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"use input.To(output...) for packet-preserving record or remux",
			"use input.Audio().Decode().To(goav.Sink(...)) or input.Video().Decode().To(...) for decoded frames",
			"use v := input.Video().Decode(), then v.Encode(codec.VP9(...)).As(name).To(output) for named forks",
		}),
		cause: errUnsupportedBuild,
	}
}
