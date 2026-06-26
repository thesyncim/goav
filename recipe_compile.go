package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

type recipeResolved struct {
	intent                intent
	runtime               *Runtime
	spec                  pipeline.Spec
	specReady             bool
	specOrigin            string
	graphPlan             graphPlan
	inputAttachments      []InputSpec
	outputAttachments     []destinationSpec
	inputProbes           []format.ProbeResult
	branchInputAttachment InputSpec
	branchInputProbe      format.ProbeResult
	branchInputProbeReady bool
	outputFormats         map[string]av.FormatID
	plan                  branchComposePlan
	streamRuleFacts       []recipeir.StreamRule
	streamRules           []streamRule
}

type recipeCompileState struct {
	operation       string
	recipe          recipeir.Recipe
	intent          intent
	runtime         *Runtime
	runtimeExplicit bool
	options         recipeCompileOptions

	jobPresent               bool
	branchCompositionPresent bool
	recipeErr                error

	inputFacts             []recipeir.Input
	destinationKinds       []recipeir.DestinationKind
	joinFacts              recipeir.Join
	streamRuleFacts        []recipeir.StreamRule
	inputAttachments       []InputSpec
	jobOutputCount         int
	outputAttachments      []destinationSpec
	outputDestinationNames []string
	inputProbes            []format.ProbeResult
	streamRules            []streamRule

	branchInputAttachment        InputSpec
	branchDestinationAttachments []namedDestinationSpec
	branchInputProbe             format.ProbeResult
	branchInputProbeReady        bool
	branchCompositionSplit       bool

	// joinTree is the captured Mix/Composite/Select arm tree consumed by join planning.
	joinTree *joinTreeSnapshot

	plan    branchComposePlan
	planErr error

	// shapeDiagnostics records the shape solver's automatic insertions; the
	// work plan carries them so Explain reports every inserted conversion.
	shapeDiagnostics []plan.Diagnostic

	spec       pipeline.Spec
	specReady  bool
	specOrigin string
	graphPlan  graphPlan
}

type recipeCompileOptions struct {
	ctx                        context.Context
	requireExplicitRuntime     bool
	preflightInputAdapters     bool
	preflightOutputAdapters    bool
	preflightDecodeAdapters    bool
	preflightEncodeAdapters    bool
	preflightTransformAdapters bool
	preflightLiveStreams       bool
	preflightMuxCompatibility  bool
}

func (o recipeCompileOptions) Context() context.Context {
	if o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}

func (s *recipeCompileState) outputFormatMap() map[string]av.FormatID {
	formats := make(map[string]av.FormatID)
	for i := range s.outputAttachments {
		formatID := destinationSpecFormat(s.outputAttachments[i])
		if formatID == "" {
			continue
		}
		formats[jobOutputDestinationName(s.outputAttachments, s.outputDestinationNames, i)] = formatID
	}
	for i := range s.branchDestinationAttachments {
		formatID := destinationSpecFormat(s.branchDestinationAttachments[i].output)
		if formatID == "" {
			continue
		}
		label := firstNonEmpty(s.branchDestinationAttachments[i].name, s.branchDestinationAttachments[i].output.label(fmt.Sprintf("output-%d", i)))
		formats[label] = formatID
	}
	if len(formats) == 0 {
		return nil
	}
	return formats
}

func (s *recipeCompileState) recipeInputIntents() []inputIntent {
	if s == nil {
		return nil
	}
	if len(s.recipe.Inputs) != 0 {
		return inputIntentsFromRecipeIR(s.recipe.Inputs)
	}
	return cloneInputIntents(s.intent.Inputs)
}

func destinationSpecFormat(output destinationSpec) av.FormatID {
	if output.resolvedFormat != "" {
		return output.resolvedFormat
	}
	return output.format
}

type recipeCompilePass interface {
	Name() string
	Apply(*recipeCompileState) error
}

type recipeCompilePassFunc struct {
	name string
	fn   func(*recipeCompileState) error
}

func (p recipeCompilePassFunc) Name() string {
	return p.name
}

func (p recipeCompilePassFunc) Apply(state *recipeCompileState) error {
	return p.fn(state)
}

type recipeIntentCompiler struct {
	passes []recipeCompilePass
}

type recipeCompilePhaseSet struct {
	recipe      []recipeCompilePass
	intent      []recipeCompilePass
	attachments []recipeCompilePass
	adapters    []recipeCompilePass
	knownInputs []recipeCompilePass
	shapes      []recipeCompilePass
	plan        []recipeCompilePass
}

func compileRecipePhaseSet(state recipeCompileState, phases recipeCompilePhaseSet) (recipeResolved, error) {
	return recipeIntentCompiler{passes: recipeCompilePhaseSequence(phases)}.Compile(state)
}

func recipeCompilePhaseSequence(phases recipeCompilePhaseSet) []recipeCompilePass {
	passes := make([]recipeCompilePass, 0, 24)
	passes = append(passes, phases.recipe...)
	passes = append(passes, validateStreamRulesPass())
	passes = append(passes, phases.intent...)
	passes = append(passes, validateRecipeAttachmentConsistencyPass())
	passes = append(passes, phases.attachments...)
	passes = append(passes, phases.adapters...)
	passes = append(passes, phases.knownInputs...)
	passes = append(passes, phases.shapes...)
	passes = append(passes, phases.plan...)
	passes = append(passes,
		emitGraphPlanSpecPass(),
		validateMuxCompatibilityPass(),
		requireGraphPlanSpecPass(),
		validateRecipeRuntimePass(),
	)
	return passes
}

func (c recipeIntentCompiler) Compile(state recipeCompileState) (recipeResolved, error) {
	for i := range c.passes {
		pass := c.passes[i]
		if pass == nil {
			return recipeResolved{}, &BuildError{
				Family:    errcode.FamilyForCode(compilerPassInvalidCode),
				Code:      compilerPassInvalidCode,
				Operation: state.operation,
				Reason:    fmt.Sprintf("recipe compiler pass %d is nil", i),
				fields:    buildErrorFields([]string{"internal invariant: the recipe compiler was assembled with a nil pass"}),
				cause:     errUnsupportedBuild,
			}
		}
		if err := pass.Apply(&state); err != nil {
			return recipeResolved{}, compilerPassError(state.operation, pass.Name(), err)
		}
	}
	return recipeResolved{
		intent:                state.intent,
		runtime:               state.runtime,
		spec:                  state.spec,
		specReady:             state.specReady,
		specOrigin:            state.specOrigin,
		graphPlan:             state.graphPlan,
		inputAttachments:      append([]InputSpec(nil), state.inputAttachments...),
		outputAttachments:     append([]destinationSpec(nil), state.outputAttachments...),
		inputProbes:           append([]format.ProbeResult(nil), state.inputProbes...),
		branchInputAttachment: state.branchInputAttachment,
		branchInputProbe:      state.branchInputProbe,
		branchInputProbeReady: state.branchInputProbeReady,
		outputFormats:         state.outputFormatMap(),
		plan:                  state.plan,
		streamRuleFacts:       cloneRecipeIRStreamRules(state.streamRuleFacts),
		streamRules:           cloneStreamRules(state.streamRules),
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
		Family:    errcode.FamilyForCode(compilerPassFailedCode),
		Code:      compilerPassFailedCode,
		Operation: firstNonEmpty(buildErr.Operation, operation),
		Reason:    "recipe compiler pass failed without a diagnostic",
		fields: buildErrorFields([]string{
			"pass=" + pass,
		}),
		fixes: buildErrorFixes([]string{
			"run Explain(ctx) to inspect the partial plan",
			"report the pass name with the recipe shape",
		}),
		cause: err,
	}
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if r.specReady && r.specOrigin == graphSpecOriginGraphPlan && r.graphPlan.ready() {
		return r.graphPlan.Describe()
	}
	return pipeline.Spec{}, recipeGraphUnsupportedError("describe recipe", r.intent)
}

func (r recipeResolved) Build(ctx context.Context) (LiveTask, error) {
	if !r.graphPlan.ready() {
		return nil, recipeGraphUnsupportedError("build recipe", r.intent)
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

func compileJobRecipe(job *Job) (recipeResolved, error) {
	return compileJobRecipeWithOptions(job, recipeCompileOptions{})
}

func compileJobRecipeForBuild(job *Job) (recipeResolved, error) {
	return compileJobRecipeForBuildContext(context.Background(), job)
}

func compileJobRecipeForBuildContext(ctx context.Context, job *Job) (recipeResolved, error) {
	return compileJobRecipeWithOptions(job, recipeCompileOptions{
		ctx:                        ctx,
		requireExplicitRuntime:     true,
		preflightInputAdapters:     true,
		preflightOutputAdapters:    true,
		preflightDecodeAdapters:    true,
		preflightEncodeAdapters:    true,
		preflightTransformAdapters: true,
		preflightLiveStreams:       true,
		preflightMuxCompatibility:  true,
	})
}

func compileJobRecipeWithOptions(job *Job, options recipeCompileOptions) (recipeResolved, error) {
	return compileRecipeSnapshotWithOptions(newJobRecipeSnapshot(job), options)
}

func compileRecipeSnapshotWithOptions(snapshot recipeCompileSnapshot, options recipeCompileOptions) (recipeResolved, error) {
	return compileRecipePhaseSet(
		recipeCompileStateFromSnapshot(snapshot, options),
		recipeCompilePhasesForSnapshot(snapshot),
	)
}

func jobRecipeCompilePhases() recipeCompilePhaseSet {
	return recipeCompilePhaseSet{
		recipe: []recipeCompilePass{
			validateJobRecipePass(),
		},
		intent: []recipeCompilePass{
			validateJobIntentShapePass(),
		},
		attachments: []recipeCompilePass{
			validateJobAttachmentsPass(),
			validateJobOutputBindingsPass(),
			validateJobStreamOutputKindsPass(),
			validatePacketJobOutputsPass(),
			validateJobLiveStreamSelectionPass(),
		},
		adapters: []recipeCompilePass{
			validateJobOutputFormatAdaptersPass(),
			validateJobDecodeAdaptersPass(),
			validateJobEncodeAdaptersPass(),
			validateJobTransformAdaptersPass(),
			validateJobInputFormatAdaptersPass(),
		},
		knownInputs: []recipeCompilePass{
			validateJobKnownInputStreamSelectionPass(),
		},
		shapes: []recipeCompilePass{
			validateRecipeOperationShapesPass(),
			validateRecipeDestinationShapesPass(),
			validateJobKnownInputDecodeAdaptersPass(),
		},
	}
}

func joinRecipeCompilePhases() recipeCompilePhaseSet {
	return recipeCompilePhaseSet{
		recipe: []recipeCompilePass{
			validateJoinRecipePass(),
		},
		adapters: []recipeCompilePass{
			validateJobInputFormatAdaptersPass(),
		},
	}
}

func validateJoinRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate join recipe", fn: func(state *recipeCompileState) error {
		if !state.joinPresent() {
			return nilRecipeError(state.operation, "nil join")
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func (state *recipeCompileState) joinPresent() bool {
	if state == nil {
		return false
	}
	return state.joinFacts.Kind != "" || state.joinTree != nil
}

func compileJobBranchRecipeWithOptions(job *Job, options recipeCompileOptions) (recipeResolved, error) {
	if job == nil {
		return compileRecipeSnapshotWithOptions(recipeCompileSnapshot{}, options)
	}
	return compileRecipeSnapshotWithOptions(newBranchJobRecipeSnapshot(job), options)
}

func branchCompositionRecipeCompilePhases() recipeCompilePhaseSet {
	return recipeCompilePhaseSet{
		recipe: []recipeCompilePass{
			validateBranchCompositionRecipePass(),
		},
		intent: []recipeCompilePass{
			validateBranchCompositionIntentShapePass(),
		},
		attachments: []recipeCompilePass{
			validateBranchCompositionAttachmentsPass(),
			validateBranchDestinationBindingsPass(),
			validateBranchDestinationKindsPass(),
		},
		adapters: []recipeCompilePass{
			validateBranchDestinationFormatAdaptersPass(),
			validateBranchEncodeAdaptersPass(),
			validateBranchTransformAdaptersPass(),
			validateBranchInputFormatAdaptersPass(),
		},
		knownInputs: []recipeCompilePass{
			validateKnownBranchInputStreamSelectionPass(),
		},
		shapes: []recipeCompilePass{
			validateRecipeOperationShapesPass(),
			validateRecipeDestinationShapesPass(),
			validateKnownBranchInputDecodeAdaptersPass(),
		},
		plan: []recipeCompilePass{
			planBranchCompositionIntentPass(),
		},
	}
}

// nilRecipeError marks a recipe compile invoked without its job/join
// attachment — an internal invariant, not a user-fixable refusal.
func nilRecipeError(operation string, reason string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.JobInvalid),
		Code:      errcode.JobInvalid,
		Operation: operation,
		Reason:    reason,
		fields:    buildErrorFields([]string{"internal invariant: the compiler was invoked without its recipe attachment (recipes are constructed with goav.From(...))"}),
		cause:     errUnsupportedBuild,
	}
}

func unconstructedJobError() error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.JobInvalid),
		Code:      errcode.JobInvalid,
		Operation: "build job",
		Reason:    "empty job",
		fixes: buildErrorFixes([]string{
			"start the recipe with goav.From(input)",
			"use goav.From(goav.FileInput(\"in.webm\", reader)) for reader-backed input",
			"use goav.From(goav.Source(name, shape, fn)) for application-pushed input",
		}),
		cause: errUnsupportedBuild,
	}
}

// runtimeMissingError is the no-runtime refusal shared by every recipe form.
func runtimeMissingError(operation string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RuntimeMissing),
		Code:      errcode.RuntimeMissing,
		Operation: operation,
		Reason:    "no runtime is configured",
		fixes: buildErrorFixes([]string{
			"pass a non-nil runtime with .UseRuntime(runtime)",
			"build a bare runtime with goav.New(...)",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), bundle.Build(ctx, job), or bundle.Run(ctx, job)",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateJobRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job recipe", fn: func(state *recipeCompileState) error {
		if !state.jobPresent {
			return nilRecipeError(state.operation, "nil job")
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func validateRecipeRuntimePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe runtime", fn: func(state *recipeCompileState) error {
		if !state.options.requireExplicitRuntime {
			return nil
		}
		if state.runtimeExplicit && state.runtime == nil {
			return runtimeMissingError(state.operation)
		}
		if state.requiresExplicitRuntime() {
			if state.runtime != nil && state.runtimeExplicit {
				return nil
			}
			return runtimeMissingError(state.operation)
		}
		return nil
	}}
}

func (state *recipeCompileState) adapterRuntime() *Runtime {
	if state.requiresExplicitRuntime() && !state.runtimeExplicit {
		return nil
	}
	return state.runtime
}

func (state *recipeCompileState) requiresExplicitRuntime() bool {
	if !state.options.requireExplicitRuntime {
		return false
	}
	if inputSpecsRequireRuntime(state.inputAttachments) || inputSpecRequiresRuntime(state.branchInputAttachment) {
		return true
	}
	if destinationSpecsRequireRuntime(state.outputAttachments) || namedDestinationSpecsRequireRuntime(state.branchDestinationAttachments) {
		return true
	}
	if streamRulesRequireRuntime(state.streamRules) {
		return true
	}
	return intentRequiresRuntime(state.intent)
}

func inputSpecsRequireRuntime(inputs []InputSpec) bool {
	for i := range inputs {
		if inputSpecRequiresRuntime(inputs[i]) {
			return true
		}
	}
	return false
}

func inputSpecRequiresRuntime(input InputSpec) bool {
	if input.err != nil || input.provider != nil || input.source != nil {
		return false
	}
	return input.input.Name != "" ||
		input.input.URI != "" ||
		input.input.Protocol != "" ||
		input.input.MIMEType != "" ||
		input.input.Reader != nil ||
		input.input.ReaderAt != nil
}

func destinationSpecsRequireRuntime(destinations []destinationSpec) bool {
	for i := range destinations {
		if destinationSpecRequiresRuntime(destinations[i]) {
			return true
		}
	}
	return false
}

func namedDestinationSpecsRequireRuntime(destinations []namedDestinationSpec) bool {
	for i := range destinations {
		if destinationSpecRequiresRuntime(destinations[i].output) {
			return true
		}
	}
	return false
}

func destinationSpecRequiresRuntime(destination destinationSpec) bool {
	if destination.err != nil || destination.sink != nil || destination.custom != nil {
		return false
	}
	return destination.output.Name != "" ||
		destination.output.URI != "" ||
		destination.output.Protocol != "" ||
		destination.output.MIMEType != "" ||
		destination.output.Writer != nil ||
		destination.format != ""
}

func intentRequiresRuntime(intent intent) bool {
	for i := range intent.Streams {
		if operationSpecsRequireRuntime(intent.Streams[i].Operations) {
			return true
		}
	}
	return false
}

func operationSpecsRequireRuntime(operations []operationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpDecode, plan.OpEncode, plan.OpTransform:
			return true
		}
	}
	return false
}

func streamRulesRequireRuntime(rules []streamRule) bool {
	for i := range rules {
		if branchSpecsRequireRuntime(rules[i].branches) {
			return true
		}
	}
	return false
}

func branchSpecsRequireRuntime(branches []BranchSpec) bool {
	for i := range branches {
		if branchSpecRequiresRuntime(branches[i]) {
			return true
		}
	}
	return false
}

func branchSpecRequiresRuntime(branch BranchSpec) bool {
	return operationSpecsRequireRuntime(branch.operations) || destinationRefsRequireRuntime(branch.destinations)
}

func destinationRefsRequireRuntime(destinations []destinationRef) bool {
	for i := range destinations {
		if destinationSpecRequiresRuntime(destinations[i].dest) {
			return true
		}
	}
	return false
}

func validateJobIntentShapePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job intent shape", fn: func(state *recipeCompileState) error {
		return validateJobRecipeIntentShape(state.operation, state.recipe, state.jobOutputCount)
	}}
}

func validateJobRecipeIntentShape(operation string, recipe recipeir.Recipe, jobOutputCount int) error {
	if len(recipe.Inputs) == 0 {
		return &BuildError{
			Family:    errcode.FamilyForCode(inputMissingCode),
			Code:      inputMissingCode,
			Operation: operation,
			Reason:    "no input is configured",
			fixes: buildErrorFixes([]string{
				"start the recipe from an input: goav.From(goav.FileInput(\"in.webm\", reader))",
			}),
			cause: errUnsupportedBuild,
		}
	}
	streams := streamIntentsFromRecipeIR(recipe.Streams)
	stream, hasStream := jobRecipeIntentStream(streams)
	if len(recipe.Destinations) == 0 {
		return &BuildError{
			Family:    errcode.FamilyForCode(outputMissingCode),
			Code:      outputMissingCode,
			Operation: operation,
			Reason:    "no output is configured",
			fixes: buildErrorFixes([]string{
				"route the job to a destination: .To(goav.Write(\"out.webm\", writer))",
				"deliver frames to code with .To(goav.Sink(sink))",
			}),
			cause: errUnsupportedBuild,
		}
	}
	if len(streams) > 1 {
		return validateMultiStreamJobRecipeShape(operation, streams, jobOutputCount)
	}
	if err := validateJobRecipeOutputScope(operation, len(recipe.Destinations), jobOutputCount, stream, hasStream); err != nil {
		return err
	}
	if !hasStream {
		return nil
	}
	return validateJobStreamIntentShape(operation, stream)
}

// validateMultiStreamJobRecipeShape checks a job with several stream chains:
// every chain carries its own operations and stream-local destinations, and
// the job-level output scope stays empty (the chains own the routing).
func validateMultiStreamJobRecipeShape(operation string, streams []streamIntent, jobOutputCount int) error {
	if jobOutputCount != 0 {
		return jobOutputScopeMixedError(operation, streams[0])
	}
	for i := range streams {
		stream := streams[i]
		if len(stream.Destinations) == 0 {
			return jobStreamDestinationMissingError(operation, stream)
		}
		if err := validateJobStreamIntentShape(operation, stream); err != nil {
			return err
		}
	}
	return nil
}

func jobRecipeIntentStream(streams []streamIntent) (streamIntent, bool) {
	if len(streams) == 0 {
		return streamIntent{}, false
	}
	return streams[0], true
}

func jobStreamDestinationMissingError(operation string, stream streamIntent) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(outputMissingCode),
		Code:      outputMissingCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream chain has no destination",
		fixes: buildErrorFixes([]string{
			"finish each chain with .To(destination) before starting the next .Audio()/.Video()/.Stream()",
			"pass goav.Mux(name, destination) across chains to mux them together",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateJobRecipeOutputScope(operation string, destinationCount int, jobOutputCount int, stream streamIntent, hasStream bool) error {
	if !hasStream {
		return nil
	}
	if jobOutputCount == 0 && destinationCount == len(stream.Destinations) {
		return nil
	}
	return jobOutputScopeMixedError(operation, stream)
}

func jobOutputScopeMixedError(operation string, stream streamIntent) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(outputScopeMixedCode),
		Code:      outputScopeMixedCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes use stream-local outputs",
		fixes: buildErrorFixes([]string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).Encode(codec.VP9(...)).To(output)) for named branches",
		}),
		cause: errUnsupportedBuild,
	}
}

func jobDestinationReferenceMissingError(operation string, stream streamIntent, label string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(outputMissingCode),
		Code:      outputMissingCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream route output " + label + " is not attached",
		fixes: buildErrorFixes([]string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"finish each branch with a typed destination such as .To(output)",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateJobStreamIntentShape(operation string, stream streamIntent) error {
	selector := streamIntentSelector(stream)
	node := jobStreamIntentName(stream)
	if !streamIntentHasOperation(stream) {
		return operationSpecMissingError(operation, node)
	}
	if err := validateRecipeStreamSelector(operation, node, selector); err != nil {
		return err
	}
	if err := validateRecipeEncode(chainEncodeSpec(stream.Operations), operation, stream.Name); err != nil {
		return err
	}
	if err := validateCodecChangePolicy(operation, node, stream.CodecChange); err != nil {
		return err
	}
	return validateJobStreamTransformIntentShape(operation, stream)
}

func validateJobStreamTransformIntentShape(operation string, stream streamIntent) error {
	selector := streamIntentSelector(stream)
	node := jobStreamIntentName(stream)
	transforms := streamIntentTransformSpecs(stream)
	for i := range transforms {
		transform := transforms[i]
		if err := validateTransformSpec(operation, node, transform); err != nil {
			return err
		}
		switch {
		case transform.resize != nil && transform.resample != nil:
			return &BuildError{
				Family:    errcode.FamilyForCode(transformInvalidCode),
				Code:      transformInvalidCode,
				Operation: operation,
				Node:      node,
				Reason:    "one stream transform cannot be both resize and resample",
				fixes:     buildErrorFixes([]string{"declare two separate steps instead: .Resize(width, height).Resample(rate, channels)"}),
				cause:     errUnsupportedBuild,
			}
		case transform.resize != nil:
			if selector.Type == av.MediaAudio {
				return transformMediaError(node, "resize", av.MediaVideo, selector.Type)
			}
		case transform.resample != nil:
			if selector.Type == av.MediaVideo {
				return transformMediaError(node, "resample", av.MediaAudio, selector.Type)
			}
		default:
			return &BuildError{
				Family:    errcode.FamilyForCode(transformInvalidCode),
				Code:      transformInvalidCode,
				Operation: operation,
				Node:      node,
				Reason:    "empty stream transform",
				fixes: buildErrorFixes([]string{
					"call .Resize(width, height) for video streams",
					"call .Resample(sampleRate, channels) for audio streams",
				}),
				cause: errUnsupportedBuild,
			}
		}
	}
	return nil
}

func operationSpecMissingError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(streamOperationMissingCode),
		Code:      streamOperationMissingCode,
		Operation: operation,
		Node:      node,
		Reason:    "the stream was selected but no decode, processing stage, or encoder was requested",
		fixes: buildErrorFixes([]string{
			"call .Decode().To(goav.Sink(...)) to receive decoded frames",
			"call .Decode().Encode(codec.Opus(...)), .Decode().Encode(codec.VP8(...)), or .Decode().Encode(codec.VP9(...)) before writing to a file output",
			"use .Copy().To(output) for packet-preserving record or remux",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateJobAttachmentsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job attachments", fn: func(state *recipeCompileState) error {
		if err := validateJobInputs(state.inputAttachments); err != nil {
			return err
		}
		return validateDestinationSpecs(state.operation, state.outputAttachments, state.outputDestinationNames...)
	}}
}

func validateJobOutputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job output format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightOutputAdapters {
			return nil
		}
		outputs, err := validateOutputFormatAdapters(state.options.Context(), state.adapterRuntime(), state.outputAttachments, state.outputDestinationNames...)
		if err != nil {
			return err
		}
		state.outputAttachments = outputs
		return nil
	}}
}

func validateJobInputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job input format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightInputAdapters {
			return nil
		}
		probes, err := validateInputFormatAdapters(state.options.Context(), state.adapterRuntime(), state.inputAttachments)
		if err != nil {
			return err
		}
		state.inputProbes = probes
		return nil
	}}
}

func validateJobDecodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job decode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightDecodeAdapters {
			return nil
		}
		intent := state.intent
		intent.Streams = make([]streamIntent, 0, len(state.intent.Streams))
		for i := range state.intent.Streams {
			if streamNeedsDecodeForState(state, state.intent.Streams[i]) {
				intent.Streams = append(intent.Streams, state.intent.Streams[i])
			}
		}
		if len(intent.Streams) == 0 {
			return nil
		}
		return validateRecipeDecodeAdapters(state.operation, state.adapterRuntime(), intent)
	}}
}

func validateJobEncodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job encode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightEncodeAdapters {
			return nil
		}
		return validateRecipeEncodeAdapters(state.operation, state.adapterRuntime(), streamIntentsFromRecipeIR(state.recipe.Streams))
	}}
}

func validateJobTransformAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job transform adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightTransformAdapters {
			return nil
		}
		return validateRecipeTransformAdapters(state.operation, state.adapterRuntime(), streamIntentsFromRecipeIR(state.recipe.Streams))
	}}
}

func validateJobOutputBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job output bindings", fn: func(state *recipeCompileState) error {
		return validateJobRecipeOutputBindings(state.operation, state.recipe)
	}}
}

func validateJobStreamOutputKindsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job stream output kinds", fn: func(state *recipeCompileState) error {
		return validateJobRecipeStreamOutputKinds(state.operation, state.recipe)
	}}
}

func validateJobRecipeOutputBindings(operation string, recipe recipeir.Recipe) error {
	streams := streamIntentsFromRecipeIR(recipe.Streams)
	destinations := recipeIRDestinationLabelSet(recipe.Destinations)
	for i := range streams {
		stream := streams[i]
		for _, destinationName := range stream.Destinations {
			if _, ok := destinations[destinationName]; ok {
				continue
			}
			return jobDestinationReferenceMissingError(operation, stream, destinationName)
		}
	}
	return nil
}

func validateJobRecipeStreamOutputKinds(operation string, recipe recipeir.Recipe) error {
	streams := streamIntentsFromRecipeIR(recipe.Streams)
	if len(streams) == 1 {
		return validateJobStreamOutputKindsByKind(operation, streams[0], recipeIRDestinationKinds(recipe))
	}
	// Multiple chains may mix kinds across destinations (one chain to a mux
	// file, another to a sink); each chain is checked against its own output refs.
	kindByName := recipeIRDestinationKindSet(recipe.Destinations)
	for i := range streams {
		stream := streams[i]
		if err := validateJobStreamOutputKindsByKind(operation, stream, recipeIRDestinationKindsForStream(stream, kindByName)); err != nil {
			return err
		}
	}
	return nil
}

func validateJobStreamOutputKindsByKind(operation string, stream streamIntent, kinds []recipeir.DestinationKind) error {
	encode := chainEncodeSpec(stream.Operations)
	if recipeIRDestinationKindsContainSink(kinds) && recipeIRDestinationKindsContainMux(kinds) && !codecIntentSet(encode) {
		return mixedStreamOutputError(operation, stream)
	}
	if encode.ID == "" && !encode.Copy && recipeIRDestinationKindsContainMux(kinds) {
		return streamEncodeMissingError(operation, stream)
	}
	return nil
}

func recipeIRDestinationKindsForStream(stream streamIntent, kindByName map[string]recipeir.DestinationKind) []recipeir.DestinationKind {
	kinds := make([]recipeir.DestinationKind, 0, len(stream.Destinations))
	for _, name := range stream.Destinations {
		kind, ok := kindByName[name]
		if !ok {
			continue
		}
		kinds = append(kinds, kind)
	}
	return kinds
}

func recipeIRDestinationLabelSet(destinations []recipeir.Destination) map[string]struct{} {
	out := make(map[string]struct{}, len(destinations))
	for i := range destinations {
		out[recipeIRDestinationLabel(destinations[i], i)] = struct{}{}
	}
	return out
}

func recipeIRDestinationKindSet(destinations []recipeir.Destination) map[string]recipeir.DestinationKind {
	out := make(map[string]recipeir.DestinationKind, len(destinations))
	for i := range destinations {
		out[recipeIRDestinationLabel(destinations[i], i)] = destinations[i].Kind
	}
	return out
}

func recipeIRDestinationLabel(destination recipeir.Destination, index int) string {
	return firstNonEmpty(destination.Name, destination.URI, fmt.Sprintf("output-%d", index))
}

func recipeIRDestinationKindsContainSink(kinds []recipeir.DestinationKind) bool {
	for i := range kinds {
		if kinds[i] == recipeir.DestinationKindSink {
			return true
		}
	}
	return false
}

func recipeIRDestinationKindsContainMux(kinds []recipeir.DestinationKind) bool {
	for i := range kinds {
		if kinds[i] != recipeir.DestinationKindSink {
			return true
		}
	}
	return false
}

func validateBranchCompositionRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode recipe", fn: func(state *recipeCompileState) error {
		if !state.branchCompositionPresent {
			return nilRecipeError(state.operation, "nil transcode job")
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func validateBranchCompositionIntentShapePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode intent shape", fn: func(state *recipeCompileState) error {
		return validateBranchCompositionRecipeShape(state.operation, state.recipe)
	}}
}

func validateBranchCompositionAttachmentsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode attachments", fn: func(state *recipeCompileState) error {
		return validateBranchCompositionAttachments(state.branchInputAttachment, state.branchDestinationAttachments, state.branchCompositionSplit)
	}}
}

func validateBranchDestinationFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate branch destination format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightOutputAdapters {
			return nil
		}
		outputs := make([]destinationSpec, 0, len(state.branchDestinationAttachments))
		destinationNames := make([]string, 0, len(state.branchDestinationAttachments))
		for i := range state.branchDestinationAttachments {
			output := state.branchDestinationAttachments[i].output.withName(firstNonEmpty(
				state.branchDestinationAttachments[i].output.name,
				state.branchDestinationAttachments[i].name,
			))
			outputs = append(outputs, output)
			destinationNames = append(destinationNames, state.branchDestinationAttachments[i].name)
		}
		resolved, err := validateOutputFormatAdapters(state.options.Context(), state.adapterRuntime(), outputs, destinationNames...)
		if err != nil {
			return err
		}
		for i := range state.branchDestinationAttachments {
			state.branchDestinationAttachments[i].output = resolved[i]
		}
		return nil
	}}
}

func validateBranchInputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode input format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightInputAdapters {
			return nil
		}
		probes, err := validateInputFormatAdapters(state.options.Context(), state.adapterRuntime(), []InputSpec{state.branchInputAttachment})
		if err != nil {
			return err
		}
		if len(probes) != 0 {
			state.branchInputProbe = probes[0]
			state.branchInputProbeReady = true
		}
		return nil
	}}
}

func validateBranchEncodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode encode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightEncodeAdapters {
			return nil
		}
		return validateRecipeEncodeAdapters(state.operation, state.adapterRuntime(), streamIntentsFromRecipeIR(state.recipe.Streams))
	}}
}

func validateBranchTransformAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode transform adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightTransformAdapters {
			return nil
		}
		return validateRecipeTransformAdapters(state.operation, state.adapterRuntime(), streamIntentsFromRecipeIR(state.recipe.Streams))
	}}
}

func validateBranchDestinationBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate branch destination bindings", fn: func(state *recipeCompileState) error {
		return validateBranchRecipeDestinationBindings(state.recipe)
	}}
}

func validateBranchDestinationKindsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate branch destination kinds", fn: func(state *recipeCompileState) error {
		return validateBranchRecipeDestinationKinds(state.recipe)
	}}
}

func validateBranchRecipeDestinationBindings(recipe recipeir.Recipe) error {
	streams := streamIntentsFromRecipeIR(recipe.Streams)
	destinations := recipeIRDestinationLabelSet(recipe.Destinations)
	for i := range streams {
		stream := streams[i]
		for _, label := range stream.Destinations {
			if _, ok := destinations[label]; ok {
				continue
			}
			return branchDestinationReferenceMissingError(stream, label)
		}
	}
	return nil
}

func validateBranchRecipeDestinationKinds(recipe recipeir.Recipe) error {
	streams := streamIntentsFromRecipeIR(recipe.Streams)
	kindByName := recipeIRDestinationKindSet(recipe.Destinations)
	for i := range streams {
		stream := streams[i]
		hasMuxDestination := false
		for _, label := range stream.Destinations {
			kind, ok := kindByName[label]
			if !ok {
				continue
			}
			if kind != recipeir.DestinationKindSink {
				hasMuxDestination = true
				break
			}
		}
		if hasMuxDestination && !codecIntentSet(chainEncodeSpec(stream.Operations)) {
			return branchEncodeMissingError(stream)
		}
	}
	return nil
}

func validateRecipeAttachmentConsistencyPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe attachments", fn: func(state *recipeCompileState) error {
		switch {
		case state.jobPresent:
			inputCount := len(state.recipe.Inputs)
			if inputCount != len(state.inputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "inputs", inputCount, len(state.inputAttachments))
			}
			outputCount := len(state.recipe.Destinations)
			if outputCount != len(state.outputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "destinations", outputCount, len(state.outputAttachments))
			}
		case state.branchCompositionPresent:
			inputCount := len(state.recipe.Inputs)
			if inputCount != 1 {
				return recipeAttachmentMismatchError(state.operation, "inputs", inputCount, 1)
			}
			outputCount := len(state.recipe.Destinations)
			if outputCount != len(state.branchDestinationAttachments) {
				return recipeAttachmentMismatchError(state.operation, "destinations", outputCount, len(state.branchDestinationAttachments))
			}
		}
		return nil
	}}
}

func recipeAttachmentMismatchError(operation string, kind string, intentCount int, attachmentCount int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(recipeAttachmentMismatchCode),
		Code:      recipeAttachmentMismatchCode,
		Operation: operation,
		Reason:    kind + " intent and concrete attachments disagree",
		fields: buildErrorFields([]string{
			fmt.Sprintf("intent %s: %d", kind, intentCount),
			fmt.Sprintf("attached %s: %d", kind, attachmentCount),
		}),
		fixes: buildErrorFixes([]string{
			"build recipes through goav.From(input)",
			"keep custom compiler passes aligned with the public intent and captured attachments",
		}),
		cause: errUnsupportedBuild,
	}
}

func validatePacketJobOutputsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate packet job outputs", fn: func(state *recipeCompileState) error {
		return nil
	}}
}

func validateJobLiveStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job live stream selection", fn: func(state *recipeCompileState) error {
		if !state.options.preflightLiveStreams {
			return nil
		}
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			if jobStreamSelectionNeedsUnion(state, stream) {
				if err := validateJobStreamSelectionAcrossInputs(state, stream); err != nil {
					return err
				}
				continue
			}
			if !streamNeedsDecodeForState(state, stream) {
				continue
			}
			if err := validateLiveStreamSelection(state.recipeInputIntents(), stream); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validateJobKnownInputStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job known input stream selection", fn: func(state *recipeCompileState) error {
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			if jobStreamSelectionNeedsUnion(state, stream) {
				if err := validateJobStreamSelectionAcrossInputs(state, stream); err != nil {
					return err
				}
				continue
			}
			if !streamNeedsDecodeForState(state, stream) {
				continue
			}
			if err := validateKnownInputStreamSelection(state.inputProbes, stream); err != nil {
				return err
			}
		}
		return nil
	}}
}

// jobStreamSelectionNeedsUnion reports whether a stream chain selects across
// the union of several inputs (or was explicitly narrowed with InputName),
// which replaces per-probe selection checks with one combined view.
func jobStreamSelectionNeedsUnion(state *recipeCompileState, stream streamIntent) bool {
	if state == nil || !state.jobPresent {
		return false
	}
	return len(state.recipeInputIntents()) > 1 || stream.Select.Input != ""
}

// validateJobStreamSelectionAcrossInputs resolves one stream chain against the
// union of all input streams. Exactly one match is required; several matches
// fail with the candidate list (input + stream id + media kind) and
// InputName/StreamID narrowing suggestions.
func validateJobStreamSelectionAcrossInputs(state *recipeCompileState, stream streamIntent) error {
	sets := jobInputStreamSetsFromRecipeIR(state.recipeInputIntents(), state.inputFacts, state.inputProbes)
	_, _, err := selectStreamAcrossInputSets(sets, streamIntentSelector(stream), stream.Select.Input)
	return err
}

func validateJobKnownInputDecodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job known input decode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightDecodeAdapters {
			return nil
		}
		streams := make([]streamIntent, 0, len(state.intent.Streams))
		for i := range state.intent.Streams {
			if !streamNeedsDecodeForState(state, state.intent.Streams[i]) {
				continue
			}
			streams = append(streams, state.intent.Streams[i])
		}
		if len(streams) == 0 {
			return nil
		}
		return validateKnownRecipeDecodeAdapters(state.operation, state.adapterRuntime(), state.inputProbes, streams)
	}}
}

func streamNeedsDecodeForState(state *recipeCompileState, stream streamIntent) bool {
	if spec, ok := jobStreamCustomSourceShape(state, stream); ok && spec.Domain == shape.DomainFrame {
		return false
	}
	return streamNeedsDecode(stream)
}

// jobStreamCustomSourceShape resolves the declared source shape feeding one
// stream chain: the single input's shape on single-input jobs, or the shape of
// the input the chain's selector binds to on multi-input jobs.
func jobStreamCustomSourceShape(state *recipeCompileState, stream streamIntent) (shape.Spec, bool) {
	if state == nil {
		return shape.Spec{}, false
	}
	if state.branchCompositionPresent || len(state.inputAttachments) <= 1 {
		return compileStateCustomSourceShape(state)
	}
	sets := jobInputStreamSetsFromRecipeIR(state.recipeInputIntents(), state.inputFacts, state.inputProbes)
	index, ok := resolveInputSetIndex(sets, streamIntentSelector(stream), stream.Select.Input)
	if !ok {
		return shape.Spec{}, false
	}
	return state.inputSourceShape(index)
}

func (state *recipeCompileState) inputSourceShape(index int) (shape.Spec, bool) {
	if state == nil || index < 0 {
		return shape.Spec{}, false
	}
	if index < len(state.inputFacts) {
		if spec, ok := recipeIRInputSourceShape(state.inputFacts[index]); ok {
			return spec, true
		}
	}
	if index < len(state.inputAttachments) {
		return declaredSourceShape(state.inputAttachments[index])
	}
	return shape.Spec{}, false
}

func validateKnownBranchInputStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode known input stream selection", fn: func(state *recipeCompileState) error {
		if !state.branchInputProbeReady || len(state.branchInputProbe.Streams) == 0 {
			return nil
		}
		if spec, ok := state.inputSourceShape(0); ok && spec.Domain == shape.DomainFrame {
			for i := range state.intent.Streams {
				if _, err := selectStream(state.branchInputProbe.Streams, streamIntentSelector(state.intent.Streams[i])); err != nil {
					return err
				}
			}
			return nil
		}
		for i := range state.intent.Streams {
			if err := validateKnownProbeStreamSelection(state.branchInputProbe, state.intent.Streams[i]); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validateKnownBranchInputDecodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode known input decode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightDecodeAdapters || !state.branchInputProbeReady || len(state.branchInputProbe.Streams) == 0 {
			return nil
		}
		if spec, ok := state.inputSourceShape(0); ok && spec.Domain == shape.DomainFrame {
			return nil
		}
		return validateKnownRecipeDecodeAdapters(state.operation, state.adapterRuntime(), []format.ProbeResult{state.branchInputProbe}, state.intent.Streams)
	}}
}

func validateKnownInputStreamSelection(probes []format.ProbeResult, stream streamIntent) error {
	for i := range probes {
		if err := validateKnownProbeStreamSelection(probes[i], stream); err != nil {
			return err
		}
	}
	return nil
}

func validateKnownProbeStreamSelection(probe format.ProbeResult, stream streamIntent) error {
	if len(probe.Streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(probe.Streams, streamIntentSelector(stream))
	return err
}

// validateRecipeOperationShapesPass is the shape SOLVER pass: it propagates
// each chain's media shape across the canonical operation list, validates
// every operation contract, and — under an active .Auto(...) policy — inserts
// the allowed conversions as real planned operations on the intent (so every
// view, Describe, Explain, and the lowering, sees them).
func validateRecipeOperationShapesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe operation shapes", fn: func(state *recipeCompileState) error {
		rt := state.runtime
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			initial := recipeInitialStreamShape(state, stream)
			solved, diagnostics, err := solveOperationSpecShapes(state.operation, rt, stream, initial)
			if err != nil {
				return err
			}
			if solved == nil {
				continue
			}
			state.setSolvedStreamOperations(i, solved)
			state.shapeDiagnostics = append(state.shapeDiagnostics, diagnostics...)
			state.patchBranchComposeOperations(stream.Name, solved)
		}
		return nil
	}}
}

func (s *recipeCompileState) setSolvedStreamOperations(index int, solved []operationSpec) {
	if s == nil || index < 0 {
		return
	}
	solved = cloneOperationSpecs(solved)
	if index < len(s.intent.Streams) {
		s.intent.Streams[index].Operations = cloneOperationSpecs(solved)
	}
	if index < len(s.recipe.Streams) {
		s.recipe.Streams[index].Operations = recipeIROperationsFromSpecs(solved)
	}
}

// patchBranchComposeOperations re-points a pre-planned branch composition at
// the solved operation list, so the Build-side lowerer executes exactly the
// operations the solver planned (Describe ≡ Build).
func (s *recipeCompileState) patchBranchComposeOperations(name string, solved []operationSpec) {
	if s == nil || !branchComposePlanReady(s.plan) {
		return
	}
	for i := range s.plan.Branches {
		if s.plan.Branches[i].Name != name {
			continue
		}
		shared, private := splitOperationSpecsByShared(solved)
		s.plan.Branches[i].Operations = cloneOperationSpecs(solved)
		s.plan.Branches[i].SharedOperations = shared
		s.plan.Branches[i].PrivateOperations = private
		return
	}
}

func validateRecipeDestinationShapesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe destination shapes", fn: func(state *recipeCompileState) error {
		outputs := state.recipeDestinationSet()
		if len(outputs) == 0 {
			return nil
		}
		kinds := state.recipeDestinationKindSet()
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			shape := recipeFinalStreamShape(state, stream)
			node := firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
			for _, label := range stream.Destinations {
				destination, ok := outputs[label]
				if !ok {
					continue
				}
				kind := kinds[label]
				if kind == recipeir.DestinationKindUnknown {
					kind = recipeIRDestinationKindFromSpec(destination)
				}
				if err := validateRecipeDestinationShape(state.operation, node, label, kind, destination, shape); err != nil {
					return err
				}
			}
		}
		return nil
	}}
}

func (s *recipeCompileState) recipeDestinationSet() map[string]destinationSpec {
	if s == nil {
		return nil
	}
	if s.branchCompositionPresent {
		return branchDestinationSet(s.branchDestinationAttachments)
	}
	outputs := make(map[string]destinationSpec, len(s.outputAttachments))
	for i := range s.outputAttachments {
		outputs[jobOutputDestinationName(s.outputAttachments, s.outputDestinationNames, i)] = s.outputAttachments[i]
	}
	return outputs
}

func (s *recipeCompileState) recipeDestinationKindSet() map[string]recipeir.DestinationKind {
	if s == nil || len(s.destinationKinds) == 0 {
		return nil
	}
	kinds := make(map[string]recipeir.DestinationKind, len(s.destinationKinds))
	for i := range s.intent.Destinations {
		if i >= len(s.destinationKinds) {
			break
		}
		kind := s.destinationKinds[i]
		if kind == recipeir.DestinationKindUnknown {
			continue
		}
		label := firstNonEmpty(s.intent.Destinations[i].Name, s.intent.Destinations[i].URI, fmt.Sprintf("output-%d", i))
		kinds[label] = kind
	}
	return kinds
}

func recipeInitialStreamShape(state *recipeCompileState, stream streamIntent) shape.Spec {
	var spec shape.Spec
	sourceShape, sourceShapeOK := jobStreamCustomSourceShape(state, stream)
	if selected, ok := planSelectedStream(state, stream); ok {
		domain := shape.DomainPacket
		if sourceShapeOK && sourceShape.Domain != "" {
			domain = sourceShape.Domain
		}
		spec = shape.FromStream(selected, domain)
		if sourceShapeOK {
			spec = shape.Merge(spec, sourceShape)
		}
	}
	if state != nil {
		spec = normalizePlanBranchShape(spec, stream, planStreamInput(state, stream))
	} else {
		spec = normalizePlanBranchShape(spec, stream, inputIntent{})
	}
	return spec
}

func recipeFinalStreamShape(state *recipeCompileState, stream streamIntent) shape.Spec {
	shape := recipeInitialStreamShape(state, stream)
	shape = normalizeTapShape(shape)
	if shape.MediaKind == "" {
		shape.MediaKind = stream.Select.Type
	}
	if shape.Codec == "" {
		shape.Codec = stream.Select.Codec
	}
	for i := range stream.Operations {
		shape = operationSpecOutputShape(shape, stream.Operations[i])
	}
	return shape
}

func validateRecipeDestinationShape(operation string, node string, destinationName string, kind recipeir.DestinationKind, destination destinationSpec, spec shape.Spec) error {
	if kind == recipeir.DestinationKindSink {
		return nil
	}
	if kind == recipeir.DestinationKindUnknown && !destinationSpecHasOutput(destination) {
		return nil
	}
	if spec.Domain == shape.DomainPacket {
		return nil
	}
	return destinationShapeMismatchError(operation, node, destinationName, destination, spec)
}

func destinationShapeMismatchError(operation string, node string, destinationName string, destination destinationSpec, spec shape.Spec) error {
	label := firstNonEmpty(destinationName, destination.label("destination"))
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationShapeMismatchCode),
		Code:      destinationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, label, "destination"),
		Reason:    "byte or mux destination requires packet-domain media",
		fields: buildErrorFields([]string{
			"destination=" + label,
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(spec.MediaKind)).String(),
			"actual_shape=" + spec.String(),
		}),
		fixes: buildErrorFixes([]string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before writing to file, URI, or writer destinations",
			"use .Copy() from a packet-domain stream point for packet-preserving output",
			"send frame-domain media to goav.Sink(...) instead of a byte destination",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateOperationSpecShapes(operation string, stream streamIntent, initial shape.Spec) error {
	shape := normalizeTapShape(initial)
	if shape.MediaKind == "" {
		shape.MediaKind = stream.Select.Type
	}
	if shape.Codec == "" {
		shape.Codec = stream.Select.Codec
	}
	node := firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
	for i := range stream.Operations {
		next := stream.Operations[i]
		// Taps and shape annotations advance the lineage unchecked; a
		// .Require(...) assertion falls through to the contract check below.
		if next.Kind == plan.OpTap || (next.Kind == plan.OpShape && next.Require == nil) {
			shape = operationSpecOutputShape(shape, next)
			continue
		}
		expected := next.InputShapes()
		if len(expected) != 0 && !expected.Accepts(shape) {
			return operationShapeFailureError(operation, node, i, next, expected, shape)
		}
		shape = operationSpecOutputShape(shape, next)
	}
	return nil
}

// operationShapeFailureError dispatches a failed shape contract to its
// surface: a .Require(...) assertion gets the requirement-specific refusal,
// every other operation keeps the established mismatch error.
func operationShapeFailureError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	if step.Kind == plan.OpShape && step.Require != nil {
		return shapeRequirementUnmetError(operation, node, index, step, expected, actual)
	}
	return operationShapeMismatchError(operation, node, index, step, expected, actual)
}

// shapeRequirementUnmetError is the hard .Require(...) refusal: the stream at
// this chain position does not satisfy the asserted shape. It carries the
// actual and required shapes in the established refusal format; the solver
// appends the exact .Auto(...) fix when a conversion could satisfy it.
func shapeRequirementUnmetError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	required := shape.Spec{}
	if step.Require != nil {
		required = *step.Require
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(shapeRequirementUnmetCode),
		Code:      shapeRequirementUnmetCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf(".Require(...) is not satisfied: the stream is %s, required %s",
			humanizeShape(actual), humanizeShape(required)),
		fields: buildErrorFields([]string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=require",
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + shapeSetString(expected),
		}),
		fixes: buildErrorFixes([]string{
			"adjust the chain so the stream satisfies the required shape before .Require(...)",
			"relax or remove the .Require(...) assertion",
		}),
		cause: errUnsupportedBuild,
	}
}

func operationSpecOutputShape(input shape.Spec, operation operationSpec) shape.Spec {
	out := operation.OutputShapes(input)
	if len(out) == 0 {
		return input
	}
	return out[0]
}

func operationShapeMismatchError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	component := firstNonEmpty(step.Component, operationSpecComponent(step), string(step.Kind), "operation")
	return &BuildError{
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    component + " cannot consume the current media shape",
		fields: buildErrorFields([]string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=" + string(step.Kind),
			"expected_shape=" + shapeSetString(expected),
			"actual_shape=" + actual.String(),
		}),
		fixes: buildErrorFixes(operationShapeMismatchSuggestions(step)),
		cause: errUnsupportedBuild,
	}
}

func operationSpecComponent(operation operationSpec) string {
	switch operation.Kind {
	case plan.OpDecode:
		return firstNonEmpty(string(operation.Decode.ID), operation.Component, "decode")
	case plan.OpTransform:
		return firstNonEmpty(transformFactoryName(operation.Transform), "transform")
	case plan.OpEncode:
		return firstNonEmpty(string(operation.Encode.ID), operation.Component, "encode")
	case plan.OpCopy:
		return "packet-copy"
	default:
		return operation.Component
	}
}

func shapeSetString(shapes shape.Set) string {
	if len(shapes) == 0 {
		return "any"
	}
	parts := make([]string, 0, len(shapes))
	for i := range shapes {
		parts = append(parts, shapes[i].String())
	}
	return strings.Join(parts, " | ")
}

func operationShapeMismatchSuggestions(operation operationSpec) []string {
	switch operation.Kind {
	case plan.OpDecode:
		return []string{
			"decode only consumes packet-domain media",
			"remove duplicate .Decode() calls after a frame tap",
			"start from goav.PacketTap(name) when a runtime branch should decode",
		}
	case plan.OpTransform:
		return []string{
			"call .Decode() before frame transforms when starting from packets",
			"use .Video().Resize(...) for video frames",
			"use .Audio().Resample(...) for audio frames",
		}
	case plan.OpEncode:
		return []string{
			"call .Decode() before encoding when starting from packets",
			"keep .Shape(...) annotations in the frame domain before encoders",
			"use .Copy() instead of an encoder for packet-preserving fanout",
		}
	case plan.OpCopy:
		return []string{
			"copy only consumes packet-domain media",
			"move .Copy() before decode or start from goav.PacketTap(name)",
			"use .Encode(codec...) instead of .Copy() to turn decoded frames back into packets",
			"use a sink destination when the branch should remain decoded",
		}
	default:
		return []string{
			"inspect Explain(ctx) to see operation shapes",
			"keep structural facts in goav.Shape(...) and codec behavior in codec.CodecSpec options",
		}
	}
}

func planBranchCompositionIntentPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "plan branch composition intent", fn: func(state *recipeCompileState) error {
		if state.planErr != nil {
			return state.planErr
		}
		recipe := state.recipe
		if branchComposePlanReady(state.plan) {
			fresh, err := planBranchCompositionRecipe(recipe, state.branchInputAttachment, state.branchDestinationAttachments)
			if err != nil {
				return err
			}
			state.plan.Input = fresh.Input
			state.plan.Destinations = fresh.Destinations
			return nil
		}
		composePlan, err := planBranchCompositionRecipe(recipe, state.branchInputAttachment, state.branchDestinationAttachments)
		if err != nil {
			return err
		}
		state.plan = composePlan
		return nil
	}}
}

func recipeGraphUnsupportedError(operation string, intent intent) error {
	details := []string{
		fmt.Sprintf("recipe: %s", firstNonEmpty(intent.Name, "unnamed")),
		fmt.Sprintf("inputs: %d", len(intent.Inputs)),
		fmt.Sprintf("streams: %d", len(intent.Streams)),
		fmt.Sprintf("destinations: %d", len(intent.Destinations)),
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.RecipeGraphUnsupported),
		Code:      errcode.RecipeGraphUnsupported,
		Operation: operation,
		Reason:    "recipe intent did not match a supported graph plan",
		fields:    buildErrorFields(details),
		fixes: buildErrorFixes([]string{
			"use goav.From(input).Copy().To(output...) for packet-preserving record or remux",
			"use goav.From(input).Audio().Decode().To(goav.Sink(...)) or .Video().Decode().To(...) for decoded frames",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).Encode(codec.VP9(...)).To(output)) for named branches",
		}),
		cause: errUnsupportedBuild,
	}
}

func requireGraphPlanSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "require graph plan spec", fn: func(state *recipeCompileState) error {
		if state.specReady && state.specOrigin == graphSpecOriginGraphPlan && state.graphPlan.ready() {
			return nil
		}
		if state.runtimeExplicit && state.runtime == nil {
			return nil
		}
		return recipeGraphUnsupportedError(state.operation, state.intent)
	}}
}
