package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type recipeResolved struct {
	intent                Intent
	runtime               Runtime
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
}

type recipeCompileState struct {
	operation string
	intent    Intent
	runtime   Runtime
	options   recipeCompileOptions

	jobPresent               bool
	branchCompositionPresent bool
	recipeErr                error

	inputAttachments       []InputSpec
	jobOutputCount         int
	outputAttachments      []destinationSpec
	outputDestinationNames []string
	inputProbes            []format.ProbeResult

	branchInputAttachment        InputSpec
	branchDestinationAttachments []namedDestinationSpec
	branchInputProbe             format.ProbeResult
	branchInputProbeReady        bool
	branchCompositionSplit       bool

	// joinAttachment is the captured joinSpec for a Mix/Composite/Select job;
	// joinPlan is the planned multi-upstream join the lowerer pass produced,
	// the source buildWorkPlan renders the join work plan from.
	joinAttachment *joinSpec
	joinPlan       *joinPlan

	plan    branchComposePlan
	planErr error

	spec       pipeline.Spec
	specReady  bool
	specOrigin string
	graphPlan  graphPlan
}

type recipeCompileOptions struct {
	ctx                        context.Context
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

func (c recipeIntentCompiler) Compile(state recipeCompileState) (recipeResolved, error) {
	for i := range c.passes {
		pass := c.passes[i]
		if pass == nil {
			return recipeResolved{}, &BuildError{
				Code:      "compiler_pass_invalid",
				Operation: state.operation,
				Reason:    fmt.Sprintf("recipe compiler pass %d is nil", i),
				Cause:     ErrUnsupportedBuild,
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
	}, nil
}

func compilerPassError(operation string, pass string, err error) error {
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr == nil {
		return err
	}
	if buildErr.Code != "" || buildErr.Reason != "" || len(buildErr.Details) != 0 || len(buildErr.Suggestions) != 0 {
		return err
	}
	return &BuildError{
		Code:      "compiler_pass_failed",
		Operation: firstNonEmpty(buildErr.Operation, operation),
		Reason:    "recipe compiler pass failed without a diagnostic",
		Details: []string{
			"pass=" + pass,
		},
		Suggestions: []string{
			"run Explain(ctx) to inspect the partial plan",
			"report the pass name with the recipe shape",
		},
		Cause: err,
	}
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if r.specReady && r.specOrigin == graphSpecOriginGraphPlan && r.graphPlan.ready() {
		return r.graphPlan.Describe()
	}
	return pipeline.Spec{}, recipeGraphUnsupportedError("describe recipe", r.intent)
}

func (r recipeResolved) Build(ctx context.Context) (Task, error) {
	if !r.graphPlan.ready() {
		return nil, recipeGraphUnsupportedError("build recipe", r.intent)
	}
	task, err := r.graphPlan.Build(ctx)
	if err != nil {
		return nil, err
	}
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

func tapInfosFromPlan(taps []workTap) []info.Tap {
	out := make([]info.Tap, 0, len(taps))
	seen := make(map[string]struct{}, len(taps))
	for i := range taps {
		if taps[i].Name == "" {
			continue
		}
		if _, ok := seen[taps[i].Name]; ok {
			continue
		}
		seen[taps[i].Name] = struct{}{}
		out = append(out, info.Tap{
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
	if job != nil && job.join != nil {
		return compileJobJoinRecipeWithOptions(job, options)
	}
	if job != nil && len(job.branchStreams) != 0 {
		return compileJobBranchRecipeWithOptions(job, options)
	}
	state := recipeCompileState{
		operation: "build job",
		options:   options,
	}
	if job != nil {
		state.jobPresent = true
		state.intent = job.plan()
		state.runtime = job.runtime
		state.recipeErr = job.err
		state.inputAttachments = append([]InputSpec(nil), job.inputs...)
		state.jobOutputCount = len(job.outputs)
		streamOutputs, _ := job.streamOutputsAndNames()
		state.outputAttachments = jobAllOutputs(job.outputs, streamOutputs)
		state.outputDestinationNames = job.allOutputNames()
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateJobRecipePass(),
		validateJobIntentShapePass(),
		validateRecipeAttachmentConsistencyPass(),
		validateJobAttachmentsPass(),
		validateJobOutputBindingsPass(),
		validateJobStreamOutputKindsPass(),
		validatePacketJobOutputsPass(),
		validateJobLiveStreamSelectionPass(),
		validateJobOutputFormatAdaptersPass(),
		validateJobDecodeAdaptersPass(),
		validateJobEncodeAdaptersPass(),
		validateJobTransformAdaptersPass(),
		validateJobInputFormatAdaptersPass(),
		validateJobKnownInputStreamSelectionPass(),
		validateRecipeOperationShapesPass(),
		validateRecipeDestinationShapesPass(),
		validateJobKnownInputDecodeAdaptersPass(),
		validateRecipeRuntimePass(),
		emitGraphPlanSpecPass(),
		validateMuxCompatibilityPass(),
		requireGraphPlanSpecPass(),
	}}.Compile(state)
}

// compileJobJoinRecipeWithOptions lowers a join job (Mix/Composite/Select)
// through the same recipe compiler as every other job: the joinSpec is
// normalized into the compile state, the join lowerer plans the N-to-1 graph,
// and the shared passes emit and validate the one plan Describe and Build run
// from. No separate graph assembly exists for joins.
func compileJobJoinRecipeWithOptions(job *Job, options recipeCompileOptions) (recipeResolved, error) {
	spec := job.join
	state := recipeCompileState{
		operation:      "build " + string(spec.kind),
		options:        options,
		intent:         joinIntent(job),
		runtime:        job.runtime,
		recipeErr:      job.err,
		joinAttachment: spec,
	}
	state.inputAttachments = joinArmInputs(spec)
	state.outputAttachments, state.outputDestinationNames = joinOutputAttachments(spec)
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateJoinRecipePass(),
		validateJobInputFormatAdaptersPass(),
		validateRecipeRuntimePass(),
		emitGraphPlanSpecPass(),
		validateMuxCompatibilityPass(),
		requireGraphPlanSpecPass(),
	}}.Compile(state)
}

func validateJoinRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate join recipe", fn: func(state *recipeCompileState) error {
		if state.joinAttachment == nil {
			return &BuildError{
				Code:      "job_invalid",
				Operation: state.operation,
				Reason:    "nil join",
				Cause:     ErrUnsupportedBuild,
			}
		}
		if state.runtime == nil {
			return &BuildError{Code: "runtime_missing", Operation: state.operation, Reason: "no runtime is configured", Cause: ErrUnsupportedBuild}
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func compileJobBranchRecipeWithOptions(job *Job, options recipeCompileOptions) (recipeResolved, error) {
	branchJob := &branchCompositionJob{
		runtime:         job.runtime,
		name:            job.name,
		streams:         append([]streamBuild(nil), job.branchStreams...),
		outputs:         append([]namedDestinationSpec(nil), job.branchDestinations...),
		err:             job.err,
		fromBranchSplit: true,
	}
	if len(job.inputs) == 1 {
		branchJob.input = job.inputs[0]
	} else if branchJob.err == nil {
		branchJob.err = branchInputCountError("branches", len(job.inputs))
	}
	return compileBranchCompositionRecipeWithOptions(branchJob, options)
}

func compileBranchCompositionRecipeWithOptions(job *branchCompositionJob, options recipeCompileOptions) (recipeResolved, error) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		options:   options,
	}
	if job != nil {
		state.branchCompositionPresent = true
		state.intent = job.plan()
		state.runtime = job.runtime
		state.recipeErr = job.err
		state.branchInputAttachment = job.input
		state.branchDestinationAttachments = append([]namedDestinationSpec(nil), job.outputs...)
		state.branchCompositionSplit = job.fromBranchSplit
		state.plan, state.planErr = job.composePlan()
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateBranchCompositionRecipePass(),
		validateBranchCompositionIntentShapePass(),
		validateRecipeAttachmentConsistencyPass(),
		validateBranchCompositionAttachmentsPass(),
		validateBranchDestinationBindingsPass(),
		validateBranchDestinationKindsPass(),
		validateBranchDestinationFormatAdaptersPass(),
		validateBranchEncodeAdaptersPass(),
		validateBranchTransformAdaptersPass(),
		validateBranchInputFormatAdaptersPass(),
		validateKnownBranchInputStreamSelectionPass(),
		validateRecipeOperationShapesPass(),
		validateRecipeDestinationShapesPass(),
		validateKnownBranchInputDecodeAdaptersPass(),
		planBranchCompositionIntentPass(),
		validateRecipeRuntimePass(),
		emitGraphPlanSpecPass(),
		validateMuxCompatibilityPass(),
		requireGraphPlanSpecPass(),
	}}.Compile(state)
}

func validateJobRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job recipe", fn: func(state *recipeCompileState) error {
		if !state.jobPresent {
			return &BuildError{
				Code:      "job_invalid",
				Operation: state.operation,
				Reason:    "nil job",
				Cause:     ErrUnsupportedBuild,
			}
		}
		if state.runtime == nil {
			return &BuildError{Code: "runtime_missing", Operation: state.operation, Reason: "no runtime is configured", Cause: ErrUnsupportedBuild}
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func validateRecipeRuntimePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe runtime", fn: func(state *recipeCompileState) error {
		if _, ok := state.runtime.(*runtime); ok {
			return nil
		}
		return recipeRuntimeUnsupportedError(state.operation)
	}}
}

func validateJobIntentShapePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job intent shape", fn: func(state *recipeCompileState) error {
		return validateJobIntentShape(state.operation, state.intent, state.jobOutputCount)
	}}
}

func validateJobIntentShape(operation string, intent Intent, jobOutputCount int) error {
	if len(intent.Inputs) == 0 {
		return &BuildError{Code: "input_missing", Operation: operation, Reason: "no input is configured", Cause: ErrUnsupportedBuild}
	}
	stream, hasStream := jobIntentStream(intent)
	if len(intent.Destinations) == 0 {
		return &BuildError{Code: "output_missing", Operation: operation, Reason: "no output is configured", Cause: ErrUnsupportedBuild}
	}
	if len(intent.Streams) > 1 {
		return validateMultiStreamJobIntentShape(operation, intent, jobOutputCount)
	}
	if err := validateJobIntentOutputScope(operation, intent, jobOutputCount, stream, hasStream); err != nil {
		return err
	}
	if !hasStream {
		return nil
	}
	return validateJobStreamIntentShape(operation, stream)
}

// validateMultiStreamJobIntentShape checks a job with several stream chains:
// every chain carries its own operations and stream-local destinations, and
// the job-level output scope stays empty (the chains own the routing).
func validateMultiStreamJobIntentShape(operation string, intent Intent, jobOutputCount int) error {
	if jobOutputCount != 0 {
		return jobOutputScopeMixedError(operation, intent.Streams[0])
	}
	for i := range intent.Streams {
		stream := intent.Streams[i]
		if len(stream.Destinations) == 0 {
			return jobStreamDestinationMissingError(operation, stream)
		}
		if err := validateJobStreamIntentShape(operation, stream); err != nil {
			return err
		}
	}
	return nil
}

func jobStreamDestinationMissingError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      "output_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream chain has no destination",
		Suggestions: []string{
			"finish each chain with .To(destination) before starting the next .Audio()/.Video()/.Stream()",
			"share one destination handle across chains to mux them together",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateJobIntentOutputScope(operation string, intent Intent, jobOutputCount int, stream streamIntent, hasStream bool) error {
	if !hasStream {
		return nil
	}
	if jobOutputCount == 0 && len(intent.Destinations) == len(stream.Destinations) {
		return nil
	}
	return jobOutputScopeMixedError(operation, stream)
}

func jobOutputScopeMixedError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      "output_scope_mixed",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes use stream-local outputs",
		Suggestions: []string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).Encode(codec.VP9(...)).To(output)) for named branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobDestinationReferenceMissingError(operation string, stream streamIntent, label string) error {
	return &BuildError{
		Code:      "output_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream route output " + label + " is not attached",
		Suggestions: []string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"finish each branch with a typed destination such as .To(output)",
		},
		Cause: ErrUnsupportedBuild,
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
	if err := validateRecipeEncode(stream.Encode, operation, stream.Name); err != nil {
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
		case transform.Resize != nil && transform.Resample != nil:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: operation,
				Node:      node,
				Reason:    "one stream transform cannot be both resize and resample",
				Cause:     ErrUnsupportedBuild,
			}
		case transform.Resize != nil:
			if selector.Type == av.MediaAudio {
				return transformMediaError(node, "resize", av.MediaVideo, selector.Type)
			}
		case transform.Resample != nil:
			if selector.Type == av.MediaVideo {
				return transformMediaError(node, "resample", av.MediaAudio, selector.Type)
			}
		default:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: operation,
				Node:      node,
				Reason:    "empty stream transform",
				Suggestions: []string{
					"call .Resize(width, height) for video streams",
					"call .Resample(sampleRate, channels) for audio streams",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
	}
	return nil
}

func operationSpecMissingError(operation string, node string) error {
	return &BuildError{
		Code:      "stream_operation_missing",
		Operation: operation,
		Node:      node,
		Reason:    "the stream was selected but no decode, processing stage, or encoder was requested",
		Suggestions: []string{
			"call .To(goav.Sink(...)) to receive decoded frames",
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before writing to a file output",
			"use .Copy().To(output) for packet-preserving record or remux",
		},
		Cause: ErrUnsupportedBuild,
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
		outputs, err := validateOutputFormatAdapters(state.options.Context(), state.runtime, state.outputAttachments, state.outputDestinationNames...)
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
		probes, err := validateInputFormatAdapters(state.options.Context(), state.runtime, state.inputAttachments)
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
		return validateRecipeDecodeAdapters(state.operation, state.runtime, state.intent)
	}}
}

func validateJobEncodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job encode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightEncodeAdapters {
			return nil
		}
		return validateRecipeEncodeAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateJobTransformAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job transform adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightTransformAdapters {
			return nil
		}
		return validateRecipeTransformAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateJobOutputBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job output bindings", fn: func(state *recipeCompileState) error {
		for i := range state.intent.Streams {
			if err := validateJobOutputBindings(state.operation, state.intent.Streams[i], state.outputAttachments, state.outputDestinationNames); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validateJobStreamOutputKindsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job stream output kinds", fn: func(state *recipeCompileState) error {
		if len(state.intent.Streams) == 1 {
			return validateJobStreamOutputKinds(state.operation, state.intent.Streams[0], state.outputAttachments)
		}
		// Multiple chains may mix kinds across destinations (one chain to a
		// mux file, another to a sink); each chain is checked against the
		// destinations it actually routes to.
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			if err := validateJobStreamOutputKinds(state.operation, stream, jobStreamDestinationSubset(state, stream)); err != nil {
				return err
			}
		}
		return nil
	}}
}

func jobStreamDestinationSubset(state *recipeCompileState, stream streamIntent) []destinationSpec {
	outputs := make([]destinationSpec, 0, len(stream.Destinations))
	for i := range state.outputAttachments {
		name := jobOutputDestinationName(state.outputAttachments, state.outputDestinationNames, i)
		if stringInSlice(name, stream.Destinations) {
			outputs = append(outputs, state.outputAttachments[i])
		}
	}
	return outputs
}

func validateBranchCompositionRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode recipe", fn: func(state *recipeCompileState) error {
		if !state.branchCompositionPresent {
			return &BuildError{
				Code:      "job_invalid",
				Operation: state.operation,
				Reason:    "nil transcode job",
				Cause:     ErrUnsupportedBuild,
			}
		}
		if state.runtime == nil {
			return &BuildError{Code: "runtime_missing", Operation: state.operation, Reason: "no runtime is configured", Cause: ErrUnsupportedBuild}
		}
		if state.recipeErr != nil {
			return state.recipeErr
		}
		return nil
	}}
}

func validateBranchCompositionIntentShapePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode intent shape", fn: func(state *recipeCompileState) error {
		return validateBranchCompositionIntentShape(state.operation, state.intent)
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
		resolved, err := validateOutputFormatAdapters(state.options.Context(), state.runtime, outputs, destinationNames...)
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
		probes, err := validateInputFormatAdapters(state.options.Context(), state.runtime, []InputSpec{state.branchInputAttachment})
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
		return validateRecipeEncodeAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateBranchTransformAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode transform adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightTransformAdapters {
			return nil
		}
		return validateRecipeTransformAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateBranchDestinationBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate branch destination bindings", fn: func(state *recipeCompileState) error {
		return validateBranchDestinationBindings(state.intent, state.branchDestinationAttachments)
	}}
}

func validateBranchDestinationKindsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate branch destination kinds", fn: func(state *recipeCompileState) error {
		return validateBranchDestinationKinds(state.intent, state.branchDestinationAttachments)
	}}
}

func validateRecipeAttachmentConsistencyPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe attachments", fn: func(state *recipeCompileState) error {
		switch {
		case state.jobPresent:
			if len(state.intent.Inputs) != len(state.inputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "inputs", len(state.intent.Inputs), len(state.inputAttachments))
			}
			if len(state.intent.Destinations) != len(state.outputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "destinations", len(state.intent.Destinations), len(state.outputAttachments))
			}
		case state.branchCompositionPresent:
			if len(state.intent.Inputs) != 1 {
				return recipeAttachmentMismatchError(state.operation, "inputs", len(state.intent.Inputs), 1)
			}
			if len(state.intent.Destinations) != len(state.branchDestinationAttachments) {
				return recipeAttachmentMismatchError(state.operation, "destinations", len(state.intent.Destinations), len(state.branchDestinationAttachments))
			}
		}
		return nil
	}}
}

func recipeAttachmentMismatchError(operation string, kind string, intentCount int, attachmentCount int) error {
	return &BuildError{
		Code:      "recipe_attachment_mismatch",
		Operation: operation,
		Reason:    kind + " intent and concrete attachments disagree",
		Details: []string{
			fmt.Sprintf("intent %s: %d", kind, intentCount),
			fmt.Sprintf("attached %s: %d", kind, attachmentCount),
		},
		Suggestions: []string{
			"build recipes through goav.From(input)",
			"keep custom compiler passes aligned with the public Intent and captured attachments",
		},
		Cause: ErrUnsupportedBuild,
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
			if err := validateLiveStreamSelection(state.intent.Inputs, stream); err != nil {
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
// which replaces the legacy per-probe selection checks.
func jobStreamSelectionNeedsUnion(state *recipeCompileState, stream streamIntent) bool {
	if state == nil || !state.jobPresent {
		return false
	}
	return len(state.intent.Inputs) > 1 || stream.Select.Input != ""
}

// validateJobStreamSelectionAcrossInputs resolves one stream chain against the
// union of all input streams. Exactly one match is required; several matches
// fail with the candidate list (input + stream id + media kind) and
// InputName/StreamID narrowing suggestions.
func validateJobStreamSelectionAcrossInputs(state *recipeCompileState, stream streamIntent) error {
	sets := jobInputStreamSets(state.intent.Inputs, state.inputAttachments, state.inputProbes)
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
		return validateKnownRecipeDecodeAdapters(state.operation, state.runtime, state.inputProbes, streams)
	}}
}

func streamNeedsDecodeForState(state *recipeCompileState, stream streamIntent) bool {
	if spec, ok := jobStreamCustomSourceShape(state, stream); ok && spec.Domain == shape.DomainFrame {
		return false
	}
	return streamNeedsDecode(stream)
}

// jobStreamCustomSourceShape resolves the custom-source shape feeding one
// stream chain: the single input's shape on single-input jobs (legacy), or the
// shape of the input the chain's selector binds to on multi-input jobs.
func jobStreamCustomSourceShape(state *recipeCompileState, stream streamIntent) (shape.Spec, bool) {
	if state == nil {
		return shape.Spec{}, false
	}
	if state.branchCompositionPresent || len(state.inputAttachments) <= 1 {
		return compileStateCustomSourceShape(state)
	}
	sets := jobInputStreamSets(state.intent.Inputs, state.inputAttachments, state.inputProbes)
	index, ok := resolveInputSetIndex(sets, streamIntentSelector(stream), stream.Select.Input)
	if !ok || index >= len(state.inputAttachments) {
		return shape.Spec{}, false
	}
	return customSourceShape(state.inputAttachments[index])
}

func validateKnownBranchInputStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode known input stream selection", fn: func(state *recipeCompileState) error {
		if !state.branchInputProbeReady || len(state.branchInputProbe.Streams) == 0 {
			return nil
		}
		if spec, ok := customSourceShape(state.branchInputAttachment); ok && spec.Domain == shape.DomainFrame {
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
		if spec, ok := customSourceShape(state.branchInputAttachment); ok && spec.Domain == shape.DomainFrame {
			return nil
		}
		return validateKnownRecipeDecodeAdapters(state.operation, state.runtime, []format.ProbeResult{state.branchInputProbe}, state.intent.Streams)
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

func validateRecipeOperationShapesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe operation shapes", fn: func(state *recipeCompileState) error {
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			shape := recipeInitialStreamShape(state, stream)
			if err := validateOperationSpecShapes(state.operation, stream, shape); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validateRecipeDestinationShapesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe destination shapes", fn: func(state *recipeCompileState) error {
		outputs := state.recipeDestinationSet()
		if len(outputs) == 0 {
			return nil
		}
		for i := range state.intent.Streams {
			stream := state.intent.Streams[i]
			shape := recipeFinalStreamShape(state, stream)
			node := firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
			for _, label := range stream.Destinations {
				destination, ok := outputs[label]
				if !ok {
					continue
				}
				if err := validateRecipeDestinationShape(state.operation, node, label, destination, shape); err != nil {
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

func validateRecipeDestinationShape(operation string, node string, destinationName string, destination destinationSpec, spec shape.Spec) error {
	if destination.sink != nil {
		return nil
	}
	if !destinationSpecHasOutput(destination) {
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
		Code:      "destination_shape_mismatch",
		Operation: operation,
		Node:      firstNonEmpty(node, label, "destination"),
		Reason:    "byte or mux destination requires packet-domain media",
		Details: []string{
			"destination=" + label,
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(spec.MediaKind)).String(),
			"actual_shape=" + spec.String(),
		},
		Suggestions: []string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before writing to file, URI, writer, or object destinations",
			"use .Copy() from a packet-domain stream point for packet-preserving output",
			"send frame-domain media to goav.Sink(...) instead of a byte destination",
		},
		Cause: ErrUnsupportedBuild,
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
		if next.Kind == info.OpTap || next.Kind == info.OpShape {
			shape = operationSpecOutputShape(shape, next)
			continue
		}
		expected := next.InputShapes()
		if len(expected) != 0 && !expected.Accepts(shape) {
			return operationShapeMismatchError(operation, node, i, next, expected, shape)
		}
		shape = operationSpecOutputShape(shape, next)
	}
	return nil
}

func operationSpecOutputShape(input shape.Spec, operation OperationSpec) shape.Spec {
	out := operation.OutputShapes(input)
	if len(out) == 0 {
		return input
	}
	return out[0]
}

func operationShapeMismatchError(operation string, node string, index int, step OperationSpec, expected shape.Set, actual shape.Spec) error {
	component := firstNonEmpty(step.Component, operationSpecComponent(step), string(step.Kind), "operation")
	return &BuildError{
		Code:      "operation_shape_mismatch",
		Operation: operation,
		Node:      node,
		Reason:    component + " cannot consume the current media shape",
		Details: []string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=" + string(step.Kind),
			"expected_shape=" + shapeSetString(expected),
			"actual_shape=" + actual.String(),
		},
		Suggestions: operationShapeMismatchSuggestions(step),
		Cause:       ErrUnsupportedBuild,
	}
}

func operationSpecComponent(operation OperationSpec) string {
	switch operation.Kind {
	case info.OpDecode:
		return firstNonEmpty(string(operation.Decode.ID), operation.Component, "decode")
	case info.OpTransform:
		return firstNonEmpty(transformFactoryName(operation.Transform), "transform")
	case info.OpEncode:
		return firstNonEmpty(string(operation.Encode.ID), operation.Component, "encode")
	case info.OpCopy:
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

func operationShapeMismatchSuggestions(operation OperationSpec) []string {
	switch operation.Kind {
	case info.OpDecode:
		return []string{
			"decode only consumes packet-domain media",
			"remove duplicate .Decode() calls after a frame tap",
			"start from goav.PacketTap(name) when a runtime branch should decode",
		}
	case info.OpTransform:
		return []string{
			"call .Decode() before frame transforms when starting from packets",
			"use .Video().Resize(...) for video frames",
			"use .Audio().Resample(...) for audio frames",
		}
	case info.OpEncode:
		return []string{
			"call .Decode() before encoding when starting from packets",
			"keep .Shape(...) annotations in the frame domain before encoders",
			"use .Copy() instead of an encoder for packet-preserving fanout",
		}
	case info.OpCopy:
		return []string{
			"copy only consumes packet-domain media",
			"move .Copy() before decode or start from goav.PacketTap(name)",
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
		if branchComposePlanReady(state.plan) {
			fresh, err := planBranchCompositionRecipe(state.intent, state.branchInputAttachment, state.branchDestinationAttachments, nil)
			if err != nil {
				return err
			}
			state.plan.Input = fresh.Input
			state.plan.Destinations = fresh.Destinations
			return nil
		}
		plan, err := planBranchCompositionRecipe(state.intent, state.branchInputAttachment, state.branchDestinationAttachments, nil)
		if err != nil {
			return err
		}
		state.plan = plan
		return nil
	}}
}

func recipeGraphUnsupportedError(operation string, intent Intent) error {
	details := []string{
		fmt.Sprintf("recipe: %s", firstNonEmpty(intent.Name, "unnamed")),
		fmt.Sprintf("inputs: %d", len(intent.Inputs)),
		fmt.Sprintf("streams: %d", len(intent.Streams)),
		fmt.Sprintf("destinations: %d", len(intent.Destinations)),
	}
	return &BuildError{
		Code:      "recipe_graph_unsupported",
		Operation: operation,
		Reason:    "recipe intent did not match a supported graph plan",
		Details:   details,
		Suggestions: []string{
			"use goav.From(input).Copy().To(output...) for packet-preserving record or remux",
			"use goav.From(input).Audio().To(goav.Sink(...)) or .Video().To(...) for decoded frames",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).Encode(codec.VP9(...)).To(output)) for named branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func requireGraphPlanSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "require graph plan spec", fn: func(state *recipeCompileState) error {
		if state.specReady && state.specOrigin == graphSpecOriginGraphPlan && state.graphPlan.ready() {
			return nil
		}
		return recipeGraphUnsupportedError(state.operation, state.intent)
	}}
}
