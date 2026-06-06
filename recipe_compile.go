package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	transcodepkg "github.com/thesyncim/goav/transcode"
)

type recipeResolved struct {
	intent                   Intent
	runtime                  Runtime
	builder                  builderAPI
	spec                     pipeline.Spec
	specReady                bool
	specOrigin               string
	mediaBuildKind           string
	inputAttachments         []InputSpec
	outputAttachments        []EndpointSpec
	inputProbes              []format.ProbeResult
	transcodeInputProbe      format.ProbeResult
	transcodeInputProbeReady bool
	outputFormats            map[string]av.FormatID
	mediaPlan                mediaPlan
	plan                     transcodepkg.Plan
}

type recipeCompileState struct {
	operation string
	intent    Intent
	runtime   Runtime
	options   recipeCompileOptions

	jobPresent       bool
	transcodePresent bool
	recipeErr        error

	inputAttachments  []InputSpec
	jobOutputCount    int
	streamSteps       []jobStreamStepAttachment
	outputAttachments []EndpointSpec
	inputProbes       []format.ProbeResult

	transcodeInputAttachment   InputSpec
	transcodeTargetAttachments []namedTargetSpec
	transcodeInputProbe        format.ProbeResult
	transcodeInputProbeReady   bool
	transcodeBranchSplit       bool

	plan transcodepkg.Plan

	builder        builderAPI
	spec           pipeline.Spec
	specReady      bool
	specOrigin     string
	mediaBuildKind string
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
		formatID := endpointSpecFormat(s.outputAttachments[i])
		if formatID == "" {
			continue
		}
		formats[s.outputAttachments[i].label(fmt.Sprintf("output-%d", i))] = formatID
	}
	for i := range s.transcodeTargetAttachments {
		formatID := endpointSpecFormat(s.transcodeTargetAttachments[i].output)
		if formatID == "" {
			continue
		}
		label := firstNonEmpty(s.transcodeTargetAttachments[i].name, s.transcodeTargetAttachments[i].output.label(fmt.Sprintf("output-%d", i)))
		formats[label] = formatID
	}
	if len(formats) == 0 {
		return nil
	}
	return formats
}

func endpointSpecFormat(output EndpointSpec) av.FormatID {
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
			return recipeResolved{}, err
		}
	}
	if state.builder == nil {
		return recipeResolved{}, &BuildError{
			Code:      "runtime_builder_missing",
			Operation: state.operation,
			Reason:    "recipe compiler produced no runtime builder",
			Suggestions: []string{
				"use goav.Default() for the standard recipe runtime",
				"use goav.New(...) when customizing adapters",
				"use runtime.Graph() for explicit graph wiring",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return recipeResolved{
		intent:                   state.intent,
		runtime:                  state.runtime,
		builder:                  state.builder,
		spec:                     state.spec,
		specReady:                state.specReady,
		specOrigin:               state.specOrigin,
		mediaBuildKind:           state.mediaBuildKind,
		inputAttachments:         append([]InputSpec(nil), state.inputAttachments...),
		outputAttachments:        append([]EndpointSpec(nil), state.outputAttachments...),
		inputProbes:              append([]format.ProbeResult(nil), state.inputProbes...),
		transcodeInputProbe:      state.transcodeInputProbe,
		transcodeInputProbeReady: state.transcodeInputProbeReady,
		outputFormats:            state.outputFormatMap(),
		mediaPlan:                buildMediaPlan(&state),
		plan:                     state.plan,
	}, nil
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if r.specReady {
		return r.spec, nil
	}
	return r.builder.Describe()
}

func (r recipeResolved) Build(ctx context.Context) (Task, error) {
	var (
		task Task
		err  error
	)
	switch r.mediaBuildKind {
	case mediaBuildKindPacketCopy:
		task, err = r.buildMediaPlanPacketCopyTask(ctx)
	case mediaBuildKindFrameSink:
		task, err = r.buildMediaPlanFrameSinkTask(ctx)
	case mediaBuildKindEncode:
		task, err = r.buildMediaPlanEncodeTask(ctx)
	case mediaBuildKindBranch:
		task, err = r.buildMediaPlanBranchComposerTask(ctx)
	default:
		err = recipeGraphUnsupportedError("build recipe", r.intent)
	}
	if err != nil {
		return nil, err
	}
	installTaskTaps(task, r.mediaPlan.Taps)
	return task, nil
}

func installTaskTaps(mediaTask Task, taps []planTap) {
	if len(taps) == 0 {
		return
	}
	runtimeTask, ok := mediaTask.(*task)
	if !ok || runtimeTask == nil {
		return
	}
	runtimeTask.taps = tapInfosFromPlan(taps)
}

func tapInfosFromPlan(taps []planTap) []TapInfo {
	out := make([]TapInfo, 0, len(taps))
	seen := make(map[string]struct{}, len(taps))
	for i := range taps {
		if taps[i].Name == "" {
			continue
		}
		if _, ok := seen[taps[i].Name]; ok {
			continue
		}
		seen[taps[i].Name] = struct{}{}
		out = append(out, TapInfo{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Caps:      taps[i].Caps,
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
	if job != nil && len(job.branchStreams) != 0 {
		return compileJobBranchRecipeWithOptions(job, options)
	}
	state := recipeCompileState{
		operation: "build job",
		options:   options,
	}
	if job != nil {
		state.jobPresent = true
		state.intent = job.Intent()
		state.runtime = job.runtime
		state.recipeErr = job.err
		state.inputAttachments = append([]InputSpec(nil), job.inputs...)
		state.jobOutputCount = len(job.outputs)
		state.outputAttachments = jobAllOutputs(job.outputs, jobStreamOutputs(job.stream))
		state.streamSteps = jobStreamStepAttachments(job.stream)
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateJobRecipePass(),
		validateJobIntentShapePass(),
		validateRecipeAttachmentConsistencyPass(),
		validateJobAttachmentsPass(),
		validateJobStreamAttachmentsPass(),
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
		validateJobKnownInputDecodeAdaptersPass(),
		openRecipeRuntimeBuilderPass(),
		validateJobStreamRuntimeCapabilitiesPass(),
		lowerJobInputsPass(),
		lowerJobStreamPass(),
		lowerJobOutputsPass(),
		emitMediaPlanGraphSpecPass(),
		validateMuxCompatibilityPass(),
		requireMediaPlanGraphSpecPass(),
	}}.Compile(state)
}

func compileJobBranchRecipeWithOptions(job *Job, options recipeCompileOptions) (recipeResolved, error) {
	branchJob := &transcodeJob{
		runtime:         job.runtime,
		name:            job.name,
		streams:         append([]streamBuild(nil), job.branchStreams...),
		outputs:         append([]namedTargetSpec(nil), job.branchTargets...),
		err:             job.err,
		fromBranchSplit: true,
	}
	if len(job.inputs) == 1 {
		branchJob.input = job.inputs[0]
	} else if branchJob.err == nil {
		branchJob.err = branchInputCountError("branches", len(job.inputs))
	}
	return compileTranscodeRecipeWithOptions(branchJob, options)
}

func compileTranscodeRecipeWithOptions(job *transcodeJob, options recipeCompileOptions) (recipeResolved, error) {
	state := recipeCompileState{
		operation: transcodeRecipeOperation,
		options:   options,
	}
	if job != nil {
		state.transcodePresent = true
		state.intent = job.Intent()
		state.runtime = job.runtime
		state.recipeErr = job.err
		state.transcodeInputAttachment = job.input
		state.transcodeTargetAttachments = append([]namedTargetSpec(nil), job.outputs...)
		state.transcodeBranchSplit = job.fromBranchSplit
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateTranscodeRecipePass(),
		validateTranscodeIntentShapePass(),
		validateRecipeAttachmentConsistencyPass(),
		validateTranscodeAttachmentsPass(),
		validateTranscodeOutputBindingsPass(),
		validateTranscodeOutputFormatAdaptersPass(),
		validateTranscodeEncodeAdaptersPass(),
		validateTranscodeTransformAdaptersPass(),
		validateTranscodeInputFormatAdaptersPass(),
		validateTranscodeKnownInputStreamSelectionPass(),
		validateTranscodeKnownInputDecodeAdaptersPass(),
		planTranscodeIntentPass(),
		openRecipeRuntimeBuilderPass(),
		lowerTranscodePlanPass(),
		emitMediaPlanGraphSpecPass(),
		validateMuxCompatibilityPass(),
		requireMediaPlanGraphSpecPass(),
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
	if len(intent.Streams) > 1 {
		return jobIntentTooManyStreamsError(operation, intent.Streams)
	}
	if len(intent.Outputs) == 0 {
		return &BuildError{Code: "output_missing", Operation: operation, Reason: "no output is configured", Cause: ErrUnsupportedBuild}
	}
	if err := validateJobIntentOutputScope(operation, intent, jobOutputCount, stream, hasStream); err != nil {
		return err
	}
	if !hasStream {
		return nil
	}
	return validateJobStreamIntentShape(operation, stream, nil)
}

func validateJobIntentOutputScope(operation string, intent Intent, jobOutputCount int, stream StreamIntent, hasStream bool) error {
	if !hasStream {
		return nil
	}
	if jobOutputCount == 0 && len(intent.Outputs) == len(stream.RouteTo) {
		return nil
	}
	return jobOutputScopeMixedError(operation, stream)
}

func jobOutputScopeMixedError(operation string, stream StreamIntent) error {
	return &BuildError{
		Code:      "output_scope_mixed",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes use stream-local outputs",
		Suggestions: []string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).VP9(...).To(goav.Target(\"web\", output))) for named branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobOutputReferenceMissingError(operation string, stream StreamIntent, label string) error {
	return &BuildError{
		Code:      "output_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream route output " + label + " is not attached",
		Suggestions: []string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.From(input).Copy().To(output...) for packet-preserving record/remux",
			"finish each branch with a typed target such as .To(goav.Target(\"web\", output))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobIntentTooManyStreamsError(operation string, streams []StreamIntent) error {
	err := &BuildError{
		Code:      "stream_duplicate",
		Operation: operation,
		Reason:    "ordinary stream recipes select one audio or video stream",
		Suggestions: []string{
			"keep one .Audio(...) or .Video(...) chain on goav.From(...)",
			"use goav.From(input).Video().Decode().Branches(...) for multiple branches from one stream",
			"use the expert graph API for custom multi-stream routing",
		},
		Cause: ErrUnsupportedBuild,
	}
	if len(streams) > 0 {
		err.Details = append(err.Details, "first stream: "+jobStreamIntentName(streams[0]))
	}
	if len(streams) > 1 {
		err.Node = jobStreamIntentName(streams[1])
		err.Details = append(err.Details, "second stream: "+jobStreamIntentName(streams[1]))
	}
	return err
}

func validateJobStreamIntentShape(operation string, stream StreamIntent, steps []jobStreamStepAttachment) error {
	selector := streamIntentSelector(stream)
	node := jobStreamIntentName(stream)
	if !streamIntentHasOperation(stream, steps) {
		return streamOperationMissingError(operation, node)
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

func validateJobStreamTransformIntentShape(operation string, stream StreamIntent) error {
	selector := streamIntentSelector(stream)
	node := jobStreamIntentName(stream)
	for i := range stream.Transforms {
		transform := stream.Transforms[i]
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
				return transformMediaError(node, "resize", "video")
			}
		case transform.Resample != nil:
			if selector.Type == av.MediaVideo {
				return transformMediaError(node, "resample", "audio")
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

func streamOperationMissingError(operation string, node string) error {
	return &BuildError{
		Code:      "stream_operation_missing",
		Operation: operation,
		Node:      node,
		Reason:    "the stream was selected but no decode, processing stage, or encoder was requested",
		Suggestions: []string{
			"call .To(goav.FrameSink(...)) to receive decoded frames",
			"call .Opus(...), .VP8(...), or .VP9(...) before writing to a file output",
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
		return validateEndpointSpecs(state.operation, state.outputAttachments)
	}}
}

func validateJobOutputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job output format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightOutputAdapters {
			return nil
		}
		outputs, err := validateOutputFormatAdapters(state.options.Context(), state.runtime, state.outputAttachments)
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

func validateJobStreamAttachmentsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job stream attachments", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok {
			return nil
		}
		return validateJobStreamAttachments(state.operation, stream, state.streamSteps)
	}}
}

func validateJobStreamAttachments(operation string, stream StreamIntent, steps []jobStreamStepAttachment) error {
	for i := range steps {
		step := steps[i]
		if step.stage != nil {
			continue
		}
		if step.hasTransform {
			if step.transformIndex >= 0 && step.transformIndex < len(stream.Transforms) {
				continue
			}
			return jobStreamTransformAttachmentMismatchError(operation, stream, step, len(stream.Transforms))
		}
		if step.tap != "" {
			continue
		}
		return streamStageMissingError(stream)
	}
	return nil
}

func jobStreamTransformAttachmentMismatchError(operation string, stream StreamIntent, step jobStreamStepAttachment, transformCount int) error {
	return &BuildError{
		Code:      "recipe_attachment_mismatch",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream transform attachment does not match intent transforms",
		Details: []string{
			fmt.Sprintf("step index: %d", step.stepIndex),
			fmt.Sprintf("transform index: %d", step.transformIndex),
			fmt.Sprintf("intent transforms: %d", transformCount),
		},
		Suggestions: []string{
			"build stream recipes through goav.From(...).Audio() or goav.From(...).Video()",
			"keep custom compiler passes aligned with Intent.Transforms and captured stream attachments",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateJobOutputBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job output bindings", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok {
			return nil
		}
		return validateJobOutputBindings(state.operation, stream, state.outputAttachments)
	}}
}

func validateJobStreamOutputKindsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job stream output kinds", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok {
			return nil
		}
		return validateJobStreamOutputKinds(state.operation, stream, state.outputAttachments)
	}}
}

func validateTranscodeRecipePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode recipe", fn: func(state *recipeCompileState) error {
		if !state.transcodePresent {
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

func validateTranscodeIntentShapePass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode intent shape", fn: func(state *recipeCompileState) error {
		return validateTranscodeIntentShape(state.operation, state.intent)
	}}
}

func validateTranscodeAttachmentsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode attachments", fn: func(state *recipeCompileState) error {
		return validateTranscodeAttachments(state.transcodeInputAttachment, state.transcodeTargetAttachments, state.transcodeBranchSplit)
	}}
}

func validateTranscodeOutputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode output format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightOutputAdapters {
			return nil
		}
		outputs := make([]EndpointSpec, 0, len(state.transcodeTargetAttachments))
		for i := range state.transcodeTargetAttachments {
			output := state.transcodeTargetAttachments[i].output.Name(firstNonEmpty(
				state.transcodeTargetAttachments[i].output.name,
				state.transcodeTargetAttachments[i].name,
			))
			outputs = append(outputs, output)
		}
		resolved, err := validateOutputFormatAdapters(state.options.Context(), state.runtime, outputs)
		if err != nil {
			return err
		}
		for i := range state.transcodeTargetAttachments {
			state.transcodeTargetAttachments[i].output = resolved[i]
		}
		return nil
	}}
}

func validateTranscodeInputFormatAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode input format adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightInputAdapters {
			return nil
		}
		probes, err := validateInputFormatAdapters(state.options.Context(), state.runtime, []InputSpec{state.transcodeInputAttachment})
		if err != nil {
			return err
		}
		if len(probes) != 0 {
			state.transcodeInputProbe = probes[0]
			state.transcodeInputProbeReady = true
		}
		return nil
	}}
}

func validateTranscodeEncodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode encode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightEncodeAdapters {
			return nil
		}
		return validateRecipeEncodeAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateTranscodeTransformAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode transform adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightTransformAdapters {
			return nil
		}
		return validateRecipeTransformAdapters(state.operation, state.runtime, state.intent.Streams)
	}}
}

func validateTranscodeOutputBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode output bindings", fn: func(state *recipeCompileState) error {
		return validateTranscodeOutputBindings(state.intent, state.transcodeTargetAttachments)
	}}
}

func validateRecipeAttachmentConsistencyPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate recipe attachments", fn: func(state *recipeCompileState) error {
		switch {
		case state.jobPresent:
			if len(state.intent.Inputs) != len(state.inputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "inputs", len(state.intent.Inputs), len(state.inputAttachments))
			}
			if len(state.intent.Outputs) != len(state.outputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "outputs", len(state.intent.Outputs), len(state.outputAttachments))
			}
		case state.transcodePresent:
			if len(state.intent.Inputs) != 1 {
				return recipeAttachmentMismatchError(state.operation, "inputs", len(state.intent.Inputs), 1)
			}
			if len(state.intent.Outputs) != len(state.transcodeTargetAttachments) {
				return recipeAttachmentMismatchError(state.operation, "outputs", len(state.intent.Outputs), len(state.transcodeTargetAttachments))
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
		if !state.jobPresent || jobIntentHasStream(state.intent) {
			return nil
		}
		for i := range state.outputAttachments {
			if state.outputAttachments[i].sink == nil {
				continue
			}
			return &BuildError{
				Code:      "output_kind_invalid",
				Operation: state.operation,
				Node:      state.outputAttachments[i].label(fmt.Sprintf("output-%d", i)),
				Reason:    "packet-preserving recipes write to muxed outputs, not frame sinks",
				Suggestions: []string{
					"use goav.From(input).Stream().Decode().To(goav.FrameSink(sink)) for decoded frames when the input has one obvious stream",
					"use goav.From(input).Audio().Decode().To(goav.FrameSink(sink)) or .Video().Decode().To(...) when stream selection matters",
					"use goav.FileOutput(...) or goav.URIOutput(...) with .Copy().To(...) for packet-preserving output",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
		return nil
	}}
}

func validateJobLiveStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job live stream selection", fn: func(state *recipeCompileState) error {
		if !state.options.preflightLiveStreams {
			return nil
		}
		stream, ok := jobIntentStream(state.intent)
		if !ok || !streamNeedsDecode(stream) {
			return nil
		}
		return validateLiveStreamSelection(state.intent.Inputs, stream)
	}}
}

func validateJobKnownInputStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job known input stream selection", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok || !streamNeedsDecode(stream) {
			return nil
		}
		return validateKnownInputStreamSelection(state.inputProbes, stream)
	}}
}

func validateJobKnownInputDecodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job known input decode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightDecodeAdapters {
			return nil
		}
		stream, ok := jobIntentStream(state.intent)
		if !ok || !streamNeedsDecode(stream) {
			return nil
		}
		return validateKnownRecipeDecodeAdapters(state.operation, state.runtime, state.inputProbes, []StreamIntent{stream})
	}}
}

func validateTranscodeKnownInputStreamSelectionPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode known input stream selection", fn: func(state *recipeCompileState) error {
		if !state.transcodeInputProbeReady || len(state.transcodeInputProbe.Streams) == 0 {
			return nil
		}
		for i := range state.intent.Streams {
			if err := validateKnownProbeStreamSelection(state.transcodeInputProbe, state.intent.Streams[i]); err != nil {
				return err
			}
		}
		return nil
	}}
}

func validateTranscodeKnownInputDecodeAdaptersPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode known input decode adapters", fn: func(state *recipeCompileState) error {
		if !state.options.preflightDecodeAdapters || !state.transcodeInputProbeReady || len(state.transcodeInputProbe.Streams) == 0 {
			return nil
		}
		return validateKnownRecipeDecodeAdapters(state.operation, state.runtime, []format.ProbeResult{state.transcodeInputProbe}, state.intent.Streams)
	}}
}

func validateKnownInputStreamSelection(probes []format.ProbeResult, stream StreamIntent) error {
	for i := range probes {
		if err := validateKnownProbeStreamSelection(probes[i], stream); err != nil {
			return err
		}
	}
	return nil
}

func validateKnownProbeStreamSelection(probe format.ProbeResult, stream StreamIntent) error {
	if len(probe.Streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(probe.Streams, streamIntentSelector(stream))
	return err
}

func openRecipeRuntimeBuilderPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "open runtime builder", fn: func(state *recipeCompileState) error {
		builder, err := newRuntimeBuilder(state.runtime, state.operation)
		if err != nil {
			return err
		}
		state.builder = builder
		return nil
	}}
}

func validateJobStreamRuntimeCapabilitiesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job stream runtime capabilities", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok {
			return nil
		}
		return validateJobStreamRuntimeCapabilities(state.operation, state.builder, stream)
	}}
}

func lowerJobInputsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower job inputs", fn: func(state *recipeCompileState) error {
		for i := range state.inputAttachments {
			state.builder = state.inputAttachments[i].apply(state.builder)
		}
		return nil
	}}
}

func lowerJobStreamPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower job stream", fn: func(state *recipeCompileState) error {
		stream, ok := jobIntentStream(state.intent)
		if !ok {
			return nil
		}
		builder, err := applyJobStream(state.builder, state.outputAttachments, stream, state.streamSteps)
		if err != nil {
			return err
		}
		state.builder = builder
		return nil
	}}
}

func lowerJobOutputsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower job outputs", fn: func(state *recipeCompileState) error {
		for i := range state.outputAttachments {
			builder, err := state.outputAttachments[i].apply(state.builder)
			if err != nil {
				return err
			}
			state.builder = builder
		}
		return nil
	}}
}

func planTranscodeIntentPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "plan transcode intent", fn: func(state *recipeCompileState) error {
		plan, err := planTranscodeRecipe(state.intent, state.transcodeInputAttachment, state.transcodeTargetAttachments)
		if err != nil {
			return err
		}
		state.plan = plan
		return nil
	}}
}

func lowerTranscodePlanPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower transcode plan", fn: func(state *recipeCompileState) error {
		if state.transcodeBranchSplit && state.transcodeInputAttachment.rtp != nil {
			state.builder = state.transcodeInputAttachment.apply(state.builder)
		}
		state.builder = state.builder.Transcode(state.plan)
		return nil
	}}
}

func recipeGraphUnsupportedError(operation string, intent Intent) error {
	details := []string{
		fmt.Sprintf("recipe: %s", firstNonEmpty(intent.Name, "unnamed")),
		fmt.Sprintf("inputs: %d", len(intent.Inputs)),
		fmt.Sprintf("streams: %d", len(intent.Streams)),
		fmt.Sprintf("outputs: %d", len(intent.Outputs)),
	}
	return &BuildError{
		Code:      "recipe_graph_unsupported",
		Operation: operation,
		Reason:    "recipe intent did not match a supported media-plan graph",
		Details:   details,
		Suggestions: []string{
			"use goav.From(input).Copy().To(output...) for packet-preserving record or remux",
			"use goav.From(input).Audio().To(goav.FrameSink(...)) or .Video().To(...) for decoded frames",
			"use goav.From(input).Video().Decode().Branches(goav.Branch(name).VP9(...).To(goav.Target(\"web\", output))) for named branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func requireMediaPlanGraphSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "require media plan graph spec", fn: func(state *recipeCompileState) error {
		if state.specReady && state.specOrigin == graphSpecOriginMediaPlan && state.mediaBuildKind != "" {
			return nil
		}
		return recipeGraphUnsupportedError(state.operation, state.intent)
	}}
}
