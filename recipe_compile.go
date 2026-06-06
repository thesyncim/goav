package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/pipeline"
	transcodepkg "github.com/thesyncim/goav/transcode"
)

type recipeResolved struct {
	intent    Intent
	builder   builderAPI
	migration *builder
	compiler  builderCompiler
	spec      pipeline.Spec
	specReady bool
}

type recipeCompileState struct {
	operation string
	intent    Intent
	runtime   Runtime

	jobPresent       bool
	transcodePresent bool
	recipeErr        error

	inputAttachments  []InputSpec
	jobOutputCount    int
	streamSteps       []jobStreamStepAttachment
	outputAttachments []OutputSpec

	transcodeInputAttachment   InputSpec
	transcodeOutputAttachments []namedOutputSpec

	plan transcodepkg.Plan

	builder   builderAPI
	migration *builder
	compiler  builderCompiler
	spec      pipeline.Spec
	specReady bool
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
		intent:    state.intent,
		builder:   state.builder,
		migration: state.migration,
		compiler:  state.compiler,
		spec:      state.spec,
		specReady: state.specReady,
	}, nil
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if r.specReady {
		return r.spec, nil
	}
	return r.builder.Describe()
}

func (r recipeResolved) Build(ctx context.Context) (Task, error) {
	if r.compiler != nil && r.migration != nil {
		return r.compiler.build(ctx, r.migration)
	}
	return r.builder.Build(ctx)
}

func compileJobRecipe(job *Job) (recipeResolved, error) {
	state := recipeCompileState{
		operation: "build job",
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
		validatePacketJobOutputsPass(),
		openRecipeRuntimeBuilderPass(),
		lowerJobInputsPass(),
		lowerJobStreamPass(),
		lowerJobOutputsPass(),
		selectMigrationGraphCompilerPass(),
		emitMigrationGraphSpecPass(),
	}}.Compile(state)
}

func compileTranscodeRecipe(job *TranscodeJob) (recipeResolved, error) {
	state := recipeCompileState{
		operation: transcodeRecipeOperation,
	}
	if job != nil {
		state.transcodePresent = true
		state.intent = job.Intent()
		state.runtime = job.runtime
		state.recipeErr = job.err
		state.transcodeInputAttachment = job.input
		state.transcodeOutputAttachments = append([]namedOutputSpec(nil), job.outputs...)
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateTranscodeRecipePass(),
		validateTranscodeIntentShapePass(),
		validateRecipeAttachmentConsistencyPass(),
		validateTranscodeAttachmentsPass(),
		validateTranscodeOutputBindingsPass(),
		planTranscodeIntentPass(),
		openRecipeRuntimeBuilderPass(),
		lowerTranscodePlanPass(),
		selectMigrationGraphCompilerPass(),
		emitMigrationGraphSpecPass(),
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
			"use goav.Record(input, output) or goav.From(input).To(output...) for packet-preserving record/remux",
			"use goav.Transcode(input) when one input needs separate record, preview, or ladder branches",
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
			"use goav.Transcode(input) for multiple audio or video branches",
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
	return validateCodecChangePolicy(operation, node, stream.CodecChange)
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
			"use goav.Record(input, output) for packet-preserving record or remux",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateJobAttachmentsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job attachments", fn: func(state *recipeCompileState) error {
		if err := validateJobInputs(state.inputAttachments); err != nil {
			return err
		}
		return validateOutputSpecs(state.operation, state.outputAttachments)
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
		return validateTranscodeAttachments(state.transcodeInputAttachment, state.transcodeOutputAttachments)
	}}
}

func validateTranscodeOutputBindingsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate transcode output bindings", fn: func(state *recipeCompileState) error {
		return validateTranscodeOutputBindings(state.intent, state.transcodeOutputAttachments)
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
			if len(state.intent.Outputs) != len(state.transcodeOutputAttachments) {
				return recipeAttachmentMismatchError(state.operation, "outputs", len(state.intent.Outputs), len(state.transcodeOutputAttachments))
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
			"build recipes through goav.Record, goav.From, goav.Decode, or goav.Transcode",
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
					"use goav.Decode(input, goav.FrameSink(sink)) for decoded frames when the input has one obvious stream",
					"use goav.From(input).Audio().To(goav.FrameSink(sink)) or .Video().To(...) when stream selection matters",
					"use goav.FileOutput(...) or goav.URIOutput(...) with goav.Record(...) for packet-preserving output",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
		return nil
	}}
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
		plan, err := planTranscodeRecipe(state.intent, state.transcodeInputAttachment, state.transcodeOutputAttachments)
		if err != nil {
			return err
		}
		state.plan = plan
		return nil
	}}
}

func lowerTranscodePlanPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower transcode plan", fn: func(state *recipeCompileState) error {
		state.builder = state.builder.Transcode(state.plan)
		return nil
	}}
}

func selectMigrationGraphCompilerPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "select migration graph compiler", fn: func(state *recipeCompileState) error {
		builder, ok := state.builder.(*builder)
		if !ok {
			return nil
		}
		compiler, err := builder.selectCompiler()
		if err != nil {
			return err
		}
		state.migration = builder
		state.compiler = compiler
		return nil
	}}
}

func emitMigrationGraphSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit migration graph spec", fn: func(state *recipeCompileState) error {
		if state.migration == nil || state.compiler == nil {
			return nil
		}
		spec, err := state.migration.describeWithCompiler(state.compiler)
		if err != nil {
			return err
		}
		state.spec = spec
		state.specReady = true
		return nil
	}}
}
