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

	inputs        []InputSpec
	jobOutputs    []OutputSpec
	streamOutputs []OutputSpec
	streamSteps   []jobStreamStepAttachment

	transcodeInput   InputSpec
	transcodeOutputs []namedOutputSpec

	outputs []OutputSpec
	plan    transcodepkg.Plan

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
		state.inputs = append([]InputSpec(nil), job.inputs...)
		state.jobOutputs = append([]OutputSpec(nil), job.outputs...)
		state.streamOutputs = jobStreamOutputs(job.stream)
		state.streamSteps = jobStreamStepAttachments(job.stream)
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateJobIntentPass(),
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
		state.transcodeInput = job.input
		state.transcodeOutputs = append([]namedOutputSpec(nil), job.outputs...)
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		planTranscodeIntentPass(),
		openRecipeRuntimeBuilderPass(),
		lowerTranscodePlanPass(),
		selectMigrationGraphCompilerPass(),
		emitMigrationGraphSpecPass(),
	}}.Compile(state)
}

func validateJobIntentPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job intent", fn: func(state *recipeCompileState) error {
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
		if len(state.inputs) == 0 {
			return &BuildError{Code: "input_missing", Operation: state.operation, Reason: "no input is configured", Cause: ErrUnsupportedBuild}
		}
		if err := validateJobInputs(state.inputs); err != nil {
			return err
		}
		stream, hasStream := jobIntentStream(state.intent)
		if err := validateJobOutputScope(state.jobOutputs, stream, hasStream); err != nil {
			return err
		}
		state.outputs = jobAllOutputs(state.jobOutputs, state.streamOutputs)
		if len(state.outputs) == 0 {
			return &BuildError{Code: "output_missing", Operation: state.operation, Reason: "no output is configured", Cause: ErrUnsupportedBuild}
		}
		return validateOutputSpecs(state.operation, state.outputs)
	}}
}

func validatePacketJobOutputsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate packet job outputs", fn: func(state *recipeCompileState) error {
		if !state.jobPresent || jobIntentHasStream(state.intent) {
			return nil
		}
		for i := range state.outputs {
			if state.outputs[i].sink == nil {
				continue
			}
			return &BuildError{
				Code:      "output_kind_invalid",
				Operation: state.operation,
				Node:      state.outputs[i].label(fmt.Sprintf("output-%d", i)),
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
		for i := range state.inputs {
			state.builder = state.inputs[i].apply(state.builder)
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
		builder, err := applyJobStream(state.builder, state.outputs, stream, state.streamSteps)
		if err != nil {
			return err
		}
		state.builder = builder
		return nil
	}}
}

func lowerJobOutputsPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower job outputs", fn: func(state *recipeCompileState) error {
		for i := range state.outputs {
			builder, err := state.outputs[i].apply(state.builder)
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
		plan, err := planTranscodeRecipe(state.intent, state.transcodeInput, state.transcodeOutputs, state.recipeErr)
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
