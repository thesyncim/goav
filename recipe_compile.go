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
}

type recipeCompileState struct {
	operation string
	intent    Intent
	runtime   Runtime

	job       *Job
	transcode *TranscodeJob

	outputs []OutputSpec
	plan    transcodepkg.Plan

	builder   builderAPI
	migration *builder
	compiler  builderCompiler
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
	}, nil
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if r.compiler != nil && r.migration != nil {
		return r.compiler.describe(r.migration, pipeline.Spec{
			Name:     "goav",
			Realtime: r.migration.runtime.realtime,
		})
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
		job:       job,
	}
	if job != nil {
		state.intent = job.Intent()
		state.runtime = job.runtime
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		validateJobIntentPass(),
		openRecipeRuntimeBuilderPass(),
		lowerJobInputsPass(),
		lowerJobStreamPass(),
		lowerJobOutputsPass(),
		selectMigrationGraphCompilerPass(),
	}}.Compile(state)
}

func compileTranscodeRecipe(job *TranscodeJob) (recipeResolved, error) {
	state := recipeCompileState{
		operation: transcodeRecipeOperation,
		transcode: job,
	}
	if job != nil {
		state.intent = job.Intent()
		state.runtime = job.runtime
	}
	return recipeIntentCompiler{passes: []recipeCompilePass{
		planTranscodeIntentPass(),
		openRecipeRuntimeBuilderPass(),
		lowerTranscodePlanPass(),
		selectMigrationGraphCompilerPass(),
	}}.Compile(state)
}

func validateJobIntentPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate job intent", fn: func(state *recipeCompileState) error {
		job := state.job
		if job == nil {
			return &BuildError{
				Code:      "job_invalid",
				Operation: state.operation,
				Reason:    "nil job",
				Cause:     ErrUnsupportedBuild,
			}
		}
		if job.runtime == nil {
			return &BuildError{Code: "runtime_missing", Operation: state.operation, Reason: "no runtime is configured"}
		}
		if job.err != nil {
			return job.err
		}
		if len(job.inputs) == 0 {
			return &BuildError{Code: "input_missing", Operation: state.operation, Reason: "no input is configured"}
		}
		if err := job.validateInputs(); err != nil {
			return err
		}
		if err := job.validateOutputScope(); err != nil {
			return err
		}
		state.outputs = job.allOutputs()
		if len(state.outputs) == 0 {
			return &BuildError{Code: "output_missing", Operation: state.operation, Reason: "no output is configured"}
		}
		return validateOutputSpecs(state.operation, state.outputs)
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
		for i := range state.job.inputs {
			state.builder = state.job.inputs[i].apply(state.builder)
		}
		return nil
	}}
}

func lowerJobStreamPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "lower job stream", fn: func(state *recipeCompileState) error {
		if state.job.stream == nil {
			return nil
		}
		builder, err := state.job.applyStream(state.builder, state.job.stream)
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
		job := state.transcode
		if job == nil {
			return &BuildError{
				Code:      "job_invalid",
				Operation: state.operation,
				Reason:    "nil transcode job",
				Cause:     ErrUnsupportedBuild,
			}
		}
		if job.runtime == nil {
			return &BuildError{Code: "runtime_missing", Operation: state.operation, Reason: "no runtime is configured"}
		}
		plan, err := job.plan()
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
