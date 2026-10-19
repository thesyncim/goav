package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
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

// compileRecipe compiles one recipe DAG against rt: it projects the DAG and
// its handles into the compile request, runs the compiler, and binds the
// result back to the handles the build opens.
func compileRecipe(g *recipeGraph, rt *Runtime, options recipeCompileOptions) (recipeResolved, error) {
	rt = g.compileRuntime(rt)
	handles := g.handles(rt)
	req := g.compileRequest(handles)
	req.Env, req.Preflight, req.Ctx = rt.compileEnv(), options.preflight, options.ctx
	result, err := compile.Compile(req)
	if err != nil {
		return recipeResolved{}, publicError(err)
	}
	resolved := recipeResolved{
		recipe:            result.Recipe,
		runtime:           rt,
		inputAttachments:  handles.inputs,
		outputAttachments: handles.outputs(result.OutputSlots),
		inputProbes:       result.InputProbes,
		outputFormats:     result.OutputFormats,
		streamRules:       g.handles(rt).streamRules(result.StreamRules),
		name:              result.Name,
		rulesSource:       result.RulesSource,
		rulesDomain:       result.RulesDomain,
	}
	if result.Lowered {
		resolved.graphPlan = newGraphPlan(rt, result.Spec, result.Work, &recipeLowerer{
			runtime:   rt,
			recipe:    result.Recipe,
			joins:     result.Joins,
			joinOrder: result.JoinOrder,
			graph:     result.Graph,
			inputs:    resolved.inputAttachments,
			outputs:   resolved.outputAttachments,
		})
	}
	return resolved, nil
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

func destinationSpecFormat(output destinationSpec) av.FormatID {
	if output.resolvedFormat != "" {
		return output.resolvedFormat
	}
	return output.format
}

func (r recipeResolved) Describe() (pipeline.Spec, error) {
	if !r.graphPlan.ready() {
		return pipeline.Spec{}, publicError(compile.RecipeGraphUnsupportedRecipeError("describe recipe", r.recipe))
	}
	return r.graphPlan.Describe()
}

func (r recipeResolved) Build(ctx context.Context) (LiveTask, error) {
	if !r.graphPlan.ready() {
		return nil, publicError(compile.RecipeGraphUnsupportedRecipeError("build recipe", r.recipe))
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
func (r recipeResolved) workIR() compile.WorkPlan {
	if r.graphPlan.ready() {
		return r.graphPlan.workPlan()
	}
	return compile.WorkPlan{}
}

func installTaskTaps(mediaTask Task, taps []compile.WorkTap) {
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

func tapInfosFromPlan(taps []compile.WorkTap) []snapshot.Tap {
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

// --- attachments ---

// --- adapters ---

// --- runtime ---
