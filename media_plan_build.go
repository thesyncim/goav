package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func buildGraphPlanTask(ctx context.Context, gp graphPlan) (LiveTask, error) {
	runtime := runtimeForGraphExecution(gp.runtime)
	// One shared timeline per task: the runtime clone carries it as the clock
	// every lowering step threads to paced sources, so a task-wide rate change
	// re-anchors all of them together.
	timeline := newTimeline(runtime.clock)
	runtime = runtimeWithClock(runtime, timeline)
	// The QoS reporter rides the same clone: lowering binds it into playout
	// and sync gates next to the timeline, and the finished task attaches
	// itself below so gate reports reach its Watch surface.
	runtime.qos = newQoSReporter()
	gp = graphPlanWithRuntime(gp, runtime)
	if err := validateGraphPlanLowering(gp); err != nil {
		return nil, err
	}
	service := &builder{runtime: runtime, requireRunOK: true}
	config, err := gp.lowerer.graphConfig(gp)
	if err != nil {
		return nil, err
	}
	runtime = runtimeWithBuffer(runtime, config.Buffer)
	gp = graphPlanWithRuntime(gp, runtime)
	service.runtime = runtime
	graph, err := pipeline.NewGraph(config)
	if err != nil {
		return nil, err
	}
	if err := gp.lower(ctx, graph, service); err != nil {
		graph.Close()
		return nil, err
	}
	task := newTaskWithRootDestinations(graph, runtime, gp.work.Destinations, service.destinationTxs...)
	task.encoders = service.encoders
	task.timeline = timeline
	runtime.qos.attach(task)
	task.startQoSPolicy(runtime.qosPolicy)
	return task, nil
}

func runtimeForGraphExecution(rt *Runtime) *Runtime {
	if rt != nil {
		return rt
	}
	return mustNew()
}

func runtimeRealtime(rt *Runtime) bool {
	if rt == nil {
		return true
	}
	return rt.realtime
}

// runtimeWithRealtime is a view of rt with realtime pacing set, sharing its
// registries and pools.
func runtimeWithRealtime(rt *Runtime, realtime bool) *Runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.realtime = realtime
	return &clone
}

func runtimeWithBuffer(rt *Runtime, buffer pipeline.BufferPolicy) *Runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.buffer = buffer
	return &clone
}

// runtimeWithClock clones the runtime with the task's timeline as its clock,
// so every builder a lowering step creates hands paced sources the shared
// timeline instead of the raw runtime clock.
func runtimeWithClock(rt *Runtime, clock av.Clock) *Runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.clock = clock
	return &clone
}

func joinPlanWithRuntime(plan *joinPlan, rt *Runtime) *joinPlan {
	if plan == nil {
		return nil
	}
	clone := *plan
	clone.runtime = rt
	if len(plan.arms) != 0 {
		clone.arms = append([]joinArmPlan(nil), plan.arms...)
		for i := range clone.arms {
			clone.arms[i].sub = joinPlanWithRuntime(plan.arms[i].sub, rt)
		}
	}
	return &clone
}

// mediaShapeFromMediaTransform is the frame shape a lowered transform
// produces.
func mediaShapeFromMediaTransform(transform mediaTransform) shape.Spec {
	switch {
	case transform.video != nil:
		return mediaShapeFromRecipeIRTransform(recipeir.Transform{Kind: recipeir.TransformResize, Resize: *transform.video})
	case transform.audio != nil:
		return mediaShapeFromRecipeIRTransform(recipeir.Transform{Kind: recipeir.TransformResample, Resample: *transform.audio})
	default:
		return shape.Spec{}
	}
}
