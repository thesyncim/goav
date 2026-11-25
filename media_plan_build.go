package goav

import (
	"context"

	"github.com/thesyncim/goav/pipeline"
)

func buildGraphPlanTask(ctx context.Context, gp graphPlan) (Task, error) {
	runtime := runtimeForGraphExecution(gp.runtime)
	// One shared timeline, QoS reporter, and control seam per task — kept on
	// the per-build builder and finished task so the shared Runtime remains
	// strictly immutable across concurrent builds and runs.
	timeline := newTimeline(runtime.clock)
	qos := newQoSReporter()
	controls := &taskControls{}
	if err := validateGraphPlanLowering(gp); err != nil {
		return nil, err
	}
	config, err := gp.lowerer.graphConfig(gp)
	if err != nil {
		return nil, err
	}
	service := &builder{
		runtime:      runtime,
		clock:        timeline,
		realtime:     config.Realtime,
		buffer:       config.Buffer,
		bufferSet:    true,
		qos:          qos,
		controls:     controls,
		requireRunOK: true,
	}
	graph, err := pipeline.NewGraph(config)
	if err != nil {
		return nil, err
	}
	if err := gp.lower(ctx, graph, service); err != nil {
		graph.Close()
		return nil, err
	}
	task := newTaskWithRootDestinations(graph, runtime, gp.work.Destinations, service.destinationTxs...)
	task.realtime = config.Realtime
	task.buffer = config.Buffer
	task.bufferSet = true
	task.qos = qos
	task.controls = controls
	task.isolated, err = task.installIsolation(gp.lowerer.isolation, service.destinationTxs, gp.work.Destinations, nil)
	if err != nil {
		_ = task.Close()
		return nil, err
	}
	task.encoders = service.encoders
	task.timeline = timeline
	qos.attach(task)
	controls.attach(task)
	task.startQoSPolicy(runtime.qosPolicy)
	return task, nil
}

func runtimeForGraphExecution(rt *Runtime) *Runtime {
	if rt != nil {
		return rt
	}
	return mustNew()
}
