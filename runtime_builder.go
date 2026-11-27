package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/stage"
	"github.com/thesyncim/goav/pipeline"
)

// New returns the explicit-graph builder behind expert.Graph(runtime). The
// recipe compiler lowers high-level jobs straight onto pipeline graphs; the
// builder only assembles caller-provided Source/Stage/Sink nodes and routes.
func (r *Runtime) newBuilder() *builder {
	return &builder{runtime: r}
}

type builder struct {
	runtime        *Runtime
	clock          av.Clock
	realtime       bool
	buffer         pipeline.BufferPolicy
	bufferSet      bool
	qos            *qosReporter
	controls       *taskControls
	destinationTxs []*destinationTransaction
	// encoders are the encode stages this build opened, so stream-addressed
	// encoder controls reach the encoder producing the stream.
	encoders     []*codec.EncoderStage
	requireRunOK bool
	sources      []pipeline.Source
	stages       []pipeline.Stage
	sinks        []pipeline.Sink
	routes       []pipeline.Route
}

func (b *builder) newGraph(_ context.Context) (pipeline.Graph, error) {
	return pipeline.NewGraph(pipeline.GraphConfig{
		Name:             "goav",
		Realtime:         b.effectiveRealtime(),
		Buffer:           b.effectiveBuffer(),
		EventCapacity:    b.runtime.eventCapacity,
		CloseWaitTimeout: b.runtime.closeWaitTimeout,
	})
}

// taskClock returns the clock the build hands to paced sources — on recipe
// builds the task's shared timeline, falling back to the runtime clock.
func (b *builder) taskClock() av.Clock {
	if b == nil {
		return nil
	}
	if b.clock != nil {
		return b.clock
	}
	if b.runtime == nil {
		return nil
	}
	return b.runtime.clock
}

func (b *builder) effectiveRealtime() bool {
	if b == nil {
		return false
	}
	return b.realtime || (b.runtime != nil && b.runtime.realtime)
}

func (b *builder) effectiveBuffer() pipeline.BufferPolicy {
	if b == nil {
		return pipeline.BufferPolicy{}
	}
	if b.bufferSet {
		return b.buffer
	}
	if b.runtime != nil {
		return b.runtime.buffer
	}
	return pipeline.BufferPolicy{}
}

func (b *builder) qosReportFunc() func(av.Event) {
	if b == nil || b.qos == nil {
		return nil
	}
	return b.qos.publish
}

func (b *builder) controlFunc() func(context.Context, control.Control) error {
	if b == nil || b.controls == nil {
		return nil
	}
	return b.controls.control
}

func (b *builder) Source(source pipeline.Source) *builder {
	b.sources = append(b.sources, source)
	return b
}

func (b *builder) Stage(stage pipeline.Stage) *builder {
	b.stages = append(b.stages, stage)
	return b
}

func (b *builder) Sink(sink pipeline.Sink) *builder {
	b.sinks = append(b.sinks, sink)
	return b
}

func (b *builder) Routes(routes ...pipeline.Route) *builder {
	b.routes = append(b.routes, routes...)
	return b
}

func (b *builder) Build(ctx context.Context) (*Task, error) {
	b.destinationTxs = nil
	b.requireRunOK = true
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if b.hasExplicitGraph() {
		if err := b.compileExplicitGraph(graph); err != nil {
			graph.Close()
			return nil, err
		}
	}
	task := newTask(graph, b.runtime, b.destinationTxs...)
	// Explicit-graph sources are caller-built and pace themselves, so the
	// task timeline has no paced consumers; it exists for uniform task shape.
	task.timeline = stage.NewTimeline(b.runtime.clock)
	// Explicit graphs lower no gates, but buffered MaxLatency shedding still
	// reports QoS on the graph event stream, so the policy applies here too.
	task.startQoSPolicy(b.runtime.qosPolicy)
	return task, nil
}

func (b *builder) hasExplicitGraph() bool {
	return len(b.sources) != 0 || len(b.stages) != 0 || len(b.sinks) != 0 ||
		len(b.routes) != 0
}

func (b *builder) compileExplicitGraph(graph pipeline.Graph) error {
	if len(b.sources) == 0 {
		return explicitGraphMissingSourceError()
	}

	sourceRefs := make([]pipeline.NodeRef, len(b.sources))
	stageRefs := make([]pipeline.NodeRef, len(b.stages))
	sinkRefs := make([]pipeline.NodeRef, len(b.sinks))

	for i := range b.sources {
		if b.sources[i] == nil {
			return errNilSource
		}
		ref, err := graph.AddSource(b.sources[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sourceRefs[i] = ref
	}
	for i := range b.stages {
		if err := publicError(compile.ValidateStageComponent(b.stages[i])); err != nil {
			return err
		}
		ref, err := graph.AddStage(b.stages[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		stageRefs[i] = ref
	}
	for i := range b.sinks {
		if err := validateSinkComponent(b.sinks[i]); err != nil {
			return err
		}
		ref, err := graph.AddSink(b.sinks[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sinkRefs[i] = ref
	}

	if len(b.routes) != 0 {
		nodes := make(map[string]pipeline.NodeRef, len(sourceRefs)+len(stageRefs)+len(sinkRefs))
		addRuntimeNodes(nodes, sourceRefs)
		addRuntimeNodes(nodes, stageRefs)
		addRuntimeNodes(nodes, sinkRefs)
		return b.compileExplicitRoutes(graph, nodes)
	}

	if len(stageRefs) == 0 {
		return linkMany(graph, sourceRefs, sinkRefs)
	}
	if err := linkMany(graph, sourceRefs, stageRefs[:1]); err != nil {
		return err
	}
	for i := 0; i < len(stageRefs)-1; i++ {
		if err := connectRefs(graph, stageRefs[i], stageRefs[i+1]); err != nil {
			return err
		}
	}
	return linkMany(graph, stageRefs[len(stageRefs)-1:], sinkRefs)
}

func (b *builder) compileExplicitRoutes(graph pipeline.Graph, nodes map[string]pipeline.NodeRef) error {
	for i := range b.routes {
		route, err := resolveRuntimeRoute(nodes, b.routes[i])
		if err != nil {
			return err
		}
		if err := graph.Connect(route); err != nil {
			return err
		}
	}
	return nil
}

func resolveRuntimeRoute(nodes map[string]pipeline.NodeRef, route pipeline.Route) (pipeline.Route, error) {
	from, err := resolveRuntimeNode(nodes, pipeline.NodeRef(route.From))
	if err != nil {
		return pipeline.Route{}, err
	}
	to := make([]string, len(route.To))
	for i := range route.To {
		ref, err := resolveRuntimeNode(nodes, pipeline.NodeRef(route.To[i]))
		if err != nil {
			return pipeline.Route{}, err
		}
		to[i] = ref.String()
	}
	route.From = from.String()
	route.To = to
	return route, nil
}

func addRuntimeNodes(nodes map[string]pipeline.NodeRef, refs []pipeline.NodeRef) {
	for i := range refs {
		nodes[refs[i].String()] = refs[i]
	}
}

func resolveRuntimeNode(nodes map[string]pipeline.NodeRef, ref pipeline.NodeRef) (pipeline.NodeRef, error) {
	node, ok := nodes[ref.String()]
	if !ok {
		return "", pipeline.ErrUnknownNode
	}
	return node, nil
}

func linkMany(graph pipeline.Graph, from []pipeline.NodeRef, to []pipeline.NodeRef) error {
	for i := range from {
		for j := range to {
			if err := connectRefs(graph, from[i], to[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func connectRefs(graph pipeline.Graph, from pipeline.NodeRef, to pipeline.NodeRef) error {
	return graph.Connect(pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: pipeline.RouteAll,
	})
}
