package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

type transcodeGraphCompiler struct{}

type transcodeBranch struct {
	name      string
	rendition transcode.Rendition
	request   encodeRequest
}

type transcodeOutputBranch struct {
	output  transcode.Output
	target  Output
	matches []int
}

func (transcodeGraphCompiler) match(b *builder) bool {
	return len(b.transcodes) == 1 &&
		len(b.inputs) == 0 &&
		len(b.rtpInputs) == 0 &&
		len(b.outputs) == 0 &&
		len(b.decodes) == 0 &&
		len(b.encodes) == 0 &&
		len(b.filters) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0 &&
		len(b.sinks) == 0 &&
		len(b.links) == 0 &&
		len(b.routes) == 0
}

func (transcodeGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planTranscode(spec)
}

func (transcodeGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildTranscode(ctx)
}

func (b *builder) planTranscode(spec pipeline.Spec) (pipeline.Spec, error) {
	plan := b.transcodes[0]
	branches, outputs, err := prepareTranscodePlan(plan)
	if err != nil {
		return pipeline.Spec{}, err
	}

	nodes := make(map[string]plannedNode, 3+len(branches)+len(outputs))
	sourceName := demuxNodeName(plan.Input)
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef); err != nil {
		return pipeline.Spec{}, err
	}

	previous, err := b.planDecodeFilterPath(nodes, &spec, []pipeline.NodeRef{sourceRef}, branches[0].rendition.Selector)
	if err != nil {
		return pipeline.Spec{}, err
	}

	encodeRefs := make([]pipeline.NodeRef, len(branches))
	for i := range branches {
		encodeName := encodeNodeName(branches[i].request)
		encodeRef := pipeline.NodeRef(encodeName)
		if err := addPlannedNode(nodes, &spec, encodeName, pipeline.NodeStage, encodeRef); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   previous,
			To:     encodeRef,
			Policy: pipeline.RouteAll,
		})
		encodeRefs[i] = encodeRef
	}

	for i := range outputs {
		outputName := muxNodeName(outputs[i].target, i)
		outputRef := pipeline.NodeRef(outputName)
		if err := addPlannedNode(nodes, &spec, outputName, pipeline.NodeStage, outputRef); err != nil {
			return pipeline.Spec{}, err
		}
		for _, branchIndex := range outputs[i].matches {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   encodeRefs[branchIndex],
				To:     outputRef,
				Policy: pipeline.RouteAll,
			})
		}
	}
	return spec, nil
}

func (b *builder) buildTranscode(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileTranscode(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return &task{graph: graph}, nil
}

func (b *builder) compileTranscode(ctx context.Context, graph pipeline.Graph) error {
	plan := b.transcodes[0]
	branches, outputs, err := prepareTranscodePlan(plan)
	if err != nil {
		return err
	}

	demux, err := b.openDemuxSource(ctx, plan.Input)
	if err != nil {
		return err
	}
	sourceRef, err := graph.AddSource(demux.source, b.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}

	selector := branches[0].rendition.Selector
	stream, err := selectDecodeStream(demux.streams, selector)
	if err != nil {
		return err
	}
	for i := 1; i < len(branches); i++ {
		branchStream, err := selectDecodeStream(demux.streams, branches[i].rendition.Selector)
		if err != nil {
			return err
		}
		if branchStream.ID != stream.ID || branchStream.Epoch != stream.Epoch {
			return ErrUnsupportedBuild
		}
	}

	realtime := b.runtime.realtime || plan.Input.Realtime
	previousRef, err := b.compileDecodeFilterPath(ctx, graph, []pipeline.NodeRef{sourceRef}, selector, stream, realtime, false)
	if err != nil {
		return err
	}

	encodeRefs := make([]pipeline.NodeRef, len(branches))
	encodedStreams := make([]av.Stream, len(branches))
	for i := range branches {
		config, encodedStream, err := prepareEncodeConfig(stream, branches[i].request, realtime)
		if err != nil {
			return err
		}
		encodeRef, err := b.compileEncodeStage(ctx, graph, previousRef, branches[i].request, config)
		if err != nil {
			return err
		}
		encodeRefs[i] = encodeRef
		encodedStreams[i] = encodedStream
	}

	for i := range outputs {
		streams := make([]av.Stream, 0, len(outputs[i].matches))
		for _, branchIndex := range outputs[i].matches {
			streams = append(streams, encodedStreams[branchIndex])
		}
		muxStage, err := b.openMuxStageWithFormat(ctx, outputs[i].target, i, streams, outputs[i].output.Format)
		if err != nil {
			return err
		}
		muxRef, err := graph.AddStage(muxStage, b.runtime.buffer)
		if err != nil {
			muxStage.Close()
			return err
		}
		for _, branchIndex := range outputs[i].matches {
			if err := graph.Link(pipeline.Link{From: encodeRefs[branchIndex], To: muxRef}); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareTranscodePlan(plan transcode.Plan) ([]transcodeBranch, []transcodeOutputBranch, error) {
	if len(plan.Renditions) == 0 || len(plan.Outputs) == 0 {
		return nil, nil, ErrUnsupportedBuild
	}
	branches, err := transcodeBranches(plan)
	if err != nil {
		return nil, nil, err
	}
	outputs, err := transcodeOutputs(plan, branches)
	if err != nil {
		return nil, nil, err
	}
	return branches, outputs, nil
}

func transcodeBranches(plan transcode.Plan) ([]transcodeBranch, error) {
	branches := make([]transcodeBranch, len(plan.Renditions))
	names := make(map[string]struct{}, len(plan.Renditions))
	for i := range plan.Renditions {
		rendition := plan.Renditions[i]
		if rendition.Resize != nil || rendition.Resample != nil {
			return nil, ErrUnsupportedBuild
		}
		name := transcodeRenditionName(rendition, i, len(plan.Renditions))
		if _, ok := names[name]; ok {
			return nil, ErrUnsupportedBuild
		}
		names[name] = struct{}{}

		config := rendition.Encode
		if config.Stream.ID == "" {
			config.Stream.ID = av.StreamID(name)
		}
		if config.Stream.Name == "" {
			config.Stream.Name = name
		}
		if config.Stream.Metadata == nil && rendition.Metadata != nil {
			config.Stream.Metadata = rendition.Metadata
		}
		branches[i] = transcodeBranch{
			name:      name,
			rendition: rendition,
			request: encodeRequest{
				name:     name,
				selector: rendition.Selector,
				config:   config,
			},
		}
	}
	return branches, nil
}

func transcodeOutputs(plan transcode.Plan, branches []transcodeBranch) ([]transcodeOutputBranch, error) {
	outputs := make([]transcodeOutputBranch, len(plan.Outputs))
	for i := range plan.Outputs {
		output := plan.Outputs[i]
		target := transcodeOutputTarget(plan, output)
		matches := transcodeOutputMatches(output, branches)
		if len(matches) == 0 {
			return nil, ErrUnsupportedBuild
		}
		outputs[i] = transcodeOutputBranch{
			output:  output,
			target:  target,
			matches: matches,
		}
	}
	return outputs, nil
}

func transcodeRenditionName(rendition transcode.Rendition, index int, total int) string {
	if rendition.Name != "" {
		return rendition.Name
	}
	if total == 1 {
		return "rendition"
	}
	return "rendition-" + strconv.Itoa(index+1)
}

func transcodeOutputTarget(plan transcode.Plan, output transcode.Output) Output {
	target := output.Target
	if target.Name == "" {
		target.Name = output.Name
	}
	if target.Metadata == nil {
		switch {
		case output.Metadata != nil:
			target.Metadata = output.Metadata
		case plan.Metadata != nil:
			target.Metadata = plan.Metadata
		}
	}
	return target
}

func transcodeOutputMatches(output transcode.Output, branches []transcodeBranch) []int {
	if len(output.Renditions) == 0 {
		matches := make([]int, len(branches))
		for i := range branches {
			matches[i] = i
		}
		return matches
	}

	matches := make([]int, 0, len(output.Renditions))
	for i := range branches {
		if transcodeOutputSelectsBranch(output, branches[i]) {
			matches = append(matches, i)
		}
	}
	return matches
}

func transcodeOutputSelectsBranch(output transcode.Output, branch transcodeBranch) bool {
	for i := range output.Renditions {
		name := output.Renditions[i]
		if name == branch.name || name == branch.rendition.Name {
			return true
		}
		for j := range branch.rendition.Labels {
			if name == branch.rendition.Labels[j] {
				return true
			}
		}
	}
	return false
}
