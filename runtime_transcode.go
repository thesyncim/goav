package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

type transcodeGraphCompiler struct{}

type transcodeBranch struct {
	name       string
	rendition  transcode.Rendition
	transforms []transcodeTransform
	request    encodeRequest
}

type transcodeTransform struct {
	name    string
	factory string
	video   *filter.ResizeConfig
	audio   *filter.ResampleConfig
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
		len(b.sinks) == 0
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

	nodes := make(map[string]plannedNode, 3+len(branches)+transcodeTransformCount(branches)+len(outputs))
	sourceName := demuxNodeName(plan.Input)
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, inputNodeDetail(plan.Input)); err != nil {
		return pipeline.Spec{}, err
	}

	previous, err := b.planDecodeFilterPath(nodes, &spec, []pipeline.NodeRef{sourceRef}, branches[0].rendition.Selector)
	if err != nil {
		return pipeline.Spec{}, err
	}

	encodeRefs := make([]pipeline.NodeRef, len(branches))
	branchNodeOrder := make([]pipeline.NodeRef, 0, len(branches)+transcodeTransformCount(branches))
	outgoing := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(branches)*2+transcodeTransformCount(branches))
	for i := range branches {
		branchRef := previous
		for j := range branches[i].transforms {
			transformRef := pipeline.NodeRef(branches[i].transforms[j].name)
			if err := addPlannedNode(nodes, &spec, branches[i].transforms[j].name, pipeline.NodeStage, transformRef, transcodeTransformDetail(branches[i].transforms[j])); err != nil {
				return pipeline.Spec{}, err
			}
			outgoing[branchRef] = append(outgoing[branchRef], pipeline.EdgeSpec{
				From:   branchRef,
				To:     transformRef,
				Policy: pipeline.RouteAll,
			})
			branchRef = transformRef
			branchNodeOrder = append(branchNodeOrder, transformRef)
		}

		encodeName := encodeNodeName(branches[i].request)
		encodeRef := pipeline.NodeRef(encodeName)
		if err := addPlannedNode(nodes, &spec, encodeName, pipeline.NodeStage, encodeRef, encodeNodeDetail(branches[i].request)); err != nil {
			return pipeline.Spec{}, err
		}
		outgoing[branchRef] = append(outgoing[branchRef], pipeline.EdgeSpec{
			From:   branchRef,
			To:     encodeRef,
			Policy: pipeline.RouteAll,
		})
		encodeRefs[i] = encodeRef
		branchNodeOrder = append(branchNodeOrder, encodeRef)
	}

	for i := range outputs {
		outputName := muxNodeName(outputs[i].target, i)
		outputRef := pipeline.NodeRef(outputName)
		if err := addPlannedNode(nodes, &spec, outputName, pipeline.NodeStage, outputRef, outputNodeDetail(outputs[i].target)); err != nil {
			return pipeline.Spec{}, err
		}
		for _, branchIndex := range outputs[i].matches {
			encodeRef := encodeRefs[branchIndex]
			outgoing[encodeRef] = append(outgoing[encodeRef], pipeline.EdgeSpec{
				From:   encodeRefs[branchIndex],
				To:     outputRef,
				Policy: pipeline.RouteAll,
			})
		}
	}
	spec.Edges = append(spec.Edges, outgoing[previous]...)
	for i := range branchNodeOrder {
		spec.Edges = append(spec.Edges, outgoing[branchNodeOrder[i]]...)
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
	previousRef, _, err := b.compileDecodeFilterPath(ctx, graph, []pipeline.NodeRef{sourceRef}, selector, stream, realtime, false, codec.DecodeBounds{})
	if err != nil {
		return err
	}

	encodeRefs := make([]pipeline.NodeRef, len(branches))
	encodedStreams := make([]av.Stream, len(branches))
	for i := range branches {
		branchRef := previousRef
		branchStream := stream
		for j := range branches[i].transforms {
			stage, outputStream, err := b.newTranscodeFilterStage(ctx, branches[i].transforms[j], branchStream, realtime)
			if err != nil {
				return err
			}
			stageRef, err := graph.AddStage(stage, b.runtime.buffer)
			if err != nil {
				stage.Close()
				return err
			}
			if err := connectRefs(graph, branchRef, stageRef); err != nil {
				return err
			}
			branchRef = stageRef
			branchStream = outputStream
		}

		config, encodedStream, err := prepareEncodeConfig(branchStream, branches[i].request, realtime)
		if err != nil {
			return err
		}
		encodeRef, err := b.compileEncodeStage(ctx, graph, branchRef, branches[i].request, config)
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
			if err := connectRefs(graph, encodeRefs[branchIndex], muxRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *builder) newTranscodeFilterStage(ctx context.Context, transform transcodeTransform, stream av.Stream, realtime bool) (*filter.Stage, av.Stream, error) {
	outputStream, err := applyTranscodeTransformToStream(stream, transform)
	if err != nil {
		return nil, av.Stream{}, err
	}
	factory, err := b.runtime.filters.Factory(transform.factory)
	if err != nil {
		return nil, av.Stream{}, err
	}
	config := filter.Config{
		Stream:   stream,
		Realtime: realtime,
		Video:    transform.video,
		Audio:    transform.audio,
	}
	frameFilter, err := factory.NewFilter(ctx, config)
	if err != nil {
		return nil, av.Stream{}, err
	}
	stage, err := filter.NewStage(filter.StageConfig{
		Name:   transform.name,
		Detail: transcodeTransformDetail(transform),
		Filter: frameFilter,
		Result: filterResultForStream(outputStream),
	})
	if err != nil {
		frameFilter.Close()
		return nil, av.Stream{}, err
	}
	return stage, outputStream, nil
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
		name := transcodeRenditionName(rendition, i, len(plan.Renditions))
		if _, ok := names[name]; ok {
			return nil, ErrUnsupportedBuild
		}
		names[name] = struct{}{}
		transforms, err := transcodeTransforms(name, rendition)
		if err != nil {
			return nil, err
		}

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
			name:       name,
			rendition:  rendition,
			transforms: transforms,
			request: encodeRequest{
				name:     name,
				selector: rendition.Selector,
				config:   config,
			},
		}
	}
	return branches, nil
}

func transcodeTransforms(name string, rendition transcode.Rendition) ([]transcodeTransform, error) {
	if rendition.Resize != nil && rendition.Resample != nil {
		return nil, ErrUnsupportedBuild
	}
	if rendition.Resize != nil {
		return []transcodeTransform{{
			name:    "resize-" + name,
			factory: filter.FactoryResize,
			video:   rendition.Resize,
		}}, nil
	}
	if rendition.Resample != nil {
		return []transcodeTransform{{
			name:    "resample-" + name,
			factory: filter.FactoryResample,
			audio:   rendition.Resample,
		}}, nil
	}
	return nil, nil
}

func transcodeTransformCount(branches []transcodeBranch) int {
	count := 0
	for i := range branches {
		count += len(branches[i].transforms)
	}
	return count
}

func applyTranscodeTransformToStream(stream av.Stream, transform transcodeTransform) (av.Stream, error) {
	out := stream
	switch {
	case transform.audio != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return av.Stream{}, ErrUnsupportedBuild
		}
		out.Type = av.MediaAudio
		out.Codec.Type = av.MediaAudio
		if transform.audio.SampleRate != 0 {
			out.Codec.SampleRate = transform.audio.SampleRate
			out.Codec.ClockRate = uint32(transform.audio.SampleRate)
			out.TimeBase = av.TimeBase{Num: 1, Den: int64(transform.audio.SampleRate)}
		}
		if transform.audio.Channels != 0 {
			out.Codec.Channels = transform.audio.Channels
		}
		if transform.audio.ChannelLayout != "" {
			out.Codec.ChannelLayout = transform.audio.ChannelLayout
		}
		if transform.audio.SampleFormat != "" {
			out.Codec.SampleFormat = transform.audio.SampleFormat
		}
	case transform.video != nil:
		if stream.Type != av.MediaVideo && stream.Codec.Type != av.MediaVideo {
			return av.Stream{}, ErrUnsupportedBuild
		}
		out.Type = av.MediaVideo
		out.Codec.Type = av.MediaVideo
		if err := applyResizeConfigToStream(&out, *transform.video); err != nil {
			return av.Stream{}, err
		}
		if transform.video.PixelFormat != "" {
			out.Codec.PixelFormat = transform.video.PixelFormat
		}
	}
	return out, nil
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

func filterResultForStream(stream av.Stream) filter.Result {
	frame := av.Frame{}
	if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
		frame.Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, audioDecodeBufferSize(stream))}}}
	}
	if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
		frame = preallocVideoFilterFrame(stream)
	}
	return filter.Result{
		Frames: []av.Frame{frame}[:0],
		Events: make([]av.Event, 0, 1),
	}
}

func applyResizeConfigToStream(stream *av.Stream, config filter.ResizeConfig) error {
	mode := config.Mode
	if mode == "" {
		mode = filter.ResizeExact
	}
	switch mode {
	case filter.ResizePassthrough:
		return nil
	case filter.ResizeExact:
		if config.Width != 0 {
			stream.Codec.Width = config.Width
		}
		if config.Height != 0 {
			stream.Codec.Height = config.Height
		}
		return nil
	case filter.ResizeFit:
		if config.Width <= 0 || config.Height <= 0 || stream.Codec.Width <= 0 || stream.Codec.Height <= 0 {
			return ErrUnsupportedBuild
		}
		stream.Codec.Width, stream.Codec.Height = resizeFitStreamDimensions(stream.Codec.Width, stream.Codec.Height, config.Width, config.Height)
		if stream.Codec.Width == 0 || stream.Codec.Height == 0 {
			return ErrUnsupportedBuild
		}
		return nil
	case filter.ResizeFill:
		if config.Width <= 0 || config.Height <= 0 {
			return ErrUnsupportedBuild
		}
		stream.Codec.Width = config.Width
		stream.Codec.Height = config.Height
		return nil
	default:
		return ErrUnsupportedBuild
	}
}

func preallocVideoFilterFrame(stream av.Stream) av.Frame {
	frame := av.Frame{Planes: make([]av.Plane, 3)}
	width := stream.Codec.Width
	height := stream.Codec.Height
	if width <= 0 || height <= 0 || width%2 != 0 || height%2 != 0 {
		return frame
	}
	frame.Planes[0].Buffer.Bytes = make([]byte, 0, width*height)
	frame.Planes[1].Buffer.Bytes = make([]byte, 0, width*height/4)
	frame.Planes[2].Buffer.Bytes = make([]byte, 0, width*height/4)
	return frame
}

func resizeFitStreamDimensions(inputWidth int, inputHeight int, targetWidth int, targetHeight int) (int, int) {
	if targetWidth*inputHeight <= targetHeight*inputWidth {
		return evenStreamDimension(targetWidth), evenStreamDimension((inputHeight*targetWidth + inputWidth/2) / inputWidth)
	}
	return evenStreamDimension((inputWidth*targetHeight + inputHeight/2) / inputHeight), evenStreamDimension(targetHeight)
}

func evenStreamDimension(value int) int {
	if value < 2 {
		return 0
	}
	return value &^ 1
}
