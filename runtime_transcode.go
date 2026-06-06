package goav

import (
	"context"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

type transcodeGraphCompiler struct{}
type rtpTranscodeGraphCompiler struct{}

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
	target  format.Output
	matches []int
}

type transcodeSelectorGroup struct {
	selector av.StreamSelector
	branches []int
}

type transcodeStreamGroup struct {
	selector av.StreamSelector
	stream   av.Stream
	branches []int
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

func (rtpTranscodeGraphCompiler) match(b *builder) bool {
	return len(b.transcodes) == 1 &&
		len(b.rtpInputs) > 0 &&
		len(b.inputs) == 0 &&
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

func (rtpTranscodeGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planRTPTranscode(spec)
}

func (transcodeGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildTranscode(ctx)
}

func (rtpTranscodeGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildRTPTranscode(ctx)
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

	return b.planTranscodeBranches(spec, nodes, []pipeline.NodeRef{sourceRef}, branches, outputs)
}

func (b *builder) planRTPTranscode(spec pipeline.Spec) (pipeline.Spec, error) {
	branches, outputs, err := prepareTranscodePlan(b.transcodes[0])
	if err != nil {
		return pipeline.Spec{}, err
	}

	nodes := make(map[string]plannedNode, len(b.rtpInputs)+3+len(branches)+transcodeTransformCount(branches)+len(outputs))
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, rtpInputDetail(b.rtpInputs[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}
	return b.planTranscodeBranches(spec, nodes, sourceRefs, branches, outputs)
}

func (b *builder) planTranscodeBranches(
	spec pipeline.Spec,
	nodes map[string]plannedNode,
	sourceRefs []pipeline.NodeRef,
	branches []transcodeBranch,
	outputs []transcodeOutputBranch,
) (pipeline.Spec, error) {
	groups := transcodeSelectorGroups(branches)
	branchInputs := make([]pipeline.NodeRef, len(branches))
	groupNodeOrder := make([]pipeline.NodeRef, 0, len(groups))
	sourceEdges := make([]pipeline.EdgeSpec, 0, len(groups)*len(sourceRefs))
	groupEdges := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(groups))
	for i := range groups {
		selectName := selectNodeName(groups[i].selector)
		selectRef := pipeline.NodeRef(selectName)
		if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(groups[i].selector)); err != nil {
			return pipeline.Spec{}, err
		}
		decodeName := decodeNodeName(groups[i].selector)
		decodeRef := pipeline.NodeRef(decodeName)
		if err := addPlannedNode(nodes, &spec, decodeName, pipeline.NodeStage, decodeRef, decodeNodeDetail(groups[i].selector)); err != nil {
			return pipeline.Spec{}, err
		}
		for _, sourceRef := range sourceRefs {
			sourceEdges = append(sourceEdges, pipeline.EdgeSpec{
				From:   sourceRef,
				To:     selectRef,
				Policy: pipeline.RouteAll,
			})
		}
		groupEdges[selectRef] = append(groupEdges[selectRef], pipeline.EdgeSpec{
			From:   selectRef,
			To:     decodeRef,
			Policy: pipeline.RouteAll,
		})
		groupNodeOrder = append(groupNodeOrder, selectRef, decodeRef)
		for _, branchIndex := range groups[i].branches {
			branchInputs[branchIndex] = decodeRef
		}
	}

	encodeRefs := make([]pipeline.NodeRef, len(branches))
	branchNodeOrder := make([]pipeline.NodeRef, 0, len(branches)+transcodeTransformCount(branches))
	outgoing := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(branches)*2+transcodeTransformCount(branches))
	for i := range branches {
		branchRef := branchInputs[i]
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
		if err := addPlannedNode(nodes, &spec, outputName, pipeline.NodeStage, outputRef, outputNodeDetailWithFormat(outputs[i].target, outputs[i].output.Format)); err != nil {
			return pipeline.Spec{}, err
		}
		for _, branchIndex := range outputs[i].matches {
			encodeRef := encodeRefs[branchIndex]
			outgoing[encodeRef] = append(outgoing[encodeRef], pipeline.EdgeSpec{
				From:   encodeRef,
				To:     outputRef,
				Policy: pipeline.RouteAll,
			})
		}
	}
	spec.Edges = append(spec.Edges, sourceEdges...)
	for i := range groupNodeOrder {
		spec.Edges = append(spec.Edges, groupEdges[groupNodeOrder[i]]...)
		spec.Edges = append(spec.Edges, outgoing[groupNodeOrder[i]]...)
	}
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

func (b *builder) buildRTPTranscode(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileRTPTranscode(ctx, graph); err != nil {
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

	realtime := b.runtime.realtime || plan.Input.Realtime
	groups, err := resolveTranscodeStreamGroups(demux.streams, branches)
	if err != nil {
		return err
	}
	branchInputs := make([]pipeline.NodeRef, len(branches))
	branchStreams := make([]av.Stream, len(branches))
	for i := range groups {
		previousRef, decodedStream, err := b.compileDecodeFilterPath(ctx, graph, []pipeline.NodeRef{sourceRef}, decodeRequest{selector: groups[i].selector}, groups[i].stream, realtime, false, codec.DecodeBounds{})
		if err != nil {
			return err
		}
		for _, branchIndex := range groups[i].branches {
			branchInputs[branchIndex] = previousRef
			branchStreams[branchIndex] = decodedStream
		}
	}

	return b.compileTranscodeBranches(ctx, graph, branches, outputs, branchInputs, branchStreams, realtime)
}

func (b *builder) compileRTPTranscode(ctx context.Context, graph pipeline.Graph) error {
	branches, outputs, err := prepareTranscodePlan(b.transcodes[0])
	if err != nil {
		return err
	}

	sourceRefs := make([]pipeline.NodeRef, 0, len(b.rtpInputs))
	streams := make([]av.Stream, 0, len(b.rtpInputs))
	builds := make([]rtpBuild, 0, len(b.rtpInputs))
	for i := range b.rtpInputs {
		receiver, err := b.openRTPSource(ctx, b.rtpInputs[i], i)
		if err != nil {
			return err
		}
		sourceRef, err := graph.AddSource(receiver.source, b.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
		builds = append(builds, receiver)
	}

	realtime := true
	groups, err := resolveTranscodeStreamGroups(streams, branches)
	if err != nil {
		return err
	}
	branchInputs := make([]pipeline.NodeRef, len(branches))
	branchStreams := make([]av.Stream, len(branches))
	for i := range groups {
		previousRef, decodedStream, err := b.compileDecodeFilterPath(
			ctx,
			graph,
			sourceRefs,
			decodeRequest{selector: groups[i].selector},
			groups[i].stream,
			realtime,
			false,
			rtpDecodeBoundsForStream(groups[i].stream, builds),
		)
		if err != nil {
			return err
		}
		for _, branchIndex := range groups[i].branches {
			branchInputs[branchIndex] = previousRef
			branchStreams[branchIndex] = decodedStream
		}
	}

	return b.compileTranscodeBranches(ctx, graph, branches, outputs, branchInputs, branchStreams, realtime)
}

func (b *builder) compileTranscodeBranches(
	ctx context.Context,
	graph pipeline.Graph,
	branches []transcodeBranch,
	outputs []transcodeOutputBranch,
	branchInputs []pipeline.NodeRef,
	branchStreams []av.Stream,
	realtime bool,
) error {
	encodeRefs := make([]pipeline.NodeRef, len(branches))
	encodedStreams := make([]av.Stream, len(branches))
	for i := range branches {
		branchRef := branchInputs[i]
		branchStream := branchStreams[i]
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
		muxStage, err := b.openMuxStageWithFormat(ctx, outputs[i].target, i, streams, transcodeOutputOpenFormat(outputs[i].output), outputs[i].output.Format)
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
	if len(plan.Renditions) == 0 {
		return nil, nil, transcodePlanEmptyError("renditions")
	}
	if len(plan.Outputs) == 0 {
		return nil, nil, transcodePlanEmptyError("outputs")
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

func transcodePlanEmptyError(kind string) error {
	suggestions := []string{
		"add at least one transcode.Rendition with a selector and encoder",
		"add at least one transcode.Output with a target output",
		"use goav.From(input).Video().Decode().Tap(...).Branch(...).To(label).Output(label, output) for the recipe API",
	}
	reason := "transcode plan has no " + kind
	return &BuildError{
		Code:        "transcode_plan_empty",
		Operation:   "build transcode",
		Node:        kind,
		Reason:      reason,
		Suggestions: suggestions,
		Cause:       ErrUnsupportedBuild,
	}
}

func transcodeSelectorGroups(branches []transcodeBranch) []transcodeSelectorGroup {
	groups := make([]transcodeSelectorGroup, 0, len(branches))
	index := make(map[string]int, len(branches))
	for i := range branches {
		key := transcodeSelectorKey(branches[i].rendition.Selector)
		groupIndex, ok := index[key]
		if !ok {
			groupIndex = len(groups)
			index[key] = groupIndex
			groups = append(groups, transcodeSelectorGroup{selector: branches[i].rendition.Selector})
		}
		groups[groupIndex].branches = append(groups[groupIndex].branches, i)
	}
	return groups
}

func resolveTranscodeStreamGroups(streams []av.Stream, branches []transcodeBranch) ([]transcodeStreamGroup, error) {
	groups := make([]transcodeStreamGroup, 0, len(branches))
	index := make(map[string]int, len(branches))
	for i := range branches {
		stream, err := selectDecodeStream(streams, branches[i].rendition.Selector)
		if err != nil {
			return nil, err
		}
		key := transcodeStreamKey(stream)
		groupIndex, ok := index[key]
		if !ok {
			groupIndex = len(groups)
			index[key] = groupIndex
			groups = append(groups, transcodeStreamGroup{
				selector: branches[i].rendition.Selector,
				stream:   stream,
			})
		}
		groups[groupIndex].branches = append(groups[groupIndex].branches, i)
	}
	return groups, nil
}

func transcodeSelectorKey(selector av.StreamSelector) string {
	return strings.Join([]string{
		string(selector.ID),
		strconv.FormatInt(int64(selector.Index), 10),
		strconv.FormatBool(selector.UseIndex),
		string(selector.Type),
		string(selector.Codec),
		selector.Name,
	}, "\x00")
}

func transcodeStreamKey(stream av.Stream) string {
	return strings.Join([]string{
		string(stream.ID),
		strconv.FormatInt(int64(stream.Index), 10),
		string(stream.Type),
		strconv.FormatUint(uint64(stream.Epoch), 10),
	}, "\x00")
}

func transcodeBranches(plan transcode.Plan) ([]transcodeBranch, error) {
	branches := make([]transcodeBranch, len(plan.Renditions))
	names := make(map[string]struct{}, len(plan.Renditions))
	for i := range plan.Renditions {
		rendition := plan.Renditions[i]
		name := transcodeRenditionName(rendition, i, len(plan.Renditions))
		if _, ok := names[name]; ok {
			return nil, transcodeDuplicateRenditionError(name, i)
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

func transcodeDuplicateRenditionError(name string, index int) error {
	return &BuildError{
		Code:      "transcode_rendition_duplicate",
		Operation: "build transcode",
		Node:      name,
		Reason:    "transcode rendition name is defined more than once",
		Details: []string{
			"duplicate index: " + strconv.Itoa(index),
		},
		Suggestions: []string{
			"give each transcode.Rendition a stable unique Name",
			"use distinct branch names when multiple renditions share one selected stream",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeTransforms(name string, rendition transcode.Rendition) ([]transcodeTransform, error) {
	if rendition.Resize != nil && rendition.Resample != nil {
		return nil, advancedTranscodeTransformChainError(name)
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

func advancedTranscodeTransformChainError(name string) error {
	return &BuildError{
		Code:      "transcode_transform_chain_unsupported",
		Operation: "build transcode",
		Node:      name,
		Reason:    "transcode rendition cannot combine resize and resample",
		Suggestions: []string{
			"use resize on video renditions and resample on audio renditions",
			"split audio and video work into separate transcode.Rendition values",
			"use goav.From(input).Video().Decode().Tap(...).Branch(...) or .Audio().Decode().Tap(...).Branch(...) to keep transforms stream-scoped",
		},
		Cause: ErrUnsupportedBuild,
	}
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
			return av.Stream{}, advancedTranscodeTransformMediaError(transform, stream, "resample", "audio")
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
			return av.Stream{}, advancedTranscodeTransformMediaError(transform, stream, "resize", "video")
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

func advancedTranscodeTransformMediaError(transform transcodeTransform, stream av.Stream, operation string, media string) error {
	details := []string{
		"stream id: " + string(stream.ID),
		"stream type: " + string(stream.Type),
		"codec type: " + string(stream.Codec.Type),
	}
	return &BuildError{
		Code:      "transcode_transform_media_mismatch",
		Operation: "build transcode",
		Node:      transform.name,
		Reason:    operation + " applies to " + media + " streams",
		Details:   details,
		Suggestions: []string{
			"use resize on video renditions",
			"use resample on audio renditions",
			"check the transcode.Rendition selector for this branch",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeOutputs(plan transcode.Plan, branches []transcodeBranch) ([]transcodeOutputBranch, error) {
	outputs := make([]transcodeOutputBranch, len(plan.Outputs))
	for i := range plan.Outputs {
		output := plan.Outputs[i]
		target := transcodeOutputTarget(plan, output)
		matches := transcodeOutputMatches(output, branches)
		if len(matches) == 0 {
			return nil, transcodeOutputUnmatchedError(output, target)
		}
		outputs[i] = transcodeOutputBranch{
			output:  output,
			target:  target,
			matches: matches,
		}
	}
	return outputs, nil
}

func transcodeOutputUnmatchedError(output transcode.Output, target format.Output) error {
	node := firstNonEmpty(output.Name, target.Name, target.URI, "output")
	details := make([]string, 0, 1)
	if len(output.Renditions) != 0 {
		details = append(details, "requested: "+strings.Join(output.Renditions, ", "))
	}
	return &BuildError{
		Code:      "transcode_output_unmatched",
		Operation: "build transcode",
		Node:      node,
		Reason:    "output selects no transcode branches",
		Details:   details,
		Suggestions: []string{
			"reference a branch name from transcode.Rendition.Name",
			"reference a label listed on the branch",
			"leave Renditions empty when the output should receive every branch",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeOutputOpenFormat(output transcode.Output) av.FormatID {
	return output.OpenFormat()
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

func transcodeOutputTarget(plan transcode.Plan, output transcode.Output) format.Output {
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
		if config.Width <= 0 || config.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit requires positive target width and height")
		}
		if stream.Codec.Width <= 0 || stream.Codec.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit requires known positive input width and height")
		}
		stream.Codec.Width, stream.Codec.Height = resizeFitStreamDimensions(stream.Codec.Width, stream.Codec.Height, config.Width, config.Height)
		if stream.Codec.Width == 0 || stream.Codec.Height == 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit produced empty output geometry")
		}
		return nil
	case filter.ResizeFill:
		if config.Width <= 0 || config.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fill requires positive target width and height")
		}
		stream.Codec.Width = config.Width
		stream.Codec.Height = config.Height
		return nil
	default:
		return transcodeResizeConfigError(*stream, mode, config, "unsupported resize mode")
	}
}

func transcodeResizeConfigError(stream av.Stream, mode filter.ResizeMode, config filter.ResizeConfig, reason string) error {
	node := "resize"
	if stream.ID != "" {
		node += "-" + string(stream.ID)
	}
	return &BuildError{
		Code:      "transcode_resize_invalid",
		Operation: "build transcode",
		Node:      node,
		Reason:    reason,
		Details: []string{
			"mode: " + string(mode),
			"stream id: " + string(stream.ID),
			"input width: " + strconv.Itoa(stream.Codec.Width),
			"input height: " + strconv.Itoa(stream.Codec.Height),
			"target width: " + strconv.Itoa(config.Width),
			"target height: " + strconv.Itoa(config.Height),
		},
		Suggestions: []string{
			"use resize mode exact, fit, fill, or passthrough",
			"provide positive target dimensions for fit and fill",
			"use exact resize when input dimensions are not known before filtering",
		},
		Cause: ErrUnsupportedBuild,
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
