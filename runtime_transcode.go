package goav

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type branchComposeRoute struct {
	name              string
	branch            branchComposeBranch
	copy              bool
	decode            codec.CodecSpec
	codecChange       CodecChangePolicy
	dropDecodeEvents  bool
	sourceDomain      shape.MediaDomain
	sharedOperations  []OperationSpec
	privateOperations []OperationSpec
	request           encodeRequest
}

type mediaTransform struct {
	name    string
	factory string
	stage   pipeline.Stage
	video   *filter.ResizeConfig
	audio   *filter.ResampleConfig
}

type branchComposeTargetRoute struct {
	node    pipeline.NodeRef
	output  branchComposeTarget
	target  format.Output
	sink    pipeline.Sink
	matches []int
}

type branchComposeSelectorGroup struct {
	selector av.StreamSelector
	input    string
	branches []int
}

type branchComposeStreamGroup struct {
	selector av.StreamSelector
	input    string
	stream   av.Stream
	branches []int
}

type branchComposeSharedStepGroup struct {
	operations []OperationSpec
	branches   []int
}

func planBranchComposeRoutes(
	spec pipeline.Spec,
	nodes map[string]plannedNode,
	sourceRefs []pipeline.NodeRef,
	branches []branchComposeRoute,
	outputs []branchComposeTargetRoute,
) (pipeline.Spec, error) {
	groups := branchComposeSelectorGroups(branches)
	branchInputs := make([]pipeline.NodeRef, len(branches))
	groupNodeOrder := make([]pipeline.NodeRef, 0, len(groups))
	sourceEdges := make([]pipeline.EdgeSpec, 0, len(groups)*len(sourceRefs))
	groupEdges := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(groups))
	for i := range groups {
		selectName := branchComposeInputNodeName(selectNodeName(groups[i].selector), groups[i].input)
		selectRef := pipeline.NodeRef(selectName)
		if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(groups[i].selector)); err != nil {
			return pipeline.Spec{}, err
		}
		for _, sourceRef := range sourceRefs {
			sourceEdges = append(sourceEdges, pipeline.EdgeSpec{
				From:   sourceRef,
				To:     selectRef,
				Policy: pipeline.RouteAll,
			})
		}
		groupNodeOrder = append(groupNodeOrder, selectRef)
		decodedBranches := branchComposeDecodedBranchIndices(groups[i].branches, branches)
		for _, branchIndex := range groups[i].branches {
			if !branchComposeRouteNeedsDecode(branches[branchIndex]) {
				branchInputs[branchIndex] = selectRef
			}
		}
		if len(decodedBranches) == 0 {
			continue
		}
		decodeConfig, err := branchComposeGroupDecodeConfig(decodedBranches, branches)
		if err != nil {
			return pipeline.Spec{}, err
		}
		codecChange, err := branchComposeGroupCodecChangePolicy(decodedBranches, branches)
		if err != nil {
			return pipeline.Spec{}, err
		}
		decodeName := branchComposeInputNodeName(decodeNodeName(groups[i].selector), groups[i].input)
		decodeRef := pipeline.NodeRef(decodeName)
		decodeDetail := decodeRequestDetail(decodeRequest{selector: groups[i].selector, config: decodeConfig, codecChange: codecChange})
		if err := addPlannedNode(nodes, &spec, decodeName, pipeline.NodeStage, decodeRef, decodeDetail); err != nil {
			return pipeline.Spec{}, err
		}
		groupEdges[selectRef] = append(groupEdges[selectRef], pipeline.EdgeSpec{
			From:   selectRef,
			To:     decodeRef,
			Policy: pipeline.RouteAll,
		})
		groupNodeOrder = append(groupNodeOrder, decodeRef)
		for _, prefix := range branchComposeSharedStepGroups(decodedBranches, branches) {
			if len(prefix.branches) == 0 {
				continue
			}
			firstBranch := prefix.branches[0]
			transforms, err := branchComposeRouteOperationTransformsForName(branchComposeSharedOperationName(branches[firstBranch]), prefix.operations)
			if err != nil {
				return pipeline.Spec{}, err
			}
			branchRef := decodeRef
			for j := range transforms {
				stepRef := pipeline.NodeRef(transforms[j].name)
				if err := addPlannedNode(nodes, &spec, transforms[j].name, pipeline.NodeStage, stepRef, mediaTransformDetail(transforms[j])); err != nil {
					return pipeline.Spec{}, err
				}
				outgoingEdge := pipeline.EdgeSpec{
					From:   branchRef,
					To:     stepRef,
					Policy: pipeline.RouteAll,
				}
				groupEdges[branchRef] = append(groupEdges[branchRef], outgoingEdge)
				branchRef = stepRef
				groupNodeOrder = append(groupNodeOrder, stepRef)
			}
			for _, branchIndex := range prefix.branches {
				branchInputs[branchIndex] = branchRef
			}
		}
	}

	branchOutputRefs := make([]pipeline.NodeRef, len(branches))
	branchNodeOrder := make([]pipeline.NodeRef, 0, len(branches)+branchComposeOperationStageCount(branches))
	outgoing := make(map[pipeline.NodeRef][]pipeline.EdgeSpec, len(branches)*2+branchComposeOperationStageCount(branches))
	for i := range branches {
		branchRef := branchInputs[i]
		transforms, err := branchComposePrivateOperationTransforms(branches[i])
		if err != nil {
			return pipeline.Spec{}, err
		}
		for j := range transforms {
			stepRef := pipeline.NodeRef(transforms[j].name)
			if err := addPlannedNode(nodes, &spec, transforms[j].name, pipeline.NodeStage, stepRef, mediaTransformDetail(transforms[j])); err != nil {
				return pipeline.Spec{}, err
			}
			outgoing[branchRef] = append(outgoing[branchRef], pipeline.EdgeSpec{
				From:   branchRef,
				To:     stepRef,
				Policy: pipeline.RouteAll,
			})
			branchRef = stepRef
			branchNodeOrder = append(branchNodeOrder, stepRef)
		}

		if branchComposeRouteNeedsEncode(branches[i]) {
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
			branchRef = encodeRef
			branchNodeOrder = append(branchNodeOrder, encodeRef)
		}
		branchOutputRefs[i] = branchRef
	}

	for i := range outputs {
		if outputs[i].sink != nil {
			outputName := branchComposeTargetSinkNodeName(outputs[i], i)
			outputRef := pipeline.NodeRef(outputName)
			if err := addPlannedNode(nodes, &spec, outputName, pipeline.NodeSink, outputRef, describedNodeDetail(outputs[i].sink)); err != nil {
				return pipeline.Spec{}, err
			}
			for _, branchIndex := range outputs[i].matches {
				branchRef := branchOutputRefs[branchIndex]
				outgoing[branchRef] = append(outgoing[branchRef], pipeline.EdgeSpec{
					From:   branchRef,
					To:     outputRef,
					Policy: pipeline.RouteAll,
				})
			}
			continue
		}

		outputName := muxNodeName(outputs[i].target, i)
		outputRef := pipeline.NodeRef(outputName)
		if err := addPlannedNode(nodes, &spec, outputName, pipeline.NodeStage, outputRef, outputNodeDetailWithFormat(outputs[i].target, outputs[i].output.Format)); err != nil {
			return pipeline.Spec{}, err
		}
		for _, branchIndex := range outputs[i].matches {
			branchRef := branchOutputRefs[branchIndex]
			outgoing[branchRef] = append(outgoing[branchRef], pipeline.EdgeSpec{
				From:   branchRef,
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

func compileBranchComposeInputs(
	ctx context.Context,
	runtime *runtime,
	graph pipeline.Graph,
	sourceRefs []pipeline.NodeRef,
	groups []branchComposeStreamGroup,
	builds []rtpBuild,
	branches []branchComposeRoute,
	inputPlan map[string]graphPlanBranchComposeInputOperation,
	realtime bool,
) ([]pipeline.NodeRef, []av.Stream, error) {
	service := &builder{runtime: runtime}
	branchInputs := make([]pipeline.NodeRef, len(branches))
	branchStreams := make([]av.Stream, len(branches))
	for i := range groups {
		selector := groups[i].selector
		selected := groups[i].stream
		planned, ok := inputPlan[branchComposeSelectorGroupKey(selector, groups[i].input)]
		if !ok {
			planned = inputPlan[branchComposeSelectorKey(selector)]
		}
		selectName := firstNonEmpty(planned.selectNode.String(), branchComposeInputNodeName(selectNodeName(selector), groups[i].input))
		selectStage := newStreamSelectStage(selectName, selected, selector, selectNodeDetail(selector))
		selectRef, err := graph.AddStage(selectStage, runtime.buffer)
		if err != nil {
			selectStage.Close()
			return nil, nil, err
		}
		for j := range sourceRefs {
			if err := connectRefs(graph, sourceRefs[j], selectRef); err != nil {
				return nil, nil, err
			}
		}
		decodedBranches := branchComposeDecodedBranchIndices(groups[i].branches, branches)
		for _, branchIndex := range groups[i].branches {
			if !branchComposeRouteNeedsDecode(branches[branchIndex]) {
				branchInputs[branchIndex] = selectRef
				branchStreams[branchIndex] = selected
			}
		}
		if len(decodedBranches) == 0 {
			continue
		}
		bounds := codec.DecodeBounds{}
		if len(builds) != 0 {
			bounds = rtpDecodeBoundsForStream(selected, builds)
		}
		decodeConfig, err := branchComposeGroupDecodeConfig(decodedBranches, branches)
		if err != nil {
			return nil, nil, err
		}
		codecChange, err := branchComposeGroupCodecChangePolicy(decodedBranches, branches)
		if err != nil {
			return nil, nil, err
		}
		dropDecodeEvents := branchComposeGroupDropDecodeEvents(decodedBranches, branches)
		decodeName := firstNonEmpty(planned.decodeNode.String(), decodeNodeName(selector))
		decodeStage, err := service.newDecodeStageNamed(ctx, decodeName, decodeRequest{selector: selector, config: decodeConfig, codecChange: codecChange}, selected, realtime, dropDecodeEvents, bounds)
		if err != nil {
			return nil, nil, err
		}
		decodeRef, err := graph.AddStage(decodeStage, runtime.buffer)
		if err != nil {
			decodeStage.Close()
			return nil, nil, err
		}
		if err := connectRefs(graph, selectRef, decodeRef); err != nil {
			return nil, nil, err
		}
		decodedStream := selected
		for _, branchIndex := range decodedBranches {
			branchInputs[branchIndex] = decodeRef
			branchStreams[branchIndex] = decodedStream
		}
	}
	return branchInputs, branchStreams, nil
}

func compileBranchComposeRoutes(
	ctx context.Context,
	service *builder,
	graph pipeline.Graph,
	branches []branchComposeRoute,
	outputs []branchComposeTargetRoute,
	branchInputs []pipeline.NodeRef,
	branchStreams []av.Stream,
	sharedStepPlan map[string][]pipeline.NodeRef,
	branchPlan map[string]graphPlanBranchComposeBranchOperation,
	realtime bool,
) error {
	runtime := service.runtime
	branchRefs := append([]pipeline.NodeRef(nil), branchInputs...)
	branchInputStreams := append([]av.Stream(nil), branchStreams...)
	for _, prefix := range branchComposeRuntimeSharedStepGroups(branches, branchInputs, branchStreams) {
		if len(prefix.branches) == 0 {
			continue
		}
		firstBranch := prefix.branches[0]
		branchRef := branchInputs[firstBranch]
		branchStream := branchStreams[firstBranch]
		stepRefs := branchComposeSharedStepPlanRefs(sharedStepPlan, branches, prefix.branches)
		transforms, err := branchComposeRouteOperationTransformsForName(branchComposeSharedOperationName(branches[firstBranch]), prefix.operations)
		if err != nil {
			return err
		}
		for j := range transforms {
			stageName := ""
			if j < len(stepRefs) {
				stageName = stepRefs[j].String()
			}
			stage, outputStream, err := service.newBranchComposeStepStageNamed(ctx, stageName, transforms[j], branchStream, realtime)
			if err != nil {
				return err
			}
			stageRef, err := graph.AddStage(stage, runtime.buffer)
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
		for _, branchIndex := range prefix.branches {
			branchRefs[branchIndex] = branchRef
			branchInputStreams[branchIndex] = branchStream
		}
	}

	branchOutputRefs := make([]pipeline.NodeRef, len(branches))
	branchOutputStreams := make([]av.Stream, len(branches))
	for i := range branches {
		branchRef := branchRefs[i]
		branchStream := branchInputStreams[i]
		planned := branchPlan[branches[i].name]
		transforms, err := branchComposePrivateOperationTransforms(branches[i])
		if err != nil {
			return err
		}
		for j := range transforms {
			stageName := ""
			if j < len(planned.privateStageNodes) {
				stageName = planned.privateStageNodes[j].String()
			}
			stage, outputStream, err := service.newBranchComposeStepStageNamed(ctx, stageName, transforms[j], branchStream, realtime)
			if err != nil {
				return err
			}
			stageRef, err := graph.AddStage(stage, runtime.buffer)
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

		if branchComposeRouteNeedsEncode(branches[i]) {
			request := branches[i].request
			config, encodedStream, err := prepareEncodeConfig(branchStream, request, realtime)
			if err != nil {
				return err
			}
			encodeRef, err := service.compileEncodeStageNamed(ctx, graph, planned.encodeNode.String(), branchRef, request, config)
			if err != nil {
				return err
			}
			branchRef = encodeRef
			branchStream = encodedStream
		}
		branchOutputRefs[i] = branchRef
		branchOutputStreams[i] = branchStream
	}

	for i := range outputs {
		if outputs[i].sink != nil {
			sink := outputs[i].sink
			if outputs[i].node != "" {
				sink = namedSink{name: outputs[i].node.String(), sink: sink}
			}
			sinkRef, err := graph.AddSink(sink, runtime.buffer)
			if err != nil {
				return err
			}
			for _, branchIndex := range outputs[i].matches {
				if err := connectRefs(graph, branchOutputRefs[branchIndex], sinkRef); err != nil {
					return err
				}
			}
			continue
		}

		streams := make([]av.Stream, 0, len(outputs[i].matches))
		for _, branchIndex := range outputs[i].matches {
			streams = append(streams, branchOutputStreams[branchIndex])
		}
		destination := outputs[i].output.Destination
		if destinationSpecEmpty(destination) {
			destination = destinationSpec{
				output: outputs[i].target,
				format: outputs[i].output.Format,
			}
		}
		muxStage, err := service.openMuxDestinationStage(ctx, destination, i, streams, branchComposeTargetOpenFormat(outputs[i].output), outputs[i].output.Format)
		if err != nil {
			return err
		}
		stage := pipeline.Stage(muxStage)
		if outputs[i].node != "" {
			stage = namedStage{name: outputs[i].node.String(), stage: muxStage}
		}
		muxRef, err := graph.AddStage(stage, runtime.buffer)
		if err != nil {
			muxStage.Close()
			return err
		}
		for _, branchIndex := range outputs[i].matches {
			if err := connectRefs(graph, branchOutputRefs[branchIndex], muxRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func branchComposeSharedStepPlanRefs(sharedStepPlan map[string][]pipeline.NodeRef, branches []branchComposeRoute, indices []int) []pipeline.NodeRef {
	if len(sharedStepPlan) == 0 {
		return nil
	}
	for _, index := range indices {
		if index < 0 || index >= len(branches) {
			continue
		}
		if refs := sharedStepPlan[branches[index].name]; len(refs) != 0 {
			return refs
		}
	}
	return nil
}

func (b *builder) newBranchComposeStepStageNamed(ctx context.Context, name string, transform mediaTransform, stream av.Stream, realtime bool) (pipeline.Stage, av.Stream, error) {
	if transform.stage != nil {
		if name != "" && name != transform.stage.Name() {
			return namedStage{name: name, stage: transform.stage}, stream, nil
		}
		return transform.stage, stream, nil
	}
	return b.newMediaTransformStageNamed(ctx, name, transform, stream, realtime)
}

func (b *builder) newMediaTransformStage(ctx context.Context, transform mediaTransform, stream av.Stream, realtime bool) (*filter.Stage, av.Stream, error) {
	return b.newMediaTransformStageNamed(ctx, "", transform, stream, realtime)
}

func (b *builder) newMediaTransformStageNamed(ctx context.Context, name string, transform mediaTransform, stream av.Stream, realtime bool) (*filter.Stage, av.Stream, error) {
	outputStream, err := applyMediaTransformToStream(stream, transform)
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
	name = firstNonEmpty(name, transform.name)
	stage, err := filter.NewStage(filter.StageConfig{
		Name:   name,
		Detail: mediaTransformDetail(transform),
		Filter: frameFilter,
		Result: filterResultForStream(outputStream),
	})
	if err != nil {
		frameFilter.Close()
		return nil, av.Stream{}, err
	}
	return stage, outputStream, nil
}

func prepareBranchComposePlan(plan branchComposePlan) ([]branchComposeRoute, []branchComposeTargetRoute, error) {
	if len(plan.Branches) == 0 {
		return nil, nil, branchComposePlanEmptyError("branches")
	}
	if len(plan.Destinations) == 0 {
		return nil, nil, branchComposePlanEmptyError("destinations")
	}
	branches, err := branchComposeRoutes(plan)
	if err != nil {
		return nil, nil, err
	}
	outputs, err := branchComposeDestinations(plan, branches)
	if err != nil {
		return nil, nil, err
	}
	return branches, outputs, nil
}

func branchComposePlanEmptyError(kind string) error {
	suggestions := []string{
		"add at least one branch with a selector and encoder",
		"add at least one target destination",
		"use goav.From(input).Video().Decode().Branches(goav.Branch(name).Encode(codec.VP9(...)).To(output)) for the recipe API",
	}
	reason := "branch composition has no " + kind
	return &BuildError{
		Code:        "branch_compose_plan_empty",
		Operation:   "build branch composition",
		Node:        kind,
		Reason:      reason,
		Suggestions: suggestions,
		Cause:       ErrUnsupportedBuild,
	}
}

func branchComposeSelectorGroups(branches []branchComposeRoute) []branchComposeSelectorGroup {
	groups := make([]branchComposeSelectorGroup, 0, len(branches))
	index := make(map[string]int, len(branches))
	for i := range branches {
		key := branchComposeSelectorGroupKey(branches[i].branch.Selector, branches[i].branch.Input)
		groupIndex, ok := index[key]
		if !ok {
			groupIndex = len(groups)
			index[key] = groupIndex
			groups = append(groups, branchComposeSelectorGroup{
				selector: branches[i].branch.Selector,
				input:    branches[i].branch.Input,
			})
		}
		groups[groupIndex].branches = append(groups[groupIndex].branches, i)
	}
	return groups
}

func resolveBranchComposeStreamGroups(streams []av.Stream, branches []branchComposeRoute) ([]branchComposeStreamGroup, error) {
	return resolveBranchComposeStreamGroupsForInputs(mediaPlanCompiledSources{
		streams:      streams,
		streamGroups: [][]av.Stream{streams},
	}, nil, branches)
}

// resolveBranchComposeStreamGroupsForInputs binds every branch to one concrete
// stream. A single input keeps the legacy flat selection; with several inputs
// the branch selects across the union of all input streams — narrowed to one
// input by goav.InputName — and ambiguity fails with the candidate list.
func resolveBranchComposeStreamGroupsForInputs(sources mediaPlanCompiledSources, inputs []InputSpec, branches []branchComposeRoute) ([]branchComposeStreamGroup, error) {
	multi := len(sources.streamGroups) > 1
	var sets []inputStreamSet
	if multi || branchComposeRoutesNarrowed(branches) {
		sets = runtimeInputStreamSets(inputs, sources.streamGroups)
	}
	groups := make([]branchComposeStreamGroup, 0, len(branches))
	index := make(map[string]int, len(branches))
	for i := range branches {
		var stream av.Stream
		var err error
		if sets != nil {
			selected, ok, selectErr := selectStreamAcrossInputSets(sets, branches[i].branch.Selector, branches[i].branch.Input)
			if selectErr != nil {
				return nil, selectErr
			}
			if !ok {
				return nil, streamSelectionError("stream_missing", branches[i].branch.Selector, sources.streams)
			}
			stream = selected.stream
		} else {
			stream, err = selectDecodeStream(sources.streams, branches[i].branch.Selector)
			if branches[i].sourceDomain == shape.DomainFrame {
				stream, err = selectStream(sources.streams, branches[i].branch.Selector)
			}
			if err != nil {
				return nil, err
			}
		}
		key := branchComposeStreamKey(stream) + "\x00@" + branches[i].branch.Input
		groupIndex, ok := index[key]
		if !ok {
			groupIndex = len(groups)
			index[key] = groupIndex
			groups = append(groups, branchComposeStreamGroup{
				selector: branches[i].branch.Selector,
				input:    branches[i].branch.Input,
				stream:   stream,
			})
		}
		groups[groupIndex].branches = append(groups[groupIndex].branches, i)
	}
	return groups, nil
}

func branchComposeRoutesNarrowed(branches []branchComposeRoute) bool {
	for i := range branches {
		if branches[i].branch.Input != "" {
			return true
		}
	}
	return false
}

// runtimeInputStreamSets attributes the compiled per-input stream groups with
// the input names and media domains, for union selection at lowering time.
func runtimeInputStreamSets(inputs []InputSpec, groups [][]av.Stream) []inputStreamSet {
	sets := make([]inputStreamSet, 0, len(groups))
	for i := range groups {
		set := inputStreamSet{
			name:    fmt.Sprintf("input-%d", i),
			domain:  shape.DomainPacket,
			streams: groups[i],
			known:   true,
		}
		if i < len(inputs) {
			set.name = inputs[i].inputName(fmt.Sprintf("input-%d", i))
			if spec, ok := customSourceShape(inputs[i]); ok && spec.Domain != "" {
				set.domain = spec.Domain
			}
		}
		sets = append(sets, set)
	}
	return sets
}

func branchComposeSelectorKey(selector av.StreamSelector) string {
	return strings.Join([]string{
		string(selector.ID),
		strconv.FormatInt(int64(selector.Index), 10),
		strconv.FormatBool(selector.UseIndex),
		string(selector.Type),
		string(selector.Codec),
		selector.Name,
	}, "\x00")
}

// branchComposeSelectorGroupKey keys input nodes by selector AND input
// narrowing so two same-selector chains narrowed to different inputs get their
// own select/decode nodes.
func branchComposeSelectorGroupKey(selector av.StreamSelector, input string) string {
	return branchComposeSelectorKey(selector) + "\x00@" + input
}

// branchComposeInputNodeName suffixes a select/decode node name with the input
// narrowing so same-selector chains on different inputs stay distinct nodes.
func branchComposeInputNodeName(name string, input string) string {
	if input == "" {
		return name
	}
	return name + "@" + input
}

func branchComposeStreamKey(stream av.Stream) string {
	return strings.Join([]string{
		string(stream.ID),
		strconv.FormatInt(int64(stream.Index), 10),
		string(stream.Type),
		strconv.FormatUint(uint64(stream.Epoch), 10),
	}, "\x00")
}

func branchComposeRoutes(plan branchComposePlan) ([]branchComposeRoute, error) {
	branches := make([]branchComposeRoute, len(plan.Branches))
	names := make(map[string]struct{}, len(plan.Branches))
	for i := range plan.Branches {
		branch := plan.Branches[i]
		name := runtimeBranchComposeBranchName(branch, i, len(plan.Branches))
		if _, ok := names[name]; ok {
			return nil, branchComposeDuplicateBranchError(name, i)
		}
		names[name] = struct{}{}
		sharedOperations := branchComposeSharedRouteOperations(branch)
		privateOperations, err := branchComposeRouteOperations(name, branch)
		if err != nil {
			return nil, err
		}

		config := cloneEncodeConfig(branch.Encode)
		if config.Stream.ID == "" {
			config.Stream.ID = av.StreamID(name)
		}
		if config.Stream.Name == "" {
			config.Stream.Name = name
		}
		if config.Stream.Metadata == nil && branch.Metadata != nil {
			config.Stream.Metadata = branch.Metadata
		}
		// The implicit branch name "main" names its encode node from the selector
		// (empty request name), so a single Branch("main") composition lowers
		// identically to a direct chain (NORTH_STAR #2); explicit names stay for
		// multi-branch disambiguation.
		encodeName := name
		if encodeName == "main" {
			encodeName = ""
		}
		branches[i] = branchComposeRoute{
			name:              name,
			branch:            branch,
			copy:              branch.Copy,
			decode:            cloneCodecSpec(branch.DecodeConfig),
			codecChange:       branch.CodecChange,
			dropDecodeEvents:  false,
			sharedOperations:  cloneOperationSpecs(sharedOperations),
			privateOperations: cloneOperationSpecs(privateOperations),
			request: encodeRequest{
				name:     encodeName,
				selector: branch.Selector,
				config:   config,
			},
		}
	}
	return branches, nil
}

func branchComposeSharedRouteOperations(branch branchComposeBranch) []OperationSpec {
	return cloneOperationSpecs(branch.SharedOperations)
}

func branchComposeRouteNeedsEncode(branch branchComposeRoute) bool {
	return branch.request.config.Parameters.ID != ""
}

func branchComposeRouteNeedsDecode(branch branchComposeRoute) bool {
	if branch.sourceDomain == shape.DomainFrame {
		return false
	}
	return !branch.copy
}

func branchComposeDecodedBranchIndices(indices []int, branches []branchComposeRoute) []int {
	decoded := make([]int, 0, len(indices))
	for _, index := range indices {
		if index < 0 || index >= len(branches) {
			continue
		}
		if branchComposeRouteNeedsDecode(branches[index]) {
			decoded = append(decoded, index)
		}
	}
	return decoded
}

func branchComposeGroupDecodeConfig(indices []int, branches []branchComposeRoute) (codec.CodecSpec, error) {
	var config codec.CodecSpec
	var owner string
	haveConfig := false
	for _, index := range indices {
		if index < 0 || index >= len(branches) {
			continue
		}
		candidate := cloneCodecSpec(branches[index].decode)
		if !codecSpecHasDecodeIntent(candidate) {
			continue
		}
		if !haveConfig {
			config = candidate
			owner = branches[index].name
			haveConfig = true
			continue
		}
		if !reflect.DeepEqual(config, candidate) {
			return codec.CodecSpec{}, branchComposeDecodeConfigConflictError(owner, branches[index].name)
		}
	}
	return config, nil
}

func branchComposeGroupCodecChangePolicy(indices []int, branches []branchComposeRoute) (CodecChangePolicy, error) {
	var policy CodecChangePolicy
	var owner string
	havePolicy := false
	for _, index := range indices {
		if index < 0 || index >= len(branches) {
			continue
		}
		candidate := branches[index].codecChange
		if !codecChangePolicySet(candidate) {
			continue
		}
		if !havePolicy {
			policy = candidate
			owner = branches[index].name
			havePolicy = true
			continue
		}
		if policy != candidate {
			return CodecChangePolicy{}, branchComposeCodecChangeConflictError(owner, branches[index].name)
		}
	}
	return policy, nil
}

func branchComposeGroupDropDecodeEvents(indices []int, branches []branchComposeRoute) bool {
	if len(indices) == 0 {
		return false
	}
	for _, index := range indices {
		if index < 0 || index >= len(branches) {
			return false
		}
		if !branches[index].dropDecodeEvents {
			return false
		}
	}
	return true
}

func codecSpecHasDecodeIntent(spec codec.CodecSpec) bool {
	return spec.ID != "" ||
		spec.Type != "" ||
		codecSpecHasParameters(spec) ||
		spec.Settings.Bitrate != 0 ||
		spec.Settings.Framerate != (av.Duration{}) ||
		spec.Settings.KeyframeInterval != 0 ||
		spec.Settings.Profile != "" ||
		spec.Settings.Level != "" ||
		spec.Settings.Control != nil
}

func branchComposeDecodeConfigConflictError(first string, second string) error {
	return &BuildError{
		Code:      "decode_config_conflict",
		Operation: "build branch composition",
		Node:      second,
		Reason:    "branches that share one decoder declared different decode configs",
		Details: []string{
			"first branch: " + first,
			"conflicting branch: " + second,
		},
		Suggestions: []string{
			"move shared decode config to the stream chain with .Decode(...)",
			"use the same decode config for branches that share a decoder",
		},
	}
}

func branchComposeCodecChangeConflictError(first string, second string) error {
	return &BuildError{
		Code:      "decode_policy_conflict",
		Operation: "build branch composition",
		Node:      second,
		Reason:    "branches that share one decoder declared different codec-change policies",
		Details: []string{
			"first branch: " + first,
			"conflicting branch: " + second,
		},
		Suggestions: []string{
			"use the same codec-change policy for branches that share a decoder",
			"split branches by stream selector when policies must differ",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeDuplicateBranchError(name string, index int) error {
	return &BuildError{
		Code:      "branch_duplicate",
		Operation: "build branch composition",
		Node:      name,
		Reason:    "branch name is defined more than once",
		Details: []string{
			"duplicate index: " + strconv.Itoa(index),
		},
		Suggestions: []string{
			"give each branch a stable unique name",
			"use distinct branch names when multiple branches share one selected stream",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeRouteOperations(name string, branch branchComposeBranch) ([]OperationSpec, error) {
	if len(branch.PrivateOperations) != 0 {
		return cloneOperationSpecs(branch.PrivateOperations), nil
	}
	return branchComposeInlineTransformOperations(name, branch)
}

func branchComposeRouteOperationTransformsForName(name string, operations []OperationSpec) ([]mediaTransform, error) {
	out := make([]mediaTransform, 0, len(operations))
	transformIndex := 0
	for i := range operations {
		step, err := branchComposeRouteOperationTransform(name, transformIndex, operations[i])
		if err != nil {
			return nil, err
		}
		if operations[i].Kind == OpTransform && (operations[i].Transform.Resize != nil || operations[i].Transform.Resample != nil) {
			transformIndex++
		}
		if !mediaTransformEmpty(step) {
			out = append(out, step)
		}
	}
	return out, nil
}

func branchComposeInlineTransformOperations(name string, branch branchComposeBranch) ([]OperationSpec, error) {
	if branch.Resize != nil && branch.Resample != nil {
		return nil, branchChainStepError(name, "branch cannot combine resize and resample in one step")
	}
	switch {
	case branch.Resize != nil:
		return []OperationSpec{operationSpecForTransform(TransformSpec{Resize: branch.Resize})}, nil
	case branch.Resample != nil:
		return []OperationSpec{operationSpecForTransform(TransformSpec{Resample: branch.Resample})}, nil
	default:
		return nil, nil
	}
}

func branchComposeRouteOperationTransform(branchName string, transformIndex int, operation OperationSpec) (mediaTransform, error) {
	suffix := ""
	if transformIndex > 0 {
		suffix = "-" + strconv.Itoa(transformIndex+1)
	}
	switch operation.Kind {
	case OpStage:
		if operation.Stage == nil {
			return mediaTransform{}, branchChainStepError(branchName, "branch stage operation has no stage")
		}
		return mediaTransform{
			name:  operation.Stage.Name(),
			stage: operation.Stage,
		}, nil
	case OpTransform:
		if operation.Transform.Resize != nil && operation.Transform.Resample != nil {
			return mediaTransform{}, branchChainStepError(branchName, "branch transform operation cannot combine resize and resample")
		}
		if operation.Transform.Resize != nil {
			resize := *operation.Transform.Resize
			return mediaTransform{
				name:    "resize-" + branchName + suffix,
				factory: filter.FactoryResize,
				video:   &resize,
			}, nil
		}
		if operation.Transform.Resample != nil {
			resample := *operation.Transform.Resample
			return mediaTransform{
				name:    "resample-" + branchName + suffix,
				factory: filter.FactoryResample,
				audio:   &resample,
			}, nil
		}
		return mediaTransform{}, branchChainStepError(branchName, "branch transform operation is empty")
	default:
		return mediaTransform{}, nil
	}
}

func mediaTransformEmpty(transform mediaTransform) bool {
	return transform.name == "" &&
		transform.factory == "" &&
		transform.stage == nil &&
		transform.video == nil &&
		transform.audio == nil
}

func branchComposeRouteStageOperationCount(operations []OperationSpec) int {
	count := 0
	for i := range operations {
		switch operations[i].Kind {
		case OpStage:
			if operations[i].Stage != nil {
				count++
			}
		case OpTransform:
			if operations[i].Transform.Resize != nil || operations[i].Transform.Resample != nil {
				count++
			}
		}
	}
	return count
}

func branchComposeRouteStageOperations(operations []OperationSpec) []OperationSpec {
	if len(operations) == 0 {
		return nil
	}
	out := make([]OperationSpec, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case OpStage:
			if operation.Stage != nil {
				out = append(out, operation)
			}
		case OpTransform:
			if operation.Transform.Resize != nil || operation.Transform.Resample != nil {
				out = append(out, operation)
			}
		}
	}
	return cloneOperationSpecs(out)
}

func branchComposeRouteOperationsKey(operations []OperationSpec) string {
	return mediaTransformsKeyFromOperations(branchComposeRouteStageOperations(operations))
}

func mediaTransformsKeyFromOperations(operations []OperationSpec) string {
	if len(operations) == 0 {
		return ""
	}
	transforms, err := branchComposeRouteOperationTransformsForName("", operations)
	if err != nil {
		return ""
	}
	return mediaTransformsKey(transforms)
}

func branchComposeSharedOperationName(branch branchComposeRoute) string {
	return branchComposeSharedStepName(branch.branch)
}

func branchComposePrivateOperationTransforms(branch branchComposeRoute) ([]mediaTransform, error) {
	// The implicit "main" branch names its private transform nodes from the
	// selector scope, matching a direct chain (NORTH_STAR #2); explicit names keep
	// their name for multi-branch disambiguation.
	name := branch.name
	if name == "main" {
		name = branchComposeSharedStepName(branch.branch)
	}
	return branchComposeRouteOperationTransformsForName(name, branch.privateOperations)
}

func branchChainStepError(name string, reason string) error {
	return &BuildError{
		Code:      "branch_operation_chain_unsupported",
		Operation: "build branch composition",
		Node:      name,
		Reason:    reason,
		Suggestions: []string{
			"use one operation per branch call",
			"use resize on video branches and resample on audio branches",
			"use goav.Branch(name).Do(stage).Resize(...).Encode(codec.VP9(...)).To(output) for recipe branch operations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeOperationStageCount(branches []branchComposeRoute) int {
	count := 0
	for i := range branches {
		count += branchComposeRouteStageOperationCount(branches[i].sharedOperations) +
			branchComposeRouteStageOperationCount(branches[i].privateOperations)
	}
	return count
}

func branchComposeSharedStepName(branch branchComposeBranch) string {
	return firstNonEmpty(string(branch.Selector.Type), string(branch.Selector.ID), branch.Selector.Name, "stream")
}

func branchComposeSharedStepGroups(indices []int, branches []branchComposeRoute) []branchComposeSharedStepGroup {
	groups := make([]branchComposeSharedStepGroup, 0, len(indices))
	positions := make(map[string]int, len(indices))
	for _, index := range indices {
		key := branchComposeRouteOperationsKey(branches[index].sharedOperations)
		position, ok := positions[key]
		if !ok {
			position = len(groups)
			positions[key] = position
			groups = append(groups, branchComposeSharedStepGroup{
				operations: cloneOperationSpecs(branches[index].sharedOperations),
			})
		}
		groups[position].branches = append(groups[position].branches, index)
	}
	return groups
}

func branchComposeRuntimeSharedStepGroups(branches []branchComposeRoute, inputs []pipeline.NodeRef, streams []av.Stream) []branchComposeSharedStepGroup {
	groups := make([]branchComposeSharedStepGroup, 0, len(branches))
	positions := make(map[string]int, len(branches))
	for i := range branches {
		key := strings.Join([]string{
			string(inputs[i]),
			string(streams[i].ID),
			string(streams[i].Type),
			string(streams[i].Codec.ID),
			branchComposeRouteOperationsKey(branches[i].sharedOperations),
		}, "\x00")
		position, ok := positions[key]
		if !ok {
			position = len(groups)
			positions[key] = position
			groups = append(groups, branchComposeSharedStepGroup{
				operations: cloneOperationSpecs(branches[i].sharedOperations),
			})
		}
		groups[position].branches = append(groups[position].branches, i)
	}
	return groups
}

func mediaTransformsKey(steps []mediaTransform) string {
	if len(steps) == 0 {
		return ""
	}
	keys := make([]string, 0, len(steps))
	for i := range steps {
		keys = append(keys, mediaTransformKey(steps[i]))
	}
	return strings.Join(keys, "\x01")
}

func mediaTransformKey(step mediaTransform) string {
	parts := []string{step.name, step.factory}
	if step.stage != nil {
		parts = append(parts, "stage", step.stage.Name())
	}
	if step.video != nil {
		parts = append(parts, "resize", strconv.Itoa(step.video.Width), strconv.Itoa(step.video.Height), string(step.video.Mode), step.video.PixelFormat)
	}
	if step.audio != nil {
		parts = append(parts, "resample", strconv.Itoa(step.audio.SampleRate), strconv.Itoa(step.audio.Channels), step.audio.ChannelLayout, step.audio.SampleFormat)
	}
	return strings.Join(parts, "\x02")
}

func applyMediaTransformToStream(stream av.Stream, transform mediaTransform) (av.Stream, error) {
	out := stream
	switch {
	case transform.audio != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resample", "audio")
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
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resize", "video")
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

func mediaTransformMismatchError(transform mediaTransform, stream av.Stream, operation string, media string) error {
	details := []string{
		"stream id: " + string(stream.ID),
		"stream type: " + string(stream.Type),
		"codec type: " + string(stream.Codec.Type),
	}
	return &BuildError{
		Code:      "branch_transform_media_mismatch",
		Operation: "build branch composition",
		Node:      transform.name,
		Reason:    operation + " applies to " + media + " streams",
		Details:   details,
		Suggestions: []string{
			"use resize on video branches",
			"use resample on audio branches",
			"check the branch selector",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeDestinations(plan branchComposePlan, branches []branchComposeRoute) ([]branchComposeTargetRoute, error) {
	outputs := make([]branchComposeTargetRoute, len(plan.Destinations))
	for i := range plan.Destinations {
		output := plan.Destinations[i]
		if output.Sink != nil && branchComposeTargetHasMuxDestination(output) {
			return nil, branchComposeTargetDestinationInvalidError(output, "target cannot configure both a sink and a mux destination")
		}
		target := branchComposeFormatTarget(plan, output)
		matches := branchComposeTargetMatches(output, branches)
		if len(matches) == 0 {
			return nil, branchComposeTargetUnmatchedError(output, target)
		}
		if output.Sink == nil {
			for _, branchIndex := range matches {
				if !branchComposeRouteNeedsEncode(branches[branchIndex]) && !branches[branchIndex].copy {
					return nil, branchComposeTargetEncodeMissingError(output, target, branches[branchIndex])
				}
			}
		}
		outputs[i] = branchComposeTargetRoute{
			output:  output,
			target:  target,
			sink:    output.Sink,
			matches: matches,
		}
	}
	return outputs, nil
}

func branchComposeTargetHasMuxDestination(output branchComposeTarget) bool {
	if !destinationSpecEmpty(output.Destination) && output.Destination.sink == nil {
		return true
	}
	return output.Target.Name != "" ||
		output.Target.URI != "" ||
		output.Target.Protocol != "" ||
		output.Target.Writer != nil ||
		output.Target.MIMEType != "" ||
		output.Format != "" ||
		output.OpenFormat() != ""
}

func branchComposeTargetUnmatchedError(output branchComposeTarget, destination format.Output) error {
	node := firstNonEmpty(output.Name, destination.Name, destination.URI, "output")
	details := make([]string, 0, 1)
	if len(output.Branches) != 0 {
		details = append(details, "requested: "+strings.Join(output.Branches, ", "))
	}
	return &BuildError{
		Code:      "branch_destination_unmatched",
		Operation: "build branch composition",
		Node:      node,
		Reason:    "destination selects no branches",
		Details:   details,
		Suggestions: []string{
			"reference a branch name",
			"reference a destination name listed on the branch",
			"omit explicit branch filters when the destination should receive every branch",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeTargetDestinationInvalidError(output branchComposeTarget, reason string) error {
	return &BuildError{
		Code:      "branch_destination_invalid",
		Operation: "build branch composition",
		Node:      branchComposeTargetNodeName(output, "output"),
		Reason:    reason,
		Suggestions: []string{
			"use goav.Sink(sink) for frame or packet sink destinations",
			"use goav.File(...) or goav.URIOut(...) for muxed destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeTargetEncodeMissingError(output branchComposeTarget, destination format.Output, branch branchComposeRoute) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: "build branch composition",
		Node:      firstNonEmpty(branch.name, branch.branch.Name, branchComposeTargetNodeName(output, "output")),
		Reason:    "muxed destinations require encoded branches",
		Details: []string{
			"destination: " + firstNonEmpty(output.Name, destination.Name, destination.URI, "output"),
		},
		Suggestions: []string{
			"encode the branch before routing it to a mux destination",
			"route raw decoded branches to goav.Sink(sink)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchComposeTargetNodeName(output branchComposeTarget, fallback string) string {
	name := firstNonEmpty(output.Name, output.Target.Name, output.Target.URI)
	if name != "" {
		return name
	}
	if output.Sink != nil && output.Sink.Name() != "" {
		return output.Sink.Name()
	}
	return fallback
}

func branchComposeTargetSinkNodeName(output branchComposeTargetRoute, index int) string {
	if output.sink != nil && output.sink.Name() != "" {
		return output.sink.Name()
	}
	if output.output.Name != "" {
		return output.output.Name
	}
	if index == 0 {
		return "sink"
	}
	return "sink-" + strconv.Itoa(index)
}

func branchComposeTargetOpenFormat(output branchComposeTarget) av.FormatID {
	return output.OpenFormat()
}

func runtimeBranchComposeBranchName(branch branchComposeBranch, index int, total int) string {
	if branch.Name != "" {
		return branch.Name
	}
	if total == 1 {
		return "branch"
	}
	return "branch-" + strconv.Itoa(index+1)
}

func branchComposeFormatTarget(plan branchComposePlan, output branchComposeTarget) format.Output {
	target := output.Target
	if !destinationSpecEmpty(output.Destination) {
		target = output.Destination.output
	}
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

func branchComposeTargetMatches(output branchComposeTarget, branches []branchComposeRoute) []int {
	if len(output.Branches) == 0 {
		matches := make([]int, len(branches))
		for i := range branches {
			matches[i] = i
		}
		return matches
	}

	matches := make([]int, 0, len(output.Branches))
	for i := range branches {
		if branchComposeTargetSelectsRoute(output, branches[i]) {
			matches = append(matches, i)
		}
	}
	return matches
}

func branchComposeTargetSelectsRoute(output branchComposeTarget, branch branchComposeRoute) bool {
	for i := range output.Branches {
		name := output.Branches[i]
		if name == branch.name || name == branch.branch.Name {
			return true
		}
		for j := range branch.branch.Labels {
			if name == branch.branch.Labels[j] {
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
