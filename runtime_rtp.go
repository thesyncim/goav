package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

type rtpBuild struct {
	source       *rtpav.Source
	streams      []av.Stream
	decodeBounds codec.DecodeBounds
}

type rtpRecordGraphCompiler struct{}

func WithRTPName(name string) RTPOption {
	return func(input *rtpInput) {
		input.name = name
	}
}

func WithRTPFeedback(feedback rtpav.FeedbackWriter) RTPOption {
	return func(input *rtpInput) {
		input.feedback = feedback
	}
}

func WithRTPJitter(jitter rtpav.JitterBuffer) RTPOption {
	return func(input *rtpInput) {
		input.jitter = jitter
	}
}

func WithRTPDepacketizers(depacketizers ...rtpav.Depacketizer) RTPOption {
	return func(input *rtpInput) {
		for i := range depacketizers {
			if depacketizers[i] != nil {
				input.depacketizers = append(input.depacketizers, depacketizers[i])
			}
		}
	}
}

func WithRTPBufferLimits(limits RTPBufferLimits) RTPOption {
	return func(input *rtpInput) {
		input.limits = limits
	}
}

// WithRTPDecodeBounds seeds codec.DecodeConfig.Bounds for high-level RTP decode
// builders using this packet reader.
func WithRTPDecodeBounds(bounds codec.DecodeBounds) RTPOption {
	return func(input *rtpInput) {
		input.decodeBounds = bounds
	}
}

func WithRTPMaxTimestampGap(gap av.Duration) RTPOption {
	return func(input *rtpInput) {
		input.maxTSGap = gap
	}
}

func (rtpRecordGraphCompiler) match(b *builder) bool {
	return len(b.rtpInputs) > 0 &&
		len(b.outputs) > 0 &&
		len(b.inputs) == 0 &&
		len(b.decodes) == 0 &&
		len(b.encodes) == 0 &&
		len(b.filters) == 0 &&
		len(b.transcodes) == 0 &&
		!b.hasExplicitGraph()
}

func (rtpRecordGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planRTPRecord(spec)
}

func (rtpRecordGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildRTPRecord(ctx)
}

func (b *builder) planRTPRecord(spec pipeline.Spec) (pipeline.Spec, error) {
	nodes := make(map[string]plannedNode, len(b.rtpInputs)+len(b.outputs))
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, rtpInputDetail(b.rtpInputs[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}
	stageRefs := make([]pipeline.NodeRef, len(b.outputs))
	for i := range b.outputs {
		stageName := muxNodeName(b.outputs[i], i)
		stageRef := pipeline.NodeRef(stageName)
		if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stageRef, outputNodeDetailWithFormat(b.outputs[i], b.outputFormat(i))); err != nil {
			return pipeline.Spec{}, err
		}
		stageRefs[i] = stageRef
	}
	for i := range sourceRefs {
		for j := range stageRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   sourceRefs[i],
				To:     stageRefs[j],
				Policy: pipeline.RouteAll,
			})
		}
	}
	return spec, nil
}

func (b *builder) buildRTPRecord(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileRTPRecord(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return &task{graph: graph}, nil
}

func (b *builder) compileRTPRecord(ctx context.Context, graph pipeline.Graph) error {
	sourceRefs := make([]pipeline.NodeRef, 0, len(b.rtpInputs))
	streams := make([]av.Stream, 0, len(b.rtpInputs))
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
	}

	for i := range b.outputs {
		stage, err := b.openMuxStage(ctx, b.outputs[i], i, streams)
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(stage, b.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		for j := range sourceRefs {
			if err := connectRefs(graph, sourceRefs[j], stageRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *builder) openRTPSource(ctx context.Context, input rtpInput, index int) (rtpBuild, error) {
	if input.receiver == nil {
		return rtpBuild{}, ErrNilSource
	}
	streams, err := input.receiver.Streams(ctx)
	if err != nil {
		return rtpBuild{}, err
	}
	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Name:            rtpNodeName(input, index),
		Detail:          rtpInputDetail(input),
		Receiver:        input.receiver,
		Feedback:        input.feedback,
		Jitter:          input.jitter,
		Depacketizers:   input.depacketizers,
		Streams:         streams,
		MaxTimestampGap: input.maxTSGap,
		MaxReady:        input.limits.MaxReady,
		MaxEvents:       input.limits.MaxEvents,
		MaxFeedback:     input.limits.MaxFeedback,
		MaxPackets:      input.limits.MaxPackets,
	})
	if err != nil {
		return rtpBuild{}, err
	}
	return rtpBuild{source: source, streams: streams, decodeBounds: input.decodeBounds}, nil
}

func rtpNodeName(input rtpInput, index int) string {
	if input.name != "" {
		return input.name
	}
	if index > 0 {
		return "rtp-" + strconv.Itoa(index)
	}
	return "rtp"
}

func rtpDecodeBoundsForStream(stream av.Stream, builds []rtpBuild) codec.DecodeBounds {
	var bounds codec.DecodeBounds
	for i := range builds {
		if !rtpDecodeBoundsConfigured(builds[i].decodeBounds) {
			continue
		}
		for j := range builds[i].streams {
			if !sameDecodeBoundsStream(stream, builds[i].streams[j]) {
				continue
			}
			bounds = maxDecodeBounds(bounds, builds[i].decodeBounds)
			break
		}
	}
	return bounds
}

func rtpDecodeBoundsConfigured(bounds codec.DecodeBounds) bool {
	return bounds.MaxFramesPerInput > 0 ||
		bounds.MaxEventsPerInput > 0 ||
		bounds.MaxRequestsPerInput > 0 ||
		bounds.MaxPayloadBytes > 0 ||
		bounds.MaxRetainedBytes > 0 ||
		bounds.MaxWidth > 0 ||
		bounds.MaxHeight > 0
}

func sameDecodeBoundsStream(a av.Stream, b av.Stream) bool {
	if a.ID != "" && b.ID != "" {
		return a.ID == b.ID
	}
	if a.Index != 0 || b.Index != 0 {
		return a.Index == b.Index
	}
	return a.Type == b.Type && a.Codec.ID == b.Codec.ID
}

func maxDecodeBounds(a codec.DecodeBounds, b codec.DecodeBounds) codec.DecodeBounds {
	if b.MaxFramesPerInput > a.MaxFramesPerInput {
		a.MaxFramesPerInput = b.MaxFramesPerInput
	}
	if b.MaxEventsPerInput > a.MaxEventsPerInput {
		a.MaxEventsPerInput = b.MaxEventsPerInput
	}
	if b.MaxRequestsPerInput > a.MaxRequestsPerInput {
		a.MaxRequestsPerInput = b.MaxRequestsPerInput
	}
	if b.MaxPayloadBytes > a.MaxPayloadBytes {
		a.MaxPayloadBytes = b.MaxPayloadBytes
	}
	if b.MaxRetainedBytes > a.MaxRetainedBytes {
		a.MaxRetainedBytes = b.MaxRetainedBytes
	}
	if b.MaxWidth > a.MaxWidth {
		a.MaxWidth = b.MaxWidth
	}
	if b.MaxHeight > a.MaxHeight {
		a.MaxHeight = b.MaxHeight
	}
	return a
}
