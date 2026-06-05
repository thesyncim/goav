package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

type rtpBuild struct {
	source  *rtpav.Source
	streams []av.Stream
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

func WithRTPDepacketizer(depacketizer rtpav.Depacketizer) RTPOption {
	return func(input *rtpInput) {
		if depacketizer != nil {
			input.depacketizers = append(input.depacketizers, depacketizer)
		}
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

func (rtpRecordGraphCompiler) match(b *builder) bool {
	return len(b.rtpInputs) == 1 &&
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
	nodes := make(map[string]plannedNode, 1+len(b.outputs))
	sourceName := rtpNodeName(b.rtpInputs[0])
	sourcePad := pipeline.PadRef{Node: sourceName, Pad: "out"}
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourcePad); err != nil {
		return pipeline.Spec{}, err
	}
	for i := range b.outputs {
		stageName := muxNodeName(b.outputs[i], i)
		stagePad := pipeline.PadRef{Node: stageName, Pad: "inout"}
		if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stagePad); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   sourcePad,
			To:     stagePad,
			Policy: pipeline.RouteAll,
		})
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
	rtpInput := b.rtpInputs[0]
	receiver, err := b.openRTPSource(ctx, rtpInput)
	if err != nil {
		return err
	}
	sourcePad, err := graph.AddSource(receiver.source, b.runtime.buffer)
	if err != nil {
		receiver.source.Close()
		return err
	}

	for i := range b.outputs {
		output := b.outputs[i]
		outputProbe, err := b.runtime.formats.Probe(ctx, outputProbeRequest(output))
		if err != nil {
			return err
		}
		muxFactory, err := b.runtime.formats.MuxerFactory(outputProbe.Format)
		if err != nil {
			return err
		}
		muxer, err := muxFactory.NewMuxer(ctx, outputProbe.Format)
		if err != nil {
			return err
		}
		if err := muxer.Open(ctx, output, receiver.streams, format.OpenOptions{
			Realtime: b.runtime.realtime || output.Realtime,
			Metadata: output.Metadata,
		}); err != nil {
			muxer.Close()
			return err
		}
		stage, err := format.NewMuxStage(format.MuxStageConfig{
			Name:            muxNodeName(output, i),
			Muxer:           muxer,
			Result:          format.WriteResult{Events: make([]av.Event, 0, 1)},
			DropInputEvents: true,
		})
		if err != nil {
			muxer.Close()
			return err
		}
		stagePad, err := graph.AddStage(stage, b.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		if err := graph.Link(pipeline.Link{From: sourcePad, To: stagePad}); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) openRTPSource(ctx context.Context, input rtpInput) (rtpBuild, error) {
	if input.receiver == nil {
		return rtpBuild{}, ErrNilSource
	}
	streams, err := input.receiver.Streams(ctx)
	if err != nil {
		return rtpBuild{}, err
	}
	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Name:          rtpNodeName(input),
		Receiver:      input.receiver,
		Feedback:      input.feedback,
		Jitter:        input.jitter,
		Depacketizers: input.depacketizers,
		MaxReady:      input.limits.MaxReady,
		MaxEvents:     input.limits.MaxEvents,
		MaxFeedback:   input.limits.MaxFeedback,
		MaxPackets:    input.limits.MaxPackets,
	})
	if err != nil {
		return rtpBuild{}, err
	}
	return rtpBuild{source: source, streams: streams}, nil
}

func rtpNodeName(input rtpInput) string {
	if input.name != "" {
		return input.name
	}
	return "rtp"
}
