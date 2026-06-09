package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Composite overlays N synchronized video source-chains into one stream delivered
// to a Sink — the video dual of Mix (N→1) and the convergent dual of Branches.
// Each arm is a source chain such as From(frameSource).Video(); arms must have
// distinct stream ids and I420 video format, and each may declare its canvas
// position with .Region(x, y). This reuses the existing Job, so .To/Build/Run are
// unchanged. (First slice: frame sources to a Sink; decode/encode arms build on
// the same OpJoin mechanism as Mix — see docs/MULTI_INPUT.md.)
func Composite(arms ...*jobStreamBuilder) *compositeStream {
	return &compositeStream{arms: arms}
}

type compositeStream struct {
	arms   []*jobStreamBuilder
	encode *CodecSpec
}

type compositeSpec struct {
	arms   []*jobStreamBuilder
	dest   Destination
	encode *CodecSpec
}

// compositeRegion is an arm's top-left placement on the composite canvas. It is
// composite-only — it has no meaning outside a Composite arm.
type compositeRegion struct {
	x, y int
}

// Encode encodes the composited stream before the destination, so a Composite can
// record to a File/mux (not only a frame Sink). Without it the composite delivers
// frames.
func (c *compositeStream) Encode(spec CodecSpec) *compositeStream {
	c.encode = &spec
	return c
}

// To delivers the composited stream to a destination and returns a Job, so the
// composite runs through the same Build/Run as every other recipe.
func (c *compositeStream) To(dest Destination) *Job {
	job := &Job{name: "composite", runtime: Default()}
	if len(c.arms) < 2 {
		job.setErr(&BuildError{Code: "composite_inputs", Operation: "build composite", Node: "composite", Reason: "composite requires at least two source arms", Cause: ErrUnsupportedBuild})
		return job
	}
	job.composite = &compositeSpec{arms: c.arms, dest: dest, encode: c.encode}
	return job
}

func (j *Job) buildComposite(ctx context.Context) (Task, error) {
	if j.err != nil {
		return nil, j.err
	}
	rt, ok := j.runtime.(*runtime)
	if !ok {
		return nil, ErrExpertRuntimeRequired
	}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "goav-composite", Buffer: rt.buffer, Realtime: rt.realtime})
	if err != nil {
		return nil, err
	}
	armRefs := make([]string, 0, len(j.composite.arms))
	armIDs := make([]av.StreamID, 0, len(j.composite.arms))
	layouts := make([]compositeLayout, 0, len(j.composite.arms))
	seen := make(map[av.StreamID]struct{}, len(j.composite.arms))
	service := &builder{runtime: rt}
	for i := range j.composite.arms {
		arm := j.composite.arms[i]
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			graph.Close()
			return nil, &BuildError{Code: "composite_arm", Operation: "build composite", Node: "composite", Reason: "each composite arm must be a single-input source chain", Cause: ErrUnsupportedBuild}
		}
		source, streams, domain, err := arm.job.inputs[0].openGraphSource(ctx, service, i)
		if err != nil {
			graph.Close()
			return nil, err
		}
		stream, err := selectStream(streams, av.StreamSelector{Type: av.MediaVideo})
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		id := stream.ID
		if _, dup := seen[id]; dup {
			source.Close()
			graph.Close()
			return nil, &BuildError{Code: "composite_arm", Operation: "build composite", Node: string(id), Reason: "composite arms must have distinct stream ids", Cause: ErrUnsupportedBuild}
		}
		seen[id] = struct{}{}
		srcRef, err := graph.AddSource(source, rt.buffer)
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		upstream := string(srcRef)
		// Packet-domain arms decode to frames before the composite (auto-inserted —
		// the compositor overlays decoded video). Frame-domain arms feed it directly.
		if domain == shape.DomainPacket {
			request := decodeRequest{selector: av.StreamSelector{Type: stream.Type}}
			decodeStage, err := service.newDecodeStageNamed(ctx, "composite-decode-"+string(id), request, stream, rt.realtime, true, codec.DecodeBounds{})
			if err != nil {
				graph.Close()
				return nil, err
			}
			decodeRef, err := graph.AddStage(decodeStage, rt.buffer)
			if err != nil {
				graph.Close()
				return nil, err
			}
			if err := graph.Connect(pipeline.Route{From: upstream, To: []string{string(decodeRef)}, Policy: pipeline.RouteAll}); err != nil {
				graph.Close()
				return nil, err
			}
			upstream = string(decodeRef)
		}
		// Each arm places its frame on the canvas at its declared Region; an arm
		// without one defaults to the top-left corner {0,0}.
		layout := compositeLayout{}
		if arm.region != nil {
			layout = compositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		armRefs = append(armRefs, upstream)
		armIDs = append(armIDs, id)
		layouts = append(layouts, layout)
	}
	compositeRef, err := graph.AddStage(newVideoCompositeStage("composite", armIDs, av.StreamID("composite"), layouts), rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(compositeRef)}, Policy: pipeline.RouteAll}); err != nil {
			graph.Close()
			return nil, err
		}
	}
	if j.composite.encode != nil {
		shape, _ := customSourceShape(j.composite.arms[0].job.inputs[0])
		compositedStream := av.Stream{
			ID:   av.StreamID("composite"),
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				Type:        av.MediaVideo,
				Width:       shape.Width,
				Height:      shape.Height,
				PixelFormat: shape.PixelFormat,
			},
		}
		request := encodeRequest{name: "composite-encode", selector: av.StreamSelector{Type: av.MediaVideo}, config: encodeConfigFromSpec(*j.composite.encode)}
		config, encodedStream, err := prepareEncodeConfig(compositedStream, request, rt.realtime)
		if err != nil {
			graph.Close()
			return nil, err
		}
		service := &builder{runtime: rt}
		if err := compileEncodeDestinationPath(ctx, service, graph, compositeRef, request, config, encodedStream, []destinationSpec{j.composite.dest.spec}); err != nil {
			graph.Close()
			return nil, err
		}
		// openMuxDestinationStage records the destination transaction (file commit/
		// abort) on service; carry it to the task so the composite file finalizes.
		return newTask(graph, rt, service.destinationTxs...), nil
	}
	sink := j.composite.dest.spec.sink
	if sink == nil {
		graph.Close()
		return nil, &BuildError{Code: "composite_destination", Operation: "build composite", Node: "composite", Reason: "composite to a non-Sink destination requires .Encode(...)", Cause: ErrUnsupportedBuild}
	}
	sinkRef, err := graph.AddSink(sink, rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	if err := graph.Connect(pipeline.Route{From: string(compositeRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll}); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, rt), nil
}
