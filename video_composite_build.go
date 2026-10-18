package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// compositeRegion is an arm's top-left placement on the composite canvas. It is
// composite-only — it has no meaning outside a Composite arm.
type compositeRegion struct {
	x, y int
}

// compositeJoinProfile is Composite's entry in the join table: video arms,
// explicit packet-arm decode, per-arm Region layout collection,
// videoCompositeStage convergence, and an encodable output stream derived from
// the Region bounding box over every arm's frame shape.
var compositeJoinProfile = joinProfile{
	placesArms: true,
	media:      av.MediaVideo,
	decodeArms: true,
	graphBuffer: &pipeline.BufferPolicy{
		Capacity: realtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	},
	graphBufferRealtimeOnly: true,
	// Each arm places its frame on the canvas at its declared Region — a chain
	// arm's .Region(x, y) or a nested composite's — defaulting to the top-left
	// corner {0,0}.
	planArm: func(p *joinPlan, arm joinArmSnapshot, _ av.Stream) (*joinArmStagePlan, error) {
		layout := compositeLayout{}
		if arm.region != nil {
			layout = compositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		p.layouts = append(p.layouts, layout)
		return nil, nil
	},
	// The canvas takes each arm at its own size; an arm whose format is known
	// only once it opens converts to I420 at run time.
	conformTarget: func(*joinPlan, av.Stream) shape.Spec {
		return shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420))
	},
	newStage: func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
		return newVideoCompositeStageWithPreallocAndPools(p.name, armIDs, av.StreamID(p.name), p.layouts, p.tree.sync, joinStagePreallocDepth(p.runtime), runtimeMediaPools(p.runtime)), nil
	},
	// The output id is the join's planned node name (composite, or composite-2
	// when nested); the geometry facts are the full Region bounding box over
	// all arms, not the first arm's tile.
	joinedStream: func(p *joinPlan) av.Stream {
		spec := compositeJoinedOutputShape(p)
		return av.Stream{
			ID:   av.StreamID(p.name),
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				Type:        av.MediaVideo,
				Width:       spec.Width,
				Height:      spec.Height,
				PixelFormat: spec.PixelFormat,
			},
		}
	},
	sinkOnlyReason: "composite to a non-Sink destination requires .Encode(...)",
	sinkOnlySuggestions: []string{
		"encode the composited video first: goav.Composite(a, b).Encode(codec.VP8(codec.Bitrate(1_000_000))).To(out)",
		"deliver raw composited frames with .To(goav.Sink(sink)) instead",
	},
}

func compositeJoinedOutputShape(p *joinPlan) shape.Spec {
	if p == nil || len(p.arms) == 0 {
		return shape.Frame(av.MediaVideo)
	}
	spec := shape.Frame(av.MediaVideo, shape.Stream(av.StreamID(p.name)))
	canvasW, canvasH := 0, 0
	missingGeometry := false
	missingPixelFormat := false
	unsupportedPixelFormat := ""
	for i := range p.arms {
		arm := compositeArmFrameShape(p.arms[i])
		if arm.Width <= 0 || arm.Height <= 0 {
			missingGeometry = true
		} else {
			layout := compositeArmLayout(p, i)
			canvasW = max(canvasW, layout.X+arm.Width)
			canvasH = max(canvasH, layout.Y+arm.Height)
		}
		switch arm.PixelFormat {
		case "":
			missingPixelFormat = true
		case av.PixelFormatI420:
		default:
			if unsupportedPixelFormat == "" {
				unsupportedPixelFormat = arm.PixelFormat
			}
		}
	}
	if !missingGeometry {
		spec.Width = canvasW
		spec.Height = canvasH
	}
	switch {
	case missingPixelFormat:
		spec.PixelFormat = ""
	case unsupportedPixelFormat != "":
		spec.PixelFormat = unsupportedPixelFormat
	default:
		spec.PixelFormat = av.PixelFormatI420
	}
	return spec
}

func compositeArmFrameShape(arm joinArmPlan) shape.Spec {
	if arm.sub != nil && !arm.late {
		return shape.FromStream(arm.sub.joined, arm.sub.joinedDomain)
	}
	return shape.FromStream(arm.joinStream(), shape.DomainFrame)
}

func compositeArmLayout(p *joinPlan, index int) compositeLayout {
	if p == nil || index < 0 || index >= len(p.layouts) {
		return compositeLayout{}
	}
	return p.layouts[index]
}
