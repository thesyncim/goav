package compile

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
	DecodeArms: true,
	graphBuffer: &pipeline.BufferPolicy{
		Capacity: RealtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	},
	graphBufferRealtimeOnly: true,
	// Each arm places its frame on the canvas at its declared Region — a chain
	// arm's .Region(x, y) or a nested composite's — defaulting to the top-left
	// corner {0,0}.
	planArm: func(p *JoinPlan, arm joinArmSnapshot, _ av.Stream) (*joinArmStagePlan, error) {
		layout := CompositeLayout{}
		if arm.region != nil {
			layout = CompositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		p.Layouts = append(p.Layouts, layout)
		return nil, nil
	},
	// The canvas takes each arm at its own size; an arm whose format is known
	// only once it opens converts to I420 at run time.
	ConformTarget: func(*JoinPlan, av.Stream) shape.Spec {
		return shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420))
	},
	// The output id is the join's planned node name (composite, or composite-2
	// when nested); the geometry facts are the full Region bounding box over
	// all arms, not the first arm's tile.
	joinedStream: func(p *JoinPlan) av.Stream {
		spec := compositeJoinedOutputShape(p)
		return av.Stream{
			ID:   av.StreamID(p.Name),
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

func compositeJoinedOutputShape(p *JoinPlan) shape.Spec {
	if p == nil || len(p.Arms) == 0 {
		return shape.Frame(av.MediaVideo)
	}
	spec := shape.Frame(av.MediaVideo, shape.Stream(av.StreamID(p.Name)))
	canvasW, canvasH := 0, 0
	missingGeometry := false
	missingPixelFormat := false
	unsupportedPixelFormat := ""
	for i := range p.Arms {
		arm := compositeArmFrameShape(p.Arms[i])
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

func compositeArmFrameShape(arm JoinArmPlan) shape.Spec {
	if arm.sub != nil && !arm.late {
		return shape.FromStream(arm.sub.Joined, arm.sub.joinedDomain)
	}
	return shape.FromStream(arm.joinStream(), shape.DomainFrame)
}

func compositeArmLayout(p *JoinPlan, index int) CompositeLayout {
	if p == nil || index < 0 || index >= len(p.Layouts) {
		return CompositeLayout{}
	}
	return p.Layouts[index]
}
