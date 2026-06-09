package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
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
	encode *codec.CodecSpec
	taps   []TapRef
}

// compositeRegion is an arm's top-left placement on the composite canvas. It is
// composite-only — it has no meaning outside a Composite arm.
type compositeRegion struct {
	x, y int
}

// Encode encodes the composited stream before the destination, so a Composite can
// record to a File/mux (not only a frame Sink). Without it the composite delivers
// frames.
func (c *compositeStream) Encode(spec codec.CodecSpec) *compositeStream {
	c.encode = &spec
	return c
}

// Tap names the composited stream as a stable frame-domain attach point — the
// same tap a normal chain declares: it appears in task.Taps() and runtime
// branches can Attach from it later.
func (c *compositeStream) Tap(tap TapRef) *compositeStream {
	c.taps = append(c.taps, tap)
	return c
}

// Branches fans the composited stream out to planned branch chains, each with
// its own destinations — the same goav.Branch specs an ordinary stream chain
// accepts after decode.
func (c *compositeStream) Branches(branches ...BranchSpec) *Job {
	return newJoinBranchesJob(joinComposite, joinSpec{arms: c.arms, encode: c.encode, taps: c.taps}, branches)
}

// To delivers the composited stream to a destination and returns a Job, so the
// composite runs through the same Build/Run as every other recipe. It lowers to
// the one joinSpec shared by every convergence builder.
func (c *compositeStream) To(dest Destination) *Job {
	return newJoinJob(joinComposite, joinSpec{arms: c.arms, dest: dest, encode: c.encode, taps: c.taps})
}

// compositeJoinProfile is Composite's entry in the join table: video arms,
// auto-decode for packet arms, per-arm Region layout collection,
// videoCompositeStage convergence, and an encodable output stream derived from
// the first arm's shape.
var compositeJoinProfile = joinProfile{
	media:      av.MediaVideo,
	decodeArms: true,
	// Each arm places its frame on the canvas at its declared Region; an arm
	// without one defaults to the top-left corner {0,0}.
	armStage: func(b *joinBuild, arm *jobStreamBuilder, _ av.Stream, upstream string) (string, error) {
		layout := compositeLayout{}
		if arm.region != nil {
			layout = compositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		b.layouts = append(b.layouts, layout)
		return upstream, nil
	},
	newStage: func(b *joinBuild, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
		return newVideoCompositeStage("composite", armIDs, av.StreamID("composite"), b.layouts), nil
	},
	joinedStream: func(b *joinBuild) av.Stream {
		shape, _ := customSourceShape(b.spec.arms[0].job.inputs[0])
		return av.Stream{
			ID:   av.StreamID("composite"),
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				Type:        av.MediaVideo,
				Width:       shape.Width,
				Height:      shape.Height,
				PixelFormat: shape.PixelFormat,
			},
		}
	},
	sinkOnlyReason: "composite to a non-Sink destination requires .Encode(...)",
}
