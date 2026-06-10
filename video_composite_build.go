package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

// Composite overlays N synchronized video arms into one stream delivered to a
// Sink — the video dual of Mix (N→1) and the convergent dual of Branches.
// Each arm is a source chain such as From(frameSource).Video() or another
// video-producing join — Composite(Composite(a, b), logo) paints a sub-canvas
// like any arm. Arms must have distinct stream ids and I420 video format, and
// each may declare its canvas position with .Region(x, y). This reuses the
// existing Job, so .To/Build/Run are unchanged (see docs/MULTI_INPUT.md).
func Composite(arms ...JoinArm) *compositeStream {
	return &compositeStream{arms: arms}
}

type compositeStream struct {
	arms   []JoinArm
	encode *codec.CodecSpec
	taps   []TapRef
	sync   joinSyncMode
	region *compositeRegion
}

// joinArm lets a Composite stand as an arm of an outer join: the outer join
// consumes the COMPOSITED canvas under the sub-composite's output id, placed
// at this composite's .Region(x, y) (top-left by default). A nested composite
// may not carry .Encode(...) — encode belongs to the outer join or its chain.
func (c *compositeStream) joinArm() joinArmSpec {
	if c == nil {
		return joinArmSpec{}
	}
	return joinArmSpec{
		join:   &joinSpec{kind: joinComposite, arms: c.arms, encode: c.encode, taps: c.taps, sync: c.sync},
		region: c.region,
	}
}

// Region places this composite's canvas at (x, y) when the composite is used
// as an arm of an outer Composite. It has no effect on a top-level composite.
func (c *compositeStream) Region(x, y int) *compositeStream {
	c.region = &compositeRegion{x: x, y: y}
	return c
}

// SyncByPTS aligns the arms by presentation timestamp instead of arrival
// order. Per step the earliest head frame across the arms (on a common
// nanosecond clock) sets the step time; arms within tolerance (half a frame
// duration) are painted, an arm whose head is newer is left out of the step
// (the canvas keeps covering its last-known extent so geometry stays stable),
// and frames left behind the already-composited timeline are dropped to catch
// up. A discontinuity on one arm (Seek/Segment) flushes that arm's buffer and
// re-syncs at its new position. Use it when arms are files starting at
// different offsets, after a Seek on one arm, or under drift; the arrival
// default pairs frames one-per-arm and is right for live same-clock sources.
func (c *compositeStream) SyncByPTS() *compositeStream {
	c.sync = joinSyncPTS
	return c
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
	return newJoinBranchesJob(joinComposite, joinSpec{arms: c.arms, encode: c.encode, taps: c.taps, sync: c.sync}, branches)
}

// To delivers the composited stream to a destination and returns a Job, so the
// composite runs through the same Build/Run as every other recipe. It lowers to
// the one joinSpec shared by every convergence builder.
func (c *compositeStream) To(dest Destination) *Job {
	return newJoinJob(joinComposite, joinSpec{arms: c.arms, dest: dest, encode: c.encode, taps: c.taps, sync: c.sync})
}

// compositeJoinProfile is Composite's entry in the join table: video arms,
// auto-decode for packet arms, per-arm Region layout collection,
// videoCompositeStage convergence, and an encodable output stream derived from
// the first arm's shape.
var compositeJoinProfile = joinProfile{
	media:      av.MediaVideo,
	decodeArms: true,
	// Each arm places its frame on the canvas at its declared Region — a chain
	// arm's .Region(x, y) or a nested composite's — defaulting to the top-left
	// corner {0,0}.
	planArm: func(p *joinPlan, arm joinArmSpec, _ av.Stream) (*joinArmStagePlan, error) {
		layout := compositeLayout{}
		if arm.region != nil {
			layout = compositeLayout{X: arm.region.x, Y: arm.region.y}
		}
		p.layouts = append(p.layouts, layout)
		return nil, nil
	},
	newStage: func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
		return newVideoCompositeStage(p.name, armIDs, av.StreamID(p.name), p.layouts, p.join.sync), nil
	},
	// The output id is the join's planned node name (composite, or composite-2
	// when nested); the format facts come from the first arm — a leaf's
	// declared source shape or a sub-join's joined stream.
	joinedStream: func(p *joinPlan) av.Stream {
		shape := p.firstArmSourceShape()
		return av.Stream{
			ID:   av.StreamID(p.name),
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
	sinkOnlySuggestions: []string{
		"encode the composited video first: goav.Composite(a, b).Encode(codec.VP8(codec.Bitrate(1_000_000))).To(out)",
		"deliver raw composited frames with .To(goav.Sink(sink)) instead",
	},
}
