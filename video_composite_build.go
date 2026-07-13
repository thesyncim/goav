package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Composite overlays N synchronized video arms into one stream delivered to
// its destinations — the video dual of Mix (N→1) and the convergent dual of
// Branches.
// Each arm is a source chain such as From(frameSource).Video() or another
// video-producing join — Composite(Composite(a, b), logo) paints a sub-canvas
// like any arm. Arms must have distinct stream ids and I420 video format, and
// each may declare its canvas position with .Region(x, y). This reuses the
// existing Job, so .To/Build/Run are unchanged (see docs/MULTI_INPUT.md).
func Composite(arms ...JoinArm) *CompositeStream {
	return &CompositeStream{arms: arms}
}

// CompositeStream is the builder Composite returns: the join grammar surface
// (Region/SyncByPTS/Auto/Require/Prefer/Encode/Tap/Branches/To), plus the
// unexported joinArm method so a composite nests as an arm of an outer join.
// It is sealed — its fields are unexported, so the grammar methods are the
// only mutation surface.
type CompositeStream struct {
	arms       []JoinArm
	encode     *codec.Spec
	taps       []tapRef
	operations []operationSpec
	sync       joinSyncMode
	region     *compositeRegion
}

// joinArm lets a Composite stand as an arm of an outer join: the outer join
// consumes the COMPOSITED canvas under the sub-composite's output id, placed
// at this composite's .Region(x, y) (top-left by default). A nested composite
// may not carry .Encode(...) — encode belongs to the outer join or its chain.
func (c *CompositeStream) joinArm() joinArmSpec {
	if c == nil {
		return joinArmSpec{}
	}
	return joinArmSpec{
		join:   &joinSpec{kind: joinComposite, arms: c.arms, encode: c.encode, operations: cloneOperationSpecs(c.operations), taps: c.taps, sync: c.sync},
		region: c.region,
	}
}

// Region places this composite's canvas at (x, y) when the composite is used
// as an arm of an outer Composite. It has no effect on a top-level composite.
func (c *CompositeStream) Region(x, y int) *CompositeStream {
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
func (c *CompositeStream) SyncByPTS() *CompositeStream {
	c.sync = joinSyncPTS
	return c
}

// Auto lets the planner insert format conversions on the composited output
// before a terminal .Encode(...) or planned branches, using the same shape
// solver as a normal stream chain.
func (c *CompositeStream) Auto(policies ...shape.Policy) *CompositeStream {
	c.operations = append(c.operations, operationSpecForAutoPolicy(policies))
	return c
}

// Require asserts shape facts on the composited output before the terminal
// encode or planned branches.
func (c *CompositeStream) Require(spec shape.Spec) *CompositeStream {
	c.operations = append(c.operations, operationSpecForRequire(spec))
	return c
}

// Prefer biases automatic conversion adapter selection on the composited output.
func (c *CompositeStream) Prefer(spec shape.Spec) *CompositeStream {
	c.operations = append(c.operations, operationSpecForPreference(spec))
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
func (c *CompositeStream) Encode(spec codec.Spec) *CompositeStream {
	c.encode = &spec
	return c
}

// Tap names the composited stream as a stable frame-domain attach point — the
// same tap a normal chain declares: it appears in task.Taps() and runtime
// branches can Attach from it later.
func (c *CompositeStream) Tap(tap tapRef) *CompositeStream {
	c.taps = append(c.taps, tap)
	return c
}

// Branches fans the composited stream out to planned branch chains, each with
// its own destinations — the same goav.Branch specs an ordinary stream chain
// accepts after decode.
func (c *CompositeStream) Branches(branches ...BranchSpec) *Job {
	return newJoinBranchesJob(joinComposite, joinSpec{arms: c.arms, encode: c.encode, operations: cloneOperationSpecs(c.operations), taps: c.taps, sync: c.sync}, branches)
}

// To delivers the composited stream to one or more destinations (a fanout
// when several are given — each destination receives the joined stream,
// exactly like a chain's multi-destination .To) and returns a Job, so the
// composite runs through the same Build/Run as every other recipe. It lowers
// to the one joinSpec shared by every convergence builder.
func (c *CompositeStream) To(destinations ...Destination) *Job {
	return newJoinJob(joinComposite, joinSpec{arms: c.arms, dests: destinations, encode: c.encode, operations: cloneOperationSpecs(c.operations), taps: c.taps, sync: c.sync})
}

// compositeJoinProfile is Composite's entry in the join table: video arms,
// explicit packet-arm decode, per-arm Region layout collection,
// videoCompositeStage convergence, and an encodable output stream derived from
// the Region bounding box over every arm's frame shape.
var compositeJoinProfile = joinProfile{
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
	if arm.sub != nil {
		return shape.FromStream(arm.sub.joined, arm.sub.joinedDomain)
	}
	return shape.FromStream(arm.stream, shape.DomainFrame)
}

func compositeArmLayout(p *joinPlan, index int) compositeLayout {
	if p == nil || index < 0 || index >= len(p.layouts) {
		return compositeLayout{}
	}
	return p.layouts[index]
}
