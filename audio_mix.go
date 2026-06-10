package goav

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// audioMixStage sums N synchronized audio inputs into one output stream — the
// runtime heart of a convergent Mix. Each input arm is identified by its frame
// StreamID. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
//
// Contract: inputs are same-format S16 interleaved. Arm alignment follows the
// joinSyncState mode: arrival (default — one frame per arm per step) or PTS
// (SyncByPTS — arms join the step their head timestamp matches; arms whose
// head is newer sit the step out, which IS the silence contribution since
// silence is the summing identity). An arm that ends keeps the others mixing
// without it. The shape solver plans per-arm conversions at compile time
// (joinProfile.armExpected under the implicit arm policy), so mismatched arms
// are resampled before they reach this stage; a residual format mismatch still
// fails the first mismatched frame defensively.
type audioMixStage struct {
	name string
	out  av.StreamID
	sync *joinSyncState
}

func newAudioMixStage(name string, inputs []av.StreamID, out av.StreamID, mode joinSyncMode) *audioMixStage {
	return &audioMixStage{name: name, out: out, sync: newJoinSyncState(mode, inputs)}
}

func (s *audioMixStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the planned
// node, keeping Describe() ≡ Build() when SyncByPTS annotates the join.
func (s *audioMixStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: joinSyncNodeDetail(s.sync.mode)}
}

// DroppedMessages implements pipeline.DropReporter: frames the join discarded
// to stay aligned (stale catch-up heads, discontinuity flushes), surfaced as
// the join node's Dropped count (reason "sync") in Stats and snapshots.
func (s *audioMixStage) DroppedMessages() uint64 { return s.sync.droppedFrames() }

func (s *audioMixStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Audio == nil {
			return nil
		}
		s.sync.buffer(cloneMixFrame(msg.Frame))
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
		}
		switch msg.Event.Type {
		case av.EventEndOfStream:
			ended := s.sync.markEOS(msg.Event.StreamID)
			// The ended arm stops gating: drain what the remaining arms can
			// mix before (possibly) ending the joined stream.
			if err := s.drain(ctx, emitter); err != nil {
				return err
			}
			if ended {
				out := av.Event{Type: av.EventEndOfStream, StreamID: s.out}
				return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
			}
			return nil
		case av.EventDiscontinuity:
			if !s.sync.discontinuity(msg.Event.StreamID) {
				return nil
			}
			// The joined timeline jumps with the repositioned arm: forward one
			// discontinuity re-stamped to the join output, the same signal a
			// seeked single-source chain delivers to its sink.
			out := *msg.Event
			out.StreamID = s.out
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		if msg.Event.Reason == selectorActiveReason {
			// Control-plane events ride the data path through joins UNCHANGED:
			// a SelectActive heading for a selector downstream of this join
			// (Select(Mix(a, b), Mix(c, d))) carries its target arm in
			// Event.StreamID and is consumed by the selector — re-stamping it
			// to the mix output would erase the target.
			return emitter.Emit(ctx, msg)
		}
		return nil
	default:
		return nil
	}
}

func (s *audioMixStage) Close() error { return nil }

func (s *audioMixStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		frames, ref, ok := s.sync.step()
		if !ok {
			return nil
		}
		mixed, err := mixS16Frames(compactJoinFrames(frames), s.out)
		if err != nil {
			return err
		}
		// The step's timing reference stamps the output (in arrival mode this
		// is the first arm, exactly as before).
		mixed.PTS = frames[ref].PTS
		mixed.Duration = frames[ref].Duration
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixed}); err != nil {
			return err
		}
	}
}

// mixS16Frames sums S16-interleaved audio frames sample-by-sample with clamping.
func mixS16Frames(frames []*av.Frame, out av.StreamID) (*av.Frame, error) {
	base := frames[0]
	if base.Audio == nil || len(base.Planes) == 0 {
		return nil, fmt.Errorf("goav: mix input has no audio plane")
	}
	n := len(base.Planes[0].Buffer.Bytes)
	for i := range frames {
		f := frames[i]
		if f.Audio == nil || len(f.Planes) == 0 {
			return nil, fmt.Errorf("goav: mix input has no audio plane")
		}
		if f.Audio.SampleFormat != av.SampleFormatS16 {
			return nil, fmt.Errorf("goav: audio mix requires %s, got %s", av.SampleFormatS16, f.Audio.SampleFormat)
		}
		if f.Audio.Channels != base.Audio.Channels || f.Audio.SampleRate != base.Audio.SampleRate {
			return nil, fmt.Errorf("goav: audio mix inputs differ (%d ch/%d Hz vs %d ch/%d Hz)",
				f.Audio.Channels, f.Audio.SampleRate, base.Audio.Channels, base.Audio.SampleRate)
		}
		if l := len(f.Planes[0].Buffer.Bytes); l < n {
			n = l
		}
	}
	n -= n % 2 // whole int16 samples
	mixed := make([]byte, n)
	for off := 0; off < n; off += 2 {
		var sum int32
		for i := range frames {
			sum += int32(int16(binary.LittleEndian.Uint16(frames[i].Planes[0].Buffer.Bytes[off:])))
		}
		if sum > 32767 {
			sum = 32767
		} else if sum < -32768 {
			sum = -32768
		}
		binary.LittleEndian.PutUint16(mixed[off:], uint16(int16(sum)))
	}
	audio := *base.Audio
	audio.Samples = n / 2 / maxInt(base.Audio.Channels, 1)
	return &av.Frame{
		StreamID: out,
		Type:     av.MediaAudio,
		PTS:      base.PTS,
		Duration: base.Duration,
		Audio:    &audio,
		// The mixed buffer is freshly allocated per step and never written
		// again — published immutable, so a buffered graph (a Select over
		// sub-mixes) may queue it by reference.
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: mixed, Ownership: av.BufferImmutable}}},
	}, nil
}

func cloneMixFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		src := frame.Planes[i].Buffer.Bytes
		bytes := make([]byte, len(src))
		copy(bytes, src)
		clone.Planes[i].Buffer = av.Buffer{Bytes: bytes, Ownership: av.BufferOwned}
	}
	return &clone
}

// Mix sums N synchronized audio arms into one stream delivered to a Sink —
// the convergent dual of Branches (N→1). Each arm is a source chain such as
// From(frameSource).Audio() or another audio-producing join — Mix(Mix(a, b), c)
// sub-mixes two arms and mixes the result with a third. Arms must have
// distinct stream ids; mismatched formats resample to the first arm's format
// through the implicit arm policy. This reuses the existing Job, so
// .To/Build/Run are unchanged (see docs/MULTI_INPUT.md).
func Mix(arms ...JoinArm) *mixStream {
	return &mixStream{arms: arms}
}

type mixStream struct {
	arms   []JoinArm
	encode *codec.CodecSpec
	taps   []TapRef
	sync   joinSyncMode
}

// joinArm lets a Mix stand as an arm of an outer join: the outer join consumes
// the MIXED output stream under the sub-mix's output id. A nested mix may not
// carry .Encode(...) — encode belongs to the outer join or its chain.
func (m *mixStream) joinArm() joinArmSpec {
	if m == nil {
		return joinArmSpec{}
	}
	return joinArmSpec{join: &joinSpec{kind: joinMix, arms: m.arms, encode: m.encode, taps: m.taps, sync: m.sync}}
}

// SyncByPTS aligns the arms by presentation timestamp instead of arrival
// order. Per step the earliest head frame across the arms (on a common
// nanosecond clock) sets the step time; arms within tolerance (half a frame
// duration) join it, arms whose head is newer contribute silence for the step,
// and frames left behind the already-mixed timeline are dropped to catch up. A
// discontinuity on one arm (Seek/Segment) flushes that arm's buffer and
// re-syncs at its new position. Use it when arms are files starting at
// different offsets, after a Seek on one arm, or under drift; the arrival
// default pairs frames one-per-arm and is right for live same-clock sources.
func (m *mixStream) SyncByPTS() *mixStream {
	m.sync = joinSyncPTS
	return m
}

// Encode encodes the mixed stream before the destination, so a Mix can record to
// a File/mux (not only a frame Sink). Without it the mix delivers frames.
func (m *mixStream) Encode(spec codec.CodecSpec) *mixStream {
	m.encode = &spec
	return m
}

// Tap names the mixed stream as a stable frame-domain attach point — the same
// tap a normal chain declares: it appears in task.Taps() and runtime branches
// can Attach from it later.
func (m *mixStream) Tap(tap TapRef) *mixStream {
	m.taps = append(m.taps, tap)
	return m
}

// Branches fans the mixed stream out to planned branch chains, each with its
// own destinations — the same goav.Branch specs an ordinary stream chain
// accepts after decode. The mix output is a normal stream point: branches may
// transform, encode, tap, and deliver independently.
func (m *mixStream) Branches(branches ...BranchSpec) *Job {
	return newJoinBranchesJob(joinMix, joinSpec{arms: m.arms, encode: m.encode, taps: m.taps, sync: m.sync}, branches)
}

// To delivers the mixed stream to a destination and returns a Job, so the mix
// runs through the same Build/Run as every other recipe. It lowers to the one
// joinSpec shared by every convergence builder.
func (m *mixStream) To(dest Destination) *Job {
	return newJoinJob(joinMix, joinSpec{arms: m.arms, dest: dest, encode: m.encode, taps: m.taps, sync: m.sync})
}

// mixJoinProfile is Mix's entry in the join table: audio arms, auto-decode for
// packet arms, first-arm-wins format solving through the shared shape solver
// (the implicit always-on arm policy — no user opt-in needed), audioMixStage
// convergence, and an encodable S16 output stream derived from the first arm's
// shape.
var mixJoinProfile = joinProfile{
	media:      av.MediaAudio,
	decodeArms: true,
	// The first arm's audio format is the mix target; later arms that differ
	// get a conversion planned by the shape solver under the arm policy.
	armExpected: func(p *joinPlan, stream av.Stream) shape.Spec {
		if stream.Codec.SampleRate <= 0 {
			return shape.Spec{}
		}
		channels := maxInt(stream.Codec.Channels, 1)
		if p.targetRate == 0 {
			p.targetRate, p.targetChannels = stream.Codec.SampleRate, channels
			return shape.Spec{}
		}
		return shape.Frame(av.MediaAudio, shape.Audio(p.targetRate, p.targetChannels, ""))
	},
	armPolicy: shape.AllowResample().Union(shape.AllowConvert()),
	newStage: func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
		return newAudioMixStage(p.name, armIDs, av.StreamID(p.name), p.join.sync), nil
	},
	// The output id is the join's planned node name (mix, or mix-2 when
	// nested under another mix); the format facts come from the first arm —
	// a leaf's declared source shape or a sub-join's joined stream.
	joinedStream: func(p *joinPlan) av.Stream {
		shape := p.firstArmSourceShape()
		return av.Stream{
			ID:   av.StreamID(p.name),
			Type: av.MediaAudio,
			Codec: av.CodecParameters{
				Type:         av.MediaAudio,
				SampleRate:   shape.SampleRate,
				Channels:     shape.Channels,
				SampleFormat: shape.SampleFormat,
				ClockRate:    uint32(shape.SampleRate),
			},
		}
	},
	sinkOnlyReason: "mix to a non-Sink destination requires .Encode(...)",
}
