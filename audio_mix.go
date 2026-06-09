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
// Contract: inputs are same-format S16 interleaved and frame-aligned (one frame
// per arm advances one mix step). The shape solver plans per-arm conversions at
// compile time (joinProfile.armExpected under the implicit arm policy), so
// mismatched arms are resampled before they reach this stage; a residual
// format mismatch still fails the first mismatched frame defensively.
type audioMixStage struct {
	name    string
	inputs  []av.StreamID
	out     av.StreamID
	pending map[av.StreamID][]*av.Frame
	eos     map[av.StreamID]struct{}
}

func newAudioMixStage(name string, inputs []av.StreamID, out av.StreamID) *audioMixStage {
	return &audioMixStage{
		name:    name,
		inputs:  append([]av.StreamID(nil), inputs...),
		out:     out,
		pending: make(map[av.StreamID][]*av.Frame, len(inputs)),
		eos:     make(map[av.StreamID]struct{}, len(inputs)),
	}
}

func (s *audioMixStage) Name() string { return s.name }

func (s *audioMixStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Audio == nil {
			return nil
		}
		id := msg.Frame.StreamID
		s.pending[id] = append(s.pending[id], cloneMixFrame(msg.Frame))
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event != nil && msg.Event.Type == av.EventEndOfStream {
			s.eos[msg.Event.StreamID] = struct{}{}
			if len(s.eos) >= len(s.inputs) {
				out := av.Event{Type: av.EventEndOfStream, StreamID: s.out}
				return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
			}
		}
		return nil
	default:
		return nil
	}
}

func (s *audioMixStage) Close() error { return nil }

// ready reports whether every input arm has at least one buffered frame.
func (s *audioMixStage) ready() bool {
	for i := range s.inputs {
		if len(s.pending[s.inputs[i]]) == 0 {
			return false
		}
	}
	return true
}

func (s *audioMixStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for s.ready() {
		frames := make([]*av.Frame, len(s.inputs))
		for i := range s.inputs {
			id := s.inputs[i]
			frames[i] = s.pending[id][0]
			s.pending[id] = s.pending[id][1:]
		}
		mixed, err := mixS16Frames(frames, s.out)
		if err != nil {
			return err
		}
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixed}); err != nil {
			return err
		}
	}
	return nil
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
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: mixed, Ownership: av.BufferOwned}}},
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

// Mix sums N synchronized audio source-chains into one stream delivered to a
// Sink — the convergent dual of Branches (N→1). Each arm is a source chain such
// as From(frameSource).Audio(); arms must have distinct stream ids and matching
// S16 audio format. This is the thinnest multi-input entry: one symbol that
// reuses the existing Job, so .To/Build/Run are unchanged. (First slice: frame
// sources to a Sink; decode/encode arms and Composite build on the same info.OpJoin
// mechanism next — see docs/MULTI_INPUT.md.)
func Mix(arms ...*jobStreamBuilder) *mixStream {
	return &mixStream{arms: arms}
}

type mixStream struct {
	arms   []*jobStreamBuilder
	encode *codec.CodecSpec
	taps   []TapRef
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
	return newJoinBranchesJob(joinMix, joinSpec{arms: m.arms, encode: m.encode, taps: m.taps}, branches)
}

// To delivers the mixed stream to a destination and returns a Job, so the mix
// runs through the same Build/Run as every other recipe. It lowers to the one
// joinSpec shared by every convergence builder.
func (m *mixStream) To(dest Destination) *Job {
	return newJoinJob(joinMix, joinSpec{arms: m.arms, dest: dest, encode: m.encode, taps: m.taps})
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
		return newAudioMixStage("mix", armIDs, av.StreamID("mix")), nil
	},
	joinedStream: func(p *joinPlan) av.Stream {
		shape, _ := customSourceShape(p.join.arms[0].job.inputs[0])
		return av.Stream{
			ID:   av.StreamID("mix"),
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
