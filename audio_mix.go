package goav

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// audioMixStage sums N synchronized audio inputs into one output stream — the
// runtime heart of a convergent Mix. Each input arm is identified by its frame
// StreamID. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
//
// First-slice contract: inputs are same-format S16 interleaved and frame-aligned
// (one frame per arm advances one mix step). The shape solver will later
// negotiate/insert conversion so this precondition is guaranteed; for now a
// format mismatch fails the open of the first mismatched frame.
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
// sources to a Sink; decode/encode arms and Composite build on the same OpJoin
// mechanism next — see docs/MULTI_INPUT.md.)
func Mix(arms ...*jobStreamBuilder) *mixStream {
	return &mixStream{arms: arms}
}

type mixStream struct {
	arms   []*jobStreamBuilder
	encode *CodecSpec
}

type mixSpec struct {
	arms   []*jobStreamBuilder
	dest   Destination
	encode *CodecSpec
}

// Encode encodes the mixed stream before the destination, so a Mix can record to
// a File/mux (not only a frame Sink). Without it the mix delivers frames.
func (m *mixStream) Encode(spec CodecSpec) *mixStream {
	m.encode = &spec
	return m
}

// To delivers the mixed stream to a destination and returns a Job, so the mix
// runs through the same Build/Run as every other recipe.
func (m *mixStream) To(dest Destination) *Job {
	job := &Job{name: "mix", runtime: Default()}
	if len(m.arms) < 2 {
		job.setErr(&BuildError{Code: "mix_inputs", Operation: "build mix", Node: "mix", Reason: "mix requires at least two source arms", Cause: ErrUnsupportedBuild})
		return job
	}
	job.mix = &mixSpec{arms: m.arms, dest: dest, encode: m.encode}
	return job
}

func (j *Job) buildMix(ctx context.Context) (Task, error) {
	if j.err != nil {
		return nil, j.err
	}
	rt, ok := j.runtime.(*runtime)
	if !ok {
		return nil, ErrExpertRuntimeRequired
	}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "goav-mix", Buffer: rt.buffer, Realtime: rt.realtime})
	if err != nil {
		return nil, err
	}
	armRefs := make([]string, 0, len(j.mix.arms))
	armIDs := make([]av.StreamID, 0, len(j.mix.arms))
	seen := make(map[av.StreamID]struct{}, len(j.mix.arms))
	service := &builder{runtime: rt}
	var targetRate, targetChannels int
	for i := range j.mix.arms {
		arm := j.mix.arms[i]
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			graph.Close()
			return nil, &BuildError{Code: "mix_arm", Operation: "build mix", Node: "mix", Reason: "each mix arm must be a single-input source chain", Cause: ErrUnsupportedBuild}
		}
		source, streams, domain, err := arm.job.inputs[0].openGraphSource(ctx, service, i)
		if err != nil {
			graph.Close()
			return nil, err
		}
		stream, err := selectStream(streams, av.StreamSelector{Type: av.MediaAudio})
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		id := stream.ID
		if _, dup := seen[id]; dup {
			source.Close()
			graph.Close()
			return nil, &BuildError{Code: "mix_arm", Operation: "build mix", Node: string(id), Reason: "mix arms must have distinct stream ids", Cause: ErrUnsupportedBuild}
		}
		seen[id] = struct{}{}
		srcRef, err := graph.AddSource(source, rt.buffer)
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		upstream := string(srcRef)
		// Packet-domain arms decode to frames before the mix (auto-inserted —
		// the mixer sums decoded audio). Frame-domain arms feed it directly.
		if domain == shape.DomainPacket {
			request := decodeRequest{selector: av.StreamSelector{Type: stream.Type}}
			decodeStage, err := service.newDecodeStageNamed(ctx, "mix-decode-"+string(id), request, stream, rt.realtime, true, codec.DecodeBounds{})
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
		// Join shape-solving: the first arm's audio format is the mix target;
		// later arms that differ get an auto-inserted resample so the mixer sees
		// one format. No new API — the arms just declare their own shape.
		if stream.Codec.SampleRate > 0 {
			channels := maxInt(stream.Codec.Channels, 1)
			if targetRate == 0 {
				targetRate, targetChannels = stream.Codec.SampleRate, channels
			} else if stream.Codec.SampleRate != targetRate || channels != targetChannels {
				armStream := av.Stream{ID: id, Type: av.MediaAudio, Codec: av.CodecParameters{
					Type: av.MediaAudio, SampleRate: stream.Codec.SampleRate, Channels: channels,
					SampleFormat: av.SampleFormatS16, ClockRate: uint32(stream.Codec.SampleRate),
				}}
				transform := mediaTransform{
					name:    "mix-resample-" + string(id),
					factory: filter.FactoryResample,
					audio:   &filter.ResampleConfig{SampleRate: targetRate, Channels: targetChannels, SampleFormat: av.SampleFormatS16},
				}
				stage, _, err := service.newMediaTransformStageNamed(ctx, transform.name, transform, armStream, rt.realtime)
				if err != nil {
					graph.Close()
					return nil, err
				}
				resampleRef, err := graph.AddStage(stage, rt.buffer)
				if err != nil {
					graph.Close()
					return nil, err
				}
				if err := graph.Connect(pipeline.Route{From: upstream, To: []string{string(resampleRef)}, Policy: pipeline.RouteAll}); err != nil {
					graph.Close()
					return nil, err
				}
				upstream = string(resampleRef)
			}
		}
		armRefs = append(armRefs, upstream)
		armIDs = append(armIDs, id)
	}
	mixRef, err := graph.AddStage(newAudioMixStage("mix", armIDs, av.StreamID("mix")), rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(mixRef)}, Policy: pipeline.RouteAll}); err != nil {
			graph.Close()
			return nil, err
		}
	}
	if j.mix.encode != nil {
		shape, _ := customSourceShape(j.mix.arms[0].job.inputs[0])
		mixedStream := av.Stream{
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
		request := encodeRequest{name: "mix-encode", selector: av.StreamSelector{Type: av.MediaAudio}, config: encodeConfigFromSpec(*j.mix.encode)}
		config, encodedStream, err := prepareEncodeConfig(mixedStream, request, rt.realtime)
		if err != nil {
			graph.Close()
			return nil, err
		}
		service := &builder{runtime: rt}
		if err := compileEncodeDestinationPath(ctx, service, graph, mixRef, request, config, encodedStream, []destinationSpec{j.mix.dest.spec}); err != nil {
			graph.Close()
			return nil, err
		}
		// openMuxDestinationStage records the destination transaction (file commit/
		// abort) on service; carry it to the task so the mix file finalizes.
		return newTask(graph, rt, service.destinationTxs...), nil
	}
	sink := j.mix.dest.spec.sink
	if sink == nil {
		graph.Close()
		return nil, &BuildError{Code: "mix_destination", Operation: "build mix", Node: "mix", Reason: "mix to a non-Sink destination requires .Encode(...)", Cause: ErrUnsupportedBuild}
	}
	sinkRef, err := graph.AddSink(sink, rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	if err := graph.Connect(pipeline.Route{From: string(mixRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll}); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, rt), nil
}
